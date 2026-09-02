// P3-F provider capability cache. Pure module (no OpenClaw SDK; the readiness probe is INJECTED). ADVISORY
// only — the actual durable-job outcome is always authoritative. It NEVER sends a model completion / consumes
// quota / polls: the probe seam must be a non-quota local readiness check (or return UNKNOWN). The cache key
// binds the runner profile to the toolchain hash + a non-secret config fingerprint + the probe version, so a
// changed executable or config is an automatic miss. No token/API key/env is ever stored.
import { mkdir, rm, stat, readFile, rename, open } from "node:fs/promises";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";

export const PROVIDER_PROBE_VERSION = 1;
export const PROVIDER_CAPABILITIES = new Set(["READY", "BLOCKED", "UNKNOWN"]);
const LOCK_STALE_MS = 30_000;
const LOCK_TIMEOUT_MS = 10_000;

const sha256 = (v) => createHash("sha256").update(v).digest("hex");

export function providerCacheKeyHash({ runnerType, runnerProfile, toolchainHash, probeVersion = PROVIDER_PROBE_VERSION, configFingerprint = "none" }) {
  // NEVER include secrets — only non-secret identity + the toolchain aggregate hash.
  return sha256(JSON.stringify({ runnerType, runnerProfile, toolchainHash, probeVersion, configFingerprint }));
}

function cacheFile(root, keyHash) { return path.join(root, "provider-cache", `${keyHash}.json`); }

async function atomicWriteJson(file, value) {
  const tmp = `${file}.${process.pid}.${randomUUID()}.tmp`;
  let renamed = false, fh;
  try {
    fh = await open(tmp, "wx", 0o600);
    await fh.writeFile(`${JSON.stringify(value, null, 2)}\n`);
    await fh.sync();
    await fh.close(); fh = null;
    await rename(tmp, file); renamed = true;
  } finally {
    if (fh) await fh.close().catch(() => {});
    if (!renamed) await rm(tmp, { force: true }).catch(() => {});
  }
}

export async function readProviderCache(root, keyHash) {
  try { return JSON.parse(await readFile(cacheFile(root, keyHash), "utf8")); } catch { return null; }
}

async function withCacheLock(root, keyHash, fn) {
  const dir = path.join(root, "provider-cache", ".locks");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const lockPath = path.join(dir, `${keyHash}.lock`);
  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  let release;
  while (true) {
    try { await mkdir(lockPath, { mode: 0o700 }); release = async () => rm(lockPath, { recursive: true, force: true }); break; }
    catch (e) {
      if (e?.code !== "EEXIST") throw e;
      try { const info = await stat(lockPath); if (Date.now() - info.mtimeMs > LOCK_STALE_MS) { await rm(lockPath, { recursive: true, force: true }); continue; } }
      catch (se) { if (se?.code !== "ENOENT") throw se; continue; }
      if (Date.now() >= deadline) { const err = new Error("PROVIDER_CACHE_LOCK_TIMEOUT"); err.code = "PROVIDER_CACHE_LOCK_TIMEOUT"; throw err; }
      await new Promise((r) => setTimeout(r, 40));
    }
  }
  try { return await fn(); } finally { await release(); }
}

function ttlFor(status, ttls) {
  if (status === "READY") return ttls.readyMs;
  if (status === "BLOCKED") return ttls.negativeMs;
  return ttls.unknownMs; // UNKNOWN — very short (or 0 = never cached as authoritative)
}

// Return the provider capability for a key: a fresh cached entry is a HIT; otherwise the INJECTED probe runs
// once (serialized by the cache lock so concurrent callers probe at most once) and the result is cached with
// a status-dependent TTL. An expired entry is never treated as authoritative. `probe` returns
// { status: READY|BLOCKED|UNKNOWN, providerState?, failureCode?, boundedSummary? } and MUST NOT consume quota.
export async function getProviderCapability(root, { keyHash, toolchainHash, probe, ttls, now = Date.now() }) {
  const cached = await readProviderCache(root, keyHash);
  if (cached && cached.toolchainHash === toolchainHash && cached.expiresAt && new Date(cached.expiresAt).getTime() > now) {
    return { ...cached, cacheHit: true };
  }
  return withCacheLock(root, keyHash, async () => {
    // double-check inside the lock (a peer may have just probed)
    const again = await readProviderCache(root, keyHash);
    if (again && again.toolchainHash === toolchainHash && again.expiresAt && new Date(again.expiresAt).getTime() > Date.now()) {
      return { ...again, cacheHit: true };
    }
    const result = await probe(); // injected, non-quota
    const status = PROVIDER_CAPABILITIES.has(result?.status) ? result.status : "UNKNOWN";
    const ttl = ttlFor(status, ttls);
    const checkedAt = new Date().toISOString();
    const record = {
      version: 1, cacheKeyHash: keyHash, status,
      providerState: result?.providerState ?? null,
      checkedAt, expiresAt: ttl > 0 ? new Date(Date.now() + ttl).toISOString() : null,
      probeVersion: PROVIDER_PROBE_VERSION, toolchainHash,
      failureCode: result?.failureCode ?? null,
      boundedSummary: result?.boundedSummary ? String(result.boundedSummary).slice(0, 240) : null,
    };
    if (ttl > 0) {
      await mkdir(path.dirname(cacheFile(root, keyHash)), { recursive: true, mode: 0o700 });
      await atomicWriteJson(cacheFile(root, keyHash), record);
    }
    return { ...record, cacheHit: false };
  });
}

// Public-safe projection (no cache key, no summary internals beyond status).
export function publicProviderCapability(cap) {
  if (!cap) return null;
  return { status: cap.status, providerCapability: cap.status, cacheHit: cap.cacheHit ?? false, checkedAt: cap.checkedAt ?? null, failureCode: cap.failureCode ?? null };
}
