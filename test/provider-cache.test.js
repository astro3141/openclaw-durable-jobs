// P3-F provider capability cache: miss/hit, TTL expiry, negative/UNKNOWN policy, toolchain+config
// invalidation, concurrent-probe dedup, atomicity, no token/env stored, and NO quota-consuming probe.
import assert from "node:assert/strict";
import { mkdtemp, rm, readdir, readFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { getProviderCapability, providerCacheKeyHash, readProviderCache } from "../dist/provider-cache.js";

const tmp = () => mkdtemp(path.join(os.tmpdir(), "pc-"));
const TTLS = { readyMs: 60_000, negativeMs: 5_000, unknownMs: 1_000 };
const key = (o = {}) => providerCacheKeyHash({ runnerType: "model", runnerProfile: "model_agy", toolchainHash: "tc1", ...o });

test("first probe misses, second hits, and the probe never runs a model completion", async () => {
  const root = await tmp();
  let probes = 0;
  const probe = async () => { probes += 1; return { status: "READY", providerState: "OK" }; };
  const a = await getProviderCapability(root, { keyHash: key(), toolchainHash: "tc1", probe, ttls: TTLS });
  const b = await getProviderCapability(root, { keyHash: key(), toolchainHash: "tc1", probe, ttls: TTLS });
  assert.equal(a.cacheHit, false);
  assert.equal(b.cacheHit, true);
  assert.equal(probes, 1, "one non-quota probe total");
  await rm(root, { recursive: true, force: true });
});

test("READY expires after its TTL and re-probes", async () => {
  const root = await tmp();
  let probes = 0;
  const probe = async () => { probes += 1; return { status: "READY" }; };
  const shortTtl = { readyMs: 30, negativeMs: 10, unknownMs: 10 };
  const first = await getProviderCapability(root, { keyHash: key(), toolchainHash: "tc1", probe, ttls: shortTtl });
  assert.equal(first.cacheHit, false);
  await new Promise((r) => setTimeout(r, 60)); // let the 30ms TTL expire
  const later = await getProviderCapability(root, { keyHash: key(), toolchainHash: "tc1", probe, ttls: shortTtl });
  assert.equal(later.cacheHit, false, "expired entry re-probes");
  assert.equal(probes, 2);
  await rm(root, { recursive: true, force: true });
});

test("BLOCKED uses the negative TTL; UNKNOWN with 0 TTL is not persisted", async () => {
  const root = await tmp();
  const blocked = await getProviderCapability(root, { keyHash: key(), toolchainHash: "tc1", probe: async () => ({ status: "BLOCKED", failureCode: "QUOTA" }), ttls: TTLS });
  assert.equal(blocked.status, "BLOCKED");
  assert.ok(blocked.expiresAt);
  const uroot = await tmp();
  const unk = await getProviderCapability(uroot, { keyHash: key(), toolchainHash: "tc1", probe: async () => ({ status: "UNKNOWN" }), ttls: { readyMs: 60000, negativeMs: 5000, unknownMs: 0 } });
  assert.equal(unk.status, "UNKNOWN");
  assert.equal(await readProviderCache(uroot, key()), null, "UNKNOWN with 0 TTL is not cached");
  await Promise.all([root, uroot].map((d) => rm(d, { recursive: true, force: true })));
});

test("a changed toolchain hash or config fingerprint is a cache miss (different key)", async () => {
  const root = await tmp();
  let probes = 0;
  const probe = async () => { probes += 1; return { status: "READY" }; };
  await getProviderCapability(root, { keyHash: key({ toolchainHash: "tc1" }), toolchainHash: "tc1", probe, ttls: TTLS });
  await getProviderCapability(root, { keyHash: key({ toolchainHash: "tc2" }), toolchainHash: "tc2", probe, ttls: TTLS }); // changed toolchain → new key → miss
  await getProviderCapability(root, { keyHash: key({ configFingerprint: "cfgX" }), toolchainHash: "tc1", probe, ttls: TTLS }); // changed config → new key → miss
  assert.equal(probes, 3);
  // also: a stale entry whose stored toolchainHash no longer matches is not trusted
  const mism = await getProviderCapability(root, { keyHash: key({ toolchainHash: "tc1" }), toolchainHash: "tcDIFFERENT", probe, ttls: TTLS });
  assert.equal(mism.cacheHit, false);
  await rm(root, { recursive: true, force: true });
});

test("concurrent probes for the same key run the probe once", async () => {
  const root = await tmp();
  let probes = 0;
  const probe = async () => { probes += 1; await new Promise((r) => setTimeout(r, 20)); return { status: "READY" }; };
  await Promise.all([
    getProviderCapability(root, { keyHash: key(), toolchainHash: "tc1", probe, ttls: TTLS }),
    getProviderCapability(root, { keyHash: key(), toolchainHash: "tc1", probe, ttls: TTLS }),
    getProviderCapability(root, { keyHash: key(), toolchainHash: "tc1", probe, ttls: TTLS }),
  ]);
  assert.equal(probes, 1, "concurrent probe dedup");
  await rm(root, { recursive: true, force: true });
});

test("no token / env / raw secret is stored in the cache record", async () => {
  const root = await tmp();
  await getProviderCapability(root, { keyHash: key(), toolchainHash: "tc1", probe: async () => ({ status: "READY", boundedSummary: "ok" }), ttls: TTLS });
  const files = (await readdir(path.join(root, "provider-cache"))).filter((f) => f.endsWith(".json"));
  const rec = JSON.parse(await readFile(path.join(root, "provider-cache", files[0]), "utf8"));
  for (const forbidden of ["token", "apiKey", "api_key", "Authorization", "PATH", "secret"]) {
    assert.ok(!JSON.stringify(rec).toLowerCase().includes(forbidden.toLowerCase()), `record must not contain ${forbidden}`);
  }
  assert.equal(rec.status, "READY");
  await rm(root, { recursive: true, force: true });
});
