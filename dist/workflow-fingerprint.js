// P3-F canonical execution fingerprints. Pure module (no OpenClaw SDK). Produces a read-only Git worktree
// checkpoint fingerprint and an executable/toolchain fingerprint. argv-only git spawns (no shell), bounded
// timeout / output, fixed locale, never run under the workflow lock, never persists raw diffs / untracked
// file contents / env / credentials. A fingerprint is COMPLETE only when it fully and deterministically
// captured the state; anything ambiguous is INCOMPLETE/UNAVAILABLE and must NOT be auto-trusted.
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { lstat, readlink, realpath, open, readdir } from "node:fs/promises";
import path from "node:path";

export const FINGERPRINT_VERSION = 1;
const GIT_TIMEOUT_MS = 15_000;
const GIT_MAX_BUFFER = 64 * 1024 * 1024;
const DEFAULT_MAX_FILES = 5000;
const DEFAULT_MAX_BYTES = 256 * 1024 * 1024;

const sha256 = (buf) => createHash("sha256").update(buf).digest("hex");
const canonicalHash = (obj) => sha256(JSON.stringify(obj));

function git(cwd, args, { buffer = false, timeoutMs = GIT_TIMEOUT_MS } = {}) {
  return new Promise((resolve, reject) => {
    execFile(
      "git",
      ["-c", "core.quotepath=false", ...args],
      { cwd, timeout: timeoutMs, maxBuffer: GIT_MAX_BUFFER, encoding: buffer ? "buffer" : "utf8", env: { ...process.env, LC_ALL: "C", LANG: "C", GIT_OPTIONAL_LOCKS: "0", GIT_TERMINAL_PROMPT: "0" } },
      (err, stdout) => (err ? reject(err) : resolve(stdout)),
    );
  });
}

async function sha256File(file, remainingBytes) {
  const fh = await open(file, "r");
  try {
    const hash = createHash("sha256");
    let read = 0;
    const buf = Buffer.allocUnsafe(1 << 16);
    while (true) {
      const { bytesRead } = await fh.read(buf, 0, buf.length, null);
      if (bytesRead === 0) break;
      read += bytesRead;
      if (read > remainingBytes) return { over: true, read };
      hash.update(buf.subarray(0, bytesRead));
    }
    return { over: false, read, hash: hash.digest("hex") };
  } finally {
    await fh.close();
  }
}

// Hash the content of an untracked path (file / symlink), or return { special:true } for a type we cannot
// deterministically fingerprint (socket/fifo/device). Directories are walked in lexical order.
async function hashUntracked(abs, budget) {
  const st = await lstat(abs);
  if (st.isSymbolicLink()) return { entries: [["L", sha256(`symlink:${await readlink(abs)}`)]], bytes: 0, files: 1 };
  if (st.isFile()) {
    const res = await sha256File(abs, budget.bytes);
    if (res.over) return { over: true };
    return { entries: [["F", res.hash]], bytes: res.read, files: 1 };
  }
  if (st.isDirectory()) {
    const names = (await readdir(abs)).sort();
    const entries = [];
    let bytes = 0, files = 0;
    for (const name of names) {
      const child = await hashUntracked(path.join(abs, name), { bytes: budget.bytes - bytes });
      if (child.special || child.over) return child;
      entries.push([name, canonicalHash(child.entries)]);
      bytes += child.bytes; files += child.files;
    }
    return { entries, bytes, files };
  }
  return { special: true }; // socket / fifo / device → cannot be COMPLETE
}

function unavailable(worktree, reason) {
  return { fingerprintVersion: FINGERPRINT_VERSION, status: "UNAVAILABLE", reason, repository: { worktreeRealpath: null, repoRootRealpath: null, headCommit: null, branch: null, detached: null }, components: null, aggregateHash: null, capturedAt: new Date().toISOString() };
}

// Read-only Git worktree checkpoint fingerprint. Ignored files are OUT of scope (git respects .gitignore).
export async function computeWorktreeFingerprint(worktree, { maxFiles = DEFAULT_MAX_FILES, maxBytes = DEFAULT_MAX_BYTES, timeoutMs = GIT_TIMEOUT_MS } = {}) {
  const g = (root, args, opt = {}) => git(root, args, { timeoutMs, ...opt });
  let worktreeRealpath;
  try { worktreeRealpath = await realpath(worktree); } catch { return unavailable(worktree, "WORKTREE_MISSING"); }
  let repoRootRealpath;
  try { repoRootRealpath = await realpath((await g(worktreeRealpath, ["rev-parse", "--show-toplevel"])).trim()); } catch { return unavailable(worktree, "NOT_A_GIT_REPO"); }
  let headCommit = null, branch = null, detached = false;
  try { headCommit = (await g(repoRootRealpath, ["rev-parse", "HEAD"])).trim(); } catch { headCommit = null; }
  try { const b = (await g(repoRootRealpath, ["symbolic-ref", "--short", "-q", "HEAD"])).trim(); branch = b || null; detached = !b; } catch { branch = null; detached = true; }

  const stagedDiffHash = sha256(await g(repoRootRealpath, ["diff", "--cached", "--binary", "--no-ext-diff"], { buffer: true }));
  const trackedDiffHash = sha256(await g(repoRootRealpath, ["diff", "--binary", "--no-ext-diff"], { buffer: true }));
  const untrackedRaw = await g(repoRootRealpath, ["ls-files", "-z", "--others", "--exclude-standard"]);
  const untrackedPaths = untrackedRaw.split("\0").filter(Boolean).sort();

  let incomplete = false;
  let untrackedFileCount = 0;
  let totalBytes = 0;
  const untrackedEntries = [];
  if (untrackedPaths.length > maxFiles) incomplete = true;
  for (const rel of untrackedPaths) {
    if (incomplete) break;
    const child = await hashUntracked(path.join(repoRootRealpath, rel), { bytes: maxBytes - totalBytes }).catch(() => ({ special: true }));
    if (child.special || child.over) { incomplete = true; break; }
    untrackedEntries.push([rel, canonicalHash(child.entries)]);
    untrackedFileCount += child.files; totalBytes += child.bytes;
    if (untrackedFileCount > maxFiles || totalBytes > maxBytes) { incomplete = true; break; }
  }
  const untrackedContentHash = canonicalHash(untrackedEntries);
  const components = { stagedDiffHash, trackedDiffHash, untrackedContentHash, untrackedFileCount };
  const repository = { worktreeRealpath, repoRootRealpath, headCommit, branch, detached };
  const aggregateHash = canonicalHash({ v: FINGERPRINT_VERSION, worktreeRealpath, repoRootRealpath, headCommit, branch, detached, ...components });
  return { fingerprintVersion: FINGERPRINT_VERSION, status: incomplete ? "INCOMPLETE" : "COMPLETE", repository, components, aggregateHash, capturedAt: new Date().toISOString() };
}

// Resolve an executable the same way the worker's spawn would: an argv[0] with a slash is relative to cwd
// (or absolute); a bare name is looked up on PATH. Returns the resolved absolute path or null.
export async function resolveExecutable(argv0, cwd, env = process.env) {
  if (!argv0) return null;
  if (argv0.includes("/")) {
    const abs = path.resolve(cwd, argv0);
    return (await lstat(abs).then((s) => s.isFile() || s.isSymbolicLink()).catch(() => false)) ? abs : null;
  }
  for (const dir of (env.PATH ?? "").split(path.delimiter).filter(Boolean)) {
    const abs = path.join(dir, argv0);
    if (await lstat(abs).then((s) => s.isFile() || s.isSymbolicLink()).catch(() => false)) return abs;
  }
  return null;
}

// Executable/toolchain fingerprint for one execution candidate. `versionProbe(realpath)` is an OPTIONAL
// injected non-quota probe returning a bounded string; its output is stored ONLY as a hash + bounded summary
// (never raw). No probe → versionOutputHash null.
export async function computeToolchainFingerprint({ argv, cwd, runnerType, runnerProfile, versionProbe } = {}) {
  const base = { fingerprintVersion: FINGERPRINT_VERSION, runnerType: runnerType ?? null, runnerProfile: runnerProfile ?? null, executableBasename: null, executableRealpath: null, executableContentHash: null, executableSize: null, versionOutputHash: null, versionSummary: null, aggregateHash: null };
  const argv0 = Array.isArray(argv) && argv.length ? argv[0] : null;
  const resolved = await resolveExecutable(argv0, cwd);
  if (!resolved) return { ...base, executableBasename: argv0 ? path.basename(argv0) : null, status: "MISSING" };
  const real = await realpath(resolved).catch(() => resolved);
  const st = await lstat(real).catch(() => null);
  if (!st || !st.isFile()) return { ...base, executableBasename: path.basename(real), executableRealpath: real, status: "UNSUPPORTED" };
  const contentHash = (await sha256File(real, Number.MAX_SAFE_INTEGER)).hash;
  let versionOutputHash = null, versionSummary = null;
  if (typeof versionProbe === "function") {
    try { const out = String(await versionProbe(real)).slice(0, 4096); versionOutputHash = sha256(out); versionSummary = out.slice(0, 120); } catch { /* probe optional */ }
  }
  const fp = { ...base, executableBasename: path.basename(real), executableRealpath: real, executableContentHash: contentHash, executableSize: st.size, versionOutputHash, versionSummary };
  fp.aggregateHash = canonicalHash({ v: FINGERPRINT_VERSION, runnerType: fp.runnerType, runnerProfile: fp.runnerProfile, real, contentHash, size: st.size, versionOutputHash });
  return { ...fp, status: "COMPLETE" };
}

// Public-safe projection of a worktree fingerprint (hashes only; never realpaths, diffs, or file names).
export function publicWorktreeCheckpoint(fp) {
  if (!fp) return null;
  return { aggregateHash: fp.aggregateHash, complete: fp.status === "COMPLETE", status: fp.status, capturedAt: fp.capturedAt };
}
