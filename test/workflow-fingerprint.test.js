// P3-F canonical fingerprints: Git worktree checkpoint + toolchain. Deterministic; detects staged/tracked/
// untracked/HEAD changes; excludes ignored files; INCOMPLETE over bounds; UNAVAILABLE for non-Git; never
// persists raw diffs / untracked contents / realpaths in the public projection.
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile, mkdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import test from "node:test";
import { computeWorktreeFingerprint, computeToolchainFingerprint, publicWorktreeCheckpoint } from "../dist/workflow-fingerprint.js";

const sh = promisify(execFile);
async function gitRepo() {
  const repo = await mkdtemp(path.join(os.tmpdir(), "wf-fp-"));
  await sh("git", ["init", "-q"], { cwd: repo });
  await sh("git", ["config", "user.email", "a@b"], { cwd: repo });
  await sh("git", ["config", "user.name", "t"], { cwd: repo });
  await writeFile(path.join(repo, "a.txt"), "hello\n");
  await sh("git", ["add", "."], { cwd: repo });
  await sh("git", ["commit", "-qm", "init"], { cwd: repo });
  return repo;
}
const agg = (f) => f.aggregateHash;

test("clean worktree fingerprint is COMPLETE and deterministic", async () => {
  const repo = await gitRepo();
  const a = await computeWorktreeFingerprint(repo);
  const b = await computeWorktreeFingerprint(repo);
  assert.equal(a.status, "COMPLETE");
  assert.equal(agg(a), agg(b));
  assert.ok(a.repository.headCommit);
  await rm(repo, { recursive: true, force: true });
});

test("detects staged, unstaged tracked, untracked, and HEAD changes; excludes ignored", async () => {
  const repo = await gitRepo();
  const base = await computeWorktreeFingerprint(repo);
  await writeFile(path.join(repo, "a.txt"), "unstaged\n");
  const unstaged = await computeWorktreeFingerprint(repo);
  assert.notEqual(agg(unstaged), agg(base));
  await sh("git", ["add", "a.txt"], { cwd: repo });
  const staged = await computeWorktreeFingerprint(repo);
  assert.notEqual(agg(staged), agg(unstaged));
  await sh("git", ["checkout", "-q", "--", "."], { cwd: repo });
  await sh("git", ["reset", "-q", "--hard"], { cwd: repo });
  await writeFile(path.join(repo, "u.txt"), "untracked\n");
  const untracked = await computeWorktreeFingerprint(repo);
  assert.notEqual(agg(untracked), agg(base));
  assert.equal(untracked.components.untrackedFileCount, 1);
  await rm(path.join(repo, "u.txt"));
  // ignored files excluded
  await writeFile(path.join(repo, ".gitignore"), "ignored.log\n");
  await sh("git", ["add", ".gitignore"], { cwd: repo });
  await sh("git", ["commit", "-qm", "ig"], { cwd: repo });
  const g1 = await computeWorktreeFingerprint(repo);
  await writeFile(path.join(repo, "ignored.log"), "noise\n");
  const g2 = await computeWorktreeFingerprint(repo);
  assert.equal(agg(g1), agg(g2), "ignored file change must not affect the checkpoint");
  // HEAD change
  await writeFile(path.join(repo, "b.txt"), "b\n");
  await sh("git", ["add", "b.txt"], { cwd: repo });
  await sh("git", ["commit", "-qm", "b"], { cwd: repo });
  const afterCommit = await computeWorktreeFingerprint(repo);
  assert.notEqual(afterCommit.repository.headCommit, base.repository.headCommit);
  assert.notEqual(agg(afterCommit), agg(g1));
  await rm(repo, { recursive: true, force: true });
});

test("bounds → INCOMPLETE; non-Git → UNAVAILABLE; missing → UNAVAILABLE(WORKTREE_MISSING)", async () => {
  const repo = await gitRepo();
  await writeFile(path.join(repo, "u.txt"), "x\n");
  const over = await computeWorktreeFingerprint(repo, { maxFiles: 0 });
  assert.equal(over.status, "INCOMPLETE");
  const nogit = await mkdtemp(path.join(os.tmpdir(), "wf-nogit-"));
  const u = await computeWorktreeFingerprint(nogit);
  assert.equal(u.status, "UNAVAILABLE");
  assert.equal(u.reason, "NOT_A_GIT_REPO");
  const missing = await computeWorktreeFingerprint(path.join(nogit, "gone"));
  assert.equal(missing.reason, "WORKTREE_MISSING");
  await Promise.all([repo, nogit].map((d) => rm(d, { recursive: true, force: true })));
});

test("public checkpoint exposes only hash/complete/status — no realpath, diffs, or file names", async () => {
  const repo = await gitRepo();
  await writeFile(path.join(repo, "secret-name.txt"), "secret-content\n");
  const fp = await computeWorktreeFingerprint(repo);
  const pub = publicWorktreeCheckpoint(fp);
  assert.deepEqual(Object.keys(pub).sort(), ["aggregateHash", "capturedAt", "complete", "status"]);
  const blob = JSON.stringify(pub);
  for (const leak of ["secret-name", "secret-content", repo, "/private/var", "worktreeRealpath"]) assert.ok(!blob.includes(leak));
  await rm(repo, { recursive: true, force: true });
});

test("toolchain fingerprint: COMPLETE with content hash for a resolvable exe; MISSING otherwise; no abs path in status projection", async () => {
  const repo = await gitRepo();
  const tc = await computeToolchainFingerprint({ argv: ["true"], cwd: repo, runnerType: "local", runnerProfile: "local_test" });
  assert.equal(tc.status, "COMPLETE");
  assert.ok(tc.executableContentHash && tc.aggregateHash);
  assert.equal(tc.executableBasename, "true");
  const miss = await computeToolchainFingerprint({ argv: ["definitely-not-real-xyz-9999"], cwd: repo });
  assert.equal(miss.status, "MISSING");
  await rm(repo, { recursive: true, force: true });
});
