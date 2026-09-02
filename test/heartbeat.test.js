import assert from "node:assert/strict";
import { mkdtemp, rm, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";

import { readdir } from "node:fs/promises";
import {
  DEFAULT_RUNNER_OBSERVABILITY,
  classifyLiveness,
  parseCpuTime,
  parseProgressMarker,
  readHeartbeat,
  readTailBytes,
  resolveObservabilityPolicy,
  scanLatestStep,
  writeHeartbeat,
} from "../dist/heartbeat.js";
import { deriveLiveness, reconcileOnce } from "../dist/core.js";
import { createJob, createJobId, readJob } from "../dist/job-store.js";

const iso = (ms) => new Date(ms).toISOString();
const NOW = Date.now();
const policy = { silenceBudgetMs: 60_000, stallConfirmMs: 60_000, stallConfirmSignal: null };
const cpuPolicy = { silenceBudgetMs: 60_000, stallConfirmMs: 60_000, stallConfirmSignal: "cpu" };
const fresh = (ms = 1000) => iso(NOW - ms);
const stale = (ms = 120_000) => iso(NOW - ms);
const hbAt = (observer, progress, extra = {}) => ({ observerHeartbeatAt: observer, lastProgressAt: progress, ...extra });

// ---- classifyLiveness (pure) — observer vs CHILD-progress ----

test("fresh child progress → HEALTHY", () => {
  assert.equal(classifyLiveness({ prev: {}, heartbeat: hbAt(fresh(), fresh()), policy, pidAlive: true, now: NOW }).livenessState, "HEALTHY");
});

test("FIX: observer heartbeat fresh but child progress STALE → SUSPECTED_STALL (not HEALTHY)", () => {
  const r = classifyLiveness({ prev: {}, heartbeat: hbAt(fresh(), stale()), policy, pidAlive: true, now: NOW });
  assert.equal(r.livenessState, "SUSPECTED_STALL");
});

test("observer STALE (worker not sampling) → keep prior observation, never invent a stall", () => {
  const r = classifyLiveness({ prev: { livenessState: "HEALTHY", livenessSince: fresh() }, heartbeat: hbAt(stale(), stale()), policy, pidAlive: true, now: NOW });
  assert.equal(r.livenessState, "HEALTHY"); // unchanged (LOST handles a truly dead worker via pid)
});

test("before the confirm window it stays SUSPECTED_STALL", () => {
  const r = classifyLiveness({ prev: { livenessState: "SUSPECTED_STALL", livenessSince: iso(NOW - 30_000) }, heartbeat: hbAt(fresh(), stale()), policy, pidAlive: true, now: NOW });
  assert.equal(r.livenessState, "SUSPECTED_STALL");
});

test("child progress returns after suspicion → HEALTHY recovery", () => {
  const r = classifyLiveness({ prev: { livenessState: "SUSPECTED_STALL", livenessSince: iso(NOW - 70_000) }, heartbeat: hbAt(fresh(), fresh()), policy, pidAlive: true, now: NOW });
  assert.equal(r.livenessState, "HEALTHY");
});

test("no confirm signal → NEVER promoted to STALLED (capped at SUSPECTED_STALL) even past the window", () => {
  const r = classifyLiveness({ prev: { livenessState: "SUSPECTED_STALL", livenessSince: iso(NOW - 70_000), suspectCpuMs: 100 }, heartbeat: hbAt(fresh(), stale(), { childCpuMs: 100 }), policy, pidAlive: true, now: NOW });
  assert.equal(r.livenessState, "SUSPECTED_STALL");
});

test("cpu confirm + CPU FLAT over the window → STALLED", () => {
  const r = classifyLiveness({ prev: { livenessState: "SUSPECTED_STALL", livenessSince: iso(NOW - 70_000), suspectCpuMs: 500 }, heartbeat: hbAt(fresh(), stale(), { childCpuMs: 500 }), policy: cpuPolicy, pidAlive: true, now: NOW });
  assert.equal(r.livenessState, "STALLED");
  assert.equal(r.livenessSince, iso(NOW - 70_000));
});

test("cpu confirm but CPU INCREASED (child computing quietly) → stays SUSPECTED_STALL, not STALLED", () => {
  const r = classifyLiveness({ prev: { livenessState: "SUSPECTED_STALL", livenessSince: iso(NOW - 70_000), suspectCpuMs: 500 }, heartbeat: hbAt(fresh(), stale(), { childCpuMs: 900 }), policy: cpuPolicy, pidAlive: true, now: NOW });
  assert.equal(r.livenessState, "SUSPECTED_STALL");
});

test("cpu confirm requested but CPU unavailable (null) → cannot confirm → SUSPECTED_STALL", () => {
  const r = classifyLiveness({ prev: { livenessState: "SUSPECTED_STALL", livenessSince: iso(NOW - 70_000), suspectCpuMs: null }, heartbeat: hbAt(fresh(), stale(), { childCpuMs: null }), policy: cpuPolicy, pidAlive: true, now: NOW });
  assert.equal(r.livenessState, "SUSPECTED_STALL");
});

test("no heartbeat file → classifyLiveness returns null (no derivation)", () => {
  assert.equal(classifyLiveness({ prev: {}, heartbeat: null, policy, pidAlive: true, now: NOW }), null);
});

// ---- resolveObservabilityPolicy ----

test("profile defaults differ per runnerProfile (docker is more patient)", () => {
  assert.equal(resolveObservabilityPolicy({}, "local_test").silenceBudgetMs, DEFAULT_RUNNER_OBSERVABILITY.local_test.silenceBudgetMs);
  assert.equal(resolveObservabilityPolicy({}, "local_docker").silenceBudgetMs, 300_000);
  assert.ok(resolveObservabilityPolicy({}, "local_docker").silenceBudgetMs > resolveObservabilityPolicy({}, "local_test").silenceBudgetMs);
  assert.equal(resolveObservabilityPolicy({}, "unknown_profile").silenceBudgetMs, DEFAULT_RUNNER_OBSERVABILITY.generic_local.silenceBudgetMs);
});

test("config overrides: global heartbeat + per-profile", () => {
  const p = resolveObservabilityPolicy({ heartbeatIntervalMs: 7000, runnerObservability: { local_test: { silenceBudgetMs: 90_000 } } }, "local_test");
  assert.equal(p.heartbeatIntervalMs, 7000);
  assert.equal(p.silenceBudgetMs, 90_000);
});

test("invalid observability config is rejected", () => {
  // heartbeatInterval >= silenceBudget
  assert.throws(() => resolveObservabilityPolicy({ runnerObservability: { local_test: { heartbeatIntervalMs: 60_000, silenceBudgetMs: 60_000 } } }, "local_test"), (e) => e.code === "OBSERVABILITY_CONFIG_INVALID");
});

// ---- progress marker ----

test("parseProgressMarker: valid / malformed / unsafe", () => {
  assert.equal(parseProgressMarker("##WF-STEP name=vitest"), "vitest");
  assert.equal(parseProgressMarker("  ##WF-STEP name=build.step-1  "), "build.step-1");
  assert.equal(parseProgressMarker("##WF-STEP name=has space"), null); // \S+ then trailing → no match
  assert.equal(parseProgressMarker("##WF-STEP name=" + "x".repeat(100)), null); // too long
  assert.equal(parseProgressMarker("##WF-STEP name=a;b|c"), null); // unsafe chars
  assert.equal(parseProgressMarker("just some log line"), null);
  assert.equal(parseProgressMarker("##WF-STEP"), null);
});

// ---- heartbeat.json atomic write / safe read ----

test("writeHeartbeat then readHeartbeat round-trips; corrupt/missing → null", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "hb-io-"));
  assert.equal(await readHeartbeat(dir), null); // missing
  await writeHeartbeat(dir, { lastHeartbeatAt: "t", stdoutBytes: 5 });
  assert.deepEqual(await readHeartbeat(dir), { lastHeartbeatAt: "t", stdoutBytes: 5 });
  await writeFile(path.join(dir, "heartbeat.json"), "{not json", "utf8"); // simulate a partial/corrupt file
  assert.equal(await readHeartbeat(dir), null);
  await rm(dir, { recursive: true, force: true });
});

test("writeHeartbeat cleans up its temp file on repeated rename failures (no .tmp accumulation)", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "hb-tmpclean-"));
  await mkdir(path.join(dir, "heartbeat.json")); // make the rename target a directory → rename always fails
  for (let i = 0; i < 5; i++) {
    await assert.rejects(() => writeHeartbeat(dir, { n: i })); // write succeeds, rename fails, re-thrown
  }
  const files = await readdir(dir);
  assert.ok(!files.some((f) => f.endsWith(".tmp")), `no leftover temp files: ${files.join(",")}`);
  await rm(dir, { recursive: true, force: true });
});

// ---- worker heartbeat E2E ----

const workerPath = new URL("../dist/worker.js", import.meta.url).pathname;
async function runWorker(program, over = {}) {
  const root = await mkdtemp(path.join(os.tmpdir(), "hb-worker-"));
  const id = createJobId();
  const now = new Date().toISOString();
  await createJob(root, {
    id, state: "QUEUED", processState: "QUEUED", resultProtocol: "none",
    observability: { heartbeatIntervalMs: 100, silenceBudgetMs: 60_000, stallConfirmMs: 60_000 },
    command: [process.execPath, "-e", program], cwd: root, timeoutSeconds: 0, createdAt: now, updatedAt: now,
    parent: { agentId: "a", sessionKey: null }, notification: { status: "pending" }, ...over,
  });
  const child = spawn(process.execPath, [workerPath, root, id], { stdio: "ignore" });
  await new Promise((r) => child.once("exit", r));
  let job; for (let i = 0; i < 100; i++) { job = await readJob(root, id); if (["SUCCEEDED", "FAILED", "TIMED_OUT"].includes(job.state)) break; await new Promise((r) => setTimeout(r, 30)); }
  const heartbeat = await readHeartbeat(path.join(root, id));
  await rm(root, { recursive: true, force: true });
  return { job, heartbeat };
}

test("worker writes heartbeat.json, tracks output + currentStep, and finalizes EXITED (timer cleaned on exit)", async () => {
  const { job, heartbeat } = await runWorker(
    "process.stdout.write('##WF-STEP name=phase1\\n');let n=0;const t=setInterval(()=>{process.stdout.write('tick '+(n++)+'\\n');if(n>=3){clearInterval(t)}},120)",
  );
  assert.equal(job.state, "SUCCEEDED");
  assert.equal(job.processState, "COMPLETED");
  assert.ok(heartbeat, "heartbeat.json exists");
  assert.equal(heartbeat.childProcessState, "EXITED"); // final write after exit → timer stopped
  assert.ok(heartbeat.stdoutBytes > 0);
  assert.ok(heartbeat.lastOutputAt, "lastOutputAt set from byte growth");
  assert.equal(heartbeat.currentStep, "phase1");
});

test("a heartbeat write failure never blocks the terminal result", async () => {
  // Make heartbeat.json a DIRECTORY so the worker's atomic rename fails on every write; the job must
  // still record its terminal result.
  const root = await mkdtemp(path.join(os.tmpdir(), "hb-fail-"));
  const id = createJobId();
  const now = new Date().toISOString();
  await createJob(root, {
    id, state: "QUEUED", processState: "QUEUED", resultProtocol: "none",
    observability: { heartbeatIntervalMs: 50, silenceBudgetMs: 60_000, stallConfirmMs: 60_000 },
    command: [process.execPath, "-e", "process.stdout.write('ok')"], cwd: root, timeoutSeconds: 0, createdAt: now, updatedAt: now,
    parent: { agentId: "a", sessionKey: null }, notification: { status: "pending" },
  });
  await mkdir(path.join(root, id, "heartbeat.json"), { recursive: true }); // block the write path
  const child = spawn(process.execPath, [workerPath, root, id], { stdio: "ignore" });
  await new Promise((r) => child.once("exit", r));
  let job; for (let i = 0; i < 100; i++) { job = await readJob(root, id); if (job.state === "SUCCEEDED") break; await new Promise((r) => setTimeout(r, 30)); }
  assert.equal(job.state, "SUCCEEDED");
  assert.equal(job.jobOutcome, "COMPLETED_UNVERIFIED");
  // repeated rename failures (heartbeat.json is a dir) must not leave orphaned temp files
  const files = await readdir(path.join(root, id));
  assert.ok(!files.some((f) => f.endsWith(".tmp")), `no leftover temp files: ${files.join(",")}`);
  await rm(root, { recursive: true, force: true });
});

// ---- deriveLiveness (writes only on state change; observation-only) ----

async function seedRunning(store, over = {}) {
  const id = createJobId();
  const now = new Date().toISOString();
  await createJob(store, {
    id, state: "RUNNING", processState: "RUNNING", childPid: process.pid, workerPid: process.pid,
    directory: path.join(store, id),
    observability: { silenceBudgetMs: 60_000, stallConfirmMs: 60_000, heartbeatIntervalMs: 5000, stallConfirmSignal: null },
    notification: { status: "pending" }, ...over,
  });
  return id;
}

test("deriveLiveness: observer fresh + child progress stale + pid alive → SUSPECTED_STALL (no kill, processState unchanged)", async () => {
  const store = await mkdtemp(path.join(os.tmpdir(), "hb-derive-"));
  const id = await seedRunning(store);
  await writeHeartbeat(path.join(store, id), { observerHeartbeatAt: fresh(), lastProgressAt: stale(), childPid: process.pid });
  await deriveLiveness(store, await readJob(store, id));
  const job = await readJob(store, id);
  assert.equal(job.livenessState, "SUSPECTED_STALL");
  assert.equal(job.state, "RUNNING"); // NOT killed / changed
  assert.equal(job.processState, "RUNNING");
  await rm(store, { recursive: true, force: true });
});

test("deriveLiveness: no confirm signal → stays SUSPECTED even past the window (never STALLED)", async () => {
  const store = await mkdtemp(path.join(os.tmpdir(), "hb-derive2-"));
  const id = await seedRunning(store, { livenessState: "SUSPECTED_STALL", livenessSince: iso(Date.now() - 70_000) });
  await writeHeartbeat(path.join(store, id), { observerHeartbeatAt: fresh(), lastProgressAt: stale(), childPid: process.pid });
  await deriveLiveness(store, await readJob(store, id));
  assert.equal((await readJob(store, id)).livenessState, "SUSPECTED_STALL");
  await rm(store, { recursive: true, force: true });
});

test("deriveLiveness: cpu confirm profile + CPU flat past window → STALLED, still no processState change", async () => {
  const store = await mkdtemp(path.join(os.tmpdir(), "hb-derive3-"));
  const id = await seedRunning(store, {
    observability: { silenceBudgetMs: 60_000, stallConfirmMs: 60_000, heartbeatIntervalMs: 5000, stallConfirmSignal: "cpu" },
    livenessState: "SUSPECTED_STALL", livenessSince: iso(Date.now() - 70_000), livenessSuspectCpuMs: 100,
  });
  await writeHeartbeat(path.join(store, id), { observerHeartbeatAt: fresh(), lastProgressAt: stale(), childPid: process.pid, childCpuMs: 100 });
  await deriveLiveness(store, await readJob(store, id));
  const job = await readJob(store, id);
  assert.equal(job.livenessState, "STALLED");
  assert.equal(job.state, "RUNNING");
  assert.equal(job.processState, "RUNNING");
  await rm(store, { recursive: true, force: true });
});

test("deriveLiveness: terminal job is not observed", async () => {
  const store = await mkdtemp(path.join(os.tmpdir(), "hb-term-"));
  const id = await seedRunning(store, { state: "SUCCEEDED", processState: "COMPLETED" });
  await writeHeartbeat(path.join(store, id), { observerHeartbeatAt: fresh(), lastProgressAt: stale(), childPid: process.pid });
  await deriveLiveness(store, await readJob(store, id));
  assert.equal((await readJob(store, id)).livenessState ?? null, null);
  await rm(store, { recursive: true, force: true });
});

test("deriveLiveness: legacy RUNNING job with no heartbeat.json is skipped (no livenessState)", async () => {
  const store = await mkdtemp(path.join(os.tmpdir(), "hb-legacy-"));
  const id = await seedRunning(store, { observability: undefined });
  await deriveLiveness(store, await readJob(store, id));
  assert.equal((await readJob(store, id)).livenessState ?? null, null);
  await rm(store, { recursive: true, force: true });
});

test("reconcileOnce derives SUSPECTED for a running job and does not abort other jobs", async () => {
  const store = await mkdtemp(path.join(os.tmpdir(), "hb-recon-"));
  const a = await seedRunning(store);
  await writeHeartbeat(path.join(store, a), { observerHeartbeatAt: fresh(), lastProgressAt: stale(), childPid: process.pid });
  const b = await seedRunning(store); // no heartbeat → skipped, must not throw
  await reconcileOnce({ rootDir: store, config: { queuedGraceMs: 30_000, sendLeaseMs: 30_000, deliveryMaxAttempts: 8 }, gatewayCall: async () => ({ result: {} }), settleFlow: async () => {}, logger: null });
  assert.equal((await readJob(store, a)).livenessState, "SUSPECTED_STALL");
  assert.equal((await readJob(store, b)).livenessState ?? null, null);
  await rm(store, { recursive: true, force: true });
});

// ---- REAL worker E2E: reproduce "live observer, stalled child" and recovery ----

test("REAL worker with a live observer but a silent (no-progress) child → SUSPECTED_STALL, not HEALTHY; then recovers on output", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "hb-realstall-"));
  const id = createJobId();
  const now = new Date().toISOString();
  const progressFile = path.join(root, "go.txt");
  await createJob(root, {
    id, state: "QUEUED", processState: "QUEUED", resultProtocol: "none",
    // tiny silence budget so a real silent child is "progress-stale" quickly; short heartbeat
    observability: { heartbeatIntervalMs: 60, silenceBudgetMs: 250, stallConfirmMs: 300, stallConfirmSignal: null },
    command: [process.execPath, "-e", `const fs=require('fs');const f=${JSON.stringify(progressFile)};(function w(){ if(fs.existsSync(f)){process.stdout.write('resumed\\n');setTimeout(()=>process.exit(0),3000);} else setTimeout(w,40); })()`],
    cwd: root, timeoutSeconds: 0, createdAt: now, updatedAt: now, childPid: null,
    parent: { agentId: "a", sessionKey: null }, notification: { status: "pending" },
  });
  const child = spawn(process.execPath, [workerPath, root, id], { stdio: "ignore" });
  try {
    // wait until the worker has spawned the child + is heartbeating with no child progress
    let hb;
    for (let i = 0; i < 100; i++) { hb = await readHeartbeat(path.join(root, id)); if (hb?.childPid && (await readJob(root, id)).childPid) break; await new Promise((r) => setTimeout(r, 30)); }
    // let the silence budget elapse with no output
    await new Promise((r) => setTimeout(r, 400));
    let job = await readJob(root, id);
    hb = await readHeartbeat(path.join(root, id));
    assert.ok(hb.observerHeartbeatAt, "observer heartbeat present (worker alive)");
    await deriveLiveness(root, job);
    job = await readJob(root, id);
    assert.equal(job.livenessState, "SUSPECTED_STALL", "live observer + no child progress ⇒ SUSPECTED, NOT HEALTHY");
    assert.equal(job.state, "RUNNING"); // observation only — not killed

    // now let the child make progress (emit output) → recovery to HEALTHY
    await writeFile(progressFile, "go", "utf8");
    for (let i = 0; i < 60; i++) { if ((await readHeartbeat(path.join(root, id)))?.stdoutBytes > 0) break; await new Promise((r) => setTimeout(r, 30)); }
    await deriveLiveness(root, await readJob(root, id));
    assert.equal((await readJob(root, id)).livenessState, "HEALTHY", "child progress resumed ⇒ HEALTHY recovery");
  } finally {
    try { process.kill(child.pid); } catch {}
    await rm(root, { recursive: true, force: true });
  }
});

test("serialized heartbeat writes: concurrent writes never overlap and the final file is EXITED", async () => {
  // A slow-writing child + short interval would otherwise overlap; the chain + EXITED-last ordering must
  // still leave a clean EXITED file with no leftover temp files.
  const { job, heartbeat } = await runWorker("let n=0;const t=setInterval(()=>{process.stdout.write('x'.repeat(2000)+'\\n');if(++n>=5)clearInterval(t)},40)");
  assert.equal(job.state, "SUCCEEDED");
  assert.equal(heartbeat.childProcessState, "EXITED");
});

// ---- bounded tail I/O (issue 3) ----

test("readTailBytes reads at most maxBytes from the END of a large file", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "hb-tail-"));
  const file = path.join(dir, "big.log");
  await writeFile(file, "A".repeat(50_000) + "\nZ".repeat(4000), "utf8"); // ~54KB
  const { text, truncated } = await readTailBytes(file, 8192);
  assert.ok(text.length <= 8192, `read ${text.length} <= 8192`);
  assert.equal(truncated, true);
  assert.ok(text.includes("Z"), "tail content present");
  assert.ok(!text.includes("A".repeat(50_000)), "head not read");
  await rm(dir, { recursive: true, force: true });
});

test("scanLatestStep: marker outside the tail is NOT read; last complete marker in tail wins; partial first line ignored; malformed ignored", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "hb-scan-"));
  const file = path.join(dir, "out.log");
  const head = "##WF-STEP name=OUTSIDE\n" + "x".repeat(9000) + "\n"; // marker beyond the 4KB tail
  const tail = "partial-first-line-no-newline-start ##WF-STEP name=EARLY\n##WF-STEP name=middle\n##WF-STEP name=bad name\n##WF-STEP name=final\ntrailing\n";
  await writeFile(file, head + tail, "utf8");
  const step = await scanLatestStep([file], 4096);
  assert.equal(step, "final"); // last valid complete marker within the tail
  assert.notEqual(step, "OUTSIDE"); // beyond tail → never read
  await rm(dir, { recursive: true, force: true });
});

test("scanLatestStep drops a truncated partial first line so it is not mis-parsed as a marker", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "hb-scan2-"));
  const file = path.join(dir, "out.log");
  // Make the tail boundary fall in the middle of a marker line; the partial first line must be ignored.
  const filler = "y".repeat(4090);
  await writeFile(file, filler + "##WF-STEP name=shouldbecut\n##WF-STEP name=kept\n", "utf8");
  const step = await scanLatestStep([file], 4096);
  assert.equal(step, "kept");
  await rm(dir, { recursive: true, force: true });
});

test("parseCpuTime parses mm:ss.ss and hh:mm:ss", () => {
  assert.equal(parseCpuTime("0:00.00"), 0);
  assert.equal(parseCpuTime("0:01.50"), 1500);
  assert.equal(parseCpuTime("1:02"), 62_000);
  assert.equal(parseCpuTime("01:00:00"), 3_600_000);
  assert.equal(parseCpuTime("garbage"), null);
});

test("no leftover heartbeat temp files after a worker run", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "hb-tmp-"));
  const id = createJobId();
  const now = new Date().toISOString();
  await createJob(root, {
    id, state: "QUEUED", processState: "QUEUED", resultProtocol: "none",
    observability: { heartbeatIntervalMs: 30, silenceBudgetMs: 60_000, stallConfirmMs: 60_000, stallConfirmSignal: null },
    command: [process.execPath, "-e", "let n=0;const t=setInterval(()=>{process.stdout.write('tick\\n');if(++n>=6)clearInterval(t)},30)"],
    cwd: root, timeoutSeconds: 0, createdAt: now, updatedAt: now,
    parent: { agentId: "a", sessionKey: null }, notification: { status: "pending" },
  });
  const child = spawn(process.execPath, [new URL("../dist/worker.js", import.meta.url).pathname, root, id], { stdio: "ignore" });
  await new Promise((r) => child.once("exit", r));
  for (let i = 0; i < 100; i++) { if (["SUCCEEDED", "FAILED"].includes((await readJob(root, id)).state)) break; await new Promise((r) => setTimeout(r, 30)); }
  const files = await readdir(path.join(root, id));
  assert.ok(!files.some((f) => f.includes(".tmp")), `no temp files: ${files.join(",")}`);
  assert.ok(files.includes("heartbeat.json"));
  await rm(root, { recursive: true, force: true });
});
