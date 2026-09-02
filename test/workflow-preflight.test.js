// P3-F preflight lifecycle + checkpoint capture + checkpoint-verified provider fallback + safe resume, using
// INJECTED deterministic fingerprint / toolchain / provider-probe seams (no real Git). Never auto-PASSES.
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { readConfig } from "../dist/core.js";
import { listJobs, readJob } from "../dist/job-store.js";
import { createWorkflow, readStageAttempt, readWorkflow, transitionStageAttempt } from "../dist/workflow-store.js";
import { advanceWorkflowOnce } from "../dist/workflow-reconciler.js";
import { controlResume } from "../dist/workflow-control.js";
import { makeFakeStartJob, setJobTerminal } from "./wf-linkage-helpers.js";

const ROUTE = { routeKind: "channel_root", channel: "slack", to: "C1" };
const tmp = (p) => mkdtemp(path.join(os.tmpdir(), p));

// controllable fingerprint / toolchain / provider seams
function fakeFp(initialHash = "H1") {
  let hash = initialHash, status = "COMPLETE";
  return {
    set(h, st = "COMPLETE") { hash = h; status = st; },
    capture: async () => ({ fingerprintVersion: 1, status, aggregateHash: status === "COMPLETE" ? hash : null, reason: status === "UNAVAILABLE" ? "NOT_A_GIT_REPO" : status === "MISSINGWT" ? "WORKTREE_MISSING" : undefined, capturedAt: new Date().toISOString() }),
  };
}
const fakeToolchain = (status = "COMPLETE") => async ({ runnerType, runnerProfile }) => ({ fingerprintVersion: 1, status, aggregateHash: "tc-" + runnerProfile, executableBasename: "x", executableRealpath: "/frozen/bin/x", runnerType, runnerProfile });
// a fingerprint / toolchain that returns a SEQUENCE of hashes across calls (to simulate a TOCTOU change
// between the preflight capture and the spawn-time re-verify)
const seqFp = (hashes) => { let i = 0; return async () => { const h = hashes[Math.min(i, hashes.length - 1)]; i += 1; return { fingerprintVersion: 1, status: h == null ? "INCOMPLETE" : "COMPLETE", aggregateHash: h, capturedAt: new Date().toISOString() }; }; };
const seqTc = (hashes) => { let i = 0; return async ({ runnerType, runnerProfile }) => { const h = hashes[Math.min(i, hashes.length - 1)]; i += 1; return { fingerprintVersion: 1, status: "COMPLETE", aggregateHash: h, executableBasename: "x", executableRealpath: "/frozen/bin/x", runnerType, runnerProfile }; }; };

async function makeWorkflow(root, ws, { pipeline }) {
  const id = await createWorkflow(root, {
    name: "wf", ownerKey: "ok", requestId: null, payloadFingerprint: "fp",
    parent: { agentId: "a", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null },
    repository: { worktree: ws, branch: null, baseCommit: null, verificationProfile: null },
    deliveryRoute: ROUTE, forbiddenActions: [], pipeline,
  });
  return readWorkflow(root, id);
}
function depsFor(root, config, seams = {}) {
  const fake = makeFakeStartJob();
  return { fake, deps: { rootDir: root, config, startJob: fake.startJob, startDeps: { rootDir: root }, captureFingerprint: seams.fp ?? (async () => ({ status: "UNAVAILABLE", reason: "NOT_A_GIT_REPO" })), captureToolchain: seams.tc ?? fakeToolchain("COMPLETE"), providerProbe: seams.probe ?? (async () => ({ status: "READY" })), providerConfigFingerprint: "cfg" } };
}
const localStage = (i, n = `s${i}`, over = {}) => ({ pipelineIndex: i * 10, stageName: n, runnerType: "local", runnerProfile: "local_test", activity: { argv: ["true"] }, candidateId: `c${i}`, fallbacks: [], ...over });
const st = (root, id, sid, a = 1) => readStageAttempt(root, id, sid, a).then((x) => x?.stageState);
const cleanup = (root, ws) => Promise.all([rm(root, { recursive: true, force: true }), rm(ws, { recursive: true, force: true })]);

async function setup(pipeline, seams) {
  const root = await tmp("wf-pf-root-");
  const ws = await tmp("wf-pf-ws-");
  const config = readConfig({ pluginConfig: { workflowEnabled: true, allowedRoots: [ws] } });
  const { fake, deps } = depsFor(root, config, seams);
  const wf = await makeWorkflow(root, ws, { pipeline });
  return { root, ws, config, fake, deps, id: wf.workflowId };
}

// ---- preflight lifecycle ----
test("local candidate: preflight PASS → job; checkpoint.before captured", async () => {
  const fp = fakeFp("H1");
  const { root, ws, deps, id } = await setup([localStage(0)], { fp: fp.capture });
  await advanceWorkflowOnce(deps, id);
  assert.equal(await st(root, id, "000-s0"), "RUNNING");
  assert.equal((await listJobs(root)).length, 1);
  const a = await readStageAttempt(root, id, "000-s0", 1);
  assert.equal(a.preflight.status, "PASSED");
  assert.equal(a.checkpoint.before.aggregateHash, "H1");
  await cleanup(root, ws);
});

test("local needs no probe (PASS); model READY → PASS; model BLOCKED → job 0; model UNKNOWN → fail-closed job 0, no fallback", async () => {
  const model = (over = {}) => ({ pipelineIndex: 0, stageName: "m", runnerType: "model", runnerProfile: "model_agy", activity: { argv: ["agy", "x"] }, candidateId: "cm", fallbacks: [], ...over });
  // local: probe never called
  let probes = 0;
  let s = await setup([localStage(0)], { fp: fakeFp("H1").capture, probe: async () => { probes += 1; return { status: "READY" }; } });
  await advanceWorkflowOnce(s.deps, s.id);
  assert.equal(await st(s.root, s.id, "000-s0"), "RUNNING");
  assert.equal((await readStageAttempt(s.root, s.id, "000-s0", 1)).preflight.providerCapability, "NOT_REQUIRED");
  assert.equal(probes, 0, "local runner does not probe the provider");
  await cleanup(s.root, s.ws);
  // model READY
  s = await setup([model()], { fp: fakeFp("H1").capture, probe: async () => ({ status: "READY" }) });
  await advanceWorkflowOnce(s.deps, s.id);
  assert.equal(await st(s.root, s.id, "000-m"), "RUNNING");
  assert.equal((await listJobs(s.root)).length, 1);
  await cleanup(s.root, s.ws);
  // model BLOCKED
  s = await setup([model()], { fp: fakeFp("H1").capture, probe: async () => ({ status: "BLOCKED", providerState: "BLOCKED_QUOTA", failureCode: "QUOTA" }) });
  await advanceWorkflowOnce(s.deps, s.id);
  assert.equal(await st(s.root, s.id, "000-m"), "BLOCKED_DEPENDENCY");
  assert.equal((await listJobs(s.root)).length, 0);
  await cleanup(s.root, s.ws);
  // model UNKNOWN → fail-closed (no job, no fallback), even with a declared fallback
  s = await setup([model({ fallbacks: [{ candidateId: "cm2", runnerType: "model", runnerProfile: "model_agy", activity: { argv: ["other", "x"] } }] })], { fp: fakeFp("H1").capture, probe: async () => ({ status: "UNKNOWN" }) });
  await advanceWorkflowOnce(s.deps, s.id);
  assert.equal(await st(s.root, s.id, "000-m"), "BLOCKED_DEPENDENCY", "UNKNOWN fails closed to BLOCKED_DEPENDENCY");
  assert.equal((await listJobs(s.root)).length, 0, "no model job on UNKNOWN");
  assert.equal(await readStageAttempt(s.root, s.id, "000-m", 2).catch(() => null), null, "no fallback attempt on UNKNOWN");
  await cleanup(s.root, s.ws);
  // UNKNOWN cache hit also yields job 0
  const uroot = s;
  s = await setup([model()], { fp: fakeFp("H1").capture, probe: async () => ({ status: "UNKNOWN" }) });
  await advanceWorkflowOnce(s.deps, s.id);
  await advanceWorkflowOnce(s.deps, s.id); // second tick (cache-ish)
  assert.equal((await listJobs(s.root)).length, 0, "UNKNOWN never yields a job across ticks");
  await cleanup(s.root, s.ws);
});

test("preflight-blocked automatic fallback: BLOCKED primary → attempt2 PROVIDER_FALLBACK (job 0), next tick submits candidate1", async () => {
  // primary model provider BLOCKED (fallback-eligible); fallback is a LOCAL candidate (no provider probe → PASS)
  const stage = { pipelineIndex: 0, stageName: "m", runnerType: "model", runnerProfile: "model_agy", activity: { argv: ["agy", "x"] }, candidateId: "c0", fallbacks: [{ candidateId: "c1", runnerType: "local", runnerProfile: "local_build", activity: { argv: ["true"] } }] };
  const { root, ws, deps, id } = await setup([stage], { fp: fakeFp("H1").capture, probe: async () => ({ status: "BLOCKED", providerState: "RATE_LIMITED", failureCode: "RL" }) });
  await advanceWorkflowOnce(deps, id); // preflight BLOCKED → BLOCKED_DEPENDENCY + fallback attempt2
  assert.equal(await st(root, id, "000-m", 1), "BLOCKED_DEPENDENCY");
  const a2 = await readStageAttempt(root, id, "000-m", 2);
  assert.ok(a2, "fallback attempt2 created");
  assert.equal(a2.executionCandidate.candidateIndex, 1);
  assert.equal(a2.executionCandidate.runnerType, "local");
  assert.equal(a2.fallback.resumeMode, "PROVIDER_FALLBACK");
  assert.equal((await listJobs(root)).length, 0, "no job for a preflight-blocked primary");
  await advanceWorkflowOnce(deps, id); // preflight+submit candidate1 (local → PASS)
  assert.equal(await st(root, id, "000-m", 2), "RUNNING");
  assert.equal((await listJobs(root)).length, 1, "exactly one job for the fallback candidate");
  await cleanup(root, ws);
});

test("preflight-blocked fallback exhaustion + per-tick bound: last candidate BLOCKED → no further attempt", async () => {
  const stage = { pipelineIndex: 0, stageName: "m", runnerType: "model", runnerProfile: "model_agy", activity: { argv: ["agy", "x"] }, candidateId: "c0", fallbacks: [{ candidateId: "c1", runnerType: "model", runnerProfile: "model_agy", activity: { argv: ["other", "x"] } }] };
  const { root, ws, deps, id } = await setup([stage], { fp: fakeFp("H1").capture, probe: async () => ({ status: "BLOCKED", providerState: "RATE_LIMITED", failureCode: "RL" }) });
  await advanceWorkflowOnce(deps, id); // primary BLOCKED → attempt2
  assert.ok(await readStageAttempt(root, id, "000-m", 2));
  await advanceWorkflowOnce(deps, id); // candidate1 BLOCKED → no attempt3 (exhausted)
  assert.equal(await st(root, id, "000-m", 2), "BLOCKED_DEPENDENCY");
  assert.equal(await readStageAttempt(root, id, "000-m", 3).catch(() => null), null, "no attempt beyond the last candidate");
  await advanceWorkflowOnce(deps, id); // idempotent
  assert.equal(await readStageAttempt(root, id, "000-m", 3).catch(() => null), null);
  await cleanup(root, ws);
});

test("worktree INCOMPLETE → ARTIFACT_MISSING; toolchain MISSING → FAILED; both without a job", async () => {
  let s = await setup([localStage(0)], { fp: (async () => ({ status: "INCOMPLETE", aggregateHash: null })) });
  await advanceWorkflowOnce(s.deps, s.id);
  assert.equal(await st(s.root, s.id, "000-s0"), "ARTIFACT_MISSING");
  assert.equal((await listJobs(s.root)).length, 0);
  await cleanup(s.root, s.ws);
  s = await setup([localStage(0)], { fp: fakeFp("H1").capture, tc: fakeToolchain("MISSING") });
  await advanceWorkflowOnce(s.deps, s.id);
  assert.equal(await st(s.root, s.id, "000-s0"), "FAILED");
  assert.equal((await listJobs(s.root)).length, 0);
  await cleanup(s.root, s.ws);
});

test("checkpoint.after captured at terminal; concurrent advance preflights once (one job)", async () => {
  const fp = fakeFp("H1");
  const { root, ws, deps, id } = await setup([localStage(0)], { fp: fp.capture });
  await Promise.all([advanceWorkflowOnce(deps, id), advanceWorkflowOnce(deps, id)]);
  assert.equal((await listJobs(root)).length, 1, "concurrent preflight+submit → one job");
  const jobId = (await readStageAttempt(root, id, "000-s0", 1)).jobId;
  await setJobTerminal(root, jobId, { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" });
  await advanceWorkflowOnce(deps, id);
  const a = await readStageAttempt(root, id, "000-s0", 1);
  assert.equal(a.stageState, "UNVERIFIED");
  assert.equal(a.checkpoint.after.aggregateHash, "H1");
  await cleanup(root, ws);
});

// ---- spawn-time TOCTOU re-verification ----
test("worktree changes between preflight and spawn → WORKFLOW_CHECKPOINT_CHANGED, no job", async () => {
  const { root, ws, deps, id } = await setup([localStage(0)], { fp: seqFp(["H1", "H2"]) }); // preflight H1, guard H2
  await assert.rejects(() => advanceWorkflowOnce(deps, id), (e) => e.code === "WORKFLOW_CHECKPOINT_CHANGED");
  assert.equal((await listJobs(root)).length, 0, "no job when the worktree drifted");
  await cleanup(root, ws);
});

test("executable changes between preflight and spawn → WORKFLOW_TOOLCHAIN_CHANGED, no job", async () => {
  const { root, ws, deps, id } = await setup([localStage(0)], { fp: fakeFp("H1").capture, tc: seqTc(["TC1", "TC2"]) });
  await assert.rejects(() => advanceWorkflowOnce(deps, id), (e) => e.code === "WORKFLOW_TOOLCHAIN_CHANGED");
  assert.equal((await listJobs(root)).length, 0, "no job when the executable drifted");
  await cleanup(root, ws);
});

test("unchanged → exactly one job that spawns the FROZEN absolute executable path", async () => {
  const { root, ws, fake, deps, id } = await setup([localStage(0)], { fp: fakeFp("H1").capture, tc: fakeToolchain("COMPLETE") });
  await advanceWorkflowOnce(deps, id);
  assert.equal((await listJobs(root)).length, 1);
  assert.equal(fake.calls[0].params.command[0], "/frozen/bin/x", "spawn uses the frozen absolute executable path");
  await cleanup(root, ws);
});

test("crash C: a PASS record without checkpoint.before is NOT trusted — a fresh preflight recaptures before submitting", async () => {
  const fp = fakeFp("H1");
  const { root, ws, deps, id } = await setup([localStage(0)], { fp: fp.capture });
  await transitionStageAttempt(root, id, { stageId: "000-s0", attempt: 1, toState: "SUBMITTING" });
  // simulate a corrupt/legacy state: preflight PASSED but no checkpoint.before (via the store's preflight recorder)
  const { recordCheckpoint } = await import("../dist/workflow-store.js");
  await recordCheckpoint(root, id, { stageId: "000-s0", attempt: 1, phase: "after", checkpoint: { aggregateHash: "x" } }); // unrelated
  const { commitPreflightResult } = await import("../dist/workflow-store.js");
  await commitPreflightResult(root, id, { stageId: "000-s0", attempt: 1, preflight: { status: "PASSED", providerCapability: "NOT_REQUIRED" } }); // PASSED but NO before/frozen
  await advanceWorkflowOnce(deps, id); // must re-run preflight (recapture before) before submitting
  const a = await readStageAttempt(root, id, "000-s0", 1);
  assert.ok(a.checkpoint.before, "checkpoint.before recaptured before the job was submitted");
  assert.equal(a.stageState, "RUNNING");
  assert.equal((await listJobs(root)).length, 1);
  await cleanup(root, ws);
});

// ---- checkpoint-verified fallback ----
const modelWithFallback = { pipelineIndex: 0, stageName: "m", runnerType: "model", runnerProfile: "model_agy", activity: { argv: ["agy", "x"] }, candidateId: "cprimary", fallbacks: [{ candidateId: "cfb", runnerType: "model", runnerProfile: "model_agy", activity: { argv: ["other", "x"] } }] };

test("terminal FAILED_PROVIDER + UNCHANGED worktree → attempt2 candidate1 (PROVIDER_FALLBACK, checkpointVerified)", async () => {
  const fp = fakeFp("H1");
  const { root, ws, deps, id } = await setup([modelWithFallback], { fp: fp.capture });
  await advanceWorkflowOnce(deps, id); // attempt1 primary RUNNING
  const jobId = (await readStageAttempt(root, id, "000-m", 1)).jobId;
  await setJobTerminal(root, jobId, { processState: "COMPLETED", providerState: "AUTH_FAILED", jobOutcome: "FAILED_PROVIDER", state: "SUCCEEDED" });
  await advanceWorkflowOnce(deps, id); // unchanged (H1) → fallback attempt2
  const a2 = await readStageAttempt(root, id, "000-m", 2);
  assert.ok(a2, "attempt2 created");
  assert.equal(a2.stageState, "PENDING");
  assert.equal(a2.executionCandidate.candidateIndex, 1);
  assert.equal(a2.fallback.resumeMode, "PROVIDER_FALLBACK");
  assert.equal(a2.fallback.checkpointVerified, true);
  assert.equal((await readStageAttempt(root, id, "000-m", 1)).stageState, "FAILED", "failed primary preserved (AUTH_FAILED → FAILED)");
  await advanceWorkflowOnce(deps, id); // preflight+submit candidate1
  assert.equal(await st(root, id, "000-m", 2), "RUNNING");
  assert.equal((await listJobs(root)).length, 2);
  await cleanup(root, ws);
});

test("terminal FAILED_PROVIDER + CHANGED worktree → APPROVAL_REQUIRED, no fallback job", async () => {
  const fp = fakeFp("H1");
  const { root, ws, deps, id } = await setup([modelWithFallback], { fp: fp.capture });
  await advanceWorkflowOnce(deps, id);
  const jobId = (await readStageAttempt(root, id, "000-m", 1)).jobId;
  await setJobTerminal(root, jobId, { processState: "COMPLETED", providerState: "RATE_LIMITED", jobOutcome: "FAILED_PROVIDER", state: "SUCCEEDED" });
  fp.set("H2"); // worktree changed after the primary ran
  await advanceWorkflowOnce(deps, id);
  assert.equal(await st(root, id, "000-m", 1), "APPROVAL_REQUIRED");
  assert.equal(await readStageAttempt(root, id, "000-m", 2).catch(() => null), null, "no fallback attempt on mismatch");
  assert.equal((await listJobs(root)).length, 1);
  await cleanup(root, ws);
});

test("non-fallback-eligible terminal (FAILED_COMMAND / COMPLETED_UNVERIFIED) never auto-fallbacks", async () => {
  for (const term of [
    { processState: "TIMED_OUT", jobOutcome: "FAILED_COMMAND", state: "TIMED_OUT", expect: "FAILED" },
    { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED", expect: "UNVERIFIED" },
  ]) {
    const { root, ws, deps, id } = await setup([modelWithFallback], { fp: fakeFp("H1").capture });
    await advanceWorkflowOnce(deps, id);
    const jobId = (await readStageAttempt(root, id, "000-m", 1)).jobId;
    await setJobTerminal(root, jobId, term);
    await advanceWorkflowOnce(deps, id);
    assert.equal(await st(root, id, "000-m", 1), term.expect);
    assert.equal(await readStageAttempt(root, id, "000-m", 2).catch(() => null), null, `no fallback for ${term.jobOutcome}`);
    await cleanup(root, ws);
  }
});

// ---- safe resume ----
test("require_match resume with UNCHANGED checkpoint → attempt2 CHECKPOINT_RERUN; CHANGED → MISMATCH", async () => {
  const fp = fakeFp("H1");
  const { root, ws, config, fake, id } = await setup([localStage(0)], { fp: fp.capture });
  const deps = { rootDir: root, config, startJob: fake.startJob, startDeps: { rootDir: root }, captureFingerprint: fp.capture, captureToolchain: fakeToolchain("COMPLETE"), providerProbe: async () => ({ status: "READY" }), providerConfigFingerprint: "cfg" };
  await advanceWorkflowOnce(deps, id); // attempt1 RUNNING
  const jobId = (await readStageAttempt(root, id, "000-s0", 1)).jobId;
  await setJobTerminal(root, jobId, { processState: "FAILED_COMMAND", jobOutcome: "FAILED_COMMAND", state: "FAILED" });
  await advanceWorkflowOnce(deps, id); // → FAILED + checkpoint.after (H1)
  assert.equal(await st(root, id, "000-s0", 1), "FAILED");
  const params = { workflowId: id, stageId: "000-s0", attempt: 1, requestId: "sr", reason: "retry", ownerKeyHash: "h", payloadFingerprint: "pf", actor: { agentId: "a" }, checkpointPolicy: "require_match" };
  await controlResume({ ...deps, cancelJob: async () => {}, readJob }, params); // unchanged → CHECKPOINT_RERUN
  const a2 = await readStageAttempt(root, id, "000-s0", 2);
  assert.equal(a2.resume.resumeMode, "CHECKPOINT_RERUN");
  assert.equal(a2.resume.checkpointVerified, true);
  assert.equal(a2.checkpoint.expectedBeforeHash, "H1");
  // now a CHANGED worktree → mismatch on a fresh require_match
  await setJobTerminal(root, (await readStageAttempt(root, id, "000-s0", 2)).jobId, { processState: "FAILED_COMMAND", jobOutcome: "FAILED_COMMAND", state: "FAILED" });
  await advanceWorkflowOnce(deps, id);
  fp.set("H2");
  await assert.rejects(() => controlResume({ ...deps, cancelJob: async () => {}, readJob }, { ...params, attempt: 2, requestId: "sr2" }), (e) => e.code === "WORKFLOW_CHECKPOINT_MISMATCH");
  assert.equal(await readStageAttempt(root, id, "000-s0", 3).catch(() => null), null, "no attempt on mismatch");
  await cleanup(root, ws);
});

test("require_match resume when source checkpoint.after is not COMPLETE → WORKFLOW_CHECKPOINT_UNAVAILABLE", async () => {
  // Production policy requires a COMPLETE Git fingerprint to SUBMIT, so the source attempt is submitted on a
  // COMPLETE worktree (H1). But by the time the job terminalizes the worktree has become non-git, so the
  // recorded checkpoint.after is UNAVAILABLE — a require_match resume then cannot verify equality → fail-closed.
  const fp = fakeFp("H1");
  const { root, ws, config, fake, id } = await setup([localStage(0)], { fp: fp.capture });
  const deps = { rootDir: root, config, startJob: fake.startJob, startDeps: { rootDir: root }, captureFingerprint: fp.capture, captureToolchain: fakeToolchain("COMPLETE"), providerProbe: async () => ({ status: "READY" }), providerConfigFingerprint: "cfg" };
  await advanceWorkflowOnce(deps, id); // submit on COMPLETE worktree → job created
  const jobId = (await readStageAttempt(root, id, "000-s0", 1)).jobId;
  await setJobTerminal(root, jobId, { processState: "FAILED_COMMAND", jobOutcome: "FAILED_COMMAND", state: "FAILED" });
  fp.set(null, "UNAVAILABLE"); // worktree became non-git before terminal settlement records checkpoint.after
  await advanceWorkflowOnce(deps, id); // → FAILED + checkpoint.after (UNAVAILABLE)
  await assert.rejects(() => controlResume({ ...deps, cancelJob: async () => {}, readJob }, { workflowId: id, stageId: "000-s0", attempt: 1, requestId: "u", reason: "r", ownerKeyHash: "h", payloadFingerprint: "pf", actor: { agentId: "a" }, checkpointPolicy: "require_match" }), (e) => e.code === "WORKFLOW_CHECKPOINT_UNAVAILABLE");
  await cleanup(root, ws);
});

// ---- #5 preflight step timeouts (fail-closed, no job, no quota) ----
const slow = (ms) => new Promise((r) => setTimeout(r, ms));

test("toolchain capture timeout → FAILED, no job (fail-closed)", async () => {
  const { root, ws, config, fake, id } = await setup([localStage(0)], { fp: fakeFp("H1").capture });
  const deps = { rootDir: root, config: { ...config, workflowPreflightTimeoutMs: 20 }, startJob: fake.startJob, startDeps: { rootDir: root }, captureFingerprint: fakeFp("H1").capture, captureToolchain: async () => { await slow(300); return { status: "COMPLETE", aggregateHash: "tc", executableBasename: "x" }; }, providerProbe: async () => ({ status: "READY" }), providerConfigFingerprint: "cfg" };
  await advanceWorkflowOnce(deps, id);
  assert.equal(await st(root, id, "000-s0"), "FAILED");
  assert.equal((await listJobs(root)).length, 0, "no job on toolchain timeout");
  assert.equal((await readStageAttempt(root, id, "000-s0", 1)).preflight.failureCode, "WORKFLOW_TOOLCHAIN_TIMEOUT");
  await cleanup(root, ws);
});

test("provider probe timeout → UNKNOWN → BLOCKED_DEPENDENCY, no job, no fallback, at most one probe", async () => {
  const stage = { pipelineIndex: 0, stageName: "m", runnerType: "model", runnerProfile: "model_agy", activity: { argv: ["agy", "x"] }, candidateId: "c0", fallbacks: [{ candidateId: "c1", runnerType: "local", runnerProfile: "local_build", activity: { argv: ["true"] } }] };
  const { root, ws, config, fake, id } = await setup([stage], { fp: fakeFp("H1").capture });
  let probes = 0;
  const deps = { rootDir: root, config: { ...config, workflowPreflightTimeoutMs: 20 }, startJob: fake.startJob, startDeps: { rootDir: root }, captureFingerprint: fakeFp("H1").capture, captureToolchain: fakeToolchain("COMPLETE"), providerProbe: async () => { probes += 1; await slow(300); return { status: "READY" }; }, providerConfigFingerprint: "cfg" };
  await advanceWorkflowOnce(deps, id);
  assert.equal(await st(root, id, "000-m"), "BLOCKED_DEPENDENCY");
  assert.equal((await listJobs(root)).length, 0, "no job on probe timeout");
  assert.equal(await readStageAttempt(root, id, "000-m", 2).catch(() => null), null, "no fallback on UNKNOWN(timeout)");
  assert.equal(probes, 1, "at most one probe per reconcile pass");
  await cleanup(root, ws);
});
