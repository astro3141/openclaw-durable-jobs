// P3-C workflow reconciler tests: SUBMITTING crash recovery, job↔stage attach, terminal → stage verdict,
// no next-stage auto-advance, feature-flag behavior, and standalone-vs-linked continuation separation.
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { readConfig, reconcileOnce } from "../dist/core.js";
import { listJobs, readJob } from "../dist/job-store.js";
import {
  createWorkflow,
  readStageAttempt,
  readStageProjection,
  readWorkflow,
  transitionStageAttempt,
} from "../dist/workflow-store.js";
import { ensureLinkedJob, makeActivityIdempotencyKey } from "../dist/workflow-activity.js";
import { reconcileWorkflowsOnce, advanceWorkflowOnce } from "../dist/workflow-reconciler.js";
import { makeFakeStartJob, setJobTerminal, completeSeams } from "./wf-linkage-helpers.js";
import { rm as _rm } from "node:fs/promises";

async function tmpDir(p = "wf-rec-") {
  return mkdtemp(path.join(os.tmpdir(), p));
}

const ROUTE = { routeKind: "channel_root", channel: "slack", to: "C1" };
const PIPELINE = [
  { pipelineIndex: 0, stageName: "impl", runnerType: "local", runnerProfile: "generic_local", activity: { argv: ["true"] } },
  { pipelineIndex: 10, stageName: "test", runnerType: "local", runnerProfile: "local_test", activity: { argv: ["true"] } },
];

async function makeWorkflow(root, worktree) {
  const id = await createWorkflow(root, {
    name: "wf", ownerKey: "ok", requestId: null, payloadFingerprint: "fp",
    parent: { agentId: "agent-1", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null },
    repository: { worktree, branch: null, baseCommit: null, verificationProfile: null },
    deliveryRoute: ROUTE, forbiddenActions: [], pipeline: PIPELINE,
  });
  return readWorkflow(root, id);
}

async function setup({ enabled = true } = {}) {
  const root = await tmpDir("wf-rec-root-");
  const ws = await tmpDir("wf-rec-ws-");
  const config = readConfig({ pluginConfig: { workflowEnabled: enabled, allowedRoots: [ws] } });
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, config, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams() };
  const workflow = await makeWorkflow(root, ws);
  return { root, ws, config, fake, deps, workflow };
}

const firstState = (root, wfId) => readStageAttempt(root, wfId, "000-impl", 1).then((a) => a?.stageState);
const cleanup = (root, ws) => Promise.all([rm(root, { recursive: true, force: true }), rm(ws, { recursive: true, force: true })]);

// ---- crash recovery ----
test("Case A — SUBMITTING with no job → reconcile submits exactly one job and moves to RUNNING (idempotent)", async () => {
  const { root, ws, deps, workflow } = await setup();
  await transitionStageAttempt(root, workflow.workflowId, { stageId: "000-impl", attempt: 1, toState: "SUBMITTING" });
  await reconcileWorkflowsOnce(deps);
  assert.equal(await firstState(root, workflow.workflowId), "RUNNING");
  const jobs = await listJobs(root);
  assert.equal(jobs.length, 1);
  assert.equal(jobs[0].workflowLink.activityIdempotencyKey, makeActivityIdempotencyKey(workflow.workflowId, "000-impl", 1));
  const attempt = await readStageAttempt(root, workflow.workflowId, "000-impl", 1);
  assert.equal(attempt.jobId, jobs[0].id, "stage↔job linkage consistent");
  await reconcileWorkflowsOnce(deps); // idempotent
  assert.equal((await listJobs(root)).length, 1);
  assert.equal(await firstState(root, workflow.workflowId), "RUNNING");
  await cleanup(root, ws);
});

test("Case B — SUBMITTING with an already-created job (jobId not yet linked) → attach, no second job", async () => {
  const { root, ws, deps, workflow } = await setup();
  await transitionStageAttempt(root, workflow.workflowId, { stageId: "000-impl", attempt: 1, toState: "SUBMITTING" });
  // job created but the crash happened before stage.jobId was recorded
  const key = makeActivityIdempotencyKey(workflow.workflowId, "000-impl", 1);
  const spec = workflow.pipeline.find((s) => s.stageId === "000-impl");
  await ensureLinkedJob({ rootDir: root, startJob: deps.startJob, startDeps: { rootDir: root } }, { workflow, stageSpec: spec, stageId: "000-impl", attempt: 1, activityIdempotencyKey: key });
  assert.equal((await listJobs(root)).length, 1);
  await reconcileWorkflowsOnce(deps);
  const attempt = await readStageAttempt(root, workflow.workflowId, "000-impl", 1);
  assert.equal(attempt.stageState, "RUNNING");
  assert.ok(attempt.jobId);
  assert.equal((await listJobs(root)).length, 1, "no duplicate job created");
  await cleanup(root, ws);
});

// ---- terminal → stage verdict ----
for (const [label, terminal, expected] of [
  ["verified-unverified", { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" }, "UNVERIFIED"],
  ["timeout", { processState: "TIMED_OUT", jobOutcome: "FAILED_COMMAND", state: "TIMED_OUT" }, "FAILED"],
  ["retryable-provider", { processState: "COMPLETED", providerState: "RATE_LIMITED", jobOutcome: "FAILED_PROVIDER", state: "SUCCEEDED" }, "BLOCKED_DEPENDENCY"],
  ["auth-failure", { processState: "COMPLETED", providerState: "AUTH_FAILED", jobOutcome: "FAILED_PROVIDER", state: "SUCCEEDED" }, "FAILED"],
]) {
  test(`terminal reconciliation: ${label} → stage ${expected} (idempotent)`, async () => {
    const { root, ws, deps, workflow } = await setup();
    await transitionStageAttempt(root, workflow.workflowId, { stageId: "000-impl", attempt: 1, toState: "SUBMITTING" });
    await reconcileWorkflowsOnce(deps); // → RUNNING with a QUEUED linked job
    assert.equal(await firstState(root, workflow.workflowId), "RUNNING");
    const jobId = (await readStageAttempt(root, workflow.workflowId, "000-impl", 1)).jobId;
    await setJobTerminal(root, jobId, terminal);
    await reconcileWorkflowsOnce(deps);
    const attempt = await readStageAttempt(root, workflow.workflowId, "000-impl", 1);
    assert.equal(attempt.stageState, expected);
    assert.equal(attempt.jobOutcome, terminal.jobOutcome);
    assert.equal(attempt.processState, terminal.processState);
    await reconcileWorkflowsOnce(deps); // idempotent re-tick — no re-transition
    assert.equal((await readStageAttempt(root, workflow.workflowId, "000-impl", 1)).stageState, expected);
    // second stage never auto-started
    assert.equal((await readStageAttempt(root, workflow.workflowId, "010-test", 1)).stageState, "PENDING");
    assert.equal((await listJobs(root)).length, 1);
    await cleanup(root, ws);
  });
}

test("a still-running linked job keeps the stage at RUNNING", async () => {
  const { root, ws, deps, workflow } = await setup();
  await transitionStageAttempt(root, workflow.workflowId, { stageId: "000-impl", attempt: 1, toState: "SUBMITTING" });
  await reconcileWorkflowsOnce(deps);
  await reconcileWorkflowsOnce(deps); // job still QUEUED
  assert.equal(await firstState(root, workflow.workflowId), "RUNNING");
  await cleanup(root, ws);
});

// ---- UNVERIFIED does not advance (only PASSED unlocks the next stage) ----
test("an UNVERIFIED frontier does NOT advance to the next stage (no verified-success producer)", async () => {
  const { root, ws, deps, workflow } = await setup();
  await transitionStageAttempt(root, workflow.workflowId, { stageId: "000-impl", attempt: 1, toState: "SUBMITTING" });
  await reconcileWorkflowsOnce(deps); // RUNNING + job
  const jobId = (await readStageAttempt(root, workflow.workflowId, "000-impl", 1)).jobId;
  await setJobTerminal(root, jobId, { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" });
  await reconcileWorkflowsOnce(deps); // → UNVERIFIED
  assert.equal(await firstState(root, workflow.workflowId), "UNVERIFIED");
  await reconcileWorkflowsOnce(deps); // UNVERIFIED frontier → stopped, no advance
  assert.equal((await readStageAttempt(root, workflow.workflowId, "010-test", 1)).stageState, "PENDING");
  assert.equal((await listJobs(root)).length, 1, "no second-stage job while the first is UNVERIFIED");
  await cleanup(root, ws);
});

// ---- feature flag ----
test("workflowEnabled=false: no NEW submission, but terminal reconciliation of an existing linked job still runs", async () => {
  // 1) disabled + SUBMITTING with no job → must NOT submit
  const a = await setup({ enabled: false });
  await transitionStageAttempt(a.root, a.workflow.workflowId, { stageId: "000-impl", attempt: 1, toState: "SUBMITTING" });
  await reconcileWorkflowsOnce(a.deps);
  assert.equal((await listJobs(a.root)).length, 0, "disabled must not start new work");
  assert.equal(await firstState(a.root, a.workflow.workflowId), "SUBMITTING");
  await cleanup(a.root, a.ws);

  // 2) an already-linked RUNNING job reaches terminal → verdict applied even while disabled
  const b = await setup({ enabled: true });
  await transitionStageAttempt(b.root, b.workflow.workflowId, { stageId: "000-impl", attempt: 1, toState: "SUBMITTING" });
  await reconcileWorkflowsOnce(b.deps); // RUNNING + job
  const jobId = (await readStageAttempt(b.root, b.workflow.workflowId, "000-impl", 1)).jobId;
  await setJobTerminal(b.root, jobId, { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" });
  const disabledDeps = { ...b.deps, config: { ...b.config, workflowEnabled: false } };
  await reconcileWorkflowsOnce(disabledDeps);
  assert.equal(await firstState(b.root, b.workflow.workflowId), "UNVERIFIED", "terminal reconciliation runs regardless of the flag");
  await cleanup(b.root, b.ws);
});

// ---- standalone vs linked continuation separation (via core.reconcileOnce) ----
test("a linked terminal job gets NO standalone continuation; a standalone terminal job does", async () => {
  const root = await tmpDir("wf-rec-root-");
  const ws = await tmpDir("wf-rec-ws-");
  const config = { ...readConfig({ pluginConfig: { workflowEnabled: true, allowedRoots: [ws] } }), continuationEnabled: true };
  const fake = makeFakeStartJob();
  // standalone terminal job (parent + session, no workflowLink)
  const standalone = await fake.startJob({ rootDir: root }, { agentId: "a", sessionKey: "sess-standalone", sessionId: "s1", deliveryContext: null }, { name: "solo", command: ["true"], cwd: ws, deliveryRoute: ROUTE });
  // linked terminal job (workflowLink present, session-free)
  const linked = await fake.startJob({ rootDir: root }, { agentId: "a", sessionKey: null }, { name: "linked", command: ["true"], cwd: ws, deliveryRoute: ROUTE, workflowLink: { workflowId: "wf-22222222-2222-2222-2222-222222222222", stageId: "000-impl", attempt: 1, activityIdempotencyKey: "k" } });
  for (const j of [standalone.id, linked.id]) await setJobTerminal(root, j, { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" });

  const gateway = async (method) => {
    if (method === "chat.history") return { result: { sessionInfo: { origin: { provider: "slack", to: "C1", chatType: "channel" } }, messages: [] } };
    if (method === "chat.send") return { result: { status: "started" } };
    return { result: {} };
  };
  await reconcileOnce({ rootDir: root, config, gatewayCall: gateway, settleFlow: async () => {}, logger: undefined, createFlow: () => ({ flowId: null }), spawnWorker: () => 0 });

  assert.equal((await readJob(root, linked.id)).continuation ?? null, null, "linked job must not enter standalone continuation");
  assert.notEqual((await readJob(root, standalone.id)).continuation ?? null, null, "standalone job keeps P1 continuation");
  await cleanup(root, ws);
});

// ---- explicit crash windows (boundary 2) ----
// Helper: claim SUBMITTING, create the (correctly-linked) job, and record its jobId on the still-SUBMITTING
// attempt — the exact "attached but not yet RUNNING" crash state.
async function submittingWithAttachedJob(ctx) {
  const { root, deps, workflow } = ctx;
  const stageId = "000-impl";
  await transitionStageAttempt(root, workflow.workflowId, { stageId, attempt: 1, toState: "SUBMITTING" });
  const key = makeActivityIdempotencyKey(workflow.workflowId, stageId, 1);
  const spec = workflow.pipeline.find((s) => s.stageId === stageId);
  const job = await ensureLinkedJob({ rootDir: root, startJob: deps.startJob, startDeps: { rootDir: root } }, { workflow, stageSpec: spec, stageId, attempt: 1, activityIdempotencyKey: key });
  await transitionStageAttempt(root, workflow.workflowId, { stageId, attempt: 1, toState: "SUBMITTING", mutation: { jobId: job.id } }); // idempotent merge
  return job;
}

test("A. attach-before-RUNNING: SUBMITTING with a matching linked job (still QUEUED) → RUNNING, no new job, idempotent", async () => {
  const c = await setup();
  const job = await submittingWithAttachedJob(c);
  await reconcileWorkflowsOnce(c.deps);
  const a = await readStageAttempt(c.root, c.workflow.workflowId, "000-impl", 1);
  assert.equal(a.stageState, "RUNNING");
  assert.equal(a.jobId, job.id);
  assert.equal((await listJobs(c.root)).length, 1);
  await reconcileWorkflowsOnce(c.deps); // idempotent
  assert.equal(await firstState(c.root, c.workflow.workflowId), "RUNNING");
  assert.equal((await listJobs(c.root)).length, 1);
  await cleanup(c.root, c.ws);
});

test("B. attach-before-terminal-reconciliation: SUBMITTING with a matching TERMINAL job → converges to verdict (not stuck RUNNING)", async () => {
  const c = await setup();
  const job = await submittingWithAttachedJob(c);
  await setJobTerminal(c.root, job.id, { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" });
  await reconcileWorkflowsOnce(c.deps);
  assert.equal(await firstState(c.root, c.workflow.workflowId), "UNVERIFIED");
  assert.equal((await listJobs(c.root)).length, 1);
  await reconcileWorkflowsOnce(c.deps); // idempotent
  assert.equal(await firstState(c.root, c.workflow.workflowId), "UNVERIFIED");
  await cleanup(c.root, c.ws);
});

test("C. canonical-before-projection: canonical RUNNING + terminal job with projections deleted → canonical wins, rebuilt, no job recreate", async () => {
  const c = await setup();
  await transitionStageAttempt(c.root, c.workflow.workflowId, { stageId: "000-impl", attempt: 1, toState: "SUBMITTING" });
  await reconcileWorkflowsOnce(c.deps); // RUNNING + linked job
  const jobId = (await readStageAttempt(c.root, c.workflow.workflowId, "000-impl", 1)).jobId;
  await setJobTerminal(c.root, jobId, { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" });
  // wipe the projections; canonical attempt (RUNNING, jobId) survives
  await _rm(path.join(c.root, "workflows", c.workflow.workflowId, "workflow.json"), { force: true });
  await _rm(path.join(c.root, "workflows", c.workflow.workflowId, "stages", "000-impl", "stage.json"), { force: true });
  await reconcileWorkflowsOnce(c.deps);
  assert.equal(await firstState(c.root, c.workflow.workflowId), "UNVERIFIED"); // canonical-driven verdict
  const wf = await readWorkflow(c.root, c.workflow.workflowId);
  assert.equal(wf.currentStage, "000-impl"); // projection rebuilt
  assert.equal((await listJobs(c.root)).length, 1, "no job recreated");
  await cleanup(c.root, c.ws);
});

test("D. mismatched link: attempt.jobId points at a job whose workflowLink does not match → fail-closed, isolated, no new job", async () => {
  const c = await setup();
  await transitionStageAttempt(c.root, c.workflow.workflowId, { stageId: "000-impl", attempt: 1, toState: "SUBMITTING" });
  await reconcileWorkflowsOnce(c.deps); // RUNNING + correct job J_A
  // a foreign job whose workflowLink points at a DIFFERENT workflow
  const otherWf = "wf-99999999-9999-4999-8999-999999999999";
  const bad = await c.fake.startJob({ rootDir: c.root }, { agentId: "a", sessionKey: null }, { name: "bad", command: ["true"], cwd: c.ws, deliveryRoute: ROUTE, workflowLink: { workflowId: otherWf, stageId: "000-impl", attempt: 1, activityIdempotencyKey: `wf:${otherWf}:stage:000-impl:attempt:1` } });
  // corrupt the linkage: point the attempt at the foreign job
  await transitionStageAttempt(c.root, c.workflow.workflowId, { stageId: "000-impl", attempt: 1, toState: "RUNNING", mutation: { jobId: bad.id } });

  // advanceWorkflowOnce fails closed on the mismatch (never trusts/overwrites, never creates a replacement)
  await assert.rejects(() => advanceWorkflowOnce(c.deps, c.workflow.workflowId), (e) => e.code === "WORKFLOW_LINKAGE_MISMATCH");
  const jobsBefore = (await listJobs(c.root)).length;

  // add a healthy second workflow; reconcileWorkflowsOnce isolates the bad one and still drives the good one
  const goodWf = await makeWorkflow(c.root, c.ws);
  await transitionStageAttempt(c.root, goodWf.workflowId, { stageId: "000-impl", attempt: 1, toState: "SUBMITTING" });
  await reconcileWorkflowsOnce(c.deps); // must not throw despite the mismatched workflow
  assert.equal((await readStageAttempt(c.root, c.workflow.workflowId, "000-impl", 1)).jobId, bad.id, "mismatched jobId not overwritten");
  assert.equal((await readStageAttempt(c.root, goodWf.workflowId, "000-impl", 1)).stageState, "RUNNING", "healthy workflow still reconciled");
  assert.equal((await listJobs(c.root)).length, jobsBefore + 1, "only the healthy workflow's job was added");
  await cleanup(c.root, c.ws);
});
