// P3-E control plane: approve / reject / cancel / resume — authorization, stale-control protection, control
// idempotency, decision semantics, and the feature-flag policy. Driven through the service dispatcher
// (runWorkflowAction) with a fake startJob (no worker spawn) and the real cancelJob.
import assert from "node:assert/strict";
import { mkdtemp, rm, readFile, readdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { cancelJob, readConfig } from "../dist/core.js";
import { listJobs, readJob } from "../dist/job-store.js";
import { createWorkflow, readStageAttempt, readWorkflow, recordCancel, transitionStageAttempt } from "../dist/workflow-store.js";
import { runWorkflowAction } from "../dist/workflow-service.js";
import { advanceWorkflowOnce } from "../dist/workflow-reconciler.js";
import { ensureLinkedJob, makeActivityIdempotencyKey } from "../dist/workflow-activity.js";
import { controlCancel } from "../dist/workflow-control.js";
import { makeFakeStartJob, setJobTerminal, completeSeams } from "./wf-linkage-helpers.js";

const okGateway = async (method) => {
  if (method === "chat.history") return { result: { sessionInfo: { origin: { provider: "slack", to: "C1", chatType: "channel" } } } };
  return { result: {} };
};
const trustedCtx = (o = {}) => ({ agentId: "agent-1", sessionKey: "sess-1", sessionId: "s1", workspaceDir: o.workspaceDir, ...o });
const PIPELINE = [
  { name: "impl", runnerType: "local", runnerProfile: "local_test", activity: { argv: ["true"] } },
  { name: "test", runnerType: "local", runnerProfile: "local_test", activity: { argv: ["true"] } },
];
const S0 = "000-impl", S1 = "010-test";
const tmp = (p) => mkdtemp(path.join(os.tmpdir(), p));

async function setup() {
  const root = await tmp("wf-ctl-root-");
  const ws = await tmp("wf-ctl-ws-");
  const config = readConfig({ pluginConfig: { workflowEnabled: true, allowedRoots: [ws] } });
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, config, gatewayCall: okGateway, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams() };
  const ctx = trustedCtx({ workspaceDir: ws });
  const started = await runWorkflowAction(deps, ctx, { action: "start", name: "wf", worktree: ws, pipeline: PIPELINE });
  return { root, ws, config, fake, deps, ctx, id: started.workflowId };
}
const advDeps = (deps) => ({ rootDir: deps.rootDir, config: deps.config, startJob: deps.startJob, startDeps: deps.startDeps });
const cleanup = (root, ws) => Promise.all([rm(root, { recursive: true, force: true }), rm(ws, { recursive: true, force: true })]);
// settle stage0's job to UNVERIFIED (the manual-approval precondition)
async function toUnverified(deps, id, stageId = S0) {
  const jobId = (await readStageAttempt(deps.rootDir, id, stageId, 1)).jobId;
  await setJobTerminal(deps.rootDir, jobId, { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" });
  await advanceWorkflowOnce(advDeps(deps), id);
  return jobId;
}
const ctl = (deps, ctx, action, id, over = {}) => runWorkflowAction(deps, ctx, { action, workflowId: id, stageId: S0, attempt: 1, requestId: "r1", reason: "ok", ...over });

// ---- authorization / stale ----
test("only the owner may control; a different session is forbidden", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  const other = trustedCtx({ agentId: "agent-1", sessionKey: "someone-else", workspaceDir: ws });
  for (const action of ["approve", "reject", "cancel", "resume"]) {
    await assert.rejects(() => ctl(deps, other, action, id), /WORKFLOW_FORBIDDEN/, `${action} must be owner-only`);
  }
  await cleanup(root, ws);
});

test("stale stageId / attempt are rejected (frontier moved)", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  await assert.rejects(() => ctl(deps, ctx, "approve", id, { stageId: S1 }), /WORKFLOW_CONTROL_STALE/); // wrong stage
  await assert.rejects(() => ctl(deps, ctx, "approve", id, { attempt: 2 }), /WORKFLOW_CONTROL_STALE/); // wrong attempt
  await cleanup(root, ws);
});

test("control input is validated (reason required, requestId bounded)", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  await assert.rejects(() => ctl(deps, ctx, "approve", id, { reason: "" }), /WORKFLOW_INPUT_INVALID/);
  await assert.rejects(() => ctl(deps, ctx, "approve", id, { requestId: "bad id!" }), /WORKFLOW_INPUT_INVALID/);
  await cleanup(root, ws);
});

// ---- approve ----
test("approve: UNVERIFIED → PASSED (MANUAL), history preserved, next stage advances", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  const res = await ctl(deps, ctx, "approve", id, { reason: "looks good" });
  const s0 = res.stages[0];
  assert.equal(s0.stageState, "PASSED");
  assert.equal(s0.verificationSource, "MANUAL_APPROVAL");
  assert.equal(s0.decision.action, "APPROVE");
  assert.equal(s0.decision.source, "MANUAL");
  assert.equal(s0.decision.reason, "looks good");
  assert.equal(s0.jobOutcome, "COMPLETED_UNVERIFIED", "process/job outcome preserved (not falsified)");
  assert.equal(s0.processState, "COMPLETED");
  assert.equal(res.stages[1].stageState, "RUNNING", "next stage advanced exactly once");
  assert.equal((await listJobs(root)).length, 2);
  await cleanup(root, ws);
});

test("approve: APPROVAL_REQUIRED → PASSED; last-stage approve → SUCCEEDED", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  // stage0 is RUNNING after start; drive it to APPROVAL_REQUIRED via the store
  await transitionStageAttempt(root, id, { stageId: S0, attempt: 1, toState: "UNVERIFIED" });
  await transitionStageAttempt(root, id, { stageId: S0, attempt: 1, toState: "APPROVAL_REQUIRED" });
  await ctl(deps, ctx, "approve", id, { requestId: "a0" });
  assert.equal((await readStageAttempt(root, id, S0, 1)).stageState, "PASSED");
  // stage1 → UNVERIFIED → approve → SUCCEEDED
  await toUnverified(deps, id, S1);
  const res = await runWorkflowAction(deps, ctx, { action: "approve", workflowId: id, stageId: S1, attempt: 1, requestId: "a1", reason: "ok" });
  assert.equal(res.workflowState, "SUCCEEDED");
  await cleanup(root, ws);
});

test("approve rejected from non-approvable states; approve-then-reject is stale", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await assert.rejects(() => ctl(deps, ctx, "approve", id, { requestId: "x" }), /WORKFLOW_CONTROL_NOT_ALLOWED/); // stage0 RUNNING
  await toUnverified(deps, id);
  await ctl(deps, ctx, "approve", id, { requestId: "app" });
  // stage0 now PASSED (frontier moved to stage1) → rejecting stage0 attempt1 is stale
  await assert.rejects(() => runWorkflowAction(deps, ctx, { action: "reject", workflowId: id, stageId: S0, attempt: 1, requestId: "rej", reason: "no" }), /WORKFLOW_CONTROL_STALE/);
  await cleanup(root, ws);
});

// ---- reject ----
test("reject: UNVERIFIED → FAILED, workflow FAILED, no next stage, history preserved, idempotent", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  const res = await ctl(deps, ctx, "reject", id, { requestId: "rj", reason: "bad" });
  assert.equal(res.stages[0].stageState, "FAILED");
  assert.equal(res.stages[0].decision.action, "REJECT");
  assert.equal(res.stages[0].jobOutcome, "COMPLETED_UNVERIFIED", "history preserved");
  assert.equal(res.workflowState, "FAILED");
  assert.equal(res.stages[1].stageState, "PENDING");
  assert.equal((await listJobs(root)).length, 1, "no next-stage job on reject");
  const again = await ctl(deps, ctx, "reject", id, { requestId: "rj", reason: "bad" }); // idempotent
  assert.equal(again.stages[0].stageState, "FAILED");
  await cleanup(root, ws);
});

// ---- control idempotency ----
test("same requestId+payload replays; different payload conflicts; concurrent applies once", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  const a = await ctl(deps, ctx, "approve", id, { requestId: "dup", reason: "ok" });
  const b = await ctl(deps, ctx, "approve", id, { requestId: "dup", reason: "ok" }); // replay
  assert.equal(a.stages[0].decision.decidedAt, b.stages[0].decision.decidedAt, "replay returns the same decision");
  assert.equal((await listJobs(root)).length, 2, "replay does not advance twice");
  // different payload, same requestId → conflict
  await assert.rejects(() => runWorkflowAction(deps, ctx, { action: "approve", workflowId: id, stageId: S1, attempt: 1, requestId: "dup", reason: "different" }), /WORKFLOW_CONTROL_REQUEST_CONFLICT|WORKFLOW_CONTROL_STALE/);
  await cleanup(root, ws);
});

test("concurrent duplicate approve applies exactly once", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  await Promise.all([ctl(deps, ctx, "approve", id, { requestId: "c" }), ctl(deps, ctx, "approve", id, { requestId: "c" })]);
  assert.equal((await readStageAttempt(root, id, S0, 1)).stageState, "PASSED");
  assert.equal((await listJobs(root)).length, 2, "one advancement (one stage1 job)");
  await cleanup(root, ws);
});

// ---- cancel ----
test("cancel a RUNNING stage: job cancelled, stage+workflow CANCELLED, no next job; flag-independent", async () => {
  const { root, ws, config, fake, id } = await setup();
  const disabledDeps = { rootDir: root, config: { ...config, workflowEnabled: false }, gatewayCall: okGateway, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams() };
  const ctx = trustedCtx({ workspaceDir: ws });
  const res = await runWorkflowAction(disabledDeps, ctx, { action: "cancel", workflowId: id, stageId: S0, attempt: 1, requestId: "cx", reason: "stop" }); // cancel works while disabled
  assert.equal(res.stages[0].stageState, "CANCELLED");
  assert.equal(res.workflowState, "CANCELLED");
  const jobId = res.stages[0].jobId;
  assert.equal((await readJob(root, jobId)).jobOutcome, "CANCELLED", "linked job cancelled");
  assert.equal(res.stages[1].stageState, "PENDING");
  assert.equal((await listJobs(root)).length, 1, "no next-stage job after cancel");
  await cleanup(root, ws);
});

test("cancel a PENDING next stage frontier: CANCELLED, no job created", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  // stage0 RUNNING after start → drive to PASSED so the frontier is S1 PENDING
  await transitionStageAttempt(root, id, { stageId: S0, attempt: 1, toState: "UNVERIFIED" });
  await transitionStageAttempt(root, id, { stageId: S0, attempt: 1, toState: "PASSED" });
  const before = (await listJobs(root)).length;
  const res = await runWorkflowAction(deps, ctx, { action: "cancel", workflowId: id, stageId: S1, attempt: 1, requestId: "cp", reason: "stop" });
  assert.equal(res.stages[1].stageState, "CANCELLED");
  assert.equal(res.workflowState, "CANCELLED");
  assert.equal((await listJobs(root)).length, before, "no job created for a cancelled PENDING stage");
  await cleanup(root, ws);
});

test("cancel is idempotent and does not falsify an already-terminal job", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id); // stage0 UNVERIFIED (job already COMPLETED_UNVERIFIED)
  const res = await runWorkflowAction(deps, ctx, { action: "cancel", workflowId: id, stageId: S0, attempt: 1, requestId: "ct", reason: "stop" });
  assert.equal(res.stages[0].stageState, "CANCELLED");
  assert.equal(res.stages[0].jobOutcome, "COMPLETED_UNVERIFIED", "real job outcome preserved");
  assert.equal(res.stages[0].cancel.cancelledAfterTerminal, true);
  const again = await runWorkflowAction(deps, ctx, { action: "cancel", workflowId: id, stageId: S0, attempt: 1, requestId: "ct", reason: "stop" });
  assert.equal(again.stages[0].stageState, "CANCELLED");
  await cleanup(root, ws);
});

// ---- resume ----
test("resume a FAILED stage → attempt 2 PENDING/RUNNING, attempt 1 preserved, one new job", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  await ctl(deps, ctx, "reject", id, { requestId: "rj", reason: "bad" }); // stage0 FAILED
  const attempt1Raw = await readFile(path.join(root, "workflows", id, "stages", S0, "attempts", "0001.json"), "utf8");
  const res = await runWorkflowAction(deps, ctx, { action: "resume", workflowId: id, stageId: S0, attempt: 1, requestId: "rs", reason: "retry" });
  assert.equal(res.stages[0].currentAttempt, 2);
  assert.equal(res.stages[0].stageState, "RUNNING"); // resumed + advanced
  assert.equal(res.stages[0].resume.resumeMode, "MANUAL_RERUN");
  assert.equal(res.stages[0].resume.checkpointVerified, false);
  assert.equal(res.stages[0].resume.resumeOfAttempt, 1);
  // attempt 1 byte-for-byte preserved
  assert.equal(await readFile(path.join(root, "workflows", id, "stages", S0, "attempts", "0001.json"), "utf8"), attempt1Raw);
  const a2 = await readStageAttempt(root, id, S0, 2);
  assert.equal(a2.activityIdempotencyKey, `wf:${id}:stage:${S0}:attempt:2`);
  assert.equal((await listJobs(root)).length, 2, "one new linked job for attempt 2");
  await cleanup(root, ws);
});

test("resume rejected from non-resumable states; concurrent resume increments attempt once", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  // stage0 RUNNING → resume not allowed
  await assert.rejects(() => runWorkflowAction(deps, ctx, { action: "resume", workflowId: id, stageId: S0, attempt: 1, requestId: "r", reason: "x" }), /WORKFLOW_RESUME_NOT_ALLOWED/);
  await toUnverified(deps, id);
  await ctl(deps, ctx, "reject", id, { requestId: "rj", reason: "bad" });
  await Promise.all([
    runWorkflowAction(deps, ctx, { action: "resume", workflowId: id, stageId: S0, attempt: 1, requestId: "rr", reason: "retry" }),
    runWorkflowAction(deps, ctx, { action: "resume", workflowId: id, stageId: S0, attempt: 1, requestId: "rr", reason: "retry" }),
  ]);
  const attempts = (await readdir(path.join(root, "workflows", id, "stages", S0, "attempts"))).filter((f) => f.endsWith(".json"));
  assert.deepEqual(attempts.sort(), ["0001.json", "0002.json"], "concurrent resume creates exactly one new attempt");
  await cleanup(root, ws);
});

// ---- cross-action requestId namespace ----
test("a requestId is one control namespace across actions: reusing it for a different action conflicts", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  await ctl(deps, ctx, "approve", id, { requestId: "n1", reason: "ok" }); // approve consumes requestId n1
  await assert.rejects(() => runWorkflowAction(deps, ctx, { action: "cancel", workflowId: id, stageId: S0, attempt: 1, requestId: "n1", reason: "ok" }), /WORKFLOW_CONTROL_REQUEST_CONFLICT/);
  await assert.rejects(() => runWorkflowAction(deps, ctx, { action: "resume", workflowId: id, stageId: S0, attempt: 1, requestId: "n1", reason: "ok" }), /WORKFLOW_CONTROL_REQUEST_CONFLICT/);
  await cleanup(root, ws);
});

test("reject then resume with the same requestId conflicts; same action + changed reason conflicts", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  await ctl(deps, ctx, "reject", id, { requestId: "n2", reason: "bad" });
  await assert.rejects(() => runWorkflowAction(deps, ctx, { action: "resume", workflowId: id, stageId: S0, attempt: 1, requestId: "n2", reason: "retry" }), /WORKFLOW_CONTROL_REQUEST_CONFLICT/);
  // same action but a different reason (payload) → conflict (idempotency requires an identical payload)
  await assert.rejects(() => runWorkflowAction(deps, ctx, { action: "reject", workflowId: id, stageId: S0, attempt: 1, requestId: "n2", reason: "different" }), /WORKFLOW_CONTROL_REQUEST_CONFLICT/);
  await cleanup(root, ws);
});

test("concurrent different actions with the same requestId apply exactly one", async () => {
  const { root, ws, deps, ctx, id } = await setup();
  await toUnverified(deps, id);
  const results = await Promise.allSettled([
    runWorkflowAction(deps, ctx, { action: "approve", workflowId: id, stageId: S0, attempt: 1, requestId: "race", reason: "ok" }),
    runWorkflowAction(deps, ctx, { action: "cancel", workflowId: id, stageId: S0, attempt: 1, requestId: "race", reason: "stop" }),
  ]);
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 1, "exactly one action applied");
  assert.ok(results.some((r) => r.status === "rejected" && /CONFLICT/.test(r.reason?.message)), "the other conflicted");
  const st = (await readStageAttempt(root, id, S0, 1)).stageState;
  assert.ok(st === "PASSED" || st === "CANCELLED", `stage settled to one decision (${st})`);
  await cleanup(root, ws);
});

// ---- cancel ↔ ensureLinkedJob barrier ----
const ROUTE = { routeKind: "channel_root", channel: "slack", to: "C1" };
async function bareWorkflow(root, ws) {
  const wid = await createWorkflow(root, {
    name: "wf", ownerKey: "ok", requestId: null, payloadFingerprint: "fp",
    parent: { agentId: "a", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null },
    repository: { worktree: ws, branch: null, baseCommit: null, verificationProfile: null },
    deliveryRoute: ROUTE, forbiddenActions: [],
    pipeline: [{ pipelineIndex: 0, stageName: "s0", runnerType: "local", runnerProfile: "local_test", activity: { argv: ["true"] } }],
  });
  return readWorkflow(root, wid);
}
const ctlDeps = (root, config, fake) => ({ rootDir: root, config, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams(), cancelJob, readJob });
const cp = (wid, over = {}) => ({ workflowId: wid, stageId: "000-s0", attempt: 1, requestId: "c", reason: "stop", ownerKeyHash: "h", payloadFingerprint: "fp", actor: { agentId: "a" }, ...over });

test("barrier: a recorded cancelRequest makes ensureLinkedJob refuse to submit (no orphan job)", async () => {
  const root = await tmp("wf-ctl-root-");
  const ws = await tmp("wf-ctl-ws-");
  const config = readConfig({ pluginConfig: { workflowEnabled: true, allowedRoots: [ws] } });
  const fake = makeFakeStartJob();
  const wf = await bareWorkflow(root, ws);
  await transitionStageAttempt(root, wf.workflowId, { stageId: "000-s0", attempt: 1, toState: "SUBMITTING", mutation: { cancelRequest: { requestId: "c", status: "REQUESTED", reason: "stop", requestedAt: new Date().toISOString() } } });
  const spec = wf.pipeline.find((s) => s.stageId === "000-s0");
  await assert.rejects(
    () => ensureLinkedJob(ctlDeps(root, config, fake), { workflow: wf, stageSpec: spec, stageId: "000-s0", attempt: 1, activityIdempotencyKey: makeActivityIdempotencyKey(wf.workflowId, "000-s0", 1) }),
    (e) => e.code === "WORKFLOW_ACTIVITY_CANCELLED",
  );
  assert.equal((await listJobs(root)).length, 0, "no orphan job created for a cancelling stage");
  await cleanup(root, ws);
});

test("barrier: concurrent cancel and submit → at most one job, no RUNNING orphan, final CANCELLED", async () => {
  for (let iter = 0; iter < 6; iter++) {
    const root = await tmp("wf-ctl-root-");
    const ws = await tmp("wf-ctl-ws-");
    const config = readConfig({ pluginConfig: { workflowEnabled: true, allowedRoots: [ws] } });
    const fake = makeFakeStartJob();
    const wf = await bareWorkflow(root, ws);
    await transitionStageAttempt(root, wf.workflowId, { stageId: "000-s0", attempt: 1, toState: "SUBMITTING" }); // claimed, no job
    const deps = ctlDeps(root, config, fake);
    await Promise.allSettled([
      advanceWorkflowOnce(deps, wf.workflowId), // settleActiveStage → ensureLinkedJob (may submit)
      controlCancel(deps, cp(wf.workflowId)),
    ]);
    // drain: a follow-up reconcile converges any in-flight cancel
    await advanceWorkflowOnce(deps, wf.workflowId).catch(() => {});
    const jobs = await listJobs(root);
    assert.ok(jobs.length <= 1, `at most one job (iter ${iter}, got ${jobs.length})`);
    for (const j of jobs) assert.notEqual(j.state, "RUNNING", "no orphan RUNNING job survives a cancel");
    const st = (await readStageAttempt(root, wf.workflowId, "000-s0", 1)).stageState;
    assert.equal(st, "CANCELLED", `final stage CANCELLED (iter ${iter}, got ${st})`);
    await cleanup(root, ws);
  }
});

// ---- feature flag ----
test("workflowEnabled=false: approve/reject/resume disabled, cancel allowed", async () => {
  const { root, ws, config, fake, id, deps } = await setup();
  await toUnverified(deps, id);
  const disabled = { rootDir: root, config: { ...config, workflowEnabled: false }, gatewayCall: okGateway, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams() };
  const ctx = trustedCtx({ workspaceDir: ws });
  for (const action of ["approve", "reject", "resume"]) {
    await assert.rejects(() => runWorkflowAction(disabled, ctx, { action, workflowId: id, stageId: S0, attempt: 1, requestId: `d-${action}`, reason: "x" }), /WORKFLOW_DISABLED/);
  }
  const res = await runWorkflowAction(disabled, ctx, { action: "cancel", workflowId: id, stageId: S0, attempt: 1, requestId: "dc", reason: "stop" });
  assert.equal(res.stages[0].stageState, "CANCELLED");
  await cleanup(root, ws);
});
