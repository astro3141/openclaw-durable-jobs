// P3-D linear multi-stage advancement: frontier computation, atomic runnable claim, three-stage lifecycle,
// per-tick submission bound, crash recovery, invariants, feature flag, idempotency/concurrency.
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { readConfig } from "../dist/core.js";
import { listJobs, readJob } from "../dist/job-store.js";
import {
  claimRunnableStage,
  computeFrontier,
  createWorkflow,
  readStageAttempt,
  readWorkflow,
  transitionStageAttempt,
} from "../dist/workflow-store.js";
import { advanceWorkflowOnce, reconcileWorkflowsOnce } from "../dist/workflow-reconciler.js";
import { makeFakeStartJob, setJobTerminal, completeSeams } from "./wf-linkage-helpers.js";

const ROUTE = { routeKind: "channel_root", channel: "slack", to: "C1" };
const sid = (i) => `${String(i * 10).padStart(3, "0")}-s${i}`;
const tmp = (p) => mkdtemp(path.join(os.tmpdir(), p));

async function makeWorkflow(root, ws, n = 3) {
  const pipeline = Array.from({ length: n }, (_, i) => ({ pipelineIndex: i * 10, stageName: `s${i}`, runnerType: "local", runnerProfile: "local_test", activity: { argv: ["true"] } }));
  const id = await createWorkflow(root, {
    name: "wf", ownerKey: "ok", requestId: null, payloadFingerprint: "fp",
    parent: { agentId: "a", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null },
    repository: { worktree: ws, branch: null, baseCommit: null, verificationProfile: null },
    deliveryRoute: ROUTE, forbiddenActions: [], pipeline,
  });
  return readWorkflow(root, id);
}

async function setup({ enabled = true, n = 3 } = {}) {
  const root = await tmp("wf-adv-root-");
  const ws = await tmp("wf-adv-ws-");
  const config = readConfig({ pluginConfig: { workflowEnabled: enabled, allowedRoots: [ws] } });
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, config, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams() };
  const workflow = await makeWorkflow(root, ws, n);
  return { root, ws, config, fake, deps, workflow, id: workflow.workflowId };
}

const stateOf = (root, id, i) => readStageAttempt(root, id, sid(i), 1).then((a) => a?.stageState);
const cleanup = (root, ws) => Promise.all([rm(root, { recursive: true, force: true }), rm(ws, { recursive: true, force: true })]);
// drive a canonical stage all the way to PASSED via the store (stands in for a future verified-success producer)
const forcePassed = (root, id, i) => ["SUBMITTING", "RUNNING", "UNVERIFIED", "PASSED"].reduce((p, s) => p.then(() => transitionStageAttempt(root, id, { stageId: sid(i), attempt: 1, toState: s })), Promise.resolve());

// ---- frontier / claim ----
test("computeFrontier classifies each linear state", async () => {
  const { root, ws, id } = await setup({ n: 3 });
  assert.equal((await computeFrontier(root, id)).status, "runnable"); // all PENDING → stage0 runnable
  assert.equal((await computeFrontier(root, id)).frontier.stageId, "000-s0");
  await transitionStageAttempt(root, id, { stageId: "000-s0", attempt: 1, toState: "SUBMITTING" });
  assert.equal((await computeFrontier(root, id)).status, "active"); // SUBMITTING → active
  await forcePassed(root, id, 0);
  assert.equal((await computeFrontier(root, id)).frontier.stageId, "010-s1"); // stage0 PASSED → frontier is stage1
  await forcePassed(root, id, 1);
  await forcePassed(root, id, 2);
  assert.equal((await computeFrontier(root, id)).status, "succeeded"); // all PASSED
  await cleanup(root, ws);
});

test("computeFrontier fail-closes on non-linear / missing shapes", async () => {
  // later stage active before predecessor PASSED
  let s = await setup({ n: 2 });
  await transitionStageAttempt(s.root, s.id, { stageId: "000-s0", attempt: 1, toState: "SUBMITTING" });
  await transitionStageAttempt(s.root, s.id, { stageId: "000-s0", attempt: 1, toState: "RUNNING" });
  await transitionStageAttempt(s.root, s.id, { stageId: "000-s0", attempt: 1, toState: "UNVERIFIED" });
  await transitionStageAttempt(s.root, s.id, { stageId: "010-s1", attempt: 1, toState: "SUBMITTING" });
  await assert.rejects(() => computeFrontier(s.root, s.id), (e) => e.code === "WORKFLOW_PIPELINE_INVARIANT");
  await cleanup(s.root, s.ws);
  // multiple active stages
  s = await setup({ n: 2 });
  await transitionStageAttempt(s.root, s.id, { stageId: "000-s0", attempt: 1, toState: "SUBMITTING" });
  await transitionStageAttempt(s.root, s.id, { stageId: "010-s1", attempt: 1, toState: "SUBMITTING" });
  await assert.rejects(() => computeFrontier(s.root, s.id), (e) => e.code === "WORKFLOW_PIPELINE_INVARIANT");
  await cleanup(s.root, s.ws);
  // missing canonical attempt
  s = await setup({ n: 2 });
  await rm(path.join(s.root, "workflows", s.id, "stages", "010-s1", "attempts", "0001.json"), { force: true });
  await assert.rejects(() => computeFrontier(s.root, s.id), (e) => e.code === "WORKFLOW_PIPELINE_INCOMPLETE");
  await cleanup(s.root, s.ws);
});

test("claimRunnableStage claims exactly the frontier and is a no-op for stop/succeeded/active states", async () => {
  const { root, ws, id } = await setup({ n: 2 });
  const c = await claimRunnableStage(root, id, { enabled: true });
  assert.equal(c.status, "claimed");
  assert.equal(c.stageId, "000-s0");
  assert.equal(await stateOf(root, id, 0), "SUBMITTING");
  // re-claim while stage0 is SUBMITTING → active (no new claim)
  assert.equal((await claimRunnableStage(root, id, { enabled: true })).status, "active");
  await cleanup(root, ws);
});

test("claimRunnableStage returns disabled (no claim) when the flag is off", async () => {
  const { root, ws, id } = await setup({ n: 2 });
  const c = await claimRunnableStage(root, id, { enabled: false });
  assert.equal(c.status, "disabled");
  assert.equal(await stateOf(root, id, 0), "PENDING", "disabled must not claim");
  await cleanup(root, ws);
});

// ---- three-stage lifecycle ----
test("three-stage linear lifecycle: one job per stage, in order, to SUCCEEDED", async () => {
  const { root, ws, deps, id } = await setup({ n: 3 });
  const jobIds = [];
  for (let i = 0; i < 3; i++) {
    await advanceWorkflowOnce(deps, id); // claim + submit stage i
    assert.equal(await stateOf(root, id, i), "RUNNING", `stage${i} RUNNING`);
    const a = await readStageAttempt(root, id, sid(i), 1);
    assert.equal((await readJob(root, a.jobId)).workflowLink.stageId, sid(i), `job linked to stage${i}`);
    jobIds.push(a.jobId);
    assert.equal((await listJobs(root)).length, i + 1, `total jobs = ${i + 1}`);
    // job finishes UNVERIFIED, then the (future) producer marks it PASSED
    await setJobTerminal(root, a.jobId, { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" });
    await advanceWorkflowOnce(deps, id);
    assert.equal(await stateOf(root, id, i), "UNVERIFIED");
    // next stage must NOT be running yet
    if (i < 2) assert.equal(await stateOf(root, id, i + 1), "PENDING", "next stage stays PENDING until PASSED");
    await transitionStageAttempt(root, id, { stageId: sid(i), attempt: 1, toState: "PASSED" });
  }
  await advanceWorkflowOnce(deps, id); // all PASSED → SUCCEEDED, no new job
  const wf = await readWorkflow(root, id);
  assert.equal(wf.workflowState, "SUCCEEDED");
  assert.deepEqual(wf.completedStages, ["000-s0", "010-s1", "020-s2"]);
  assert.equal((await listJobs(root)).length, 3, "exactly three jobs total");
  assert.equal(new Set(jobIds).size, 3, "three distinct jobs");
  await cleanup(root, ws);
});

// ---- per-tick submission bound ----
test("at most ONE new submission per advancement pass (even with two predecessors already PASSED)", async () => {
  const { root, ws, deps, id } = await setup({ n: 3 });
  await forcePassed(root, id, 0);
  await forcePassed(root, id, 1); // stage2 is now the runnable frontier
  await advanceWorkflowOnce(deps, id);
  assert.equal(await stateOf(root, id, 2), "RUNNING");
  assert.equal((await listJobs(root)).length, 1, "one submission this tick (stage2 only, not chained)");
  await cleanup(root, ws);
});

// ---- crash recovery (non-first stage) ----
test("crash: predecessor PASSED, next stage SUBMITTING with no job → reconcile submits one, RUNNING", async () => {
  const { root, ws, deps, id } = await setup({ n: 2 });
  await forcePassed(root, id, 0);
  await transitionStageAttempt(root, id, { stageId: "010-s1", attempt: 1, toState: "SUBMITTING" }); // claimed then crashed
  await advanceWorkflowOnce(deps, id);
  assert.equal(await stateOf(root, id, 1), "RUNNING");
  assert.equal((await listJobs(root)).length, 1);
  await advanceWorkflowOnce(deps, id); // idempotent
  assert.equal((await listJobs(root)).length, 1);
  await cleanup(root, ws);
});

test("crash: RUNNING next stage + terminal job with projections deleted → canonical-driven settle, no recreate", async () => {
  const { root, ws, deps, id } = await setup({ n: 2 });
  await forcePassed(root, id, 0);
  await advanceWorkflowOnce(deps, id); // stage1 RUNNING + job
  const jobId = (await readStageAttempt(root, id, "010-s1", 1)).jobId;
  await setJobTerminal(root, jobId, { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" });
  await rm(path.join(root, "workflows", id, "workflow.json"), { force: true });
  await rm(path.join(root, "workflows", id, "stages", "010-s1", "stage.json"), { force: true });
  await advanceWorkflowOnce(deps, id);
  assert.equal(await stateOf(root, id, 1), "UNVERIFIED");
  assert.equal((await listJobs(root)).length, 1, "no job recreated");
  await cleanup(root, ws);
});

test("crash: canonical predecessor PASSED but workflow.json projection is stale → frontier from canonical, advances", async () => {
  const { root, ws, deps, id } = await setup({ n: 2 });
  await forcePassed(root, id, 0);
  // corrupt the projection's currentStage/workflowState (canonical stage0 is PASSED)
  const { readFile, writeFile } = await import("node:fs/promises");
  const wfFile = path.join(root, "workflows", id, "workflow.json");
  await writeFile(wfFile, JSON.stringify({ ...JSON.parse(await readFile(wfFile, "utf8")), currentStage: "000-s0", workflowState: "RUNNING" }));
  await advanceWorkflowOnce(deps, id);
  assert.equal(await stateOf(root, id, 1), "RUNNING", "advanced from the canonical frontier, not the stale projection");
  assert.equal((await listJobs(root)).length, 1);
  await cleanup(root, ws);
});

test("crash: last stage PASSED with projections deleted → rebuild to SUCCEEDED, no new job", async () => {
  const { root, ws, deps, id } = await setup({ n: 2 });
  await forcePassed(root, id, 0);
  await forcePassed(root, id, 1);
  await rm(path.join(root, "workflows", id, "workflow.json"), { force: true });
  await advanceWorkflowOnce(deps, id);
  assert.equal((await readWorkflow(root, id)).workflowState, "SUCCEEDED");
  assert.equal((await listJobs(root)).length, 0);
  await cleanup(root, ws);
});

// ---- idempotency / concurrency ----
test("repeated + concurrent advancement keeps one job per stage", async () => {
  const { root, ws, deps, id } = await setup({ n: 2 });
  await forcePassed(root, id, 0);
  await Promise.all([advanceWorkflowOnce(deps, id), advanceWorkflowOnce(deps, id), advanceWorkflowOnce(deps, id)]);
  assert.equal((await listJobs(root)).length, 1, "concurrent advance → one stage1 job");
  await advanceWorkflowOnce(deps, id);
  await advanceWorkflowOnce(deps, id);
  assert.equal((await listJobs(root)).length, 1, "repeated advance → still one job");
  // re-recording the predecessor PASSED does not create another next job
  await advanceWorkflowOnce(deps, id);
  assert.equal((await listJobs(root)).length, 1);
  await cleanup(root, ws);
});

// ---- feature flag ----
test("workflowEnabled=false: PASSED+PENDING does NOT submit the next stage; re-enabling submits exactly one", async () => {
  const { root, ws, config, deps, id } = await setup({ enabled: true, n: 2 });
  await forcePassed(root, id, 0);
  const disabled = { ...deps, config: { ...config, workflowEnabled: false } };
  await advanceWorkflowOnce(disabled, id);
  assert.equal(await stateOf(root, id, 1), "PENDING", "disabled must not claim/submit the next stage");
  assert.equal((await listJobs(root)).length, 0);
  await advanceWorkflowOnce(deps, id); // re-enabled
  assert.equal(await stateOf(root, id, 1), "RUNNING");
  assert.equal((await listJobs(root)).length, 1);
  await cleanup(root, ws);
});

test("workflowEnabled=false still settles an already-RUNNING linked stage to its terminal verdict", async () => {
  const { root, ws, config, deps, id } = await setup({ enabled: true, n: 2 });
  await advanceWorkflowOnce(deps, id); // stage0 RUNNING + job
  const jobId = (await readStageAttempt(root, id, "000-s0", 1)).jobId;
  await setJobTerminal(root, jobId, { processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", state: "SUCCEEDED" });
  await advanceWorkflowOnce({ ...deps, config: { ...config, workflowEnabled: false } }, id);
  assert.equal(await stateOf(root, id, 0), "UNVERIFIED", "terminal reconciliation runs regardless of the flag");
  await cleanup(root, ws);
});

// ---- activity-missing frontier (P3-B skeleton) ----
test("a runnable frontier with no activity fails closed WORKFLOW_ACTIVITY_MISSING and is not claimed", async () => {
  const { root, ws, id } = await setup({ n: 2 });
  // strip the activity from stage1's canonical header by rebuilding the workflow with a null activity — simulate
  // a legacy skeleton by directly claiming: make stage0 PASSED, then remove stage1 activity from the header.
  await forcePassed(root, id, 0);
  const { readFile, writeFile } = await import("node:fs/promises");
  // find + null the stage1 activity in the workflow_created journal header
  const jf = path.join(root, "workflows", id, "journal", "000001.json");
  const created = JSON.parse(await readFile(jf, "utf8"));
  created.header.pipeline = created.header.pipeline.map((s) => (s.stageId === "010-s1" ? { ...s, activity: null } : s));
  await writeFile(jf, JSON.stringify(created, null, 2));
  await assert.rejects(() => claimRunnableStage(root, id, { enabled: true }), (e) => e.code === "WORKFLOW_ACTIVITY_MISSING");
  assert.equal(await stateOf(root, id, 1), "PENDING", "activity-less frontier is not claimed");
  await cleanup(root, ws);
});

// ---- isolation ----
test("an invariant-violating workflow is isolated; healthy workflows still advance", async () => {
  const bad = await setup({ n: 2 });
  await transitionStageAttempt(bad.root, bad.id, { stageId: "000-s0", attempt: 1, toState: "SUBMITTING" });
  await transitionStageAttempt(bad.root, bad.id, { stageId: "010-s1", attempt: 1, toState: "SUBMITTING" }); // two active → invariant
  const good = await makeWorkflow(bad.root, bad.ws, 2);
  await reconcileWorkflowsOnce(bad.deps); // must not throw
  assert.equal(await stateOf(bad.root, good.workflowId, 0), "RUNNING", "healthy workflow advanced despite the invalid one");
  await cleanup(bad.root, bad.ws);
});
