// P3-B/P3-C workflow tool surface (service layer) tests: feature flag, activity+start validation,
// parent/route freeze, first-stage submission, creation idempotency, status/list authorization, public
// response shaping. index.js imports the host-only `openclaw` SDK and is not unit-importable here, so the
// feature-flag DECISION is tested via readConfig and all behavior via the service layer. A fake startJob
// (test/wf-linkage-helpers.js) persists a real job row without spawning a worker.
import assert from "node:assert/strict";
import { mkdtemp, rm, readdir, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { readConfig } from "../dist/core.js";
import { listJobs } from "../dist/job-store.js";
import { listWorkflows, readWorkflow } from "../dist/workflow-store.js";
import { startWorkflow, statusWorkflow, listWorkflowSummaries, normalizeStartInput, runWorkflowAction } from "../dist/workflow-service.js";
import { makeFakeStartJob, completeSeams } from "./wf-linkage-helpers.js";

async function tmpDir(prefix) {
  return mkdtemp(path.join(os.tmpdir(), prefix));
}

const okGateway = async (method) => {
  if (method === "chat.history") {
    return { result: { sessionInfo: { origin: { provider: "slack", to: "C-slack", chatType: "channel" } } } };
  }
  throw new Error(`unexpected gateway call: ${method}`);
};
const noRouteGateway = async () => ({ result: { sessionInfo: {} } });

function trustedCtx(overrides = {}) {
  return { agentId: "agent-1", sessionKey: "claude-queue-test", sessionId: "sess-1", workspaceDir: overrides.workspaceDir, ...overrides };
}

// local stages: under P3-F, a model stage would fail-closed at preflight (no non-quota READY probe); these
// ownership/idempotency tests only need an executable first stage, so both stages are local.
const PIPELINE = [
  { name: "implementation", runnerType: "local", runnerProfile: "local_test", activity: { argv: ["true"], timeoutSeconds: 60 } },
  { name: "full_unittest", runnerType: "local", runnerProfile: "local_test", activity: { argv: ["true"] } },
];

function depsFor(root, config) {
  const fake = makeFakeStartJob();
  return { deps: { rootDir: root, config, gatewayCall: okGateway, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams() }, fake };
}

async function trustedSetup() {
  const root = await tmpDir("wf-svc-root-");
  const ws = await tmpDir("wf-svc-ws-");
  const config = readConfig({ pluginConfig: { workflowEnabled: true, allowedRoots: [ws] } });
  const ctx = trustedCtx({ workspaceDir: ws });
  const { deps, fake } = depsFor(root, config);
  return { root, ws, config, ctx, deps, fake };
}

function startParams(overrides = {}) {
  return { action: "start", name: "infra-batch-workflow", worktree: overrides.worktree, pipeline: PIPELINE, forbiddenActions: ["push", "tag_change"], ...overrides };
}

// ---- feature flag ----
test("workflowEnabled defaults false and honors true", () => {
  assert.equal(readConfig({ pluginConfig: { stateSubdir: "durable-jobs" } }).workflowEnabled, false);
  assert.equal(readConfig({ pluginConfig: { workflowEnabled: true } }).workflowEnabled, true);
  assert.equal(readConfig({ pluginConfig: { workflowEnabled: "yes" } }).workflowEnabled, false);
});

test("runWorkflowAction fails closed with WORKFLOW_DISABLED when the flag is off (creates nothing)", async () => {
  const { root, ws, config, ctx } = await trustedSetup();
  const disabledDeps = { rootDir: root, config: { ...config, workflowEnabled: false }, gatewayCall: okGateway };
  for (const action of [
    { action: "start", name: "x", worktree: ws, pipeline: PIPELINE },
    { action: "status", workflowId: "wf-00000000-0000-0000-0000-000000000000" },
    { action: "list" },
  ]) {
    await assert.rejects(() => runWorkflowAction(disabledDeps, ctx, action), (e) => e.code === "WORKFLOW_DISABLED");
  }
  assert.equal((await listWorkflows(root)).length, 0);
  assert.equal((await listJobs(root)).length, 0);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

// ---- start: happy paths + first-stage submission ----
test("trusted start submits the first stage as exactly ONE linked durable job (outside the workflow lock)", async () => {
  const { root, ws, deps, ctx, fake } = await trustedSetup();
  const res = await startWorkflow(deps, ctx, startParams({ worktree: ws }));
  assert.match(res.workflowId, /^wf-/);
  assert.equal(res.workflowState, "RUNNING");
  const first = res.stages[0];
  assert.equal(first.stageId, "000-implementation");
  assert.equal(first.stageState, "RUNNING");
  assert.ok(first.jobId, "first stage must be linked to a jobId");
  assert.equal(res.stages[1].stageState, "PENDING"); // second stage untouched (no auto-advance)

  const jobs = await listJobs(root);
  assert.equal(jobs.length, 1, "exactly one durable job (first stage) is submitted");
  const job = jobs[0];
  assert.equal(job.workflowLink.workflowId, res.workflowId);
  assert.equal(job.workflowLink.stageId, "000-implementation");
  assert.equal(job.workflowLink.attempt, 1);
  assert.equal(job.workflowLink.activityIdempotencyKey, `wf:${res.workflowId}:stage:000-implementation:attempt:1`);
  const worktree = res.repository.worktree; // realpath of ws (macOS /var → /private/var)
  assert.equal(job.cwd, worktree, "job cwd forced to the workflow worktree");

  // startJob was invoked once, session-free (no TaskFlow), reusing the frozen route, OUTSIDE the wf lock.
  assert.equal(fake.calls.length, 1);
  assert.equal(fake.calls[0].ctx.sessionKey, null);
  assert.equal(fake.calls[0].params.deliveryRoute.to, "C-slack");
  assert.equal(fake.calls[0].params.cwd, worktree);
  assert.equal(fake.wfLockHeldDuringCall, false, "startJob must run outside the workflow lock");
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("context-free configured-owner start submits the first stage via the frozen owner route (no gateway)", async () => {
  const root = await tmpDir("wf-svc-root-");
  const ws = await tmpDir("wf-svc-ws-");
  const config = readConfig({
    pluginConfig: {
      workflowEnabled: true,
      owners: [{ agentId: "agent-cf", workspaceDir: ws, allowedRoots: [ws], deliveryRoute: { channel: "slack", routeKind: "channel_root", to: "C-owner" } }],
    },
  });
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, config, gatewayCall: async () => { throw new Error("gateway must not be called context-free"); }, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams() };
  const res = await startWorkflow(deps, {}, startParams({ worktree: ws }));
  assert.equal(res.stages[0].stageState, "RUNNING");
  assert.equal((await listJobs(root)).length, 1);
  assert.equal(fake.calls[0].params.deliveryRoute.to, "C-owner");
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("trusted start creates the workflow + freezes parent, deferring the delivery route, when chat.history needs gateway credentials", async () => {
  const { root, ws, config, ctx, fake } = await trustedSetup();
  const credsGateway = async (method) => {
    if (method === "chat.history") {
      const err = new Error("gateway chat.history requires credentials before opening a websocket");
      err.name = "GatewayCredentialsRequiredError";
      throw err;
    }
    throw new Error(`unexpected gateway call: ${method}`);
  };
  const deps = {
    rootDir: root,
    config,
    gatewayCall: credsGateway,
    startJob: fake.startJob,
    startDeps: { rootDir: root },
    ...completeSeams(),
  };
  const res = await startWorkflow(deps, ctx, startParams({ worktree: ws }));
  assert.match(res.workflowId, /^wf-/, "workflow created despite an unreachable gateway (no chat.history credentials)");
  assert.equal((await listWorkflows(root)).length, 1);
  const wf = await readWorkflow(root, res.workflowId);
  assert.equal(wf.deliveryRoute, null, "delivery route deferred to null");
  assert.equal(wf.parent.agentId, "agent-1", "parent agentId still frozen from the trusted context");
  assert.equal(wf.parent.sessionKey, "claude-queue-test", "parent sessionKey still frozen from the trusted context");
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("trusted start defers the delivery route (workflow still created) when the chat.history gateway call times out", async () => {
  const { root, ws, config, ctx, fake } = await trustedSetup();
  const timeoutGateway = async (method) => {
    if (method === "chat.history") {
      throw new Error("Gateway call timed out after 15000ms");
    }
    throw new Error(`unexpected gateway call: ${method}`);
  };
  const deps = {
    rootDir: root,
    config,
    gatewayCall: timeoutGateway,
    startJob: fake.startJob,
    startDeps: { rootDir: root },
    ...completeSeams(),
  };
  const res = await startWorkflow(deps, ctx, startParams({ worktree: ws }));
  assert.match(res.workflowId, /^wf-/, "workflow created despite a delivery-route gateway timeout");
  assert.equal((await listWorkflows(root)).length, 1);
  const wf = await readWorkflow(root, res.workflowId);
  assert.equal(wf.deliveryRoute, null, "delivery route deferred to null on timeout");
  assert.equal(wf.parent.agentId, "agent-1");
  assert.equal(wf.parent.sessionKey, "claude-queue-test");
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("route-freeze failure leaves ZERO workflows and ZERO jobs on disk", async () => {
  const { root, ws, config, ctx } = await trustedSetup();
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, config, gatewayCall: noRouteGateway, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams() };
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws })), /DELIVERY_ROUTE_UNAVAILABLE/);
  assert.equal((await listWorkflows(root)).length, 0);
  assert.equal((await listJobs(root)).length, 0);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

// ---- activity + start validation ----
test("start input validation (activity required + argv/runner rules)", async () => {
  const { root, ws, deps, ctx } = await trustedSetup();
  const outside = await tmpDir("wf-svc-outside-");
  const withAct = (stages) => stages.map((s) => ({ activity: { argv: ["true"] }, ...s }));
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: outside })), /outside allowed roots/);
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws, pipeline: [] })), /WORKFLOW_INPUT_INVALID/);
  // missing activity on an executable stage
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws, pipeline: [{ name: "impl", runnerType: "local" }] })), /requires an activity/);
  // empty argv
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws, pipeline: [{ name: "impl", runnerType: "local", activity: { argv: [] } }] })), /WORKFLOW_INPUT_INVALID/);
  // shell string instead of argv object
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws, pipeline: [{ name: "impl", runnerType: "local", activity: "rm -rf /" }] })), /WORKFLOW_INPUT_INVALID/);
  // non-string argv item
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws, pipeline: [{ name: "impl", runnerType: "local", activity: { argv: ["ok", 5] } }] })), /WORKFLOW_INPUT_INVALID/);
  // caller cwd injection (unsupported activity field)
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws, pipeline: [{ name: "impl", runnerType: "local", activity: { argv: ["true"], cwd: "/etc" } }] })), /WORKFLOW_INPUT_INVALID/);
  // duplicate stage name
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws, pipeline: withAct([{ name: "a", runnerType: "local" }, { name: "a", runnerType: "local" }]) })), /duplicate stage name/);
  // unsafe stage name
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws, pipeline: withAct([{ name: "../evil", runnerType: "local" }]) })), /unsafe stage name/);
  // runner/profile/executable mismatch: model_agy needs the agy executable
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws, pipeline: [{ name: "impl", runnerProfile: "model_agy", activity: { argv: ["node", "x"] } }] })), /WORKFLOW_INPUT_INVALID/);
  // agy cannot be downgraded to a local runner
  await assert.rejects(() => startWorkflow(deps, ctx, startParams({ worktree: ws, pipeline: [{ name: "impl", runnerType: "local", activity: { argv: ["agy", "x"] } }] })), /WORKFLOW_INPUT_INVALID/);
  assert.equal((await listWorkflows(root)).length, 0, "no workflow created on any rejected start");
  assert.equal((await listJobs(root)).length, 0);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
  await rm(outside, { recursive: true, force: true });
});

test("normalizeStartInput validates the argv activity, derives the runner, and assigns pipeline indices", () => {
  const n = normalizeStartInput({ name: "w", worktree: "/x", pipeline: [
    { name: "impl", runnerProfile: "model_agy", activity: { argv: ["agy", "go"], timeoutSeconds: 30 } },
    { name: "test", runnerType: "local", activity: { argv: ["true"] } },
  ] });
  assert.equal(n.pipeline[0].stageName, "impl");
  assert.equal(n.pipeline[0].pipelineIndex, 0);
  assert.equal(n.pipeline[0].runnerType, "model");
  assert.equal(n.pipeline[0].runnerProfile, "model_agy");
  assert.deepEqual(n.pipeline[0].activity, { argv: ["agy", "go"], timeoutSeconds: 30 });
  assert.ok(n.pipeline[0].candidateId); // P3-F candidate identity
  assert.deepEqual(n.pipeline[0].fallbacks, []);
  assert.equal(n.pipeline[1].runnerProfile, "generic_local");
});

// ---- idempotency ----
test("same owner + requestId + payload → same workflow and ONE job (no duplicate)", async () => {
  const { root, ws, deps, ctx } = await trustedSetup();
  const p = startParams({ worktree: ws, requestId: "req-1" });
  const a = await startWorkflow(deps, ctx, p);
  const b = await startWorkflow(deps, ctx, p);
  assert.equal(a.workflowId, b.workflowId);
  assert.equal(b.reused, true);
  assert.equal((await listWorkflows(root)).length, 1);
  assert.equal((await listJobs(root)).length, 1, "reused start must not create a second job");
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("same owner + requestId + DIFFERENT activity payload → WORKFLOW_REQUEST_CONFLICT", async () => {
  const { root, ws, deps, ctx } = await trustedSetup();
  await startWorkflow(deps, ctx, startParams({ worktree: ws, requestId: "req-2" }));
  const otherPipeline = [{ name: "implementation", runnerProfile: "model_agy", activity: { argv: ["agy", "DIFFERENT"] } }, { name: "full_unittest", runnerType: "local", activity: { argv: ["true"] } }];
  await assert.rejects(
    () => startWorkflow(deps, ctx, startParams({ worktree: ws, requestId: "req-2", pipeline: otherPipeline })),
    /WORKFLOW_REQUEST_CONFLICT/,
  );
  assert.equal((await listWorkflows(root)).length, 1);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("concurrent duplicate start (same requestId) → ONE workflow and ONE job", async () => {
  const { root, ws, deps, ctx } = await trustedSetup();
  const p = startParams({ worktree: ws, requestId: "req-race" });
  const [a, b] = await Promise.all([startWorkflow(deps, ctx, p), startWorkflow(deps, ctx, p)]);
  assert.equal(a.workflowId, b.workflowId);
  assert.equal((await listWorkflows(root)).length, 1);
  assert.equal((await listJobs(root)).length, 1, "concurrent duplicate start must dedup the linked job");
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("different owners may reuse the same requestId (separate workflows + jobs)", async () => {
  const { root, ws, config } = await trustedSetup();
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, config, gatewayCall: okGateway, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams() };
  const ctxA = trustedCtx({ agentId: "agent-A", sessionKey: "sess-A", workspaceDir: ws });
  const ctxB = trustedCtx({ agentId: "agent-B", sessionKey: "sess-B", workspaceDir: ws });
  const a = await startWorkflow(deps, ctxA, startParams({ worktree: ws, requestId: "shared" }));
  const b = await startWorkflow(deps, ctxB, startParams({ worktree: ws, requestId: "shared" }));
  assert.notEqual(a.workflowId, b.workflowId);
  assert.equal((await listWorkflows(root)).length, 2);
  assert.equal((await listJobs(root)).length, 2);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

// ---- status / list authorization + recovery ----
test("status reconciles a corrupt projection back from canonical", async () => {
  const { root, ws, deps, ctx } = await trustedSetup();
  const started = await startWorkflow(deps, ctx, startParams({ worktree: ws }));
  const wfFile = path.join(root, "workflows", started.workflowId, "workflow.json");
  const wf = JSON.parse(await readFile(wfFile, "utf8"));
  wf.currentStage = "999-bogus"; wf.completedStages = ["000-implementation"]; wf.workflowState = "SUCCEEDED";
  await writeFile(wfFile, JSON.stringify(wf));
  const status = await statusWorkflow(deps, ctx, { action: "status", workflowId: started.workflowId });
  assert.equal(status.workflowState, "RUNNING");
  assert.equal(status.currentStage, "000-implementation");
  assert.deepEqual(status.completedStages, []);
  // status stage summary exposes linkage outcome fields
  assert.ok(status.stages[0].jobId);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("status by a different session is rejected", async () => {
  const { root, ws, deps, ctx } = await trustedSetup();
  const started = await startWorkflow(deps, ctx, startParams({ worktree: ws }));
  const otherCtx = trustedCtx({ agentId: "agent-1", sessionKey: "someone-else", workspaceDir: ws });
  await assert.rejects(() => statusWorkflow(deps, otherCtx, { action: "status", workflowId: started.workflowId }), /WORKFLOW_FORBIDDEN/);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("list is owner-scoped, filterable, ordered, and limited", async () => {
  const { root, ws, config } = await trustedSetup();
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, config, gatewayCall: okGateway, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams() };
  const ctxA = trustedCtx({ agentId: "agent-A", sessionKey: "sess-A", workspaceDir: ws });
  const ctxB = trustedCtx({ agentId: "agent-B", sessionKey: "sess-B", workspaceDir: ws });
  const w1 = await startWorkflow(deps, ctxA, startParams({ worktree: ws }));
  await new Promise((r) => setTimeout(r, 5));
  const w2 = await startWorkflow(deps, ctxA, startParams({ worktree: ws }));
  await startWorkflow(deps, ctxB, startParams({ worktree: ws }));
  const listA = await listWorkflowSummaries(deps, ctxA, { action: "list" });
  assert.deepEqual(listA.map((w) => w.workflowId), [w2.workflowId, w1.workflowId]);
  assert.equal((await listWorkflowSummaries(deps, ctxA, { action: "list", state: "SUCCEEDED" })).length, 0);
  assert.equal((await listWorkflowSummaries(deps, ctxA, { action: "list", state: "RUNNING" })).length, 2);
  assert.equal((await listWorkflowSummaries(deps, ctxA, { action: "list", limit: 1 })).length, 1);
  assert.equal((await listWorkflowSummaries(deps, ctxB, { action: "list" })).length, 1);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("a corrupt workflow.json is isolated — list still returns the healthy ones", async () => {
  const { root, ws, deps, ctx } = await trustedSetup();
  const good = await startWorkflow(deps, ctx, startParams({ worktree: ws }));
  const bad = await startWorkflow(deps, ctx, startParams({ worktree: ws }));
  await writeFile(path.join(root, "workflows", bad.workflowId, "workflow.json"), "{ not valid json");
  const list = await listWorkflowSummaries(deps, ctx, { action: "list" });
  assert.deepEqual(list.map((w) => w.workflowId), [good.workflowId]);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("public response excludes the frozen route, requesterOrigin, ownerKey, and activity idempotency key", async () => {
  const { root, ws, deps, ctx } = await trustedSetup();
  const res = await startWorkflow(deps, ctx, startParams({ worktree: ws, requestId: "r" }));
  const status = await statusWorkflow(deps, ctx, { action: "status", workflowId: res.workflowId });
  for (const shaped of [res, status]) {
    const blob = JSON.stringify(shaped);
    for (const leak of ["deliveryRoute", "ownerKey", "requesterOrigin", "routeResolvedAt", "C-slack", "payloadFingerprint", "activityIdempotencyKey", "wf:"]) {
      assert.ok(!blob.includes(leak), `public response leaked "${leak}"`);
    }
  }
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});
