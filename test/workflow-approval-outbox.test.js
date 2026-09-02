// P3-E approval-request outbox: an APPROVAL_REQUIRED frontier sends exactly one approval notice via the
// frozen route (idempotent across ticks; retryable on error; bounded summary; separate from the terminal
// notice). No automatic producer creates APPROVAL_REQUIRED, so this drives it via a store fixture.
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { readConfig } from "../dist/core.js";
import { createWorkflow, readApprovalRecord, readWorkflow, transitionStageAttempt } from "../dist/workflow-store.js";
import { reconcileWorkflowsOnce } from "../dist/workflow-reconciler.js";
import { makeFakeStartJob, completeSeams } from "./wf-linkage-helpers.js";

const ROUTE = { routeKind: "channel_root", channel: "slack", to: "C-approve" };
const tmp = (p) => mkdtemp(path.join(os.tmpdir(), p));

async function setup(gatewayCall) {
  const root = await tmp("wf-appr-root-");
  const ws = await tmp("wf-appr-ws-");
  const config = readConfig({ pluginConfig: { workflowEnabled: true, allowedRoots: [ws] } });
  const fake = makeFakeStartJob();
  const id = await createWorkflow(root, {
    name: "review-wf", ownerKey: "ok", requestId: null, payloadFingerprint: "fp",
    parent: { agentId: "a", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null },
    repository: { worktree: ws, branch: null, baseCommit: null, verificationProfile: null },
    deliveryRoute: ROUTE, forbiddenActions: [],
    pipeline: [{ pipelineIndex: 0, stageName: "gate", runnerType: "local", runnerProfile: "local_test", activity: { argv: ["true"] } }],
  });
  // drive the single stage to APPROVAL_REQUIRED
  for (const s of ["SUBMITTING", "RUNNING", "UNVERIFIED", "APPROVAL_REQUIRED"]) {
    await transitionStageAttempt(root, id, { stageId: "000-gate", attempt: 1, toState: s });
  }
  const deps = { rootDir: root, config, gatewayCall, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams(), cancelJob: async () => {}, logger: undefined };
  return { root, ws, id, deps };
}
const cleanup = (root, ws) => Promise.all([rm(root, { recursive: true, force: true }), rm(ws, { recursive: true, force: true })]);

test("APPROVAL_REQUIRED workflow is PAUSED and sends exactly one approval notice (idempotent across ticks)", async () => {
  const sends = [];
  const { root, ws, id, deps } = await setup(async (method, payload) => { if (method === "send") sends.push(payload); return { result: {} }; });
  assert.equal((await readWorkflow(root, id)).workflowState, "PAUSED");
  await reconcileWorkflowsOnce(deps);
  await reconcileWorkflowsOnce(deps);
  await reconcileWorkflowsOnce(deps);
  assert.equal(sends.length, 1, "approval notice sent exactly once across ticks");
  assert.match(sends[0].text, /WORKFLOW_APPROVAL/);
  assert.equal(sends[0].to, "C-approve");
  // bounded: no owner metadata / route internals leaked
  for (const leak of ["ownerKey", "sessionKey", "routeResolvedAt", "requesterOrigin"]) {
    assert.ok(!JSON.stringify(sends[0]).includes(leak));
  }
  assert.equal((await readApprovalRecord(root, id, "000-gate", 1)).status, "SENT");
  await cleanup(root, ws);
});

test("a STRUCTURAL pre-send reject stays retryable (PENDING) and a later tick delivers once", async () => {
  let fail = true;
  const sends = [];
  const { root, ws, id, deps } = await setup(async (method, payload) => {
    if (method === "send") { if (fail) { const e = new Error("rejected before send"); e.code = "PRE_SEND_REJECTED"; throw e; } sends.push(payload); }
    return { result: {} };
  });
  await reconcileWorkflowsOnce(deps); // proven pre-send reject → PENDING (retryable), stage unchanged
  assert.equal((await readApprovalRecord(root, id, "000-gate", 1)).status, "PENDING");
  assert.equal((await readWorkflow(root, id)).workflowState, "PAUSED", "a failed notice never changes the stage state");
  fail = false;
  await reconcileWorkflowsOnce(deps); // now delivers
  await reconcileWorkflowsOnce(deps);
  assert.equal(sends.length, 1);
  assert.equal((await readApprovalRecord(root, id, "000-gate", 1)).status, "SENT");
  await cleanup(root, ws);
});

test("an AMBIGUOUS send error is parked DELIVERY_UNKNOWN and never auto-resent", async () => {
  let calls = 0;
  const { root, ws, id, deps } = await setup(async (method) => {
    if (method === "send") { calls += 1; throw new Error("timeout after write (may have posted)"); }
    return { result: {} };
  });
  await reconcileWorkflowsOnce(deps); // ambiguous (not a proven pre-send reject) → DELIVERY_UNKNOWN
  assert.equal((await readApprovalRecord(root, id, "000-gate", 1)).status, "DELIVERY_UNKNOWN");
  await reconcileWorkflowsOnce(deps);
  await reconcileWorkflowsOnce(deps);
  assert.equal(calls, 1, "no auto-resend after an ambiguous outcome");
  assert.equal((await readWorkflow(root, id)).workflowState, "PAUSED");
  await cleanup(root, ws);
});

test("a stale SENDING lease is parked DELIVERY_UNKNOWN (never blind-resent)", async () => {
  const { root, ws, id, deps } = await setup(async () => ({ result: {} }));
  const { claimApprovalSend } = await import("../dist/workflow-store.js");
  await claimApprovalSend(root, id, "000-gate", 1); // claim SENDING but never mark sent
  // a fresh claim with a 0ms lease sees the SENDING as stale → parks DELIVERY_UNKNOWN
  const { record } = await claimApprovalSend(root, id, "000-gate", 1, { leaseMs: 0 });
  assert.equal(record.status, "DELIVERY_UNKNOWN");
  await cleanup(root, ws);
});

test("after the stage is approved, no further approval notice is sent", async () => {
  const sends = [];
  const { root, ws, id, deps } = await setup(async (method, payload) => { if (method === "send") sends.push(payload); return { result: {} }; });
  await reconcileWorkflowsOnce(deps);
  assert.equal(sends.length, 1);
  // approve the stage (store transition) → frontier moves off APPROVAL_REQUIRED
  await transitionStageAttempt(root, id, { stageId: "000-gate", attempt: 1, toState: "PASSED" });
  await reconcileWorkflowsOnce(deps);
  await reconcileWorkflowsOnce(deps);
  assert.equal(sends.length, 1, "no approval notice after the stage leaves APPROVAL_REQUIRED");
  await cleanup(root, ws);
});
