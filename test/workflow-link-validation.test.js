// P3-C boundary 1: strict workflowLink validation + pre-frozen deliveryRoute gate in startJob.
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import test from "node:test";

import { readConfig, startJob, validateWorkflowLink, validateFrozenDeliveryRoute } from "../dist/core.js";
import { readJob } from "../dist/job-store.js";

const WF = `wf-${randomUUID()}`; // v4 uuid
const goodLink = (wf = WF, stage = "000-impl", attempt = 1) => ({
  workflowId: wf, stageId: stage, attempt, activityIdempotencyKey: `wf:${wf}:stage:${stage}:attempt:${attempt}`,
});
const goodRoute = { routeKind: "channel_root", channel: "slack", to: "C1" };

// ---- validateWorkflowLink ----
test("validateWorkflowLink accepts a well-formed link and returns null for absent", () => {
  assert.equal(validateWorkflowLink(undefined), null);
  assert.equal(validateWorkflowLink(null), null);
  assert.deepEqual(validateWorkflowLink(goodLink()), goodLink());
});

test("validateWorkflowLink fail-closes on malformed id / unsafe stage / bad attempt / key mismatch", () => {
  const bad = (o) => ({ ...goodLink(), ...o });
  assert.throws(() => validateWorkflowLink(bad({ workflowId: "wf-not-a-uuid" })), /WORKFLOW_LINK_INVALID/);
  assert.throws(() => validateWorkflowLink(bad({ workflowId: "job-1234" })), /WORKFLOW_LINK_INVALID/);
  assert.throws(() => validateWorkflowLink(bad({ stageId: "../evil" })), /WORKFLOW_LINK_INVALID/);
  assert.throws(() => validateWorkflowLink(bad({ stageId: "impl" })), /WORKFLOW_LINK_INVALID/); // missing NNN- prefix
  assert.throws(() => validateWorkflowLink(bad({ attempt: 0 })), /WORKFLOW_LINK_INVALID/);
  assert.throws(() => validateWorkflowLink(bad({ attempt: 1.5 })), /WORKFLOW_LINK_INVALID/);
  // activityIdempotencyKey must EXACTLY equal the deterministic key derived from the ids
  assert.throws(() => validateWorkflowLink(bad({ activityIdempotencyKey: "wf:other:stage:000-impl:attempt:1" })), /WORKFLOW_LINK_INVALID/);
  assert.throws(() => validateWorkflowLink(bad({ activityIdempotencyKey: `wf:${WF}:stage:000-impl:attempt:2` })), /WORKFLOW_LINK_INVALID/);
  assert.throws(() => validateWorkflowLink("not-an-object"), /WORKFLOW_LINK_INVALID/);
});

test("validateFrozenDeliveryRoute accepts a frozen route and rejects malformed", () => {
  assert.equal(validateFrozenDeliveryRoute(goodRoute), goodRoute);
  assert.equal(validateFrozenDeliveryRoute({ routeKind: "thread", channel: "slack", to: "C1", threadId: "t" }).routeKind, "thread");
  assert.throws(() => validateFrozenDeliveryRoute(null), /WORKFLOW_LINK_INVALID/);
  assert.throws(() => validateFrozenDeliveryRoute({ routeKind: "unknown", channel: "slack", to: "C1" }), /WORKFLOW_LINK_INVALID/);
  assert.throws(() => validateFrozenDeliveryRoute({ routeKind: "channel_root", channel: "slack" }), /WORKFLOW_LINK_INVALID/); // no `to`
  assert.throws(() => validateFrozenDeliveryRoute({ routeKind: "channel_root", to: "C1" }), /WORKFLOW_LINK_INVALID/); // no channel
});

// ---- startJob pre-frozen route gate ----
async function jobEnv() {
  const root = await mkdtemp(path.join(os.tmpdir(), "wf-link-root-"));
  const cwd = await mkdtemp(path.join(os.tmpdir(), "wf-link-cwd-"));
  const config = readConfig({ pluginConfig: { allowedRoots: [cwd] } });
  const ctx = { agentId: "a", sessionKey: null, workspaceDir: cwd, durableAllowedRoots: [cwd], ownerDeliveryRoute: null };
  let spawned = 0;
  const deps = {
    rootDir: root, config,
    gatewayCall: async () => { throw new Error("gateway must not be called with a pre-frozen route"); },
    createFlow: () => ({ flowId: null }),
    spawnWorker: () => { spawned += 1; return 4242; },
  };
  return { root, cwd, config, ctx, deps, spawned: () => spawned };
}

test("startJob rejects a pre-frozen deliveryRoute WITHOUT a workflowLink (no side effects)", async () => {
  const { root, cwd, ctx, deps, spawned } = await jobEnv();
  await assert.rejects(
    () => startJob(deps, ctx, { name: "n", command: ["true"], cwd, deliveryRoute: goodRoute }),
    /WORKFLOW_LINK_INVALID/,
  );
  assert.equal(spawned(), 0, "no worker spawned on a rejected start");
  await rm(root, { recursive: true, force: true });
  await rm(cwd, { recursive: true, force: true });
});

test("startJob rejects a malformed pre-frozen route even WITH a valid workflowLink", async () => {
  const { root, cwd, ctx, deps, spawned } = await jobEnv();
  await assert.rejects(
    () => startJob(deps, ctx, { name: "n", command: ["true"], cwd, workflowLink: goodLink(), deliveryRoute: { routeKind: "bogus", to: "C1" } }),
    /WORKFLOW_LINK_INVALID/,
  );
  assert.equal(spawned(), 0);
  await rm(root, { recursive: true, force: true });
  await rm(cwd, { recursive: true, force: true });
});

test("startJob accepts a valid workflowLink + frozen route, reuses the route, and stores the linkage", async () => {
  const { root, cwd, ctx, deps } = await jobEnv();
  const link = goodLink();
  const res = await startJob(deps, ctx, { name: "linked", command: ["true"], cwd, workflowLink: link, deliveryRoute: goodRoute });
  const job = await readJob(root, res.id);
  assert.deepEqual(job.workflowLink, link);
  assert.equal(job.deliveryRoute.to, "C1"); // frozen route reused verbatim (no gateway call)
  assert.equal(job.flowId, null); // session-free → no TaskFlow
  await rm(root, { recursive: true, force: true });
  await rm(cwd, { recursive: true, force: true });
});

test("standalone startJob (no workflowLink) still resolves its route via the trusted session — no regression", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "wf-link-root-"));
  const cwd = await mkdtemp(path.join(os.tmpdir(), "wf-link-cwd-"));
  const config = readConfig({ pluginConfig: { allowedRoots: [cwd] } });
  const ctx = { agentId: "a", sessionKey: "sess-1", sessionId: "s", workspaceDir: cwd, durableAllowedRoots: [cwd], deliveryContext: null };
  const gatewayCall = async (method) => {
    if (method === "chat.history") return { result: { sessionInfo: { origin: { provider: "slack", to: "C-live", chatType: "channel" } } } };
    throw new Error(`unexpected ${method}`);
  };
  const deps = { rootDir: root, config, gatewayCall, createFlow: () => ({ flowId: "flow-1" }), spawnWorker: () => 7 };
  const res = await startJob(deps, ctx, { name: "solo", command: ["true"], cwd });
  const job = await readJob(root, res.id);
  assert.equal(job.workflowLink ?? null, null);
  assert.equal(job.deliveryRoute.to, "C-live"); // resolved from the live session, not injected
  await rm(root, { recursive: true, force: true });
  await rm(cwd, { recursive: true, force: true });
});

// ---- #4 validatedExecution strict validation (startJob, before any side effect) ----
const SHA = "a".repeat(64);
const veFor = (cwd, over = {}) => ({ worktree: cwd, worktreeAggregateHash: SHA, fingerprint: { maxFiles: 10, maxBytes: 100, timeoutMs: 1000 }, toolchain: { executableRealpath: "/bin/sh", executableBasename: "sh", executableContentHash: SHA, executableSize: 42, aggregateHash: SHA }, ...over });

test("startJob rejects validatedExecution WITHOUT a workflowLink (standalone injection, no side effects)", async () => {
  const { root, cwd, ctx, deps, spawned } = await jobEnv();
  await assert.rejects(() => startJob(deps, ctx, { name: "n", command: ["true"], cwd, validatedExecution: veFor(cwd) }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/);
  assert.equal(spawned(), 0, "no worker spawned on a rejected start");
  await rm(root, { recursive: true, force: true }); await rm(cwd, { recursive: true, force: true });
});

test("startJob rejects malformed validatedExecution (hash / path / size / extra field / cwd / runner mismatch)", async () => {
  const { root, cwd, ctx, deps, spawned } = await jobEnv();
  const base = { name: "n", command: ["true"], cwd, workflowLink: goodLink(), deliveryRoute: goodRoute };
  const tc = veFor(cwd).toolchain;
  const bad = (over) => startJob(deps, ctx, { ...base, validatedExecution: veFor(cwd, over) });
  await assert.rejects(() => bad({ toolchain: { ...tc, aggregateHash: "NOTHEX".repeat(4) } }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "bad hash");
  await assert.rejects(() => bad({ toolchain: { ...tc, aggregateHash: SHA.toUpperCase() } }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "uppercase hash");
  await assert.rejects(() => bad({ toolchain: { ...tc, executableRealpath: "relative/x" } }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "not absolute");
  await assert.rejects(() => bad({ toolchain: { ...tc, executableSize: -1 } }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "negative size");
  await assert.rejects(() => bad({ toolchain: { ...tc, executableSize: 1.5 } }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "non-integer size");
  await assert.rejects(() => bad({ toolchain: { ...tc, sneaky: 1 } }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "extra toolchain field");
  await assert.rejects(() => bad({ evil: 1 }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "extra top field");
  await assert.rejects(() => bad({ worktree: "/somewhere/else" }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "cwd/worktree mismatch");
  await assert.rejects(() => bad({ toolchain: { ...tc, runnerType: "model" } }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "runner mismatch");
  await assert.rejects(() => bad({ fingerprint: { maxFiles: -5 } }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "unbounded fingerprint");
  await assert.rejects(() => startJob(deps, ctx, { ...base, validatedExecution: { worktree: cwd } }), /WORKFLOW_VALIDATED_EXECUTION_INVALID/, "no verifiable section");
  assert.equal(spawned(), 0, "no worker spawned on any rejected start");
  await rm(root, { recursive: true, force: true }); await rm(cwd, { recursive: true, force: true });
});

test("startJob accepts valid workflow-linked validatedExecution; stored but NOT exposed on publicJob", async () => {
  const { root, cwd, ctx, deps, spawned } = await jobEnv();
  const created = await startJob(deps, ctx, { name: "n", command: ["true"], cwd, workflowLink: goodLink(), deliveryRoute: goodRoute, validatedExecution: veFor(cwd) });
  assert.equal(spawned(), 1, "worker spawned on accept");
  assert.equal(created.validatedExecution, undefined, "publicJob must not expose validatedExecution");
  const stored = await readJob(root, created.id);
  assert.equal(stored.validatedExecution.toolchain.aggregateHash, SHA, "stored on the job row");
  await rm(root, { recursive: true, force: true }); await rm(cwd, { recursive: true, force: true });
});
