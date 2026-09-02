// P3-C workflow-activity unit tests: activity normalization, deterministic idempotency key, linked-job
// dedup (reservation-before-spawn), and the conservative terminal → stage-state mapping.
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { listJobs } from "../dist/job-store.js";
import {
  ensureLinkedJob,
  findLinkedJob,
  makeActivityIdempotencyKey,
  mapJobToStageState,
  normalizeActivity,
} from "../dist/workflow-activity.js";
import { makeFakeStartJob } from "./wf-linkage-helpers.js";

async function tmpDir() {
  return mkdtemp(path.join(os.tmpdir(), "wf-act-"));
}

test("normalizeActivity accepts a valid argv activity and bounded timeout", () => {
  assert.deepEqual(normalizeActivity({ argv: ["agy", "go"] }), { argv: ["agy", "go"] });
  assert.deepEqual(normalizeActivity({ argv: ["true"], timeoutSeconds: 60 }), { argv: ["true"], timeoutSeconds: 60 });
});

test("normalizeActivity rejects shell strings, empty/non-string argv, unknown fields, bad timeout", () => {
  assert.throws(() => normalizeActivity("rm -rf /"), /WORKFLOW_ACTIVITY_INVALID/); // shell string
  assert.throws(() => normalizeActivity({ argv: [] }), /WORKFLOW_ACTIVITY_INVALID/); // empty
  assert.throws(() => normalizeActivity({ argv: ["ok", 5] }), /WORKFLOW_ACTIVITY_INVALID/); // non-string
  assert.throws(() => normalizeActivity({ argv: ["true"], cwd: "/etc" }), /WORKFLOW_ACTIVITY_INVALID/); // cwd injection
  assert.throws(() => normalizeActivity({ argv: ["true"], env: { X: "1" } }), /WORKFLOW_ACTIVITY_INVALID/); // env injection
  assert.throws(() => normalizeActivity({ argv: ["true"], timeoutSeconds: -1 }), /WORKFLOW_ACTIVITY_INVALID/);
  assert.throws(() => normalizeActivity({ argv: ["true"], timeoutSeconds: 999999999 }), /WORKFLOW_ACTIVITY_INVALID/);
});

test("makeActivityIdempotencyKey is deterministic and distinct per workflow/stage/attempt", () => {
  const k = makeActivityIdempotencyKey("wf-1", "000-impl", 1);
  assert.equal(k, "wf:wf-1:stage:000-impl:attempt:1");
  assert.equal(makeActivityIdempotencyKey("wf-1", "000-impl", 1), k); // deterministic
  assert.notEqual(k, makeActivityIdempotencyKey("wf-1", "000-impl", 2));
  assert.notEqual(k, makeActivityIdempotencyKey("wf-2", "000-impl", 1));
  assert.notEqual(k, makeActivityIdempotencyKey("wf-1", "010-test", 1));
});

test("mapJobToStageState: conservative, never auto-PASSES", () => {
  const j = (o) => ({ id: "job-x", state: "SUCCEEDED", processState: "COMPLETED", providerState: null, jobOutcome: null, ...o });
  // non-terminal → RUNNING
  assert.equal(mapJobToStageState({ id: "j", state: "RUNNING", jobOutcome: null }).state, "RUNNING");
  assert.equal(mapJobToStageState({ id: "j", state: "QUEUED", jobOutcome: null }).state, "RUNNING");
  // COMPLETED + provider OK → COMPLETED_UNVERIFIED → UNVERIFIED (NOT passed)
  assert.equal(mapJobToStageState(j({ providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED" })).state, "UNVERIFIED");
  // command/process failure → FAILED
  assert.equal(mapJobToStageState(j({ state: "TIMED_OUT", processState: "TIMED_OUT", jobOutcome: "FAILED_COMMAND" })).state, "FAILED");
  assert.equal(mapJobToStageState(j({ state: "CANCELLED", processState: "CANCELLED", jobOutcome: "CANCELLED" })).state, "CANCELLED"); // cancelled job → stage CANCELLED (P3-E)
  // retryable provider dependency → BLOCKED_DEPENDENCY
  assert.equal(mapJobToStageState(j({ providerState: "RATE_LIMITED", jobOutcome: "FAILED_PROVIDER" })).state, "BLOCKED_DEPENDENCY");
  assert.equal(mapJobToStageState(j({ providerState: "BLOCKED_QUOTA", jobOutcome: "FAILED_PROVIDER" })).state, "BLOCKED_DEPENDENCY");
  // non-retryable provider/auth/context/internal → FAILED
  assert.equal(mapJobToStageState(j({ providerState: "AUTH_FAILED", jobOutcome: "FAILED_PROVIDER" })).state, "FAILED");
  assert.equal(mapJobToStageState(j({ providerState: "CONTEXT_LIMIT", jobOutcome: "FAILED_PROVIDER" })).state, "FAILED");
  assert.equal(mapJobToStageState(j({ providerState: "ERROR_UNCLASSIFIED", jobOutcome: "FAILED_PROVIDER" })).state, "FAILED");
  // terminal but unmapped (legacy row, no outcome) → fail closed to FAILED (never guessed PASSED)
  assert.equal(mapJobToStageState({ id: "j", state: "FAILED", jobOutcome: undefined }).state, "FAILED");
  // the mapping carries the outcome fields into the stage attempt
  const m = mapJobToStageState(j({ providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", endedAt: "t" }));
  assert.equal(m.mutation.jobId, "job-x");
  assert.equal(m.mutation.jobOutcome, "COMPLETED_UNVERIFIED");
});

const wf = (worktree) => ({
  workflowId: "wf-11111111-1111-1111-1111-111111111111",
  name: "w",
  parent: { agentId: "a", sessionKey: null, sessionId: null, requesterOrigin: null },
  repository: { worktree },
  deliveryRoute: { routeKind: "channel_root", channel: "slack", to: "C1" },
});
const stageSpec = { stageId: "000-impl", pipelineIndex: 0, runnerType: "local", runnerProfile: "generic_local", activity: { argv: ["true"] } };

test("ensureLinkedJob submits at most one job per key (dedup on repeat + concurrent)", async () => {
  const root = await tmpDir();
  const ws = await tmpDir();
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, startJob: fake.startJob, startDeps: { rootDir: root } };
  const key = makeActivityIdempotencyKey(wf(ws).workflowId, "000-impl", 1);
  const a = await ensureLinkedJob(deps, { workflow: wf(ws), stageSpec, stageId: "000-impl", attempt: 1, activityIdempotencyKey: key });
  const b = await ensureLinkedJob(deps, { workflow: wf(ws), stageSpec, stageId: "000-impl", attempt: 1, activityIdempotencyKey: key });
  assert.equal(a.id, b.id, "repeat ensure returns the same job");
  const [c, d] = await Promise.all([
    ensureLinkedJob(deps, { workflow: wf(ws), stageSpec, stageId: "000-impl", attempt: 1, activityIdempotencyKey: key }),
    ensureLinkedJob(deps, { workflow: wf(ws), stageSpec, stageId: "000-impl", attempt: 1, activityIdempotencyKey: key }),
  ]);
  assert.equal(c.id, d.id);
  assert.equal((await listJobs(root)).length, 1, "exactly one job for the key");
  assert.equal(fake.calls.length, 1, "startJob invoked exactly once");
  assert.ok(await findLinkedJob(root, key));
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("a different attempt key produces a separate job", async () => {
  const root = await tmpDir();
  const ws = await tmpDir();
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, startJob: fake.startJob, startDeps: { rootDir: root } };
  await ensureLinkedJob(deps, { workflow: wf(ws), stageSpec, stageId: "000-impl", attempt: 1, activityIdempotencyKey: makeActivityIdempotencyKey(wf(ws).workflowId, "000-impl", 1) });
  await ensureLinkedJob(deps, { workflow: wf(ws), stageSpec, stageId: "000-impl", attempt: 2, activityIdempotencyKey: makeActivityIdempotencyKey(wf(ws).workflowId, "000-impl", 2) });
  assert.equal((await listJobs(root)).length, 2);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});

test("ensureLinkedJob fails closed WORKFLOW_ACTIVITY_MISSING for an activity-less stage", async () => {
  const root = await tmpDir();
  const ws = await tmpDir();
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, startJob: fake.startJob, startDeps: { rootDir: root } };
  await assert.rejects(
    () => ensureLinkedJob(deps, { workflow: wf(ws), stageSpec: { stageId: "000-impl", pipelineIndex: 0, activity: null }, stageId: "000-impl", attempt: 1, activityIdempotencyKey: "k" }),
    (e) => e.code === "WORKFLOW_ACTIVITY_MISSING",
  );
  assert.equal((await listJobs(root)).length, 0);
  await rm(root, { recursive: true, force: true });
  await rm(ws, { recursive: true, force: true });
});
