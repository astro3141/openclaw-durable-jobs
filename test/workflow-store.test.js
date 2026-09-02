// P3-A workflow-store tests: schema/path safety, workflow lock, state-transition validation, the crash
// matrix (fail at every atomic step), multiple-pending recovery, and projection rebuild. Storage only —
// no tool registration, no durable-job creation, no stage↔job linkage, no advancement.
import assert from "node:assert/strict";
import { mkdtemp, rm, readdir, readFile, writeFile, open, rename, stat, mkdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { randomUUID } from "node:crypto";

import {
  WORKFLOW_SCHEMA_VERSION,
  HARNESS_VERSION,
  STAGE_STATES,
  TERMINAL_STAGE_STATES,
  ALLOWED_STAGE_TRANSITIONS,
  isAllowedStageTransition,
  makeStageId,
  newWorkflowId,
  createWorkflow,
  readWorkflow,
  listWorkflows,
  readStageAttempt,
  transitionStageAttempt,
  rebuildStageProjection,
  rebuildWorkflowProjection,
  reconcileWorkflow,
  withWorkflowLock,
} from "../dist/workflow-store.js";

async function tmpRoot() {
  return mkdtemp(path.join(os.tmpdir(), "wf-store-test-"));
}

const PIPELINE = [
  { pipelineIndex: 0, stageName: "preflight", runnerType: "local", runnerProfile: "local_test" },
  { pipelineIndex: 10, stageName: "implementation", runnerType: "model", runnerProfile: "model_agy" },
  { pipelineIndex: 20, stageName: "review", runnerType: "model", runnerProfile: "model_agy" },
];

async function seedWorkflow(root) {
  const id = await createWorkflow(root, {
    parent: { agentId: "a", sessionKey: "claude-queue-test" },
    repository: { worktree: "/tmp/wt", branch: "master", baseCommit: "deadbeef" },
    deliveryRoute: { channel: "C1" },
    forbiddenActions: ["push"],
    pipeline: PIPELINE,
  });
  return id;
}

// ---- schema / path ----
test("workflow / stage / attempt id generation", async () => {
  assert.match(newWorkflowId(), /^wf-[0-9a-f-]{36}$/);
  assert.equal(makeStageId(10, "implementation"), "010-implementation");
  assert.equal(makeStageId(0, "pre.flight_v2"), "000-pre.flight_v2");
});

test("unsafe stage names and path traversal are rejected", () => {
  for (const bad of ["../x", "a/b", "..", ".", "name with space", "x".repeat(65), "sl/ash", "nul\0"]) {
    assert.throws(() => makeStageId(1, bad), /STAGE_NAME_INVALID|STAGE_ID_INVALID/, `should reject ${JSON.stringify(bad)}`);
  }
  assert.throws(() => makeStageId(1000, "x"), /STAGE_ID_INVALID/);
  assert.throws(() => makeStageId(-1, "x"), /STAGE_ID_INVALID/);
});

test("createWorkflow builds the on-disk layout with journal + stages + attempts", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const wfDir = path.join(root, "workflows", id);
  const journal = await readdir(path.join(wfDir, "journal"));
  assert.deepEqual(journal.sort(), ["000001.json"]); // workflow_created
  // harnessVersion is stamped into the durable header and the workflow_created journal evidence.
  const createdEntry = JSON.parse(await readFile(path.join(wfDir, "journal", "000001.json"), "utf8"));
  assert.equal(createdEntry.operation, "workflow_created");
  assert.equal(createdEntry.header.harnessVersion, HARNESS_VERSION);
  const stages = (await readdir(path.join(wfDir, "stages"))).sort();
  assert.deepEqual(stages, ["000-preflight", "010-implementation", "020-review"]);
  for (const s of stages) {
    const attempts = await readdir(path.join(wfDir, "stages", s, "attempts"));
    assert.deepEqual(attempts, ["0001.json"]);
    assert.ok((await readStageAttempt(root, id, s, 1)).stageState === "PENDING");
  }
  const wf = await readWorkflow(root, id);
  assert.equal(wf.workflowState, "RUNNING");
  assert.equal(wf.currentStage, "000-preflight");
  assert.deepEqual(wf.completedStages, []);
  assert.equal(wf.forbiddenActions[0], "push");
  assert.equal(wf.harnessVersion, HARNESS_VERSION);
  await rm(root, { recursive: true, force: true });
});

test("HARNESS_VERSION matches the package version (provenance stamp stays in sync)", async () => {
  const pkg = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
  assert.equal(HARNESS_VERSION, pkg.version);
});

test("schema version mismatch fails closed on read", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const attemptFile = path.join(root, "workflows", id, "stages", "000-preflight", "attempts", "0001.json");
  const rec = JSON.parse(await readFile(attemptFile, "utf8"));
  rec.version = WORKFLOW_SCHEMA_VERSION + 99;
  await writeFile(attemptFile, JSON.stringify(rec));
  await assert.rejects(() => readStageAttempt(root, id, "000-preflight", 1), /WORKFLOW_SCHEMA_VERSION_MISMATCH/);
  await rm(root, { recursive: true, force: true });
});

// ---- lock ----
test("workflow lock serializes concurrent transitions on the same workflow", async () => {
  const root = await tmpRoot();
  const id = newWorkflowId();
  let inside = 0, maxInside = 0;
  const body = async () => {
    inside += 1; maxInside = Math.max(maxInside, inside);
    await new Promise((r) => setTimeout(r, 20));
    inside -= 1;
  };
  await Promise.all([1, 2, 3, 4].map(() => withWorkflowLock(root, id, body)));
  assert.equal(maxInside, 1, "lock did not serialize");
  await rm(root, { recursive: true, force: true });
});

test("workflow lock releases (finally) even when the body throws", async () => {
  const root = await tmpRoot();
  const id = newWorkflowId();
  await assert.rejects(() => withWorkflowLock(root, id, async () => { throw new Error("boom"); }), /boom/);
  // lock dir must be gone → next acquire succeeds
  let ran = false;
  await withWorkflowLock(root, id, async () => { ran = true; });
  assert.ok(ran);
  await rm(root, { recursive: true, force: true });
});

test("stale workflow lock is reclaimed", async () => {
  const root = await tmpRoot();
  const id = newWorkflowId();
  const lockPath = path.join(root, "workflows", id, ".wf.lock");
  await mkdir(lockPath, { recursive: true });
  // backdate mtime beyond the 30s stale window
  const old = new Date(Date.now() - 60_000);
  const { utimes } = await import("node:fs/promises");
  await utimes(lockPath, old, old);
  let ran = false;
  await withWorkflowLock(root, id, async () => { ran = true; });
  assert.ok(ran, "stale lock not reclaimed");
  await rm(root, { recursive: true, force: true });
});

test("lock acquisition failure raises WORKFLOW_LOCK_TIMEOUT", async () => {
  const root = await tmpRoot();
  const id = newWorkflowId();
  await withWorkflowLock(root, id, async () => {
    // hold the lock, then a second (short-deadline) acquire must time out
    await assert.rejects(() => withWorkflowLock(root, id, async () => {}), (e) => e.code === "WORKFLOW_LOCK_TIMEOUT");
  });
  await rm(root, { recursive: true, force: true });
});

// ---- transitions ----
test("allowed transitions succeed, and journal records commit", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const sid = "010-implementation";
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING", mutation: { jobId: "job-1" } });
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "RUNNING" });
  const rec = await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "UNVERIFIED" });
  assert.equal(rec.stageState, "UNVERIFIED");
  assert.equal(rec.jobId, "job-1");
  assert.ok(rec.startedAt, "startedAt stamped at RUNNING");
  const journal = (await readdir(path.join(root, "workflows", id, "journal"))).sort();
  // 1 created + 3 transitions (each: pending then committed rewrite of same seq) → 4 files
  assert.deepEqual(journal, ["000001.json", "000002.json", "000003.json", "000004.json"]);
  for (const f of journal.slice(1)) {
    const e = JSON.parse(await readFile(path.join(root, "workflows", id, "journal", f), "utf8"));
    assert.equal(e.status, "COMMITTED");
  }
  await rm(root, { recursive: true, force: true });
});

test("illegal / backward / terminal transitions are rejected", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const sid = "020-review";
  // PENDING → RUNNING is not allowed (must go through SUBMITTING)
  await assert.rejects(() => transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "RUNNING" }), /STAGE_TRANSITION_ILLEGAL/);
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING" });
  // backward SUBMITTING → PENDING
  await assert.rejects(() => transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "PENDING" }), /STAGE_TRANSITION_ILLEGAL/);
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "CANCELLED" });
  // terminal → anything
  await assert.rejects(() => transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "RUNNING" }), /STAGE_TERMINAL/);
  await rm(root, { recursive: true, force: true });
});

test("same-state re-apply is an idempotent no-op (no transition journal, mutation merges)", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const sid = "010-implementation";
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING" });
  const before = (await readdir(path.join(root, "workflows", id, "journal"))).length;
  const rec = await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING", mutation: { jobId: "job-9" } });
  const after = (await readdir(path.join(root, "workflows", id, "journal"))).length;
  assert.equal(before, after, "idempotent re-apply must not add a journal entry");
  assert.equal(rec.jobId, "job-9");
  await rm(root, { recursive: true, force: true });
});

test("journal seq never collides across sequential transitions", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const sid = "000-preflight";
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING" });
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "RUNNING" });
  const seqs = (await readdir(path.join(root, "workflows", id, "journal"))).map((f) => f.slice(0, 6));
  assert.equal(new Set(seqs).size, seqs.length, "duplicate journal seq");
  await rm(root, { recursive: true, force: true });
});

// ---- crash matrix ----
// Reproduce a crash at each atomic step of transitionStageAttempt by driving the raw writes, then prove
// reconcile recovers deterministically. The steps mirror the production procedure:
//   1) PENDING journal intent  2) canonical attempt update  3) COMMITTED journal  4) stage proj  5) wf proj
async function atomicWrite(file, value) {
  const tmp = `${file}.${randomUUID()}.tmp`;
  const fh = await open(tmp, "wx");
  await fh.writeFile(JSON.stringify(value, null, 2) + "\n");
  await fh.sync();
  await fh.close();
  await rename(tmp, file);
}
function jf(root, id, seq) { return path.join(root, "workflows", id, "journal", `${String(seq).padStart(6, "0")}.json`); }
function af(root, id, sid, n) { return path.join(root, "workflows", id, "stages", sid, "attempts", `${String(n).padStart(4, "0")}.json`); }

async function noPartialJson(dir) {
  for (const e of await readdir(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) { await noPartialJson(p); continue; }
    assert.ok(!e.name.endsWith(".tmp"), `leftover temp: ${p}`);
    if (e.name.endsWith(".json")) JSON.parse(await readFile(p, "utf8")); // throws on partial
  }
}

for (const crashStep of [1, 2, 3, 4, 5]) {
  test(`crash matrix: fail at step ${crashStep} → reconcile recovers deterministically`, async () => {
    const root = await tmpRoot();
    const id = await seedWorkflow(root);
    const sid = "010-implementation";
    const seq = 2; // first transition after workflow_created
    const at = new Date().toISOString();
    const base = { version: WORKFLOW_SCHEMA_VERSION, seq, transitionId: randomUUID(), operation: "stage_transition", workflowId: id, stageId: sid, attempt: 1, fromState: "PENDING", toState: "SUBMITTING" };
    const rec = await readStageAttempt(root, id, sid, 1);

    // step 1: PENDING intent
    if (crashStep >= 1) await atomicWrite(jf(root, id, seq), { ...base, status: "PENDING", createdAt: at, resolvedAt: null, resolution: null });
    // step 2: canonical attempt update
    if (crashStep >= 2) await atomicWrite(af(root, id, sid, 1), { ...rec, stageState: "SUBMITTING", updatedAt: new Date().toISOString() });
    // step 3: COMMITTED journal
    if (crashStep >= 3) await atomicWrite(jf(root, id, seq), { ...base, status: "COMMITTED", createdAt: at, resolvedAt: new Date().toISOString(), resolution: null });
    // step 4: stage projection
    if (crashStep >= 4) await rebuildStageProjection(root, id, sid);
    // step 5: workflow projection
    if (crashStep >= 5) await rebuildWorkflowProjection(root, id);

    await noPartialJson(path.join(root, "workflows", id));
    const first = await reconcileWorkflow(root, id);
    await noPartialJson(path.join(root, "workflows", id));
    const second = await reconcileWorkflow(root, id); // idempotent
    assert.deepEqual({ ...first, updatedAt: 0 }, { ...second, updatedAt: 0 }, "reconcile not idempotent");

    // canonical wins: if the attempt was written (step>=2) it is SUBMITTING, else it stays PENDING
    const attempt = await readStageAttempt(root, id, sid, 1);
    const expected = crashStep >= 2 ? "SUBMITTING" : "PENDING";
    assert.equal(attempt.stageState, expected, `canonical mismatch at crashStep ${crashStep}`);
    // and the journal entry is resolved (no lingering PENDING)
    const je = JSON.parse(await readFile(jf(root, id, seq), "utf8"));
    assert.notEqual(je.status, "PENDING", "journal left PENDING after reconcile");
    if (crashStep < 2) assert.equal(je.resolution, "NO_CANONICAL_CHANGE");
    // no double advance: exactly one attempt file for the stage
    assert.equal((await readdir(path.join(root, "workflows", id, "stages", sid, "attempts"))).length, 1);
    await rm(root, { recursive: true, force: true });
  });
}

test("reconcile is fail-closed when canonical contradicts the journal intent", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const sid = "010-implementation";
  // advance canonical to RUNNING via real transitions
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING" });
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "RUNNING" });
  // craft an uncommitted intent claiming UNVERIFIED→PASSED — but canonical is behind at RUNNING, and RUNNING
  // is NOT reachable from PASSED (terminal) → neither committed nor a clean no-op → fail-closed.
  const nextSeq = (await readdir(path.join(root, "workflows", id, "journal"))).length + 1;
  await atomicWrite(jf(root, id, nextSeq), { version: WORKFLOW_SCHEMA_VERSION, seq: nextSeq, transitionId: randomUUID(), operation: "stage_transition", workflowId: id, stageId: sid, attempt: 1, fromState: "UNVERIFIED", toState: "PASSED", status: "PENDING", createdAt: new Date().toISOString(), resolvedAt: null, resolution: null });
  await assert.rejects(() => reconcileWorkflow(root, id), /WORKFLOW_RECONCILE_CONFLICT/);
  await rm(root, { recursive: true, force: true });
});

// ---- multiple pending journals ----
test("multiple pending journals are all processed in ascending seq order", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const sidA = "000-preflight", sidB = "010-implementation";
  // craft TWO uncommitted intents by hand (abnormal, but must be recovered)
  // seq 2: preflight PENDING→SUBMITTING, canonical WRITTEN (should COMMIT)
  await atomicWrite(jf(root, id, 2), { version: WORKFLOW_SCHEMA_VERSION, seq: 2, transitionId: randomUUID(), operation: "stage_transition", workflowId: id, stageId: sidA, attempt: 1, fromState: "PENDING", toState: "SUBMITTING", status: "PENDING", createdAt: new Date().toISOString(), resolvedAt: null, resolution: null });
  const recA = await readStageAttempt(root, id, sidA, 1);
  await atomicWrite(af(root, id, sidA, 1), { ...recA, stageState: "SUBMITTING" });
  // seq 3: implementation PENDING→SUBMITTING, canonical NOT written (should ABORT no-op)
  await atomicWrite(jf(root, id, 3), { version: WORKFLOW_SCHEMA_VERSION, seq: 3, transitionId: randomUUID(), operation: "stage_transition", workflowId: id, stageId: sidB, attempt: 1, fromState: "PENDING", toState: "SUBMITTING", status: "PENDING", createdAt: new Date().toISOString(), resolvedAt: null, resolution: null });

  await reconcileWorkflow(root, id);
  const e2 = JSON.parse(await readFile(jf(root, id, 2), "utf8"));
  const e3 = JSON.parse(await readFile(jf(root, id, 3), "utf8"));
  assert.equal(e2.status, "COMMITTED", "written-canonical pending must commit");
  assert.equal(e3.status, "ABORTED", "no-canonical pending must abort (not left dangling)");
  assert.equal(e3.resolution, "NO_CANONICAL_CHANGE");
  assert.equal((await readStageAttempt(root, id, sidA, 1)).stageState, "SUBMITTING");
  assert.equal((await readStageAttempt(root, id, sidB, 1)).stageState, "PENDING");
  await rm(root, { recursive: true, force: true });
});

// ---- projection ----
test("workflow.json deletion → rebuilt from journal header + stage records", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  await rm(path.join(root, "workflows", id, "workflow.json"), { force: true });
  const wf = await rebuildWorkflowProjection(root, id);
  assert.equal(wf.workflowId, id);
  assert.equal(wf.forbiddenActions[0], "push"); // header recovered from workflow_created journal entry
  assert.equal(wf.currentStage, "000-preflight");
  await rm(root, { recursive: true, force: true });
});

test("stage.json deletion → rebuilt from attempt record", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const sid = "010-implementation";
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING", mutation: { jobId: "job-x" } });
  await rm(path.join(root, "workflows", id, "stages", sid, "stage.json"), { force: true });
  const proj = await rebuildStageProjection(root, id, sid);
  assert.equal(proj.stageState, "SUBMITTING");
  assert.equal(proj.latestJobId, "job-x");
  await rm(root, { recursive: true, force: true });
});

test("corrupted projection currentStage/completedStages is overwritten from canonical", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const wfFile = path.join(root, "workflows", id, "workflow.json");
  const wf = JSON.parse(await readFile(wfFile, "utf8"));
  wf.currentStage = "999-bogus"; wf.completedStages = ["010-implementation"]; wf.workflowState = "SUCCEEDED";
  await writeFile(wfFile, JSON.stringify(wf));
  const rebuilt = await rebuildWorkflowProjection(root, id);
  assert.equal(rebuilt.currentStage, "000-preflight");
  assert.deepEqual(rebuilt.completedStages, []);
  assert.equal(rebuilt.workflowState, "RUNNING");
  await rm(root, { recursive: true, force: true });
});

test("workflowState derivation by stage combination", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const drive = (sid, states) => states.reduce((p, s) => p.then(() => transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: s })), Promise.resolve());

  // all PASSED → SUCCEEDED
  await drive("000-preflight", ["SUBMITTING", "RUNNING", "UNVERIFIED", "PASSED"]);
  await drive("010-implementation", ["SUBMITTING", "RUNNING", "UNVERIFIED", "PASSED"]);
  await drive("020-review", ["SUBMITTING", "RUNNING", "UNVERIFIED", "PASSED"]);
  assert.equal((await readWorkflow(root, id)).workflowState, "SUCCEEDED");
  assert.deepEqual((await readWorkflow(root, id)).completedStages, ["000-preflight", "010-implementation", "020-review"]);
  await rm(root, { recursive: true, force: true });

  // APPROVAL_REQUIRED → PAUSED ; BLOCKED_DEPENDENCY → BLOCKED ; FAILED → FAILED
  for (const [target, expected] of [["APPROVAL_REQUIRED", "PAUSED"], ["BLOCKED_DEPENDENCY", "BLOCKED"], ["FAILED", "FAILED"]]) {
    const r = await tmpRoot();
    const w = await seedWorkflow(r);
    const chain = target === "APPROVAL_REQUIRED"
      ? ["SUBMITTING", "RUNNING", "UNVERIFIED", "APPROVAL_REQUIRED"]
      : target === "BLOCKED_DEPENDENCY"
      ? ["SUBMITTING", "RUNNING", "BLOCKED_DEPENDENCY"]
      : ["SUBMITTING", "FAILED"];
    // linear/frontier-based projection: drive the FRONTIER stage (first, still PENDING) to the target
    await chain.reduce((p, s) => p.then(() => transitionStageAttempt(r, w, { stageId: "000-preflight", attempt: 1, toState: s })), Promise.resolve());
    assert.equal((await readWorkflow(r, w)).workflowState, expected, `${target} → ${expected}`);
    await rm(r, { recursive: true, force: true });
  }
});

test("attempt history is preserved (transitions mutate the same attempt file, not new ones)", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const sid = "010-implementation";
  const created = (await readStageAttempt(root, id, sid, 1)).createdAt;
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING" });
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "RUNNING" });
  assert.equal((await readdir(path.join(root, "workflows", id, "stages", sid, "attempts"))).length, 1);
  assert.equal((await readStageAttempt(root, id, sid, 1)).createdAt, created, "createdAt preserved across transitions");
  await rm(root, { recursive: true, force: true });
});

test("listWorkflows returns created workflows", async () => {
  const root = await tmpRoot();
  const a = await seedWorkflow(root);
  const b = await seedWorkflow(root);
  const ids = (await listWorkflows(root)).map((w) => w.workflowId).sort();
  assert.deepEqual(ids.sort(), [a, b].sort());
  await rm(root, { recursive: true, force: true });
});

// ---- projection completeness (pipeline-driven, canonical-only) ----
test("all pipeline stages are materialized as canonical PENDING attempts at creation", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  for (const sid of ["000-preflight", "010-implementation", "020-review"]) {
    const rec = await readStageAttempt(root, id, sid, 1);
    assert.ok(rec, `missing canonical attempt for ${sid}`);
    assert.equal(rec.stageState, "PENDING");
  }
  await rm(root, { recursive: true, force: true });
});

test("first stage PASSED but later stages PENDING → RUNNING, currentStage advances (never SUCCEEDED)", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const drive = (sid, states) => states.reduce((p, s) => p.then(() => transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: s })), Promise.resolve());
  await drive("000-preflight", ["SUBMITTING", "RUNNING", "UNVERIFIED", "PASSED"]);
  const wf = await readWorkflow(root, id);
  assert.equal(wf.workflowState, "RUNNING");
  assert.equal(wf.currentStage, "010-implementation");
  assert.deepEqual(wf.completedStages, ["000-preflight"]);
  await rm(root, { recursive: true, force: true });
});

test("SUCCEEDED only when EVERY pipeline stage is PASSED", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const drive = (sid) => ["SUBMITTING", "RUNNING", "UNVERIFIED", "PASSED"].reduce((p, s) => p.then(() => transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: s })), Promise.resolve());
  await drive("000-preflight");
  await drive("010-implementation");
  assert.equal((await readWorkflow(root, id)).workflowState, "RUNNING"); // review still PENDING
  await drive("020-review");
  assert.equal((await readWorkflow(root, id)).workflowState, "SUCCEEDED");
  await rm(root, { recursive: true, force: true });
});

test("a missing middle-stage canonical record must NOT be reported SUCCEEDED", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const drive = (sid) => ["SUBMITTING", "RUNNING", "UNVERIFIED", "PASSED"].reduce((p, s) => p.then(() => transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: s })), Promise.resolve());
  await drive("000-preflight");
  await drive("010-implementation");
  await drive("020-review"); // now all PASSED → SUCCEEDED
  assert.equal((await readWorkflow(root, id)).workflowState, "SUCCEEDED");
  // simulate loss of a middle stage's canonical record entirely
  await rm(path.join(root, "workflows", id, "stages", "010-implementation"), { recursive: true, force: true });
  const rebuilt = await rebuildWorkflowProjection(root, id);
  assert.notEqual(rebuilt.workflowState, "SUCCEEDED", "lost stage must not read as SUCCEEDED");
  assert.equal(rebuilt.workflowState, "RUNNING");
  assert.equal(rebuilt.currentStage, "010-implementation"); // first non-PASSED (the missing one)
  assert.ok(!rebuilt.completedStages.includes("010-implementation"));
  await rm(root, { recursive: true, force: true });
});

test("empty pipeline is rejected", async () => {
  const root = await tmpRoot();
  await assert.rejects(() => createWorkflow(root, { pipeline: [] }), /WORKFLOW_PIPELINE_INVALID/);
  await assert.rejects(() => createWorkflow(root, {}), /WORKFLOW_PIPELINE_INVALID/);
  await rm(root, { recursive: true, force: true });
});

test("workflow rebuild ignores a corrupt stage.json (false PASSED) and corrects from canonical", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const sid = "010-implementation"; // canonical stays PENDING
  const stageFile = path.join(root, "workflows", id, "stages", sid, "stage.json");
  const proj = JSON.parse(await readFile(stageFile, "utf8"));
  proj.stageState = "PASSED"; // lie
  await writeFile(stageFile, JSON.stringify(proj));
  const wf = await rebuildWorkflowProjection(root, id);
  assert.notEqual(wf.workflowState, "SUCCEEDED");
  assert.ok(!wf.completedStages.includes(sid));
  assert.equal(JSON.parse(await readFile(stageFile, "utf8")).stageState, "PENDING", "stage.json corrected to canonical");
  await rm(root, { recursive: true, force: true });
});

test("stage.json pointing at an older attempt is corrected to the latest canonical attempt", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  const sid = "010-implementation";
  // write a newer canonical attempt 0002 by hand (higher attempt = latest)
  const a1 = await readStageAttempt(root, id, sid, 1);
  await atomicWrite(af(root, id, sid, 2), { ...a1, attempt: 2, stageState: "SUBMITTING", jobId: "job-2" });
  // corrupt stage.json to point at attempt 1 / PENDING
  const stageFile = path.join(root, "workflows", id, "stages", sid, "stage.json");
  await writeFile(stageFile, JSON.stringify({ ...JSON.parse(await readFile(stageFile, "utf8")), currentAttempt: 1, stageState: "PENDING", latestJobId: null }));
  await rebuildWorkflowProjection(root, id);
  const corrected = JSON.parse(await readFile(stageFile, "utf8"));
  assert.equal(corrected.currentAttempt, 2);
  assert.equal(corrected.stageState, "SUBMITTING");
  assert.equal(corrected.latestJobId, "job-2");
  await rm(root, { recursive: true, force: true });
});

test("workflow.json AND all stage.json deleted → full recovery from canonical attempts + journal header", async () => {
  const root = await tmpRoot();
  const id = await seedWorkflow(root);
  await transitionStageAttempt(root, id, { stageId: "000-preflight", attempt: 1, toState: "SUBMITTING" });
  await transitionStageAttempt(root, id, { stageId: "000-preflight", attempt: 1, toState: "RUNNING" });
  // wipe every projection, keep canonical attempts + journal
  await rm(path.join(root, "workflows", id, "workflow.json"), { force: true });
  for (const sid of ["000-preflight", "010-implementation", "020-review"]) {
    await rm(path.join(root, "workflows", id, "stages", sid, "stage.json"), { force: true });
  }
  const wf = await rebuildWorkflowProjection(root, id);
  assert.equal(wf.workflowId, id);
  assert.equal(wf.forbiddenActions[0], "push"); // header from journal
  assert.equal(wf.currentStage, "000-preflight");
  assert.equal(wf.workflowState, "RUNNING");
  assert.equal((await readStageAttempt(root, id, "000-preflight", 1)).stageState, "RUNNING");
  // stage.json regenerated for every pipeline stage
  for (const sid of ["000-preflight", "010-implementation", "020-review"]) {
    assert.ok(JSON.parse(await readFile(path.join(root, "workflows", id, "stages", sid, "stage.json"), "utf8")));
  }
  await rm(root, { recursive: true, force: true });
});

test("state-model invariants", () => {
  assert.ok(STAGE_STATES.has("APPROVAL_REQUIRED"));
  for (const t of TERMINAL_STAGE_STATES) assert.deepEqual(ALLOWED_STAGE_TRANSITIONS[t], []);
  assert.ok(isAllowedStageTransition("PENDING", "SUBMITTING"));
  assert.ok(!isAllowedStageTransition("PENDING", "RUNNING"));
  assert.ok(isAllowedStageTransition("RUNNING", "RUNNING")); // idempotent
});
