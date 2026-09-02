// P3-F #2/#3 crash-recovery tests: the atomic fallback decision (settle source verdict + durable
// fallbackIntent, consumed exactly once) and preflight_result journal recovery (match → COMMIT; canonical
// missing → ABORT/re-check; canonical differs → fail-closed). These drive the store primitives directly and
// hand-craft PENDING journal entries to reproduce a crash at each seam.
import assert from "node:assert/strict";
import { mkdtemp, rm, readdir, readFile, writeFile, mkdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { randomUUID, createHash } from "node:crypto";
import {
  WORKFLOW_SCHEMA_VERSION,
  createWorkflow,
  readStageAttempt,
  transitionStageAttempt,
  recordCheckpoint,
  commitPreflightResult,
  reconcileWorkflow,
  settleWithFallbackIntent,
  consumeFallbackIntent,
  createResumeAttempt,
} from "../dist/workflow-store.js";
import { advanceWorkflowOnce } from "../dist/workflow-reconciler.js";
import { readConfig } from "../dist/core.js";
import { createJob, createJobId, listJobs } from "../dist/job-store.js";

const tmp = () => mkdtemp(path.join(os.tmpdir(), "wf-trust-"));
const jdir = (root, id) => path.join(root, "workflows", id, "journal");
async function writeJson(f, v) { await mkdir(path.dirname(f), { recursive: true }); await writeFile(f, JSON.stringify(v, null, 2) + "\n"); }
const jseq = async (root, id) => (await readdir(jdir(root, id))).length + 1;
async function appendJournal(root, id, over) {
  const seq = await jseq(root, id);
  const entry = { version: WORKFLOW_SCHEMA_VERSION, seq, transitionId: randomUUID(), workflowId: id, status: "PENDING", createdAt: new Date().toISOString(), resolvedAt: null, resolution: null, ...over };
  await writeJson(path.join(jdir(root, id), `${String(seq).padStart(6, "0")}.json`), entry);
  return seq;
}
const readJournal = async (root, id, seq) => JSON.parse(await readFile(path.join(jdir(root, id), `${String(seq).padStart(6, "0")}.json`), "utf8"));

// one stage with a declared fallback candidate (model primary → local fallback)
const fbStage = () => ({
  pipelineIndex: 0, stageName: "s0", runnerType: "model", runnerProfile: "model_agy", candidateId: "c0",
  activity: { argv: ["agy", "x"] },
  fallbacks: [{ candidateId: "c1", runnerType: "local", runnerProfile: "local_build", activity: { argv: ["true"] } }],
});
const seed = (root) => createWorkflow(root, {
  parent: { agentId: "a", sessionKey: null }, repository: { worktree: "/tmp/wt", branch: "m", baseCommit: "d" },
  deliveryRoute: { channel: "C1" }, forbiddenActions: [], pipeline: [fbStage()],
});

async function drivenToIntent(root, id, sid) {
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING" });
  await recordCheckpoint(root, id, { stageId: sid, attempt: 1, phase: "before", checkpoint: { aggregateHash: "H1", complete: true, status: "COMPLETE" } });
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "RUNNING", mutation: { jobId: "j1" } });
  // FAULT: settle source verdict + record the durable fallbackIntent, then CRASH before consuming it (no N+1).
  await settleWithFallbackIntent(root, id, { stageId: sid, attempt: 1, sourceToState: "FAILED", sourceMutation: { jobOutcome: "FAILED_PROVIDER", providerState: "BLOCKED_QUOTA" }, candidateIndex: 1, fromCandidateIndex: 0, fallbackReason: "provider BLOCKED_QUOTA", expectedCheckpointHash: "H1" });
}

test("#2 atomic fallback: reconcileWorkflow PRESERVES the intent; consume creates EXACTLY ONE fallback attempt", async () => {
  const root = await tmp();
  const id = await seed(root);
  const sid = "000-s0";
  await drivenToIntent(root, id, sid);
  const a1 = await readStageAttempt(root, id, sid, 1);
  assert.equal(a1.stageState, "FAILED", "source verdict preserved");
  assert.equal(a1.fallbackIntent.status, "PENDING");
  assert.equal(await readStageAttempt(root, id, sid, 2).catch(() => null), null, "N+1 not created before consume");

  // #3: reconcileWorkflow does storage recovery ONLY — it must NOT create the fallback attempt.
  await reconcileWorkflow(root, id);
  assert.equal((await readStageAttempt(root, id, sid, 1)).fallbackIntent.status, "PENDING", "reconcile preserves the intent");
  assert.equal(await readStageAttempt(root, id, sid, 2).catch(() => null), null, "reconcile does not create N+1");

  // consume → exactly one fallback attempt (candidate 1, verified).
  await consumeFallbackIntent(root, id, { stageId: sid, attempt: 1 });
  const a2 = await readStageAttempt(root, id, sid, 2);
  assert.equal(a2.fallback.fallbackFromAttempt, 1);
  assert.equal(a2.executionCandidate.candidateIndex, 1, "next candidate, not primary");
  assert.equal(a2.fallback.checkpointVerified, true);
  assert.equal(a2.checkpoint.expectedBeforeHash, "H1");
  assert.equal((await readStageAttempt(root, id, sid, 1)).fallbackIntent.status, "CONSUMED");

  // idempotent: a second consume creates no duplicate / chained fallback.
  await consumeFallbackIntent(root, id, { stageId: sid, attempt: 1 });
  assert.equal(await readStageAttempt(root, id, sid, 3).catch(() => null), null, "no duplicate fallback on re-consume");
  await rm(root, { recursive: true, force: true });
});

test("#3 fallbackIntent feature-flag: disabled → no N+1 (intent PENDING); enabled → exactly one N+1, one job", async () => {
  const root = await tmp();
  const id = await seed(root);
  const sid = "000-s0";
  await drivenToIntent(root, id, sid);

  let submitted = 0;
  const startJob = async (_sd, _ctx, params) => { submitted += 1; const jid = createJobId(); const now = new Date().toISOString(); await createJob(root, { version: 1, id: jid, name: params.name, state: "QUEUED", processState: "QUEUED", providerState: null, jobOutcome: null, runnerType: params.runnerType ?? null, runnerProfile: params.runnerProfile ?? null, cwd: params.cwd, command: params.command, timeoutSeconds: 0, createdAt: now, updatedAt: now, startedAt: null, endedAt: null, parent: { agentId: "a", sessionKey: null, flowId: null }, flowId: null, directory: path.join(root, jid), workflowLink: params.workflowLink ?? null, validatedExecution: params.validatedExecution ?? null, notification: { status: "pending", attempts: 0, idempotencyKey: `durable-job:${jid}:terminal` }, delivery: null }); return { id: jid }; };
  const seams = { captureFingerprint: async () => ({ fingerprintVersion: 1, status: "COMPLETE", aggregateHash: "H1", capturedAt: new Date().toISOString() }), captureToolchain: async ({ runnerType, runnerProfile }) => ({ fingerprintVersion: 1, status: "COMPLETE", aggregateHash: "tc", executableBasename: "x", executableRealpath: "/x", runnerType, runnerProfile }), providerProbe: async () => ({ status: "READY" }), providerConfigFingerprint: "t" };

  // DISABLED: recover repeatedly — no fallback attempt, intent stays PENDING, no job.
  const disabled = { rootDir: root, config: readConfig({ pluginConfig: { workflowEnabled: false, allowedRoots: ["/tmp/wt"] } }), startJob, startDeps: { rootDir: root }, ...seams };
  for (let i = 0; i < 3; i++) await advanceWorkflowOnce(disabled, id);
  assert.equal(await readStageAttempt(root, id, sid, 2).catch(() => null), null, "disabled: no N+1");
  assert.equal((await readStageAttempt(root, id, sid, 1)).fallbackIntent.status, "PENDING", "disabled: intent preserved");
  assert.equal((await listJobs(root)).length, 0, "disabled: no job");

  // ENABLED: exactly one N+1, and at most one job across passes.
  const enabled = { rootDir: root, config: readConfig({ pluginConfig: { workflowEnabled: true, allowedRoots: ["/tmp/wt"] } }), startJob, startDeps: { rootDir: root }, ...seams };
  await advanceWorkflowOnce(enabled, id); // consumes intent → N+1
  await advanceWorkflowOnce(enabled, id); // preflight+submit N+1 → one job
  await advanceWorkflowOnce(enabled, id); // idempotent
  const a2 = await readStageAttempt(root, id, sid, 2);
  assert.equal(a2.executionCandidate.candidateIndex, 1);
  assert.equal(await readStageAttempt(root, id, sid, 3).catch(() => null), null, "exactly one N+1");
  assert.equal(submitted, 1, "at most one job");
  await rm(root, { recursive: true, force: true });
});

test("#3 preflight_result recovery: matching hashes → COMMIT; differing → fail-closed", async () => {
  const root = await tmp();
  const id = await seed(root);
  const sid = "000-s0";
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING" });
  await commitPreflightResult(root, id, {
    stageId: sid, attempt: 1,
    preflight: { status: "PASSED", failureCode: null, providerCapability: "READY" },
    checkpointBefore: { aggregateHash: "H1", complete: true, status: "COMPLETE" },
    toolchain: { aggregateHash: "TC1", executableRealpath: "/x" },
  });
  const payloadHash = createHash("sha256").update(JSON.stringify({ status: "PASSED", failureCode: null, providerCapability: "READY" })).digest("hex");

  // (a) a hand-crafted PENDING preflight_result whose recorded hashes MATCH the canonical attempt → COMMIT.
  const s1 = await appendJournal(root, id, { operation: "preflight_result", stageId: sid, attempt: 1, fromState: "SUBMITTING", toState: "SUBMITTING", preflightPayloadHash: payloadHash, checkpointBeforeHash: "H1", frozenToolchainHash: "TC1" });
  await reconcileWorkflow(root, id);
  assert.equal((await readJournal(root, id, s1)).status, "COMMITTED", "matching preflight intent commits");

  // (b) a PENDING preflight_result whose hashes CONTRADICT the canonical attempt → fail-closed (throws).
  await appendJournal(root, id, { operation: "preflight_result", stageId: sid, attempt: 1, fromState: "SUBMITTING", toState: "SUBMITTING", preflightPayloadHash: "WRONG", checkpointBeforeHash: "H1", frozenToolchainHash: "TC1" });
  await assert.rejects(() => reconcileWorkflow(root, id), (e) => e.code === "WORKFLOW_RECONCILE_CONFLICT");
  await rm(root, { recursive: true, force: true });
});

test("#3 preflight_result recovery: PENDING over an attempt with no canonical preflight → ABORTED (re-check), never COMMIT-because-unchanged", async () => {
  const root = await tmp();
  const id = await seed(root);
  const sid = "000-s0";
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING" }); // no preflight ever applied
  const seq = await appendJournal(root, id, { operation: "preflight_result", stageId: sid, attempt: 1, fromState: "SUBMITTING", toState: "SUBMITTING", preflightPayloadHash: "X", checkpointBeforeHash: null, frozenToolchainHash: null });
  await reconcileWorkflow(root, id); // must NOT COMMIT just because stageState is unchanged
  const j = await readJournal(root, id, seq);
  assert.equal(j.status, "ABORTED");
  assert.equal(j.resolution, "NO_CANONICAL_CHANGE");
  await rm(root, { recursive: true, force: true });
});

// ---- #2 attempt-creation journal crash harness (next_attempt = fallback, resume_attempt = resume) ----
const adir = (root, id, sid) => path.join(root, "workflows", id, "stages", sid, "attempts");
const af = (root, id, sid, n) => path.join(adir(root, id, sid), `${String(n).padStart(4, "0")}.json`);
const stageJson = (root, id, sid) => path.join(root, "workflows", id, "stages", sid, "stage.json");
async function loadJournals(root, id) {
  const files = (await readdir(jdir(root, id))).filter((f) => /^\d{6}\.json$/.test(f)).sort();
  const out = [];
  for (const f of files) out.push({ seq: Number(f.slice(0, 6)), e: JSON.parse(await readFile(path.join(jdir(root, id), f), "utf8")) });
  return out;
}
const pendingCount = async (root, id) => (await loadJournals(root, id)).filter((x) => x.e.status === "PENDING").length;
async function creationSeq(root, id, op) {
  const hit = (await loadJournals(root, id)).filter((x) => x.e.operation === op && x.e.attempt === 2).pop();
  return hit ? hit.seq : null;
}
async function patchJournal(root, id, seq, patch) {
  const f = path.join(jdir(root, id), `${String(seq).padStart(6, "0")}.json`);
  const e = JSON.parse(await readFile(f, "utf8"));
  await writeJson(f, { ...e, ...patch });
}

async function seedFallbackAttempt2(root) {
  const id = await seed(root); const sid = "000-s0";
  await drivenToIntent(root, id, sid);                                  // attempt1 FAILED + intent
  await consumeFallbackIntent(root, id, { stageId: sid, attempt: 1 });  // attempt2 (candidate 1) + next_attempt journal
  return { id, sid };
}
async function seedResumeAttempt2(root) {
  const id = await seed(root); const sid = "000-s0";
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING" });
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "RUNNING", mutation: { jobId: "j1" } });
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "FAILED", mutation: { jobOutcome: "FAILED_COMMAND" } });
  await createResumeAttempt(root, id, { stageId: sid, expectedAttempt: 1, requestId: "r1", reason: "retry", actor: { agentId: "a" }, ownerKeyHash: "h", payloadFingerprint: "pf", checkpointPolicy: "manual_rerun" });
  return { id, sid };
}
async function assertRecovered(root, id, sid, expectCandidate) {
  assert.equal(await pendingCount(root, id), 0, "no permanent PENDING journal");
  const a2 = await readStageAttempt(root, id, sid, 2);
  assert.ok(a2, "exactly one attempt N+1");
  assert.equal(await readStageAttempt(root, id, sid, 3).catch(() => null), null, "no extra/chained attempt");
  assert.equal(a2.executionCandidate.candidateIndex, expectCandidate, "candidate preserved (no skip)");
  assert.equal((await readStageAttempt(root, id, sid, 1)).stageState, "FAILED", "source attempt unchanged");
  const proj = JSON.parse(await readFile(stageJson(root, id, sid), "utf8"));
  assert.equal(proj.currentAttempt, 2, "projection recovered to latest canonical");
}

test("#2 case A: next_attempt PENDING, canonical N+1 missing → recreate from journal", async () => {
  const root = await tmp(); const { id, sid } = await seedFallbackAttempt2(root);
  const seq = await creationSeq(root, id, "next_attempt");
  await patchJournal(root, id, seq, { status: "PENDING", resolvedAt: null, resolution: null }); // crash before COMMITTED
  await rm(af(root, id, sid, 2), { force: true });                                               // crash before canonical write
  await reconcileWorkflow(root, id);
  await assertRecovered(root, id, sid, 1);
  await reconcileWorkflow(root, id); // idempotent
  await assertRecovered(root, id, sid, 1);
  await rm(root, { recursive: true, force: true });
});

test("#2 case B: next_attempt canonical written, COMMITTED missing → finalize COMMITTED", async () => {
  const root = await tmp(); const { id, sid } = await seedFallbackAttempt2(root);
  const seq = await creationSeq(root, id, "next_attempt");
  await patchJournal(root, id, seq, { status: "PENDING", resolvedAt: null, resolution: null }); // canonical kept
  await reconcileWorkflow(root, id);
  await assertRecovered(root, id, sid, 1);
  await rm(root, { recursive: true, force: true });
});

test("#2 case C: next_attempt canonical written, projection stale → projection rebuilt", async () => {
  const root = await tmp(); const { id, sid } = await seedFallbackAttempt2(root);
  const seq = await creationSeq(root, id, "next_attempt");
  await patchJournal(root, id, seq, { status: "PENDING", resolvedAt: null, resolution: null });
  const proj = JSON.parse(await readFile(stageJson(root, id, sid), "utf8"));
  await writeJson(stageJson(root, id, sid), { ...proj, currentAttempt: 1, stageState: "FAILED" }); // stale projection
  await reconcileWorkflow(root, id);
  await assertRecovered(root, id, sid, 1);
  await rm(root, { recursive: true, force: true });
});

test("#2 case D: resume_attempt PENDING, canonical N+1 missing → recreate from journal", async () => {
  const root = await tmp(); const { id, sid } = await seedResumeAttempt2(root);
  const seq = await creationSeq(root, id, "resume_attempt");
  await patchJournal(root, id, seq, { status: "PENDING", resolvedAt: null, resolution: null });
  await rm(af(root, id, sid, 2), { force: true });
  await reconcileWorkflow(root, id);
  await assertRecovered(root, id, sid, 0); // resume preserves source candidate 0
  await rm(root, { recursive: true, force: true });
});

test("#2 case E: resume_attempt canonical written, COMMITTED missing → finalize COMMITTED", async () => {
  const root = await tmp(); const { id, sid } = await seedResumeAttempt2(root);
  const seq = await creationSeq(root, id, "resume_attempt");
  await patchJournal(root, id, seq, { status: "PENDING", resolvedAt: null, resolution: null });
  await reconcileWorkflow(root, id);
  await assertRecovered(root, id, sid, 0);
  await rm(root, { recursive: true, force: true });
});

test("#2 case F: canonical attempt candidate/trigger mismatches journal → WORKFLOW_RECONCILE_CONFLICT (no overwrite)", async () => {
  const root = await tmp(); const { id, sid } = await seedFallbackAttempt2(root);
  const seq = await creationSeq(root, id, "next_attempt");
  await patchJournal(root, id, seq, { status: "PENDING", resolvedAt: null, resolution: null, candidateIndex: 5, candidateId: "bogus" }); // journal contradicts canonical
  const before = JSON.parse(await readFile(af(root, id, sid, 2), "utf8"));
  await assert.rejects(() => reconcileWorkflow(root, id), (e) => e.code === "WORKFLOW_RECONCILE_CONFLICT");
  const after = JSON.parse(await readFile(af(root, id, sid, 2), "utf8"));
  assert.deepEqual(after, before, "canonical attempt not overwritten on conflict");
  await rm(root, { recursive: true, force: true });
});
