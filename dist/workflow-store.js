// P3-A durable workflow store (storage foundation ONLY). Pure module — no OpenClaw SDK dependency, no
// durable-job creation, no tool registration, no stage↔job linkage, no advancement. It persists workflows,
// stages, and per-attempt canonical records, guards valid stage-state transitions, records an append-only
// transition journal, rebuilds projections from canonical records, and reconciles a crash.
//
// Authoritative model (P3-A):
//   journal/<seq>.json                     append-only transition intent/result history (authoritative)
//   stages/<stageId>/attempts/<n>.json     the attempt's CANONICAL, MUTABLE record (atomic rewrite under
//                                          the workflow lock; state advances only in allowed directions)
//   stages/<stageId>/stage.json            projection, rebuildable from attempt records
//   workflow.json                          projection (header carried in the journal's workflow_created
//                                          entry; currentStage/completedStages/workflowState rebuilt)
//   job.json (elsewhere)                   authoritative only for its activity's process/provider outcome
//
// The workflow lock is held ONLY for storage reads/journal/attempt/projection writes — never across a
// Gateway RPC, startJob, child spawn, or external command. This module never acquires a job lock.

import { mkdir, rename, readFile, writeFile, readdir, rm, stat, open } from "node:fs/promises";
import path from "node:path";
import { randomUUID, createHash } from "node:crypto";

export const WORKFLOW_SCHEMA_VERSION = 1;
export const HARNESS_VERSION = "0.6.0-dev.6";
const LOCK_STALE_MS = 30_000;
const LOCK_TIMEOUT_MS = 10_000;

// ---- state model ----
export const STAGE_STATES = new Set([
  "PENDING", "SUBMITTING", "RUNNING", "UNVERIFIED", "PASSED", "FAILED",
  "ARTIFACT_MISSING", "BLOCKED_DEPENDENCY", "APPROVAL_REQUIRED", "CANCELLED",
]);
export const TERMINAL_STAGE_STATES = new Set(["PASSED", "FAILED", "CANCELLED"]);
export const WORKFLOW_STATES = new Set(["RUNNING", "PAUSED", "BLOCKED", "SUCCEEDED", "FAILED", "CANCELLED"]);

// Allowed forward transitions. Same-state re-apply is an idempotent no-op (handled separately). Any target
// not listed (and not the same state) — or any transition out of a terminal state — is rejected.
export const ALLOWED_STAGE_TRANSITIONS = {
  PENDING: ["SUBMITTING", "CANCELLED"],
  // SUBMITTING covers preflight: a failed/blocked preflight moves it to a stop state without a job.
  SUBMITTING: ["RUNNING", "BLOCKED_DEPENDENCY", "FAILED", "ARTIFACT_MISSING", "APPROVAL_REQUIRED", "CANCELLED"],
  RUNNING: ["UNVERIFIED", "FAILED", "BLOCKED_DEPENDENCY", "ARTIFACT_MISSING", "APPROVAL_REQUIRED", "CANCELLED"],
  UNVERIFIED: ["PASSED", "FAILED", "ARTIFACT_MISSING", "BLOCKED_DEPENDENCY", "APPROVAL_REQUIRED"],
  APPROVAL_REQUIRED: ["PASSED", "FAILED", "CANCELLED"],
  ARTIFACT_MISSING: ["FAILED", "CANCELLED"],
  BLOCKED_DEPENDENCY: ["FAILED", "CANCELLED"],
  PASSED: [],
  FAILED: [],
  CANCELLED: [],
};

function wfError(code, message) {
  const e = new Error(`${code}: ${message}`);
  e.code = code;
  return e;
}

export function isAllowedStageTransition(from, to) {
  if (!STAGE_STATES.has(from) || !STAGE_STATES.has(to)) return false;
  if (from === to) return true; // idempotent
  return (ALLOWED_STAGE_TRANSITIONS[from] ?? []).includes(to);
}

// Is `target` reachable from `to` via allowed transitions (i.e. the attempt already advanced past `to`)?
function reachable(to, target) {
  if (to === target) return true;
  const seen = new Set([to]);
  const queue = [to];
  while (queue.length) {
    const cur = queue.shift();
    for (const nxt of ALLOWED_STAGE_TRANSITIONS[cur] ?? []) {
      if (nxt === target) return true;
      if (!seen.has(nxt)) { seen.add(nxt); queue.push(nxt); }
    }
  }
  return false;
}

// ---- ids / paths (path-traversal safe) ----
const WORKFLOW_ID_RE = /^wf-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SAFE_STAGE_NAME_RE = /^[A-Za-z0-9_.-]{1,64}$/;

export function newWorkflowId() {
  return `wf-${randomUUID()}`;
}

export function assertWorkflowId(workflowId) {
  if (typeof workflowId !== "string" || !WORKFLOW_ID_RE.test(workflowId)) {
    throw wfError("WORKFLOW_ID_INVALID", `invalid workflowId: ${workflowId}`);
  }
  return workflowId;
}

export function makeStageId(pipelineIndex, stageName) {
  if (!Number.isInteger(pipelineIndex) || pipelineIndex < 0 || pipelineIndex > 999) {
    throw wfError("STAGE_ID_INVALID", `pipelineIndex must be an integer 0..999 (got ${pipelineIndex})`);
  }
  if (typeof stageName !== "string" || !SAFE_STAGE_NAME_RE.test(stageName) || stageName === "." || stageName === "..") {
    throw wfError("STAGE_NAME_INVALID", `unsafe stage name: ${JSON.stringify(stageName)}`);
  }
  return `${String(pipelineIndex).padStart(3, "0")}-${stageName}`;
}

function assertStageId(stageId) {
  if (typeof stageId !== "string" || stageId.includes("/") || stageId.includes("..") || !/^\d{3}-[A-Za-z0-9_.-]{1,64}$/.test(stageId)) {
    throw wfError("STAGE_ID_INVALID", `unsafe stageId: ${JSON.stringify(stageId)}`);
  }
  return stageId;
}

function attemptFileName(attempt) {
  if (!Number.isInteger(attempt) || attempt < 1) throw wfError("ATTEMPT_INVALID", `attempt must be a positive integer (got ${attempt})`);
  return `${String(attempt).padStart(4, "0")}.json`;
}

const P = {
  workflowsRoot: (root) => path.join(root, "workflows"),
  workflowDir: (root, id) => path.join(root, "workflows", assertWorkflowId(id)),
  workflowJson: (root, id) => path.join(P.workflowDir(root, id), "workflow.json"),
  lockPath: (root, id) => path.join(P.workflowDir(root, id), ".wf.lock"),
  journalDir: (root, id) => path.join(P.workflowDir(root, id), "journal"),
  journalFile: (root, id, seq) => path.join(P.journalDir(root, id), `${String(seq).padStart(6, "0")}.json`),
  stagesDir: (root, id) => path.join(P.workflowDir(root, id), "stages"),
  stageDir: (root, id, sid) => path.join(P.stagesDir(root, id), assertStageId(sid)),
  stageJson: (root, id, sid) => path.join(P.stageDir(root, id, sid), "stage.json"),
  attemptsDir: (root, id, sid) => path.join(P.stageDir(root, id, sid), "attempts"),
  attemptFile: (root, id, sid, n) => path.join(P.attemptsDir(root, id, sid), attemptFileName(n)),
};

// ---- atomic JSON (unique temp, fsync, rename, best-effort cleanup) ----
async function atomicWriteJson(file, value) {
  const tmp = `${file}.${process.pid}.${randomUUID()}.tmp`;
  let renamed = false;
  let fh;
  try {
    fh = await open(tmp, "wx", 0o600);
    await fh.writeFile(`${JSON.stringify(value, null, 2)}\n`);
    await fh.sync();
    await fh.close();
    fh = null;
    await rename(tmp, file);
    renamed = true;
  } finally {
    if (fh) await fh.close().catch(() => {});
    if (!renamed) await rm(tmp, { force: true }).catch(() => {}); // no orphaned temp accumulation
  }
}

async function readJsonOrNull(file) {
  let raw;
  try {
    raw = await readFile(file, "utf8");
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
  try {
    return JSON.parse(raw); // atomic writes guarantee completeness; a parse error is real corruption
  } catch (error) {
    throw wfError("WORKFLOW_RECORD_CORRUPT", `partial/corrupt JSON at ${file}: ${error.message}`);
  }
}

// ---- mkdir-based locks (stale reclaim, held only for storage work) ----
async function acquireMkdirLock(lockPath, label) {
  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  while (true) {
    try {
      await mkdir(lockPath, { mode: 0o700 });
      return async () => rm(lockPath, { recursive: true, force: true });
    } catch (error) {
      if (error?.code !== "EEXIST") throw error;
      try {
        const info = await stat(lockPath);
        if (Date.now() - info.mtimeMs > LOCK_STALE_MS) { await rm(lockPath, { recursive: true, force: true }); continue; }
      } catch (statError) {
        if (statError?.code !== "ENOENT") throw statError;
        continue;
      }
      if (Date.now() >= deadline) throw wfError("WORKFLOW_LOCK_TIMEOUT", `timed out acquiring lock: ${label}`);
      await new Promise((r) => setTimeout(r, 40));
    }
  }
}

// Per-workflow lock. Held ONLY for storage reads/journal/attempt/projection writes.
export async function withWorkflowLock(root, workflowId, fn) {
  await mkdir(P.workflowDir(root, workflowId), { recursive: true, mode: 0o700 }); // lock parent must exist
  const release = await acquireMkdirLock(P.lockPath(root, workflowId), `workflow ${workflowId}`);
  try {
    return await fn();
  } finally {
    await release();
  }
}

// P3-G: a DEDICATED per-attempt audit-decision lock — SEPARATE from the workflow lock and the activity/job
// creation lock, so audit-decision serialization never blurs those lock-order contracts.
// LOCK ORDER (STRICT): acquire this audit-decision lock FIRST; do all reads (target/authoritative job) AND the
// Git worktree-fingerprint capture while holding ONLY this lock (NEVER the workflow lock); then call
// applyAuditDecision, which takes the workflow lock LAST and re-validates the canonical target + job binding.
// Forbidden: capturing the fingerprint before this lock; running Git while holding the workflow lock; taking
// the workflow lock before this lock; reusing the activity/job-creation lock for audit serialization.
export async function withAuditDecisionLock(root, workflowId, stageId, attempt, fn) {
  const dir = path.join(P.workflowDir(root, workflowId), "audit-locks");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const lockPath = path.join(dir, `${createHash("sha256").update(`${assertStageId(stageId)}:${attempt}`).digest("hex")}.lock`);
  const release = await acquireMkdirLock(lockPath, `audit-decision ${workflowId}/${stageId}#${attempt}`);
  try {
    return await fn();
  } finally {
    await release();
  }
}

// Storage-root creation lock: the per-workflow `.wf.lock` cannot serialize creation of a NOT-YET-EXISTENT
// workflow, so a stable `lockName` (a caller-derived hash of owner+requestId) is locked at the workflows
// root to make the idempotency scan-then-create atomic across concurrent duplicate starts.
export async function withCreationLock(root, lockName, fn) {
  const dir = path.join(root, "workflows", ".locks");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  if (typeof lockName !== "string" || !/^[A-Za-z0-9_.-]{1,128}$/.test(lockName)) {
    throw wfError("WORKFLOW_CREATION_LOCK_INVALID", `unsafe creation lock name: ${JSON.stringify(lockName)}`);
  }
  const release = await acquireMkdirLock(path.join(dir, `${lockName}.lock`), `creation ${lockName}`);
  try {
    return await fn();
  } finally {
    await release();
  }
}

// ---- version guards ----
function assertWorkflowSchema(entryOrHeader, file) {
  if (entryOrHeader && entryOrHeader.version !== WORKFLOW_SCHEMA_VERSION) {
    throw wfError("WORKFLOW_SCHEMA_VERSION_MISMATCH", `schema ${entryOrHeader.version} != ${WORKFLOW_SCHEMA_VERSION} at ${file}`);
  }
  return entryOrHeader;
}

// ---- records ----
function nowIso() {
  return new Date().toISOString();
}

function initialAttempt({ workflowId, stageId, stageName, pipelineIndex, attempt, runnerType, runnerProfile, activityIdempotencyKey, resumeFingerprint, candidateIndex = 0, candidateId = null }) {
  const ts = nowIso();
  return {
    version: WORKFLOW_SCHEMA_VERSION,
    workflowId, stageId, attempt,
    stageName, pipelineIndex,
    runnerType: runnerType ?? null,
    runnerProfile: runnerProfile ?? null,
    // P3-F: which pipeline execution candidate this attempt runs (0 = primary; 1.. = declared fallbacks).
    executionCandidate: { candidateIndex, candidateId, runnerType: runnerType ?? null, runnerProfile: runnerProfile ?? null },
    preflight: null,   // { status, checkedAt, worktreeFingerprint, toolchainFingerprint, providerCapability, providerCacheHit, failureCode, failureReason }
    checkpoint: null,  // { before:{aggregateHash,complete,capturedAt}, after:{...}, expectedBeforeHash? }
    activityIdempotencyKey: activityIdempotencyKey ?? `wf:${workflowId}:stage:${stageId}:attempt:${attempt}`,
    stageState: "PENDING",
    jobId: null,
    processState: null,
    providerState: null,
    jobOutcome: null,
    resumeFingerprint: resumeFingerprint ?? null,
    failureReason: null,
    createdAt: ts, updatedAt: ts, startedAt: null, finishedAt: null,
  };
}

// ---- journal ----
async function nextJournalSeq(root, workflowId) {
  // seq authority is the journal filenames, NOT a projection counter (crash-safe under the lock).
  const dir = P.journalDir(root, workflowId);
  const entries = (await readdir(dir).catch(() => [])).filter((f) => /^\d{6}\.json$/.test(f));
  return entries.reduce((m, f) => Math.max(m, Number(f.slice(0, 6))), 0) + 1;
}

async function writeJournal(root, workflowId, entry) {
  await atomicWriteJson(P.journalFile(root, workflowId, entry.seq), entry);
}

async function readJournalEntries(root, workflowId) {
  const dir = P.journalDir(root, workflowId);
  const files = (await readdir(dir).catch(() => [])).filter((f) => /^\d{6}\.json$/.test(f)).sort();
  const entries = [];
  for (const f of files) {
    const e = await readJsonOrNull(path.join(dir, f));
    if (!e) throw wfError("WORKFLOW_JOURNAL_CORRUPT", `missing/unreadable journal ${f}`);
    entries.push(e);
  }
  return entries;
}

function workflowCreatedHeader(entries) {
  const created = entries.find((e) => e.operation === "workflow_created");
  if (!created) throw wfError("WORKFLOW_HEADER_MISSING", "no workflow_created journal entry");
  return created.header;
}

// ---- projections (rebuilt from canonical records; canonical always wins) ----
async function latestAttempt(root, workflowId, stageId) {
  const dir = P.attemptsDir(root, workflowId, stageId);
  const files = (await readdir(dir).catch(() => [])).filter((f) => /^\d{4}\.json$/.test(f)).sort();
  if (!files.length) return null;
  const rec = assertWorkflowSchema(await readJsonOrNull(path.join(dir, files.at(-1))), files.at(-1));
  return { attempt: Number(files.at(-1).slice(0, 4)), record: rec };
}

export async function rebuildStageProjection(root, workflowId, stageId) {
  const latest = await latestAttempt(root, workflowId, stageId);
  if (!latest) throw wfError("STAGE_NO_ATTEMPTS", `no attempts for ${stageId}`);
  const r = latest.record;
  const projection = {
    version: WORKFLOW_SCHEMA_VERSION,
    stageId, stageName: r.stageName, pipelineIndex: r.pipelineIndex,
    currentAttempt: latest.attempt,
    stageState: r.stageState,
    latestJobId: r.jobId ?? null,
    auditStatus: r.audit?.status ?? null, // P3-G: drives the PAUSED workflowState while an audit is pending
    createdAt: r.createdAt, updatedAt: nowIso(),
  };
  await atomicWriteJson(P.stageJson(root, workflowId, stageId), projection);
  return projection;
}

// Linear-pipeline workflow state = the FRONTIER (first non-PASSED stage) state, mapped to the projection.
// This gives correct CANCELLED/FAILED precedence for the CURRENT frontier: a resumed stage's LATEST attempt
// (PENDING/RUNNING) dominates a prior CANCELLED/FAILED attempt (rebuildStageProjection uses the latest
// attempt), so a stale past CANCELLED never freezes a workflow that has moved on.
function deriveWorkflowState(stages, { missing = false } = {}) {
  const states = stages.map((s) => s.stageState);
  const frontierStage = stages.find((s) => s.stageState !== "PASSED");
  const frontier = frontierStage?.stageState;
  if (frontier === undefined) return missing ? "RUNNING" : (states.length > 0 ? "SUCCEEDED" : "RUNNING");
  switch (frontier) {
    case "CANCELLED": return "CANCELLED";
    case "FAILED": return "FAILED";
    case "APPROVAL_REQUIRED": return "PAUSED";
    case "BLOCKED_DEPENDENCY":
    case "ARTIFACT_MISSING": return "BLOCKED";
    // P3-G: a UNVERIFIED frontier awaiting an independent audit decision is PAUSED (not actively RUNNING).
    case "UNVERIFIED": return (frontierStage?.auditStatus === "REQUESTED" || frontierStage?.auditStatus === "RUNNING") ? "PAUSED" : "RUNNING";
    default: return "RUNNING"; // PENDING / SUBMITTING / RUNNING / null(missing)
  }
}

// Pipeline (from the canonical workflow_created header) is the authoritative set of EXPECTED stages. Each
// stage's state is derived ONLY from its canonical attempt records (stage.json is a projection and is never
// trusted): the stage projection is rebuilt first, then the workflow projection is computed from those
// canonical-derived results. A pipeline stage whose canonical attempt is missing is treated as incomplete
// (never PASSED), so a partial/lost stage set can never be reported SUCCEEDED.
export async function rebuildWorkflowProjection(root, workflowId) {
  const header = workflowCreatedHeader(await readJournalEntries(root, workflowId));
  assertWorkflowSchema(header, "workflow_created");
  const pipeline = [...header.pipeline].sort((a, b) => a.pipelineIndex - b.pipelineIndex);
  const stages = [];
  let missing = false;
  for (const spec of pipeline) {
    const latest = await latestAttempt(root, workflowId, spec.stageId); // canonical, highest attempt
    if (!latest) { stages.push({ stageId: spec.stageId, stageState: null, present: false }); missing = true; continue; }
    const proj = await rebuildStageProjection(root, workflowId, spec.stageId); // rewrite stage.json from canonical
    stages.push({ stageId: spec.stageId, stageState: proj.stageState, auditStatus: proj.auditStatus ?? null, present: true });
  }
  const completedStages = stages.filter((s) => s.stageState === "PASSED").map((s) => s.stageId);
  const firstNonPassed = stages.find((s) => s.stageState !== "PASSED");
  const workflow = {
    ...header, // version, harnessVersion, workflowId, createdAt, parent, repository, deliveryRoute, forbiddenActions, pipeline
    updatedAt: nowIso(),
    currentStage: firstNonPassed ? firstNonPassed.stageId : (stages.at(-1)?.stageId ?? null),
    completedStages,
    workflowState: deriveWorkflowState(stages, { missing }),
  };
  await atomicWriteJson(P.workflowJson(root, workflowId), workflow);
  return workflow;
}

// ---- public API ----
export async function createWorkflow(root, { name = null, ownerKey = null, requestId = null, payloadFingerprint = null, parent = null, repository = null, deliveryRoute = null, forbiddenActions = [], pipeline }) {
  if (!Array.isArray(pipeline) || pipeline.length === 0) throw wfError("WORKFLOW_PIPELINE_INVALID", "pipeline must be a non-empty array");
  const workflowId = newWorkflowId();
  const createdAt = nowIso();
  const stageSpecs = pipeline.map((s) => ({
    stageId: makeStageId(s.pipelineIndex, s.stageName),
    stageName: s.stageName, pipelineIndex: s.pipelineIndex,
    runnerType: s.runnerType ?? null, runnerProfile: s.runnerProfile ?? null,
    // P3-C: the stage's argv activity spec ({ argv, timeoutSeconds }), or null for an activity-less
    // (P3-B-era) skeleton. Stored in the canonical header; the store does not interpret it.
    activity: s.activity ?? null,
    // P3-F: ordered declared fallback candidates (validated + frozen at start), each { candidateId,
    // runnerType, runnerProfile, activity }. The primary is candidate index 0 (this spec itself).
    candidateId: s.candidateId ?? null,
    fallbacks: Array.isArray(s.fallbacks) ? s.fallbacks : [],
    // P3-G: frozen audit policy (validated by the service) + its contract hash, checked at decide-time so a
    // decision can never be applied against a mutated contract. Default mode "none" preserves pre-P3-G behavior.
    audit: s.audit ?? { mode: "none" },
    auditContractHash: s.auditContractHash ?? null,
  }));
  const header = {
    version: WORKFLOW_SCHEMA_VERSION, harnessVersion: HARNESS_VERSION, workflowId, createdAt,
    // Additive P3-B metadata (identity, ownership, idempotency). Authorization must combine agentId with
    // sessionKey/workspace containment via ownerKey — never a lone sessionKey or delivery route.
    name, ownerKey, requestId, payloadFingerprint,
    parent, repository, deliveryRoute, forbiddenActions,
    pipeline: stageSpecs.map(({ stageId, stageName, pipelineIndex, runnerType, runnerProfile, activity, candidateId, fallbacks, audit, auditContractHash }) => ({ stageId, stageName, pipelineIndex, runnerType, runnerProfile, activity, candidateId, fallbacks, audit, auditContractHash })),
  };
  await mkdir(P.journalDir(root, workflowId), { recursive: true, mode: 0o700 });
  await mkdir(P.stagesDir(root, workflowId), { recursive: true, mode: 0o700 });
  return withWorkflowLock(root, workflowId, async () => {
    // journal entry 000001 = workflow_created (carries the canonical header → workflow.json is rebuildable)
    await writeJournal(root, workflowId, {
      version: WORKFLOW_SCHEMA_VERSION, seq: 1, transitionId: randomUUID(), operation: "workflow_created",
      workflowId, stageId: null, attempt: null, fromState: null, toState: null,
      status: "COMMITTED", createdAt, resolvedAt: createdAt, resolution: null, header,
    });
    for (const spec of stageSpecs) {
      await mkdir(P.attemptsDir(root, workflowId, spec.stageId), { recursive: true, mode: 0o700 });
      await atomicWriteJson(P.attemptFile(root, workflowId, spec.stageId, 1), initialAttempt({ workflowId, attempt: 1, ...spec }));
      await rebuildStageProjection(root, workflowId, spec.stageId);
    }
    await rebuildWorkflowProjection(root, workflowId);
    return workflowId;
  });
}

export async function readWorkflow(root, workflowId) {
  const wf = await readJsonOrNull(P.workflowJson(root, workflowId));
  return wf ? assertWorkflowSchema(wf, "workflow.json") : null;
}

// Enumerate workflow directory IDs from the filesystem — independent of whether workflow.json (a rebuildable
// projection) is present/readable. The reconciler uses this so a workflow with a missing/corrupt projection
// is still discovered and rebuilt from its journal.
export async function listWorkflowIds(root) {
  const dir = P.workflowsRoot(root);
  return (await readdir(dir, { withFileTypes: true }).catch(() => []))
    .filter((d) => d.isDirectory() && WORKFLOW_ID_RE.test(d.name))
    .map((d) => d.name);
}

export async function listWorkflows(root) {
  const ids = await listWorkflowIds(root);
  const out = [];
  for (const id of ids) {
    const wf = await readWorkflow(root, id).catch(() => null);
    if (wf) out.push(wf);
  }
  return out.sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
}

export async function readStageAttempt(root, workflowId, stageId, attempt) {
  const rec = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, attempt));
  return rec ? assertWorkflowSchema(rec, `attempt ${attempt}`) : null;
}

export async function readStageProjection(root, workflowId, stageId) {
  const proj = await readJsonOrNull(P.stageJson(root, workflowId, stageId));
  return proj ? assertWorkflowSchema(proj, "stage.json") : null;
}

export async function createStageAttempt(root, workflowId, spec) {
  return withWorkflowLock(root, workflowId, async () => {
    const stageId = assertStageId(spec.stageId);
    const attempt = spec.attempt;
    await mkdir(P.attemptsDir(root, workflowId, stageId), { recursive: true, mode: 0o700 });
    if (await readJsonOrNull(P.attemptFile(root, workflowId, stageId, attempt))) {
      throw wfError("STAGE_ATTEMPT_EXISTS", `attempt ${attempt} already exists for ${stageId}`);
    }
    await atomicWriteJson(P.attemptFile(root, workflowId, stageId, attempt), initialAttempt({ workflowId, stageId, attempt, ...spec }));
    await rebuildStageProjection(root, workflowId, stageId);
    await rebuildWorkflowProjection(root, workflowId);
    return readStageAttempt(root, workflowId, stageId, attempt);
  });
}

// Lock-free transition core — the caller MUST already hold the workflow lock (the mkdir lock is not
// re-entrant). Validates the direction, journals the intent, atomically updates the canonical attempt, and
// rebuilds projections. `mutation` merges additional canonical fields (jobId, processState, providerState,
// jobOutcome, failureReason, startedAt, finishedAt).
async function applyStageTransition(root, workflowId, { stageId, attempt, toState, mutation = {} }) {
  if (!STAGE_STATES.has(toState)) throw wfError("STAGE_STATE_INVALID", `unknown stageState: ${toState}`);
  const file = P.attemptFile(root, workflowId, stageId, attempt);
  const record = await readJsonOrNull(file);
  if (!record) throw wfError("STAGE_ATTEMPT_NOT_FOUND", `${stageId} attempt ${attempt}`);
  assertWorkflowSchema(record, `attempt ${attempt}`);
  const fromState = record.stageState;
  // idempotent same-state re-apply: merge mutation, no transition journal.
  if (fromState === toState) {
    const merged = { ...record, ...mutation, stageState: toState, updatedAt: nowIso() };
    await atomicWriteJson(file, merged);
    await rebuildStageProjection(root, workflowId, stageId);
    await rebuildWorkflowProjection(root, workflowId);
    return merged;
  }
  if (TERMINAL_STAGE_STATES.has(fromState)) throw wfError("STAGE_TERMINAL", `cannot transition terminal ${fromState}→${toState}`);
  if (!isAllowedStageTransition(fromState, toState)) throw wfError("STAGE_TRANSITION_ILLEGAL", `illegal ${fromState}→${toState}`);

  // journalled transition: PENDING intent → canonical update → COMMITTED → rebuild projections.
  const seq = await nextJournalSeq(root, workflowId);
  const transitionId = randomUUID();
  const at = nowIso();
  await writeJournal(root, workflowId, {
    version: WORKFLOW_SCHEMA_VERSION, seq, transitionId, operation: "stage_transition",
    workflowId, stageId, attempt, fromState, toState, status: "PENDING", createdAt: at, resolvedAt: null, resolution: null,
  });
  const next = { ...record, ...mutation, stageState: toState, updatedAt: nowIso() };
  if (!next.startedAt && (toState === "RUNNING")) next.startedAt = nowIso();
  if (TERMINAL_STAGE_STATES.has(toState) && !next.finishedAt) next.finishedAt = nowIso();
  await atomicWriteJson(file, next);
  await writeJournal(root, workflowId, {
    version: WORKFLOW_SCHEMA_VERSION, seq, transitionId, operation: "stage_transition",
    workflowId, stageId, attempt, fromState, toState, status: "COMMITTED", createdAt: at, resolvedAt: nowIso(), resolution: null,
  });
  await rebuildStageProjection(root, workflowId, stageId);
  await rebuildWorkflowProjection(root, workflowId);
  return next;
}

// Transition an attempt's stageState (storage-only; validates the direction), under the workflow lock.
export async function transitionStageAttempt(root, workflowId, params) {
  return withWorkflowLock(root, workflowId, () => applyStageTransition(root, workflowId, params));
}

// P3-D active/terminal stage states (used for frontier + linear-invariant checks).
const ACTIVE_STAGE_STATES = new Set(["SUBMITTING", "RUNNING"]);
const STOP_STAGE_STATES = new Set(["UNVERIFIED", "FAILED", "BLOCKED_DEPENDENCY", "ARTIFACT_MISSING", "APPROVAL_REQUIRED", "CANCELLED"]);

// Compute the linear-pipeline frontier from CANONICAL attempt records (caller holds the lock). Fail-closed
// on any non-linear/unknown/missing shape.
async function computeFrontierLocked(root, workflowId) {
  const header = workflowCreatedHeader(await readJournalEntries(root, workflowId));
  assertWorkflowSchema(header, "workflow_created");
  const pipeline = [...header.pipeline].sort((a, b) => a.pipelineIndex - b.pipelineIndex);
  const states = [];
  for (const spec of pipeline) {
    const latest = await latestAttempt(root, workflowId, spec.stageId);
    if (!latest) throw wfError("WORKFLOW_PIPELINE_INCOMPLETE", `stage ${spec.stageId} has no canonical attempt`);
    if (!STAGE_STATES.has(latest.record.stageState)) throw wfError("WORKFLOW_PIPELINE_INVARIANT", `stage ${spec.stageId} has unknown state ${latest.record.stageState}`);
    states.push({ spec, attempt: latest.attempt, stageState: latest.record.stageState });
  }
  // at most one active (SUBMITTING/RUNNING) stage
  if (states.filter((s) => ACTIVE_STAGE_STATES.has(s.stageState)).length > 1) {
    throw wfError("WORKFLOW_PIPELINE_INVARIANT", "more than one active stage");
  }
  const firstNonPassed = states.findIndex((s) => s.stageState !== "PASSED");
  if (firstNonPassed === -1) return { status: "succeeded", pipeline, states };
  // every stage AFTER the frontier must still be PENDING (linear: no stage may advance before its predecessor PASSED)
  for (let i = firstNonPassed + 1; i < states.length; i++) {
    if (states[i].stageState !== "PENDING") {
      throw wfError("WORKFLOW_PIPELINE_INVARIANT", `stage ${states[i].spec.stageId} (${states[i].stageState}) advanced before predecessor ${states[firstNonPassed].spec.stageId} PASSED`);
    }
  }
  const frontier = states[firstNonPassed];
  if (ACTIVE_STAGE_STATES.has(frontier.stageState)) return { status: "active", frontier, pipeline, states };
  if (STOP_STAGE_STATES.has(frontier.stageState)) return { status: "stopped", frontier, pipeline, states };
  return { status: "runnable", frontier, pipeline, states }; // PENDING frontier, all predecessors PASSED
}

// Read-only frontier snapshot (acquires the lock). Never claims/mutates. Throws on invariant/incomplete.
export async function computeFrontier(root, workflowId) {
  return withWorkflowLock(root, workflowId, async () => {
    const f = await computeFrontierLocked(root, workflowId);
    return { status: f.status, frontier: f.frontier ? { stageId: f.frontier.spec.stageId, attempt: f.frontier.attempt, stageState: f.frontier.stageState } : null };
  });
}

// Atomically claim the runnable next-stage (PENDING frontier with all predecessors PASSED) as SUBMITTING,
// under the workflow lock. Returns { status, stageId?, attempt? }. status ∈
//   "claimed"   — a PENDING frontier was just claimed → SUBMITTING (caller submits the job outside the lock)
//   "active"    — the frontier is SUBMITTING/RUNNING (P3-C recovery/settle owns it)
//   "stopped"   — the frontier is UNVERIFIED/FAILED/BLOCKED/APPROVAL/CANCELLED (no advancement)
//   "succeeded" — every stage PASSED (no submission)
//   "disabled"  — a PENDING frontier exists but workflowEnabled is false (no claim)
// Throws WORKFLOW_PIPELINE_INVARIANT / WORKFLOW_PIPELINE_INCOMPLETE on a non-linear/missing shape (fail-closed).
export async function claimRunnableStage(root, workflowId, { enabled = true } = {}) {
  return withWorkflowLock(root, workflowId, async () => {
    const f = await computeFrontierLocked(root, workflowId);
    if (f.status !== "runnable") {
      const frontier = f.frontier;
      return frontier ? { status: f.status, stageId: frontier.spec.stageId, attempt: frontier.attempt, stageState: frontier.stageState } : { status: f.status };
    }
    const { spec, attempt } = f.frontier;
    if (!enabled) return { status: "disabled", stageId: spec.stageId, attempt };
    // An activity-less (P3-B-era) frontier stage is fail-closed here: it is NOT claimed (never skipped, never
    // given a fabricated command), leaving it PENDING for explicit attention.
    if (!spec.activity) throw wfError("WORKFLOW_ACTIVITY_MISSING", `runnable stage ${spec.stageId} has no activity`);
    // claim PENDING → SUBMITTING atomically (lock already held → lock-free core)
    await applyStageTransition(root, workflowId, { stageId: spec.stageId, attempt, toState: "SUBMITTING" });
    return { status: "claimed", stageId: spec.stageId, attempt };
  });
}

// ---- P3-E control plane: approve / reject / cancel / resume ----
const DECISION_SOURCE_STATES = new Set(["UNVERIFIED", "APPROVAL_REQUIRED"]);
const RESUME_SOURCE_STATES = new Set(["UNVERIFIED", "FAILED", "BLOCKED_DEPENDENCY", "ARTIFACT_MISSING", "CANCELLED"]);
const CANCELLABLE_STATES = new Set(["PENDING", "SUBMITTING", "RUNNING", "UNVERIFIED", "FAILED", "BLOCKED_DEPENDENCY", "ARTIFACT_MISSING", "APPROVAL_REQUIRED"]);

// A control record is keyed by requestId ALONE (not action:requestId): within a workflow, owner + requestId
// is a single control-request namespace, so reusing one requestId across DIFFERENT actions collides. The
// action is verified from the record content (assertControlMatch), never from the filename.
function controlDir(root, workflowId) { return path.join(P.workflowDir(root, workflowId), "control"); }
function controlFile(root, workflowId, requestId) {
  return path.join(controlDir(root, workflowId), `${createHash("sha256").update(requestId).digest("hex")}.json`);
}
async function readControlRecord(root, workflowId, requestId) {
  return readJsonOrNull(controlFile(root, workflowId, requestId));
}
function assertControlMatch(record, { action, requestId, ownerKeyHash, payloadFingerprint }) {
  if (record.action !== action || record.requestId !== requestId || record.ownerKeyHash !== ownerKeyHash || record.payloadFingerprint !== payloadFingerprint) {
    throw wfError("WORKFLOW_CONTROL_REQUEST_CONFLICT", `requestId "${requestId}" already used for a different control request (stored=${record.action}, requested=${action})`);
  }
}
async function writeControlRecord(root, workflowId, record) {
  await mkdir(controlDir(root, workflowId), { recursive: true, mode: 0o700 });
  await atomicWriteJson(controlFile(root, workflowId, record.requestId), record);
}

// Resolve the current linear frontier as the ONLY controllable target; a stale stageId/attempt (the frontier
// moved) is fail-closed so a late control click cannot act on the wrong stage/attempt.
function frontierControlTarget(f, stageId, expectedAttempt) {
  const t = f.frontier ? { stageId: f.frontier.spec.stageId, attempt: f.frontier.attempt, stageState: f.frontier.stageState, spec: f.frontier.spec } : null;
  if (!t || t.stageId !== stageId || t.attempt !== expectedAttempt) {
    throw wfError("WORKFLOW_CONTROL_STALE", `control target ${stageId}#${expectedAttempt} is not the current frontier ${t ? `${t.stageId}#${t.attempt} (${t.stageState})` : "(none)"}`);
  }
  return t;
}

// Force a stage to CANCELLED preserving its job/process history (bypasses the normal transition map, but
// never from PASSED). Lock-free core — caller holds the workflow lock.
async function applyForcedCancel(root, workflowId, { stageId, attempt, mutation }) {
  const file = P.attemptFile(root, workflowId, stageId, attempt);
  const record = await readJsonOrNull(file);
  if (!record) throw wfError("STAGE_ATTEMPT_NOT_FOUND", `${stageId} attempt ${attempt}`);
  if (record.stageState === "CANCELLED") return record; // idempotent
  if (record.stageState === "PASSED") throw wfError("STAGE_TERMINAL", "cannot cancel a PASSED stage");
  const seq = await nextJournalSeq(root, workflowId);
  const at = nowIso();
  const base = { version: WORKFLOW_SCHEMA_VERSION, seq, transitionId: randomUUID(), operation: "stage_transition", workflowId, stageId, attempt, fromState: record.stageState, toState: "CANCELLED" };
  await writeJournal(root, workflowId, { ...base, status: "PENDING", createdAt: at, resolvedAt: null, resolution: null });
  const next = { ...record, ...mutation, stageState: "CANCELLED", finishedAt: record.finishedAt ?? nowIso(), updatedAt: nowIso() };
  await atomicWriteJson(file, next);
  await writeJournal(root, workflowId, { ...base, status: "COMMITTED", createdAt: at, resolvedAt: nowIso(), resolution: null });
  await rebuildStageProjection(root, workflowId, stageId);
  await rebuildWorkflowProjection(root, workflowId);
  return next;
}

// Approve/reject the frontier stage (manual operator decision). Idempotent via a control record + the
// canonical decision.requestId (replay after a crash between the transition and the record). approve →
// PASSED (verificationSource MANUAL_APPROVAL), reject → FAILED — the process/provider/jobOutcome history is
// preserved; only stageState + decision metadata change.
export async function applyControlDecision(root, workflowId, { action, stageId, expectedAttempt, requestId, reason, actor, ownerKeyHash, payloadFingerprint }) {
  const toState = action === "approve" ? "PASSED" : "FAILED";
  return withWorkflowLock(root, workflowId, async () => {
    const existing = await readControlRecord(root, workflowId, requestId);
    if (existing) {
      assertControlMatch(existing, { action, requestId, ownerKeyHash, payloadFingerprint });
      if (existing.status === "COMPLETED") return { replayed: true, resultState: existing.resultState };
    }
    // replay after a crash between the canonical transition and the control-record write
    const already = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, expectedAttempt));
    if (already?.decision?.requestId === requestId && already.decision.action === action.toUpperCase() && already.stageState === toState) {
      await writeControlRecord(root, workflowId, controlRecordFor({ action, requestId, workflowId, stageId, attempt: expectedAttempt, ownerKeyHash, payloadFingerprint, reason, resultState: toState }));
      return { replayed: true, resultState: toState };
    }
    const f = await computeFrontierLocked(root, workflowId);
    const target = frontierControlTarget(f, stageId, expectedAttempt);
    if (!DECISION_SOURCE_STATES.has(target.stageState)) {
      throw wfError("WORKFLOW_CONTROL_NOT_ALLOWED", `${action} not allowed from ${target.stageState} (need UNVERIFIED or APPROVAL_REQUIRED)`);
    }
    const decision = { action: action.toUpperCase(), source: "MANUAL", requestId, reason, decidedAt: nowIso(), actorAgentId: actor?.agentId ?? null, actorOwnerKeyHash: ownerKeyHash };
    await applyStageTransition(root, workflowId, { stageId, attempt: expectedAttempt, toState, mutation: { decision, verificationSource: action === "approve" ? "MANUAL_APPROVAL" : "MANUAL_REJECTION" } });
    await writeControlRecord(root, workflowId, controlRecordFor({ action, requestId, workflowId, stageId, attempt: expectedAttempt, ownerKeyHash, payloadFingerprint, reason, resultState: toState }));
    return { replayed: false, resultState: toState };
  });
}

// Record a cancel decision on the frontier. Non-active frontiers (PENDING / stopped) are transitioned to
// CANCELLED immediately; an active frontier (SUBMITTING/RUNNING) only records the cancelRequest and returns
// its jobId so the caller cancels the durable job OUTSIDE the lock, then calls finishStageCancel.
export async function recordCancel(root, workflowId, { stageId, expectedAttempt, requestId, reason, actor, ownerKeyHash, payloadFingerprint }) {
  return withWorkflowLock(root, workflowId, async () => {
    const existing = await readControlRecord(root, workflowId, requestId);
    if (existing) {
      assertControlMatch(existing, { action: "cancel", requestId, ownerKeyHash, payloadFingerprint });
      if (existing.status === "COMPLETED") {
        const rec = await readJsonOrNull(P.attemptFile(root, workflowId, existing.stageId, existing.attempt));
        return { replayed: true, stageState: rec?.stageState ?? null, jobId: rec?.jobId ?? null, needsJobCancel: rec?.stageState === "SUBMITTING" || rec?.stageState === "RUNNING", activityIdempotencyKey: rec?.activityIdempotencyKey ?? null };
      }
    }
    const f = await computeFrontierLocked(root, workflowId);
    const target = frontierControlTarget(f, stageId, expectedAttempt);
    if (!CANCELLABLE_STATES.has(target.stageState)) throw wfError("WORKFLOW_CONTROL_NOT_ALLOWED", `cancel not allowed from ${target.stageState}`);
    const cancelRequest = { requestId, reason, requestedAt: nowIso(), actorAgentId: actor?.agentId ?? null, actorOwnerKeyHash: ownerKeyHash, status: "REQUESTED" };
    const active = target.stageState === "SUBMITTING" || target.stageState === "RUNNING";
    // record cancelRequest (additive) via an idempotent same-state merge; no state change for active stages
    await applyStageTransition(root, workflowId, { stageId, attempt: expectedAttempt, toState: target.stageState, mutation: { cancelRequest } });
    const attemptRec = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, expectedAttempt));
    let stageState = target.stageState;
    if (!active) {
      const terminalSource = TERMINAL_STAGE_STATES.has(target.stageState) || target.stageState === "UNVERIFIED";
      await applyForcedCancel(root, workflowId, { stageId, attempt: expectedAttempt, mutation: { cancelRequest: { ...cancelRequest, status: "COMPLETED" }, ...(terminalSource ? { cancelledAfterTerminal: true } : {}) } });
      stageState = "CANCELLED";
    }
    await writeControlRecord(root, workflowId, controlRecordFor({ action: "cancel", requestId, workflowId, stageId, attempt: expectedAttempt, ownerKeyHash, payloadFingerprint, reason, resultState: stageState }));
    return { replayed: false, stageState, jobId: attemptRec?.jobId ?? null, needsJobCancel: active, activityIdempotencyKey: attemptRec?.activityIdempotencyKey ?? null };
  });
}

// After the durable job is cancelled (outside the lock), converge an active frontier to CANCELLED, preserving
// the job's real terminal history (marks cancelledAfterTerminal when the job had already finished).
export async function finishStageCancel(root, workflowId, { stageId, attempt, jobTerminalOutcome }) {
  return withWorkflowLock(root, workflowId, async () => {
    const rec = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, attempt));
    if (!rec || rec.stageState === "CANCELLED") return rec;
    const cancelledAfterTerminal = jobTerminalOutcome != null && jobTerminalOutcome !== "CANCELLED";
    const cancelRequest = rec.cancelRequest ? { ...rec.cancelRequest, status: "COMPLETED" } : null;
    return applyForcedCancel(root, workflowId, { stageId, attempt, mutation: { ...(cancelRequest ? { cancelRequest } : {}), ...(cancelledAfterTerminal ? { cancelledAfterTerminal: true } : {}) } });
  });
}

// Create attempt N+1 for a stopped frontier stage (manual same-stage rerun). Preserves attempt N byte-for-byte;
// the new attempt is PENDING with a fresh deterministic key and resume metadata. NOT a checkpoint-verified resume.
export async function createResumeAttempt(root, workflowId, { stageId, expectedAttempt, requestId, reason, actor, ownerKeyHash, payloadFingerprint, checkpointPolicy = "manual_rerun", expectedCheckpointHash = null }) {
  return withWorkflowLock(root, workflowId, async () => {
    const existing = await readControlRecord(root, workflowId, requestId);
    if (existing) {
      assertControlMatch(existing, { action: "resume", requestId, ownerKeyHash, payloadFingerprint });
      if (existing.status === "COMPLETED") return { replayed: true, newAttempt: existing.newAttempt };
    }
    const newAttempt = expectedAttempt + 1;
    // replay after a crash between attempt-creation and the control-record write
    const alreadyNew = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, newAttempt));
    if (alreadyNew?.resume?.requestId === requestId) {
      await writeControlRecord(root, workflowId, controlRecordFor({ action: "resume", requestId, workflowId, stageId, attempt: expectedAttempt, ownerKeyHash, payloadFingerprint, reason, resultState: "PENDING", newAttempt }));
      return { replayed: true, newAttempt };
    }
    const f = await computeFrontierLocked(root, workflowId);
    const target = frontierControlTarget(f, stageId, expectedAttempt);
    if (!RESUME_SOURCE_STATES.has(target.stageState)) throw wfError("WORKFLOW_RESUME_NOT_ALLOWED", `resume not allowed from ${target.stageState}`);
    const header = workflowCreatedHeader(await readJournalEntries(root, workflowId));
    const spec = header.pipeline.find((s) => s.stageId === stageId);
    // #6: a resume (manual OR checkpoint) re-runs the SOURCE attempt's execution candidate — it never
    // silently reverts to the primary (candidate 0). PROVIDER_FALLBACK is the only trigger that advances the
    // candidate index (candidateOf + 1).
    const src = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, expectedAttempt));
    const srcCandidate = candidateOf(spec, src?.executionCandidate?.candidateIndex ?? 0) ?? candidateOf(spec, 0);
    // #2: compute the full canonical record FIRST, then journal PENDING with attemptRecord + discriminators,
    // so a crash between PENDING and the canonical write recreates the exact record on recovery (no guessing).
    const attempt = initialAttempt({ workflowId, stageId, stageName: spec.stageName, pipelineIndex: spec.pipelineIndex, attempt: newAttempt, runnerType: srcCandidate.runnerType, runnerProfile: srcCandidate.runnerProfile, candidateIndex: srcCandidate.candidateIndex, candidateId: srcCandidate.candidateId });
    const safe = checkpointPolicy === "require_match";
    attempt.resume = { resumeOfAttempt: expectedAttempt, resumeMode: safe ? "CHECKPOINT_RERUN" : "MANUAL_RERUN", checkpointVerified: safe, requestId, reason, resumedAt: nowIso(), resumedByAgentId: actor?.agentId ?? null, resumedByOwnerKeyHash: ownerKeyHash };
    if (safe) attempt.checkpoint = { expectedBeforeHash: expectedCheckpointHash };
    const seq = await nextJournalSeq(root, workflowId);
    const at = nowIso();
    const base = { version: WORKFLOW_SCHEMA_VERSION, seq, transitionId: randomUUID(), operation: "resume_attempt", workflowId, stageId, attempt: newAttempt, fromState: null, toState: "PENDING", trigger: "resume", sourceAttempt: expectedAttempt, candidateIndex: srcCandidate.candidateIndex, candidateId: srcCandidate.candidateId ?? null, attemptRecord: attempt };
    await writeJournal(root, workflowId, { ...base, status: "PENDING", createdAt: at, resolvedAt: null, resolution: null });
    await atomicWriteJson(P.attemptFile(root, workflowId, stageId, newAttempt), attempt);
    await writeJournal(root, workflowId, { ...base, status: "COMMITTED", createdAt: at, resolvedAt: nowIso(), resolution: null });
    await rebuildStageProjection(root, workflowId, stageId);
    await rebuildWorkflowProjection(root, workflowId);
    await writeControlRecord(root, workflowId, controlRecordFor({ action: "resume", requestId, workflowId, stageId, attempt: expectedAttempt, ownerKeyHash, payloadFingerprint, reason, resultState: "PENDING", newAttempt }));
    return { replayed: false, newAttempt };
  });
}

// ---- P3-F execution candidates + preflight/checkpoint + fallback attempts ----
// Resolve the Nth execution candidate of a stage spec (0 = primary; 1.. = declared fallbacks).
export function candidateOf(spec, candidateIndex) {
  if (candidateIndex === 0) return { candidateIndex: 0, candidateId: spec.candidateId ?? null, runnerType: spec.runnerType ?? null, runnerProfile: spec.runnerProfile ?? null, activity: spec.activity ?? null };
  const fb = (spec.fallbacks ?? [])[candidateIndex - 1];
  if (!fb) return null;
  return { candidateIndex, candidateId: fb.candidateId ?? null, runnerType: fb.runnerType ?? null, runnerProfile: fb.runnerProfile ?? null, activity: fb.activity ?? null };
}
export function candidateCount(spec) { return 1 + (spec.fallbacks?.length ?? 0); }

// Journaled preflight result: the preflight status, the pre-execution checkpoint (COMPLETE fingerprint), and
// the frozen toolchain fingerprint converge ATOMICALLY on the canonical attempt (a preflight_pass journal
// intent → attempt write → commit). This removes the crash window where a PASSED preflight lacks its
// checkpoint.before/frozen toolchain (a cached PASS is only trusted when BOTH are present). No state change.
export async function commitPreflightResult(root, workflowId, { stageId, attempt, preflight, checkpointBefore = null, toolchain = null }) {
  return withWorkflowLock(root, workflowId, async () => {
    const file = P.attemptFile(root, workflowId, stageId, attempt);
    const rec = await readJsonOrNull(file);
    if (!rec) throw wfError("STAGE_ATTEMPT_NOT_FOUND", `${stageId} attempt ${attempt}`);
    const cp = rec.checkpoint ?? {};
    const next = {
      ...rec,
      preflight: { ...(rec.preflight ?? {}), ...preflight, ...(toolchain ? { frozenToolchain: toolchain } : {}) },
      // never overwrite an already-captured checkpoint.before
      checkpoint: checkpointBefore && !cp.before ? { ...cp, before: checkpointBefore } : cp,
      updatedAt: nowIso(),
    };
    const seq = await nextJournalSeq(root, workflowId);
    const at = nowIso();
    // #3: the journal entry carries the DECISIVE hashes it is about to apply so a crash can be recovered by
    // comparing the intent against the canonical attempt (match → COMMIT; canonical missing → revert/re-check;
    // canonical differs → fail-closed) — never "COMMIT because stageState is unchanged".
    const h = preflightIntentHashes(next);
    const base = { version: WORKFLOW_SCHEMA_VERSION, seq, transitionId: randomUUID(), operation: "preflight_result", workflowId, stageId, attempt, fromState: rec.stageState, toState: rec.stageState, preflightPayloadHash: h.payload, checkpointBeforeHash: h.checkpointBefore, frozenToolchainHash: h.frozenToolchain };
    await writeJournal(root, workflowId, { ...base, status: "PENDING", createdAt: at, resolvedAt: null, resolution: null });
    await atomicWriteJson(file, next);
    await writeJournal(root, workflowId, { ...base, status: "COMMITTED", createdAt: at, resolvedAt: nowIso(), resolution: null });
    await rebuildStageProjection(root, workflowId, stageId);
    return next;
  });
}

// The decisive, stable fingerprint of a preflight application: the resolved verdict fields plus the pre-exec
// checkpoint hash and the frozen-toolchain hash (the two values a cached PASS is only trusted with). Computed
// identically at commit-time (from the record about to be written) and at recovery-time (from the canonical
// record), so reconcileWorkflow can decide match/missing/differ without re-running the probe.
function preflightIntentHashes(rec) {
  const pf = rec?.preflight ?? {};
  const payload = pf.status == null
    ? null
    : createHash("sha256").update(JSON.stringify({ status: pf.status, failureCode: pf.failureCode ?? null, providerCapability: pf.providerCapability ?? null })).digest("hex");
  return { payload, checkpointBefore: rec?.checkpoint?.before?.aggregateHash ?? null, frozenToolchain: pf.frozenToolchain?.aggregateHash ?? null };
}

// Capture a checkpoint fingerprint (before/after) on an attempt. A captured checkpoint is NEVER overwritten
// (a post-terminal worktree change must not silently replace the recorded terminal checkpoint).
export async function recordCheckpoint(root, workflowId, { stageId, attempt, phase, checkpoint }) {
  return withWorkflowLock(root, workflowId, async () => {
    const file = P.attemptFile(root, workflowId, stageId, attempt);
    const rec = await readJsonOrNull(file);
    if (!rec) return null;
    const cp = rec.checkpoint ?? {};
    if (cp[phase]) return rec; // already captured — preserve it
    await atomicWriteJson(file, { ...rec, checkpoint: { ...cp, [phase]: checkpoint }, updatedAt: nowIso() });
    return rec;
  });
}
export async function readCheckpoint(root, workflowId, stageId, attempt) {
  return (await readJsonOrNull(P.attemptFile(root, workflowId, stageId, attempt)))?.checkpoint ?? null;
}

// Lock-free core: append attempt N+1 for a stage (preserving attempt N), with a chosen candidate + metadata.
// #2: the intended canonical attempt record is computed FIRST and embedded in the PENDING journal entry
// (attemptRecord) alongside its discriminators (trigger/sourceAttempt/candidateIndex/candidateId), so a crash
// between PENDING and the canonical write can be recovered deterministically — recreate the exact record, no
// guessing. `trigger` is "fallback" here (provider fallback); resume uses createResumeAttempt.
async function appendAttemptLocked(root, workflowId, { stageId, newAttempt, candidate, metadata, trigger = "fallback", sourceAttempt = null }) {
  const header = workflowCreatedHeader(await readJournalEntries(root, workflowId));
  const spec = header.pipeline.find((s) => s.stageId === stageId);
  const rec = initialAttempt({ workflowId, stageId, stageName: spec.stageName, pipelineIndex: spec.pipelineIndex, attempt: newAttempt, runnerType: candidate.runnerType, runnerProfile: candidate.runnerProfile, candidateIndex: candidate.candidateIndex, candidateId: candidate.candidateId });
  Object.assign(rec, metadata);
  const seq = await nextJournalSeq(root, workflowId);
  const at = nowIso();
  const base = { version: WORKFLOW_SCHEMA_VERSION, seq, transitionId: randomUUID(), operation: "next_attempt", workflowId, stageId, attempt: newAttempt, fromState: null, toState: "PENDING", trigger, sourceAttempt, candidateIndex: candidate.candidateIndex, candidateId: candidate.candidateId ?? null, attemptRecord: rec };
  await writeJournal(root, workflowId, { ...base, status: "PENDING", createdAt: at, resolvedAt: null, resolution: null });
  await atomicWriteJson(P.attemptFile(root, workflowId, stageId, newAttempt), rec);
  await writeJournal(root, workflowId, { ...base, status: "COMMITTED", createdAt: at, resolvedAt: nowIso(), resolution: null });
  await rebuildStageProjection(root, workflowId, stageId);
  await rebuildWorkflowProjection(root, workflowId);
  return rec;
}

// Automatic provider-fallback attempt: attempt N+1 running the next candidate, only from the store's POV a
// straightforward append (the reconciler enforces the checkpoint-verified + fallback-eligibility policy
// before calling). Idempotent: a re-run that finds N+1 already a fallback of N replays.
export async function createFallbackAttempt(root, workflowId, { stageId, expectedAttempt, candidateIndex, fallbackReason, expectedCheckpointHash, fromCandidateIndex }) {
  return withWorkflowLock(root, workflowId, async () => {
    const newAttempt = expectedAttempt + 1;
    const already = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, newAttempt));
    if (already?.fallback?.fallbackFromAttempt === expectedAttempt) return { replayed: true, newAttempt, candidateIndex: already.executionCandidate?.candidateIndex };
    const target = frontierControlTarget(await computeFrontierLocked(root, workflowId), stageId, expectedAttempt);
    const spec = workflowCreatedHeader(await readJournalEntries(root, workflowId)).pipeline.find((s) => s.stageId === stageId);
    const candidate = candidateOf(spec, candidateIndex);
    if (!candidate || !candidate.activity) throw wfError("WORKFLOW_FALLBACK_EXHAUSTED", `no fallback candidate ${candidateIndex} for ${stageId}`);
    await appendAttemptLocked(root, workflowId, {
      stageId, newAttempt, candidate, trigger: "fallback", sourceAttempt: expectedAttempt,
      metadata: {
        fallback: { fallbackFromAttempt: expectedAttempt, fallbackFromCandidateIndex: fromCandidateIndex ?? candidateIndex - 1, selectedCandidateIndex: candidateIndex, reason: fallbackReason, resumeMode: "PROVIDER_FALLBACK", checkpointVerified: true },
        checkpoint: expectedCheckpointHash ? { expectedBeforeHash: expectedCheckpointHash } : null,
      },
    });
    return { replayed: false, newAttempt, candidateIndex };
  });
}

// #2 atomic fallback decision — primitive shared by BOTH fallback triggers (a terminal FAILED_PROVIDER and a
// preflight-time provider BLOCK). settleWithFallbackIntent settles the SOURCE attempt's verdict AND embeds a
// durable PENDING `fallbackIntent` in the SAME canonical attempt write, so a crash can never leave the source
// settled without a recorded fallback obligation. consumeFallbackIntent then creates attempt N+1 exactly once
// and marks the intent CONSUMED. The reconciler's recovery scan (reconcileWorkflow) finishes any intent whose
// N+1 was interrupted — no permanently-broken state, no duplicate N+1, no candidate skip, attempt N preserved.
export async function settleWithFallbackIntent(root, workflowId, { stageId, attempt, sourceToState, sourceMutation = {}, candidateIndex, fromCandidateIndex, fallbackReason, expectedCheckpointHash }) {
  return withWorkflowLock(root, workflowId, async () => {
    const rec = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, attempt));
    if (!rec) throw wfError("STAGE_ATTEMPT_NOT_FOUND", `${stageId} attempt ${attempt}`);
    const existing = rec.fallbackIntent;
    // preserve an already-recorded intent for this candidate (idempotent re-settle); never revert CONSUMED.
    const intent = existing && existing.candidateIndex === candidateIndex
      ? existing
      : { status: "PENDING", candidateIndex, fromCandidateIndex: fromCandidateIndex ?? candidateIndex - 1, reason: fallbackReason ?? null, expectedCheckpointHash: expectedCheckpointHash ?? null, recordedAt: nowIso() };
    return applyStageTransition(root, workflowId, { stageId, attempt, toState: sourceToState, mutation: { ...sourceMutation, fallbackIntent: intent } });
  });
}

export async function consumeFallbackIntent(root, workflowId, { stageId, attempt }) {
  return withWorkflowLock(root, workflowId, () => consumeFallbackIntentLocked(root, workflowId, { stageId, attempt }));
}

// Lock-free core (caller holds the workflow lock). Creates N+1 (idempotent) then marks the intent CONSUMED.
async function consumeFallbackIntentLocked(root, workflowId, { stageId, attempt }) {
  const file = P.attemptFile(root, workflowId, stageId, attempt);
  const rec = await readJsonOrNull(file);
  const intent = rec?.fallbackIntent;
  if (!intent || intent.status !== "PENDING") return { consumed: false };
  const newAttempt = attempt + 1;
  const already = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, newAttempt));
  if (already?.fallback?.fallbackFromAttempt !== attempt) {
    const spec = workflowCreatedHeader(await readJournalEntries(root, workflowId)).pipeline.find((s) => s.stageId === stageId);
    const candidate = candidateOf(spec, intent.candidateIndex);
    if (!candidate || !candidate.activity) throw wfError("WORKFLOW_FALLBACK_EXHAUSTED", `no fallback candidate ${intent.candidateIndex} for ${stageId}`);
    await appendAttemptLocked(root, workflowId, {
      stageId, newAttempt, candidate, trigger: "fallback", sourceAttempt: attempt,
      metadata: {
        fallback: { fallbackFromAttempt: attempt, fallbackFromCandidateIndex: intent.fromCandidateIndex, selectedCandidateIndex: intent.candidateIndex, reason: intent.reason, resumeMode: "PROVIDER_FALLBACK", checkpointVerified: true },
        checkpoint: intent.expectedCheckpointHash ? { expectedBeforeHash: intent.expectedCheckpointHash } : null,
      },
    });
  }
  const fresh = await readJsonOrNull(file);
  await atomicWriteJson(file, { ...fresh, fallbackIntent: { ...intent, status: "CONSUMED", consumedAt: nowIso() }, updatedAt: nowIso() });
  await rebuildStageProjection(root, workflowId, stageId);
  return { consumed: true, newAttempt };
}

// Scan every pipeline stage's attempts for a PENDING fallbackIntent. The live reconciler (advanceWorkflowOnce)
// uses this to consume interrupted fallback decisions — but ONLY when workflowEnabled (a submission decision).
export async function pendingFallbackIntents(root, workflowId) {
  return withWorkflowLock(root, workflowId, () => pendingFallbackIntentsLocked(root, workflowId));
}
async function pendingFallbackIntentsLocked(root, workflowId) {
  const header = workflowCreatedHeader(await readJournalEntries(root, workflowId));
  const out = [];
  for (const spec of header.pipeline) {
    const dir = P.attemptsDir(root, workflowId, spec.stageId);
    const files = (await readdir(dir).catch(() => [])).filter((f) => /^\d{4}\.json$/.test(f)).sort();
    for (const f of files) {
      const rec = await readJsonOrNull(path.join(dir, f));
      if (rec?.fallbackIntent?.status === "PENDING") out.push({ stageId: spec.stageId, attempt: Number(f.slice(0, 4)) });
    }
  }
  return out;
}

function controlRecordFor({ action, requestId, workflowId, stageId, attempt, ownerKeyHash, payloadFingerprint, reason, resultState, newAttempt }) {
  return {
    version: WORKFLOW_SCHEMA_VERSION, controlId: randomUUID(), action, requestId, workflowId, stageId, attempt,
    ownerKeyHash, payloadFingerprint, reason, status: "COMPLETED", requestedAt: nowIso(), completedAt: nowIso(),
    resultState, ...(newAttempt !== undefined ? { newAttempt } : {}),
  };
}

// Read-only: is there a pending cancelRequest on the current frontier attempt? Used by advancement to refuse
// new submissions for a cancelling workflow.
export async function readFrontierCancelRequest(root, workflowId, stageId, attempt) {
  const rec = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, attempt));
  return rec?.cancelRequest ?? null;
}

// ---- P3-E approval-request outbox (send an APPROVAL_REQUIRED notice at most once, separate from the
// terminal notice, reusing the SENDING-lease / DELIVERY_UNKNOWN safety of the delivery outbox) ----
export function approvalKey(workflowId, stageId, attempt) {
  return `wf:${workflowId}:approval:${stageId}:attempt:${attempt}`;
}
function approvalDir(root, workflowId) { return path.join(P.workflowDir(root, workflowId), "approvals"); }
function approvalFile(root, workflowId, stageId, attempt) { return path.join(approvalDir(root, workflowId), `${assertStageId(stageId)}-${attempt}.json`); }
export async function readApprovalRecord(root, workflowId, stageId, attempt) {
  return readJsonOrNull(approvalFile(root, workflowId, stageId, attempt));
}

// Claim the approval send under the workflow lock: at most one in-flight send; a lease-expired SENDING (an
// ambiguous crash around the send) is parked DELIVERY_UNKNOWN, never blind-resent; an exhausted retry budget
// is likewise parked. Returns { claim, record }.
export async function claimApprovalSend(root, workflowId, stageId, attempt, { leaseMs = 30_000, maxAttempts = 8 } = {}) {
  return withWorkflowLock(root, workflowId, async () => {
    const file = approvalFile(root, workflowId, stageId, attempt);
    const rec = await readJsonOrNull(file);
    if (rec?.status === "SENT" || rec?.status === "DELIVERY_UNKNOWN") return { claim: false, record: rec };
    const write = async (value) => { await mkdir(approvalDir(root, workflowId), { recursive: true, mode: 0o700 }); await atomicWriteJson(file, value); return value; };
    if (rec?.status === "SENDING") {
      if (Date.now() - new Date(rec.claimedAt).getTime() < leaseMs) return { claim: false, record: rec };
      return { claim: false, record: await write({ ...rec, status: "DELIVERY_UNKNOWN" }) }; // lease expired → park
    }
    const attempts = (rec?.attempts ?? 0) + 1;
    if (attempts > maxAttempts) return { claim: false, record: await write({ key: approvalKey(workflowId, stageId, attempt), status: "DELIVERY_UNKNOWN", attempts, claimedAt: rec?.claimedAt ?? nowIso() }) };
    return { claim: true, record: await write({ key: approvalKey(workflowId, stageId, attempt), status: "SENDING", attempts, claimedAt: nowIso(), lastError: null }) };
  });
}
export async function markApprovalSent(root, workflowId, stageId, attempt) {
  return withWorkflowLock(root, workflowId, async () => {
    const file = approvalFile(root, workflowId, stageId, attempt);
    const rec = await readJsonOrNull(file);
    if (!rec || rec.status !== "SENDING") return rec;
    await atomicWriteJson(file, { ...rec, status: "SENT", sentAt: nowIso() });
  });
}
// A send error: only a PROVEN pre-send rejection is retried (→ PENDING). An ambiguous outcome (the notice
// may already have posted) is parked DELIVERY_UNKNOWN and NEVER auto-resent — same policy as the terminal
// delivery outbox / P1 fallback. `retryable` is classified by the caller via the shared seam.
export async function markApprovalError(root, workflowId, stageId, attempt, { retryable, message }) {
  return withWorkflowLock(root, workflowId, async () => {
    const file = approvalFile(root, workflowId, stageId, attempt);
    const rec = await readJsonOrNull(file);
    if (!rec || rec.status !== "SENDING") return rec;
    await atomicWriteJson(file, { ...rec, status: retryable ? "PENDING" : "DELIVERY_UNKNOWN", lastError: String(message).slice(0, 500), lastAttemptAt: nowIso() });
  });
}

// ---- P3-G Supervisor Audit Gate: canonical audit request + decision + outboxes ----

// Create the audit request bound to the EXACT canonical target (journaled). Idempotent: a re-request for the
// same attempt returns the existing record (no duplicate). Only a UNVERIFIED frontier attempt is auditable.
export async function requestAudit(root, workflowId, { stageId, attempt, auditRequestId, mode, target, contractHash }) {
  return withWorkflowLock(root, workflowId, async () => {
    const file = P.attemptFile(root, workflowId, stageId, attempt);
    const rec = await readJsonOrNull(file);
    if (!rec) throw wfError("STAGE_ATTEMPT_NOT_FOUND", `${stageId} attempt ${attempt}`);
    if (rec.audit?.auditRequestId) return { requested: false, auditRequestId: rec.audit.auditRequestId };
    if (rec.stageState !== "UNVERIFIED") throw wfError("WORKFLOW_AUDIT_NOT_ALLOWED", `audit only from UNVERIFIED (got ${rec.stageState})`);
    const seq = await nextJournalSeq(root, workflowId);
    const at = nowIso();
    const base = { version: WORKFLOW_SCHEMA_VERSION, seq, transitionId: randomUUID(), operation: "audit_request", workflowId, stageId, attempt, fromState: rec.stageState, toState: rec.stageState, auditRequestId };
    await writeJournal(root, workflowId, { ...base, status: "PENDING", createdAt: at, resolvedAt: null, resolution: null });
    const audit = { mode, status: "REQUESTED", auditRequestId, requestedAt: at, target, contractHash, decision: null };
    await atomicWriteJson(file, { ...rec, audit, updatedAt: nowIso() });
    await writeJournal(root, workflowId, { ...base, status: "COMMITTED", createdAt: at, resolvedAt: nowIso(), resolution: null });
    await rebuildStageProjection(root, workflowId, stageId);
    await rebuildWorkflowProjection(root, workflowId);
    return { requested: true, auditRequestId };
  });
}

// Apply the auditor's decision with FULL stale re-validation against canonical state. The caller has already
// authorized the trusted auditor, validated the payload, enforced required-check sufficiency for PASS, and
// (optionally) captured the current worktree hash + auditor-mutation flag. Fail-closes on any binding drift;
// DOWNGRADES a PASS to APPROVAL_REQUIRED when the worktree changed or the auditor mutated the target.
export function jobOutcomeSummaryHash(job) {
  if (!job) return null;
  return createHash("sha256").update(JSON.stringify([job.jobOutcome ?? null, job.processState ?? null, job.providerState ?? null])).digest("hex");
}
export async function applyAuditDecision(root, workflowId, { stageId, expectedAttempt, auditRequestId, verdict, verificationSource, decision, requestId, ownerKeyHash, payloadFingerprint, reason, actor, currentCheckpoint = null, readJob = null }) {
  return withWorkflowLock(root, workflowId, async () => {
    const existing = await readControlRecord(root, workflowId, requestId);
    if (existing) {
      assertControlMatch(existing, { action: "audit_decide", requestId, ownerKeyHash, payloadFingerprint });
      if (existing.status === "COMPLETED") return { replayed: true, resultState: existing.resultState };
    }
    const f = await computeFrontierLocked(root, workflowId);
    const t0 = frontierControlTarget(f, stageId, expectedAttempt);
    const rec = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, expectedAttempt));
    if (!rec || !rec.audit || rec.audit.auditRequestId !== auditRequestId) throw wfError("WORKFLOW_AUDIT_REQUEST_NOT_FOUND", `no matching audit request for ${stageId} attempt ${expectedAttempt}`);
    if (rec.audit.decision && rec.audit.decision.requestId === requestId) {
      await writeControlRecord(root, workflowId, controlRecordFor({ action: "audit_decide", requestId, workflowId, stageId, attempt: expectedAttempt, ownerKeyHash, payloadFingerprint, reason: reason ?? decision.summary, resultState: rec.stageState }));
      return { replayed: true, resultState: rec.stageState };
    }
    const spec = workflowCreatedHeader(await readJournalEntries(root, workflowId)).pipeline.find((s) => s.stageId === stageId);
    const t = rec.audit.target ?? {};
    const stale = t0.attempt !== expectedAttempt
      || rec.stageState !== "UNVERIFIED"
      || !["REQUESTED", "RUNNING"].includes(rec.audit.status)
      || (rec.cancelRequest && rec.cancelRequest.status !== "COMPLETED")
      || (rec.jobId ?? null) !== (t.jobId ?? null)
      || (rec.activityIdempotencyKey ?? null) !== (t.activityIdempotencyKey ?? null)
      || (rec.jobOutcome ?? null) !== (t.jobOutcome ?? null)
      // #3: the canonical checkpoint.after status/complete/hash must still equal the frozen audit target
      || (rec.checkpoint?.after?.status ?? null) !== (t.checkpointAfter?.status ?? null)
      || (rec.checkpoint?.after?.complete ?? null) !== (t.checkpointAfter?.complete ?? null)
      || (rec.checkpoint?.after?.aggregateHash ?? null) !== (t.checkpointAfter?.aggregateHash ?? null)
      || (spec?.auditContractHash ?? null) !== (rec.audit.contractHash ?? null);
    if (stale) throw wfError("WORKFLOW_AUDIT_STALE", `audit target changed for ${stageId} attempt ${expectedAttempt}`);
    // #3: authoritative job binding is MANDATORY for a PASS. Under the workflow lock re-read the AUTHORITATIVE
    // job row (not the canonical attempt copy) and verify its linkage + terminal outcome. A missing readJob
    // seam, a missing job row, a read error, a mis-link, or a diverged outcome all fail-close a PASS — the
    // general PASS path never proceeds without re-reading the job.
    let jobContradiction = false;
    if (verdict === "PASS") {
      let job = null;
      try { job = readJob && rec.jobId ? await readJob(root, rec.jobId) : null; } catch { job = null; }
      const link = job?.workflowLink ?? {};
      jobContradiction = !readJob || !job
        || link.workflowId !== workflowId
        || link.stageId !== stageId
        || link.attempt !== expectedAttempt
        || (link.activityIdempotencyKey ?? null) !== (rec.activityIdempotencyKey ?? null)
        || (job.jobOutcome ?? null) !== (rec.jobOutcome ?? null)
        || (job.processState ?? null) !== (rec.processState ?? null)
        || (job.providerState ?? null) !== (rec.providerState ?? null)
        || (jobOutcomeSummaryHash(job) !== (t.jobOutcomeSummaryHash ?? null));
    }
    // A PASS requires a COMPLETE checkpoint on BOTH sides — the frozen target AND the current fingerprint — and
    // an exact hash match. A missing/incomplete/unavailable checkpoint (either side) is CHECKPOINT_UNAVAILABLE;
    // two COMPLETE checkpoints with different hashes is CHECKPOINT_CHANGED (detected, not attributed to any
    // actor). `null === null` is never treated as a match. All non-PASS/failed cases skip this.
    const tCp = t.checkpointAfter ?? {};
    const targetComplete = tCp.status === "COMPLETE" && tCp.complete === true && !!tCp.aggregateHash;
    const cur = currentCheckpoint ?? {};
    const currentComplete = cur.status === "COMPLETE" && cur.complete === true && !!cur.aggregateHash;
    let toState, vsource, failureCode = null, finalVerdict = verdict;
    if (verdict === "PASS" && jobContradiction) {
      toState = "APPROVAL_REQUIRED"; vsource = null; finalVerdict = "INCONCLUSIVE"; failureCode = "WORKFLOW_AUDIT_TARGET_CONTRADICTION";
    } else if (verdict === "PASS" && (!targetComplete || !currentComplete)) {
      toState = "APPROVAL_REQUIRED"; vsource = null; finalVerdict = "INCONCLUSIVE"; failureCode = "WORKFLOW_AUDIT_CHECKPOINT_UNAVAILABLE";
    } else if (verdict === "PASS" && cur.aggregateHash !== tCp.aggregateHash) {
      toState = "APPROVAL_REQUIRED"; vsource = null; finalVerdict = "INCONCLUSIVE"; failureCode = "WORKFLOW_AUDIT_CHECKPOINT_CHANGED";
    } else {
      toState = verdict === "PASS" ? "PASSED" : verdict === "FAIL" ? "FAILED" : "APPROVAL_REQUIRED";
      vsource = verdict === "PASS" ? verificationSource : null;
    }
    const decidedAt = nowIso();
    const auditDecision = { verdict: finalVerdict, requestId, summary: decision.summary, checks: decision.checks, decidedAt, actorAgentId: actor?.agentId ?? null, source: "AUDIT_GATE", failureCode };
    const p3dDecision = { action: "AUDIT", source: "AUDIT_GATE", requestId, reason: reason ?? decision.summary, decidedAt, actorAgentId: actor?.agentId ?? null };
    const auditStatus = toState === "PASSED" ? "PASSED" : toState === "FAILED" ? "FAILED" : finalVerdict;
    await applyStageTransition(root, workflowId, { stageId, attempt: expectedAttempt, toState, mutation: {
      decision: p3dDecision,
      ...(vsource ? { verificationSource: vsource } : {}),
      ...(failureCode ? { failureReason: failureCode } : {}),
      audit: { ...rec.audit, status: auditStatus, decision: auditDecision },
    } });
    await writeControlRecord(root, workflowId, controlRecordFor({ action: "audit_decide", requestId, workflowId, stageId, attempt: expectedAttempt, ownerKeyHash, payloadFingerprint, reason: reason ?? decision.summary, resultState: toState }));
    return { replayed: false, resultState: toState, verdict: finalVerdict, failureCode };
  });
}

// #1/#2: fail-closed escalation of a UNVERIFIED audited stage to human review when the audit cannot run
// (no Supervisor session/gateway → WORKFLOW_AUDIT_UNAVAILABLE) or the authoritative job contradicts the
// target at request time (WORKFLOW_AUDIT_TARGET_CONTRADICTION). Never guesses PASS; records the reason so the
// public status + approval notice explain it. Idempotent (only a still-UNVERIFIED attempt escalates).
export async function escalateAuditToApproval(root, workflowId, { stageId, attempt, failureCode, auditStatus }) {
  return withWorkflowLock(root, workflowId, async () => {
    const rec = await readJsonOrNull(P.attemptFile(root, workflowId, stageId, attempt));
    if (!rec || rec.stageState !== "UNVERIFIED") return { escalated: false };
    await applyStageTransition(root, workflowId, { stageId, attempt, toState: "APPROVAL_REQUIRED", mutation: {
      failureReason: failureCode,
      audit: { ...(rec.audit ?? { mode: "supervisor" }), status: auditStatus, failureCode },
    } });
    return { escalated: true };
  });
}

// Audit outboxes (continuation + Slack summary) — same lease / DELIVERY_UNKNOWN safety as the approval outbox,
// kept SEPARATE from the terminal delivery outbox, the P1 continuation, and the approval-request outbox.
function auditOutboxDir(root, workflowId) { return path.join(P.workflowDir(root, workflowId), "audit"); }
function auditOutboxFile(root, workflowId, kind, stageId, attempt) { return path.join(auditOutboxDir(root, workflowId), `${kind}-${assertStageId(stageId)}-${attempt}.json`); }
async function claimOutbox(root, workflowId, file, key, { leaseMs = 30_000, maxAttempts = 8 } = {}) {
  return withWorkflowLock(root, workflowId, async () => {
    const rec = await readJsonOrNull(file);
    if (rec?.status === "SENT" || rec?.status === "DELIVERY_UNKNOWN") return { claim: false, record: rec };
    const write = async (v) => { await mkdir(path.dirname(file), { recursive: true, mode: 0o700 }); await atomicWriteJson(file, v); return v; };
    if (rec?.status === "SENDING") {
      if (Date.now() - new Date(rec.claimedAt).getTime() < leaseMs) return { claim: false, record: rec };
      return { claim: false, record: await write({ ...rec, status: "DELIVERY_UNKNOWN" }) };
    }
    const attempts = (rec?.attempts ?? 0) + 1;
    if (attempts > maxAttempts) return { claim: false, record: await write({ key, status: "DELIVERY_UNKNOWN", attempts, claimedAt: rec?.claimedAt ?? nowIso() }) };
    return { claim: true, record: await write({ key, status: "SENDING", attempts, claimedAt: nowIso(), lastError: null }) };
  });
}
async function markOutbox(root, workflowId, file, status, extra = {}) {
  return withWorkflowLock(root, workflowId, async () => {
    const rec = await readJsonOrNull(file);
    if (!rec || rec.status !== "SENDING") return rec;
    await atomicWriteJson(file, { ...rec, status, ...extra, updatedAt: nowIso() });
  });
}
export async function readAuditContinuation(root, workflowId, stageId, attempt) { return readJsonOrNull(auditOutboxFile(root, workflowId, "continuation", stageId, attempt)); }
// #1: converge the continuation outbox for the unavailable-audit decision WITHOUT claiming a new send. A
// lease-EXPIRED SENDING (an ambiguous crash around the send) is atomically parked DELIVERY_UNKNOWN (never
// treated as "definitely not sent" and never blind-resent); every other state is returned as-is. Returns
// { status, leaseFresh } so the caller can distinguish an in-flight (fresh SENDING) send from an ambiguous one.
export async function reconcileAuditContinuation(root, workflowId, stageId, attempt, { leaseMs = 30_000 } = {}) {
  return withWorkflowLock(root, workflowId, async () => {
    const file = auditOutboxFile(root, workflowId, "continuation", stageId, attempt);
    const rec = await readJsonOrNull(file);
    if (!rec) return { status: null };
    if (rec.status === "SENDING") {
      const fresh = Date.now() - new Date(rec.claimedAt).getTime() < leaseMs;
      if (fresh) return { status: "SENDING", leaseFresh: true };
      await atomicWriteJson(file, { ...rec, status: "DELIVERY_UNKNOWN", updatedAt: nowIso() });
      return { status: "DELIVERY_UNKNOWN", converged: true };
    }
    return { status: rec.status };
  });
}
export function claimAuditContinuation(root, workflowId, stageId, attempt, key, opts) { return claimOutbox(root, workflowId, auditOutboxFile(root, workflowId, "continuation", stageId, attempt), key, opts); }
export function markAuditContinuationSent(root, workflowId, stageId, attempt) { return markOutbox(root, workflowId, auditOutboxFile(root, workflowId, "continuation", stageId, attempt), "SENT", { sentAt: nowIso() }); }
export function markAuditContinuationError(root, workflowId, stageId, attempt, { retryable, message }) { return markOutbox(root, workflowId, auditOutboxFile(root, workflowId, "continuation", stageId, attempt), retryable ? "PENDING" : "DELIVERY_UNKNOWN", { lastError: String(message).slice(0, 500), lastAttemptAt: nowIso() }); }
export function claimAuditSummary(root, workflowId, stageId, attempt, key, opts) { return claimOutbox(root, workflowId, auditOutboxFile(root, workflowId, "summary", stageId, attempt), key, opts); }
export function markAuditSummarySent(root, workflowId, stageId, attempt) { return markOutbox(root, workflowId, auditOutboxFile(root, workflowId, "summary", stageId, attempt), "SENT", { sentAt: nowIso() }); }
export function markAuditSummaryError(root, workflowId, stageId, attempt, { retryable, message }) { return markOutbox(root, workflowId, auditOutboxFile(root, workflowId, "summary", stageId, attempt), retryable ? "PENDING" : "DELIVERY_UNKNOWN", { lastError: String(message).slice(0, 500), lastAttemptAt: nowIso() }); }

// Crash reconciliation: process EVERY PENDING journal entry in ascending seq order (not just the highest).
// Idempotent; canonical attempt records always win; a contradiction is fail-closed (no arbitrary advance).
export async function reconcileWorkflow(root, workflowId) {
  return withWorkflowLock(root, workflowId, async () => {
    const entries = await readJournalEntries(root, workflowId);
    for (const entry of entries) {
      if (entry.status !== "PENDING") continue;
      // #3: a preflight_result records no stage transition (toState === fromState), so it must NOT be judged by
      // "stageState unchanged". Recover it by comparing the recorded decisive hashes against the canonical
      // attempt: all match → COMMIT; canonical never written (or superseded by a later committed preflight) →
      // revert (ABORTED), the next pass re-runs preflight; canonical present but differs → fail-closed.
      if (entry.operation === "preflight_result") {
        const record = await readJsonOrNull(P.attemptFile(root, workflowId, entry.stageId, entry.attempt));
        const h = preflightIntentHashes(record);
        const matches = record && h.payload === entry.preflightPayloadHash && h.checkpointBefore === entry.checkpointBeforeHash && h.frozenToolchain === entry.frozenToolchainHash;
        if (matches) {
          await resolveJournal(root, workflowId, entry, "COMMITTED", null);
        } else if (!record || h.payload == null) {
          await resolveJournal(root, workflowId, entry, "ABORTED", "NO_CANONICAL_CHANGE"); // write never took effect → re-check
        } else if (entries.some((e) => e.operation === "preflight_result" && e.attempt === entry.attempt && e.stageId === entry.stageId && e.seq > entry.seq && e.status === "COMMITTED")) {
          await resolveJournal(root, workflowId, entry, "ABORTED", "SUPERSEDED"); // a later preflight committed over this one
        } else {
          throw wfError("WORKFLOW_RECONCILE_CONFLICT", `preflight_result seq ${entry.seq} contradicts canonical preflight (${entry.stageId} attempt ${entry.attempt})`);
        }
        continue;
      }
      // #2: attempt-creation recovery (next_attempt = provider fallback, resume_attempt = manual/checkpoint
      // resume). Both embed the intended canonical record + discriminators. canonical missing + source valid →
      // recreate the EXACT record (no guessing); canonical missing + source invalid → ABORTED; canonical
      // present + discriminators match → COMMIT; canonical present + mismatch → fail-closed (never overwrite).
      if (entry.operation === "next_attempt" || entry.operation === "resume_attempt") {
        const canonical = await readJsonOrNull(P.attemptFile(root, workflowId, entry.stageId, entry.attempt));
        const sourceValid = entry.sourceAttempt == null || (await readJsonOrNull(P.attemptFile(root, workflowId, entry.stageId, entry.sourceAttempt))) != null;
        if (!canonical) {
          if (entry.attemptRecord && sourceValid) {
            await atomicWriteJson(P.attemptFile(root, workflowId, entry.stageId, entry.attempt), entry.attemptRecord);
            await rebuildStageProjection(root, workflowId, entry.stageId);
            await resolveJournal(root, workflowId, entry, "COMMITTED", "RECREATED");
          } else {
            await resolveJournal(root, workflowId, entry, "ABORTED", "NO_CANONICAL_CHANGE");
          }
        } else {
          const src = entry.operation === "resume_attempt" ? canonical.resume?.resumeOfAttempt : canonical.fallback?.fallbackFromAttempt;
          const match = (canonical.executionCandidate?.candidateIndex ?? null) === (entry.candidateIndex ?? null)
            && (canonical.executionCandidate?.candidateId ?? null) === (entry.candidateId ?? null)
            && (entry.sourceAttempt == null || src === entry.sourceAttempt);
          if (match) await resolveJournal(root, workflowId, entry, "COMMITTED", null);
          else throw wfError("WORKFLOW_RECONCILE_CONFLICT", `${entry.operation} seq ${entry.seq} contradicts canonical attempt ${entry.attempt} (${entry.stageId})`);
        }
        continue;
      }
      // #P3-G: an audit_request records audit metadata (no stage transition). Recover by comparing the
      // recorded auditRequestId against the canonical attempt: present → COMMITTED; absent → ABORTED.
      if (entry.operation === "audit_request") {
        const rec = await readJsonOrNull(P.attemptFile(root, workflowId, entry.stageId, entry.attempt));
        if (rec?.audit?.auditRequestId === entry.auditRequestId) await resolveJournal(root, workflowId, entry, "COMMITTED", null);
        else await resolveJournal(root, workflowId, entry, "ABORTED", "NO_CANONICAL_CHANGE");
        continue;
      }
      if (entry.operation !== "stage_transition") continue;
      const record = await readJsonOrNull(P.attemptFile(root, workflowId, entry.stageId, entry.attempt));
      if (!record) {
        // canonical attempt not written → the storage transition never took effect → safe no-op.
        await resolveJournal(root, workflowId, entry, "ABORTED", "NO_CANONICAL_CHANGE");
        continue;
      }
      const canonical = record.stageState;
      if (canonical === entry.toState || reachable(entry.toState, canonical)) {
        await resolveJournal(root, workflowId, entry, "COMMITTED", null); // transition (and maybe more) happened
      } else if (canonical === entry.fromState) {
        await resolveJournal(root, workflowId, entry, "ABORTED", "NO_CANONICAL_CHANGE"); // did not happen
      } else {
        throw wfError("WORKFLOW_RECONCILE_CONFLICT", `journal seq ${entry.seq} intent ${entry.fromState}→${entry.toState} contradicts canonical ${canonical} (${entry.stageId})`);
      }
    }
    // #3: reconcileWorkflow performs storage/journal/canonical/projection recovery ONLY — it PRESERVES a
    // PENDING fallbackIntent and never creates the N+1. Consuming an intent creates a new attempt (a submission
    // decision), which is gated on workflowEnabled and therefore lives in advanceWorkflowOnce, not here (a
    // disabled workflow must still recover its journal/projection but must NOT create a fallback attempt).
    // canonical records are the source of truth — rebuild projections (pipeline-driven) from them. Note
    // rebuildWorkflowProjection re-derives each present pipeline stage's stage.json from its canonical
    // attempts, so the stage and workflow projections are both regenerated here.
    return rebuildWorkflowProjection(root, workflowId);
  });
}

async function resolveJournal(root, workflowId, entry, status, resolution) {
  await atomicWriteJson(P.journalFile(root, workflowId, entry.seq), { ...entry, status, resolution: resolution ?? entry.resolution ?? null, resolvedAt: nowIso() });
}
