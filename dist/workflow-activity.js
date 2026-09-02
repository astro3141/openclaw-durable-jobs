// P3-C workflow activity layer. Pure module (no OpenClaw plugin-SDK dependency; startJob is INJECTED to
// avoid a core.js import cycle). It normalizes/validates a stage's argv activity, derives the deterministic
// activity idempotency key, ensures at-most-one durable job per key (reservation-before-spawn under a
// storage-root job-creation lock — NOT the workflow lock), and maps a linked job's terminal outcome to a
// conservative stage verdict. It never advances to the next stage and never mutates job.json.
import { mkdir, rm, stat } from "node:fs/promises";
import path from "node:path";
import { createHash } from "node:crypto";
import { listJobs, TERMINAL_STATES } from "./job-store.js";
import { readStageAttempt } from "./workflow-store.js";

const JOB_LOCK_STALE_MS = 30_000;
const JOB_LOCK_TIMEOUT_MS = 10_000;
const MAX_ARGV_ITEMS = 128;
const MAX_ARGV_ITEM_LEN = 4096;
const MAX_TIMEOUT_SECONDS = 604800;

function actError(code, message) {
  const e = new Error(`${code}: ${message}`);
  e.code = code;
  return e;
}

// The canonical activity idempotency key format (matches the store's initialAttempt default). Deterministic
// per workflow/stage/attempt; never caller-supplied.
export function makeActivityIdempotencyKey(workflowId, stageId, attempt) {
  return `wf:${workflowId}:stage:${stageId}:attempt:${attempt}`;
}

// Validate a stage's argv activity ({ argv, timeoutSeconds? }). argv only — no shell string, no cwd/env
// injection (cwd is forced to the workflow worktree by the caller).
export function normalizeActivity(activity) {
  if (!activity || typeof activity !== "object" || Array.isArray(activity)) {
    throw actError("WORKFLOW_ACTIVITY_INVALID", "activity must be an object { argv, timeoutSeconds? }");
  }
  for (const key of Object.keys(activity)) {
    if (key !== "argv" && key !== "timeoutSeconds") {
      throw actError("WORKFLOW_ACTIVITY_INVALID", `activity has unsupported field "${key}" (only argv, timeoutSeconds)`);
    }
  }
  const { argv, timeoutSeconds } = activity;
  if (!Array.isArray(argv) || argv.length === 0 || argv.length > MAX_ARGV_ITEMS) {
    throw actError("WORKFLOW_ACTIVITY_INVALID", `activity.argv must be a non-empty string array (<= ${MAX_ARGV_ITEMS})`);
  }
  if (argv.some((a) => typeof a !== "string" || a.length === 0 || a.length > MAX_ARGV_ITEM_LEN)) {
    throw actError("WORKFLOW_ACTIVITY_INVALID", "activity.argv items must be non-empty bounded strings");
  }
  let normalizedTimeout;
  if (timeoutSeconds !== undefined) {
    if (!Number.isInteger(timeoutSeconds) || timeoutSeconds < 0 || timeoutSeconds > MAX_TIMEOUT_SECONDS) {
      throw actError("WORKFLOW_ACTIVITY_INVALID", `activity.timeoutSeconds must be an integer 0..${MAX_TIMEOUT_SECONDS}`);
    }
    normalizedTimeout = timeoutSeconds;
  }
  return { argv: [...argv], ...(normalizedTimeout !== undefined ? { timeoutSeconds: normalizedTimeout } : {}) };
}

// ---- storage-root job-creation lock (keyed by activity idempotency key; NOT the workflow lock) ----
export async function withJobCreationLock(rootDir, key, fn) {
  const dir = path.join(rootDir, ".job-locks");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const lockPath = path.join(dir, `${createHash("sha256").update(key).digest("hex")}.lock`);
  const deadline = Date.now() + JOB_LOCK_TIMEOUT_MS;
  let release;
  while (true) {
    try {
      await mkdir(lockPath, { mode: 0o700 });
      release = async () => rm(lockPath, { recursive: true, force: true });
      break;
    } catch (error) {
      if (error?.code !== "EEXIST") throw error;
      try {
        const info = await stat(lockPath);
        if (Date.now() - info.mtimeMs > JOB_LOCK_STALE_MS) { await rm(lockPath, { recursive: true, force: true }); continue; }
      } catch (statError) {
        if (statError?.code !== "ENOENT") throw statError;
        continue;
      }
      if (Date.now() >= deadline) throw actError("WORKFLOW_JOB_LOCK_TIMEOUT", `timed out acquiring job-creation lock for ${key}`);
      await new Promise((r) => setTimeout(r, 40));
    }
  }
  try {
    return await fn();
  } finally {
    await release();
  }
}

export async function findLinkedJob(rootDir, activityIdempotencyKey) {
  const jobs = await listJobs(rootDir);
  return jobs.find((j) => j.workflowLink?.activityIdempotencyKey === activityIdempotencyKey) ?? null;
}

// Ensure exactly one durable job exists for a stage attempt's activity. Deduplicates by the deterministic
// key under the job-creation lock; submits at most once via the INJECTED startJob. The workflow's frozen
// deliveryRoute is reused (session-independent, so the reconciler can submit with no live session) and no
// TaskFlow is created (linkedCtx carries no sessionKey).
export async function ensureLinkedJob(deps, { workflow, stageSpec, stageId, attempt, activityIdempotencyKey }) {
  const { rootDir, startJob, startDeps } = deps;
  if (!stageSpec || !stageSpec.activity) {
    throw actError("WORKFLOW_ACTIVITY_MISSING", `stage ${stageId} has no activity to submit`);
  }
  const activity = normalizeActivity(stageSpec.activity);
  const key = activityIdempotencyKey ?? makeActivityIdempotencyKey(workflow.workflowId, stageId, attempt);
  return withJobCreationLock(rootDir, key, async () => {
    const existing = await findLinkedJob(rootDir, key);
    if (existing) return existing; // idempotent: at most one job per key
    // Under the SAME creation lock as cancel: re-read the canonical attempt and REFUSE to submit a new job
    // for a cancelling/cancelled stage. This closes the cancel↔submit race (no orphan job for a stage that
    // was cancelled while a submit was in flight).
    const attemptRec = await readStageAttempt(rootDir, workflow.workflowId, stageId, attempt).catch(() => null);
    if (attemptRec?.cancelRequest || attemptRec?.stageState === "CANCELLED") {
      throw actError("WORKFLOW_ACTIVITY_CANCELLED", `stage ${stageId} attempt ${attempt} is cancelling; no job submitted`);
    }
    const worktree = workflow.repository?.worktree;
    if (!worktree) throw actError("WORKFLOW_WORKTREE_MISSING", `workflow ${workflow.workflowId} has no repository.worktree`);
    // P3-F spawn-time TOCTOU guard: re-verify the worktree fingerprint + toolchain (vs the preflight-frozen
    // values) UNDER this creation lock, right before startJob. Throws WORKFLOW_CHECKPOINT_CHANGED /
    // WORKFLOW_TOOLCHAIN_CHANGED on drift (no job). The frozen ABSOLUTE executable path is spawned (no PATH
    // re-resolution). This validatedExecution can never be injected via the durable_job tool schema.
    if (deps.guard?.verify) await deps.guard.verify();
    const argv = deps.guard?.frozenExecutablePath ? [deps.guard.frozenExecutablePath, ...activity.argv.slice(1)] : activity.argv;
    const linkedCtx = {
      agentId: workflow.parent?.agentId ?? null,
      sessionKey: null, // no live session → no chat.history, no TaskFlow
      sessionId: workflow.parent?.sessionId ?? null,
      deliveryContext: workflow.parent?.requesterOrigin ?? null,
      workspaceDir: worktree,
      durableAllowedRoots: [worktree],
      ownerDeliveryRoute: null,
    };
    const params = {
      name: `${workflow.name ?? workflow.workflowId} / ${stageId}`,
      command: argv, // argv-only, reuses durable_job's shell-free execution (frozen absolute exe when guarded)
      cwd: worktree, // forced to the workflow worktree; never a caller-supplied per-stage cwd
      ...(activity.timeoutSeconds !== undefined ? { timeoutSeconds: activity.timeoutSeconds } : {}),
      runnerType: stageSpec.runnerType ?? undefined,
      runnerProfile: stageSpec.runnerProfile ?? undefined,
      deliveryRoute: workflow.deliveryRoute, // pre-frozen at workflow creation
      workflowLink: { workflowId: workflow.workflowId, stageId, attempt, activityIdempotencyKey: key },
      // #1: frozen execution expectations for the worker's own pre-spawn re-verify. Honored by startJob ONLY
      // when a valid workflowLink is present, so a standalone durable_job caller can never inject it.
      ...(deps.guard?.validatedExecution ? { validatedExecution: deps.guard.validatedExecution } : {}),
    };
    return startJob(startDeps, linkedCtx, params); // publicJob (has .id); throws before side effects on bad metadata
  });
}

// Conservative terminal → stage-state mapping. Reuses the P0 verdict enums (jobOutcome/providerState); it
// invents no new enum and NEVER auto-PASSES. process COMPLETED + provider OK ⇒ COMPLETED_UNVERIFIED ⇒ stage
// UNVERIFIED (a semantic PASSED requires a verification contract that P3-C does not implement). Unknown
// terminal shapes fail closed to FAILED (never guessed PASSED).
export function mapJobToStageState(job) {
  const terminal = TERMINAL_STATES.has(job.state) || (job.jobOutcome != null);
  if (!terminal) return { state: "RUNNING", mutation: linkMutation(job) };
  const outcome = job.jobOutcome ?? null;
  const providerState = job.providerState ?? null;
  if (outcome === "COMPLETED_UNVERIFIED") return { state: "UNVERIFIED", mutation: linkMutation(job) };
  if (outcome === "FAILED_PROVIDER") {
    // retryable provider dependency (transient) → BLOCKED_DEPENDENCY; everything else → FAILED.
    const retryable = providerState === "BLOCKED_QUOTA" || providerState === "RATE_LIMITED";
    return { state: retryable ? "BLOCKED_DEPENDENCY" : "FAILED", mutation: linkMutation(job) };
  }
  if (outcome === "CANCELLED") return { state: "CANCELLED", mutation: linkMutation(job) }; // a cancelled linked job → stage CANCELLED (not FAILED)
  if (outcome === "FAILED_COMMAND") return { state: "FAILED", mutation: linkMutation(job) };
  // Terminal but unmapped (e.g. a legacy row with no jobOutcome) → fail closed, never a guessed PASSED.
  return { state: "FAILED", mutation: linkMutation(job, "UNMAPPED_TERMINAL") };
}

function linkMutation(job, failureCode) {
  return {
    jobId: job.id,
    processState: job.processState ?? null,
    providerState: job.providerState ?? null,
    jobOutcome: job.jobOutcome ?? null,
    finishedAt: job.endedAt ?? null,
    failureReason: failureCode ?? (job.jobOutcome && job.jobOutcome !== "COMPLETED_UNVERIFIED" ? (job.error ?? job.jobOutcome) : null),
  };
}
