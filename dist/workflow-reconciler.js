// P3-C/P3-D workflow reconciler. Pure module (startJob is injected via deps; no core.js import → no cycle).
// Runs inside the existing durable-jobs single-flight tick (NOT a second Gateway service). Per workflow it
//   1) recovers the storage journal (P3-A reconcileWorkflow),
//   2) computes the LINEAR frontier + invariants and, atomically, either
//      - settles/recovers the current ACTIVE (SUBMITTING/RUNNING) linked stage (P3-C), or
//      - claims + submits the next runnable PENDING stage (P3-D) — at most ONE new job submission per pass.
// It never fabricates a PASSED verdict, never advances past a non-PASSED stage, and never mutates job.json.
import { readJob } from "./job-store.js";
import {
  candidateOf,
  candidateCount,
  claimApprovalSend,
  claimRunnableStage,
  commitPreflightResult,
  settleWithFallbackIntent,
  consumeFallbackIntent,
  pendingFallbackIntents,
  requestAudit,
  escalateAuditToApproval,
  jobOutcomeSummaryHash,
  reconcileAuditContinuation,
  claimAuditContinuation,
  markAuditContinuationSent,
  markAuditContinuationError,
  claimAuditSummary,
  markAuditSummarySent,
  markAuditSummaryError,
  finishStageCancel,
  listWorkflowIds,
  markApprovalError,
  markApprovalSent,
  readStageAttempt,
  readStageProjection,
  readWorkflow,
  recordCheckpoint,
  reconcileWorkflow,
  transitionStageAttempt,
  isAllowedStageTransition,
  TERMINAL_STAGE_STATES,
} from "./workflow-store.js";
import { ensureLinkedJob, findLinkedJob, mapJobToStageState, withJobCreationLock } from "./workflow-activity.js";
import { isProvenPreSendRejection } from "./continuation.js";
import { computeToolchainFingerprint, computeWorktreeFingerprint, publicWorktreeCheckpoint } from "./workflow-fingerprint.js";
import { getProviderCapability, providerCacheKeyHash } from "./provider-cache.js";
import { auditRequestKey, auditSummaryKey, buildAuditContinuationMessage, buildAuditSummaryText, publicAuditProjection } from "./workflow-audit.js";
import { randomUUID } from "node:crypto";

// Provider states for which switching to a DIFFERENT declared candidate is eligible (verified vs the P0
// evaluator enums: a quota/rate/auth failure of one provider can be retried on another).
const FALLBACK_ELIGIBLE_PROVIDER = new Set(["BLOCKED_QUOTA", "RATE_LIMITED", "AUTH_FAILED"]);
const DEFAULT_TTLS = { readyMs: 300_000, negativeMs: 30_000, unknownMs: 5_000 };

// Injected fingerprint/provider seams with production defaults (tests inject deterministic fakes; the real
// probe defaults to UNKNOWN — there is no non-quota model readiness seam, and UNKNOWN never auto-fallbacks).
function seams(deps) {
  return {
    captureFingerprint: deps.captureFingerprint ?? ((worktree) => computeWorktreeFingerprint(worktree, { maxFiles: deps.config?.workflowFingerprintMaxFiles, maxBytes: deps.config?.workflowFingerprintMaxBytes, timeoutMs: deps.config?.workflowPreflightTimeoutMs })),
    captureToolchain: deps.captureToolchain ?? ((args) => computeToolchainFingerprint(args)),
    providerProbe: deps.providerProbe ?? (async () => ({ status: "UNKNOWN" })),
    providerConfigFingerprint: deps.providerConfigFingerprint ?? "none",
    ttls: deps.providerTtls ?? {
      readyMs: deps.config?.workflowProviderCacheTtlMs ?? DEFAULT_TTLS.readyMs,
      negativeMs: deps.config?.workflowProviderNegativeCacheTtlMs ?? DEFAULT_TTLS.negativeMs,
      unknownMs: deps.config?.workflowProviderUnknownCacheTtlMs ?? DEFAULT_TTLS.unknownMs,
    },
  };
}

// #5: bound a single preflight step by workflowPreflightTimeoutMs. On timeout the step's promise is
// abandoned and an AbortSignal is fired so a cooperating probe can cancel its background work (no polling
// loop, no lingering health poll). A non-positive/absent budget means "no bound" (the injected unit seams
// resolve instantly). The caller fail-closes every timeout (no model job, no fallback).
function withTimeout(fn, ms) {
  if (!Number.isFinite(ms) || ms <= 0) return Promise.resolve(fn(undefined));
  const ac = new AbortController();
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => { try { ac.abort(); } catch { /* best-effort */ } reject(wfErr("WORKFLOW_PREFLIGHT_TIMEOUT", `preflight step exceeded ${ms}ms`)); }, ms);
    if (timer.unref) timer.unref();
  });
  return Promise.race([Promise.resolve(fn(ac.signal)), timeout]).finally(() => clearTimeout(timer));
}

// Run preflight for a candidate OUTSIDE the workflow lock: worktree fingerprint → toolchain fingerprint →
// (model only) provider capability cache. Returns a preflight result the caller records + acts on.
async function runPreflight(deps, workflow, candidate, expectedBeforeHash = null) {
  const s = seams(deps);
  const timeoutMs = deps.config?.workflowPreflightTimeoutMs;
  const worktree = workflow.repository?.worktree;
  const wt = await s.captureFingerprint(worktree);
  const worktreeFingerprint = publicWorktreeCheckpoint(wt);
  // Safe-resume / fallback froze an expected pre-execution checkpoint: re-verify it right before submission.
  if (expectedBeforeHash) {
    if (wt.status !== "COMPLETE") return { status: "FAILED", failureCode: "WORKFLOW_CHECKPOINT_UNAVAILABLE", worktreeFingerprint, worktreeFull: wt };
    if (wt.aggregateHash !== expectedBeforeHash) return { status: "FAILED", failureCode: "WORKFLOW_CHECKPOINT_CHANGED", worktreeFingerprint, worktreeFull: wt };
  }
  // #4: a NEW submission REQUIRES a COMPLETE Git worktree fingerprint. Missing / non-Git / INCOMPLETE all
  // fail closed to ARTIFACT_MISSING (no job). (A pre-P3-F job-linked attempt still terminal-reconciles; this
  // gate governs only new submissions.)
  if (wt.status !== "COMPLETE") {
    const code = wt.reason === "WORKTREE_MISSING" ? "WORKFLOW_WORKTREE_MISSING" : wt.status === "UNAVAILABLE" ? "WORKFLOW_WORKTREE_NOT_GIT" : "WORKFLOW_FINGERPRINT_INCOMPLETE";
    return { status: "ARTIFACT_MISSING", failureCode: code, worktreeFingerprint, worktreeFull: wt };
  }
  let tc;
  try {
    tc = await withTimeout((signal) => s.captureToolchain({ argv: candidate.activity.argv, cwd: worktree, runnerType: candidate.runnerType, runnerProfile: candidate.runnerProfile, signal }), timeoutMs);
  } catch (e) {
    if (e?.code === "WORKFLOW_PREFLIGHT_TIMEOUT") return { status: "FAILED", failureCode: "WORKFLOW_TOOLCHAIN_TIMEOUT", worktreeFingerprint, worktreeFull: wt }; // fail-closed: no job
    throw e;
  }
  const toolchainFingerprint = { status: tc.status, aggregateHash: tc.aggregateHash, executableBasename: tc.executableBasename };
  if (tc.status === "MISSING") return { status: "FAILED", failureCode: "WORKFLOW_TOOLCHAIN_MISSING", worktreeFingerprint, toolchainFingerprint, worktreeFull: wt };
  if (tc.status === "UNSUPPORTED") return { status: "FAILED", failureCode: "WORKFLOW_TOOLCHAIN_UNSUPPORTED", worktreeFingerprint, toolchainFingerprint, worktreeFull: wt };
  // Local runners need no provider probe (NOT_REQUIRED → PASS). A model runner PASSES only when the provider
  // capability is explicitly READY: BLOCKED evaluates provider fallback; UNKNOWN is FAIL-CLOSED (no job, no
  // fallback) — UNKNOWN is never treated as READY, so a real model stage cannot run until an explicit
  // non-quota READY probe is injected.
  if (candidate.runnerType !== "model") {
    return { status: "PASS", providerCapability: "NOT_REQUIRED", providerCacheHit: false, providerState: null, worktreeFingerprint, toolchainFingerprint, tcFull: tc, worktreeFull: wt };
  }
  const keyHash = providerCacheKeyHash({ runnerType: candidate.runnerType, runnerProfile: candidate.runnerProfile, toolchainHash: tc.aggregateHash, configFingerprint: s.providerConfigFingerprint });
  // #5: the provider probe is time-bounded too; a timeout fail-closes to UNKNOWN (never READY), which the
  // tail below maps to BLOCKED_DEPENDENCY (no job, no fallback, no quota spent). getProviderCapability already
  // dedupes concurrent probes via a mkdir lock, so at most ONE probe runs per reconcile pass (no poll loop).
  const probe = async () => {
    try {
      return await withTimeout((signal) => s.providerProbe({ runnerType: candidate.runnerType, runnerProfile: candidate.runnerProfile, toolchainHash: tc.aggregateHash, signal }), timeoutMs);
    } catch (e) {
      if (e?.code === "WORKFLOW_PREFLIGHT_TIMEOUT") return { status: "UNKNOWN" }; // fail-closed
      throw e;
    }
  };
  const cap = await getProviderCapability(deps.rootDir, { keyHash, toolchainHash: tc.aggregateHash, probe, ttls: s.ttls });
  const common = { providerCapability: cap.status, providerCacheHit: cap.cacheHit, providerState: cap.providerState, worktreeFingerprint, toolchainFingerprint, tcFull: tc, worktreeFull: wt };
  if (cap.status === "READY") return { status: "PASS", ...common };
  if (cap.status === "BLOCKED") return { status: "BLOCKED", failureCode: cap.failureCode ?? "PROVIDER_BLOCKED", fallbackEligible: FALLBACK_ELIGIBLE_PROVIDER.has(cap.providerState), ...common };
  return { status: "UNKNOWN", failureCode: "PROVIDER_UNKNOWN", ...common }; // fail-closed: no job, no fallback
}

// Fail-closed if a resolved job's workflowLink does not match the stage attempt exactly. A mismatched jobId
// is never trusted or overwritten and no replacement job is silently created.
function assertLinkMatches(job, workflowId, stageId, attempt, key) {
  const link = job.workflowLink;
  if (!link || link.workflowId !== workflowId || link.stageId !== stageId || link.attempt !== attempt || link.activityIdempotencyKey !== key) {
    const e = new Error(`WORKFLOW_LINKAGE_MISMATCH: job ${job.id} workflowLink does not match ${workflowId}/${stageId}/attempt ${attempt}`);
    e.code = "WORKFLOW_LINKAGE_MISMATCH";
    throw e;
  }
}

// A race-tolerant transition: the pre-check reads state outside the workflow lock, so a peer (concurrent
// start vs the tick) may advance the stage between the check and the locked transition. A lost race is
// benign, so an illegal/terminal transition is swallowed and the current record returned (idempotent).
async function safeTransition(rootDir, wfId, stageId, attemptNum, toState, mutation) {
  const cur = await readStageAttempt(rootDir, wfId, stageId, attemptNum);
  if (!cur) return null;
  const hasMutation = mutation && Object.keys(mutation).length > 0;
  if (cur.stageState !== toState) {
    if (TERMINAL_STAGE_STATES.has(cur.stageState)) return cur;
    if (!isAllowedStageTransition(cur.stageState, toState)) return cur;
  } else if (!hasMutation) {
    return cur;
  }
  try {
    return await transitionStageAttempt(rootDir, wfId, { stageId, attempt: attemptNum, toState, mutation });
  } catch (error) {
    if (error?.code === "STAGE_TRANSITION_ILLEGAL" || error?.code === "STAGE_TERMINAL") {
      return readStageAttempt(rootDir, wfId, stageId, attemptNum);
    }
    throw error;
  }
}

function wfErr(code, message) { const e = new Error(`${code}: ${message}`); e.code = code; return e; }

// #2: settle the source verdict + record the durable fallbackIntent atomically, then consume it (create N+1).
// Race-tolerant: if a peer already settled the attempt terminal/illegally, swallow (an interrupted intent is
// finished by reconcileWorkflow's recovery scan on the next pass; consumption is idempotent, so N+1 is never
// duplicated). Returns true when this call recorded/consumed the intent.
async function settleAndFallback(rootDir, wfId, params) {
  try {
    await settleWithFallbackIntent(rootDir, wfId, params);
  } catch (e) {
    if (e?.code === "STAGE_TERMINAL" || e?.code === "STAGE_TRANSITION_ILLEGAL") return false;
    throw e;
  }
  await consumeFallbackIntent(rootDir, wfId, { stageId: params.stageId, attempt: params.attempt });
  return true;
}

// Apply the linked job's terminal outcome to the stage. Captures the post-run checkpoint (checkpoint.after)
// and, for a fallback-eligible provider failure with an UNCHANGED pre-execution checkpoint and a declared
// next candidate, creates a checkpoint-verified PROVIDER_FALLBACK attempt (N+1) instead of settling; a
// changed/incomplete checkpoint fails closed to APPROVAL_REQUIRED (never auto-fallback). Idempotent.
async function settleTerminal(deps, workflow, stageSpec, attemptNum, job) {
  const { rootDir, config } = deps;
  const wfId = workflow.workflowId;
  const stageId = stageSpec.stageId;
  const fresh = job?.id ? ((await readJob(rootDir, job.id).catch(() => null)) ?? job) : job;
  if (!fresh) return;
  const verdict = mapJobToStageState(fresh);
  if (verdict.state === "RUNNING") return; // job not terminal yet
  await safeTransition(rootDir, wfId, stageId, attemptNum, "RUNNING", { jobId: fresh.id }); // reach RUNNING first

  // capture the post-run checkpoint once (terminal outcome is never lost if the capture fails)
  let attempt = await readStageAttempt(rootDir, wfId, stageId, attemptNum);
  let after = attempt?.checkpoint?.after ?? null;
  if (!after) {
    const wt = await seams(deps).captureFingerprint(workflow.repository?.worktree).catch(() => null);
    after = wt ? publicWorktreeCheckpoint(wt) : { aggregateHash: null, complete: false, status: "UNAVAILABLE" };
    await recordCheckpoint(rootDir, wfId, { stageId, attempt: attemptNum, phase: "after", checkpoint: after });
    attempt = await readStageAttempt(rootDir, wfId, stageId, attemptNum);
  }

  const baseSpec = workflow.pipeline.find((s) => s.stageId === stageId);
  const idx = attempt?.executionCandidate?.candidateIndex ?? 0;
  const nextCandidate = candidateOf(baseSpec, idx + 1);
  const fallbackEligible = fresh.jobOutcome === "FAILED_PROVIDER" && FALLBACK_ELIGIBLE_PROVIDER.has(fresh.providerState) && config.workflowEnabled && nextCandidate?.activity;
  if (fallbackEligible) {
    const before = attempt?.checkpoint?.before ?? null;
    const unchanged = before && after && before.complete && after.complete && before.aggregateHash === after.aggregateHash;
    if (unchanged) {
      // #2: settle the failed primary's real verdict (history preserved) AND record the fallback obligation in
      // ONE atomic write, then consume it to append attempt N+1. A crash between settle and N+1 is finished by
      // reconcileWorkflow's recovery scan (idempotent) — never a source-terminal-without-fallback broken state.
      await settleAndFallback(rootDir, wfId, { stageId, attempt: attemptNum, sourceToState: verdict.state, sourceMutation: verdict.mutation, candidateIndex: idx + 1, fromCandidateIndex: idx, fallbackReason: `provider ${fresh.providerState}`, expectedCheckpointHash: before.aggregateHash });
      return; // one fallback attempt per tick; the next tick preflights/submits candidate idx+1
    }
    // changed/incomplete checkpoint → do NOT auto-fallback; require manual approval (RUNNING → APPROVAL_REQUIRED
    // directly, carrying the job's real process/provider history in the mutation)
    await safeTransition(rootDir, wfId, stageId, attemptNum, "APPROVAL_REQUIRED", { ...verdict.mutation, failureReason: "WORKFLOW_CHECKPOINT_MISMATCH" });
    return;
  }
  await safeTransition(rootDir, wfId, stageId, attemptNum, verdict.state, verdict.mutation);
}

// Preflight then submit a SUBMITTING attempt: verify the worktree/toolchain/provider OUTSIDE the workflow
// lock (serialized by a per-attempt preflight lock), record the result, and only on PASS capture the
// pre-execution checkpoint + ensure the linked job (candidate activity). A non-PASS preflight transitions
// the stage to the mapped stop state with NO job. Returns { submitted }.
async function preflightAndSubmit(deps, workflow, stageId, attemptNum) {
  const { rootDir, config } = deps;
  const wfId = workflow.workflowId;
  const attempt = await readStageAttempt(rootDir, wfId, stageId, attemptNum);
  if (!attempt) return { submitted: false };
  const baseSpec = workflow.pipeline.find((s) => s.stageId === stageId);
  const candidate = candidateOf(baseSpec, attempt.executionCandidate?.candidateIndex ?? 0);
  if (!candidate?.activity) throw wfErr("WORKFLOW_ACTIVITY_MISSING", `stage ${stageId} candidate has no activity`);
  const candidateSpec = { ...baseSpec, runnerType: candidate.runnerType, runnerProfile: candidate.runnerProfile, activity: candidate.activity };

  // already linked to a job? (idempotent recovery / concurrent submit)
  let job = attempt.jobId ? await readJob(rootDir, attempt.jobId).catch(() => null) : null;
  if (job) assertLinkMatches(job, wfId, stageId, attemptNum, attempt.activityIdempotencyKey);
  if (!job) job = await findLinkedJob(rootDir, attempt.activityIdempotencyKey);
  if (job) {
    await safeTransition(rootDir, wfId, stageId, attemptNum, "RUNNING", { jobId: job.id });
    await settleTerminal(deps, workflow, candidateSpec, attemptNum, job);
    return { submitted: false };
  }
  if (!config.workflowEnabled) return { submitted: false }; // never start new work while disabled
  if (attempt.cancelRequest) throw wfErr("WORKFLOW_ACTIVITY_CANCELLED", `stage ${stageId} is cancelling`);

  // run preflight exactly once (serialized by a per-attempt preflight lock) and commit the result ATOMICALLY
  // (preflight status + checkpoint.before + frozen toolchain converge — no PASS-without-checkpoint window).
  let result = null;
  await withJobCreationLock(rootDir, `${attempt.activityIdempotencyKey}:preflight`, async () => {
    const cur = await readStageAttempt(rootDir, wfId, stageId, attemptNum);
    // a cached PASS is trusted ONLY when its checkpoint.before AND frozen toolchain are both present
    if (cur?.preflight?.status === "PASSED" && cur.checkpoint?.before && cur.preflight?.frozenToolchain) { result = { status: "PASS", cached: true, frozen: cur.preflight.frozenToolchain, before: cur.checkpoint.before }; return; }
    if (await findLinkedJob(rootDir, attempt.activityIdempotencyKey)) { result = { status: "PASS", cached: true, frozen: cur?.preflight?.frozenToolchain, before: cur?.checkpoint?.before }; return; }
    const r = await runPreflight(deps, workflow, candidate, attempt.checkpoint?.expectedBeforeHash ?? null);
    const status = r.status === "PASS" ? "PASSED" : r.status; // PASSED | BLOCKED | UNKNOWN | ARTIFACT_MISSING | FAILED
    const before = r.worktreeFull?.status === "COMPLETE" ? publicWorktreeCheckpoint(r.worktreeFull) : (r.worktreeFull ? publicWorktreeCheckpoint(r.worktreeFull) : null);
    await commitPreflightResult(rootDir, wfId, { stageId, attempt: attemptNum, preflight: { status, checkedAt: new Date().toISOString(), worktreeFingerprint: r.worktreeFingerprint ?? null, toolchainFingerprint: r.toolchainFingerprint ?? null, providerCapability: r.providerCapability ?? null, providerCacheHit: r.providerCacheHit ?? false, failureCode: r.failureCode ?? null, failureReason: r.failureCode ?? null }, checkpointBefore: before, toolchain: r.status === "PASS" ? r.tcFull : null });
    result = { ...r, frozen: r.status === "PASS" ? r.tcFull : null, before };
  });

  if (result.status === "PASS") {
    const created = await ensureLinkedJob(
      { rootDir, startJob: deps.startJob, startDeps: deps.startDeps, guard: makeSpawnGuard(deps, workflow, result) },
      { workflow, stageSpec: candidateSpec, stageId, attempt: attemptNum, activityIdempotencyKey: attempt.activityIdempotencyKey },
    );
    await safeTransition(rootDir, wfId, stageId, attemptNum, "RUNNING", { jobId: created.id });
    await settleTerminal(deps, workflow, candidateSpec, attemptNum, created);
    return { submitted: true };
  }
  // non-PASS preflight: transition to the mapped stop state (no job). UNKNOWN and BLOCKED both stop as
  // BLOCKED_DEPENDENCY; only an EXPLICIT provider BLOCKED with a fallback-eligible state auto-fallbacks.
  const stopState = (result.status === "BLOCKED" || result.status === "UNKNOWN") ? "BLOCKED_DEPENDENCY" : result.status; // ARTIFACT_MISSING | FAILED
  const idx = attempt.executionCandidate?.candidateIndex ?? 0;
  const nextCandidate = candidateOf(baseSpec, idx + 1);
  // #2 preflight-blocked automatic fallback: explicit provider BLOCKED + fallback-eligible + COMPLETE
  // checkpoint.before + a declared next candidate → settle BLOCKED_DEPENDENCY AND record the fallback intent
  // atomically (same primitive as the terminal path), then consume → attempt N+1 (PROVIDER_FALLBACK). UNKNOWN
  // never fallbacks. Anything else just settles the stop state with no job and no intent.
  if (result.status === "BLOCKED" && result.fallbackEligible && result.before?.complete && nextCandidate?.activity) {
    await settleAndFallback(rootDir, wfId, { stageId, attempt: attemptNum, sourceToState: stopState, sourceMutation: { failureReason: result.failureCode }, candidateIndex: idx + 1, fromCandidateIndex: idx, fallbackReason: `provider-blocked ${result.providerState}`, expectedCheckpointHash: result.before.aggregateHash });
  } else {
    await safeTransition(rootDir, wfId, stageId, attemptNum, stopState, { failureReason: result.failureCode });
  }
  return { submitted: false };
}

// Build the spawn-time guard passed into ensureLinkedJob: re-verify (under the activity creation lock, right
// before startJob) that the worktree fingerprint still equals checkpoint.before and the toolchain still
// equals the frozen fingerprint; then spawn the FROZEN absolute executable path (no PATH re-resolution).
function makeSpawnGuard(deps, workflow, result) {
  const s = seams(deps);
  const frozen = result.frozen; // toolchain full fingerprint
  const before = result.before; // checkpoint.before (public)
  // #1: the frozen expectations the WORKER re-verifies a second time, in its own process, right before it
  // spawns the child (a deeper TOCTOU close than this reconciler-side guard, which runs earlier and elsewhere).
  // Persisted on the job row ONLY for workflow-linked jobs; never injectable via the durable_job tool schema
  // and never projected by publicJob/status (absolute realpath + content hashes stay internal).
  const validatedExecution = (frozen?.aggregateHash || before?.aggregateHash) ? {
    worktree: workflow.repository?.worktree ?? null,
    worktreeAggregateHash: before?.complete ? (before.aggregateHash ?? null) : null,
    fingerprint: { maxFiles: deps.config?.workflowFingerprintMaxFiles, maxBytes: deps.config?.workflowFingerprintMaxBytes, timeoutMs: deps.config?.workflowPreflightTimeoutMs },
    toolchain: frozen?.aggregateHash ? {
      executableRealpath: frozen.executableRealpath ?? null,
      executableBasename: frozen.executableBasename ?? null,
      executableContentHash: frozen.executableContentHash ?? null,
      executableSize: frozen.executableSize ?? null,
      aggregateHash: frozen.aggregateHash,
      runnerType: frozen.runnerType ?? null,
      runnerProfile: frozen.runnerProfile ?? null,
    } : null,
  } : null;
  return {
    frozenExecutablePath: frozen?.executableRealpath ?? null,
    validatedExecution,
    verify: async () => {
      if (before?.complete && before.aggregateHash) {
        const wt = await s.captureFingerprint(workflow.repository?.worktree);
        if (wt.status !== "COMPLETE" || wt.aggregateHash !== before.aggregateHash) throw wfErr("WORKFLOW_CHECKPOINT_CHANGED", "worktree changed between preflight and spawn");
      }
      if (frozen?.aggregateHash) {
        const tc = await s.captureToolchain({ argv: [frozen.executableRealpath ?? frozen.executableBasename], cwd: workflow.repository?.worktree, runnerType: frozen.runnerType, runnerProfile: frozen.runnerProfile });
        if (tc.status !== "COMPLETE" || tc.aggregateHash !== frozen.aggregateHash) throw wfErr("WORKFLOW_TOOLCHAIN_CHANGED", "executable changed between preflight and spawn");
      }
    },
  };
}

// Settle/recover the current ACTIVE (SUBMITTING/RUNNING) stage. Returns { submitted } — true only when a
// SUBMITTING-with-no-job recovery submitted a new durable job (the caller enforces the per-tick bound).
async function settleActiveStage(deps, workflow, stageId, attemptNum) {
  const { rootDir, config } = deps;
  const wfId = workflow.workflowId;
  const attempt = await readStageAttempt(rootDir, wfId, stageId, attemptNum);
  if (!attempt) return { submitted: false };
  const stageSpec = workflow.pipeline.find((s) => s.stageId === stageId);
  // P3-E: a cancel was requested on this active stage (control recorded it, then crashed before completing).
  // Cancel the linked job and converge to CANCELLED — NEVER submit new work for a cancelling stage.
  if (attempt.cancelRequest && attempt.cancelRequest.status !== "COMPLETED") {
    let jobTerminalOutcome = null;
    await withJobCreationLock(rootDir, attempt.activityIdempotencyKey, async () => {
      const job = attempt.jobId
        ? await readJob(rootDir, attempt.jobId).catch(() => null)
        : await findLinkedJob(rootDir, attempt.activityIdempotencyKey);
      if (job) {
        await deps.cancelJob?.(rootDir, job.id).catch(() => {});
        jobTerminalOutcome = (await readJob(rootDir, job.id).catch(() => null))?.jobOutcome ?? null;
      }
    });
    await finishStageCancel(rootDir, wfId, { stageId, attempt: attemptNum, jobTerminalOutcome });
    return { submitted: false };
  }
  if (attempt.stageState === "SUBMITTING") {
    return preflightAndSubmit(deps, workflow, stageId, attemptNum); // P3-F: preflight → PASS → ensure job
  }
  if (attempt.stageState === "RUNNING") {
    const job = attempt.jobId
      ? await readJob(rootDir, attempt.jobId).catch(() => null)
      : await findLinkedJob(rootDir, attempt.activityIdempotencyKey);
    if (!job) return { submitted: false }; // jobId set but unreadable → leave RUNNING (conservative)
    assertLinkMatches(job, wfId, stageId, attemptNum, attempt.activityIdempotencyKey);
    await settleTerminal(deps, workflow, stageSpec, attemptNum, job);
  }
  return { submitted: false };
}

// Submit a freshly CLAIMED (SUBMITTING) frontier stage: ensure the linked job OUTSIDE the workflow lock,
// attach the jobId, move to RUNNING, and settle only if that job is ALREADY terminal (not chained further).
async function submitClaimedStage(deps, workflow, stageId, attemptNum) {
  return preflightAndSubmit(deps, workflow, stageId, attemptNum); // P3-F: claimed stage goes through preflight
}

// One advancement pass for a single workflow. Shared by the reconciler tick AND workflow.start. At most ONE
// new durable-job submission per pass: claimRunnableStage atomically computes the linear frontier + invariants
// (throwing WORKFLOW_PIPELINE_INVARIANT / WORKFLOW_PIPELINE_INCOMPLETE / WORKFLOW_ACTIVITY_MISSING on a bad
// shape) and either reports the active stage (settle) or claims exactly one runnable PENDING stage (submit).
export async function advanceWorkflowOnce(deps, workflowId) {
  const { rootDir, config } = deps;
  await reconcileWorkflow(rootDir, workflowId); // storage crash recovery (all PENDING journals) + projections
  // #3: consuming a PENDING fallbackIntent CREATES the fallback attempt N+1 (a submission decision), so it is
  // gated on workflowEnabled — a disabled workflow recovers its journal/projections (above) but never grows a
  // new fallback attempt. Idempotent: reconcileWorkflow preserves the intent; consume is safe to re-run.
  if (config.workflowEnabled) {
    for (const target of await pendingFallbackIntents(rootDir, workflowId)) {
      await consumeFallbackIntent(rootDir, workflowId, target);
    }
  }
  const claim = await claimRunnableStage(rootDir, workflowId, { enabled: config.workflowEnabled });
  const workflow = await readWorkflow(rootDir, workflowId);
  if (!workflow) return;
  if (claim.status === "active") {
    // the frontier stage is SUBMITTING/RUNNING → recover/settle it (may submit a recovery job: 1/tick)
    await settleActiveStage(deps, workflow, claim.stageId, claim.attempt);
  } else if (claim.status === "claimed") {
    // a PENDING frontier was just claimed → submit exactly one new job for it (1/tick)
    await submitClaimedStage(deps, workflow, claim.stageId, claim.attempt);
  } else if (claim.status === "stopped" && claim.stageState === "APPROVAL_REQUIRED" && deps.gatewayCall) {
    // P3-E: a PAUSED workflow → send the approval request once (idempotent, separate from the terminal notice)
    await driveApprovalRequest(deps, workflow, claim.stageId, claim.attempt);
  } else if (claim.status === "stopped" && claim.stageState === "UNVERIFIED" && config.workflowAuditEnabled) {
    // P3-G: a UNVERIFIED frontier under an audit.mode=supervisor stage → wake the Supervisor Audit Gate once
    // (or fail-closed to APPROVAL_REQUIRED when the audit cannot run). gatewayCall is NOT required here — the
    // no-gateway case must still converge, not linger RUNNING.
    await driveAuditGate(deps, workflow, claim.stageId, claim.attempt);
  }
  // P3-G: send the (display-only) Slack audit summary for any DECIDED audit whose summary is not yet SENT
  // (the decided stage may already have advanced past the frontier). Idempotent + separate outbox.
  if (deps.gatewayCall) await driveAuditSummaries(deps, workflow);
  // "succeeded" / other "stopped" (FAILED/BLOCKED/CANCELLED) / "disabled" → rest
}

// P3-G Audit Gate trigger: create the audit request bound to the exact canonical target (once), then dispatch
// the locator-only continuation to the persistent Supervisor session (reusing the chat.send(deliver:false)
// seam). The decision returns asynchronously via workflow.audit_decide — the dispatch never advances the stage.
async function driveAuditGate(deps, workflow, stageId, attempt) {
  const { rootDir } = deps;
  const wfId = workflow.workflowId;
  const spec = workflow.pipeline.find((s) => s.stageId === stageId);
  if (!spec || spec.audit?.mode !== "supervisor") return; // only supervisor-audited stages
  const rec = await readStageAttempt(rootDir, wfId, stageId, attempt).catch(() => null);
  if (!rec || rec.stageState !== "UNVERIFIED") return;
  const sessionKey = workflow.parent?.sessionKey ?? null;
  const agentId = workflow.parent?.agentId ?? null;
  const available = deps.gatewayCall && sessionKey && agentId;
  // #1/#2: the Supervisor Audit Gate needs a gateway + a persistent Supervisor session (agentId + sessionKey).
  // If any is missing the audit CANNOT run. A brand-new (never-requested) stage fails closed to
  // APPROVAL_REQUIRED. An ALREADY-REQUESTED stage converges by its continuation outbox state: a delivered
  // (SENT) or ambiguous (DELIVERY_UNKNOWN) continuation is left PAUSED so a late audit_decide can still land;
  // a continuation that never sent (no record / PENDING) escalates to human. Never linger UNVERIFIED/RUNNING.
  if (!available) {
    if (!rec.audit?.auditRequestId) {
      await escalateAuditToApproval(rootDir, wfId, { stageId, attempt, failureCode: "WORKFLOW_AUDIT_UNAVAILABLE", auditStatus: "UNAVAILABLE" });
      return;
    }
    // #1: converge the continuation outbox (parks a stale SENDING → DELIVERY_UNKNOWN, never a blind resend).
    // Preserve the request (PAUSED, late decide allowed) whenever the continuation MIGHT have been delivered:
    // SENT, DELIVERY_UNKNOWN, or a FRESH SENDING (still in flight). A `SENDING` is NEVER treated as
    // "definitely not sent". Only a PROVEN not-sent state (no record, or PENDING) escalates to a human.
    const cont = await reconcileAuditContinuation(rootDir, wfId, stageId, attempt);
    if (cont.status === "SENT" || cont.status === "DELIVERY_UNKNOWN" || cont.status === "SENDING") return;
    await escalateAuditToApproval(rootDir, wfId, { stageId, attempt, failureCode: "WORKFLOW_AUDIT_UNAVAILABLE", auditStatus: "UNAVAILABLE" });
    return;
  }
  // bind the request to the exact canonical target (idempotent — a second call returns the existing request)
  let auditRequestId = rec.audit?.auditRequestId ?? null;
  if (!auditRequestId) {
    // #2/#3: read the AUTHORITATIVE job at request time; a missing/mis-linked/diverged job (incl.
    // process/provider drift) escalates to human — no audit request is created against a contradictory target.
    const authoritativeJob = rec.jobId ? await readJob(rootDir, rec.jobId).catch(() => null) : null;
    const link = authoritativeJob?.workflowLink ?? {};
    const contradiction = !authoritativeJob
      || link.workflowId !== wfId || link.stageId !== stageId || link.attempt !== attempt
      || (link.activityIdempotencyKey ?? null) !== (rec.activityIdempotencyKey ?? null)
      || (authoritativeJob.jobOutcome ?? null) !== (rec.jobOutcome ?? null)
      || (authoritativeJob.processState ?? null) !== (rec.processState ?? null)
      || (authoritativeJob.providerState ?? null) !== (rec.providerState ?? null);
    if (contradiction) { await escalateAuditToApproval(rootDir, wfId, { stageId, attempt, failureCode: "WORKFLOW_AUDIT_TARGET_CONTRADICTION", auditStatus: "CONTRADICTION" }); return; }
    // #2: an audit request requires a COMPLETE post-run checkpoint — a missing / INCOMPLETE / hash-less
    // checkpoint.after cannot be verified, so escalate to human instead of creating a continuation.
    const cp = rec.checkpoint?.after;
    if (!cp || cp.status !== "COMPLETE" || cp.complete !== true || !cp.aggregateHash) {
      await escalateAuditToApproval(rootDir, wfId, { stageId, attempt, failureCode: "WORKFLOW_AUDIT_CHECKPOINT_UNAVAILABLE", auditStatus: "INCONCLUSIVE" });
      return;
    }
    auditRequestId = `audit-${randomUUID()}`;
    const res = await requestAudit(rootDir, wfId, {
      stageId, attempt, auditRequestId, mode: "supervisor",
      target: { workflowId: wfId, stageId, attempt, jobId: rec.jobId ?? null, activityIdempotencyKey: rec.activityIdempotencyKey ?? null, checkpointAfter: { status: cp.status, complete: cp.complete, aggregateHash: cp.aggregateHash }, jobOutcome: rec.jobOutcome ?? null, jobOutcomeSummaryHash: jobOutcomeSummaryHash(authoritativeJob) },
      contractHash: spec.auditContractHash ?? null,
    });
    auditRequestId = res.auditRequestId;
  }
  if (!auditRequestId) return;
  // dispatch the continuation once (claim → chat.send → mark). Ambiguous delivery parks DELIVERY_UNKNOWN.
  const key = auditRequestKey(wfId, stageId, attempt);
  const { claim, record } = await claimAuditContinuation(rootDir, wfId, stageId, attempt, key);
  if (!claim) return;
  const marker = record.claimedAt + ":" + auditRequestId;
  const message = buildAuditContinuationMessage({
    workflowId: wfId, stageId, attempt, jobId: rec.jobId ?? null, worktree: workflow.repository?.worktree ?? null,
    stateRoot: rootDir, checkpointAfterHash: rec.checkpoint?.after?.aggregateHash ?? null, auditRequestId,
    contract: { instruction: spec.audit.instruction, requiredChecks: spec.audit.requiredChecks }, marker,
  });
  try {
    await deps.gatewayCall("chat.send", { sessionKey, message, deliver: false, idempotencyKey: key, ...(workflow.parent?.agentId ? { agentId: workflow.parent.agentId } : {}) });
    await markAuditContinuationSent(rootDir, wfId, stageId, attempt);
  } catch (error) {
    await markAuditContinuationError(rootDir, wfId, stageId, attempt, { retryable: isProvenPreSendRejection(error), message: error?.message ?? String(error) });
  }
}

// P3-G Slack audit summary: after a decision, send ONE bounded display-only summary via the frozen route. It
// is never the audit source of truth; a send failure never reverts the canonical decision. Idempotent outbox.
async function driveAuditSummaries(deps, workflow) {
  const { rootDir } = deps;
  const wfId = workflow.workflowId;
  const route = workflow.deliveryRoute ?? {};
  if (!route.channel || !route.to) return;
  const sorted = [...(workflow.pipeline ?? [])].sort((a, b) => a.pipelineIndex - b.pipelineIndex);
  for (let i = 0; i < sorted.length; i++) {
    const spec = sorted[i];
    const proj = await readStageProjection(rootDir, wfId, spec.stageId).catch(() => null);
    if (!proj) continue;
    // #5: scan EVERY canonical attempt (1..currentAttempt), not just the latest — a resume/fallback that
    // bumped currentAttempt must not drop an earlier decided attempt's one-time summary.
    for (let attempt = 1; attempt <= proj.currentAttempt; attempt++) {
      const rec = await readStageAttempt(rootDir, wfId, spec.stageId, attempt).catch(() => null);
      const decision = rec?.audit?.decision;
      if (!decision) continue; // no decided audit → nothing to summarize
      const key = auditSummaryKey(wfId, spec.stageId, attempt, rec.audit.auditRequestId);
      const { claim } = await claimAuditSummary(rootDir, wfId, spec.stageId, attempt, key);
      if (!claim) continue; // SENT / DELIVERY_UNKNOWN / lease-held → at most once, no blind resend
      const humanRequired = decision.verdict === "BLOCKED" || decision.verdict === "INCONCLUSIVE";
      const text = buildAuditSummaryText({
        stageName: spec.stageName, attempt, verdict: decision.verdict, jobOutcome: rec.jobOutcome ?? null,
        checks: decision.checks, nextStageName: sorted[i + 1]?.stageName ?? null, humanRequired, summary: decision.summary,
      });
      try {
        await deps.gatewayCall("send", { channel: route.channel, to: route.to, ...(route.threadId ? { threadId: route.threadId } : {}), text });
        await markAuditSummarySent(rootDir, wfId, spec.stageId, attempt);
      } catch (error) {
        await markAuditSummaryError(rootDir, wfId, spec.stageId, attempt, { retryable: isProvenPreSendRejection(error), message: error?.message ?? String(error) });
      }
    }
  }
}

// Send the APPROVAL_REQUIRED notice at most once via the frozen route (claim → send → mark). A send failure
// keeps the record retryable and never changes the stage state (the workflow stays PAUSED). The notice
// carries only a bounded summary — no frozen route details or owner metadata.
async function driveApprovalRequest(deps, workflow, stageId, attempt) {
  const { rootDir } = deps;
  const { claim } = await claimApprovalSend(rootDir, workflow.workflowId, stageId, attempt);
  if (!claim) return;
  const spec = workflow.pipeline.find((s) => s.stageId === stageId);
  const route = workflow.deliveryRoute ?? {};
  const attemptRec = await readStageAttempt(rootDir, workflow.workflowId, stageId, attempt).catch(() => null);
  const summary = (attemptRec?.failureReason ?? attemptRec?.decision?.reason ?? "").toString().slice(0, 240);
  const text = [
    `[WORKFLOW_APPROVAL] ${workflow.name ?? workflow.workflowId} (${workflow.workflowId})`,
    `stage=${spec?.stageName ?? stageId} (${stageId}) attempt=${attempt}`,
    summary ? `context: ${summary}` : null,
    "Action required: approve or reject this stage.",
  ].filter(Boolean).join("\n");
  try {
    await deps.gatewayCall("send", { channel: route.channel, to: route.to, ...(route.threadId ? { threadId: route.threadId } : {}), text });
    await markApprovalSent(rootDir, workflow.workflowId, stageId, attempt);
  } catch (error) {
    // reuse the delivery/P1 classification: proven pre-send reject → retry (PENDING); ambiguous → DELIVERY_UNKNOWN
    await markApprovalError(rootDir, workflow.workflowId, stageId, attempt, { retryable: isProvenPreSendRejection(error), message: error?.message ?? String(error) });
  }
}

// Reconcile every workflow, isolating per-workflow errors (a corrupt/invariant-violating workflow never
// breaks the rest or the standalone job path). deps: { rootDir, config, startJob, startDeps, logger }.
export async function reconcileWorkflowsOnce(deps) {
  const { rootDir, logger } = deps;
  let ids;
  try {
    ids = await listWorkflowIds(rootDir); // by directory id → a missing/corrupt projection is still rebuilt
  } catch (error) {
    logger?.warn?.(`durable-jobs: workflow list failed: ${error?.message ?? error}`);
    return;
  }
  for (const workflowId of ids) {
    try {
      await advanceWorkflowOnce(deps, workflowId);
    } catch (error) {
      logger?.warn?.(`durable-jobs: workflow ${workflowId} reconcile failed: ${error?.message ?? error}`);
    }
  }
}
