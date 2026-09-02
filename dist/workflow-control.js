// P3-E workflow control-plane orchestration. Pure module (startJob/cancelJob/readJob injected; no SDK).
// Authorization-INDEPENDENT: the service authorizes the owner and passes a validated, owner-derived request.
// approve/reject/resume mutate canonical state via the store then reuse the P3-D advancement primitive
// (outside the workflow lock); cancel records the intent, cancels the active durable job OUTSIDE the lock,
// then converges the stage to CANCELLED. All actions are control-idempotent (store control records).
import { applyControlDecision, createResumeAttempt, finishStageCancel, readCheckpoint, readWorkflow, recordCancel } from "./workflow-store.js";
import { findLinkedJob, withJobCreationLock } from "./workflow-activity.js";
import { advanceWorkflowOnce } from "./workflow-reconciler.js";
import { computeWorktreeFingerprint, publicWorktreeCheckpoint } from "./workflow-fingerprint.js";

function ctlErr(code, message) { const e = new Error(`${code}: ${message}`); e.code = code; return e; }

function target(params) {
  return {
    stageId: params.stageId,
    expectedAttempt: params.attempt,
    requestId: params.requestId,
    reason: params.reason,
    actor: params.actor,
    ownerKeyHash: params.ownerKeyHash,
    payloadFingerprint: params.payloadFingerprint,
  };
}

function advanceDeps(deps) {
  // pass the P3-F fingerprint/toolchain/provider seams through so a post-control advancement preflight uses
  // the same (possibly injected) seams.
  return { rootDir: deps.rootDir, config: deps.config, startJob: deps.startJob, startDeps: deps.startDeps, logger: deps.logger, captureFingerprint: deps.captureFingerprint, captureToolchain: deps.captureToolchain, providerProbe: deps.providerProbe, providerConfigFingerprint: deps.providerConfigFingerprint };
}

// approve: frontier UNVERIFIED/APPROVAL_REQUIRED → PASSED (manual decision). Then advance one stage (enabled).
export async function controlApprove(deps, params) {
  const res = await applyControlDecision(deps.rootDir, params.workflowId, { action: "approve", ...target(params) });
  if (deps.config?.workflowEnabled) await advanceWorkflowOnce(advanceDeps(deps), params.workflowId);
  return res;
}

// reject: frontier UNVERIFIED/APPROVAL_REQUIRED → FAILED (manual decision). No advancement (FAILED stops).
export async function controlReject(deps, params) {
  return applyControlDecision(deps.rootDir, params.workflowId, { action: "reject", ...target(params) });
}

// cancel: record the cancel decision; for an active frontier, cancel the durable job OUTSIDE the lock and
// converge to CANCELLED. Allowed regardless of the feature flag (an operator must be able to stop work).
export async function controlCancel(deps, params) {
  const { rootDir } = deps;
  // recordCancel writes the cancelRequest under the WORKFLOW lock, then releases it. The job search + cancel
  // then runs under the ACTIVITY job-creation lock (same lock ensureLinkedJob takes), so a concurrent submit
  // either (a) already created the job → we find and cancel it, or (b) has not yet acquired the lock →
  // ensureLinkedJob will see the recorded cancelRequest and refuse. The two locks are NEVER held together.
  const rec = await recordCancel(rootDir, params.workflowId, target(params));
  if (rec.needsJobCancel) {
    let jobTerminalOutcome = null;
    if (rec.activityIdempotencyKey) {
      await withJobCreationLock(rootDir, rec.activityIdempotencyKey, async () => {
        const jobId = rec.jobId ?? (await findLinkedJob(rootDir, rec.activityIdempotencyKey))?.id ?? null;
        if (jobId) {
          await deps.cancelJob(rootDir, jobId).catch(() => {}); // signal the child + mark the job CANCELLED
          jobTerminalOutcome = (await deps.readJob(rootDir, jobId).catch(() => null))?.jobOutcome ?? null;
        }
      });
    }
    await finishStageCancel(rootDir, params.workflowId, { stageId: params.stageId, attempt: params.attempt, jobTerminalOutcome });
  }
  return rec;
}

// resume: create attempt N+1 preserving attempt N. checkpointPolicy=manual_rerun → MANUAL_RERUN
// (checkpointVerified=false, current P3-E behavior). checkpointPolicy=require_match → verify the CURRENT
// worktree fingerprint EXACTLY equals the source attempt's post-run checkpoint (COMPLETE) before creating a
// CHECKPOINT_RERUN attempt; a mismatch/incomplete fails closed with NO new attempt/job. Then advance (the
// preflight re-verifies the frozen expected hash right before job creation).
export async function controlResume(deps, params) {
  const { rootDir } = deps;
  const policy = params.checkpointPolicy === "require_match" ? "require_match" : "manual_rerun";
  let expectedCheckpointHash = null;
  if (policy === "require_match") {
    const wf = await readWorkflow(rootDir, params.workflowId);
    const worktree = wf?.repository?.worktree;
    const cp = await readCheckpoint(rootDir, params.workflowId, params.stageId, params.attempt);
    const after = cp?.after ?? null;
    if (!after || !after.complete || !after.aggregateHash) throw ctlErr("WORKFLOW_CHECKPOINT_UNAVAILABLE", "no COMPLETE post-run checkpoint for this attempt");
    const wt = await (deps.captureFingerprint ?? computeWorktreeFingerprint)(worktree);
    const current = publicWorktreeCheckpoint(wt);
    if (!current.complete || current.aggregateHash !== after.aggregateHash) throw ctlErr("WORKFLOW_CHECKPOINT_MISMATCH", "current worktree differs from the recorded checkpoint");
    expectedCheckpointHash = after.aggregateHash;
  }
  const res = await createResumeAttempt(rootDir, params.workflowId, { ...target(params), checkpointPolicy: policy, expectedCheckpointHash });
  if (deps.config?.workflowEnabled) await advanceWorkflowOnce(advanceDeps(deps), params.workflowId);
  return res;
}
