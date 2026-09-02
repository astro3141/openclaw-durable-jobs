// P3-B workflow tool surface — service layer. Pure module (no OpenClaw plugin-SDK/gateway dependency of its
// own; the gateway call is injected). It handles input normalization, ownership/authorization, delivery
// route freeze, start idempotency, public response shaping, and calls the P3-A workflow store. It creates
// NO durable jobs, runs NO stage commands, does NO job↔stage linkage / verdict / continuation / advancement
// / approval / cancel / resume — those are later P3 steps. workflow.start only materializes the P3-A
// workflow skeleton (all stages PENDING).
import path from "node:path";
import { createHash } from "node:crypto";
import { resolveOwnerContext } from "./ownership.js";
import { freezeOwnerConfigRoute } from "./completion-turn.js";
import {
  cancelJob,
  resolveCreationRouteBestEffort,
  resolveListFilter,
  startJob,
} from "./core.js";
import { resolveRunnerMetadata } from "./evaluator.js";
import { readJob, resolveAllowedCwd } from "./job-store.js";
import { controlApprove, controlCancel, controlReject, controlResume } from "./workflow-control.js";
import {
  WORKFLOW_STATES,
  assertWorkflowId,
  applyAuditDecision,
  withAuditDecisionLock,
  createWorkflow,
  listWorkflows,
  readStageAttempt,
  readStageProjection,
  readWorkflow,
  reconcileWorkflow,
  withCreationLock,
} from "./workflow-store.js";
import { normalizeActivity } from "./workflow-activity.js";
import { advanceWorkflowOnce } from "./workflow-reconciler.js";
import { computeWorktreeFingerprint } from "./workflow-fingerprint.js";
import { normalizeAuditPolicy, auditContractHash, validateAuditDecideInput, evaluateAuditDecision, auditVerdictToStageState, publicAuditProjection } from "./workflow-audit.js";

function svcError(code, message) {
  const e = new Error(`${code}: ${message}`);
  e.code = code;
  return e;
}

const NAME_RE = /^[A-Za-z0-9_.\- ]{1,120}$/;
const SAFE_TOKEN_RE = /^[A-Za-z0-9_.\-]{1,64}$/;
const REQUEST_ID_RE = /^[A-Za-z0-9_.:\-]{1,128}$/;
const STAGE_NAME_RE = /^[A-Za-z0-9_.\-]{1,64}$/; // stricter than a workflow name (no spaces); mirrors the store
const MAX_STAGES = 64;
const MAX_FALLBACKS = 8;

function candidateIdFor(runnerType, runnerProfile, activity) {
  return createHash("sha256").update(JSON.stringify([runnerType, runnerProfile, activity.argv, activity.timeoutSeconds ?? null])).digest("hex");
}

// ---- input normalization / validation ----
// Validate a stage's runner against its argv activity by REUSING the P2-B validator (resolveRunnerMetadata):
// this checks runnerType/runnerProfile canonical agreement AND executable compatibility (model_agy requires
// the `agy` basename; a known model executable can never be downgraded to a local runner). Returns the
// validated { runnerType, runnerProfile } and the normalized activity.
function validateStageActivity(stage, i) {
  if (stage.activity === undefined || stage.activity === null) {
    // P3-C: every executable stage must carry an activity (approval-only stages are P3-E).
    throw svcError("WORKFLOW_INPUT_INVALID", `pipeline[${i}] requires an activity { argv, timeoutSeconds? }`);
  }
  let activity;
  try {
    activity = normalizeActivity(stage.activity);
  } catch (error) {
    throw svcError("WORKFLOW_INPUT_INVALID", `pipeline[${i}] ${error.message}`);
  }
  if (stage.runnerType !== undefined && stage.runnerType !== "model" && stage.runnerType !== "local") {
    throw svcError("WORKFLOW_INPUT_INVALID", `pipeline[${i}] invalid runnerType "${stage.runnerType}"`);
  }
  let runner;
  try {
    runner = resolveRunnerMetadata({ command: activity.argv, runnerType: stage.runnerType, runnerProfile: stage.runnerProfile });
  } catch (error) {
    throw svcError("WORKFLOW_INPUT_INVALID", `pipeline[${i}] ${error.message}`);
  }
  return { runnerType: runner.runnerType, runnerProfile: runner.runnerProfile, activity };
}

export function normalizeStartInput(params) {
  if (typeof params.name !== "string" || !NAME_RE.test(params.name)) {
    throw svcError("WORKFLOW_INPUT_INVALID", "name must be a bounded safe string (1..120 of [A-Za-z0-9_.- ])");
  }
  if (typeof params.worktree !== "string" || params.worktree.length === 0) {
    throw svcError("WORKFLOW_INPUT_INVALID", "worktree is required");
  }
  if (!Array.isArray(params.pipeline) || params.pipeline.length === 0) {
    throw svcError("WORKFLOW_INPUT_INVALID", "pipeline must have at least one stage");
  }
  if (params.pipeline.length > MAX_STAGES) {
    throw svcError("WORKFLOW_INPUT_INVALID", `pipeline exceeds ${MAX_STAGES} stages`);
  }
  const seen = new Set();
  const pipeline = params.pipeline.map((stage, i) => {
    if (!stage || typeof stage.name !== "string" || !STAGE_NAME_RE.test(stage.name) || stage.name === "." || stage.name === "..") {
      throw svcError("WORKFLOW_INPUT_INVALID", `pipeline[${i}] unsafe stage name ${JSON.stringify(stage?.name)}`);
    }
    if (seen.has(stage.name)) throw svcError("WORKFLOW_INPUT_INVALID", `duplicate stage name "${stage.name}"`);
    seen.add(stage.name);
    const { runnerType, runnerProfile, activity } = validateStageActivity(stage, i);
    const candidateId = candidateIdFor(runnerType, runnerProfile, activity);
    // P3-F: validate + freeze the declared ordered fallback candidates (reuse the same P2-B runner/activity
    // validation as the primary; bounded; no duplicate candidateId).
    const fallbacks = [];
    if (stage.fallbacks !== undefined) {
      if (!Array.isArray(stage.fallbacks)) throw svcError("WORKFLOW_INPUT_INVALID", `pipeline[${i}].fallbacks must be an array`);
      if (stage.fallbacks.length > MAX_FALLBACKS) throw svcError("WORKFLOW_INPUT_INVALID", `pipeline[${i}] exceeds ${MAX_FALLBACKS} fallback candidates`);
      const ids = new Set([candidateId]);
      stage.fallbacks.forEach((fb, j) => {
        const v = validateStageActivity(fb, `${i}.fallbacks[${j}]`);
        const cid = candidateIdFor(v.runnerType, v.runnerProfile, v.activity);
        if (ids.has(cid)) throw svcError("WORKFLOW_INPUT_INVALID", `pipeline[${i}] duplicate candidate (fallback ${j})`);
        ids.add(cid);
        fallbacks.push({ candidateId: cid, runnerType: v.runnerType, runnerProfile: v.runnerProfile, activity: v.activity });
      });
    }
    // P3-G: validate + freeze the optional audit policy (default mode "none") and its contract hash.
    const audit = normalizeAuditPolicy(stage.audit, `pipeline[${i}]`);
    const contractHash = auditContractHash(audit);
    return { stageName: stage.name, pipelineIndex: i * 10, runnerType, runnerProfile, activity, candidateId, fallbacks, audit, auditContractHash: contractHash };
  });
  let verificationProfile = null;
  if (params.verificationProfile !== undefined) {
    if (typeof params.verificationProfile !== "string" || !SAFE_TOKEN_RE.test(params.verificationProfile)) {
      throw svcError("WORKFLOW_INPUT_INVALID", "verificationProfile must be a bounded safe token");
    }
    verificationProfile = params.verificationProfile;
  }
  let forbiddenActions = [];
  if (params.forbiddenActions !== undefined) {
    if (!Array.isArray(params.forbiddenActions) || params.forbiddenActions.some((a) => typeof a !== "string" || !SAFE_TOKEN_RE.test(a))) {
      throw svcError("WORKFLOW_INPUT_INVALID", "forbiddenActions must be a list of bounded safe tokens");
    }
    forbiddenActions = params.forbiddenActions;
  }
  let requestId = null;
  if (params.requestId !== undefined) {
    if (typeof params.requestId !== "string" || !REQUEST_ID_RE.test(params.requestId)) {
      throw svcError("WORKFLOW_INPUT_INVALID", "requestId must be a bounded safe string");
    }
    requestId = params.requestId;
  }
  return { name: params.name, worktree: params.worktree, pipeline, verificationProfile, forbiddenActions, requestId };
}

// ---- ownership / idempotency keys ----
function computeOwnerKey(ownerCtx) {
  // Authorization identity combines agentId with the session (trusted) or the resolved workspace
  // (context-free) — never a lone sessionKey or delivery route.
  if (ownerCtx.sessionKey) return `agent:${ownerCtx.agentId}|session:${ownerCtx.sessionKey}`;
  return `agent:${ownerCtx.agentId}|ws:${path.resolve(ownerCtx.workspaceDir ?? "")}`;
}

function sha256Hex(value) {
  return createHash("sha256").update(value).digest("hex");
}

function payloadFingerprint(normalized, resolvedWorktree) {
  // Canonical, order-stable representation of the request payload (NOT the owner/requestId key).
  const canonical = JSON.stringify({
    name: normalized.name,
    worktree: resolvedWorktree,
    verificationProfile: normalized.verificationProfile,
    forbiddenActions: normalized.forbiddenActions,
    pipeline: normalized.pipeline.map((s) => [s.stageName, s.pipelineIndex, s.runnerType, s.runnerProfile, s.activity?.argv ?? null, s.activity?.timeoutSeconds ?? null, (s.fallbacks ?? []).map((f) => f.candidateId), s.auditContractHash ?? null]),
  });
  return sha256Hex(canonical);
}

function creationLockName(ownerKey, requestId) {
  return sha256Hex(JSON.stringify([ownerKey, requestId])); // 64 hex chars → filesystem-safe lock dir name
}

// ---- authorization ----
function authorizeWorkflowAccess(config, ctx, workflow) {
  const parent = workflow.parent ?? {};
  const forbidden = () => svcError("WORKFLOW_FORBIDDEN", "not authorized for this workflow");
  if (ctx.sessionKey) {
    // Trusted session: require BOTH the agentId and the exact sessionKey to match (two factors).
    if (parent.agentId !== ctx.agentId || (parent.sessionKey ?? null) !== ctx.sessionKey) throw forbidden();
    return;
  }
  // Context-free: a trusted-session workflow is NOT reachable without its session; otherwise authorize from
  // the workflow's own stored worktree and require the configured owner's agentId to match.
  if (parent.sessionKey) throw forbidden();
  let owner;
  try {
    owner = resolveOwnerContext(config, {}, { cwd: workflow.repository?.worktree });
  } catch {
    throw forbidden();
  }
  if (parent.agentId !== owner.agentId) throw forbidden();
}

// ---- public response shaping (frozen route / requesterOrigin / ownerKey / journal internals excluded) ----
async function shapeStatus(rootDir, workflow) {
  const pipeline = [...(workflow.pipeline ?? [])].sort((a, b) => a.pipelineIndex - b.pipelineIndex);
  const stages = [];
  for (const spec of pipeline) {
    const proj = await readStageProjection(rootDir, workflow.workflowId, spec.stageId).catch(() => null);
    const attempt = proj ? await readStageAttempt(rootDir, workflow.workflowId, spec.stageId, proj.currentAttempt).catch(() => null) : null;
    stages.push({
      stageId: spec.stageId,
      stageName: spec.stageName,
      currentAttempt: proj?.currentAttempt ?? null,
      stageState: proj?.stageState ?? null,
      runnerType: spec.runnerType ?? null,
      runnerProfile: spec.runnerProfile ?? null,
      jobId: proj?.latestJobId ?? attempt?.jobId ?? null,
      processState: attempt?.processState ?? null,
      providerState: attempt?.providerState ?? null,
      jobOutcome: attempt?.jobOutcome ?? null,
      // P3-E: distinguish a MANUAL_APPROVAL PASSED from an (as-yet-unimplemented) automatic verification.
      verificationSource: attempt?.verificationSource ?? null,
      decision: attempt?.decision
        ? { action: attempt.decision.action, source: attempt.decision.source, reason: attempt.decision.reason, decidedAt: attempt.decision.decidedAt, actorAgentId: attempt.decision.actorAgentId }
        : null,
      cancel: attempt?.cancelRequest
        ? { requested: true, status: attempt.cancelRequest.status, reason: attempt.cancelRequest.reason, requestedAt: attempt.cancelRequest.requestedAt, cancelledAfterTerminal: attempt.cancelledAfterTerminal ?? false }
        : null,
      resume: attempt?.resume
        ? { resumeOfAttempt: attempt.resume.resumeOfAttempt, resumeMode: attempt.resume.resumeMode, checkpointVerified: attempt.resume.checkpointVerified }
        : null,
      // P3-F: execution candidate / preflight / checkpoint / fallback summaries (hashes + status only; never
      // raw diffs, file names, executable paths, cache keys, or provider config).
      executionCandidate: attempt?.executionCandidate
        ? { index: attempt.executionCandidate.candidateIndex, runnerType: attempt.executionCandidate.runnerType, runnerProfile: attempt.executionCandidate.runnerProfile }
        : null,
      preflight: attempt?.preflight
        ? { status: attempt.preflight.status, providerCapability: attempt.preflight.providerCapability ?? null, cacheHit: attempt.preflight.providerCacheHit ?? false, checkedAt: attempt.preflight.checkedAt ?? null, failureCode: attempt.preflight.failureCode ?? null }
        : null,
      checkpoint: attempt?.checkpoint
        ? { beforeHash: attempt.checkpoint.before?.aggregateHash ?? null, afterHash: attempt.checkpoint.after?.aggregateHash ?? null, complete: Boolean(attempt.checkpoint.before?.complete && attempt.checkpoint.after?.complete) }
        : null,
      fallback: attempt?.fallback
        ? { fromAttempt: attempt.fallback.fallbackFromAttempt, fromCandidateIndex: attempt.fallback.fallbackFromCandidateIndex, reason: attempt.fallback.reason, checkpointVerified: attempt.fallback.checkpointVerified }
        : null,
      // P3-G: audit summary (status/verdict/summary/check levels only — never raw prompt, paths, session, or keys).
      auditMode: spec.audit?.mode ?? "none",
      audit: publicAuditProjection(attempt),
    });
  }
  return {
    workflowId: workflow.workflowId,
    name: workflow.name ?? null,
    workflowState: workflow.workflowState,
    currentStage: workflow.currentStage,
    completedStages: workflow.completedStages ?? [],
    createdAt: workflow.createdAt,
    updatedAt: workflow.updatedAt,
    repository: { worktree: workflow.repository?.worktree ?? null },
    stages,
  };
}

function shapeSummary(workflow) {
  return {
    workflowId: workflow.workflowId,
    name: workflow.name ?? null,
    workflowState: workflow.workflowState,
    currentStage: workflow.currentStage,
    stageCount: Array.isArray(workflow.pipeline) ? workflow.pipeline.length : null,
    completedCount: Array.isArray(workflow.completedStages) ? workflow.completedStages.length : null,
    createdAt: workflow.createdAt,
    updatedAt: workflow.updatedAt,
  };
}

// ---- actions ----
export async function startWorkflow(deps, ctx, params) {
  const { rootDir, config, gatewayCall } = deps;
  const normalized = normalizeStartInput(params);
  // resolve owner + worktree containment (reuses the durable_job ownership/allowed-root seams)
  const ownerCtx = resolveOwnerContext(config, ctx, { cwd: normalized.worktree });
  const allowedRoots = [...(ownerCtx.durableAllowedRoots ?? []), ownerCtx.workspaceDir].filter(Boolean);
  const worktree = await resolveAllowedCwd(normalized.worktree, allowedRoots); // throws on traversal/escape
  const ownerKey = computeOwnerKey(ownerCtx);
  const fingerprint = payloadFingerprint(normalized, worktree);
  const parent = {
    agentId: ownerCtx.agentId ?? null,
    sessionKey: ownerCtx.sessionKey ?? null,
    sessionId: ownerCtx.sessionId ?? null,
    requesterOrigin: ownerCtx.deliveryContext ?? null,
    flowId: null, // P3-B creates no TaskFlow; the execution lifecycle is wired in P3-C+
  };

  const doCreate = async () => {
    if (normalized.requestId) {
      const existing = (await listWorkflows(rootDir)).find(
        (w) => w.ownerKey === ownerKey && w.requestId === normalized.requestId,
      );
      if (existing) {
        if (existing.payloadFingerprint === fingerprint) {
          return { workflow: existing, reused: true }; // idempotent retry → return the same workflow
        }
        throw svcError("WORKFLOW_REQUEST_CONFLICT", `requestId "${normalized.requestId}" already used with a different payload`);
      }
    }
    // Freeze the delivery route BEFORE createWorkflow (outside the per-workflow storage lock). If it throws,
    // createWorkflow is never called, so no workflow directory is left behind. Route resolution is
    // best-effort: an unreachable gateway (no chat.history credentials in the managed plugin-tools
    // bridge) defers to a null route rather than failing workflow creation and parent-identity freeze.
    const deliveryRoute = ownerCtx.sessionKey
      ? await resolveCreationRouteBestEffort(gatewayCall, ownerCtx)
      : freezeOwnerConfigRoute(ownerCtx.ownerDeliveryRoute, ownerCtx);
    const repository = { worktree, branch: null, baseCommit: null, verificationProfile: normalized.verificationProfile };
    const workflowId = await createWorkflow(rootDir, {
      name: normalized.name, ownerKey, requestId: normalized.requestId, payloadFingerprint: fingerprint,
      parent, repository, deliveryRoute, forbiddenActions: normalized.forbiddenActions, pipeline: normalized.pipeline,
    });
    return { workflow: await readWorkflow(rootDir, workflowId), reused: false };
  };

  // Serialize the scan-then-create against concurrent duplicate retries (same owner+requestId) with a
  // storage-root creation lock; the per-workflow .wf.lock cannot cover a not-yet-existent workflow.
  const { workflow, reused } = normalized.requestId
    ? await withCreationLock(rootDir, creationLockName(ownerKey, normalized.requestId), doCreate)
    : await doCreate();

  // P3-D: submit the first runnable stage via the SAME advancement primitive the reconciler uses.
  // advanceWorkflowOnce atomically claims the runnable PENDING frontier (stage 0 on a fresh skeleton),
  // ensures at most one linked job (dedup by key, created OUTSIDE the workflow lock), and moves it to
  // RUNNING. It submits at most one stage and never chains to the next (that needs a PASSED verdict).
  // Idempotent, so a reused/concurrent retry does not create a second job.
  await advanceWorkflowOnce({ rootDir, config, startJob: deps.startJob ?? startJob, startDeps: deps.startDeps, logger: deps.logger, captureFingerprint: deps.captureFingerprint, captureToolchain: deps.captureToolchain, providerProbe: deps.providerProbe, providerConfigFingerprint: deps.providerConfigFingerprint }, workflow.workflowId);
  const fresh = await readWorkflow(rootDir, workflow.workflowId);
  return { ...(await shapeStatus(rootDir, fresh)), reused };
}

export async function statusWorkflow(deps, ctx, params) {
  const { rootDir, config } = deps;
  const workflowId = assertWorkflowId(params.workflowId);
  const workflow = await readWorkflow(rootDir, workflowId);
  if (!workflow) throw svcError("WORKFLOW_NOT_FOUND", `no workflow ${workflowId}`);
  authorizeWorkflowAccess(config, ctx, workflow); // before any projection mutation or data return
  await reconcileWorkflow(rootDir, workflowId); // recover projections from canonical records
  const fresh = await readWorkflow(rootDir, workflowId);
  return shapeStatus(rootDir, fresh);
}

// ---- control actions (approve / reject / cancel / resume) ----
const STAGE_ID_RE = /^\d{3}-[A-Za-z0-9_.-]{1,64}$/;
const CONTROL_REQUEST_ID_RE = /^[A-Za-z0-9_.:\-]{1,128}$/;

function validateControlInput(params) {
  if (typeof params.stageId !== "string" || !STAGE_ID_RE.test(params.stageId)) throw svcError("WORKFLOW_INPUT_INVALID", "stageId must target the current frontier stage");
  if (!Number.isInteger(params.attempt) || params.attempt < 1) throw svcError("WORKFLOW_INPUT_INVALID", "attempt must be an integer >= 1");
  if (typeof params.requestId !== "string" || !CONTROL_REQUEST_ID_RE.test(params.requestId)) throw svcError("WORKFLOW_INPUT_INVALID", "requestId must be a bounded safe string");
  if (typeof params.reason !== "string" || params.reason.trim().length === 0 || params.reason.length > 2000) throw svcError("WORKFLOW_INPUT_INVALID", "reason must be a non-empty bounded string");
  let checkpointPolicy = "manual_rerun";
  if (params.checkpointPolicy !== undefined) {
    if (params.checkpointPolicy !== "manual_rerun" && params.checkpointPolicy !== "require_match") throw svcError("WORKFLOW_INPUT_INVALID", "checkpointPolicy must be manual_rerun or require_match");
    checkpointPolicy = params.checkpointPolicy;
  }
  return { stageId: params.stageId, attempt: params.attempt, requestId: params.requestId, reason: params.reason, checkpointPolicy };
}

async function controlWorkflow(deps, ctx, params, action) {
  const { rootDir, config } = deps;
  const workflowId = assertWorkflowId(params.workflowId);
  const workflow = await readWorkflow(rootDir, workflowId);
  if (!workflow) throw svcError("WORKFLOW_NOT_FOUND", `no workflow ${workflowId}`);
  authorizeWorkflowAccess(config, ctx, workflow); // only the owner may control; knowing the id is not enough
  const input = validateControlInput(params);
  // Owner/actor identity is derived from the workflow owner (never caller-injected). ownerKeyHash ties the
  // control record to the owner without storing a raw sessionKey/route.
  const ownerKeyHash = sha256Hex(workflow.ownerKey ?? `agent:${workflow.parent?.agentId ?? ""}`);
  const actor = { agentId: ctx.agentId ?? workflow.parent?.agentId ?? null };
  const payloadFingerprint = sha256Hex(JSON.stringify([action, input.stageId, input.attempt, input.reason, input.checkpointPolicy]));
  const cp = { workflowId, ...input, ownerKeyHash, payloadFingerprint, actor };
  const controlDeps = { rootDir, config, startJob: deps.startJob ?? startJob, startDeps: deps.startDeps, cancelJob, readJob, logger: deps.logger, captureFingerprint: deps.captureFingerprint, captureToolchain: deps.captureToolchain, providerProbe: deps.providerProbe, providerConfigFingerprint: deps.providerConfigFingerprint };
  if (action === "approve") await controlApprove(controlDeps, cp);
  else if (action === "reject") await controlReject(controlDeps, cp);
  else if (action === "cancel") await controlCancel(controlDeps, cp);
  else if (action === "resume") await controlResume(controlDeps, cp);
  await reconcileWorkflow(rootDir, workflowId);
  return shapeStatus(rootDir, await readWorkflow(rootDir, workflowId));
}

// ---- P3-G audit decision (trusted-auditor only) ----
const AUDIT_REQUEST_ID_RE = /^audit-[A-Za-z0-9_.:\-]{1,120}$/;

// audit_decide is restricted to the session-bound Supervisor context that received the audit request: require
// BOTH the agentId AND the exact sessionKey of the workflow parent. A context-free owner, a worker, or any
// other session is denied — knowing the workflow id (or being its owner) is not sufficient.
function authorizeAuditor(ctx, workflow) {
  const parent = workflow.parent ?? {};
  if (!ctx.sessionKey || parent.agentId !== ctx.agentId || (parent.sessionKey ?? null) !== ctx.sessionKey) {
    throw svcError("WORKFLOW_AUDIT_ACCESS_DENIED", "only the workflow's Supervisor session may submit an audit decision");
  }
}

async function auditDecideWorkflow(deps, ctx, params) {
  const { rootDir, config } = deps;
  const workflowId = assertWorkflowId(params.workflowId);
  const workflow = await readWorkflow(rootDir, workflowId);
  if (!workflow) throw svcError("WORKFLOW_NOT_FOUND", `no workflow ${workflowId}`);
  authorizeAuditor(ctx, workflow); // trusted-auditor identity BEFORE any state read/return
  if (typeof params.stageId !== "string" || !STAGE_ID_RE.test(params.stageId)) throw svcError("WORKFLOW_INPUT_INVALID", "stageId must target the audited stage");
  if (!Number.isInteger(params.attempt) || params.attempt < 1) throw svcError("WORKFLOW_INPUT_INVALID", "attempt must be an integer >= 1");
  if (typeof params.auditRequestId !== "string" || !AUDIT_REQUEST_ID_RE.test(params.auditRequestId)) throw svcError("WORKFLOW_INPUT_INVALID", "auditRequestId must be a bounded audit id");
  if (typeof params.requestId !== "string" || !CONTROL_REQUEST_ID_RE.test(params.requestId)) throw svcError("WORKFLOW_INPUT_INVALID", "requestId must be a bounded safe string");
  const decision = validateAuditDecideInput(params); // { verdict, summary, checks } — payload only
  const spec = (workflow.pipeline ?? []).find((s) => s.stageId === params.stageId);
  if (!spec || spec.audit?.mode !== "supervisor") throw svcError("WORKFLOW_AUDIT_REQUEST_NOT_FOUND", "stage is not supervisor-audited");
  evaluateAuditDecision(spec.audit, decision); // PASS sufficiency (throws WORKFLOW_AUDIT_INCOMPLETE)
  const mapped = auditVerdictToStageState(decision.verdict);
  const ownerKeyHash = sha256Hex(workflow.ownerKey ?? `agent:${workflow.parent?.agentId ?? ""}`);
  const actor = { agentId: ctx.agentId ?? workflow.parent?.agentId ?? null }; // derived, never caller-injected
  const payloadFingerprint = sha256Hex(JSON.stringify(["audit_decide", params.stageId, params.attempt, params.auditRequestId, decision.verdict, decision.summary, decision.checks]));
  // #1/#6: STRICT lock order — acquire the DEDICATED per-attempt audit-decision lock FIRST, then capture the
  // CURRENT worktree fingerprint (while holding ONLY this lock, NOT the workflow lock; no Git under the
  // workflow lock), then applyAuditDecision (which takes the workflow lock LAST and re-validates the target +
  // the AUTHORITATIVE job). This serializes concurrent decisions → exactly one decision / next job / summary,
  // and a worktree change that lands WHILE a decision waits for the lock is seen by the post-lock capture.
  await withAuditDecisionLock(rootDir, workflowId, params.stageId, params.attempt, async () => {
    // Pass the FULL current-checkpoint shape (status/complete/hash), NOT a `hash | null` — so applyAuditDecision
    // can distinguish an unavailable/incomplete fingerprint (CHECKPOINT_UNAVAILABLE) from a real hash change
    // (CHECKPOINT_CHANGED). A capture error is an explicit UNAVAILABLE.
    let currentCheckpoint = { status: "UNAVAILABLE", complete: false, aggregateHash: null };
    try {
      const wt = await (deps.captureFingerprint ?? computeWorktreeFingerprint)(workflow.repository?.worktree);
      currentCheckpoint = { status: wt?.status ?? "UNAVAILABLE", complete: wt?.status === "COMPLETE", aggregateHash: wt?.status === "COMPLETE" ? wt.aggregateHash : null };
    } catch { currentCheckpoint = { status: "UNAVAILABLE", complete: false, aggregateHash: null }; }
    return applyAuditDecision(rootDir, workflowId, {
      stageId: params.stageId, expectedAttempt: params.attempt, auditRequestId: params.auditRequestId,
      verdict: decision.verdict, verificationSource: mapped.verificationSource, decision,
      requestId: params.requestId, ownerKeyHash, payloadFingerprint, reason: decision.summary, actor,
      currentCheckpoint, readJob,
    });
  });
  // Drive the Slack summary + (if enabled) advance to the next stage via the reconciler pass.
  await advanceWorkflowOnce({ rootDir, config, startJob: deps.startJob ?? startJob, startDeps: deps.startDeps, gatewayCall: deps.gatewayCall, logger: deps.logger, captureFingerprint: deps.captureFingerprint, captureToolchain: deps.captureToolchain, providerProbe: deps.providerProbe, providerConfigFingerprint: deps.providerConfigFingerprint }, workflowId);
  return shapeStatus(rootDir, await readWorkflow(rootDir, workflowId));
}

// Single dispatcher used by the plugin tool adapter. The `workflow` tool is ALWAYS registered (the OpenClaw
// loader enforces registered⊆contracts.tools at registration, and a declared-but-unregistered tool would
// throw "runtime unavailable" from the manifest descriptor path), so the feature flag gates BEHAVIOR here.
// P3-E: `cancel` is allowed regardless of the flag (an operator must be able to stop active work); start/
// approve/reject/resume require workflowEnabled (they can submit/advance work).
export async function runWorkflowAction(deps, ctx, params) {
  const enabled = deps.config?.workflowEnabled === true;
  if (params.action === "cancel") return controlWorkflow(deps, ctx, params, "cancel");
  // P3-G: audit_decide converges an ALREADY-REQUESTED audit (applyAuditDecision fail-closes if none exists), so
  // it is allowed regardless of workflowEnabled — but the post-decision next-stage advancement still respects
  // the flag inside advanceWorkflowOnce. New audit REQUEST creation (the reconciler) remains flag-gated.
  if (params.action === "audit_decide") return auditDecideWorkflow(deps, ctx, params);
  if (!enabled) throw svcError("WORKFLOW_DISABLED", "workflow tool is disabled (set workflowEnabled: true to enable)");
  if (params.action === "start") return startWorkflow(deps, ctx, params);
  if (params.action === "status") return statusWorkflow(deps, ctx, params);
  if (params.action === "list") return listWorkflowSummaries(deps, ctx, params);
  if (params.action === "approve") return controlWorkflow(deps, ctx, params, "approve");
  if (params.action === "reject") return controlWorkflow(deps, ctx, params, "reject");
  if (params.action === "resume") return controlWorkflow(deps, ctx, params, "resume");
  throw svcError("WORKFLOW_ACTION_UNKNOWN", `unsupported action: ${params.action}`);
}

export async function listWorkflowSummaries(deps, ctx, params) {
  const { rootDir, config } = deps;
  const filter = resolveListFilter(config, ctx, { cwd: params.cwd }); // { agentId, sessionKey }
  if (params.state !== undefined && !WORKFLOW_STATES.has(params.state)) {
    throw svcError("WORKFLOW_INPUT_INVALID", `unknown state filter "${params.state}"`);
  }
  const limit = Number.isInteger(params.limit) ? Math.min(Math.max(params.limit, 1), 100) : 20;
  // listWorkflows already isolates per-item read errors (a corrupt workflow.json is skipped, never guessed
  // or exposed to a possibly-wrong owner).
  const all = await listWorkflows(rootDir);
  return all
    .filter((w) => {
      const parent = w.parent ?? {};
      return parent.agentId === filter.agentId && (parent.sessionKey ?? null) === (filter.sessionKey ?? null);
    })
    .filter((w) => params.state === undefined || w.workflowState === params.state)
    .sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)))
    .slice(0, limit)
    .map(shapeSummary);
}
