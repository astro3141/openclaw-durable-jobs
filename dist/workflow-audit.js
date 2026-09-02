// P3-G Supervisor Audit Gate — pure helpers (no SDK / no gateway / no fs). The Audit Gate wakes the existing
// persistent Supervisor ACP session (reusing the P1 chat.send(deliver:false) continuation seam) to inspect the
// CANONICAL records directly and return a decision via workflow.audit_decide. This module only validates the
// audit policy, computes the frozen contract hash, builds the (locator-only, evidence-free) continuation
// request, enforces the verdict/verification-level rules, and shapes the bounded Slack summary + public status.
//
// It does NOT parse worker/tool traces, build an Evidence Pack, or persist an Audit Receipt — the auditor
// reads the existing source-of-truth. Slack is a display layer, never the audit source of truth.
import { createHash } from "node:crypto";

export const AUDIT_MODES = new Set(["none", "supervisor"]);
// Bounded enum of declarable required checks (repository-specific semantics are NOT hardcoded in core).
export const AUDIT_REQUIRED_CHECKS = new Set([
  "scope", "repository_invariants", "declared_tests", "job_outcome_consistency", "checkpoint_consistency", "artifact_presence",
]);
export const AUDIT_VERDICTS = new Set(["PASS", "FAIL", "BLOCKED", "INCONCLUSIVE"]);
// Verification levels that are SUFFICIENT to support a PASS on a required check. WORKER_REPORTED / INFERRED are
// explicitly NOT sufficient (a worker narrative can never carry a stage to PASSED).
export const SUFFICIENT_VERIFICATION_LEVELS = new Set(["REEXECUTED", "LOG_VERIFIED", "ARTIFACT_VERIFIED"]);
export const ALL_VERIFICATION_LEVELS = new Set([...SUFFICIENT_VERIFICATION_LEVELS, "WORKER_REPORTED", "INFERRED"]);

const AUDIT_INSTRUCTION_MAX = 4000;
const AUDIT_SUMMARY_MAX_LINES = 12;
const AUDIT_SUMMARY_MAX_CHARS = 1200;
const AUDIT_CHECK_DETAIL_MAX = 500;

function auditError(code, message) { const e = new Error(`${code}: ${message}`); e.code = code; return e; }
export function sha256Hex(value) { return createHash("sha256").update(value).digest("hex"); }

// #4: sanitize an auditor-supplied summary / check detail BEFORE it is stored or shown (canonical, public
// status, and Slack). Strips absolute paths, home dirs, bearer/authorization, token/secret/session/owner/
// channel key=value forms, sk-* keys, and Slack channel ids. Only sanitized text is ever persisted.
export function redactAuditText(text) {
  if (typeof text !== "string") return text;
  return text
    .replace(/[A-Za-z]:\\[^\s"']*/g, "<redacted-path>")                                        // Windows path
    .replace(/\/(?:Users|home|private|tmp|var|opt|etc|root|mnt|srv)\/[^\s"']*/g, "<redacted-path>") // Unix path
    .replace(/~\/[^\s"']*/g, "<redacted-path>")                                                 // home dir
    .replace(/\bBearer\s+[A-Za-z0-9._~+/-]+=*/gi, "Bearer <redacted>")
    .replace(/\b(authorization|token|password|secret|api[_-]?key|private[_-]?key|sessionkey|sessionid|ownerkey|ownerkeyhash|channel|thread(?:id|ts|_ts)?)\b\s*[:=]\s*[^\s"',;]+/gi, "$1=<redacted>")
    .replace(/\bsk-[A-Za-z0-9]{8,}\b/g, "<redacted-key>")
    .replace(/\bC[A-Z0-9]{8,}\b/g, "<redacted-id>");                                            // Slack channel id
}

// Validate + normalize a stage's optional audit policy. Default mode "none" preserves the pre-P3-G behavior.
// A "supervisor" policy requires a bounded instruction and a bounded set of allowed requiredChecks.
export function normalizeAuditPolicy(rawAudit, where) {
  if (rawAudit === undefined || rawAudit === null) return { mode: "none" };
  if (typeof rawAudit !== "object" || Array.isArray(rawAudit)) throw auditError("WORKFLOW_INPUT_INVALID", `${where} audit must be an object`);
  const allowed = new Set(["mode", "instruction", "requiredChecks"]);
  for (const k of Object.keys(rawAudit)) if (!allowed.has(k)) throw auditError("WORKFLOW_INPUT_INVALID", `${where} audit has unexpected field ${k}`);
  const mode = rawAudit.mode ?? "none";
  if (!AUDIT_MODES.has(mode)) throw auditError("WORKFLOW_INPUT_INVALID", `${where} audit.mode must be none|supervisor`);
  if (mode === "none") return { mode: "none" };
  const instruction = rawAudit.instruction;
  if (typeof instruction !== "string" || instruction.trim().length === 0 || instruction.length > AUDIT_INSTRUCTION_MAX) {
    throw auditError("WORKFLOW_INPUT_INVALID", `${where} audit.instruction must be a non-empty bounded string`);
  }
  if (!Array.isArray(rawAudit.requiredChecks) || rawAudit.requiredChecks.length === 0) {
    throw auditError("WORKFLOW_INPUT_INVALID", `${where} audit.requiredChecks must be a non-empty array`);
  }
  const requiredChecks = [];
  const seen = new Set();
  for (const c of rawAudit.requiredChecks) {
    if (!AUDIT_REQUIRED_CHECKS.has(c)) throw auditError("WORKFLOW_INPUT_INVALID", `${where} audit.requiredChecks has invalid check "${c}"`);
    if (!seen.has(c)) { seen.add(c); requiredChecks.push(c); }
  }
  return { mode: "supervisor", instruction: instruction.trim(), requiredChecks };
}

// Frozen contract hash — the decisive, order-stable fingerprint of the audit contract, checked at decide-time
// so a decision can never be applied against a mutated contract.
export function auditContractHash(policy) {
  if (!policy || policy.mode !== "supervisor") return null;
  return sha256Hex(JSON.stringify({ mode: policy.mode, instruction: policy.instruction, requiredChecks: [...policy.requiredChecks].sort() }));
}

export function auditRequestKey(workflowId, stageId, attempt) { return `wf:${workflowId}:audit:${stageId}:attempt:${attempt}`; }
export function auditSummaryKey(workflowId, stageId, attempt, auditRequestId) { return `wf:${workflowId}:audit-summary:${stageId}:attempt:${attempt}:${auditRequestId}`; }

// Validate the auditor-supplied decision payload (verdict + bounded summary + per-check results). Trusted
// identity, target binding, and contract are NOT taken from the payload — the harness derives them.
export function validateAuditDecideInput(params) {
  if (!AUDIT_VERDICTS.has(params.verdict)) throw auditError("WORKFLOW_INPUT_INVALID", "verdict must be PASS|FAIL|BLOCKED|INCONCLUSIVE");
  if (typeof params.summary !== "string" || params.summary.trim().length === 0 || params.summary.length > AUDIT_SUMMARY_MAX_CHARS) {
    throw auditError("WORKFLOW_INPUT_INVALID", "summary must be a non-empty bounded string");
  }
  if (!Array.isArray(params.checks)) throw auditError("WORKFLOW_INPUT_INVALID", "checks must be an array");
  const checks = params.checks.map((c, i) => {
    if (!c || typeof c !== "object" || Array.isArray(c)) throw auditError("WORKFLOW_INPUT_INVALID", `checks[${i}] must be an object`);
    if (!AUDIT_REQUIRED_CHECKS.has(c.check)) throw auditError("WORKFLOW_INPUT_INVALID", `checks[${i}].check invalid`);
    if (!["PASS", "FAIL", "NOT_CHECKED"].includes(c.result)) throw auditError("WORKFLOW_INPUT_INVALID", `checks[${i}].result must be PASS|FAIL|NOT_CHECKED`);
    const level = c.verificationLevel ?? null;
    if (level !== null && !ALL_VERIFICATION_LEVELS.has(level)) throw auditError("WORKFLOW_INPUT_INVALID", `checks[${i}].verificationLevel invalid`);
    // #4: sanitize the detail before it is ever stored/shown (bounded).
    const detail = c.detail === undefined || c.detail === null ? null : redactAuditText(String(c.detail)).slice(0, AUDIT_CHECK_DETAIL_MAX);
    return { check: c.check, result: c.result, verificationLevel: level, detail };
  });
  // #4: sanitize the summary; reject if it is empty AFTER redaction (an all-secret summary carries no signal).
  const summary = redactAuditText(params.summary.trim()).trim();
  if (summary.length === 0) throw auditError("WORKFLOW_INPUT_INVALID", "summary is empty after redaction");
  return { verdict: params.verdict, summary, checks };
}

// Enforce the PASS contract: EVERY declared requiredCheck must be present with result PASS and a SUFFICIENT
// verificationLevel (REEXECUTED | LOG_VERIFIED | ARTIFACT_VERIFIED). A missing check, NOT_CHECKED, FAIL, or a
// WORKER_REPORTED/INFERRED level rejects a PASS with WORKFLOW_AUDIT_INCOMPLETE. FAIL/BLOCKED/INCONCLUSIVE do
// not require the sufficiency proof (they never advance the stage).
export function evaluateAuditDecision(policy, decision) {
  if (decision.verdict !== "PASS") return { ok: true };
  const byCheck = new Map(decision.checks.map((c) => [c.check, c]));
  for (const req of policy.requiredChecks ?? []) {
    const c = byCheck.get(req);
    if (!c || c.result !== "PASS") throw auditError("WORKFLOW_AUDIT_INCOMPLETE", `PASS requires required check "${req}" to be verified PASS`);
    if (!SUFFICIENT_VERIFICATION_LEVELS.has(c.verificationLevel)) throw auditError("WORKFLOW_AUDIT_INCOMPLETE", `required check "${req}" needs a REEXECUTED/LOG_VERIFIED/ARTIFACT_VERIFIED level (got ${c.verificationLevel ?? "none"})`);
  }
  return { ok: true };
}

// Map a validated verdict to the target stage state. PASS never fabricates process/provider/jobOutcome.
export function auditVerdictToStageState(verdict) {
  if (verdict === "PASS") return { stageState: "PASSED", verificationSource: "INDEPENDENT_AUDIT" };
  if (verdict === "FAIL") return { stageState: "FAILED", verificationSource: null };
  return { stageState: "APPROVAL_REQUIRED", verificationSource: null }; // BLOCKED | INCONCLUSIVE → human
}

// The locator-only, evidence-FREE continuation request delivered to the Supervisor session. It carries the
// trusted locators + frozen contract + the exact workflow.audit_decide call shape as a machine-readable JSON
// block, and a unique marker so the dispatch can be correlated. No pre-summarized evidence, no raw paths.
export function buildAuditContinuationMessage({ workflowId, stageId, attempt, jobId, worktree, stateRoot, checkpointAfterHash, auditRequestId, contract, marker }) {
  const request = {
    role: "independent_audit_gate",
    target: { workflowId, stageId, attempt, jobId, worktree, stateRoot, checkpointAfterHash, auditRequestId },
    contract: { instruction: contract.instruction, requiredChecks: contract.requiredChecks },
    respondBy: {
      tool: "workflow", action: "audit_decide",
      fields: ["workflowId", "stageId", "attempt", "auditRequestId", "requestId", "verdict", "summary", "checks"],
      verdicts: ["PASS", "FAIL", "BLOCKED", "INCONCLUSIVE"],
      checkVerificationLevels: ["REEXECUTED", "LOG_VERIFIED", "ARTIFACT_VERIFIED"],
    },
  };
  return [
    "[WORKFLOW_AUDIT_REQUEST v1]",
    "You are the independent audit gate. Do not trust the worker's final narrative as evidence.",
    "Directly inspect the canonical workflow attempt, authoritative job row, stdout/stderr/heartbeat logs,",
    "Git repository state, checkpoint, and declared test artifacts. Re-run only the minimum checks required",
    "to confirm or overturn the conclusion. Do not modify repository files, workflow state, Git refs, index,",
    "or artifacts. Return the decision by calling workflow.audit_decide.",
    "AUDIT_REQUEST_JSON=" + JSON.stringify(request),
    `audit_marker=${marker}`,
  ].join("\n");
}

// Bounded, display-only Slack summary (never the audit source of truth). No raw logs, absolute paths,
// owner/session/route, tokens, or full narrative — required-check results + verification levels + the Audit
// Gate decision, with jobOutcome kept distinct from the stage verdict, and the next step / human-required flag.
export function buildAuditSummaryText({ stageName, attempt, verdict, jobOutcome, checks, nextStageName, humanRequired, summary }) {
  const head = verdict === "PASS" ? "AUDIT PASS" : verdict === "FAIL" ? "AUDIT FAIL" : `AUDIT ${verdict}`;
  const lines = [`[${head}] ${stageName} · attempt ${attempt}`];
  for (const c of (checks ?? []).slice(0, 6)) {
    lines.push(`${c.check}: ${c.result}${c.verificationLevel ? ` (${c.verificationLevel})` : ""}`);
  }
  lines.push(`job: ${jobOutcome ?? "unknown"}`);
  lines.push(`decision: ${verdict === "PASS" ? "INDEPENDENT_AUDIT → stage PASSED" : verdict === "FAIL" ? "stage FAILED" : "HUMAN APPROVAL REQUIRED"}`);
  if (humanRequired) lines.push("next: human approval");
  else if (verdict === "PASS") lines.push(`next: ${nextStageName ?? "workflow complete"}`);
  else lines.push("next: none");
  if (summary) lines.push(`note: ${String(summary).slice(0, 160)}`);
  return lines.slice(0, AUDIT_SUMMARY_MAX_LINES).join("\n").slice(0, AUDIT_SUMMARY_MAX_CHARS);
}

// Public status projection of an attempt's audit metadata (status/verdict/summary/check levels only; never the
// raw ACP prompt, absolute paths, owner/session/route, full logs, tool trace, idempotency keys, or checkpoint
// internals).
export function publicAuditProjection(attempt) {
  const a = attempt?.audit;
  if (!a) return null;
  return {
    mode: a.mode ?? null,
    status: a.status ?? null,
    auditRequestId: a.auditRequestId ?? null,
    requestedAt: a.requestedAt ?? null,
    failureCode: a.failureCode ?? a.decision?.failureCode ?? null, // e.g. WORKFLOW_AUDIT_UNAVAILABLE / _TARGET_CONTRADICTION / _CHECKPOINT_CHANGED
    decidedAt: a.decision?.decidedAt ?? null,
    verdict: a.decision?.verdict ?? null,
    summary: a.decision?.summary ?? null,
    checks: Array.isArray(a.decision?.checks)
      ? a.decision.checks.map((c) => ({ check: c.check, result: c.result, verificationLevel: c.verificationLevel ?? null }))
      : null,
  };
}
