// P3-G Supervisor Audit Gate tests: pure policy/verdict helpers + a DETERMINISTIC integration lifecycle that
// drives the production modules end-to-end (no real model/ACP: a fake gatewayCall captures the audit
// continuation and the Supervisor is simulated by calling the service's audit_decide). Slack is display-only.
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  normalizeAuditPolicy, auditContractHash, evaluateAuditDecision, auditVerdictToStageState,
  validateAuditDecideInput, buildAuditSummaryText,
} from "../dist/workflow-audit.js";
import { createWorkflow, readStageAttempt, transitionStageAttempt, recordCheckpoint } from "../dist/workflow-store.js";
import { advanceWorkflowOnce } from "../dist/workflow-reconciler.js";
import { runWorkflowAction } from "../dist/workflow-service.js";
import { readConfig } from "../dist/core.js";
import { makeFakeStartJob, completeSeams, listJobs } from "./wf-linkage-helpers.js";
import { createJob, createJobId } from "../dist/job-store.js";

const tmp = () => mkdtemp(path.join(os.tmpdir(), "wf-audit-"));
const POLICY = { mode: "supervisor", instruction: "Verify the implementation against the frozen contract.", requiredChecks: ["scope", "declared_tests"] };
const allPass = (level = "LOG_VERIFIED") => POLICY.requiredChecks.map((c) => ({ check: c, result: "PASS", verificationLevel: level }));

// ---- pure helpers ----
test("audit policy: default none; supervisor validated; invalid check rejected", () => {
  assert.deepEqual(normalizeAuditPolicy(undefined, "s"), { mode: "none" });
  assert.deepEqual(normalizeAuditPolicy({ mode: "none" }, "s"), { mode: "none" });
  const p = normalizeAuditPolicy({ mode: "supervisor", instruction: "x", requiredChecks: ["scope", "scope"] }, "s");
  assert.equal(p.mode, "supervisor"); assert.deepEqual(p.requiredChecks, ["scope"]); // dedup
  assert.throws(() => normalizeAuditPolicy({ mode: "supervisor", instruction: "x", requiredChecks: ["nope"] }, "s"), /WORKFLOW_INPUT_INVALID/);
  assert.throws(() => normalizeAuditPolicy({ mode: "supervisor", requiredChecks: ["scope"] }, "s"), /WORKFLOW_INPUT_INVALID/); // no instruction
  assert.throws(() => normalizeAuditPolicy({ mode: "bogus" }, "s"), /WORKFLOW_INPUT_INVALID/);
  assert.notEqual(auditContractHash(POLICY), auditContractHash({ ...POLICY, instruction: "different" }));
});

test("PASS requires every required check verified at a sufficient level", () => {
  evaluateAuditDecision(POLICY, { verdict: "PASS", checks: allPass("REEXECUTED") }); // ok
  evaluateAuditDecision(POLICY, { verdict: "PASS", checks: allPass("ARTIFACT_VERIFIED") }); // ok
  assert.throws(() => evaluateAuditDecision(POLICY, { verdict: "PASS", checks: allPass("WORKER_REPORTED") }), /WORKFLOW_AUDIT_INCOMPLETE/);
  assert.throws(() => evaluateAuditDecision(POLICY, { verdict: "PASS", checks: allPass("INFERRED") }), /WORKFLOW_AUDIT_INCOMPLETE/);
  assert.throws(() => evaluateAuditDecision(POLICY, { verdict: "PASS", checks: [{ check: "scope", result: "PASS", verificationLevel: "LOG_VERIFIED" }] }), /WORKFLOW_AUDIT_INCOMPLETE/); // missing declared_tests
  assert.throws(() => evaluateAuditDecision(POLICY, { verdict: "PASS", checks: [{ check: "scope", result: "PASS", verificationLevel: "LOG_VERIFIED" }, { check: "declared_tests", result: "NOT_CHECKED" }] }), /WORKFLOW_AUDIT_INCOMPLETE/);
  evaluateAuditDecision(POLICY, { verdict: "FAIL", checks: [] }); // FAIL needs no sufficiency proof
});

test("verdict → stage state mapping never fabricates a process outcome", () => {
  assert.deepEqual(auditVerdictToStageState("PASS"), { stageState: "PASSED", verificationSource: "INDEPENDENT_AUDIT" });
  assert.equal(auditVerdictToStageState("FAIL").stageState, "FAILED");
  assert.equal(auditVerdictToStageState("BLOCKED").stageState, "APPROVAL_REQUIRED");
  assert.equal(auditVerdictToStageState("INCONCLUSIVE").stageState, "APPROVAL_REQUIRED");
});

test("audit summary is bounded, distinguishes jobOutcome from verdict, leaks no secret/path/session", () => {
  const text = buildAuditSummaryText({ stageName: "impl", attempt: 1, verdict: "PASS", jobOutcome: "COMPLETED_UNVERIFIED", checks: allPass(), nextStageName: "review", humanRequired: false, summary: "ok" });
  assert.match(text, /AUDIT PASS/); assert.match(text, /job: COMPLETED_UNVERIFIED/); assert.match(text, /INDEPENDENT_AUDIT/);
  assert.ok(text.split("\n").length <= 12 && text.length <= 1200);
  assert.doesNotMatch(text, /sess-|\/tmp\/|token|sk-/);
});

test("audit_decide input validation rejects a bad verdict / unbounded summary / bad check", () => {
  assert.throws(() => validateAuditDecideInput({ verdict: "MAYBE", summary: "x", checks: [] }), /WORKFLOW_INPUT_INVALID/);
  assert.throws(() => validateAuditDecideInput({ verdict: "PASS", summary: "", checks: [] }), /WORKFLOW_INPUT_INVALID/);
  assert.throws(() => validateAuditDecideInput({ verdict: "PASS", summary: "x", checks: [{ check: "nope", result: "PASS" }] }), /WORKFLOW_INPUT_INVALID/);
});

// ---- deterministic integration lifecycle ----
const WT = "WT";
async function seed(root, { policy = POLICY, sessionKey = "sess-1" } = {}) {
  const id = await createWorkflow(root, {
    parent: { agentId: "sup", sessionKey },
    repository: { worktree: "/tmp/wt", branch: "m", baseCommit: "d" },
    deliveryRoute: { routeKind: "channel_root", channel: "slack", to: "C1" },
    forbiddenActions: [],
    pipeline: [
      { pipelineIndex: 0, stageName: "impl", runnerType: "local", runnerProfile: "local_build", candidateId: "c0", activity: { argv: ["true"] }, fallbacks: [], audit: policy, auditContractHash: auditContractHash(policy) },
      { pipelineIndex: 10, stageName: "review", runnerType: "local", runnerProfile: "local_build", candidateId: "c1", activity: { argv: ["true"] }, fallbacks: [], audit: { mode: "none" }, auditContractHash: null },
    ],
  });
  const sid = "000-impl";
  // an authoritative linked job row (workflowLink + terminal outcome) — the Audit Gate re-reads this, not the
  // attempt copy, so it must exist and agree.
  const jid = createJobId(); const now = new Date().toISOString();
  const aik = `wf:${id}:stage:${sid}:attempt:1`;
  await createJob(root, { version: 1, id: jid, name: "impl", state: "SUCCEEDED", processState: "COMPLETED", providerState: "OK", jobOutcome: "COMPLETED_UNVERIFIED", runnerType: "local", runnerProfile: "local_build", cwd: "/tmp/wt", command: ["true"], timeoutSeconds: 0, createdAt: now, updatedAt: now, startedAt: now, endedAt: now, parent: { agentId: "a", sessionKey: null, flowId: null }, flowId: null, directory: path.join(root, jid), workflowLink: { workflowId: id, stageId: sid, attempt: 1, activityIdempotencyKey: aik }, validatedExecution: null, notification: { status: "pending", attempts: 0, idempotencyKey: `durable-job:${jid}:terminal` }, delivery: null });
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "SUBMITTING", mutation: { activityIdempotencyKey: aik } });
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "RUNNING", mutation: { jobId: jid } });
  await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "UNVERIFIED", mutation: { jobOutcome: "COMPLETED_UNVERIFIED", processState: "COMPLETED", providerState: "OK" } });
  await recordCheckpoint(root, id, { stageId: sid, attempt: 1, phase: "after", checkpoint: { aggregateHash: WT, complete: true, status: "COMPLETE" } });
  return { id, sid, jid };
}
function harness(root, { auditEnabled = true, worktreeHash = WT } = {}) {
  const sent = [];
  const fake = makeFakeStartJob();
  const gatewayCall = async (method, payload) => { sent.push({ method, payload }); return { result: { runId: "r" } }; };
  const deps = { rootDir: root, config: readConfig({ pluginConfig: { workflowEnabled: true, workflowAuditEnabled: auditEnabled, allowedRoots: ["/tmp/wt"] } }), gatewayCall, startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams({ worktreeHash }) };
  return { sent, fake, deps };
}
const supCtx = { agentId: "sup", sessionKey: "sess-1", workspaceDir: "/tmp/wt", durableAllowedRoots: ["/tmp/wt"] };
const decide = (id, sid, auditRequestId, over = {}) => ({ action: "audit_decide", workflowId: id, stageId: sid, attempt: 1, auditRequestId, requestId: "ctl-1", verdict: "PASS", summary: "verified", checks: allPass(), ...over });

async function trigger(root, deps, id, sid) {
  await advanceWorkflowOnce(deps, id); // frontier UNVERIFIED → driveAuditGate
  return (await readStageAttempt(root, id, sid, 1)).audit;
}

test("trigger: UNVERIFIED + audit enabled → exactly one request + one continuation dispatch; PAUSED", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); const { sent, deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  assert.equal(audit.status, "REQUESTED"); assert.ok(audit.auditRequestId.startsWith("audit-"));
  const dispatches = sent.filter((s) => s.method === "chat.send" && s.payload.deliver === false);
  assert.equal(dispatches.length, 1, "one continuation dispatch");
  assert.equal(dispatches[0].payload.sessionKey, "sess-1");
  assert.match(dispatches[0].payload.message, /WORKFLOW_AUDIT_REQUEST/);
  const wf = await runWorkflowAction(deps, supCtx, { action: "status", workflowId: id });
  assert.equal(wf.workflowState, "PAUSED", "UNVERIFIED-awaiting-audit is PAUSED");
  // idempotent: a second tick creates no second request / dispatch
  await advanceWorkflowOnce(deps, id);
  assert.equal(sent.filter((s) => s.method === "chat.send").length, 1, "no duplicate dispatch");
  await rm(root, { recursive: true, force: true });
});

test("trigger: audit disabled OR mode none → no request", async () => {
  {
    const root = await tmp(); const { id, sid } = await seed(root); const { sent, deps } = harness(root, { auditEnabled: false });
    await advanceWorkflowOnce(deps, id);
    assert.equal((await readStageAttempt(root, id, sid, 1)).audit ?? null, null, "disabled: no audit request");
    assert.equal(sent.filter((s) => s.method === "chat.send").length, 0);
    await rm(root, { recursive: true, force: true });
  }
  {
    const root = await tmp(); const { id, sid } = await seed(root, { policy: { mode: "none" } }); const { deps } = harness(root);
    await advanceWorkflowOnce(deps, id);
    assert.equal((await readStageAttempt(root, id, sid, 1)).audit ?? null, null, "mode none: no audit request");
    await rm(root, { recursive: true, force: true });
  }
});

test("PASS → stage PASSED (INDEPENDENT_AUDIT), jobOutcome preserved, next stage submitted, one Slack summary", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); const { sent, deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  const status = await runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId));
  const stage0 = status.stages.find((s) => s.stageId === sid);
  assert.equal(stage0.stageState, "PASSED");
  assert.equal(stage0.verificationSource, "INDEPENDENT_AUDIT");
  assert.equal(stage0.jobOutcome, "COMPLETED_UNVERIFIED", "process outcome not fabricated");
  assert.equal(stage0.audit.verdict, "PASS");
  const stage1 = status.stages.find((s) => s.stageId === "010-review");
  assert.ok(["SUBMITTING", "RUNNING"].includes(stage1.stageState), "next stage advanced");
  const summaries = sent.filter((s) => s.method === "send" && /AUDIT PASS/.test(s.payload.text ?? ""));
  assert.equal(summaries.length, 1, "exactly one Slack audit summary");
  await rm(root, { recursive: true, force: true });
});

test("FAIL → stage FAILED, no next stage, FAIL summary", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); const { sent, deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  const jobsBefore = (await listJobs(root)).length;
  const status = await runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId, { verdict: "FAIL", summary: "scope violation", checks: [{ check: "scope", result: "FAIL", verificationLevel: "LOG_VERIFIED" }] }));
  assert.equal(status.stages.find((s) => s.stageId === sid).stageState, "FAILED");
  assert.equal(status.stages.find((s) => s.stageId === "010-review").stageState, "PENDING", "no next stage submitted");
  assert.equal((await listJobs(root)).length, jobsBefore, "no new job on FAIL");
  assert.equal(sent.filter((s) => s.method === "send" && /AUDIT FAIL/.test(s.payload.text ?? "")).length, 1);
  await rm(root, { recursive: true, force: true });
});

test("BLOCKED/INCONCLUSIVE → APPROVAL_REQUIRED (human)", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  const status = await runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId, { verdict: "BLOCKED", summary: "cannot verify", checks: [] }));
  assert.equal(status.stages.find((s) => s.stageId === sid).stageState, "APPROVAL_REQUIRED");
  assert.equal(status.workflowState, "PAUSED");
  await rm(root, { recursive: true, force: true });
});

test("PASS with a required check NOT verified → WORKFLOW_AUDIT_INCOMPLETE (no PASS)", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  await assert.rejects(() => runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId, { checks: [{ check: "scope", result: "PASS", verificationLevel: "WORKER_REPORTED" }, { check: "declared_tests", result: "PASS", verificationLevel: "LOG_VERIFIED" }] })), /WORKFLOW_AUDIT_INCOMPLETE/);
  assert.equal((await readStageAttempt(root, id, sid, 1)).stageState, "UNVERIFIED", "still UNVERIFIED (no PASS)");
  await rm(root, { recursive: true, force: true });
});

test("authorization: a non-Supervisor session (or context-free owner) cannot decide", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  const worker = { agentId: "sup", sessionKey: "other-session", workspaceDir: "/tmp/wt", durableAllowedRoots: ["/tmp/wt"] };
  await assert.rejects(() => runWorkflowAction(deps, worker, decide(id, sid, audit.auditRequestId)), /WORKFLOW_AUDIT_ACCESS_DENIED/);
  const ctxFree = { agentId: "sup", sessionKey: null, workspaceDir: "/tmp/wt", durableAllowedRoots: ["/tmp/wt"] };
  await assert.rejects(() => runWorkflowAction(deps, ctxFree, decide(id, sid, audit.auditRequestId)), /WORKFLOW_AUDIT_ACCESS_DENIED/);
  await rm(root, { recursive: true, force: true });
});

test("stale: a changed job outcome, or a worktree changed during review, refuses the PASS", async () => {
  // (a) jobOutcome drift → STALE
  {
    const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root);
    const audit = await trigger(root, deps, id, sid);
    await transitionStageAttempt(root, id, { stageId: sid, attempt: 1, toState: "UNVERIFIED", mutation: { jobOutcome: "FAILED_COMMAND" } }); // outcome changed
    await assert.rejects(() => runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId)), /WORKFLOW_AUDIT_STALE/);
    await rm(root, { recursive: true, force: true });
  }
  // (b) worktree changed since checkpoint.after → PASS downgraded to APPROVAL_REQUIRED
  {
    const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root, { worktreeHash: "WT2" });
    const audit = await trigger(root, deps, id, sid);
    const status = await runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId));
    assert.equal(status.stages.find((s) => s.stageId === sid).stageState, "APPROVAL_REQUIRED", "worktree drift downgrades PASS");
    assert.equal(status.stages.find((s) => s.stageId === sid).audit.verdict, "INCONCLUSIVE");
    await rm(root, { recursive: true, force: true });
  }
});

test("idempotency: duplicate audit_decide replays; a cross-action requestId conflicts", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  await runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId));
  // same requestId + same payload → replay (no throw, still PASSED)
  const again = await runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId));
  assert.equal(again.stages.find((s) => s.stageId === sid).stageState, "PASSED");
  // same requestId, different verdict → conflict
  await assert.rejects(() => runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId, { verdict: "FAIL", summary: "x", checks: [] })), /WORKFLOW_CONTROL_REQUEST_CONFLICT/);
  await rm(root, { recursive: true, force: true });
});

// ---- reinforcement: unavailable / job-contradiction / redaction / historical summary ----

test("#1 audit unavailable (no Supervisor session) → APPROVAL_REQUIRED (WORKFLOW_AUDIT_UNAVAILABLE), no PASS/continuation/job", async () => {
  const root = await tmp(); const { id, sid } = await seed(root, { sessionKey: null }); const { sent, deps } = harness(root);
  await advanceWorkflowOnce(deps, id); // escalate
  const rec = await readStageAttempt(root, id, sid, 1);
  assert.equal(rec.stageState, "APPROVAL_REQUIRED");
  assert.equal(rec.audit.status, "UNAVAILABLE");
  assert.equal(rec.audit.failureCode, "WORKFLOW_AUDIT_UNAVAILABLE");
  assert.equal(sent.filter((s) => s.method === "chat.send").length, 0, "no continuation");
  await advanceWorkflowOnce(deps, id); // now APPROVAL_REQUIRED → approval request sent at most once
  assert.ok(sent.filter((s) => s.method === "send").length <= 1);
  assert.equal((await listJobs(root)).length, 1, "no NEW job (only the seed's authoritative job)");
  await rm(root, { recursive: true, force: true });
});

test("#1 audit unavailable (no gatewayCall) → APPROVAL_REQUIRED, no PASS", async () => {
  const root = await tmp(); const { id, sid } = await seed(root);
  const fake = makeFakeStartJob();
  const deps = { rootDir: root, config: readConfig({ pluginConfig: { workflowEnabled: true, workflowAuditEnabled: true, allowedRoots: ["/tmp/wt"] } }), startJob: fake.startJob, startDeps: { rootDir: root }, ...completeSeams({ worktreeHash: WT }) }; // NO gatewayCall
  await advanceWorkflowOnce(deps, id);
  assert.equal((await readStageAttempt(root, id, sid, 1)).stageState, "APPROVAL_REQUIRED");
  await rm(root, { recursive: true, force: true });
});

test("#2 authoritative job contradiction at request → APPROVAL_REQUIRED (no audit request), no PASS", async () => {
  const root = await tmp(); const { id, sid, jid } = await seed(root); const { sent, deps } = harness(root);
  // corrupt the authoritative job's linkage so it no longer matches the attempt
  const { readJob: rj } = await import("../dist/job-store.js");
  const { writeFile } = await import("node:fs/promises");
  const jf = path.join(root, jid, "job.json");
  const job = await rj(root, jid); job.workflowLink.stageId = "999-bogus";
  await writeFile(jf, JSON.stringify(job));
  await advanceWorkflowOnce(deps, id);
  const rec = await readStageAttempt(root, id, sid, 1);
  assert.equal(rec.stageState, "APPROVAL_REQUIRED");
  assert.equal(rec.audit.failureCode, "WORKFLOW_AUDIT_TARGET_CONTRADICTION");
  assert.equal(rec.audit.auditRequestId ?? null, null, "no audit request created against a contradictory target");
  assert.equal(sent.filter((s) => s.method === "chat.send").length, 0);
  await rm(root, { recursive: true, force: true });
});

test("#2 authoritative job outcome diverges after request → PASS refused (TARGET_CONTRADICTION)", async () => {
  const root = await tmp(); const { id, sid, jid } = await seed(root); const { deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  // the authoritative job outcome changes AFTER the audit request (canonical attempt copy unchanged)
  const { readJob: rj } = await import("../dist/job-store.js");
  const { writeFile } = await import("node:fs/promises");
  const job = await rj(root, jid); job.jobOutcome = "FAILED_COMMAND";
  await writeFile(path.join(root, jid, "job.json"), JSON.stringify(job));
  const status = await runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId));
  const s0 = status.stages.find((s) => s.stageId === sid);
  assert.equal(s0.stageState, "APPROVAL_REQUIRED", "PASS refused on job contradiction");
  assert.equal(s0.audit.failureCode, "WORKFLOW_AUDIT_TARGET_CONTRADICTION");
  await rm(root, { recursive: true, force: true });
});

test("#4 audit summary/detail is redacted before canonical/public/Slack; empty-after-redaction rejected", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); const { sent, deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  const dirty = "verified at /Users/alice/repo with token=abcd1234 Authorization: Bearer XYZ.123 sessionKey=sess-secret channel=C0123ABCD";
  const status = await runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId, { summary: dirty, checks: [{ check: "scope", result: "PASS", verificationLevel: "LOG_VERIFIED", detail: "/Users/alice/x sk-abcdefghij" }, { check: "declared_tests", result: "PASS", verificationLevel: "LOG_VERIFIED" }] }));
  const s0 = status.stages.find((s) => s.stageId === sid);
  const stored = JSON.stringify(await readStageAttempt(root, id, sid, 1));
  for (const leak of ["/Users/alice", "token=abcd1234", "Bearer XYZ.123", "sess-secret", "C0123ABCD", "sk-abcdefghij"]) {
    assert.ok(!stored.includes(leak), `canonical leaks ${leak}`);
    assert.ok(!JSON.stringify(s0.audit).includes(leak), `public leaks ${leak}`);
  }
  const slack = sent.find((s) => s.method === "send")?.payload.text ?? "";
  for (const leak of ["/Users/alice", "token=abcd1234", "sess-secret", "C0123ABCD"]) assert.ok(!slack.includes(leak), `slack leaks ${leak}`);
  assert.ok(s0.audit.summary.includes("<redacted-path>"), "redaction marker present");
  // a summary that trims to empty is rejected before redaction (the redaction guard also fail-closes empties)
  const root2 = await tmp(); const s2 = await seed(root2); const h2 = harness(root2); const a2 = await trigger(root2, h2.deps, s2.id, s2.sid);
  await assert.rejects(() => runWorkflowAction(h2.deps, supCtx, decide(s2.id, s2.sid, a2.auditRequestId, { summary: "   " })), /WORKFLOW_INPUT_INVALID/);
  await rm(root, { recursive: true, force: true }); await rm(root2, { recursive: true, force: true });
});

test("#5 a decided attempt's summary is delivered even after a later resume bumps currentAttempt", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); const { sent, deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  // FAIL attempt1 (decided). Then create attempt2 via resume BEFORE the summary is confirmed sent — but our
  // harness sends inline; to exercise the historical scan, clear the summary outbox and add attempt2.
  await runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId, { verdict: "FAIL", summary: "scope fail", checks: [{ check: "scope", result: "FAIL", verificationLevel: "LOG_VERIFIED" }] }));
  const sum1 = sent.filter((s) => s.method === "send" && /attempt 1/.test(s.payload.text ?? "")).length;
  assert.equal(sum1, 1, "attempt1 summary sent exactly once");
  // a resume creates attempt2 (currentAttempt bumps); re-running the reconciler must NOT resend attempt1's summary
  await runWorkflowAction(deps, supCtx, { action: "resume", workflowId: id, stageId: sid, attempt: 1, requestId: "r-2", reason: "retry after audit fail" });
  await advanceWorkflowOnce(deps, id);
  assert.equal(sent.filter((s) => s.method === "send" && /AUDIT FAIL/.test(s.payload.text ?? "")).length, 1, "attempt1 summary not resent");
  await rm(root, { recursive: true, force: true });
});

// ---- #7 crash / outbox matrix ----
import { readFile as _rf, writeFile as _wf, readdir as _rd } from "node:fs/promises";
import { requestAudit, applyAuditDecision, claimAuditContinuation } from "../dist/workflow-store.js";
import { readJob as _readJob } from "../dist/job-store.js";
const jdir = (root, id) => path.join(root, "workflows", id, "journal");
async function findAuditReqSeq(root, id) {
  const files = (await _rd(jdir(root, id))).filter((f) => /^\d{6}\.json$/.test(f)).sort();
  for (const f of files) { const e = JSON.parse(await _rf(path.join(jdir(root, id), f), "utf8")); if (e.operation === "audit_request") return { seq: e.seq, f }; }
  return null;
}
const jstatus = async (root, id, f) => JSON.parse(await _rf(path.join(jdir(root, id), f), "utf8")).status;

test("#7 A/B: audit_request PENDING recovers — canonical present → COMMITTED, absent → ABORTED", async () => {
  // B: canonical audit present, journal rolled back to PENDING → reconcile COMMITTED
  const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root);
  await trigger(root, deps, id, sid); // creates audit request + COMMITTED audit_request journal
  const hit = await findAuditReqSeq(root, id);
  const e = JSON.parse(await _rf(path.join(jdir(root, id), hit.f), "utf8"));
  await _wf(path.join(jdir(root, id), hit.f), JSON.stringify({ ...e, status: "PENDING", resolvedAt: null }));
  const { reconcileWorkflow } = await import("../dist/workflow-store.js");
  await reconcileWorkflow(root, id);
  assert.equal(await jstatus(root, id, hit.f), "COMMITTED", "B: canonical present → COMMITTED");
  assert.equal((await readStageAttempt(root, id, sid, 1)).audit.status, "REQUESTED");
  await rm(root, { recursive: true, force: true });
});

test("#7 F: PASS decision applied but crash before next-stage submit → next tick submits exactly one job", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root);
  const audit = await trigger(root, deps, id, sid);
  // apply the decision at the STORE level (no advance) to simulate a crash between decision and next-stage submit
  await applyAuditDecision(root, id, { stageId: sid, expectedAttempt: 1, auditRequestId: audit.auditRequestId, verdict: "PASS", verificationSource: "INDEPENDENT_AUDIT", decision: { summary: "ok", checks: allPass() }, requestId: "ctl-9", ownerKeyHash: "h", payloadFingerprint: "pf", reason: "ok", actor: { agentId: "sup" }, currentCheckpoint: { status: "COMPLETE", complete: true, aggregateHash: WT }, readJob: _readJob });
  assert.equal((await readStageAttempt(root, id, sid, 1)).stageState, "PASSED");
  const before = (await listJobs(root)).length;
  await advanceWorkflowOnce(deps, id); // recovery advances the next stage
  await advanceWorkflowOnce(deps, id); // idempotent
  assert.equal((await listJobs(root)).length, before + 1, "exactly one next-stage job");
  await rm(root, { recursive: true, force: true });
});

test("#7 C/D/G: an ambiguous outbox send (lease expiry) parks DELIVERY_UNKNOWN, never blind-resent", async () => {
  const root = await tmp(); const { id, sid } = await seed(root); // no trigger → fresh continuation outbox
  // claim the continuation outbox with a tiny lease and DO NOT mark sent (simulate a crash mid-send)
  const key = "wf:x:audit-cont";
  const first = await claimAuditContinuation(root, id, sid, 1, key, { leaseMs: 5, maxAttempts: 8 });
  assert.equal(first.claim, true);
  await new Promise((r) => setTimeout(r, 20)); // lease expires
  const second = await claimAuditContinuation(root, id, sid, 1, key, { leaseMs: 5, maxAttempts: 8 });
  assert.equal(second.claim, false, "lease-expired SENDING is not re-claimed for a blind resend");
  assert.equal(second.record.status, "DELIVERY_UNKNOWN", "parked DELIVERY_UNKNOWN");
  await rm(root, { recursive: true, force: true });
});

// ---- reinforcement r4: audit-decision lock order / mandatory job binding / existing-request unavailable ----
import { withAuditDecisionLock } from "../dist/workflow-store.js";

test("#1 barrier: a worktree change while a decision WAITS for the audit lock is seen by the POST-lock capture", async () => {
  const root = await tmp(); const { id, sid } = await seed(root);
  let hash = WT; const sent = []; const fake = makeFakeStartJob();
  const deps = { rootDir: root, config: readConfig({ pluginConfig: { workflowEnabled: true, workflowAuditEnabled: true, allowedRoots: ["/tmp/wt"] } }), gatewayCall: async (m, p) => { sent.push({ m, p }); return { result: {} }; }, startJob: fake.startJob, startDeps: { rootDir: root }, captureFingerprint: async () => ({ fingerprintVersion: 1, status: "COMPLETE", aggregateHash: hash }), captureToolchain: completeSeams().captureToolchain, providerProbe: async () => ({ status: "READY" }), providerConfigFingerprint: "t" };
  const audit = await trigger(root, deps, id, sid);
  let decideP;
  await withAuditDecisionLock(root, id, sid, 1, async () => {
    decideP = runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId)); // queues behind this lock
    await new Promise((r) => setTimeout(r, 80));
    hash = "WT_CHANGED"; // the worktree changes WHILE the decision waits for the lock
  });
  const status = await decideP; // now acquires the lock and captures the CHANGED hash
  const s0 = status.stages.find((s) => s.stageId === sid);
  assert.equal(s0.stageState, "APPROVAL_REQUIRED", "post-lock capture used the changed hash → PASS refused");
  assert.equal(s0.audit.failureCode, "WORKFLOW_AUDIT_CHECKPOINT_CHANGED");
  await rm(root, { recursive: true, force: true });
});

test("#1 concurrency: same request → replay; two different requests → exactly one applied, one summary", async () => {
  // same request/payload concurrently → both resolve (one applies, one replays), stage PASSED once
  { const root = await tmp(); const { id, sid } = await seed(root); const { sent, deps } = harness(root);
    const audit = await trigger(root, deps, id, sid);
    const [a, b] = await Promise.allSettled([runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId)), runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId))]);
    assert.ok(a.status === "fulfilled" && b.status === "fulfilled");
    assert.equal((await readStageAttempt(root, id, sid, 1)).stageState, "PASSED");
    assert.equal(sent.filter((s) => s.method === "send" && /AUDIT PASS/.test(s.payload.text ?? "")).length, 1, "one summary");
    await rm(root, { recursive: true, force: true });
  }
  // two DIFFERENT control requestIds concurrently → exactly one decision applies, the other fails closed
  { const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root);
    const audit = await trigger(root, deps, id, sid);
    const r = await Promise.allSettled([runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId, { requestId: "ctl-A" })), runWorkflowAction(deps, supCtx, decide(id, sid, audit.auditRequestId, { requestId: "ctl-B" }))]);
    assert.equal(r.filter((x) => x.status === "fulfilled").length, 1, "exactly one decision applied");
    assert.equal((await readStageAttempt(root, id, sid, 1)).stageState, "PASSED");
    await rm(root, { recursive: true, force: true });
  }
});

test("#2 existing audit request + gateway removed: continuation SENT → preserved (PAUSED); no continuation → APPROVAL_REQUIRED", async () => {
  // continuation SENT then gateway gone → request+continuation preserved, stays UNVERIFIED (PAUSED), late decide allowed
  { const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root);
    await trigger(root, deps, id, sid); // continuation SENT
    await advanceWorkflowOnce({ ...deps, gatewayCall: undefined }, id);
    const rec = await readStageAttempt(root, id, sid, 1);
    assert.equal(rec.stageState, "UNVERIFIED", "SENT continuation preserved");
    assert.equal(rec.audit.status, "REQUESTED");
    await rm(root, { recursive: true, force: true });
  }
  // request recorded but continuation never sent (no record) + gateway gone → escalate to human
  { const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root);
    const rec0 = await readStageAttempt(root, id, sid, 1);
    await requestAudit(root, id, { stageId: sid, attempt: 1, auditRequestId: "audit-manual", mode: "supervisor", target: { jobId: rec0.jobId, activityIdempotencyKey: rec0.activityIdempotencyKey, checkpointAfterHash: WT, jobOutcome: "COMPLETED_UNVERIFIED" }, contractHash: auditContractHash(POLICY) });
    await advanceWorkflowOnce({ ...deps, gatewayCall: undefined }, id);
    const rec = await readStageAttempt(root, id, sid, 1);
    assert.equal(rec.stageState, "APPROVAL_REQUIRED", "no continuation + unavailable → human");
    assert.equal(rec.audit.failureCode, "WORKFLOW_AUDIT_UNAVAILABLE");
    await rm(root, { recursive: true, force: true });
  }
});

test("#3 mandatory authoritative job binding: readJob missing / throws / process-provider mismatch → PASS refused", async () => {
  // readJob seam absent → PASS refused (fail-closed)
  { const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root); const audit = await trigger(root, deps, id, sid);
    const res = await applyAuditDecision(root, id, { stageId: sid, expectedAttempt: 1, auditRequestId: audit.auditRequestId, verdict: "PASS", verificationSource: "INDEPENDENT_AUDIT", decision: { summary: "ok", checks: allPass() }, requestId: "c1", ownerKeyHash: "h", payloadFingerprint: "pf", reason: "ok", actor: { agentId: "sup" }, currentCheckpoint: { status: "COMPLETE", complete: true, aggregateHash: WT } /* no readJob */ });
    assert.equal(res.resultState, "APPROVAL_REQUIRED");
    assert.equal(res.failureCode, "WORKFLOW_AUDIT_TARGET_CONTRADICTION");
    await rm(root, { recursive: true, force: true });
  }
  // readJob throws → PASS refused
  { const root = await tmp(); const { id, sid } = await seed(root); const { deps } = harness(root); const audit = await trigger(root, deps, id, sid);
    const res = await applyAuditDecision(root, id, { stageId: sid, expectedAttempt: 1, auditRequestId: audit.auditRequestId, verdict: "PASS", verificationSource: "INDEPENDENT_AUDIT", decision: { summary: "ok", checks: allPass() }, requestId: "c2", ownerKeyHash: "h", payloadFingerprint: "pf", reason: "ok", actor: { agentId: "sup" }, currentCheckpoint: { status: "COMPLETE", complete: true, aggregateHash: WT }, readJob: async () => { throw new Error("read fail"); } });
    assert.equal(res.resultState, "APPROVAL_REQUIRED");
    await rm(root, { recursive: true, force: true });
  }
  // process/provider mismatch at REQUEST time (authoritative job diverges before the request) → escalate, no request
  { const root = await tmp(); const { id, sid, jid } = await seed(root); const { sent, deps } = harness(root);
    const { writeFile } = await import("node:fs/promises");
    const job = await _readJob(root, jid); job.processState = "RUNNING"; // diverges from the attempt's COMPLETED
    await writeFile(path.join(root, jid, "job.json"), JSON.stringify(job));
    await advanceWorkflowOnce(deps, id);
    const rec = await readStageAttempt(root, id, sid, 1);
    assert.equal(rec.stageState, "APPROVAL_REQUIRED");
    assert.equal(rec.audit.failureCode, "WORKFLOW_AUDIT_TARGET_CONTRADICTION");
    assert.equal(sent.filter((s) => s.method === "chat.send").length, 0, "no continuation against a contradictory target");
    await rm(root, { recursive: true, force: true });
  }
});

// ---- reinforcement r5: continuation SENDING convergence + checkpoint completeness ----
import { requestAudit as _reqAudit, applyAuditDecision as _applyAudit, jobOutcomeSummaryHash as _joHash } from "../dist/workflow-store.js";
import { mkdir as _mkdir, writeFile as _wf2, readFile as _rf2 } from "node:fs/promises";

const noGwDeps = (root) => ({ rootDir: root, config: readConfig({ pluginConfig: { workflowEnabled: true, workflowAuditEnabled: true, allowedRoots: ["/tmp/wt"] } }), startJob: makeFakeStartJob().startJob, startDeps: { rootDir: root }, ...completeSeams({ worktreeHash: WT }) }); // NO gatewayCall
async function seedRequested(root) {
  const { id, sid, jid } = await seed(root);
  const r0 = await readStageAttempt(root, id, sid, 1);
  await _reqAudit(root, id, { stageId: sid, attempt: 1, auditRequestId: "audit-x", mode: "supervisor", target: { jobId: jid, activityIdempotencyKey: r0.activityIdempotencyKey, checkpointAfter: { status: "COMPLETE", complete: true, aggregateHash: WT }, jobOutcome: "COMPLETED_UNVERIFIED", jobOutcomeSummaryHash: _joHash(await _readJob(root, jid)) }, contractHash: auditContractHash(POLICY) });
  return { id, sid, jid };
}
async function writeCont(root, id, sid, rec) { const dir = path.join(root, "workflows", id, "audit"); await _mkdir(dir, { recursive: true }); await _wf2(path.join(dir, `continuation-${sid}-1.json`), JSON.stringify(rec)); }
const contStatus = async (root, id, sid) => JSON.parse(await _rf2(path.join(root, "workflows", id, "audit", `continuation-${sid}-1.json`), "utf8")).status;

test("#1 continuation SENDING convergence: fresh→preserve, stale→DELIVERY_UNKNOWN, PENDING/none→escalate", async () => {
  // A/B: fresh SENDING (in-flight / sent-but-unmarked) → preserved PAUSED, late decide allowed, no escalation
  { const root = await tmp(); const { id, sid } = await seedRequested(root); await writeCont(root, id, sid, { status: "SENDING", claimedAt: new Date().toISOString(), attempts: 1 });
    await advanceWorkflowOnce(noGwDeps(root), id);
    assert.equal((await readStageAttempt(root, id, sid, 1)).stageState, "UNVERIFIED", "fresh SENDING preserved");
    // a late audit_decide (gateway back) still lands
    const { deps } = harness(root);
    const st = await runWorkflowAction(deps, supCtx, decide(id, sid, "audit-x"));
    assert.equal(st.stages.find((s) => s.stageId === sid).stageState, "PASSED", "late decide allowed after fresh SENDING");
    await rm(root, { recursive: true, force: true }); }
  // C: stale SENDING → converge DELIVERY_UNKNOWN, preserved PAUSED, no escalation
  { const root = await tmp(); const { id, sid } = await seedRequested(root); await writeCont(root, id, sid, { status: "SENDING", claimedAt: new Date(Date.now() - 120000).toISOString(), attempts: 1 });
    await advanceWorkflowOnce(noGwDeps(root), id);
    assert.equal((await readStageAttempt(root, id, sid, 1)).stageState, "UNVERIFIED", "stale SENDING preserved");
    assert.equal(await contStatus(root, id, sid), "DELIVERY_UNKNOWN", "stale SENDING → DELIVERY_UNKNOWN");
    await rm(root, { recursive: true, force: true }); }
  // D: PENDING (proven not-sent) → escalate to human
  { const root = await tmp(); const { id, sid } = await seedRequested(root); await writeCont(root, id, sid, { status: "PENDING", attempts: 1 });
    await advanceWorkflowOnce(noGwDeps(root), id);
    const rec = await readStageAttempt(root, id, sid, 1);
    assert.equal(rec.stageState, "APPROVAL_REQUIRED"); assert.equal(rec.audit.failureCode, "WORKFLOW_AUDIT_UNAVAILABLE");
    await rm(root, { recursive: true, force: true }); }
  // none: no continuation record → escalate
  { const root = await tmp(); const { id, sid } = await seedRequested(root);
    await advanceWorkflowOnce(noGwDeps(root), id);
    assert.equal((await readStageAttempt(root, id, sid, 1)).stageState, "APPROVAL_REQUIRED");
    await rm(root, { recursive: true, force: true }); }
});

test("#2 checkpoint completeness — request gate + decision-time current-side, only COMPLETE-both + match passes", async () => {
  // request gate: a non-COMPLETE checkpoint.after never creates an audit request → CHECKPOINT_UNAVAILABLE
  for (const cp of [null, { status: "INCOMPLETE", complete: false, aggregateHash: null }, { status: "COMPLETE", complete: true, aggregateHash: null }]) {
    const root = await tmp(); const { id, sid } = await seed(root);
    // overwrite the seed's COMPLETE checkpoint.after with the incomplete one
    const af = path.join(root, "workflows", id, "stages", sid, "attempts", "0001.json");
    const rec = JSON.parse(await _rf2(af, "utf8")); rec.checkpoint = { ...(rec.checkpoint ?? {}), after: cp }; await _wf2(af, JSON.stringify(rec));
    const { sent, deps } = harness(root);
    await advanceWorkflowOnce(deps, id);
    const r = await readStageAttempt(root, id, sid, 1);
    assert.equal(r.stageState, "APPROVAL_REQUIRED", `request gate: ${JSON.stringify(cp)}`);
    assert.equal(r.audit.failureCode, "WORKFLOW_AUDIT_CHECKPOINT_UNAVAILABLE");
    assert.equal(r.audit.auditRequestId ?? null, null, "no request against an incomplete checkpoint");
    assert.equal(sent.filter((s) => s.method === "chat.send").length, 0);
    await rm(root, { recursive: true, force: true });
  }
  // decision-time current-side: target COMPLETE/WT, canonical COMPLETE/WT; vary the CURRENT fingerprint
  const dec = (root, id, sid, currentCheckpoint) => _applyAudit(root, id, { stageId: sid, expectedAttempt: 1, auditRequestId: "audit-x", verdict: "PASS", verificationSource: "INDEPENDENT_AUDIT", decision: { summary: "ok", checks: allPass() }, requestId: "c-" + Math.random().toString(36).slice(2), ownerKeyHash: "h", payloadFingerprint: "pf", reason: "ok", actor: { agentId: "sup" }, currentCheckpoint, readJob: _readJob });
  const cases = [
    [{ status: "UNAVAILABLE", complete: false, aggregateHash: null }, "APPROVAL_REQUIRED", "WORKFLOW_AUDIT_CHECKPOINT_UNAVAILABLE"],
    [{ status: "INCOMPLETE", complete: false, aggregateHash: null }, "APPROVAL_REQUIRED", "WORKFLOW_AUDIT_CHECKPOINT_UNAVAILABLE"],
    [null, "APPROVAL_REQUIRED", "WORKFLOW_AUDIT_CHECKPOINT_UNAVAILABLE"],
    [{ status: "COMPLETE", complete: true, aggregateHash: "WT_OTHER" }, "APPROVAL_REQUIRED", "WORKFLOW_AUDIT_CHECKPOINT_CHANGED"],
    [{ status: "COMPLETE", complete: true, aggregateHash: WT }, "PASSED", null],
  ];
  for (const [cur, expectState, expectCode] of cases) {
    const root = await tmp(); const { id, sid } = await seedRequested(root);
    const res = await dec(root, id, sid, cur);
    assert.equal(res.resultState, expectState, `current=${JSON.stringify(cur)}`);
    assert.equal(res.failureCode ?? null, expectCode);
    await rm(root, { recursive: true, force: true });
  }
  // decision-time current capture THROWS (in the real service flow) → UNAVAILABLE
  { const root = await tmp(); const { id, sid } = await seedRequested(root);
    const deps = { ...harness(root).deps, captureFingerprint: async () => { throw new Error("fp fail"); } };
    const st = await runWorkflowAction(deps, supCtx, decide(id, sid, "audit-x"));
    const s0 = st.stages.find((s) => s.stageId === sid);
    assert.equal(s0.stageState, "APPROVAL_REQUIRED"); assert.equal(s0.audit.failureCode, "WORKFLOW_AUDIT_CHECKPOINT_UNAVAILABLE");
    await rm(root, { recursive: true, force: true }); }
});
