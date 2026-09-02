# Workflow Harness — Current Implementation Status

> **This file is the current status of record.** When it disagrees with
> [`DESIGN_workflow_harness.md`](DESIGN_workflow_harness.md) (the original design baseline), this file wins.
> Last updated: 2026-08-08.
>
> **Project close-out state:** **Backend v1 ready.** The workflow harness is usable as the durable
> backend for a future Common Platform (see [`../PLATFORM_BACKEND_CAPABILITY.md`] and §10). The
> **original P3-H acceptance is NOT 10/10 complete** — the audit-continuation round-trip (original
> H3/H4 and the duplicate-*continuation* H8) is **non-blocking deferred validation**, recorded honestly
> in §5.2 and in [`P3_H_session_bound_audit_smoke.md`](P3_H_session_bound_audit_smoke.md) §9.

---

## 1. Version & canonical commit

| Field | Value | Source of truth |
|---|---|---|
| Plugin version | **`0.6.0-dev.6`** | `package.json` (`@local/openclaw-durable-jobs`) |
| Source-audit baseline (before close-out) | **`427c0a8`** (`fix(durable-jobs): sync harness version with package`) — **NOT the final HEAD** | `git rev-parse HEAD` at audit time |
| Close-out **code** commit | **`8e44078`** `feat(durable-jobs): delivery-route best-effort defer in workflow.start` | — |
| Close-out **docs** commit(s) | **`2cbb89d`** `docs(durable-jobs): finalize P3-H status …` + a subsequent docs-accuracy finalization commit (this pass) | — |
| Branch | `master` | `git rev-parse --abbrev-ref HEAD` |
| `HARNESS_VERSION` (`dist/*`) | `0.6.0-dev.6` (matches package) | reconciled at `427c0a8` |

> Close-out sequence: **source-audit baseline `427c0a8` → code/tests `8e44078` → docs `2cbb89d` →
> docs-accuracy finalization (this pass)**. Do not read `427c0a8` as the final HEAD. Full deterministic
> suite at close-out: **389 / 389 pass, 0 fail** (`node --test`, Node 26.6.0, 2026-08-08).

---

## 2. Completed phases

- **P3-F — Execution Trust Layer** — complete, committed at `9c47409`.
- **P3-G — Supervisor Audit Gate (+ Slack audit summary seams)** — complete, committed at `9c47409`.

Earlier foundation phases (P0–P2, P3 workflow engine core) are folded into the same tree and are
covered by the deterministic suite below.

---

## 3. What P3-F and P3-G actually implement

### P3-F — Execution Trust Layer *(implemented and deterministically verified)*

- Worker-side **validated execution**: a job's success is not taken from model-declared claims; the
  worker verifies against canonical evidence with a strict verification-source priority
  (`REEXECUTED` / `LOG_VERIFIED` / `ARTIFACT_VERIFIED` are sufficient; `WORKER_REPORTED` / `INFERRED`
  are not) — see `dist/workflow-audit.js` (`SUFFICIENT_VERIFICATION_LEVELS`) and `dist/worker.js`.
- **Atomic fallback decision** and single-N+1 attempt creation (no duplicate fallback attempts);
  fallback is feature-flagged (`fallbackIntent`).
- **Journal / canonical-attempt recovery**: workflow and stage projections are rebuilt from canonical
  attempt records + journal header even when `workflow.json` / `stage.json` are deleted or corrupt;
  conflicts fail closed (`WORKFLOW_RECONCILE_CONFLICT`) rather than overwriting.
- **Preflight-result recovery**: matching hashes → COMMIT; differing → fail-closed; missing canonical
  preflight → ABORTED re-check (never "COMMIT-because-unchanged").
- Owner-context resolution with **fail-closed trust** (`dist/ownership.js`): a trusted context requires
  `ctx.sessionKey`; a context-free call never adopts a static owner `sessionKey` (it stays
  sessionKey-less and freezes the owner's fixed `deliveryRoute`), and cross-agent / cross-session
  ownership is rejected (`OWNER_UNRESOLVED`, "not authorized for this agent/session").

### P3-G — Supervisor Audit Gate *(implemented and deterministically verified)*

- A stage may reach semantic **`PASSED`** only via a trusted producer — exactly
  **`MANUAL_APPROVAL`** or **`INDEPENDENT_AUDIT`** (`dist/workflow-audit.js`,
  `auditVerdictToStageState`). No `PASSED` without a workflow contract.
- **`workflow.audit_decide`** input validation (`validateAuditDecideInput`) and decision evaluation
  (`evaluateAuditDecision`) against a **frozen contract hash** (`auditContractHash`); verdict set is
  `PASS` / `FAIL` / `BLOCKED` / `INCONCLUSIVE`.
- **Fail-closed** on missing trusted audit context → `WORKFLOW_AUDIT_UNAVAILABLE`
  (also `_TARGET_CONTRADICTION`, `_CHECKPOINT_CHANGED` for tamper / drift).
- Audit **continuation is separated from notification**: the continuation message is a machine-readable
  contract (`buildAuditContinuationMessage`) delivered to the **parent `sessionKey`** with
  `deliver:false`; the Slack summary (`buildAuditSummaryText`) is a separate human artifact.
- **Redaction** of audit text (`redactAuditText`) and a **public projection** (`publicAuditProjection`)
  so secrets / raw session keys are never surfaced.
- Idempotent audit request/summary keys (`auditRequestKey`, `auditSummaryKey`) keyed by
  `workflowId:stageId:attempt`.

---

## 4. Test & deterministic-smoke results

Measured on 2026-08-06 at `9c47409` (`node --test`, Node 26.6.0):

| Suite | Result | Notes |
|---|---|---|
| **Full deterministic suite** | **382 / 382 pass, 0 fail** | See discrepancy note below. |
| P3-G targeted (`test/workflow-audit.test.js`) | **29 / 29 pass** | Matches the known P3-G baseline. |
| Trust recovery (`test/workflow-trust-recovery.test.js`) | **10 / 10 pass** | — |

> **Discrepancy vs. prior baseline:** the task's known baseline recorded the full suite as **381/381**;
> the actual measured count at `9c47409` is **382/382**. The extra passing test is a real, currently-green
> test — not a regression. The prior "381" figure is treated as stale and this document records the
> measured **382/382**.
>
> **In-process "smokes" (P3-F 7/7, P3-G 6/6):** these were previously reported from ad-hoc real / in-process
> runs. There are **no stored smoke scripts** in this repo (no `scripts/` directory; no `*smoke*` files),
> so those counts are **not independently re-verifiable from the tree** in this pass. They are recorded here
> as *previously reported*, not as re-run in this documentation pass. Their guarantees are also exercised by
> the deterministic `test/` suite above.

---

## 5. P3-H — original acceptance NOT 10/10 (host blocker resolved; subset live-verified)

**The original P3-H acceptance contract is NOT declared complete.** The full audit-continuation
round-trip — a persistent external ACP Supervisor starts a workflow, the worker terminates, and the
**same Supervisor identity** auto-resumes to perform the audit decision — was **NOT live-verified**.
Specifically, original **H3 (continuation auto-fires)**, **H4 (`audit_decide` succeeds with
`workflowAuditEnabled=true`)**, and the duplicate-**continuation** part of **H8** remain
**NOT LIVE-VERIFIED / DEFERRED**. See
[`P3_H_session_bound_audit_smoke.md`](P3_H_session_bound_audit_smoke.md) §4 (unchanged acceptance) and
§9 (the 2026-08-08 partial run). This deferred validation is **non-blocking** for Backend v1.

### 5.1 P3-H host blocker — RESOLVED (2026-08-08)

- The original blocker (installed OpenClaw `2026.7.1-2` / `0790d9f`: the standalone
  `openclaw-plugin-tools` path did not propagate trusted `agentId` / `sessionKey` into the tool
  context, so `workflow.start` ran context-free and the P3-G gate failed closed as designed) is now
  **RESOLVED** on a patched **OpenClaw `2026.7.2-beta.7`** cell.
- Host-side fixes verified live (in OpenClaw core / `@openclaw/acpx`, **not** durable-jobs):
  ACPX core-root resolution (`resolveOpenClawCoreDistEntry` → `openclaw/dist/mcp/plugin-tools-serve.js`)
  and managed trusted-workspace propagation (`OPENCLAW_TOOLS_MCP_AGENT_SESSION_KEY` +
  `OPENCLAW_TOOLS_MCP_WORKSPACE_DIR`). The managed bridge injects the session key from the managed
  runtime; `resolveOwnerContext` requires `(agentId + workspaceDir)`.
- **Packaging caveat:** those host-side fixes build reproducibly from source but are **not yet cleanly
  upstream / reproducibly distributed** — a fresh cell still `npm install`s the unpatched registry
  `@openclaw/acpx@2026.7.2-beta.7`. Treat the host fix as verified-but-not-packaged.

### 5.2 2026-08-08 live run — SEPARATE result (subset verified)

A live run on the patched cell verified a **subset** of the P3-H trust surface (recorded here as its own
result; it does **not** rewrite the §4 acceptance). `workflowAuditEnabled=false` throughout, so **no
`audit_decide` was ever exercised live**. Reference workflow `wf-65a881c8…`, Supervisor session
`agent:claude:acp:cb9a5e34…` (redacted fingerprints; no raw keys stored per §7 secret rule).

**Fully live-verified (original criterion met):**

| Live-verified (2026-08-08) | Original criterion |
|---|---|
| `workflow.start` from Supervisor session A succeeds | **H1** |
| Parent freezes A's host-derived `agentId`/`sessionKey` via the managed bridge | **H2** |
| Missing / malformed trusted context rejected (fail-closed) | **H7** |

**Underlying boundary live-verified, but original audit-gate criterion DEFERRED** (the run tested the
*ownership boundary*, not an `audit_decide` negative test under `workflowAuditEnabled=true`):

| Underlying boundary | Status | Original criterion |
|---|---|---|
| different session → workflow ownership/status access rejected | `LIVE_VERIFIED` | **H5** (`audit_decide` reject) → **NOT LIVE-VERIFIED / DEFERRED** |
| different agent → workflow ownership/status access rejected | `LIVE_VERIFIED` | **H6** (`audit_decide` reject) → **NOT LIVE-VERIFIED / DEFERRED** |

**Partial evidence toward original H9 / H10** (record/session-bound subset only; the criteria also
presume the full audit-continuation round-trip, which did not run):

| Observed (2026-08-08) | Original criterion |
|---|---|
| workflow record + frozen parent identity survived Gateway restart; same Session E accessed the workflow post-restart | **H9**: record/session-bound recovery subset `LIVE_VERIFIED`; full continuation/audit recovery **DEFERRED** |
| Slack observable `BLOCKED` == authoritative `workflowState=BLOCKED` | **H10**: workflow-state ↔ Slack agreement subset `LIVE_VERIFIED`; full workflow/job/audit ↔ Slack agreement **DEFERRED** |

**Adjacent, not an original criterion:** `requestId` opt-in idempotent **start** (same
`(ownerKey,requestId)` → one workflow) is live-verified — this is the START path, **not** the
duplicate-**continuation** idempotency of original H8.

**NOT live-verified / DEFERRED (unchanged):** H3 (continuation auto-fires), H4 (`audit_decide` succeeds;
`workflowAuditEnabled` was **false** throughout), original H5/H6 `audit_decide` negative tests,
duplicate-**continuation** idempotency (original H8), and stage execution / transition / delivery /
supervisor-resume (explicitly out of scope). The workflows settled to `BLOCKED`/`ARTIFACT_MISSING` (noop
activity), never through an audit round-trip.

---

## 6. Live quarantine state (verified in the lab config)

Verified read-only in `~/.openclaw-lab/openclaw.json` on 2026-08-06:

| Setting | Value |
|---|---|
| `durable-jobs.config.workflowEnabled` | **`false`** |
| `durable-jobs.config.workflowAuditEnabled` | **`false`** |
| `workflow` tool in any agent allowlist | **removed / absent** |
| `durable_job` tool | allowed via global `tools.alsoAllow = ["durable_job"]` |
| Owner delivery routes | **preserved** (e.g. `claude-queue-test` → `channel_root/slack`) |

> Minor location note vs. prior notes: `durable_job` is granted at the **global** `tools.alsoAllow`
> level, and `agents.list["claude-queue-test"].tools` is currently `undefined` (no per-agent override).
> The effective policy matches the intended quarantine (workflow surface off, `durable_job` retained,
> owner routes kept); only the allowlist *location* differs from the earlier per-agent description.

The live extension remains `durable-jobs 0.6.0-dev.6` (byte-identical to the repo build). No live
rollout is performed by this documentation pass.

---

## 7. Recommended next work order

Backend v1 is closed out; the remaining P3-H work below is **non-blocking deferred validation**.

1. ~~Resolve host trusted-context propagation~~ — **DONE** on the patched `2026.7.2-beta.7` cell (§5.1);
   remaining sub-item: get those host-side ACPX/core fixes **cleanly upstream / reproducibly packaged**
   (currently source-reproducible but not distributed).
2. **P3-H Step 2 (deferred)** — enable `workflowAuditEnabled=true`; verify the same Supervisor's
   **continuation auto-fire (H3)** and **`audit_decide` (H4)** end-to-end — the part NOT yet live-run.
3. **P3-H Step 4 (deferred)** — duplicate-**continuation** idempotency (H8) + continuation/audit
   recovery across restart (beyond the record-level recovery already checked in §5.2).
4. Only after the deferred P3-H steps pass: consider lifting quarantine flags in the lab config.
5. **Common Platform (separate project)** — build the autonomous-development supervision layer *on top of*
   this backend; see §10 and [`../PLATFORM_BACKEND_CAPABILITY.md`].

Each deferred step and its rollback condition remains specified in
[`P3_H_session_bound_audit_smoke.md`](P3_H_session_bound_audit_smoke.md) (§4/§5 unchanged; §9 records the
2026-08-08 partial run).

---

## 8. Status taxonomy (what each area actually is)

**Implemented and deterministically verified**
- P3-F Execution Trust Layer (worker validated execution, atomic fallback, journal/canonical recovery,
  preflight recovery, fail-closed owner context).
- P3-G Supervisor Audit Gate (`workflow.audit_decide`, frozen contract hash, `PASSED` only via
  `MANUAL_APPROVAL` / `INDEPENDENT_AUDIT`, fail-closed, redaction, public projection).
- Workflow store, stage linkage, linear advancement, reconciler, provider cache, notification ⇄
  continuation split.

**Implemented and LIVE-verified (subset, 2026-08-08 — see §5.2)**
- Trusted parent-identity freeze from the managed bridge (H2), cross-session (H5) / cross-agent (H6) /
  missing-or-malformed (H7) rejection, `requestId` opt-in idempotent **start**, and record + parent
  identity survival across Gateway restart with state ⇄ Slack agreement (H9/H10 record-level).

**Implemented but NOT live-verified (deferred, non-blocking)**
- The audit-continuation round-trip: continuation auto-fire (H3), `audit_decide` under
  `workflowAuditEnabled=true` (H4), and duplicate-**continuation** idempotency (H8). Seams pass
  in-process; the live end-to-end round-trip was NOT run (audit disabled; stage transition out of scope).
- In-process smoke counts (P3-F 7/7, P3-G 6/6) — previously reported, no stored scripts to re-run here.

**Designed only**
- P4 items in the design doc (input-integrity/assurance-level extensions, verification-profile,
  Slack slimming, doc-drift) beyond what P3-F/P3-G already deliver.

**Host blocker — RESOLVED, with a packaging caveat**
- The original P3-H host blocker (standalone `openclaw-plugin-tools` not propagating trusted identity on
  `2026.7.1-2`/`0790d9f`) is resolved on a patched `2026.7.2-beta.7` cell (§5.1). Remaining caveat: the
  host-side ACPX/core fixes are source-reproducible but **not yet cleanly upstream / packaged**.

**Future application / profile work (NOT durable-jobs core)**
- Autonomous-development application semantics and infra-scanner profile: `READY_ITEM`,
  `THIN_FOUNDATION`, `MAJOR_FOUNDATION`, `CONTRACT_CHANGE`, infra-scanner task selection,
  `PROJECT_STATUS` updates, automatic ff-only merge, multi-task batch scheduler.

---

## 9. Boundary — durable-jobs core vs. future autonomous-development workflow

`durable-jobs` is a **generic execution and workflow platform**: detached process execution, durable
state/checkpointing, stage advancement, trusted supervisor audit, deterministic evidence, and Slack
delivery of terminal / audit events. It is **domain-agnostic**.

The following are **application/profile semantics that live above the platform** and must **not** be
represented as durable-jobs core responsibilities or as "core implemented":

- `READY_ITEM`, `THIN_FOUNDATION`, `MAJOR_FOUNDATION`, `CONTRACT_CHANGE`
- infra-scanner task selection
- `PROJECT_STATUS` updates
- automatic ff-only merge
- multi-task batch scheduler

These belong to a separate autonomous-development workflow/application and the infra-scanner profile,
to be built on top of the durable-jobs platform. The host blocker is now resolved (§5.1); the remaining
audit-continuation validation (§5.2) is non-blocking for using this as a backend.

---

## 10. Close-out — Backend v1 ready

This project closes as **Backend v1 ready**, **not** as "original P3-H 10/10 complete". A read-only
feasibility audit (2026-08-08) judged the workflow harness a viable durable backend for a future
Common Platform autonomous-development supervisor (**verdict: `OPENCLAW_CONDITIONAL_GO`** — the workflow
engine does not need rebuilding; the upper orchestration + ff-only merge + Claude-actor wiring are the
bounded extensions). The backend capability contract — split into **deterministic-tested** vs
**live-tested** capabilities and the interfaces the upper platform requires — is documented in
[`../PLATFORM_BACKEND_CAPABILITY.md`].

Remaining, explicitly **non-blocking deferred validation**: the audit-continuation smoke (original
H3/H4 + duplicate-continuation H8) and cleanly packaging the host-side ACPX/core fixes.
