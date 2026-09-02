# P3-H — Session-Bound Audit Continuation Smoke (Acceptance Contract)

> **Status: PARTIALLY EXECUTED — host blocker resolved; acceptance NOT 10/10.** This document's §1–§8
> acceptance contract is **unchanged** (still the target). As of 2026-08-08 the original §6 host blocker
> is **resolved** on a patched `2026.7.2-beta.7` cell and a **subset** of the criteria is live-verified,
> recorded separately in **§9**. The audit-continuation round-trip — original **H3, H4, and the
> duplicate-continuation part of H8** — remains **NOT LIVE-VERIFIED / DEFERRED** (non-blocking). Do NOT
> mark P3-H complete without an actual run producing the §7 evidence for those deferred criteria.
> See [`STATUS_workflow_harness.md`](STATUS_workflow_harness.md) §5.

---

## 1. Purpose

Verify the **full round-trip** in which a real **persistent external ACP Supervisor** starts a workflow,
and — after the Worker terminates — is **automatically resumed under the same Supervisor identity** to
perform an **audit decision**.

The point of P3-H is not that the code paths exist (they are covered deterministically in-process); it is
that the **real external-ACP host** binds and preserves the Supervisor's trusted identity end-to-end, so
the audit gate authorizes the genuine Supervisor and rejects everyone else.

---

## 2. Required flow

```
Slack (or a controlled Supervisor ACP session A)
  → workflow.start
  → Worker stage/job execution
  → terminal event
  → same Supervisor A continuation (auto-resume)
  → audit context confirmed
  → audit_decide
  → workflow terminal settlement
```

---

## 3. Trust requirements

- Parent `agentId` / `sessionKey` MUST be **host-derived**.
- The **model or a tool argument MUST NOT assert identity** — identity comes only from the host.
- **Missing** trusted context MUST **fail closed** (`WORKFLOW_AUDIT_UNAVAILABLE`), never default-open.
- A **malformed** session key MUST be **rejected**.
- `audit_decide` from a **different session** MUST be **rejected**.
- `audit_decide` from a **different agent** MUST be **rejected**.
- After **session rotation**, the **previous identity MUST NOT be reusable**.
- **Duplicate** terminal / reconcile events MUST NOT create a duplicate continuation (idempotent).

---

## 4. Completion criteria

| ID | Criterion |
|---|---|
| **H1** | `workflow.start` succeeds from Session A. |
| **H2** | The workflow parent freezes A's `agentId` / `sessionKey` (host-derived). |
| **H3** | After Worker completion, A is **automatically resumed** (continuation fires). |
| **H4** | A's `audit_decide` **succeeds**. |
| **H5** | Session B's `audit_decide` is **rejected**. |
| **H6** | A different Agent B's `audit_decide` is **rejected**. |
| **H7** | Missing / malformed trusted context is **rejected** (fail-closed). |
| **H8** | Duplicate continuation attempts are **idempotent** (no double continuation / double audit). |
| **H9** | After a **Gateway or reconciler restart**, state is **recovered** correctly. |
| **H10** | Final workflow / job / audit state and the **Slack delivery agree** (no divergence). |

All ten must pass in a real run for P3-H to be considered complete.

---

## 5. Staged live smoke

> Precondition for every step: run in the controlled lab host only; the OpenClaw host trusted-context
> blocker (§6) must be resolved first, otherwise Step 1 cannot even reach H2/H3.

### Step 1 — same-Supervisor continuation only
- Config: `workflowEnabled=true`, `workflowAuditEnabled=false`.
- Verify: **H1, H2, H3** — same Supervisor A resumes after the terminal event.
- **Rollback condition:** if the parent does not freeze A's host-derived identity (H2 fails), or A does
  not auto-resume (H3 fails), **set `workflowEnabled=false`** and return to quarantine; classify per §6.

### Step 2 — same-Supervisor audit decision
- Config: `workflowAuditEnabled=true` (keep `workflowEnabled=true`).
- Verify: **H4** — the same Supervisor A's `audit_decide` succeeds and settles the stage via
  `INDEPENDENT_AUDIT` (or routes to `MANUAL_APPROVAL`).
- **Rollback condition:** if audit fails closed for the *legitimate* A (H4 fails) or any secret / raw
  session key appears in Slack or logs, **set `workflowAuditEnabled=false`**; classify per §6.

### Step 3 — cross-session / cross-agent negative tests
- Config: unchanged from Step 2.
- Verify: **H5, H6, H7** — Session B rejected, Agent B rejected, missing/malformed rejected.
- **Rollback condition:** if **any** negative case is **accepted** (a non-owner audit or an
  identity-asserting argument succeeds), **immediately disable both flags** and return to full
  quarantine; treat as a trust-boundary regression and stop — do not proceed to Step 4.

### Step 4 — restart / reconciliation test
- Action: restart the Gateway / reconciler mid-flight and after settlement.
- Verify: **H8, H9, H10** — idempotent continuation, state recovery, and workflow/job/audit ⇄ Slack
  agreement.
- **Rollback condition:** if a restart produces a duplicate continuation / duplicate audit (H8), loses
  or diverges state (H9/H10), **set `workflowEnabled=false`**, capture the journal, and classify per §6.

> After any rollback the invariant is the quarantine baseline: `workflowEnabled=false`,
> `workflowAuditEnabled=false`, `workflow` tool absent from allowlists, owner delivery routes preserved.

---

## 6. Blocker classification

Use the failing symptom to route the fix to the correct layer:

| Symptom | Classification | Required fix |
|---|---|---|
| No trusted context reaches the plugin tool at all | **OpenClaw host binding absent** | **OpenClaw patch required** |
| Trusted context is delivered but `durable-jobs` does not store it | durable-jobs integration gap | **durable-jobs integration patch required** |
| Parent binding is stored but continuation does not fire | continuation gap | **P3-H continuation patch required** |
| Continuation fires but audit authorization fails for the legitimate Supervisor | audit-integration gap | **execution-trust / audit integration patch required** |

**Historical observation — 2026-08-06:** the first row — on installed OpenClaw `2026.7.1-2` (`0790d9f`),
the standalone `openclaw-plugin-tools` path did not propagate trusted `agentId` / `sessionKey`, so P3-H
was blocked at **OpenClaw host binding absent** → OpenClaw-side resolution required. (Upstream
discussion: openclaw/openclaw#117111.)

**Current patched-cell status — 2026-08-08:** the host-binding blocker is **RESOLVED** on a patched
OpenClaw `2026.7.2-beta.7` cell — ACPX core-root resolution (`resolveOpenClawCoreDistEntry`) + managed
trusted-workspace propagation (`OPENCLAW_TOOLS_MCP_AGENT_SESSION_KEY` + `OPENCLAW_TOOLS_MCP_WORKSPACE_DIR`)
deliver a host-derived trusted context; `resolveOwnerContext` requires `(agentId + workspaceDir)`. The
trust boundary (freeze / reject) was live-verified (§9).

**Remaining caveat:** the fix is source-reproducible and live-verified but **not cleanly upstreamed /
distributed** — a fresh install may still receive the unpatched registry
`@openclaw/acpx@2026.7.2-beta.7`. Verify where the managed session key is bound to the real managed
session before relying on it in any environment that has not applied the patch.

---

## 7. Evidence (required for a completed run)

A completed P3-H run MUST capture the following. **Never record raw session keys or any secret** — use a
**redacted fingerprint only** (e.g. a truncated hash), consistent with `redactAuditText` /
`publicAuditProjection`.

- OpenClaw commit / version.
- `durable-jobs` commit / version.
- Initiating `agentId` / `sessionKey` — **redacted fingerprint only**.
- `workflowId` / `stageId` / `jobId`.
- Parent binding (which identity fingerprint the parent froze).
- Continuation claim / idempotency result (H3, H8).
- Audit verdict (H4).
- Negative-test denial results (H5, H6, H7).
- Restart recovery result (H9).
- Final workflow state (H10) and Slack-delivery agreement.

> **Secret-handling rule:** no raw `sessionKey`, no tokens, no Authorization headers, and no other
> secret material may be written into this evidence or any P3-H artifact. Redacted fingerprints only.

---

## 8. Preserved architecture principles (from the design baseline)

P3-H must uphold, not weaken, the original architecture principles:

- Reuse the `durable_job` process runner (do not fork a second runner).
- Keep the **workflow / activity** separation.
- Keep **notification separate from continuation**.
- Keep **process / provider / stage** meanings distinct.
- **Fail-closed trust** (missing / malformed / non-owner → reject).
- **Deterministic evidence first** (verify against canonical records, not model claims).

---

## 9. 2026-08-08 live run — SEPARATE record (partial; §4 acceptance unchanged)

This section records an actual run. It does **not** modify the §4 acceptance and does **not** declare
P3-H complete. Redacted fingerprints only (per §7).

**Environment**: patched OpenClaw `2026.7.2-beta.7` cell (host blocker §6 resolved — ACPX core-root
resolution + managed trusted-workspace propagation). durable-jobs delivery-route best-effort landed.
Reference workflow `wf-65a881c8…`, Supervisor session `agent:claude:acp:cb9a5e34…`, `ownerKey`
`agent:claude|session:…cb9a5e34…`. `workflowAuditEnabled=false` throughout.

**Fully live-verified (original criterion met):**

| Original ID | Result | Note |
|---|---|---|
| H1 | ✅ live | `workflow.start` from Supervisor A succeeded |
| H2 | ✅ live | parent froze A's host-derived `agentId`/`sessionKey` via the managed bridge |
| H7 | ✅ live | missing / malformed trusted context rejected (fail-closed) |

**Underlying boundary live-verified; original audit-gate criterion DEFERRED.** The run tested the
*ownership/status-access boundary*, **not** an `audit_decide` negative test under
`workflowAuditEnabled=true` (audit was disabled the whole run):

| Underlying boundary (2026-08-08) | Status | Original criterion |
|---|---|---|
| different session → workflow ownership/status access rejected | `LIVE_VERIFIED` | **H5** (`audit_decide` reject) → NOT LIVE-VERIFIED / DEFERRED |
| different agent → workflow ownership/status access rejected | `LIVE_VERIFIED` | **H6** (`audit_decide` reject) → NOT LIVE-VERIFIED / DEFERRED |

**Partial evidence toward original H9 / H10** (the criteria also presume the full audit-continuation
round-trip, which did not run):

| Observed (2026-08-08) | Original criterion |
|---|---|
| workflow record + frozen parent identity survived Gateway restart; same Session E accessed the workflow post-restart | **H9**: record/session-bound recovery subset `LIVE_VERIFIED`; full continuation/audit recovery **DEFERRED** |
| Slack observable `BLOCKED` == authoritative `workflowState=BLOCKED` | **H10**: workflow-state ↔ Slack agreement subset `LIVE_VERIFIED`; full workflow/job/audit ↔ Slack agreement **DEFERRED** |

**NOT LIVE-VERIFIED / DEFERRED** (unchanged from §4; still required for completion):

| Original ID | Status | Reason |
|---|---|---|
| **H3** | ⛔ deferred | continuation auto-fire NOT exercised (stage transition / supervisor-resume out of scope) |
| **H4** | ⛔ deferred | `audit_decide` NOT run — `workflowAuditEnabled` was **false**; no audit round-trip |
| **H5 / H6** (`audit_decide`) | ⛔ deferred | only the ownership/status boundary was tested (above), not `audit_decide` negatives under audit=true |
| **H8 (continuation)** | ⛔ deferred | only `requestId` opt-in idempotent **start** was checked; duplicate-**continuation** idempotency NOT exercised |

> Note on numbering: an operational run log used a re-numbered H-set (H1 parent freeze, H2 same-session,
> H5/H6/H7 = ownership/reject boundaries, H8 = `requestId` idempotent start, H9 restart-record, H10
> agreement; H3/H4 deferred). That operational "PASS" refers to the **subset / underlying boundaries**
> above and **must not** be read as satisfying this document's §4 H3/H4/H5/H6/H8. The workflows in this
> run settled to `BLOCKED`/`ARTIFACT_MISSING` (noop activity), never through an audit decision.
