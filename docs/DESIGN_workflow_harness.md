# Workflow Harness — Technical Design & Phased Plan

> Status: **DESIGN ONLY** (no production code changed in this document's authoring pass).
> Scope: OpenClaw `durable-jobs` plugin at `/Users/astro3141/Lab/30_Projects/openclaw-durable-jobs`.
> Target runtime: single macOS host, personal Slack, OpenClaw 2026.7.1-2, plugin v0.2.0-dev.1.
> Non-target: modifying the global OpenClaw install / vendor dist; modifying the infra-scanner repo.

---

> ## ⚠️ IMPLEMENTATION-STATUS NOTE (added 2026-08-06 — read this first)
>
> **This document is the ORIGINAL design baseline. It is NOT the current implementation of record.**
> Several phases described below as future plans have since been implemented, and one described flow
> (P3-H real-world audit continuation) is **blocked at live-validation** by OpenClaw host behavior.
> The version banner above (`v0.2.0-dev.1`) is the design-era version and is **stale** — the current
> canonical implementation is **v0.6.0-dev.6** at commit **`9c47409`**.
>
> **For current status, always defer to [`STATUS_workflow_harness.md`](STATUS_workflow_harness.md).**
> For the P3-H acceptance contract, see [`P3_H_session_bound_audit_smoke.md`](P3_H_session_bound_audit_smoke.md).
>
> Quick map of what is real vs. design-only as of `9c47409`:
>
> | Area | Current reality |
> |---|---|
> | **P3-F Execution Trust Layer** | **Implemented and deterministically verified.** |
> | **P3-G Supervisor Audit Gate** | **Implemented and deterministically verified.** |
> | Workflow store, stage linkage, linear advancement | Implemented (deterministic tests). |
> | Supervisor audit gate (`workflow.audit_decide`, fail-closed) | Implemented (deterministic tests). |
> | **P3-H real ACP audit-continuation round-trip** | **Design + deterministic seams only; live-validation BLOCKED** by the OpenClaw host not propagating trusted `agentId`/`sessionKey` to the standalone `openclaw-plugin-tools` MCP path. |
> | Notification ⇄ continuation split, provider cache, reconciler, fallback | Implemented (deterministic tests). |
>
> Do **not** read any P-level heading below as "shipped and live-verified." The deterministic **unit
> suite** passes for P3-F/P3-G (382/382 measured 2026-08-06); the earlier in-process smoke counts
> (P3-F 7/7, P3-G 6/6) are *previously reported* and were **not re-run** in this pass — see
> `STATUS_workflow_harness.md` §4. **Live end-to-end verification with a real external ACP Supervisor is
> not done**, and is gated on an OpenClaw host trust-context fix (see STATUS + P3-H docs).
> `durable-jobs` is a **generic execution/workflow platform**; project-specific semantics (infra-scanner
> item selection, READY_ITEM / FOUNDATION / CONTRACT_CHANGE, PROJECT_STATUS, autonomous ff-only merge,
> multi-task batch scheduling) are **not** part of this core and are **not** implemented here.

---

## 1. Executive Summary

durable-jobs today reliably does one thing: it runs **a single argv command** detached from the
Supervisor's ACP turn, persists a JSON ledger + logs, and pushes **one deterministic terminal notice**
to a Slack route frozen at creation. That core (detached execution, frozen route, persistent outbox,
reconciler tick, dependency-injected seams) is sound and should be **kept and reused**, not rewritten.

The confirmed failure modes are all **above** that core: they are about *meaning* (did the work actually
succeed?), *continuation* (who resumes after the terminal event?), *role separation* (the model worker
should not babysit long shell commands), and *observability/checkpointing* (a workflow is more than one
command with one opaque `RUNNING` state).

**Recommendation:** add a **Workflow Harness layer inside the existing plugin** (Option 1, layered so it
can be extracted later), reusing `job-store.js` as the checkpoint store, the `delivery-outbox.js` state
machine as the transition pattern, and `reconcileOnce` as the continuation/heartbeat driver. Keep
`durable_job` as the unchanged low-level **process runner**; add a new `workflow.*` tool surface above it.
Do **not** adopt Temporal/Hatchet/DBOS-as-a-service for a single-host personal deployment — borrow their
*patterns* (Workflow/Activity split, completed-step checkpoint + version-safe resume, thread/checkpoint
identifiers + interrupt, state taxonomy + transition hooks) at minimal cost.

P0 delivers the **state separation and false-success fix** that everything else depends on, entirely
inside the plugin, backward-compatible with existing `job.json` rows.

---

## 2. Current Architecture (as audited)

Files (hand-written ES modules under `dist/`, ~1,853 LOC total; not minified):

| File | LOC | Responsibility |
|---|---|---|
| `dist/core.js` | 721 | Orchestration: config, route freeze, outbox driver, reconciler, `startJob`, ownership glue, seams. **plugin-SDK-free, unit-testable.** |
| `dist/index.js` | 192 | Thin plugin entry: registers the `durable-jobs-reconciler` service + `durable_job` tool. |
| `dist/worker.js` | 121 | Detached child: spawns the argv command, captures exit, writes terminal `state`. |
| `dist/job-store.js` | 216 | JSON ledger: per-job dir, atomic write, `.lock` dir, optimistic `updateJob`, cwd/exec guards, `isProcessAlive`, `signalProcessGroup`. |
| `dist/delivery-outbox.js` | 223 | Idempotent delivery state machine (PENDING→SENDING→DELIVERED/…), lease/claim/backoff. |
| `dist/completion-turn.js` | 217 | Route resolution + freeze, deterministic terminal message, optional ACP-wakeup completion turn. |
| `dist/ownership.js` | 111 | Owner selection (trusted session vs context-free cwd), authorization. |
| `dist/preflight.js` | 52 | Install-time legacy-job detection only. |

### Execution flow (start → terminal → delivery)

```text
durable_job(action=start)                      [index.js:143]
  └─ resolveOwnerContext(config, ctx, {cwd})   [index.js:150 → ownership.js:75]
  └─ startJob(deps, ownerCtx, params)          [core.js:553]
       ├─ concurrency + allowed-cwd + exec guards        [core.js:557-563 → job-store.js:139,166]
       ├─ FREEZE route once (session → chat.history,      [core.js:568-570]
       │        or owner-config channel_root)             [completion-turn.js:96 / :123]
       ├─ createFlow (TaskFlow managed) if sessionKey     [core.js:577 → :655]
       ├─ write job.json (state=QUEUED) + initialOutbox   [core.js:587-623 → delivery-outbox.js:60]
       └─ spawnWorkerProcess (detached worker.js)         [core.js:644]

worker.js run()                                 [worker.js:13]
  ├─ state=RUNNING, startedAt, workerPid         [worker.js:20-26]
  ├─ spawn(command[0], command.slice(1))         [worker.js:34]
  ├─ record childPid                             [worker.js:47-51]
  ├─ optional timeout → SIGTERM/SIGKILL group    [worker.js:54-71]
  └─ on exit: state = TIMED_OUT | SUCCEEDED(code0) | FAILED   [worker.js:80-88]  ◀── false-success origin

durable-jobs-reconciler tick (every pollIntervalMs)   [index.js:106 → core.js:524]
  for each job:
   ├─ markLostIfNeeded (QUEUED grace / RUNNING pid check)   [core.js:470]
   ├─ settleFlow (TaskFlow finish/fail)                     [core.js:351]
   ├─ processDeliveryOutbox → gateway "send" frozen route   [core.js:247 → delivery-outbox.js:100]
   └─ IF completionAcpWakeup && sessionKey:                 [core.js:539]  ◀── continuation is opt-in + session-coupled
        notifyTerminalJob → chat.send(deliver:false)        [core.js:407 → completion-turn.js:148]
```

### Reusable seams (dependency injection — the key extensibility asset)

- `startJob(deps, ctx, params)` and `reconcileOnce(deps)` take injected `gatewayCall`, `spawnWorker`,
  `createFlow`, `settleFlow`, `logger` (`core.js:553`, `:524`; stubbed in `test/integration.test.js`).
- `job-store.updateJob(rootDir, jobId, updater)` is an **optimistic compare-and-set under a dir lock**;
  returning `null` from the updater aborts (`job-store.js:101`). This is already a minimal durable-store
  primitive suitable for stage checkpoints.
- `delivery-outbox.js` is a self-contained, idempotent, lease-based state machine — the **template** for
  the stage/attempt machine.
- `api.runtime.taskFlow` (OpenClaw's managed-flow runtime) is available: `fromToolContext`, `bindSession`,
  `createManaged`, `finish`, `fail`, with `notifyPolicy:"silent"` (`core.js:353,357,368,370,657`).

---

## 3. Confirmed Failure Modes (restated, audited)

| ID | Confirmed behaviour |
|---|---|
| TERMINAL_EVENT_DELIVERED_BUT_SUPERVISOR_NOT_RESUMED | Terminal notice reaches Slack; no Supervisor continuation turn; user must re-ask. |
| DURABLE_JOB_FALSE_SUCCESS_ON_PROVIDER_FAILURE | AGY exits 0 after a quota/interrupt; job → `SUCCEEDED`; Slack says success. (Live: `job-2f5e7087` inverse case — false *failure* from argv error — proves exit-code is the only signal used.) |
| WORKER_POLLING_CONSUMES_MODEL_QUOTA | Model worker starts long test/build/docker commands then loops polling for completion, burning model quota with no code work. |
| INPUT_TRUNCATION_NOT_DETECTED | A truncated instruction ("…generic collect_config_kv_generic 또는") executed; the dropped tail held the hard constraints. |
| JOB_STAGE_OBSERVABILITY_MISSING | `RUNNING` cannot distinguish "waiting on model" / "running unittest" / "stalled" / "buffering". |
| WORKFLOW_STAGE_CHECKPOINT_MISSING | Implementation+tests+mutation+build+zip+docker+commit collapse into one job; mid-failure restarts from zero. |
| STRUCTURED_HANDOFF_METADATA_MISSING | Handoff is natural language (`nextAction` string), not structured workflow state. |
| PREFLIGHT_COVERAGE_INCOMPLETE | `preflight.js` only detects legacy jobs; no provider/quota/worktree/docker/disk checks before an expensive worker. |
| PROVIDER_AVAILABILITY_CACHE_MISSING | Blocked providers are re-probed every job. |
| MODEL_FALLBACK_RESUME_CONTEXT_MISSING | No fallback-with-checkpoint concept; a fallback model would redesign from scratch. |
| INDEPENDENT_VERIFICATION_DUPLICATES_FULL_CI | Supervisor re-runs the full CI the worker already ran. |
| VERIFICATION_ASSURANCE_LEVEL_NOT_EXPLICIT | Final report doesn't state whether each check was re-executed / log-verified / worker-reported. |
| SEMANTIC_SUCCESS_CRITERIA_MISSING | No structured "business success" predicate. |
| RISK_BASED_VERIFICATION_PROFILE_MISSING | No per-repository verification pipeline selection. |
| STATUS_REPORTING_DUPLICATED_AND_NOISY | Slack gets verbose/duplicated status; audit detail not separated into metadata. |
| NOTIFICATION_AND_CONTINUATION_CONFLATED | One path both notifies the user and (optionally) drives continuation. |
| DOCUMENTATION_SEMANTIC_DRIFT_RISK | Low-cost doc edits can overstate completion; no structured doc-vs-code check. |

---

## 4. Root Cause Mapping (problem → real code)

| Problem | Root cause in code | Concrete anchor |
|---|---|---|
| False success | Terminal `state` derived **only** from `exitCode` | `worker.js:83` `else if (result.code === 0) job.state = "SUCCEEDED"` |
| False success → Slack | Slack message echoes `job.state` verbatim | `delivery-outbox.js:83` `Durable job ${job.state}` |
| State conflation | One `state` field carries process + semantic meaning | `job-store.js:17` `TERMINAL_STATES`; job schema `core.js:587-621` |
| No continuation | Continuation is opt-in **and** requires a sessionKey | `core.js:539` `if (config.completionAcpWakeup && job.sessionKey)` |
| Notification ≠ continuation not separated | Same structure/path (`notification`) does both | `core.js:407 notifyTerminalJob`, `completion-turn.js:148 buildCompletionTurn(deliver:false)` |
| No parent workflow link above job | Job stores `sessionKey/sessionId/requesterOrigin/flowId` but no workflow/task id | `core.js:606-610`; `flowId` only when sessionKey present `core.js:577` |
| Model polls long commands | durable-jobs runs **one** command/job; nothing routes "run tests" to a *non-model* runner job | `worker.js:34` single `spawn`; no runner-type routing |
| Stage opacity | Worker writes only `RUNNING`→terminal; no stage/heartbeat fields | `worker.js:22`; `markLostIfNeeded` uses pid liveness only `core.js:470-489` |
| No checkpoint/resume | Job = one command; no stage records; retry restarts whole command | whole `startJob`/`worker.js`; `attempts` exists only on the outbox `delivery-outbox.js:168` |
| Natural-language handoff | `nextAction` is a free string | `core.js:595`, `completion-turn.js:8` |
| Weak preflight | Preflight only checks legacy jobs | `preflight.js:33` |
| No provider cache / fallback | Absent | — |
| Verification duplication / assurance / semantic / profile | Absent (all pushed into Supervisor prompt) | — |
| Input truncation | No completeness gate; only type/length JSON-schema | `index.js:33-51 parameters` |

**One-line diagnosis:** the job's single `state` field is overloaded, and the reconciler treats *process
completion* as *workflow completion*, with continuation bolted on as an optional side effect of
notification. Everything else is missing structure layered above the (good) process runner.

---

## 5. Proposed Architecture

Keep the process runner. Add a **Workflow Harness** module set inside the plugin, and a **Local Durable
Runner** role that is just `durable_job` used for **non-model** commands.

```text
OpenClaw Supervisor (ACP session)
  contract · plan · final verdict · minimal re-check
        │  workflow.* MCP tool surface
        ▼
Workflow Harness  (NEW modules, same plugin, same state dir)
  workflow/stage/attempt state · preflight · continuation trigger · policy gate · evidence
        │                                   ▲ terminal event (from reconciler)
        ├───────────────┬───────────────────┘
        ▼               ▼
Model Worker        Local Durable Runner
(durable_job on     (durable_job on a
 an AGY argv)        local test/build/docker argv — NO model)
        │               │
        └──────┬────────┘   each is an existing durable job (job.json + outbox)
               ▼
        Reviewer stage (harness-driven evidence check)
               ▼
     PASS / FAIL / BLOCKED → next stage or user approval
```

**Layering contract:**
- `durable_job` (unchanged) = **Activity executor** for a *single* argv command. It never learns
  repository semantics. It gains richer *process* + *provider* outcome fields but no workflow logic.
- Workflow Harness = **Workflow engine**: owns stages, checkpoints, continuation, semantic success,
  verification profile. It *submits* durable jobs (model or local) as its activities.
- `workflow.*` = **interface only**, not the state machine. State lives in files under the state dir.
  Two distinct surfaces, not to be conflated: **(a)** the **OpenClaw plugin tool** registered via
  `api.registerTool` (same mechanism as today's `durable_job`, `index.js:128`) — this is what P3 ships;
  **(b)** a future **standalone MCP server adapter** that re-exposes the same `workflow.*` verbs to
  external MCP clients — a separate packaging artifact, explicitly *not* in P0–P4 scope. Both call the same
  Harness core; neither *is* the state machine.

Why a layer inside the plugin (not a separate daemon): it reuses the reconciler tick, the state dir, the
gateway seam, and the atomic ledger; it ships/updates with the plugin and stays decoupled from the
OpenClaw dist; there is no second process to supervise. It is written as its own module set
(`workflow-*.js`) so a later extraction to a standalone service + MCP server is a packaging change, not a
rewrite.

---

## 6. State Model (three orthogonal axes + one derived verdict)

Separate the axes the current single `state` conflates. Store all on the job/stage records.

```text
process_state   (authoritative: worker.js, from the OS child)
  QUEUED RUNNING COMPLETED FAILED_COMMAND INTERRUPTED TIMED_OUT CANCELLED LOST
    - COMPLETED  = child exited 0            (renamed from today's "SUCCEEDED" at the process layer)
    - FAILED_COMMAND = non-zero exit
    - INTERRUPTED = killed by a signal we didn't send / partial (e.g. SIGPIPE, OOM)
    - CANCELLED  = terminated by our own cancelJob (core.js:700)  ◀── was missing; distinct from LOST/INTERRUPTED
    - TIMED_OUT / LOST unchanged in meaning

provider_state  (evaluator: parse the model activity's structured result — NOT full stdout; see §12)
  UNKNOWN OK ERROR_UNCLASSIFIED BLOCKED_QUOTA RATE_LIMITED AUTH_FAILED CONTEXT_LIMIT INTERNAL_ERROR TOOL_INTERRUPTED
    - For non-model (local runner) jobs: provider_state = OK by definition (no provider).
    - Classification rule (grounded in the spike-agy-json-fixtures capture; envelope has status
      SUCCESS|ERROR and only a free-text `error`, no structured error code):
        no envelope OR JSON parse failure          → UNKNOWN
        status == "SUCCESS"                         → OK
        status == "ERROR" ∧ a real captured signature matches (e.g. timeout) → that specific subtype
        status == "ERROR" ∧ not specifically classifiable → ERROR_UNCLASSIFIED
      ERROR_UNCLASSIFIED is treated as a provider failure (→ FAILED_PROVIDER). Unproven quota/auth/context
      signatures are NOT implemented until a real capture exists (they stay ERROR_UNCLASSIFIED, never guessed).

stage_state     (harness — ONLY exists inside a workflow contract; see the P0 restriction below)
  PENDING RUNNING UNVERIFIED PASSED FAILED ARTIFACT_MISSING BLOCKED_DEPENDENCY APPROVAL_REQUIRED
```

### P0 outcome restriction — no semantic `PASSED` without a workflow contract

P0 introduces **no** workflow contract (no declared artifacts / allowed paths / required commit /
verification profile). Therefore **P0 must never emit a stage `PASSED`.** The strongest positive outcome
P0 can assert for a plain durable job is `COMPLETED_UNVERIFIED`. The P0 job-outcome enum is exactly:

```text
job_outcome (P0, workflow-absent):
  FAILED_COMMAND        process_state ∈ {FAILED_COMMAND, INTERRUPTED, TIMED_OUT, LOST}
  FAILED_PROVIDER       process_state == COMPLETED ∧ provider_state ∈ {ERROR_UNCLASSIFIED, BLOCKED_QUOTA,
                                                                       RATE_LIMITED, AUTH_FAILED,
                                                                       CONTEXT_LIMIT, INTERNAL_ERROR,
                                                                       TOOL_INTERRUPTED}
  COMPLETED_UNVERIFIED  process_state == COMPLETED ∧ provider_state ∈ {OK, UNKNOWN}
  CANCELLED             process_state == CANCELLED
```

`COMPLETED_UNVERIFIED` explicitly means "the process finished and no provider failure was detected; the
*work* is not asserted correct." It is not success. Critical rule (fixes false-success):
**exit 0 alone yields at most `COMPLETED_UNVERIFIED`, never a PASS.** A provider quota exhaustion with
exit 0 yields `FAILED_PROVIDER`.

### Semantic `PASSED` — only inside a workflow (P3+)

A real stage `PASSED` is emitted **only** when a workflow contract exists to check against:

```text
stage PASSED  ⇔  process_state == COMPLETED
             ∧  provider_state == OK
             ∧  all required artifacts present            (contract-declared)
             ∧  all required commits present              (contract-declared)
             ∧  no change outside allowed paths           (contract-declared)
             ∧  no forbidden action performed             (§9 dual control)
             ∧  resume fingerprint reconciled             (§10, multi-field)
             ∧  stage verification profile satisfied      (§12)
else UNVERIFIED / FAILED / ARTIFACT_MISSING / BLOCKED_DEPENDENCY / APPROVAL_REQUIRED
```

If any contract input is absent, the stage can be at most `UNVERIFIED` — never `PASSED`. The verdict is
computed by the Workflow Harness, never by the process runner.

Backward-compat mapping for old `job.json`: `SUCCEEDED→{process:COMPLETED, provider:UNKNOWN}` →
`job_outcome = COMPLETED_UNVERIFIED`; `FAILED→FAILED_COMMAND`; `TIMED_OUT/CANCELLED/LOST` unchanged. Old
rows read cleanly (see §15).

---

## 7. Workflow / Stage / Attempt Schema

New `workflow.json` (one per workflow, sibling to job dirs under the state dir), plus per-stage records.
Job rows are unchanged except for additive fields (§6, §12).

```jsonc
// <stateDir>/durable-jobs/workflows/wf-<uuid>/workflow.json
{
  "version": 1,
  "workflow_id": "wf-...",
  "harness_version": "0.3.0",            // DBOS-style app version for version-safe resume (§10,§15)
  "created_at": "...", "updated_at": "...",
  "parent": {                            // continuation target (LangGraph thread_id analog)
    "agent_id": "infra-scanner-openclaw",
    "session_key": "agent:...:4b5d9d1b...",   // may be null (context-free)
    "session_id": "03d3a6c4-...",
    "requester_origin": { "channel": "slack", "to": "channel:C0EXAMPLE001", "...": "..." },
    "flow_id": "flow-..."                // TaskFlow linkage when present
  },
  "delivery_route": { "...": "frozen once, reused by every stage's terminal notice" },
  "repository": {
    "worktree": "/Users/astro3141/infra-scanner-openclaw",
    "branch": "openclaw/phase-5.3-b2",
    "base_commit": "61fff53...",
    "verification_profile": "collector_changed"    // §12 risk-based profile key
  },
  "forbidden_actions": ["git clean","git reset --hard","canonical_merge","push","tag_change"],
  "pipeline": ["preflight","implementation","direct_verification","full_unittest",
               "builder_and_zip","mutation","distro_smoke","independent_review",
               "commit","canonical_gate"],
  "current_stage": "full_unittest",
  "completed_stages": ["preflight","implementation","direct_verification"],
  "workflow_state": "RUNNING",           // RUNNING PAUSED BLOCKED SUCCEEDED FAILED CANCELLED
  "input_integrity": { "status": "OK", "restored_constraints": [] }   // §13
}
```

Each attempt is **three files with distinct mutability** — a single "immutable" record that is
continuously mutated (job_id, heartbeat, terminal_outcome) is an anti-pattern and is NOT used:

```text
wf-<uuid>/
  workflow.json                       ← workflow projection (regenerable)
  journal/<seq>.json                  ← committed transition journal (authoritative, append-only)
  stages/<stage>/
    stage.json                        ← stage current-state projection (regenerable)
    attempts/<n>/
      spec.json                       ← write-once IMMUTABLE (entry contract)   [authoritative]
      runtime.json                    ← MUTABLE during execution (liveness)     [projection]
      result.json                     ← write-once IMMUTABLE at terminal        [authoritative]
```

```jsonc
// attempts/<n>/spec.json   — write-once at attempt entry, never mutated
{
  "stage": "full_unittest", "attempt": 2,
  "runner_type": "local",                                 // "model" | "local"
  "invocation": { "argv": ["node","--test"], /* or */ "model_spec": {"model":"gemini-…"} },
  "activity_idempotency_key": "wf:<workflow_id>:stage:full_unittest:attempt:2",
  "resume_fingerprint": {                                 // §10 — HEAD alone is insufficient
    "head": "61fff53...", "index_fingerprint": "sha256:...", "tracked_diff_hash": "sha256:...",
    "untracked_path_manifest": ["tmp-auto-delegation-clean/summary.md"],
    "untracked_content_hash": { "tmp-auto-delegation-clean/summary.md": "sha256:..." },
    "allowed_output_manifest": ["tmp-auto-delegation-clean/**"]
  },
  "entry_contract": { "allowed_outputs": ["tmp-auto-delegation-clean/**"], "forbidden_actions": ["…"] }
}
```

```jsonc
// attempts/<n>/runtime.json   — mutable while the attempt runs (a PROJECTION; rebuildable from job.json)
{ "job_id": "job-...", "worker_pid": 96503, "child_pid": 96505,
  "heartbeat": { "last_heartbeat_at": "...", "last_output_at": "...", "child_process_state": "RUNNING" },
  "current_step": "vitest" }
```

```jsonc
// attempts/<n>/result.json   — write-once at terminal, never mutated
{ "process_state": "COMPLETED", "provider_state": "OK", "job_outcome": "COMPLETED_UNVERIFIED",
  "exit_code": 0, "started_at": "...", "finished_at": "...",
  "evidence": [ /* §12 EvidenceItem[] */ ], "terminal_error": null }
```

**Authoritative = `spec.json` + `result.json` + the committed transition journal.** `runtime.json`,
`stage.json`, and `workflow.json` are recovery/query **projections**. Retry never mutates a prior attempt;
it writes a new `attempts/<n+1>/` directory — full retry history is preserved immutably.

Identifier relationships (LangGraph-informed): `workflow_id` (thread) → `stage` (node) →
`attempt` (checkpoint). `parent.session_*` links the workflow back to the Supervisor conversation;
`runtime.job_id` links an attempt to its durable activity. Idempotency keys derive deterministically:
`wf:<workflow_id>:stage:<stage>:attempt:<n>` (activity submission, §10) and, for continuation,
`wf:<workflow_id>:continuation:<stage>`.

---

## 8. Terminal Event & Continuation Flow (separates notification from continuation)

Today `reconcileOnce` (`core.js:524`) delivers the outbox and *optionally* injects a completion turn.
Split the two responsibilities behind **two independent hooks** fired by the reconciler on a terminal job:

```text
on terminal durable job (reconciler tick):
  1. notification.emit(job)         → Slack short status via outbox (existing path, unchanged)
  2. continuation.trigger(job)      → NEW, idempotent, independent of #1
       ├─ resolve workflow from job_id → stages/<stage>.json
       ├─ evaluate stage verdict (§6 predicate) from process/provider/evidence
       ├─ PASS  → advance current_stage; if next stage local → submit local durable job;
       │           if next stage needs the model or a decision → enqueue Supervisor/Reviewer continuation
       ├─ FAIL/UNVERIFIED → attempt++ resume (same worktree/checkpoint) or mark BLOCKED
       └─ APPROVAL_REQUIRED → workflow_state=PAUSED, emit ONE Slack approval request
```

**Continuation trigger mechanism (reuse what exists, but do NOT assume sessionless works):**
`buildCompletionTurn` already injects a `deliver:false` user turn into the owning session
(`completion-turn.js:148`, dispatched at `core.js:295`). We generalize it into `continuation.trigger` and
aim to **decouple it from `completionAcpWakeup`**. Whether it can also be decoupled from a **live
`sessionKey`** — re-binding purely from the frozen `parent` (agent + Slack route) — is **NOT assumed
here**. It is an open capability that must be proven by the P1-gating spike below.

**Mandatory P1 spike (`spike-continuation-rpc`) — blocks P1 until it passes.** Empirically measure, for
each of the four parent conditions, whether a stable Gateway RPC can start a fresh Supervisor turn with
policy + cwd + workflow context restored:

| Parent condition | What to measure |
|---|---|
| live session | existing ACP resume + injected turn works; context intact |
| closed session | can a fresh Supervisor turn be started; is AGENTS.md/cwd/workflow context reloaded |
| sessionKey-less channel-default binding | can continuation target the channel binding without a sessionKey |
| frozen agent + Slack route only (no session) | can the harness bind by route alone and start a turn |

For each: record whether it produces an **ACP resume** vs a **fresh Supervisor turn**, and whether policy
(AGENTS.md), cwd, and the workflow `parent`/stage context are correctly restored.

**Gate:** if no stable Gateway RPC path is confirmed for the sessionless conditions, **P1 is BLOCKED** for
those conditions. Fallback for the blocked case (still shippable): the harness delivers a structured
"continuation-ready" notice (with `workflow_id` + `next_action`) and the user's next message triggers the
Supervisor review — i.e. we keep today's manual re-entry *only where the RPC is unproven*, rather than
silently assuming automation. The live-session and channel-default cases proceed if the spike confirms them.

**Idempotency:** continuation is claimed under the workflow lock with key `wf:<id>:continuation:<stage>`
(mirrors `claimNotification` at `core.js:379`), so a duplicated tick or a gateway retry never double-advances.

This directly resolves TERMINAL_EVENT_DELIVERED_BUT_SUPERVISOR_NOT_RESUMED and
NOTIFICATION_AND_CONTINUATION_CONFLATED (Temporal "Signal" analog: the terminal event is a durable signal
that drives workflow continuation, delivered independently of user notification).

---

## 9. Local Durable Runner Design (role separation, fixes quota burn)

The Local Durable Runner is **not a new component** — it is `durable_job` invoked with a **non-model
argv** (unittest, mutation, builder, zip, shellcheck, docker smoke). The fix is *orchestration + role
policy*, not new runtime:

- **Model Worker** (an AGY durable job) does: investigation, implementation, edits, and **declares** the
  verification commands + completion conditions, then **returns control** (its job ends). It must not run
  and poll long commands.
- **Local Durable Runner** (a `durable_job` on the declared command) executes the long command detached,
  recording stage/heartbeat/PID/exit/artifacts. No model tokens are consumed while it runs.
- **Supervisor** processes the terminal event and only re-invokes the model on FAIL.

Concretely, a stage with `runner_type:"local"` submits a durable job whose `command` is the local tool
argv; a stage with `runner_type:"model"` submits an AGY argv job. Both use the existing `startJob`. The
harness adds a **runner-type router** and a small `runner-profiles` map (which commands are "local"). This
is the Temporal Workflow/Activity split and the Hatchet worker-type routing, achieved with the runner we
already have.

### Forbidden actions: dual control (do NOT trust Model-declared commands)

A Model Worker *declares* the verification/build commands, but the Local Runner must **not** execute them
on trust. Two independent controls:

1. **Pre-execution policy (structured argv/capability allowlist).** Before a local stage is submitted, the
   harness validates the declared argv against a **runner profile allowlist** — an explicit map of
   permitted executables + argument shapes + capabilities per stage type:

   ```yaml
   runner_profiles:
     full_unittest:   { exec: ["node","npm","pytest","vitest"], allow_flags: ["--test","run"], net: false }
     distro_smoke:    { exec: ["docker"], allow_subcmd: ["build","run","rm"], net: true }
     builder_and_zip: { exec: ["bash","zip","shellcheck"], writes: ["dist/**","*.zip"] }
   ```

   Any declared command whose `command[0]` (basename) or argument shape is outside the stage's profile, or
   that matches a **forbidden-argv denylist** (`git clean`, `git reset --hard`, `git push`, `git tag`,
   `git commit` in non-commit stages, canonical-merge invocations), is **rejected before spawn** —
   `stage_state = BLOCKED_DEPENDENCY`, no durable job created. This is a structured argv check, not a
   substring scan of a free prompt.

2. **Post-execution Git/repository evidence audit.** After the stage's job terminates, the harness diffs
   the worktree (via the multi-field fingerprint, §10) and asserts: no change outside
   `allowed_output_manifest`, no forbidden ref/branch/tag mutation, no unexpected commit. A violation
   detected post-hoc ⇒ stage `FAILED` + the workflow records the evidence (it does not "undo", it reports).

Pre-execution blocks the *predictable* violations cheaply; post-execution catches anything the command did
at runtime beyond its declared argv. Both feed the semantic-success predicate (§6).

---

## 10. Checkpoint, Atomic Transition & Resume Design (DBOS/LangGraph-informed, minimal)

### Authoritative state & multi-file atomicity

State is spread across `workflow.json`, `stages/<stage>/stage.json`, the per-attempt
`spec.json`/`runtime.json`/`result.json` (§7), and each activity's `job.json`. A transition that touches
more than one file (e.g. advance `current_stage`, open the next attempt) is **not** atomic at the
filesystem level. Definition:

- **Source of Truth = per-attempt `spec.json` (write-once) + `result.json` (write-once) + the committed
  transition journal `journal/<seq>.json` (`committed:true`).** These are write-once/append-only.
- **`runtime.json`, `stage.json`, and `workflow.json` (incl. `completed_stages[]`, `current_stage`) are
  regenerable projections**, rebuilt by replaying the committed journal over the `spec`/`result` records.
  On any disagreement, the projections are discarded and rebuilt; they are never the tiebreaker.
- **`job.json` is authoritative only for its own process/provider fields**, never for stage/workflow
  verdicts (those are harness-owned and recorded in the attempt's write-once `result.json`).

**Workflow-level lock.** A single `workflows/wf-<id>/.wf.lock` (same `mkdir`-based lock as
`job-store.acquireLock`, `job-store.js:69`, with the 30s stale reclaim) serialises every multi-file
workflow transition. Per-job `.lock` still guards individual `job.json` writes; the workflow lock is a
coarser lock held only for the duration of a transition, never across a stage's execution.

**Transition journal (intent log).** Each multi-file transition is written as a single atomic journal
entry *before* mutating the target files, and cleared *after*:

```jsonc
// workflows/wf-<id>/journal/<seq>.json   (atomic write, one intent per file)
{ "seq": 42, "op": "advance_stage", "from": "direct_verification", "to": "full_unittest",
  "expected_prev_state": "PASSED", "at": "...", "committed": false }
```

Transition procedure (under the workflow lock): (1) atomic-write journal entry `committed:false`;
(2) write the new/updated stage record(s) via atomic rename (`job-store.js:55`); (3) atomic-write journal
entry `committed:true`. A crash between (1) and (3) leaves a `committed:false` entry.

**Crash reconciliation** (runs at reconciler start, per workflow, under the workflow lock): find the
highest journal seq. If its `committed:true`, state is consistent. If `committed:false`, **replay
idempotently**: re-derive the projections (`stage.json`, `workflow.json`) from the authoritative attempt
records + committed journal; if the target attempt record was not yet written, the op is a no-op (safe to
re-issue); if it was written, mark the journal committed. Because attempt records are write-once and each
write is an atomic rename, reconciliation is deterministic and never double-advances.

### Stage activity submission idempotency (crash-safe across every window)  *(P3 design; not P0)*

A `listJobs`-scan-then-spawn is **not** exactly-once by itself: the window
`idempotency lookup → job row create → worker spawn → record job_id into runtime.json` has multiple crash
points. The contract must make the **reservation atomic and prior to spawn**, and make the job row — not a
post-hoc scan — the dedupe anchor:

- **Idempotency key** `wf:<workflow_id>:stage:<stage>:attempt:<n>` lives in the write-once `spec.json` and
  is stamped onto the durable job row itself at creation (new `job.idempotencyKey` field).
- **Reservation-before-spawn (job store enforces uniqueness):** the job row is created **keyed by the
  idempotency key** with a reserved directory name derived from that key (not a random job id), so a second
  create for the same key fails atomically (`mkdir` `EEXIST`, like `createJob`/`acquireLock`,
  `job-store.js:118,74`). Row creation (state `RESERVED`) completes **before** any worker spawn. Two rows
  for one key are therefore impossible at the filesystem level.
- **Explicit lifecycle for the windows:** `RESERVED` (row exists, worker not yet spawned) → `QUEUED`
  (spawn issued) → `RUNNING`. Reconciliation, keyed by the idempotency key:
  - row `RESERVED` and no live worker → the crash happened **after row create, before spawn** → safe to
    (re)spawn once, or, if a spawn was already attempted and its pid is unknown, mark `LOST`/`BLOCKED`
    explicitly rather than blind-respawn;
  - row `QUEUED`/`RUNNING` with a live worker (`isProcessAlive`, `job-store.js:197`) → **re-attach** the
    existing job into `runtime.json`; never spawn a second;
  - row `RESERVED`/`QUEUED` with a dead worker and no terminal `result.json` → resume per the rule above.
- **Result:** the same activity never runs twice; a crash in any window resolves to exactly one of
  {re-attach, single safe respawn, explicit LOST/BLOCKED}.

This is a **P3 design item** (it needs the workflow store); it is documented precisely here but is **not
part of the P0 production scope**.

### Checkpoint granularity & resume

- **Checkpoint granularity = stage** (smallest re-runnable unit; matches the durable-job unit). A stage is
  "completed" only when its record reaches a terminal stage_state AND its journal entry is `committed:true`.
- **Resume rule (multi-field fingerprint, not HEAD alone):** for each pipeline stage, skip (do not re-run)
  **only if** the stage record is completed **and** the recorded `resume_fingerprint` reconciles with the
  live worktree across **all** required fields: `head`, `index_fingerprint`, `tracked_diff_hash`,
  `untracked_path_manifest`, policy-selected `untracked_content_hash`, and `allowed_output_manifest`.
  Any mismatch outside the `allowed_output_manifest` ⇒ do not skip; the stage is re-run or the workflow is
  `BLOCKED` (a change outside allowed outputs is a safety event, not a silent resume). HEAD equality alone
  is insufficient because uncommitted/tracked/untracked drift changes the meaning of a "completed" stage.
- **Version-safe resume (DBOS `application version`):** each stage records the `harness_version` that ran
  it; on resume, if `workflow.harness_version` ≠ current and the pipeline definition changed incompatibly,
  the workflow enters `BLOCKED` with an explicit reason rather than resuming under new semantics
  (consistent with the fail-closed legacy philosophy at `core.js:494`).

No Postgres, no external store: the atomic-JSON ledger + workflow lock + journal is sufficient for a
single host and a single reconciler. (If concurrency ever exceeds one reconciler, revisit SQLite —
explicitly out of scope now.)

---

## 11. Observability: Heartbeat & Stalled Detection

Add to the stage record a `heartbeat` block (§7) and have the worker update it:

- **Worker side (`worker.js`):** in addition to `childPid`, periodically (e.g. every N s, unref'd timer)
  touch `last_heartbeat_at`, and update `last_output_at` when the stdout/stderr fd size grows. Record a
  coarse `child_process_state` (RUNNING) and, where the runner declares it, `current_step`.
- **Runner-profile heartbeat/timeout contract.** Stall thresholds are **per runner profile**, not global:
  a `distro_smoke` may be silent for minutes; a `markdown_check` should not. Each profile declares
  `{ heartbeat_interval, silence_budget, hard_timeout }`. There is no single global `T_stall`.

- **Two-level, conservative detection (never auto-kill on absence alone).** Reconciler
  (`markLostIfNeeded`, `core.js:470`) distinguishes a *suspicion* from a *confirmation*:

```text
silence (no heartbeat AND no output) beyond the profile's silence_budget:
  ├─ child pid gone before terminal              → LOST (existing behaviour, core.js:483)
  └─ child pid alive (isProcessAlive, job-store.js:197):
        → mark SUSPECTED_STALL (observation only: NO kill, NO retry)
        → sample progress signals (fd growth, ps CPU/IO) over a second, longer confirm window
             ├─ any progress OR within hard_timeout   → healthy long run, keep waiting
             └─ still no progress AND confirm window elapsed AND past silence_budget
                                                       → STALLED → surface to policy
                                                          (STALLED does NOT itself kill/retry; the
                                                           workflow policy decides resume/BLOCKED, and
                                                           only hard_timeout triggers a signalled stop)
```

  `SUSPECTED_STALL` is a soft, self-clearing observation; only a sustained `STALLED` past both the silence
  budget and a separate confirm window, or an exceeded `hard_timeout`, is actionable. CPU/output absence
  **alone** never triggers an automatic kill or retry.

- **Local Runner progress protocol (`current_step`).** So the harness has a positive progress signal
  instead of inferring from stdout, a local stage command may emit single-line progress markers on a
  dedicated control channel (e.g. `##WF-STEP name=vitest pct=40`) which the worker records as
  `heartbeat.current_step` + refreshes `last_output_at`. Absent the protocol, the runner falls back to
  fd-growth/CPU sampling — but stall is still only *suspected*, never confirmed, from absence alone.

Never infer a stage's health from stdout absence alone (a quiet `vitest` is normal). Internal cadence is
high; **user-facing** Slack cadence stays low (§14). (Prefect Crashed/Late/TimedOut taxonomy informs the
SUSPECTED_STALL/STALLED/LOST/TIMED_OUT distinction.)

---

## 12. Verification Evidence Model

Every verification produces a structured **EvidenceItem**, and the final report states assurance level.

```jsonc
{
  "check": "full_unittest",
  "result": "PASS",                       // PASS FAIL SKIPPED
  "verification_level": "LOG_VERIFIED",   // REEXECUTED LOG_VERIFIED ARTIFACT_VERIFIED WORKER_REPORTED INFERRED
  "evidence_path": ".../job-<id>/stdout.log",
  "input_commit": "61fff53...",           // or tree_hash
  "timestamp": "..."
}
```

### Provider result source — strict priority (never scan full stdout)

`provider_state` (§6) is derived by trying sources **in order**, stopping at the first that yields a
structured value. A general full-stdout string search is **not** used:

```text
1. AGY structured result envelope     — the `--output-format json` result object's status/error fields
2. dedicated provider result artifact — a side file the runner writes (if AGY emits one)
3. control stderr / result field      — a bounded, named control channel (not arbitrary log text)
4. limited-scope string fallback      — ONLY the result envelope's error message field, size-bounded,
                                         matched against a small known-error table; never the whole log
```

If none yields a confident classification, `provider_state = UNKNOWN` (which, with `process_state=COMPLETED`,
is `COMPLETED_UNVERIFIED`, not success).

**Mandatory P0 spike (`spike-agy-json-fixtures`) — blocks P0 evaluator work.** Collect real AGY
`--output-format json` outputs for: (a) a clean success, (b) quota exhaustion / BLOCKED_QUOTA, (c) auth
failure, (d) context-limit, (e) a mid-run tool interruption. Store as fixtures and derive the exact field
names/shapes for sources 1–4 **before** implementing `evaluator.js`. Until captured, the mapping is
unproven and the evaluator must default to `UNKNOWN` rather than guess.

**Division of labour (fixes duplicate CI):**
- Worker/Local runner: runs the full verification, emits logs + artifacts + EvidenceItems.
- Supervisor: verifies commit/tree/log/artifact **integrity** (cheap), and re-executes **only** the
  minimal set of claims that could overturn the conclusion, or a stage whose evidence is missing/suspect.
  Default assurance for a worker-run check that the Supervisor did not re-run is `LOG_VERIFIED` or
  `ARTIFACT_VERIFIED` — **never** silently upgraded to `REEXECUTED`.
- Canonical gate: one full pipeline pass when policy requires it.

**Semantic success predicate** = §6 rule, evaluated by the harness. **Docker smoke** must be
`ARTIFACT_VERIFIED`/`REEXECUTED` from an explicit success artifact — a cleaned-up container is *not*
evidence (`INFERRED` is not acceptable for a smoke PASS).

**Risk-based verification profile** (explicit rules, no AI classifier initially): a YAML map from
repository/change profile → pipeline subset, resolved at workflow start into `pipeline`.

```yaml
docs_only:         [ markdown_check, diff_audit ]
evaluator_only:    [ direct_tests, full_unittest ]
collector_changed: [ full_unittest, hermetic_zip, distro_smoke ]
canonical_gate:    [ full_pipeline ]
```

**Documentation drift & repository-specific semantics live in a repository profile/hook, NOT in Harness
core.** The Harness core stays repository-agnostic: it only invokes a declared, per-repository
`verification_hook` (a plain executable/command named by the profile) and consumes its structured
EvidenceItems. The drift rules — e.g. (a) a still-present legacy dependency marked removed, (b) a contract
change reduced to a deletion, (c) a claimed verification level above the recorded EvidenceItem level — are
implemented **inside that repository's profile hook**, not hardcoded in the plugin. This keeps
repository-specific business meaning out of the durable runner and the Harness core (a §20 non-goal), and
lets a `docs_only`/`collector_changed` profile ship its own drift check without touching the plugin.

---

## 13. Input Integrity Validation

A gate **before** `startJob`/before an expensive worker submission. Applied to the instruction/prompt
payload (for AGY jobs, the prompt is `command[i]`; the harness validates the source instruction).

Detectors (rule-based):
- ends on a conjunction / open clause (e.g. trailing "또는", "그리고", "다음", ":");
- unterminated Markdown code fence (odd count of ```);
- unterminated numbered list / "Phase N" structure;
- announces a section ("다음 금지사항", "최종 보고 형식") whose body is absent;
- unbalanced brackets/quotes; malformed embedded JSON/YAML;
- a previously-required contract field suddenly missing (compare to the workflow's declared required set).

Handling — **restoration is allowed only from an explicit prior contract, never from natural-language
inference:**

```text
restorable ⇔ ALL of:
   - the request carries a structured `contract_version` AND `contract_hash`
   - a stored, structured previous instruction with that contract_hash exists
   - the missing constraints can be taken verbatim from that stored structured instruction
  → restore verbatim, RECORD restored constraints + source contract_hash in workflow.input_integrity,
    and state them before execution

otherwise (no contract_version/contract_hash, or no matching stored instruction, or a constraint that
would have to be *guessed* from surrounding prose)
  → status = INCOMPLETE_INSTRUCTION, workflow_state = BLOCKED, DO NOT submit the worker
```

The harness must **never** reconstruct forbidden-actions / stop-conditions / report-format from
natural-language context. Absent a verifiable prior contract, block. This is a harness responsibility (the
process runner stays semantics-free).

---

## 14. Slack Notification Model

- **P0 terminal wording (job-outcome, no workflow yet):** the terminal notice reports the P0 job-outcome
  (§6), never "SUCCEEDED". Examples:

```text
Durable job COMPLETED_UNVERIFIED: <name>  job_id=job-…  exit_code=0  provider=OK  (result not yet verified)
Durable job FAILED_PROVIDER: <name>       job_id=job-…  exit_code=0  provider=BLOCKED_QUOTA
Durable job FAILED_COMMAND: <name>        job_id=job-…  exit_code=2
```

  `COMPLETED_UNVERIFIED` explicitly signals "process finished, work not asserted correct" — it must not
  read as success.

- **Normal path (P1+):** one short status line per meaningful transition (RESUMED / stage done / next), e.g.:

```text
RESUMED  worktree preserved: yes  runner: local  job: job-…  stage: full_unittest  next: independent verification
```

- **Detail on demand / on anomaly:** full audit (evidence, logs, tree hashes) lives in
  `workflow.json` + stage records + logs, surfaced only when a stage is anomalous or the user asks
  (`workflow.get_evidence`).
- **Separation:** `notification.emit` (user-facing) and `continuation.trigger` (workflow advance) are
  independent (§8). Duplicate/noisy status is eliminated by making the outbox emit **one** deterministic
  line per transition (extends the existing single-terminal-notice discipline in `delivery-outbox.js`).

---

## 15. Backward Compatibility

- **`durable_job` contract unchanged.** Existing callers (Supervisor AGENTS.md argv jobs, the infra owner
  context-free path) work identically. New job fields are additive.
- **Old `job.json` rows** (no workflow wrapper) reconcile exactly as today: absence of `workflow_id` ⇒ the
  harness treats the job as a standalone activity and uses the legacy terminal notice. The §6 mapping
  keeps `SUCCEEDED` readable.
- **Outbox unchanged**; the frozen-route delivery remains the notification transport.
- **Legacy fail-closed** semantics preserved (`core.js:494` / `index.js:85`): pre-outbox active jobs still
  block; the new harness adds a parallel version guard for workflows (§10), not a replacement.
- **Migration:** no auto-migration (consistent with current policy). New workflows use the new schema;
  in-flight plain jobs finish under the old path. A `workflow.adopt` (optional, later) could wrap an
  existing job into a single-stage workflow, but is **not** required for P0–P2.

Compatibility risk: **projected low, NOT yet asserted** — this is a hypothesis to be **validated by the
back-compat test suite before the P0 gate**, not a conclusion. The intended-and-only user-visible change
is that a provider-quota exit-0 reports `FAILED_PROVIDER`/`COMPLETED_UNVERIFIED` instead of a success, and
the `durable_job` tool schema is unchanged. Required evidence before calling it "low": (1) old `job.json`
fixtures (SUCCEEDED/FAILED/TIMED_OUT/CANCELLED/LOST) reconcile under the §6 mapping; (2) the reconciler
processes a pre-workflow job with no `workflow_id` exactly as today; (3) the outbox delivery path byte-for-
byte unchanged for legacy jobs. Until those pass, the risk is **UNVERIFIED**.

---

## 16. Security & Safety Considerations

- **No new daemon, no new listening port** — the harness runs inside the existing reconciler service.
- **Path & exec containment** reused unchanged: `resolveAllowedCwd` + `assertExecutable`
  (`job-store.js:139,166`); local-runner commands are still confined to allowed roots.
- **Forbidden actions** (`git clean`, `reset --hard`, `canonical_merge`, `push`, `tag_change`) are recorded
  in `workflow.forbidden_actions` and enforced as a stage post-condition (a stage that performed one
  ⇒ FAILED), and are part of the semantic-success predicate.
- **Secrets:** `selfLoadPluginConfig` still returns only the plugin's own config section
  (`core.js:57-87`); evidence/logs must not capture provider tokens (bound-size log capture already at
  `core.js:171`).
- **Idempotency everywhere** (continuation, stage submit, delivery) prevents duplicate side effects across
  restarts (extends the existing `idempotencyKey` discipline).
- **Read-only for infra-scanner:** the harness treats target worktrees per the workflow's
  `forbidden_actions`; canonical merge/push remain gated.

---

## 17. Test Strategy

Reuse the existing DI-seam unit-test style (`test/*.test.js`, `node --test`), no live gateway.

- **Unit (pure, injected seams):**
  - process/provider/stage separation: worker sets `process_state` only; evaluator maps AGY JSON →
    `provider_state` (OK / BLOCKED_QUOTA / AUTH_FAILED / CONTEXT_LIMIT) and the fallback string classifier.
  - semantic predicate: exit0 + BLOCKED_QUOTA ⇒ stage FAILED (regression test for the confirmed bug).
  - continuation trigger: terminal job ⇒ one idempotent continuation; duplicate tick ⇒ no double-advance.
  - checkpoint/resume: completed stage with matching tree hash ⇒ skipped; changed hash ⇒ re-run; version
    mismatch ⇒ BLOCKED.
  - input integrity: truncation fixtures (trailing conjunction, unclosed fence, missing "금지사항" body)
    ⇒ INCOMPLETE_INSTRUCTION / restore-and-record.
  - heartbeat/stall: no-progress + alive ⇒ STALLED; alive + progress ⇒ keep waiting; gone ⇒ LOST.
  - evidence/assurance: worker-run-only check ⇒ level ≤ LOG_VERIFIED; docker smoke without artifact ⇒
    not PASS.
- **Integration (local-only, no model):** a real local durable job (e.g. `node --test` on a fixture) driven
  through a two-stage workflow with resume after a simulated crash.
- **Failure-recovery scenarios (documented + tested where feasible):** reconciler restart mid-SENDING
  (existing DELIVERY_UNKNOWN parking), worker killed mid-stage (LOST + resume), provider quota mid-model
  (BLOCKED_QUOTA + fallback resume with checkpoint), stalled long command (STALLED + policy), truncated
  input (blocked before spawn), gateway send flapping (outbox backoff).

---

## 18. Migration Plan

1. Ship P0 (state separation + evaluator + false-success fix) as **additive** job fields; old rows
   unaffected; `durable_job` schema unchanged. Verify with unit tests + one local smoke.
2. Ship P1 (single-job continuation) for **new-format standalone jobs that carry an explicit `parent`
   block** — the continuation re-invokes the Supervisor on that one job's terminal event. There is **no
   `workflow.json` and no multi-stage advancement in P1**; that arrives in P3. **Legacy standalone jobs
   (no `parent` block) keep the existing terminal-notice path unchanged.** So the split is by *job format*
   (has-parent-block vs legacy), not "new workflows only".
3. Introduce `workflow.json` + `workflow.*` tool surface and **workflow.json-based multi-stage
   continuation** in P3; `durable_job` remains for direct activities and back-compat.
4. No destructive migration; no rewrite of in-flight jobs. Provide `workflow.adopt` later if wrapping
   legacy jobs becomes useful.
5. Each phase is independently revertible (additive files/fields; the plugin is separable from the
   OpenClaw dist and versioned in this repo).

---

## 19. Phased Implementation Plan

Ordering follows the requested P0–P4, with two dependency notes: (a) **provider_state parsing (P0) must
precede any continuation (P1)**, because continuation decisions depend on the provider verdict — otherwise
P1 would continue on false success; (b) **P1 is scoped to single-job continuation only** (notification /
continuation split + one Supervisor re-invocation on a terminal job). **Multi-stage advancement is P3**,
because advancing to a "next stage" requires `workflow.json`/stage records that do not exist until P3. This
resolves the P1↔P3 apparent conflict: P1 does *not* advance stages; it re-invokes the Supervisor on a
terminal job so review is automatic; the workflow engine that chains stages arrives in P3.

### P0-0 — Required spikes (block their dependents)
- **`spike-agy-json-fixtures`** (blocks the P0 evaluator): collect real AGY `--output-format json` for
  success / quota / auth / context-limit / tool-interrupt; derive the exact envelope fields (§12). Until
  captured, `evaluator.js` defaults `provider_state=UNKNOWN`.
- **`spike-continuation-rpc`** (blocks P1): the four-condition Gateway RPC measurement (§8). If sessionless
  RPC is unproven, P1 ships only the proven conditions and the rest stay manual (BLOCKED), not assumed.

### P0 — State separation & false-success prevention  *(foundation, no workflow contract yet)*
- Add `process_state` (incl. `CANCELLED`) to the worker outcome; stop equating exit0 with success
  (`worker.js:80-91`). Keep `state` as a back-compat alias mapped from `process_state`.
- New `dist/evaluator.js`: derive `provider_state` from the AGY result envelope via the strict source
  priority (§12) — **not** a full-stdout scan; default `UNKNOWN` until the fixture spike lands.
- New `dist/verdict.js`: **P0 job-outcome only** — `FAILED_COMMAND | FAILED_PROVIDER |
  COMPLETED_UNVERIFIED | CANCELLED` (§6). **No stage `PASSED` is produced in P0** (there is no contract to
  check artifacts/paths/commit/profile). The full semantic-PASS predicate arrives with workflows in P3.
- Persist `parent` linkage explicitly on the job (already have `sessionKey/sessionId/requesterOrigin/
  flowId` at `core.js:606-610`; formalize into a `parent` block for the future workflow).
- Terminal Slack message reports the **job-outcome** (§14 wording), not `job.state`
  (`delivery-outbox.js:80-94`).
- **Files expected to change in P0:** `dist/worker.js`, `dist/delivery-outbox.js`, `dist/core.js`
  (job schema + reconcile outcome), **new** `dist/evaluator.js`, `dist/verdict.js`, `openclaw.plugin.json`
  (version), `README.md`, `test/worker.test.js`, `test/delivery-outbox.test.js`, **new**
  `test/evaluator.test.js`, `test/verdict.test.js`, plus the two spike fixture sets.

### P1 — Single-job continuation trigger (notification/continuation split)  *(gated by `spike-continuation-rpc`)*
- Generalize `buildCompletionTurn`/`dispatchCompletionTurn` into `dist/continuation.js`; decouple from
  `completionAcpWakeup`. Decoupling from a live `sessionKey` is applied **only** for the conditions the
  spike proved; unproven conditions fall back to a structured "continuation-ready" notice (§8).
- Reconciler fires `notification.emit` and `continuation.trigger` independently (`core.js:539` rework).
- Scope: re-invoke the Supervisor on **one terminal job**. No multi-stage advancement here (that is P3).
- Idempotent continuation claim under the workflow/job lock (mirror `claimNotification`, `core.js:379`).

### P2 — Runner separation & observability
- Runner-type router + `runner-profiles` (model vs local) — routes long commands to local durable jobs.
- Heartbeat fields in worker; STALLED detection in `markLostIfNeeded` (`worker.js`, `core.js:470`).

### P3 — Workflow engine: checkpoints, multi-stage advancement, handoff, preflight, provider cache, fallback
- `workflow.json` + stage records + workflow-level lock + transition journal + crash reconciliation
  (`dist/workflow-store.js` on top of `job-store.js`; §10). **Stage records are the SoT;
  `completed_stages[]` is a rebuildable projection.**
- **Multi-stage advancement engine:** chains stages by reusing P1's `continuation.trigger` primitive to
  submit the next stage's durable job (model or local) with the multi-field resume fingerprint (§10). This
  is the "advance to next stage" behaviour deliberately deferred from P1.
- `workflow.*` **OpenClaw plugin tool surface** (`start/status/approve/reject/cancel/resume/get_evidence`).
  This is registered as a native plugin tool via `api.registerTool` (like today's `durable_job`,
  `index.js:128`). A standalone **MCP server adapter** that re-exposes the same surface is a **separate,
  later packaging step** (§5, §20), not part of P3.
- Expand `preflight.js` into a profile-driven preflight (auth/quota/tool-probe/worktree/branch/docker/disk).
- `dist/provider-cache.js` (TTL blocked-pool cache). Fallback-with-checkpoint resume (§10).

### P4 — Input integrity, assurance levels, verification-profile, Slack slimming, doc-drift
- `dist/input-integrity.js` gate; EvidenceItem + assurance levels; risk-based profile YAML; one-line Slack
  status; documentation-drift rule check.

---

## 20. Open Questions & Explicit Non-goals

**Open questions** (the first two are the blocking spikes; see §19 P0-0)
1. `spike-continuation-rpc` (blocks P1): does OpenClaw expose a stable gateway method to start a Supervisor
   continuation turn **without** a live ACP session (re-bind by frozen `parent` route)? `chat.send(
   deliver:false)` works with a sessionKey today (`core.js:306`); the sessionless path is **unproven** and
   P1 is BLOCKED for the conditions it cannot confirm (§8).
2. `spike-agy-json-fixtures` (blocks the P0 evaluator): AGY `--output-format json` result-envelope field
   names for success/quota/auth/context/tool-interrupt must be captured from real runs; until then
   `provider_state` defaults to `UNKNOWN` rather than trusting a string scan (§12).
3. TaskFlow runtime as the parent-linkage substrate vs. a harness-owned `parent` block — TaskFlow is
   sessionKey-bound (`core.js:352`), so context-free workflows likely need the harness-owned block.
4. Confirmed-STALL progress signal on macOS: cheapest reliable "is this child making progress" probe
   (fd growth vs `ps` CPU sampling), feeding the conservative SUSPECTED_STALL→STALLED path (§11).

**Known P0 limitation (documented, conservative handling chosen)**
- The existing OpenClaw TaskFlow runtime exposes only `finish` (success) and `fail` (blocked) — there is
  **no neutral "completed-unverified" status**. Because P0 has no success outcome, `settleFlowWithApi`
  (`core.js`) **never calls `finish` for a new-format job**: every P0 outcome, including
  `COMPLETED_UNVERIFIED`, is routed to `fail` with the outcome as the blocked summary (so a P1 reviewer
  reads it as "needs verification", never "succeeded"). This is the most conservative available choice; a
  proper neutral TaskFlow status is deferred to the workflow engine (P3). Legacy jobs keep the original
  `finish`-on-`SUCCEEDED` behaviour unchanged.

**Explicit non-goals (this design)**
- No Temporal/Hatchet/DBOS *service* adoption; no external orchestrator; no Postgres.
- No SQLite migration (revisit only if multi-worker concurrency is required).
- No AI risk classifier (explicit profiles/rules only).
- No repository-specific business semantics inside the process runner or local runner.
- No Supervisor-prompt-only "fix" and no single mega-job that bundles all stages.
- No rewrite of the existing runner/outbox/reconciler; they are reused.

---

## Cross-project pattern comparison

| Concern | Current OpenClaw | Temporal | DBOS | LangGraph | Prefect/Hatchet | Recommended minimal design |
|---|---|---|---|---|---|---|
| Terminal continuation | Opt-in ACP wakeup, session-coupled (`core.js:539`) | Signal drives workflow continuation | Step returns → next step | Node completion → next node | Terminal-state hook | `continuation.trigger` fired by reconciler from frozen `parent`, independent of notification (§8) |
| Stage checkpoint | None (one job=one command) | Event history/replay | Completed-step checkpoint | thread/checkpoint snapshot | Task-level state | Stage-granular checkpoint in `workflow.json` + stage records on the atomic ledger (§10) |
| Process/provider/stage state | Single `state` (`worker.js:83`) | Activity vs workflow status | Step vs workflow | Node vs graph state | State type vs name | Three orthogonal axes + derived verdict (§6) |
| Local command runner | Model runs+polls long cmds | Activity (separate worker) | Step function | Tool node | Worker-type routing | `durable_job` on non-model argv, routed by runner-type (§9) |
| Human approval | None (user re-asks) | Signal/Update | Manual step | interrupt / HITL | pause + event wait | `APPROVAL_REQUIRED` stage → PAUSED + one Slack request → `workflow.approve` (§8,§14) |
| Retry and fallback | Outbox retry only (`delivery-outbox.js`) | Retryable vs non-retryable | Recovery by executor | Resume pending writes | Task retry + condition | Retryable/non-retryable by `provider_state`; fallback resumes from checkpoint (§6,§10) |
| Heartbeat & stalled | pid-liveness only (`core.js:470`) | Activity heartbeat | Executor liveness | — | Crashed/Late/TimedOut | Heartbeat + progress probe → STALLED/LOST distinction (§11) |
| Version-safe resume | Legacy fail-closed only (`core.js:494`) | Workflow versioning | App-version-safe resume | — | Deployment/version | `harness_version` per stage; incompatible ⇒ BLOCKED (§10) |
| Notification separation | Conflated in reconcile | Notification ≠ state | — | — | State hooks vs I/O | `notification.emit` vs `continuation.trigger` split (§8,§14) |

### Recommended architecture — option comparison

| Option | Solves the problems? | Right-sized for 1 Mac + personal Slack? | Separable from OpenClaw updates? | Back-compat with existing jobs? | Recovery & audit | MCP-exposable? | Ops complexity vs reliability |
|---|---|---|---|---|---|---|---|
| **1. Plugin-internal layered Harness** ★ | Yes — all P0–P4 map to concrete modules | Yes — no new daemon/port | Yes — ships as the plugin | Yes — additive | Atomic ledger + stage records + evidence | Yes — `workflow.*` | **Best ratio** |
| 2. Separate Harness service + MCP | Yes | Heavier — a 2nd process to run/supervise | Yes | Yes, but 2 stores to reconcile | Good | Yes | Overhead now; justified only at multi-host scale |
| 3. DBOS / embedded durable workflow | Yes (durability) | Needs Postgres — over-provisioned here | Partly | Requires migrating the ledger | Strong | Via wrapper | Infra cost > benefit for one host |
| 4. Temporal / Hatchet external | Yes (mature) | No — cluster/worker infra for personal use | Separate but heavy | Full re-model | Strongest | Yes | Reliability gain < operational burden |

**Selected: Option 1 — a Workflow Harness layer inside the existing durable-jobs plugin**, borrowing
patterns (not runtimes) from Temporal (Workflow/Activity split, Signal-driven continuation), DBOS
(completed-step checkpoint, version-safe resume), LangGraph (thread/checkpoint identifiers, interrupt),
and Prefect/Hatchet (state taxonomy, transition hooks, worker-type routing). It resolves every confirmed
failure mode with additive, backward-compatible changes, stays decoupled from the OpenClaw dist, reuses
the proven ledger/outbox/reconciler, and exposes a clean future `workflow.*` MCP surface — with the lowest
operational complexity for a single-host personal deployment.
