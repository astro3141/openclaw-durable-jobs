# OpenClaw Durable Jobs (v0.3 — P0 separated state model)

Runs long commands outside the agent turn, persists their lifecycle under the
OpenClaw state directory, and — on a terminal state — delivers a **deterministic
completion notice** to the Slack route that was **frozen at job creation**,
through a **persistent delivery outbox**. Completion no longer depends on waking
the owning ACP session or on the exact session key still being current.

The plugin targets the isolated `lab` profile first.

## P0 separated state model (v0.3)

Process completion is no longer equated with success. A new-format job (one with a
`parent` block) records three orthogonal fields in addition to the legacy `state`
alias (which is preserved verbatim for backward compatibility):

- **`processState`** — from the OS child: `QUEUED RUNNING COMPLETED FAILED_COMMAND
  INTERRUPTED TIMED_OUT CANCELLED LOST` (`dist/verdict.js`).
- **`providerState`** — from the model activity's **single stdout JSON envelope**
  (`status: SUCCESS|ERROR`, bounded `error` only; **never a full-stdout scan**):
  `UNKNOWN OK ERROR_UNCLASSIFIED …` (`dist/evaluator.js`, grounded in
  `test/fixtures/agy/`).
- **`jobOutcome`** — the P0 verdict: `FAILED_COMMAND | FAILED_PROVIDER |
  COMPLETED_UNVERIFIED | CANCELLED`. **P0 never emits a success/PASSED.** Exit 0
  alone yields at most `COMPLETED_UNVERIFIED`; a provider failure on an exit-0
  process yields `FAILED_PROVIDER` (the false-success fix).

Legacy jobs (no `parent` block) keep the exact old terminal-notice wording. See
`docs/DESIGN_workflow_harness.md` for the full design and later phases.

## P1 continuation (v0.4)

The **user-facing notification** (terminal outbox notice) and the **internal continuation** (automatic
Supervisor review) are now separate paths. Continuation is **opt-in — `continuationEnabled` defaults to
`false`** (set it `true` in the profile config to roll out): because P0 gives every new job a `parent`
block, a default-on upgrade would silently change behaviour for all new jobs. **Legacy jobs (no `parent`
block) never get P1 continuation** regardless of the flag.

When enabled, for **new-format jobs with an explicit `parent.sessionKey`** (including the canonical
channel-default binding key), the reconciler auto-dispatches a Supervisor review turn via
`chat.send(deliver:false)` with a deterministic idempotency key (`durable-job:<id>:continuation`) — no
Slack user message, and any duplicate terminal tick dispatches at most once. Every new-format outcome
(incl. `FAILED_PROVIDER`) triggers a review.

Completion is **correlated to a unique per-dispatch marker** in `chat.history` (never "some assistant
reply exists"), so unrelated/past replies or a marker trimmed out of the history window never yield a
false `COMPLETED`. A transient `chat.history` error keeps the state `DISPATCHED` (recorded in
`lastCheckError` / `lastCheckedAt`) and is isolated from the rest of the reconcile pass — it is not a
`DISPATCH_FAILED`.

The one-time fallback notice mirrors the delivery outbox's conservative posture (the gateway `send` is
**not** a proven exactly-once Slack dedup): `fallbackState: PENDING → SENDING → DELIVERED`. Because a
`send` exception (timeout, connection reset, interruption, response-parse failure) can mean the message
was **already delivered but the response was lost**, an **ambiguous transport failure is parked as
`DELIVERY_UNKNOWN` and never auto-resent** — a retry could duplicate the Slack notice. The retryable
`PENDING` path is entered **only** for an error that is *structurally proven* to be a pre-send rejection
(`error.preSendRejected === true` or `error.code === "PRE_SEND_REJECTED"`); retryability is never guessed
from an error message. The gateway-call layer surfaces no such proof today, so in practice **every** send
exception is treated as `DELIVERY_UNKNOWN`. A lease-expired `SENDING` (ambiguous crash around the send) is
likewise parked `DELIVERY_UNKNOWN`. Every fallback send error is swallowed (recorded in `fallbackLastError`
/ `fallbackLastAttemptAt`) and never aborts the reconcile pass.

**Runtime disable:** setting `continuationEnabled=false` blocks only new creation/dispatch. A continuation
already in flight (e.g. `DISPATCHED` when the operator flips the flag) is still driven to a terminal state
(completion / timeout / fallback); a not-yet-dispatched `PENDING` record found while disabled transitions
to `MANUAL_FALLBACK` (reason `CONTINUATION_DISABLED`). Config fields (`continuationEnabled`,
`continuationTimeoutMs`, `continuationDispatchLeaseMs`, `continuationMaxAttempts`) are declared in the
plugin `configSchema` (which is `additionalProperties:false`).

Grounded in the live spike (`docs/spikes/RESULTS.md`), a `chat.send` that returns `started` is only
**DISPATCHED**, never success. The continuation is a small state machine (`dist/continuation.js`):

```
PENDING → DISPATCHING → DISPATCHED → COMPLETED
                                   ↘ TIMED_OUT
        ↘ FAILED            ↘ MANUAL_FALLBACK
```

Completion is confirmed only when the injected turn's reply appears in `chat.history` within a bounded
window (`continuationTimeoutMs`, default 120s). On `FAILED` / `TIMED_OUT` / `MANUAL_FALLBACK` a **single**
structured `CONTINUATION_READY` fallback notice is sent to the frozen route inviting manual re-entry.
Auto-retry is conservative (`continuationMaxAttempts`, default 1 — no unbounded retry after an ambiguous
or abrupt backend loss). **Unsupported / unproven** conditions (route-only parent, no `sessionKey`,
clean-closed-session auto-recreation) fall back to manual re-entry rather than a claimed auto-continuation.
Legacy jobs keep the opt-in `completionAcpWakeup` path unchanged.

## P2-B runner separation (v0.5)

Every job carries **runner metadata** so a long test/build/docker command is represented as a *local*
durable activity (never a model turn the model has to poll) and the provider evaluator is applied only to
true model activities.

- Two optional `durable_job start` params: `runnerType` (`model | local`) and `runnerProfile`
  (`model_agy | local_test | local_build | local_docker | generic_local`). Absent ⇒ inferred from the
  command (`agy` basename → `model_agy`, else `generic_local`), so existing callers and legacy jobs are
  unaffected.
- **Explicit metadata is validated, never blindly trusted.** `startJob` resolves the *effective* metadata
  and **rejects incompatible combinations before any job/flow/worker is created** (`RUNNER_METADATA_INVALID`):
  a known model executable (`agy`) can never be downgraded to `local`; `model_agy` requires the `agy`
  executable; `runnerType` must match the profile's canonical type. This closes the evaluator-bypass hole
  (`agy` + `runnerType=local` + `resultProtocol=none`).
- `activityType`/`resultProtocol` are **derived only from the validated profile** — the caller can never
  set `resultProtocol` or `providerState`. `model_agy → agy-json` (AGY envelope evaluator applies);
  all `local_*`/`generic_local → none` (a local command's plain stdout, even a literal
  `{"status":"ERROR"}`, is never run through the evaluator, so it can't be mis-read as `FAILED_PROVIDER`).
- `runnerType`/`runnerProfile`/`activityType`/`resultProtocol` are stored on `job.json` and exposed in
  `publicJob`/status.

**Basename limitation (documented):** inference recognises only the exact executable basename `agy` as a
model runner — an absolute path such as `/absolute/path/to/agy` still resolves to `model_agy`, but a
wrapper like `agy-wrapper` is **not** auto-guessed as a model and resolves to `generic_local`. Arbitrary
wrappers are never trusted as model runners; forcing one to `model_agy` is rejected.

Not in P2-B (deferred): heartbeat / `livenessState` / stall detection (P2-A), and any executable
allowlist or forbidden-action policy (P3). Profiles here carry only type/protocol, no enforcement.

## P2-A heartbeat & stall observability (v0.5)

Each RUNNING job gets a lightweight liveness **observation** that is strictly separate from `processState`
and **never kills, retries, or changes `processState`** — the only forced stop remains the per-job hard
`timeoutSeconds` (`TIMED_OUT`), and a vanished worker with no terminal record is still `LOST`.

- **Observer vs child progress (important):** a fresh `observerHeartbeatAt` proves the **worker observer**
  is alive — **not** that the child is progressing. `heartbeat.json` therefore records both
  `observerHeartbeatAt` (observer liveness) and `lastProgressAt` (observable **child** progress = stdout/
  stderr byte growth or a `currentStep` change), plus `lastOutputAt, stdoutBytes, stderrBytes, childPid,
  childProcessState, currentStep, childCpuMs`. `HEALTHY` requires fresh **child progress**, not merely a
  live observer.
- **Heartbeat storage:** the worker (single writer) writes the **separate lock-free `heartbeat.json`**
  (atomic temp+rename with a unique temp name) on an **`unref`'d** timer; writes are **serialized into one
  promise chain** so two writes never overlap, and on child exit the timer stops, the in-flight write is
  drained, and a final `EXITED` sample is written **last** (a late `RUNNING` write can never overwrite it).
  A write failure is swallowed and never interrupts the child or its terminal result.
- **Progress marker (optional):** a complete `##WF-STEP name=<safe-name>` line (bounded
  `[A-Za-z0-9_.:-]{1,64}`) on stdout **or** stderr updates `currentStep`; malformed markers are ignored and
  it never affects stdout semantics or the provider evaluator. Only a **bounded tail** of each log is read
  (`open`→`stat`→read last N bytes), and a truncated partial first line is dropped.
- **Per-profile policy** (frozen onto the job at creation): `heartbeatIntervalMs` / `silenceBudgetMs` /
  `stallConfirmMs` / `stallConfirmSignal`. Defaults differ per profile (`local_docker` most patient).
  `resolveObservabilityPolicy` validates (positive; `heartbeatIntervalMs < silenceBudgetMs`) →
  `OBSERVABILITY_CONFIG_INVALID` otherwise. `silenceBudgetMs >= hard timeout` is allowed (the job simply
  `TIMED_OUT`s first); `timeoutSeconds=0` (unlimited) is allowed.
- **Liveness derivation:** `HEALTHY` (child progress fresh within the silence budget) → `SUSPECTED_STALL`
  (observer alive + child progress stale + pid alive) → `STALLED`. Progress returning flips back to
  `HEALTHY`. `job.json` is written **only on a state change**. Legacy RUNNING jobs with no `heartbeat.json`
  keep the existing pid-liveness behaviour. `livenessState`/`livenessSince`/`observability` are in
  `publicJob`; `status` includes the current `heartbeat`.
- **STALLED needs a positive confirm signal.** Because a live observer alone can't tell a quiet-but-healthy
  child from a stalled one, `STALLED` is only reached when the profile sets `stallConfirmSignal: "cpu"`
  **and** the child's CPU-time did not increase over the confirm window (no output, no `currentStep`
  change). **CPU sampling is an opt-in P2-A feature:** the worker samples child CPU-time (`childCpuMs`, via
  `ps`) **only** when a profile sets `stallConfirmSignal: "cpu"`. **By default `stallConfirmSignal` is
  unset for every profile, so jobs cap at `SUSPECTED_STALL` and are never promoted to `STALLED`.** **Documented limitation:** without a child-progress signal, a
  genuinely silent process (e.g. a long `sleep`/I-O wait) is indistinguishable from a stall — it becomes
  `SUSPECTED_STALL` after the silence budget (an observation only). All liveness states are observation-
  only: **never** kill, retry, or change `processState`; the only forced stop is the hard `timeoutSeconds`.

Not in P2-A (deferred): any automatic kill/retry, Slack stall alerting, and all P3
workflow/checkpoint/allowlist features.

## P3-A workflow store — storage foundation only (v0.6)

`dist/workflow-store.js` is the durable persistence layer for multi-stage workflows. **P3-A is storage
only** — it does **not** register a `workflow.*` tool, create durable jobs, link stages to jobs, compute
verdicts, call continuation, or advance stages. Those are later P3 steps.

- **Layout:** `<root>/workflows/<workflowId>/` holds `workflow.json`, a `.wf.lock/`, a journal — an
  **append-only sequence of `journal/<seq>.json` files** where each entry's resolution status is
  atomically finalized in place (`PENDING → COMMITTED | ABORTED`); no file's `seq` is ever reused — and
  `stages/<NNN-name>/{stage.json, attempts/<NNNN>.json}`. `workflowId` is
  `wf-<uuid>`; `stageId` is a zero-padded `<pipelineIndex>-<safe-name>` (`[A-Za-z0-9_.-]`, bounded; names
  with slashes or `..` are rejected).
- **Canonical vs projection:** the **append-only journal** and each **attempt record**
  (`attempts/<n>.json`) are authoritative. The attempt record is a **mutable canonical** record whose
  `stageState` advances only in allowed directions (`PENDING→SUBMITTING→RUNNING→UNVERIFIED→PASSED/…`).
  `stage.json` and `workflow.json` (`currentStage`, `completedStages`, `workflowState`) are **projections
  rebuildable from canonical records** (the workflow header is carried in the `workflow_created` journal
  entry, so `workflow.json` is fully rebuildable). If a projection disagrees with canonical, **canonical
  wins**. `job.json` remains authoritative only for its own activity's process/provider outcome.
- **Transition journal:** each stage-state change is journalled write-ahead — a new `journal/<seq>.json`
  file is appended with a `PENDING` intent → atomic canonical attempt update → the **same** journal file is
  atomically rewritten to `COMMITTED` in place → rebuild stage then workflow projection — all under the
  workflow lock. `seq` is the max journal filename + 1 (never a projection counter). Same-state re-apply is
  an idempotent no-op (no journal entry).
- **Projection rebuild is pipeline-driven and canonical-only:** `rebuildWorkflowProjection` walks the
  **pipeline from the `workflow_created` header** (the authoritative expected-stage set), re-derives each
  stage's `stage.json` from its canonical attempts, and computes `workflowState`/`currentStage`/
  `completedStages` from those — it never trusts an existing `stage.json`. A pipeline stage whose canonical
  attempt is missing is treated as incomplete, so a partial/lost stage set is **never** reported
  `SUCCEEDED`.
- **Crash reconciliation:** `reconcileWorkflow` scans **every** `PENDING` journal entry in ascending `seq`
  (not just the latest). Per entry: canonical already at `toState` (or a valid successor) → mark
  `COMMITTED`; canonical still at `fromState` (no side effect) → mark `ABORTED`/`NO_CANONICAL_CHANGE`;
  canonical contradicts the intent → **fail closed** (`WORKFLOW_RECONCILE_CONFLICT`, no arbitrary advance).
  Reconciliation is idempotent. (Recovery of "job created but jobId not yet linked" is **P3-C**, not P3-A —
  P3-A creates no external jobs.)
- **Atomicity:** every JSON write is unique-temp + `fsync` + atomic `rename` with best-effort temp cleanup;
  a partial JSON is never treated as valid (it raises `WORKFLOW_RECORD_CORRUPT`).
- **Workflow lock ordering (must hold going forward):** the `.wf.lock` is held **only** for storage reads,
  journal, attempt, and projection writes — **never** across a Gateway RPC, `startJob`, child spawn, or any
  external command, and this module **never** acquires a job lock. Fixed rule: **no job lock / startJob /
  RPC inside the workflow lock** (job lock, when P3-C introduces it, is always taken outside the wf lock).
- **Not yet in P3-A:** `workflow.*` tool, durable-job submission, stage↔job linkage, terminal verdict
  reconciliation, continuation calls, multi-stage advancement, approval/cancel/resume, preflight/provider
  cache/fallback, and any live rollout.

## P3-B workflow tool surface — storage interface only (v0.6)

`dist/workflow-service.js` (pure service layer) plus a thin adapter in `dist/index.js` expose a minimal
`workflow` tool with `action: start | status | list`. **It is a storage/creation interface only** — it
creates no durable job, runs no stage command, and does not link/advance/approve anything. `workflow.start`
merely materializes the P3-A skeleton (all stages `PENDING`); the workflow does **not** progress until P3-C.

- **Tool contract + feature flag `workflowEnabled` (default `false`):** the `workflow` tool is declared in
  `openclaw.plugin.json` `contracts.tools` and is **always registered**. This is required by the OpenClaw
  loader (verified against 2026.7.1-2): `registerTool` rejects any tool whose name is not in
  `contracts.tools`, and a *declared-but-unregistered* tool would throw `plugin tool runtime unavailable`
  from the manifest descriptor path — so a conditionally-registered tool is unsafe. Instead the flag gates
  **behavior**: while `workflowEnabled` is `false`, every `workflow` action fails closed with
  `WORKFLOW_DISABLED` and creates nothing (`durable_job`/P0/P1/P2 code paths are unaffected either way).
- **`workflow.start`** takes `name`, `worktree`, `pipeline` (`[{name, runnerType?, runnerProfile?}]`),
  optional `verificationProfile`/`forbiddenActions`/`requestId` — **no command/argv** (activity commands
  arrive in P3-C). Validation: bounded-safe name; `worktree` reuses the durable-job allowed-root/cwd
  containment; ≥1 stage; no duplicate/unsafe stage names; `runnerType`/`runnerProfile` must agree with the
  canonical profile (`model_agy→model`, `local_*`/`generic_local→local`). Stage `pipelineIndex` is assigned
  by position (`0,10,20,…`). Executable compatibility is deferred to P3-C submission.
- **Parent context + frozen route:** the trusted tool context is frozen into the workflow header
  (`parent = {agentId, sessionKey, sessionId, requesterOrigin, flowId: null}`); P3-B creates **no**
  TaskFlow. The delivery route is frozen exactly like `durable_job` — a trusted session resolves it from one
  `chat.history`, a context-free call freezes the configured owner's fixed `deliveryRoute`. Route
  resolution happens **before** `createWorkflow`, so a freeze failure leaves **no** workflow directory.
- **Start idempotency (`requestId`):** `creationKey = ownerKey + requestId`. Same owner + `requestId` +
  payload returns the existing workflow (no duplicate); same `requestId` + a different payload →
  `WORKFLOW_REQUEST_CONFLICT`; a different owner may reuse the same `requestId`. The scan-then-create is
  serialized by a storage-root creation lock (the per-workflow `.wf.lock` cannot cover a not-yet-existent
  workflow), so concurrent duplicate retries create exactly one workflow.
- **Ownership/authorization:** the creating owner is stored (`parent` + additive `ownerKey`). A trusted
  workflow is readable only from the same agentId **and** sessionKey (two factors, never a lone sessionKey
  or delivery route); a context-free workflow only from the configured owner resolved by its stored
  worktree. Knowing a `workflowId` alone never grants access; there is no admin bypass.
- **`workflow.status`** reconciles projections from canonical records before returning; **`workflow.list`**
  is owner-scoped (never exposes another owner's workflows), newest-`updatedAt` first, default limit 20 /
  max 100, optional `state` filter, and isolates a corrupt workflow per-item (hidden, never guessed). Public
  responses **exclude** the frozen `deliveryRoute`, `requesterOrigin`, `ownerKey`, journal internals, and
  lock/temp paths.
- **Not yet in P3-B:** durable-job creation, stage command execution, job↔stage linkage, terminal verdict,
  continuation, automatic advancement, approval/cancel/resume, preflight/provider cache/fallback, TaskFlow
  finish/fail, and any live rollout.

## P3-C job↔stage linkage + terminal verdict (v0.6)

`dist/workflow-activity.js` + `dist/workflow-reconciler.js` submit a workflow's **first** stage as a real
`durable_job` and reflect that job's authoritative process/provider outcome back onto the linked stage
attempt. **P3-C stops after the first stage** — it never auto-starts the next stage (that is P3-D).

- **Stage activity:** each pipeline stage carries an argv activity `{ argv, timeoutSeconds? }` (no shell, no
  env; `cwd` is forced to the workflow worktree). `workflow.start` validates it by **reusing** the P2-B
  `resolveRunnerMetadata` (runner/executable compatibility: `model_agy` requires the `agy` basename; a model
  executable can never be a local runner). An executable stage with no activity is rejected; an activity-less
  (P3-B-era) skeleton stays inspectable but fails closed with `WORKFLOW_ACTIVITY_MISSING` if submitted.
- **`workflow.start` now submits the first stage:** create skeleton → claim first stage `SUBMITTING` →
  release the workflow lock → **ensure** the durable job (created OUTSIDE the workflow lock) → reacquire →
  record `jobId` → `RUNNING`. The `.wf.lock` is never held across `startJob`/spawn/RPC.
- **Deterministic activity idempotency key** `wf:<workflowId>:stage:<stageId>:attempt:<n>` is frozen on the
  attempt and stamped on the linked job. A storage-root **job-creation lock** keyed by it makes
  reservation-before-spawn atomic, so the `SUBMITTING → create → link` window (and concurrent/duplicate
  starts) yields **at most one** job per key.
- **Job linkage metadata:** the linked job carries additive internal `workflowLink`
  `{ workflowId, stageId, attempt, activityIdempotencyKey }`. It is never accepted from the `durable_job`
  tool schema, never leaks through the standalone `publicJob` surface, and does not change the job's
  process/provider outcome. A job with no `workflowLink` is standalone.
- **Crash recovery** (folded into the existing single-flight reconciler tick — **no** second Gateway
  service): `SUBMITTING` with no job → ensure/submit once; `SUBMITTING` with an already-created job → attach
  the `jobId`, no duplicate; `RUNNING` linked job terminal → apply the stage verdict; every case is
  idempotent across ticks and never re-transitions a terminal stage.
- **Terminal → stage verdict** (reuses the P0 `verdict.js`/evaluator enums, invents none): non-terminal →
  `RUNNING`; `COMPLETED_UNVERIFIED` → `UNVERIFIED` (**process COMPLETED + provider OK is never auto-PASSED**;
  a semantic `PASSED` needs a verification contract not in P3-C); command/timeout/lost/cancelled →
  `FAILED`; retryable provider dependency (`RATE_LIMITED`/`BLOCKED_QUOTA`) → `BLOCKED_DEPENDENCY`;
  non-retryable provider/auth/context/internal → `FAILED`; any unmapped terminal shape fails closed to
  `FAILED`. The verdict writes the stage attempt only (`jobId`/`processState`/`providerState`/`jobOutcome`/
  `finishedAt`/`failureReason`) — never `job.json`.
- **Continuation separation:** a **standalone** job keeps its terminal notice **and** P1 continuation; a
  **workflow-linked** job keeps a single terminal notice (existing outbox) but its **standalone P1
  continuation is suppressed** — the workflow reconciler owns the stage. P3-C adds no workflow next-stage
  continuation (P3-D).
- **Feature flag:** while `workflowEnabled` is `false`, new workflow actions/submissions are refused, **but
  terminal reconciliation of an already-linked job still runs** (a stage never gets stuck because the flag
  was flipped). No next-stage auto-execution exists yet — after a stage settles, the workflow rests at the
  next `PENDING` stage until P3-D.

## P3-D linear multi-stage advancement (v0.6)

P3-D advances a **linear** pipeline: when a stage is canonical `PASSED`, the next `PENDING` stage is claimed
and submitted. When every stage is `PASSED` the workflow projection is `SUCCEEDED` and no further job runs.

- **Linear only.** Stages run strictly in `pipelineIndex` order; at most one frontier stage is active; a
  stage becomes eligible only when **all** predecessors are `PASSED`. No skipping, branching, DAG, fan-out,
  conditional selection, or dependency lists.
- **P3-D produces no `PASSED`.** The honesty rule from P3-C holds: `COMPLETED_UNVERIFIED → UNVERIFIED` and
  **UNVERIFIED never auto-advances**. Advancement requires a stage to be canonical `PASSED`, which only a
  (future) verification contract produces — there is **no verified-success producer yet**. `FAILED`,
  `BLOCKED_DEPENDENCY`, `ARTIFACT_MISSING`, `APPROVAL_REQUIRED`, `CANCELLED` all stop the pipeline.
- **Canonical frontier.** `computeFrontier`/`claimRunnableStage` (store, under the workflow lock) read the
  **canonical attempt records** in pipeline order (never the possibly-stale `workflow.json` projection),
  validate the linear invariants, and — for a runnable `PENDING` frontier — claim it `SUBMITTING`
  atomically (journal intent → attempt update → commit → projections). Fail-closed on a non-linear shape
  (`WORKFLOW_PIPELINE_INVARIANT`: a later stage active/advanced before its predecessor `PASSED`, or more
  than one active stage), a missing canonical attempt (`WORKFLOW_PIPELINE_INCOMPLETE`), an unknown state, or
  a runnable frontier with no activity (`WORKFLOW_ACTIVITY_MISSING`) — never guessed, skipped, or overwritten.
- **One primitive, one submission per tick.** `advanceWorkflowOnce` is shared by `workflow.start` (first
  stage) and the reconciler (next stages): claim the frontier under the lock, then `ensureLinkedJob` +
  attach + `RUNNING` **outside** the lock (never hold the workflow lock across `startJob`/spawn/RPC/job
  lock). A pass either settles the current active stage **or** submits exactly one newly-claimed stage —
  **at most one new job per pass**; a freshly-submitted job is not re-settled in the same pass, so stages
  never chain-execute within one tick.
- **Crash recovery + idempotency** (all idempotent across ticks, reusing the P3-C deterministic key so a
  stage yields at most one job): predecessor `PASSED` before the next claim; next stage `SUBMITTING` with no
  job / with an un-linked job / with a linked job; `RUNNING` with projections deleted; a stale predecessor
  projection (canonical wins); last stage `PASSED` with projections deleted (rebuild → `SUCCEEDED`);
  duplicate/concurrent reconcile and a `workflow.start`↔reconciler race all converge to one job per stage.
- **Feature flag.** `workflowEnabled=false` refuses a new next-stage claim/submission even when the
  predecessor is `PASSED`, yet still attaches/settles an already-active linked stage; flipping back to
  `true` submits exactly one eligible stage on the next pass.
- **Continuation/notice unchanged from P3-C.** Every workflow-linked stage job keeps at most one terminal
  notice via the existing outbox and no standalone P1 continuation; P3-D adds no workflow continuation /
  `chat.send`. Standalone `durable_job` is entirely unaffected.

## P3-E workflow control plane — approve / reject / cancel / resume (v0.6)

The `workflow` tool gains operator control actions over the current linear frontier. **Honesty rule:**
`approve` is a **manual** decision, not automatic verification — there is still **no verified-success
producer**.

- **`approve`** (frontier `UNVERIFIED`/`APPROVAL_REQUIRED` → `PASSED`) records a manual decision
  (`verificationSource = MANUAL_APPROVAL`, `decision.source = MANUAL`) and **preserves** the stage's
  `processState`/`providerState`/`jobOutcome` (e.g. `COMPLETED_UNVERIFIED`) — a `PASSED` stage's public
  status shows whether it was a manual approval vs an automatic contract. After approving, the P3-D
  advancement primitive submits the next stage (one stage); approving the last stage → `SUCCEEDED`.
- **`reject`** (frontier `UNVERIFIED`/`APPROVAL_REQUIRED` → `FAILED`) records the manual decision, preserves
  history, submits no next stage, and marks the workflow `FAILED`. Terminal manual decisions are not
  reversible (an approve-then-reject / reject-then-approve is rejected as stale/conflict).
- **`cancel`** records an additive `cancelRequest` on the frontier attempt (blocking any new submission),
  cancels the active durable job via the existing durable-job cancel primitive **outside** the workflow
  lock, and converges the stage to `CANCELLED` (job history preserved; a job that had already finished is
  marked `cancelledAfterTerminal`, never falsified to a fake success). A `PENDING` frontier cancels with no
  job. `cancel` is allowed **regardless of `workflowEnabled`** (an operator must be able to stop work);
  start/approve/reject/resume require `workflowEnabled: true`.
- **`resume`** is a **manual same-stage rerun** — it creates **attempt N+1** (`PENDING`, a fresh
  deterministic key, `resumeMode = MANUAL_RERUN`, `checkpointVerified = false`) and **preserves attempt N
  byte-for-byte**. Allowed from `UNVERIFIED`/`FAILED`/`BLOCKED_DEPENDENCY`/`ARTIFACT_MISSING`/`CANCELLED`
  (not `APPROVAL_REQUIRED` — use approve/reject). This is **not** a checkpoint-verified resume; full
  fingerprint/version-safe resume and provider fallback are **P3-F**.
- **Stale-control protection.** Every action re-validates, under the workflow lock, that `stageId`/`attempt`
  equal the current canonical frontier; a late click on a superseded stage/attempt fails
  `WORKFLOW_CONTROL_STALE` (no state change, no job).
- **Control idempotency.** Each write action is keyed by `wf:<id>:control:<action>:<requestId>` with a
  journaled control record: same owner + `requestId` + payload replays the stored result (no double effect,
  no duplicate job/cancel); a different payload/action → `WORKFLOW_CONTROL_REQUEST_CONFLICT`; concurrent
  duplicates are serialized to one. Owner/actor identity is derived from the workflow owner (never
  caller-injected) and stored only as an `ownerKeyHash` (no raw sessionKey/route).
- **Approval-request notice.** An `APPROVAL_REQUIRED` frontier makes the workflow `PAUSED` and sends **one**
  bounded approval notice via the frozen route (its own idempotency key, `SENDING`-lease /
  `DELIVERY_UNKNOWN` safety, retryable on error, no P1 continuation) — separate from the terminal notice; a
  failed notice never changes the stage state.
- **Public status** adds per-stage `verificationSource`, `decision`, `cancel`, and `resume` summaries; it
  never exposes `ownerKeyHash`, sessionKey, the frozen route, or control-journal internals.

## P3-F execution trust layer — preflight / fingerprint / provider cache / checkpoint-safe resume (v0.6)

Before a stage activity is submitted, its worktree and toolchain are validated and a canonical fingerprint
is captured; the same fingerprints gate provider fallback and safe resume. **Honesty rule (unchanged):**
none of `exit 0` / provider OK / preflight PASS / fingerprint match means semantic success —
`COMPLETED_UNVERIFIED → UNVERIFIED` still holds, and the ONLY `PASSED` producer is a manual approval.
`checkpointVerified = true` means **only** "the current **worktree** checkpoint exactly equals the recorded
baseline, and the **selected** toolchain (which for a `PROVIDER_FALLBACK` attempt may be a *different*
toolchain from the source) passed its own execution preflight." It does **not** mean the fallback runs the
same toolchain as the source, and it says nothing about whether the result is correct, complete, secure, or
reproducible on another provider.

- **Canonical Git worktree fingerprint** (`dist/workflow-fingerprint.js`, pure, argv-only, bounded timeout/
  output, fixed locale, never under the workflow lock): `HEAD` + branch/detached + staged/unstaged tracked
  diff hashes + a bounded untracked content hash (symlink target hashed; a socket/fifo/device → `INCOMPLETE`;
  over the file/byte bounds → `INCOMPLETE`). **Ignored files are out of scope.** It stores only hashes — never
  raw diffs, untracked contents, file names, or realpaths in the public projection. A non-Git worktree is
  `UNAVAILABLE` (execution still proceeds, but checkpoint-verified fallback/resume are disabled for it); a
  missing worktree or an `INCOMPLETE` Git worktree fails the stage `ARTIFACT_MISSING`. **An `INCOMPLETE`
  fingerprint never enables automatic fallback or safe resume.**
- **Toolchain fingerprint**: the executable is resolved exactly as spawn would (PATH/relative), its content
  is streamed to a SHA-256 with size; a missing/non-regular executable fails closed
  (`WORKFLOW_TOOLCHAIN_MISSING`/`_UNSUPPORTED`). The public status never exposes the absolute executable path.
- **Preflight** runs OUTSIDE the workflow lock (serialized by a per-attempt preflight lock) between the
  `SUBMITTING` claim and job creation: worktree → toolchain → (model only) provider capability. **PASS** →
  capture `checkpoint.before` + ensure the linked job; **BLOCKED** → `BLOCKED_DEPENDENCY`; **ARTIFACT_MISSING**
  → `ARTIFACT_MISSING`; **FAILED** → `FAILED` — all without a job. `UNKNOWN` provider capability proceeds
  (the authoritative job result governs) but never triggers automatic fallback.
- **Provider capability cache** (`dist/provider-cache.js`, pure, ADVISORY): keyed by
  runnerProfile+runnerType+**toolchain hash**+probe version+**non-secret config fingerprint** (a changed
  executable/config is an automatic miss). READY uses the normal TTL, BLOCKED a short negative TTL, UNKNOWN a
  very short (or no) TTL; concurrent probes run **once**. The readiness probe is **injected and must not
  consume model quota / poll** — there is no non-quota `agy` readiness seam, so the default probe returns
  `UNKNOWN` (never auto-fallback on `UNKNOWN`). No token/API key/env is stored. The actual job outcome is
  always authoritative over the cache.
- **Declared ordered fallback candidates** (stage `fallbacks[]`, validated + frozen at start; each a valid
  P2-B runner/activity; no duplicate `candidateId`; bounded). The primary is candidate 0; execution only
  proceeds in declared order (no skip/cycle).
- **Checkpoint-verified automatic fallback**: only for a terminal `FAILED_PROVIDER` with a fallback-eligible
  provider state (`BLOCKED_QUOTA`/`RATE_LIMITED`/`AUTH_FAILED`) **and** an UNCHANGED, COMPLETE pre-execution
  checkpoint (the failed provider left the worktree untouched) **and** a declared next candidate → attempt
  N+1 with `resumeMode = PROVIDER_FALLBACK`, `checkpointVerified = true`, `selectedCandidateIndex`. A
  changed/incomplete checkpoint fails closed to `APPROVAL_REQUIRED` (`WORKFLOW_CHECKPOINT_MISMATCH`, no
  auto-fallback, no worktree rollback). `FAILED_COMMAND`/`TIMED_OUT`/`LOST`/`CANCELLED`/`COMPLETED_UNVERIFIED`
  and manual reject never auto-fallback. At most one fallback attempt + one job per reconcile pass (no
  chained candidates in one tick); the failed attempt is preserved.
- **Atomic fallback decision + feature-flag gating**: the source verdict and a durable `fallbackIntent` are
  written in ONE canonical attempt update (a crash can never leave the source terminal *without* a recorded
  fallback obligation); the intent is then consumed exactly once to create attempt N+1. Consuming an intent is
  a *submission decision*, so it is gated on `workflowEnabled`: `reconcileWorkflow` (storage/journal/projection
  recovery) **preserves** a PENDING intent but never creates the N+1, and only `advanceWorkflowOnce` consumes
  it when `workflowEnabled: true`. A disabled workflow still reconciles linked-job terminals, records
  `checkpoint.after`, repairs projections/journals, and converges cancels — but grows **no** new fallback
  attempt, runs **no** preflight, and submits **no** job.
- **Terminal post-run checkpoint** (`checkpoint.after`) is captured read-only outside the workflow lock; a
  capture failure never loses the terminal job outcome and never silently overwrites a recorded checkpoint.
- **Resume `checkpointPolicy`**: `manual_rerun` (default) keeps the P3-E behavior (`MANUAL_RERUN`,
  `checkpointVerified = false`); `require_match` verifies the current worktree fingerprint EXACTLY equals the
  source attempt's `checkpoint.after` (COMPLETE) before creating a `CHECKPOINT_RERUN` attempt, and the
  submit-time preflight re-verifies the frozen hash (`WORKFLOW_CHECKPOINT_CHANGED` if it drifted). A
  mismatch/absent checkpoint fails closed (`WORKFLOW_CHECKPOINT_MISMATCH`/`_UNAVAILABLE`) with no new
  attempt/job. This is **not** a fingerprint-safe/version-safe resume with provider fallback recovery — that
  is a later step; evidence assurance is P4.
- **Legacy**: pre-P3-F attempts (no fingerprints) status/list + terminal reconcile normally; `require_match`
  → `CHECKPOINT_UNAVAILABLE`, `manual_rerun` unchanged; a stage with no `fallbacks` runs primary-only (no
  auto-fallback). Config adds bounded `workflowProviderCacheTtlMs` / `...NegativeCacheTtlMs` /
  `...UnknownCacheTtlMs` / `workflowPreflightTimeoutMs` / `workflowFingerprintMaxFiles` / `...MaxBytes`.

## P3-G supervisor audit gate + Slack audit summary (v0.6)

A stage worker's final narrative or Evidence Pack is **not** the audit basis. When a stage reaches `UNVERIFIED`
and it declares `audit.mode = supervisor` (and `workflowAuditEnabled`), the reconciler wakes the **existing
persistent Supervisor ACP session** (reusing the P1 `chat.send(deliver:false)` continuation seam) as an
independent **Audit Gate**. The Supervisor inspects the CANONICAL sources directly — the workflow attempt,
authoritative `job.json`, stdout/stderr/heartbeat logs, the Git repository/worktree, the P3-F
checkpoint/fingerprint, and the declared test/build artifacts — and returns a structured decision by calling
`workflow.audit_decide`. Slack receives only a bounded **display-only** summary of the checks the Gate itself
verified and the verdict — never the raw tool trace or the full audit conversation. **Slack is never the audit
source of truth.**

- **PASS producers are now exactly two: `MANUAL_APPROVAL` and `INDEPENDENT_AUDIT`.** `exit 0` / provider OK /
  preflight PASS / checkpoint match / toolchain match / a worker final report are still **not** a PASSED
  source. A PASS keeps `verificationSource = INDEPENDENT_AUDIT`, `decision.source = AUDIT_GATE`, and does
  **not** fabricate `processState`/`providerState`/`jobOutcome` — a `COMPLETED_UNVERIFIED` job stays
  `COMPLETED_UNVERIFIED` under a PASSED stage. jobOutcome and the stage verdict are different layers.
- **Verdict mapping**: `PASS → PASSED` (then the P3-D advancement submits at most the next stage);
  `FAIL → FAILED` (no next stage); `BLOCKED`/`INCONCLUSIVE → APPROVAL_REQUIRED` (the P3-E approval outbox asks
  a human — the Gate never guesses PASS when unsure).
- **Required-check sufficiency**: a `PASS` is refused (`WORKFLOW_AUDIT_INCOMPLETE`) unless EVERY declared
  `requiredCheck` is present, `PASS`, and at a sufficient `verificationLevel`
  (`REEXECUTED`/`LOG_VERIFIED`/`ARTIFACT_VERIFIED`). `WORKER_REPORTED`/`INFERRED`/`NOT_CHECKED`/missing never
  carry a stage to PASSED. (`verificationLevel` is a minimal decision-annotation field, not a full
  EvidenceItem/assurance subsystem.)
- **Trusted auditor only**: `audit_decide` is restricted to the workflow's session-bound Supervisor context
  (both `agentId` and the exact `sessionKey`). A worker, another session, or a context-free owner is denied
  (`WORKFLOW_AUDIT_ACCESS_DENIED`). Auditor identity / target / contract / outcome are derived by the harness,
  never accepted from the caller.
- **Authoritative job re-binding (mandatory for PASS)**: at request time AND (under the workflow lock) at PASS
  time the Audit Gate re-reads the **authoritative `job.json`** — not just the canonical attempt copy — and
  verifies its `workflowLink` (workflowId/stageId/attempt/activityIdempotencyKey), `jobOutcome`,
  `processState`, `providerState`, and an outcome-summary hash (the hash is an additive detector, not a
  replacement for the field-by-field checks). A PASS **never** proceeds without this re-read: a missing readJob
  seam, a missing job row, a read error, a mis-link, or a diverged outcome all fail-close a PASS
  (`WORKFLOW_AUDIT_TARGET_CONTRADICTION`) → `APPROVAL_REQUIRED`; no canonical record is auto-corrected.
- **Stale-target protection**: under the workflow lock the decision re-verifies the frontier attempt, the
  `auditRequestId`, `jobId`, `activityIdempotencyKey`, `jobOutcome`, the `checkpoint.after`
  **status + complete + hash** (not the hash alone), the frozen audit-contract hash, and that the audit is
  still `REQUESTED`/`RUNNING` with no cancel — any drift is `WORKFLOW_AUDIT_STALE` with no state change. A
  `PASS` requires a **COMPLETE checkpoint on both sides** — the frozen target AND the freshly captured
  fingerprint — with an exact hash match; `null === null` is never a match. If either side is missing /
  `INCOMPLETE` / `UNAVAILABLE` (or the capture errors) the PASS is downgraded to `APPROVAL_REQUIRED`
  (`WORKFLOW_AUDIT_CHECKPOINT_UNAVAILABLE`); if both are COMPLETE but the hashes differ (the **worktree changed
  during review**) it is `WORKFLOW_AUDIT_CHECKPOINT_CHANGED`. An audit request is not even created for a
  non-COMPLETE `checkpoint.after` (it escalates to human at request time). A worktree change is **detected but
  not attributed** to any actor — the Gate does not claim the auditor mutated the target (a before/after
  fingerprint proves change, not authorship).
  No arbitrary rollback. Each decision is serialized by a **dedicated per-attempt audit-decision lock**
  (separate from the workflow lock and the activity/job-creation lock). Lock order is strict: acquire the
  audit-decision lock FIRST, capture the Git worktree fingerprint while holding ONLY that lock (never the
  workflow lock — no Git under the workflow lock), then `applyAuditDecision` takes the workflow lock LAST and
  re-validates the target + authoritative job. So a worktree change that lands while a decision waits for the
  lock is seen by the post-lock capture, and concurrent `audit_decide` calls yield exactly one decision, one
  next-stage job, and one Slack summary.
- **Text redaction**: `summary` and `checks[].detail` are sanitized (absolute paths, home dirs, bearer/
  authorization, token/secret/session/owner/channel key=values, `sk-*` keys, Slack channel ids) **before** they
  are stored canonically or shown in public status / Slack. Only sanitized text is persisted; a summary that is
  empty after trimming/redaction is rejected.
- **Separate outboxes**: the audit continuation and the Slack audit summary are separate at-most-once outboxes
  (their own idempotency keys), reusing the delivery/continuation lease + `DELIVERY_UNKNOWN` posture — an
  ambiguous send is parked, never blind-resent, and a Slack failure never reverts the canonical decision. The
  summary sweep scans **every** decided attempt (not just the latest), so a resume/fallback that bumps the
  current attempt never drops an earlier decided attempt's one-time summary.
- **Feature flag** `workflowAuditEnabled` (default `false`): while off, an `UNVERIFIED` stage keeps the P3-F
  manual-approval path and no audit request/continuation is created; an already-requested audit may still
  converge via `audit_decide`. `audit.mode` defaults to `none` (unchanged behavior). When the audit **cannot
  run** — no persistent Supervisor session, no `agentId`, or no gateway — a never-requested stage fails
  **closed** to `APPROVAL_REQUIRED` (`WORKFLOW_AUDIT_UNAVAILABLE`, surfaced in public status) and a human is
  asked; it never guesses PASS and never lingers `RUNNING`. An **already-requested** stage converges by its
  continuation-outbox state: a delivered (`SENT`), ambiguous (`DELIVERY_UNKNOWN`), or **in-flight** (`SENDING`)
  continuation is left `PAUSED` so a late `audit_decide` can still land (no duplicate human escalation). A
  `SENDING` is **never** treated as "definitely not sent": a fresh-lease `SENDING` is left in flight, and a
  stale-lease `SENDING` is atomically converged to `DELIVERY_UNKNOWN` (never blind-resent). Only a **proven
  not-sent** state — no continuation record, or `PENDING` — escalates to human. The audit request + continuation
  records are preserved as historical metadata, never deleted.
- **Not a real ACP model audit yet**: the audit continuation seam and the full decision lifecycle are exercised
  by a **deterministic integration harness** (a fake gateway captures the continuation and the decision is
  submitted via the real `audit_decide`); a genuine independent Supervisor ACP model audit has **not** been run
  here (no model quota consumed). The production wiring is real; the model turn is simulated.
- **Not in this step** (deferred to a later P3-F.1/P3-G.1 / P4): `workflow.get_evidence`, an Audit Receipt /
  Evidence Pack, tool-trace normalization, worker-conversation parsing, a full EvidenceItem/assurance
  subsystem, long-term trace retention/redaction, and a forensic UI. The auditor reads the existing
  source-of-truth; the raw tool trace is not the default audit input (the Gate opens raw logs only on an
  anomaly — a worker/canonical mismatch, an unexpected file change, a suspected failure or ref mutation).

## Architecture

- `dist/worker.js` — unchanged detached worker: argv-only (no shell), writes
  `stdout.log`/`stderr.log`, enforces `timeoutSeconds`, records the terminal
  state (`SUCCEEDED|FAILED|TIMED_OUT|CANCELLED`).
- `dist/job-store.js` — unchanged JSON job ledger (per-job `job.json`, atomic
  write + directory lock). **No SQLite.**
- `dist/delivery-outbox.js` — the persistent outbox state machine, deterministic
  terminal message, and send-result classification (pure, fully unit-tested).
- `dist/completion-turn.js` — `freezeDeliveryRoute()` / `classifyRouteKind()`
  plus the legacy ACP-wakeup helpers.
- `dist/core.js` — orchestration (`startJob`, `reconcileOnce`,
  `processDeliveryOutbox`, …) with **no plugin-SDK dependency**, so it is unit
  testable with injected gateway / worker / flow seams.
- `dist/index.js` — thin OpenClaw plugin entry: config, the `durable_job` tool,
  and the reconciler service.

## Delivery route is frozen at creation

At `job.start`, using the caller's current `sessionKey`/`sessionId`, the plugin
reads `chat.history` **exactly once** and freezes the originating route into the
job record. Every later stage (delivery, retry, Gateway-restart recovery) uses
only the frozen `job.deliveryRoute` — it never re-reads the session, the session
key, or `chat.history`, and never re-resolves the route.

The route kind is decided from **trusted session metadata** using **positive
evidence**, never from a model argument:

- `routeKind: "channel_root"` — **limited to the only proven shape**:
  provider `slack` **and** `chatType: "channel"` **and** a target, with no thread
  id. A read-only probe of the lab Slack sessions confirmed a Slack channel-root
  origin carries exactly `provider/surface: "slack"`, `chatType: "channel"`, a
  `to`/`nativeChannelId`, and no thread id. Delivered to the channel root.
- `routeKind: "unknown"` — no channel/target; a **non-slack provider**; or a
  **chatType other than "channel"** (e.g. group/space/dm). `job.start` is
  **rejected** with `DELIVERY_ROUTE_UNAVAILABLE` (before any job, flow, or worker
  is created); these shapes are not yet proven.
- `routeKind: "thread"` — **preparatory code only, not a reachable feature
  today.** It requires an explicit thread id in the resolved route, but OpenClaw
  does not currently surface one (see below), so no session ever classifies as
  `thread`. It is retained for the day OpenClaw provides a thread id.

> **Thread requests are not distinguishable, and the plugin does not fail-close
> them.** A read-only probe (a real thread reply in `#infra-agent`) showed that
> OpenClaw's `chat.history.sessionInfo` reports the thread reply exactly like a
> channel-root message — same `chatType: "channel"`, no thread id anywhere
> (origin, `deliveryContext`, or message provenance) — and reuses the same
> channel session key. So a job started from inside a Slack thread is classified
> `channel_root` and its completion is delivered to the **channel root, not the
> thread**. This is not a plugin misclassification; OpenClaw provides no thread
> routing for this binding (its own replies also go to the channel).

Frozen fields: `routeKind`, `channel`, `to` (channelId / delivery target),
`threadId`, `accountId`, `agentId`, `chatType`, `routeResolvedAt`,
`routeResolutionSource`. The send payload derives its address from these only —
**`sessionKey` is never included in the send payload**; the same frozen route
produces the same address even if the session key changes, is dropped, or
expires.

## Support scope (this PoC)

- **Supported:** Slack **channel-root** durable-job requests and **channel-root**
  completion delivery.
- **Not supported:** returning a completion notice to the **originating Slack
  thread**. OpenClaw does not expose a thread id, so a request made from inside a
  Slack thread cannot be technically identified and its completion may be
  delivered to the channel root.
- The plugin does **not** claim to fail-close thread requests — such a request is
  simply indistinguishable from a channel-root request.
- **Operationally:** run `#infra-agent` durable-job requests **from the channel
  root** (not from a thread) so the completion lands where expected.

Original-thread return is tracked as a follow-up (see below).

## Delivery state machine

```
PENDING ─claim(lease)─▶ SENDING ─▶ DELIVERED                     (provider messageId returned)
                                └▶ GATEWAY_ACCEPTED_UNCONFIRMED   (runId only, no messageId)
                                └▶ DELIVERY_UNKNOWN               (send result not confirmable)
SENDING(send threw) ─▶ FAILED_RETRYABLE ─backoff─▶ … ─▶ FAILED_FINAL (attempts exhausted)
SENDING(lease expired) ─▶ DELIVERY_UNKNOWN                        (never a blind resend)
```

- The message is deterministic (state, name, `job_id`, `delivery_id`, exit code,
  error) and carries the `job_id` and `delivery_id` so an operator — or a future
  Slack-history dedupe check — can recognise a resend.
- **`SENDING` uses a lease** (`sendingStartedAt`, `sendingLeaseUntil`,
  `sendingAttemptId`). Only a *lease-expired* `SENDING` is treated as stale; a
  fresh `SENDING` is a concurrent in-flight attempt and is skipped.
- The reconciler additionally holds an **in-process single-flight guard** so a
  slow send (up to the 10s send timeout, longer than the 2s poll) cannot let the
  next tick run concurrently. Even without it, the lease + claim guarantee a
  single send attempt per job.

## Guarantee level (read this precisely)

> Persistent best-effort delivery with retries for confirmed failures and
> explicit parking of ambiguous outcomes. Not exactly-once and not guaranteed
> at-least-once Slack delivery.

- A **deterministic idempotency key** per terminal job
  (`durable-job:<id>:terminal:delivery`) with best-effort duplicate suppression.
- A send whose result cannot be confirmed is parked in `DELIVERY_UNKNOWN` and is
  **not** auto-resent (that would risk a duplicate). A genuine ambiguous crash
  (process dies after Slack accepted the message but before the result is
  persisted) can therefore **under-deliver**.
- `GATEWAY_ACCEPTED_UNCONFIRMED` (Gateway returned a `runId` but no provider
  `messageId`) is a **distinct, operator-visible** state — it is *not* hidden as
  "delivered". Whether Slack sends always return a `messageId` is a **live-smoke
  verification item**.

### Operating DELIVERY_UNKNOWN / GATEWAY_ACCEPTED_UNCONFIRMED

Both are surfaced by `durable_job status`. There is **no automatic resend** — an
ambiguous outcome is a manual, operator-verified case. An operator inspects the
Slack thread (the message carries `job_id` / `delivery_id`) and decides.
Automatic resend is intentionally **out of scope** for this PoC; it is left as a
separate feature to be designed only alongside a Slack-history-based dedupe check
or an explicit operator command.

## ACP wakeup is optional

The legacy ACP-wakeup completion path (rich model report) is **disabled by
default** (`completionAcpWakeup: false`). The deterministic outbox is the
primary, always-on path. TaskFlow settlement runs regardless.

## Config

| key | default | meaning |
|---|---|---|
| `completionAcpWakeup` | `false` | also wake the ACP session for a rich report |
| `deliveryMaxAttempts` | `8` | send retries before `FAILED_FINAL` |
| `sendLeaseMs` | `30000` | in-flight send lease; only an expired lease is stale |

Plus the existing `owners`, `allowedRoots`, `maxConcurrent`, `pollIntervalMs`,
`queuedGraceMs`, `openclawCommand` (use an absolute path), `stateSubdir`.

## Legacy (v0.1.x) job upgrade limit — fail-closed

There is **no automatic migration**. On service start the plugin runs
`detectLegacyActiveJobs` for jobs that are `QUEUED`/`RUNNING` **and** have no
`deliveryRoute` and no `delivery` outbox. If any exist the upgrade is
**fail-closed**:

- the v0.2 **reconciler is not started**;
- new `durable_job start` is **rejected** with `LEGACY_ACTIVE_JOBS_PRESENT`
  (the error lists each blocked job's id, state, and createdAt; the same detail
  is logged at error level);
- `list`, `status`, and `cancel` **remain available** so operators can drain the
  jobs;
- the Gateway is not killed and plugin registration does not fail;
- terminal legacy jobs are left unchanged; no auto-migration.

Once every active legacy job reaches a terminal state, restarting the service or
Gateway re-evaluates the gate and activates normally.

**Read-only preflight** (run before installing):

```
node dist/preflight.js --state-dir <lab-state-dir>   # or --root-dir <durable-jobs-dir>
```

Exit `0` when there are no active legacy jobs, `1` when any are present (with the
blocked job ids/state/createdAt on stderr), `2` on usage/error.

## Verification

`npm test` (`node --test`) runs pure unit tests plus an integration suite that
injects the gateway / worker / flow seams:

- `chat.history` is called exactly once, at creation;
- completion delivery calls **only** `send` — never `chat.history`, `chat.send`,
  or a session wakeup — and uses the stored frozen route verbatim;
- a channel-root route delivers without a `threadId`;
- an unknown route rejects `job.start` **before** any worker spawn;
- `messageId` → `DELIVERED`; `runId`-only → `GATEWAY_ACCEPTED_UNCONFIRMED`;
- two overlapping reconciles perform a single send and the second never marks
  the delivery stale/unknown;
- a `SENDING` record becomes `DELIVERY_UNKNOWN` only after its lease expires;
- the legacy-active-job preflight.

## Live install procedure (deferred — requires explicit approval)

This PoC stops at source + unit tests. Installing touches the running lab
Gateway, which hosts the live infra-agent session, so it is a separate,
approved step:

1. **Preflight (read-only, no active legacy jobs):**
   ```
   node dist/preflight.js --state-dir ~/.openclaw-lab
   ```
   Must exit `0`. If it exits `1`, drain the listed active v0.1.x jobs first.
2. Bump the version, `npm pack`, install the tarball into
   `~/.openclaw-lab/extensions/durable-jobs`.
3. Restart the lab Gateway.
4. Live smoke: confirm channel-root frozen-route delivery, and that Slack `send`
   returns a provider `messageId` (i.e. real `DELIVERED`, not
   `GATEWAY_ACCEPTED_UNCONFIRMED`); exercise quiet success, nonzero-exit
   `FAILED`, worker-death `LOST`, Gateway-restart survival, and single completion
   under repeated reconciliation. (Thread-return is out of scope — see Support
   scope.)

### Rollback

The install is a directory swap plus a restart, so rollback is symmetric:

1. Back up the current install first:
   `cp -R ~/.openclaw-lab/extensions/durable-jobs ~/.openclaw-lab/backups/durable-jobs.pre-0.2-$(date +%Y%m%dT%H%M)`.
2. To roll back, restore the v0.1.11 install (the byte-identical baseline is at
   `~/.codex/.chatgpt-projects/…/openclaw-durable-jobs`, or the backup above)
   over `~/.openclaw-lab/extensions/durable-jobs`.
3. Restart the lab Gateway.
4. Job records are forward-compatible: v0.1.11 ignores the added
   `deliveryRoute`/`delivery` fields, and v0.2 leaves terminal v0.1 jobs
   untouched. No state migration or teardown is required either way.

## Follow-up (not in this PoC): original-thread return

Returning a completion notice to the **originating Slack thread** requires an
OpenClaw platform change, not a plugin change: the **Slack inbound adapter must
preserve the Slack event's `thread_ts`** into the trusted tool context and/or
`chat.history.sessionInfo`. Once a thread id is exposed there, `durable_job`
would freeze it as the route at creation (the `routeKind: "thread"` code path is
already prepared) and deliver completions back to the thread.

## Environment / ACP bridge setup

The lab-specific ACP / Antigravity bridge wiring, trusted-workspace settings,
and safety model from v0.1 still apply; see `deploy/` for the checked-in
snapshots used by this machine.
