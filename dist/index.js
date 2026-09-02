import path from "node:path";
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { ensureStore, listJobs, readJob } from "./job-store.js";
import { resolveOwnerContext } from "./ownership.js";
import { runWorkflowAction } from "./workflow-service.js";
import {
  authorizeJobAccess,
  cancelJob,
  detectLegacyActiveJobs,
  inspectJob,
  makeFlowCreator,
  makeGatewayCall,
  makeLegacyBlockError,
  publicJob,
  readConfig,
  reconcileOnce,
  resolveListFilter,
  settleFlowWithApi,
  spawnWorkerProcess,
  startJob,
} from "./core.js";

// Thin OpenClaw plugin entry. All orchestration lives in ./core.js, which has
// no plugin-SDK dependency and is unit-testable with injected gateway/worker/
// flow seams.

function textResult(value, isError = false) {
  return {
    content: [{ type: "text", text: typeof value === "string" ? value : JSON.stringify(value, null, 2) }],
    ...(isError ? { isError: true } : {}),
  };
}

const parameters = {
  type: "object",
  additionalProperties: false,
  required: ["action"],
  properties: {
    action: { type: "string", enum: ["start", "status", "list", "cancel"] },
    jobId: { type: "string", minLength: 8 },
    name: { type: "string", minLength: 1, maxLength: 200 },
    command: {
      type: "array",
      items: { type: "string", minLength: 1 },
      minItems: 1,
      maxItems: 128,
    },
    cwd: { type: "string", minLength: 1 },
    nextAction: { type: "string", minLength: 1, maxLength: 2000 },
    timeoutSeconds: { type: "integer", minimum: 0, maximum: 604800 },
    // P2-B runner metadata (optional; absent → inferred from the command). resultProtocol/providerState
    // are NEVER caller-supplied — they are derived from the validated runner profile.
    runnerType: { type: "string", enum: ["model", "local"] },
    runnerProfile: {
      type: "string",
      enum: ["model_agy", "local_test", "local_build", "local_docker", "generic_local"],
    },
  },
};

// P3-B workflow tool schema. Broad optional properties + runtime action validation (no oneOf assumption).
// Server-controlled fields (workflowState/currentStage/completedStages/deliveryRoute/parent/ownerKey/
// stageState/jobId/processState/providerState/jobOutcome) are absent → additionalProperties:false rejects
// any caller attempt to inject them. workflowId is a status/lookup key only; start always mints its own id.
const workflowParameters = {
  type: "object",
  additionalProperties: false,
  required: ["action"],
  properties: {
    action: { type: "string", enum: ["start", "status", "list", "approve", "reject", "cancel", "resume", "audit_decide"] },
    // start
    name: { type: "string", minLength: 1, maxLength: 120 },
    worktree: { type: "string", minLength: 1 },
    pipeline: {
      type: "array",
      minItems: 1,
      maxItems: 64,
      items: {
        type: "object",
        additionalProperties: false,
        required: ["name"],
        properties: {
          name: { type: "string", minLength: 1, maxLength: 64 },
          runnerType: { type: "string", enum: ["model", "local"] },
          runnerProfile: {
            type: "string",
            enum: ["model_agy", "local_test", "local_build", "local_docker", "generic_local"],
          },
          // P3-C: argv-only activity for the stage (no shell, no cwd/env — cwd is forced to the worktree).
          activity: {
            type: "object",
            additionalProperties: false,
            required: ["argv"],
            properties: {
              argv: { type: "array", items: { type: "string", minLength: 1 }, minItems: 1, maxItems: 128 },
              timeoutSeconds: { type: "integer", minimum: 0, maximum: 604800 },
            },
          },
          // P3-F: ordered declared fallback candidates (each validated like the primary).
          fallbacks: {
            type: "array",
            maxItems: 8,
            items: {
              type: "object",
              additionalProperties: false,
              required: ["activity"],
              properties: {
                runnerType: { type: "string", enum: ["model", "local"] },
                runnerProfile: { type: "string", enum: ["model_agy", "local_test", "local_build", "local_docker", "generic_local"] },
                activity: {
                  type: "object",
                  additionalProperties: false,
                  required: ["argv"],
                  properties: {
                    argv: { type: "array", items: { type: "string", minLength: 1 }, minItems: 1, maxItems: 128 },
                    timeoutSeconds: { type: "integer", minimum: 0, maximum: 604800 },
                  },
                },
              },
            },
          },
          // P3-G: optional per-stage audit policy (default mode "none"). Frozen at start; part of the approved contract.
          audit: {
            type: "object",
            additionalProperties: false,
            required: ["mode"],
            properties: {
              mode: { type: "string", enum: ["none", "supervisor"] },
              instruction: { type: "string", minLength: 1, maxLength: 4000 },
              requiredChecks: {
                type: "array", minItems: 1, maxItems: 6,
                items: { type: "string", enum: ["scope", "repository_invariants", "declared_tests", "job_outcome_consistency", "checkpoint_consistency", "artifact_presence"] },
              },
            },
          },
        },
      },
    },
    verificationProfile: { type: "string", minLength: 1, maxLength: 64 },
    forbiddenActions: { type: "array", maxItems: 32, items: { type: "string", minLength: 1, maxLength: 64 } },
    requestId: { type: "string", minLength: 1, maxLength: 128 },
    // status / list
    workflowId: { type: "string", minLength: 8 },
    state: {
      type: "string",
      enum: ["RUNNING", "PAUSED", "BLOCKED", "SUCCEEDED", "FAILED", "CANCELLED"],
    },
    limit: { type: "integer", minimum: 1, maximum: 100 },
    cwd: { type: "string", minLength: 1 },
    // P3-E control (approve / reject / cancel / resume): target the current frontier stage/attempt.
    // Server-controlled decision/actor/state fields are NOT accepted (additionalProperties:false).
    stageId: { type: "string", minLength: 1, maxLength: 68 },
    attempt: { type: "integer", minimum: 1 },
    reason: { type: "string", minLength: 1, maxLength: 2000 },
    // P3-F resume: manual_rerun (default) or checkpoint-verified require_match.
    checkpointPolicy: { type: "string", enum: ["manual_rerun", "require_match"] },
    // P3-G audit_decide (trusted Supervisor auditor only). Server-derived identity/target/contract are NOT
    // accepted here (additionalProperties:false) — only the verdict + bounded summary + per-check results.
    auditRequestId: { type: "string", minLength: 1, maxLength: 128 },
    verdict: { type: "string", enum: ["PASS", "FAIL", "BLOCKED", "INCONCLUSIVE"] },
    summary: { type: "string", minLength: 1, maxLength: 1200 },
    checks: {
      type: "array", maxItems: 12,
      items: {
        type: "object", additionalProperties: false, required: ["check", "result"],
        properties: {
          check: { type: "string", enum: ["scope", "repository_invariants", "declared_tests", "job_outcome_consistency", "checkpoint_consistency", "artifact_presence"] },
          result: { type: "string", enum: ["PASS", "FAIL", "NOT_CHECKED"] },
          verificationLevel: { type: "string", enum: ["REEXECUTED", "LOG_VERIFIED", "ARTIFACT_VERIFIED", "WORKER_REPORTED", "INFERRED"] },
          detail: { type: "string", maxLength: 500 },
        },
      },
    },
  },
};

export default definePluginEntry({
  id: "durable-jobs",
  name: "Durable Jobs",
  description:
    "Durable detached jobs with Task Flow state and a persistent, frozen-route Slack delivery outbox",
  register(api) {
    const config = readConfig(api);
    let rootDir;
    let stateDir;
    let interval;
    let reconciling = false; // in-process single-flight guard for overlapping ticks
    // Fail-closed legacy gate, decided once at service start. While set, the
    // reconciler does not run and new starts are rejected; only a restart
    // (after the legacy jobs reach terminal) clears it.
    let legacyBlock = null;
    const resolveRoot = () => {
      if (rootDir) return rootDir;
      stateDir = api.runtime.state.resolveStateDir();
      rootDir = path.join(stateDir, config.stateSubdir);
      return rootDir;
    };

    api.registerService({
      id: "durable-jobs-reconciler",
      async start(ctx) {
        stateDir = ctx.stateDir;
        rootDir = path.join(ctx.stateDir, config.stateSubdir);
        await ensureStore(rootDir);
        // Legacy upgrade guard (fail-closed): if any pre-outbox job is still
        // active, do NOT start the v0.2 reconciler and block new starts. The
        // plugin stays registered and list/status/cancel remain available so
        // operators can drain the jobs; no auto-migration.
        const legacy = await detectLegacyActiveJobs(rootDir);
        if (legacy.length > 0) {
          legacyBlock = legacy;
          ctx.logger.error?.(
            `durable-jobs: BLOCKED — ${legacy.length} active legacy job(s) without a frozen route/outbox. ` +
              `Reconciler NOT started and new durable_job starts are rejected until these reach a terminal ` +
              `state and the service/Gateway is restarted. Blocked jobs: ` +
              legacy
                .map((job) => `[${job.id} state=${job.state} createdAt=${job.createdAt ?? "unknown"}]`)
                .join(" "),
          );
          return; // do not start the reconcile loop
        }
        const deps = {
          rootDir,
          config,
          gatewayCall: makeGatewayCall(config.openclawCommand, stateDir),
          settleFlow: (job) => settleFlowWithApi(api, job),
          logger: ctx.logger,
          // P3-C: seams the folded workflow-reconciliation pass uses to submit a linked stage's durable job
          // (createFlow is unused for linked jobs — they carry no session — but passed for completeness).
          createFlow: makeFlowCreator(api),
          spawnWorker: spawnWorkerProcess,
        };
        // Single-flight: a slow send must not let the next tick run concurrently.
        const tick = async () => {
          if (reconciling) return;
          reconciling = true;
          try {
            await reconcileOnce(deps);
          } catch (error) {
            ctx.logger.warn?.(`durable-jobs: reconcile tick failed: ${error?.message ?? error}`);
          } finally {
            reconciling = false;
          }
        };
        await tick();
        interval = setInterval(tick, config.pollIntervalMs);
        interval.unref?.();
        ctx.logger.info?.(`durable-jobs: watching ${rootDir}`);
      },
      stop() {
        if (interval) clearInterval(interval);
        interval = undefined;
      },
    });

    api.registerTool(
      (ctx) => ({
        name: "durable_job",
        description:
          "Start and inspect long-running commands that must survive the current agent turn. " +
          "Use action=start instead of nohup, '&', sleep/poll loops, or foreground waits. " +
          "The command is argv-based (no implicit shell) and state and logs are persisted. On completion a " +
          "deterministic terminal notice is delivered to the Slack route frozen at job creation. " +
          "Use status only for debugging or intervention; normal completion is push-driven.",
        parameters,
        async execute(_toolCallId, params) {
          const activeRoot = resolveRoot();
          const activeStateDir = stateDir;
          await ensureStore(activeRoot);
          try {
            if (params.action === "start") {
              // Fail-closed: while active legacy jobs block the upgrade, reject
              // new starts (list/status/cancel below remain available).
              if (legacyBlock) throw makeLegacyBlockError(legacyBlock);
              if (!params.name || !params.command || !params.cwd) {
                throw new Error("start requires name, command, and cwd");
              }
              const ownerCtx = resolveOwnerContext(config, ctx, { cwd: params.cwd });
              const startDeps = {
                rootDir: activeRoot,
                config,
                gatewayCall: makeGatewayCall(config.openclawCommand, activeStateDir),
                createFlow: makeFlowCreator(api),
                spawnWorker: spawnWorkerProcess,
              };
              return textResult(await startJob(startDeps, ownerCtx, params));
            }
            if (params.action === "list") {
              const filter = resolveListFilter(config, ctx, { cwd: params.cwd });
              const jobs = await listJobs(activeRoot);
              return textResult(
                jobs
                  .filter(
                    (job) =>
                      job.agentId === filter.agentId &&
                      (job.sessionKey ?? null) === (filter.sessionKey ?? null),
                  )
                  .slice(0, 30)
                  .map((job) => publicJob(job)),
              );
            }
            if (!params.jobId) throw new Error(`${params.action} requires jobId`);
            const job = await readJob(activeRoot, params.jobId);
            authorizeJobAccess(config, ctx, job);
            if (params.action === "status") {
              return textResult(await inspectJob(activeRoot, params.jobId, true));
            }
            if (params.action === "cancel") {
              return textResult(await cancelJob(activeRoot, params.jobId));
            }
            throw new Error(`unsupported action: ${params.action}`);
          } catch (error) {
            return textResult(error instanceof Error ? error.message : String(error), true);
          }
        },
      }),
      { name: "durable_job" },
    );

    // P3-B workflow tool surface (storage interface only). The OpenClaw loader enforces
    // registered⊆contracts.tools at registration and would throw "runtime unavailable" for a
    // declared-but-unregistered tool via the manifest descriptor path, so the `workflow` tool is ALWAYS
    // registered (and declared in openclaw.plugin.json contracts.tools). The workflowEnabled flag gates
    // BEHAVIOR: while disabled, every action fails closed with WORKFLOW_DISABLED and creates nothing.
    // start (when enabled) only materializes the workflow skeleton (all stages PENDING); no job runs yet.
    // durable_job/P0/P1/P2 code paths are unaffected.
    api.registerTool(
      (ctx) => ({
        name: "workflow",
        description:
          "Create and inspect durable multi-stage workflows (storage interface). action=start records a " +
          "workflow skeleton (all stages PENDING) with a frozen delivery route and trusted parent context; " +
          "it does NOT run any command or job yet. action=status/list read owner-scoped workflow state. " +
          "Requires workflowEnabled: true (otherwise returns WORKFLOW_DISABLED). Stage commands, submission, " +
          "and advancement are added in a later step.",
        parameters: workflowParameters,
        async execute(_toolCallId, params) {
          const activeRoot = resolveRoot();
          const activeStateDir = stateDir;
          await ensureStore(activeRoot);
          const activeGatewayCall = makeGatewayCall(config.openclawCommand, activeStateDir);
          const svcDeps = {
            rootDir: activeRoot,
            config,
            gatewayCall: activeGatewayCall,
            // P3-C: first-stage submission uses the real startJob seams (linked jobs carry no session → no
            // TaskFlow is created; createFlow is passed for signature completeness only).
            startDeps: {
              rootDir: activeRoot,
              config,
              gatewayCall: activeGatewayCall,
              createFlow: makeFlowCreator(api),
              spawnWorker: spawnWorkerProcess,
            },
          };
          try {
            return textResult(await runWorkflowAction(svcDeps, ctx, params));
          } catch (error) {
            return textResult(error instanceof Error ? error.message : String(error), true);
          }
        },
      }),
      { name: "workflow" },
    );
  },
});
