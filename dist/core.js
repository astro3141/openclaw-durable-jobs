import path from "node:path";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import {
  assertExecutable,
  createJob,
  createJobId,
  ensureStore,
  isProcessAlive,
  jobDir,
  listJobs,
  nowIso,
  readJob,
  resolveAllowedCwd,
  signalProcessGroup,
  tailFile,
  TERMINAL_STATES,
  updateJob,
} from "./job-store.js";
import { assertJobOwner, resolveOwnerContext } from "./ownership.js";
import {
  buildCompletionTurn,
  extractCompletionResponse,
  freezeDeliveryRoute,
  freezeOwnerConfigRoute,
  hasPersistedCompletionTurn,
  hasPersistedDelivery,
  resolveCompletionRouting,
  terminalCompletionMessage,
} from "./completion-turn.js";
import {
  applySendError,
  applySendResult,
  buildDeliveryPayload,
  classifyForTick,
  claimSending,
  initialOutbox,
  markStaleSendingUnknown,
} from "./delivery-outbox.js";
import { resolveRunnerMetadata } from "./evaluator.js";
import { classifyLiveness, readHeartbeat, resolveObservabilityPolicy } from "./heartbeat.js";
import {
  applyCompleted,
  applyDispatched,
  applyFailed,
  applyFallbackDelivered,
  applyFallbackError,
  applyManualFallback,
  applyTimedOut,
  buildContinuationFallbackPayload,
  buildContinuationTurn,
  claimDispatching,
  claimFallbackSending,
  classifyContinuationForTick,
  classifyFallbackForTick,
  continuationCompleted,
  continuationTimedOut,
  initialContinuation,
  markFallbackUnknown,
  recordCheck,
} from "./continuation.js";
import { reconcileWorkflowsOnce } from "./workflow-reconciler.js";

const CONTROLLER_ID = "durable-jobs/v1";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const workerPath = path.join(__dirname, "worker.js");

function textResult(value, isError = false) {
  return {
    content: [{ type: "text", text: typeof value === "string" ? value : JSON.stringify(value, null, 2) }],
    ...(isError ? { isError: true } : {}),
  };
}

// Standalone MCP bridge fallback: when the host did not deliver the plugin's
// own config (api.pluginConfig is empty), read ONLY this plugin's config section
// from the profile config on disk. The rest of the OpenClaw config is never
// returned or logged. Fail-closed on any resolution/parse/section error.
export function selfLoadPluginConfig(env = process.env) {
  const configPath =
    (typeof env.OPENCLAW_CONFIG_PATH === "string" && env.OPENCLAW_CONFIG_PATH) ||
    (typeof env.OPENCLAW_STATE_DIR === "string" && env.OPENCLAW_STATE_DIR
      ? path.join(env.OPENCLAW_STATE_DIR, "openclaw.json")
      : null);
  if (!configPath) {
    const error = new Error(
      "durable-jobs: cannot self-load config — neither OPENCLAW_CONFIG_PATH nor OPENCLAW_STATE_DIR is set",
    );
    error.code = "PLUGIN_CONFIG_UNAVAILABLE";
    throw error;
  }
  let full;
  try {
    full = JSON.parse(readFileSync(configPath, "utf8"));
  } catch (cause) {
    const error = new Error(`durable-jobs: failed to read/parse config at ${configPath}: ${cause?.message ?? cause}`);
    error.code = "PLUGIN_CONFIG_UNAVAILABLE";
    throw error;
  }
  const section = full?.plugins?.entries?.["durable-jobs"]?.config;
  if (!section || typeof section !== "object" || Array.isArray(section)) {
    const error = new Error(
      `durable-jobs: no plugins.entries["durable-jobs"].config section in ${configPath}`,
    );
    error.code = "PLUGIN_CONFIG_UNAVAILABLE";
    throw error;
  }
  return section;
}

export function readConfig(api, env = process.env) {
  const provided = api?.pluginConfig ?? {};
  // Trust a host-delivered config; only self-load when it is genuinely empty.
  const value = Object.keys(provided).length > 0 ? provided : selfLoadPluginConfig(env);
  return {
    stateSubdir: typeof value.stateSubdir === "string" ? value.stateSubdir : "durable-jobs",
    allowedRoots: Array.isArray(value.allowedRoots)
      ? value.allowedRoots.filter((item) => typeof item === "string" && item.length > 0)
      : [],
    owners: Array.isArray(value.owners) ? value.owners : [],
    ownerAgentId: typeof value.ownerAgentId === "string" ? value.ownerAgentId.trim() : "",
    ownerSessionKey: typeof value.ownerSessionKey === "string" ? value.ownerSessionKey.trim() : "",
    workspaceDir: typeof value.workspaceDir === "string" ? value.workspaceDir.trim() : "",
    deliveryRoute:
      value.deliveryRoute && typeof value.deliveryRoute === "object" ? value.deliveryRoute : undefined,
    openclawCommand:
      typeof value.openclawCommand === "string" && value.openclawCommand.trim()
        ? value.openclawCommand.trim()
        : "openclaw",
    pollIntervalMs: Number.isInteger(value.pollIntervalMs) ? value.pollIntervalMs : 2000,
    queuedGraceMs: Number.isInteger(value.queuedGraceMs) ? value.queuedGraceMs : 30000,
    maxConcurrent: Number.isInteger(value.maxConcurrent) ? value.maxConcurrent : 4,
    defaultTimeoutSeconds: Number.isInteger(value.defaultTimeoutSeconds)
      ? value.defaultTimeoutSeconds
      : 0,
    // Deterministic outbox delivery is the primary completion path. The ACP
    // wakeup (rich model report) is an opt-in follow-up, disabled by default.
    completionAcpWakeup: value.completionAcpWakeup === true,
    deliveryMaxAttempts: Number.isInteger(value.deliveryMaxAttempts) ? value.deliveryMaxAttempts : 8,
    // Lease held while a send is in flight; only a lease-expired SENDING record
    // is treated as stale (default comfortably exceeds the 10s send timeout).
    sendLeaseMs: Number.isInteger(value.sendLeaseMs) ? value.sendLeaseMs : 30_000,
    // P1 continuation (new-format jobs only). Auto-review the Supervisor on a terminal event via
    // chat.send(deliver:false), SEPARATE from the terminal notification. Default OFF (opt-in): P0 makes
    // every new job carry a parent block, so a default-on rollout would silently change behaviour for all
    // new jobs on upgrade. Enable explicitly in the profile config (`continuationEnabled: true`). Legacy
    // jobs (no parent block) never get P1 continuation regardless of this flag.
    continuationEnabled: value.continuationEnabled === true,
    // Bounded completion window: a chat.send that returns "started" is DISPATCHED, not COMPLETED. If no
    // completion evidence appears within this window the continuation is TIMED_OUT → one Slack fallback.
    continuationTimeoutMs: Number.isInteger(value.continuationTimeoutMs) ? value.continuationTimeoutMs : 120_000,
    continuationDispatchLeaseMs: Number.isInteger(value.continuationDispatchLeaseMs)
      ? value.continuationDispatchLeaseMs
      : 60_000,
    // Conservative: one dispatch, then fallback (no unbounded auto-retry after an ambiguous/abrupt loss).
    continuationMaxAttempts: Number.isInteger(value.continuationMaxAttempts) ? value.continuationMaxAttempts : 1,
    // P2-A observability: global heartbeat interval + per-runnerProfile overrides. The effective policy is
    // resolved and frozen onto each job at creation (resolveObservabilityPolicy validates the values).
    heartbeatIntervalMs: Number.isInteger(value.heartbeatIntervalMs) ? value.heartbeatIntervalMs : 5000,
    runnerObservability:
      value.runnerObservability && typeof value.runnerObservability === "object" && !Array.isArray(value.runnerObservability)
        ? value.runnerObservability
        : {},
    // P3-B workflow tool surface. Default OFF: the workflow execution engine is not complete (no job
    // submission / advancement yet), so the workflow.* tool is only registered when explicitly enabled.
    workflowEnabled: value.workflowEnabled === true,
    // P3-F execution-trust tunables. Invalid/out-of-range values fail closed to the default, then clamp to
    // the manifest bounds. Provider cache TTLs: READY normal, negative shorter, UNKNOWN very short.
    workflowProviderCacheTtlMs: clampInt(value.workflowProviderCacheTtlMs, 300_000, 0, 86_400_000),
    workflowProviderNegativeCacheTtlMs: clampInt(value.workflowProviderNegativeCacheTtlMs, 30_000, 0, 3_600_000),
    workflowProviderUnknownCacheTtlMs: clampInt(value.workflowProviderUnknownCacheTtlMs, 5_000, 0, 600_000),
    workflowPreflightTimeoutMs: clampInt(value.workflowPreflightTimeoutMs, 15_000, 1_000, 600_000),
    workflowFingerprintMaxFiles: clampInt(value.workflowFingerprintMaxFiles, 5000, 1, 200_000),
    workflowFingerprintMaxBytes: clampInt(value.workflowFingerprintMaxBytes, 268_435_456, 1024, 4_294_967_296),
    // P3-G Supervisor Audit Gate. Default OFF: an UNVERIFIED stage keeps the P3-F manual-approval path until
    // this is explicitly enabled AND the stage declares audit.mode=supervisor.
    workflowAuditEnabled: value.workflowAuditEnabled === true,
  };
}

// Fail-closed integer config: non-integer → default; then clamp to [min, max].
function clampInt(value, fallback, min, max) {
  const n = Number.isInteger(value) ? value : fallback;
  return Math.max(min, Math.min(max, n));
}

export function publicJob(job, logs = {}) {
  return {
    id: job.id,
    name: job.name,
    state: job.state,
    // Additive P0 separated state model (null on legacy rows).
    processState: job.processState ?? null,
    providerState: job.providerState ?? null,
    jobOutcome: job.jobOutcome ?? null,
    runnerType: job.runnerType ?? null,
    runnerProfile: job.runnerProfile ?? null,
    activityType: job.activityType ?? null,
    resultProtocol: job.resultProtocol ?? null,
    // P2-A observation (null on legacy rows / before first derivation).
    livenessState: job.livenessState ?? null,
    livenessSince: job.livenessSince ?? null,
    observability: job.observability ?? null,
    parent: job.parent ?? null,
    cwd: job.cwd,
    command: job.command,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    endedAt: job.endedAt,
    exitCode: job.exitCode,
    exitSignal: job.exitSignal,
    error: job.error,
    nextAction: job.nextAction,
    flowId: job.flowId,
    jobDir: job.directory,
    notification: job.notification,
    deliveryRoute: job.deliveryRoute ?? null,
    delivery: job.delivery ?? null,
    continuation: job.continuation ?? null,
    ...logs,
  };
}

export async function inspectJob(rootDir, jobId, includeLogs = true) {
  const job = await readJob(rootDir, jobId);
  if (!includeLogs) return publicJob(job);
  const directory = jobDir(rootDir, jobId);
  const [stdoutTail, stderrTail, workerTail, heartbeat] = await Promise.all([
    tailFile(path.join(directory, "stdout.log")),
    tailFile(path.join(directory, "stderr.log")),
    tailFile(path.join(directory, "worker.log")),
    readHeartbeat(directory),
  ]);
  return publicJob(job, { stdoutTail, stderrTail, workerTail, heartbeat });
}

async function runGatewayCall(command, args, stateDir, timeoutMs = 15_000) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        OPENCLAW_STATE_DIR: stateDir,
        OPENCLAW_CONFIG_PATH: path.join(stateDir, "openclaw.json"),
      },
    });
    let stdout = "";
    let stderr = "";
    const appendBounded = (current, chunk) => `${current}${chunk}`.slice(-1_048_576);
    child.stdout.on("data", (chunk) => {
      stdout = appendBounded(stdout, chunk.toString("utf8"));
    });
    child.stderr.on("data", (chunk) => {
      stderr = appendBounded(stderr, chunk.toString("utf8"));
    });
    const timer = setTimeout(() => {
      child.kill("SIGTERM");
      reject(new Error(`Gateway call timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    timer.unref?.();
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      if (code !== 0) {
        reject(
          new Error(
            `Gateway call failed (code=${code}, signal=${signal ?? "none"}): ${stderr || stdout}`,
          ),
        );
        return;
      }
      try {
        resolve({ result: JSON.parse(stdout.trim()), stderr: stderr.trim() });
      } catch {
        reject(new Error(`Gateway call returned invalid JSON: ${stdout || stderr}`));
      }
    });
  });
}

// Injectable Gateway RPC seam. Returns { result, stderr }. The default
// implementation spawns the OpenClaw CLI; tests inject a stub.
export function makeGatewayCall(command, stateDir) {
  return (method, params, timeoutMs = 10_000) =>
    runGatewayCall(
      command,
      ["gateway", "call", method, "--json", "--timeout", String(timeoutMs), "--params", JSON.stringify(params)],
      stateDir,
    );
}

// Resolve the originating delivery route exactly once, at job creation, using
// the caller's current session. The frozen route becomes authoritative; no
// later stage re-reads the session, session key, or chat.history.
export async function resolveCreationRoute(gatewayCall, ctx) {
  const { result: history } = await gatewayCall("chat.history", {
    sessionKey: ctx.sessionKey,
    agentId: ctx.agentId,
    limit: 1,
    maxChars: 4_000,
  });
  const resolved = resolveCompletionRouting({ requesterOrigin: ctx.deliveryContext }, history);
  // freezeDeliveryRoute classifies the route from trusted metadata and throws
  // { code: "DELIVERY_ROUTE_UNAVAILABLE" } only when it is "unknown".
  return freezeDeliveryRoute(resolved, ctx, { source: "sessionInfo" });
}

// The originating delivery route is best-effort enrichment. A trusted
// workflow.start must still create the record and freeze the parent identity
// when the route-resolution channel itself is UNREACHABLE — e.g. the managed
// plugin-tools bridge cannot authenticate a chat.history websocket, so the
// probe either fails fast (GatewayCredentialsRequiredError) or the spawned
// gateway-call CLI hangs until the durable-jobs timeout fires. Both are the same
// infrastructure gap (no reachable gateway for this subprocess), not a route
// defect, so they collapse to a null (deferred) route. Everything else still
// surfaces — a REACHABLE gateway that returns an UNPROVABLE route
// (DELIVERY_ROUTE_UNAVAILABLE) is still a hard rejection, and programming/state
// errors still throw. This classifier is only consulted by the delivery-route
// best-effort wrapper, so it never swallows unrelated gateway calls. Delivery,
// which reads the frozen route, is out of scope here.
function isDeferrableDeliveryRouteFailure(error) {
  if (!error) return false;
  const name = error.name ?? error.constructor?.name;
  const code = error.code;
  const message = typeof error.message === "string" ? error.message : "";
  return (
    name === "GatewayCredentialsRequiredError" ||
    code === "GATEWAY_CREDENTIALS_REQUIRED" ||
    /requires credentials before opening a websocket/i.test(message) ||
    /gateway call timed out after \d+ms/i.test(message)
  );
}

export async function resolveCreationRouteBestEffort(gatewayCall, ctx) {
  try {
    return await resolveCreationRoute(gatewayCall, ctx);
  } catch (error) {
    if (isDeferrableDeliveryRouteFailure(error)) {
      return null;
    }
    throw error;
  }
}

// Send a deterministic terminal message straight to the frozen route. No ACP
// wakeup, no session/history re-read.
export async function sendDeliveryOutbox(gatewayCall, job) {
  const built = buildDeliveryPayload(job);
  if (!built.ok) {
    const error = new Error(`durable-job delivery route unusable: ${built.reason}`);
    error.code = built.reason;
    throw error;
  }
  const { result, stderr } = await gatewayCall("send", built.payload);
  return { ...(result ?? {}), stderr: stderr || undefined };
}

// Drive the persistent outbox for one terminal job.
export async function processDeliveryOutbox(deps, job) {
  const { rootDir, config, gatewayCall } = deps;
  if (!job.delivery || !job.deliveryRoute) return;
  const disposition = classifyForTick(job.delivery);
  if (disposition === "skip" || disposition === "resolved") return;

  if (disposition === "stale_sending") {
    // Lease expired: the previous attempt's outcome is genuinely unknown. Always
    // park it as DELIVERY_UNKNOWN for manual operator verification — never
    // blind-resend (that would risk a duplicate).
    await updateJob(rootDir, job.id, (current) => {
      if (current.delivery?.state !== "SENDING") return null;
      current.delivery = markStaleSendingUnknown(current.delivery);
      return current;
    });
    return;
  }

  // Claim: transition to SENDING atomically under the job lock. A concurrent
  // reconcile that already claimed leaves state=SENDING, so claimSending here
  // returns null and this tick makes no second send.
  const leaseMs = config.sendLeaseMs;
  const claimed = await updateJob(rootDir, job.id, (current) => {
    const next = claimSending(current.delivery, { leaseMs });
    if (!next) return null;
    current.delivery = next;
    return current;
  });
  if (!claimed) return;

  try {
    const result = await sendDeliveryOutbox(gatewayCall, claimed);
    await updateJob(rootDir, claimed.id, (current) => {
      if (current.delivery?.state !== "SENDING") return null;
      current.delivery = applySendResult(current.delivery, result);
      return current;
    });
  } catch (error) {
    await updateJob(rootDir, claimed.id, (current) => {
      if (current.delivery?.state !== "SENDING") return null;
      current.delivery = applySendError(current.delivery, error);
      return current;
    });
    throw error;
  }
}

// Drive the one-time structured continuation fallback notice to the frozen route. Conservative and
// crash-/error-isolated:
//   - claim SENDING (lease) before the send so a duplicate tick cannot double-send in-process;
//   - a lease-expired SENDING (ambiguous crash around the send) is PARKED as DELIVERY_UNKNOWN, never
//     blind-resent — the gateway `send` is not a proven exactly-once Slack dedup (README), so we must not
//     risk a second user message;
//   - a send that ERRORS in-process (did not post) is retried with bounded backoff, recording
//     fallbackLastError / fallbackLastAttemptAt; the error is swallowed so one job cannot disturb the
//     rest of the reconcile pass.
async function driveContinuationFallback(deps, job, reason) {
  const { rootDir, config, gatewayCall } = deps;
  // Phase 1 (under the lock): classify, park an ambiguous send, or claim SENDING. updateJob returns the
  // row even on a no-op, so the intended action is signalled by a closure flag.
  let action = "skip";
  await updateJob(rootDir, job.id, (current) => {
    const c = current.continuation;
    if (!c) return null;
    const disposition = classifyFallbackForTick(c);
    if (disposition === "resolved" || disposition === "skip") return null;
    if (disposition === "park_unknown") {
      current.continuation = markFallbackUnknown(c);
      action = "parked";
      return current;
    }
    current.continuation = claimFallbackSending(c, { leaseMs: config.continuationDispatchLeaseMs, reason });
    action = "send";
    return current;
  });
  if (action !== "send") return;
  const built = buildContinuationFallbackPayload(job, reason);
  if (!built.ok) return; // route invalid — leave SENDING to expire → parked next tick
  try {
    await gatewayCall("send", built.payload);
    await updateJob(rootDir, job.id, (current) => {
      if (current.continuation?.fallbackState !== "SENDING") return null;
      current.continuation = applyFallbackDelivered(current.continuation);
      return current;
    });
  } catch (error) {
    // In-process send failure (did not post): retryable with backoff, bounded. Never rethrown.
    await updateJob(rootDir, job.id, (current) => {
      if (current.continuation?.fallbackState !== "SENDING") return null;
      current.continuation = applyFallbackError(current.continuation, error);
      return current;
    });
  }
}

// Drive the P1 continuation for one new-format terminal job. Separate from the terminal notification.
// A chat.send "started" is DISPATCHED (never COMPLETED); completion is CORRELATED to a unique marker in
// chat.history within a bounded window, else TIMED_OUT → one fallback notice. Unsupported/failed
// conditions → MANUAL_FALLBACK. A fallback is always routed through the conservative, error-isolated
// driveContinuationFallback, so call sites do not guard on a prior send.
//
// Runtime disable policy (audit): `continuationEnabled=false` blocks only the CREATION and initial
// DISPATCH of continuations. A continuation record that already exists (e.g. a DISPATCHED turn from before
// the operator flipped the flag) is still driven safely to a terminal state — completion, timeout, or
// fallback — so nothing is left permanently DISPATCHED. A not-yet-dispatched PENDING record found while
// disabled is transitioned to MANUAL_FALLBACK rather than dispatched.
export async function processContinuation(deps, job) {
  const { rootDir, config, gatewayCall } = deps;
  if (!job.parent) return; // new-format only; legacy jobs use the completionAcpWakeup path
  // Lazily initialize the continuation record on the first terminal tick — ONLY when enabled. When
  // disabled, an existing record is still driven below; a missing one is simply not created.
  if (config.continuationEnabled) {
    await updateJob(rootDir, job.id, (current) => {
      if (!TERMINAL_STATES.has(current.state) || !current.parent || current.continuation) return null;
      current.continuation = initialContinuation(current);
      if (Number.isInteger(config.continuationMaxAttempts)) {
        current.continuation.maxAttempts = config.continuationMaxAttempts;
      }
      return current;
    });
  }
  const current = await readJob(rootDir, job.id);
  const record = current.continuation;
  if (!record) return; // disabled and nothing in flight
  const disposition = classifyContinuationForTick(record, current);

  if (disposition === "resolved" || disposition === "skip") {
    // A resolved fallback state still owes exactly one Slack notice (the claim makes it idempotent).
    if (["FAILED", "TIMED_OUT", "MANUAL_FALLBACK"].includes(record.state)) {
      await driveContinuationFallback(deps, current, record.reason ?? "TURN_TIMEOUT");
    }
    return;
  }

  if (disposition === "manual_fallback_no_session") {
    const changed = await updateJob(rootDir, job.id, (j) => {
      if (j.continuation?.state !== "PENDING") return null;
      j.continuation = applyManualFallback(j.continuation, "NO_SESSION_KEY");
      return j;
    });
    await driveContinuationFallback(deps, changed ?? (await readJob(rootDir, job.id)), "NO_SESSION_KEY");
    return;
  }

  if (disposition === "dispatch_stale") {
    // Crash mid-dispatch: ambiguous. Conservative TIMED_OUT, never a blind re-dispatch.
    const changed = await updateJob(rootDir, job.id, (j) => {
      if (j.continuation?.state !== "DISPATCHING") return null;
      j.continuation = applyTimedOut(j.continuation, "TURN_TIMEOUT");
      return j;
    });
    await driveContinuationFallback(deps, changed ?? (await readJob(rootDir, job.id)), "TURN_TIMEOUT");
    return;
  }

  if (disposition === "dispatch") {
    // Disabled after this record was created but before it dispatched: do NOT start a new turn — route to
    // manual re-entry instead (audit: runtime disable blocks new dispatch, in-flight is drained elsewhere).
    if (!config.continuationEnabled) {
      const changed = await updateJob(rootDir, job.id, (j) => {
        if (j.continuation?.state !== "PENDING") return null;
        j.continuation = applyManualFallback(j.continuation, "CONTINUATION_DISABLED");
        return j;
      });
      await driveContinuationFallback(deps, changed ?? (await readJob(rootDir, job.id)), "CONTINUATION_DISABLED");
      return;
    }
    let didClaim = false;
    const claimed = await updateJob(rootDir, job.id, (j) => {
      const next = claimDispatching(j.continuation, j, { leaseMs: config.continuationDispatchLeaseMs });
      if (!next) return null;
      j.continuation = next;
      didClaim = true;
      return j;
    });
    if (!didClaim) return; // updateJob returns the row even on a no-op; success is the closure flag
    const built = buildContinuationTurn(claimed);
    if (!built.ok) {
      const changed = await updateJob(rootDir, job.id, (j) => {
        if (j.continuation?.state !== "DISPATCHING") return null;
        j.continuation = applyManualFallback(j.continuation, "NO_SESSION_KEY");
        return j;
      });
      await driveContinuationFallback(deps, changed ?? claimed, "NO_SESSION_KEY");
      return;
    }
    try {
      const { result } = await gatewayCall("chat.send", built.payload);
      // "started"/runId means DISPATCHED — explicitly NOT completed.
      await updateJob(rootDir, job.id, (j) => {
        if (j.continuation?.state !== "DISPATCHING") return null;
        j.continuation = applyDispatched(j.continuation, result);
        return j;
      });
    } catch (error) {
      const changed = await updateJob(rootDir, job.id, (j) => {
        if (j.continuation?.state !== "DISPATCHING") return null;
        j.continuation = applyFailed(j.continuation, error, "DISPATCH_FAILED");
        return j;
      });
      await driveContinuationFallback(deps, changed ?? (await readJob(rootDir, job.id)), "DISPATCH_FAILED");
    }
    return;
  }

  if (disposition === "check_completion") {
    // Isolate a transient chat.history failure (audit 4): keep DISPATCHED, record the error, and let the
    // bounded timeout eventually fire — a lookup error is NOT a DISPATCH_FAILED. Never propagate (so one
    // job's history error cannot disturb the rest of the reconcile pass).
    let history;
    let checkError = null;
    try {
      ({ result: history } = await gatewayCall("chat.history", {
        sessionKey: current.parent?.sessionKey,
        agentId: current.agentId ?? current.parent?.agentId,
        limit: 30,
        maxChars: 50_000,
      }));
    } catch (error) {
      checkError = error;
    }
    if (!checkError && continuationCompleted(history, record)) {
      await updateJob(rootDir, job.id, (j) => {
        if (j.continuation?.state !== "DISPATCHED") return null;
        j.continuation = applyCompleted(j.continuation);
        return j;
      });
      return;
    }
    if (continuationTimedOut(record, { timeoutMs: config.continuationTimeoutMs })) {
      const changed = await updateJob(rootDir, job.id, (j) => {
        if (j.continuation?.state !== "DISPATCHED") return null;
        j.continuation = applyTimedOut(j.continuation, "TURN_TIMEOUT");
        return j;
      });
      await driveContinuationFallback(deps, changed ?? (await readJob(rootDir, job.id)), "TURN_TIMEOUT");
      return;
    }
    // Still within the window: record the probe (error or not-yet-complete) and wait for the next tick.
    await updateJob(rootDir, job.id, (j) => {
      if (j.continuation?.state !== "DISPATCHED") return null;
      j.continuation = recordCheck(j.continuation, { error: checkError });
      return j;
    });
    return;
  }
}

// Legacy ACP-wakeup completion path (opt-in via config.completionAcpWakeup).
async function dispatchCompletionTurn(gatewayCall, job) {
  const { result: history } = await gatewayCall("chat.history", {
    sessionKey: job.sessionKey,
    agentId: job.agentId,
    limit: 30,
    maxChars: 50_000,
  });
  const payload = buildCompletionTurn(job);
  if (hasPersistedCompletionTurn(history, payload.idempotencyKey, payload.message)) {
    return { runId: payload.idempotencyKey, status: "already_persisted" };
  }
  const { result, stderr } = await gatewayCall("chat.send", payload);
  if (!result || result.runId !== payload.idempotencyKey) {
    throw new Error(`Gateway did not acknowledge completion run ${payload.idempotencyKey}`);
  }
  return { ...result, stderr: stderr || undefined };
}

async function deliverCompletionResponse(gatewayCall, job) {
  const { result: history } = await gatewayCall("chat.history", {
    sessionKey: job.sessionKey,
    agentId: job.agentId,
    limit: 30,
    maxChars: 50_000,
  });
  const deliveryIdempotencyKey = `${job.notification.idempotencyKey}:delivery`;
  if (hasPersistedDelivery(history, deliveryIdempotencyKey)) {
    return { runId: deliveryIdempotencyKey, status: "already_persisted" };
  }
  const message = extractCompletionResponse(
    history,
    job.notification.idempotencyKey,
    terminalCompletionMessage(job),
  );
  if (!message) return null;
  const route = resolveCompletionRouting(job, history);
  if (!route.originatingChannel || !route.originatingTo) {
    throw new Error("owning session has no deliverable channel route");
  }
  const payload = {
    to: route.originatingTo,
    message,
    channel: route.originatingChannel,
    ...(route.originatingAccountId ? { accountId: route.originatingAccountId } : {}),
    ...(route.originatingThreadId ? { threadId: route.originatingThreadId } : {}),
    agentId: job.agentId,
    sessionKey: job.sessionKey,
    idempotencyKey: deliveryIdempotencyKey,
  };
  const { result, stderr } = await gatewayCall("send", payload);
  if (!result || result.runId !== deliveryIdempotencyKey) {
    throw new Error(`Gateway did not acknowledge Slack delivery ${deliveryIdempotencyKey}`);
  }
  return { ...result, stderr: stderr || undefined };
}

export async function settleFlowWithApi(api, job) {
  if (!job.flowId || !job.sessionKey) return;
  const flowRuntime = api.runtime.taskFlow.bindSession({
    sessionKey: job.sessionKey,
    requesterOrigin: job.requesterOrigin,
  });
  const flow = flowRuntime.get(job.flowId);
  if (!flow || ["succeeded", "failed", "cancelled", "lost"].includes(flow.status)) return;
  const stateJson = {
    jobId: job.id,
    state: job.state,
    jobOutcome: job.jobOutcome ?? null,
    processState: job.processState ?? null,
    providerState: job.providerState ?? null,
    exitCode: job.exitCode ?? null,
    error: job.error ?? null,
    jobDir: job.directory,
    nextAction: job.nextAction ?? null,
  };
  // New-format jobs (P0) settle by jobOutcome, never by the legacy SUCCEEDED alias — otherwise a
  // FAILED_PROVIDER job (whose state alias is still "SUCCEEDED" for exit 0) would be finished as a
  // TaskFlow success. P0 has NO success outcome, and the TaskFlow runtime exposes only finish(success) /
  // fail(blocked); there is no neutral "completed-unverified" status. Conservative choice, and its
  // limitation, are documented in docs/DESIGN_workflow_harness.md §8/§20: never finish() a P0 job —
  // route every outcome (including COMPLETED_UNVERIFIED) to fail() with the outcome as the blocked
  // summary, so a downstream reviewer (P1) treats it as "needs verification", not "succeeded".
  if (typeof job.jobOutcome === "string") {
    flowRuntime.fail({
      flowId: flow.flowId,
      expectedRevision: flow.revision,
      stateJson,
      blockedSummary: job.error || `durable job outcome ${job.jobOutcome}`,
    });
    return;
  }
  // Legacy jobs (no jobOutcome): preserve the original state-based settlement exactly.
  if (job.state === "SUCCEEDED") {
    flowRuntime.finish({ flowId: flow.flowId, expectedRevision: flow.revision, stateJson });
  } else {
    flowRuntime.fail({
      flowId: flow.flowId,
      expectedRevision: flow.revision,
      stateJson,
      blockedSummary: job.error || `durable job ended as ${job.state}`,
    });
  }
}

async function claimNotification(rootDir, jobId) {
  let claimed = false;
  const job = await updateJob(rootDir, jobId, (current) => {
    if (!TERMINAL_STATES.has(current.state)) return null;
    const notification = current.notification ?? { status: "pending" };
    if (
      notification.status === "pending" &&
      notification.nextAttemptAt &&
      Date.parse(notification.nextAttemptAt) > Date.now()
    ) {
      return null;
    }
    const staleClaim =
      notification.status === "processing" &&
      Date.now() - Date.parse(notification.claimedAt ?? 0) > 60_000;
    if (notification.status !== "pending" && !staleClaim) return null;
    current.notification = {
      ...notification,
      status: "processing",
      claimedAt: nowIso(),
      attempts: (notification.attempts ?? 0) + 1,
    };
    claimed = true;
    return current;
  });
  return claimed ? job : null;
}

async function notifyTerminalJob(deps, job) {
  const { rootDir, gatewayCall, settleFlow } = deps;
  const claimed = await claimNotification(rootDir, job.id);
  if (!claimed) return;
  try {
    await settleFlow(claimed);
    const gatewayResult = await dispatchCompletionTurn(gatewayCall, claimed);
    await updateJob(rootDir, claimed.id, (current) => {
      current.notification.status = "awaiting_response";
      current.notification.queuedAt = nowIso();
      current.notification.nextAttemptAt = null;
      current.notification.lastError = null;
      current.notification.gateway = gatewayResult;
      return current;
    });
  } catch (error) {
    await updateJob(rootDir, claimed.id, (current) => {
      current.notification.status = "pending";
      current.notification.lastError = error instanceof Error ? error.message : String(error);
      const attempts = Math.max(1, current.notification.attempts ?? 1);
      const delayMs = Math.min(60_000, 2_000 * 2 ** Math.min(attempts - 1, 5));
      current.notification.nextAttemptAt = new Date(Date.now() + delayMs).toISOString();
      return current;
    });
    throw error;
  }
}

async function deliverTerminalResponse(deps, job) {
  const { rootDir, gatewayCall } = deps;
  const notification = job.notification ?? {};
  if (
    notification.nextAttemptAt &&
    Date.parse(notification.nextAttemptAt) > Date.now()
  ) {
    return;
  }
  try {
    const deliveryResult = await deliverCompletionResponse(gatewayCall, job);
    if (!deliveryResult) return;
    await updateJob(rootDir, job.id, (current) => {
      if (current.notification.status !== "awaiting_response") return null;
      current.notification.status = "delivered";
      current.notification.deliveredAt = nowIso();
      current.notification.nextAttemptAt = null;
      current.notification.lastError = null;
      current.notification.delivery = deliveryResult;
      return current;
    });
  } catch (error) {
    await updateJob(rootDir, job.id, (current) => {
      if (current.notification.status !== "awaiting_response") return null;
      current.notification.deliveryAttempts = (current.notification.deliveryAttempts ?? 0) + 1;
      current.notification.lastError = error instanceof Error ? error.message : String(error);
      const attempts = current.notification.deliveryAttempts;
      const delayMs = Math.min(60_000, 2_000 * 2 ** Math.min(attempts - 1, 5));
      current.notification.nextAttemptAt = new Date(Date.now() + delayMs).toISOString();
      return current;
    });
    throw error;
  }
}

async function markLostIfNeeded(rootDir, job, queuedGraceMs) {
  if (job.state === "QUEUED") {
    if (Date.now() - Date.parse(job.createdAt) <= queuedGraceMs) return job;
  } else if (job.state === "RUNNING") {
    if (isProcessAlive(job.workerPid)) return job;
    if (isProcessAlive(job.childPid)) {
      signalProcessGroup(job.childPid, "SIGTERM");
    }
  } else {
    return job;
  }
  return updateJob(rootDir, job.id, (current) => {
    if (!new Set(["QUEUED", "RUNNING"]).has(current.state)) return null;
    current.state = "LOST";
    current.endedAt = nowIso();
    current.error = "authoritative worker process disappeared before recording a terminal result";
    // New-format jobs (with a parent block) get the separated outcome; LOST → FAILED_COMMAND (verdict.js).
    if (current.parent) {
      current.processState = "LOST";
      current.providerState = "UNKNOWN";
      current.jobOutcome = "FAILED_COMMAND";
    }
    current.notification.status = "pending";
    return current;
  });
}

// P2-A: derive the liveness OBSERVATION for a RUNNING job from its heartbeat.json. Writes job.json ONLY
// when livenessState actually changes (no high-frequency copy of heartbeat values into job.json). Never
// kills, retries, or changes processState — SUSPECTED_STALL/STALLED are observations for policy only.
export async function deriveLiveness(rootDir, job, now = Date.now()) {
  if (job.state !== "RUNNING") return; // only running jobs; terminal states are not observed
  const policy = job.observability;
  if (!policy) return; // legacy job (no frozen policy) → keep the existing pid-liveness behaviour only
  const heartbeat = await readHeartbeat(job.directory ?? jobDir(rootDir, job.id));
  if (!heartbeat) return; // legacy / no heartbeat file → no derivation
  const pidAlive = isProcessAlive(heartbeat.childPid ?? job.childPid);
  const next = classifyLiveness({
    prev: { livenessState: job.livenessState, livenessSince: job.livenessSince, suspectCpuMs: job.livenessSuspectCpuMs },
    heartbeat,
    policy,
    pidAlive,
    now,
  });
  // A job with no recorded liveness is implicitly HEALTHY; only a real state change writes job.json.
  const prevEffective = job.livenessState ?? "HEALTHY";
  if (!next || next.livenessState === prevEffective) return;
  await updateJob(rootDir, job.id, (current) => {
    if (current.state !== "RUNNING") return null; // raced to terminal — do not stamp liveness
    current.livenessState = next.livenessState;
    current.livenessSince = next.livenessSince;
    current.livenessSuspectCpuMs = next.suspectCpuMs ?? null;
    current.lastLivenessCheckAt = new Date(now).toISOString();
    return current;
  });
}

// Legacy jobs from before the outbox existed: still active but carrying no
// frozen route and no outbox record. They must not be silently handled by the
// new completion path.
export function isBlockingLegacyJob(job) {
  return (job.state === "QUEUED" || job.state === "RUNNING") && !job.deliveryRoute && !job.delivery;
}

export async function detectLegacyActiveJobs(rootDir) {
  const jobs = await listJobs(rootDir);
  return jobs.filter(isBlockingLegacyJob);
}

export const LEGACY_BLOCK_CODE = "LEGACY_ACTIVE_JOBS_PRESENT";

// Build the fail-closed error used when active legacy jobs are present. Includes
// each blocked job's id, state, and createdAt so operators can drain them.
export function makeLegacyBlockError(legacyJobs) {
  const detail = (legacyJobs ?? [])
    .map((job) => `${job.id} state=${job.state} createdAt=${job.createdAt ?? "unknown"}`)
    .join("; ");
  const error = new Error(
    `${LEGACY_BLOCK_CODE}: ${(legacyJobs ?? []).length} active legacy job(s) block new durable jobs ` +
      `until they reach a terminal state and the service/Gateway is restarted [${detail}]`,
  );
  error.code = LEGACY_BLOCK_CODE;
  error.jobs = (legacyJobs ?? []).map((job) => ({
    id: job.id,
    state: job.state,
    createdAt: job.createdAt ?? null,
  }));
  return error;
}

export async function reconcileOnce(deps) {
  const { rootDir, config, gatewayCall, settleFlow, logger } = deps;
  const jobs = await listJobs(rootDir);
  for (const original of jobs) {
    try {
      const job = await markLostIfNeeded(rootDir, original, config.queuedGraceMs);
      // P2-A: derive the liveness OBSERVATION for a still-running job (never kills/retries/changes
      // processState). Skipped for legacy jobs with no heartbeat.json. Then this job is not terminal, so
      // it is not eligible for notification/continuation this tick.
      if (!TERMINAL_STATES.has(job.state)) {
        await deriveLiveness(rootDir, job).catch((error) =>
          logger?.warn?.(`durable-jobs: liveness derive failed for ${job.id}: ${error?.message ?? error}`),
        );
        continue;
      }
      // TaskFlow settlement runs regardless of the delivery path.
      await settleFlow(job);
      // Notification path (user-facing): deterministic terminal message to the frozen route.
      // Legacy jobs (no route/outbox) are skipped inside processDeliveryOutbox.
      await processDeliveryOutbox({ rootDir, config, gatewayCall }, job);
      // Continuation path (internal Supervisor review), SEPARATE from the notification above.
      //  - New-format jobs (parent block): P1 auto-continuation via chat.send(deliver:false).
      //  - Legacy jobs (no parent): unchanged optional ACP wakeup (opt-in, needs a live sessionKey).
      // P3-C: a WORKFLOW-LINKED job's stage state is owned by the workflow reconciler; its standalone P1
      // continuation is suppressed (the terminal notice via the outbox above is kept — one notice).
      if (job.parent && !job.workflowLink) {
        // Drive continuation when enabled OR when a record already exists (so a continuation in flight
        // when the operator disables the feature is still drained to a terminal state, never stuck).
        if (config.continuationEnabled || job.continuation) {
          await processContinuation({ rootDir, config, gatewayCall }, job);
        }
      } else if (!job.parent && config.completionAcpWakeup && job.sessionKey) {
        const latest = await readJob(rootDir, job.id);
        if (latest.notification?.status === "awaiting_response") {
          await deliverTerminalResponse({ rootDir, gatewayCall }, latest);
        } else {
          await notifyTerminalJob({ rootDir, gatewayCall, settleFlow }, latest);
        }
      }
    } catch (error) {
      logger?.warn?.(`durable-jobs: reconcile failed for ${original.id}: ${error?.message ?? error}`);
    }
  }
  // P3-C: workflow storage + linked-stage reconciliation, folded into the SAME single-flight tick (no second
  // reconciler service). A failure here must not break the standalone job path above — it is isolated. New
  // submissions are gated by workflowEnabled inside the workflow reconciler; terminal reconciliation of an
  // already-linked job runs regardless of the flag.
  try {
    await reconcileWorkflowsOnce({
      rootDir,
      config,
      startJob,
      startDeps: { rootDir, config, gatewayCall, createFlow: deps.createFlow, spawnWorker: deps.spawnWorker },
      cancelJob, // P3-E: converge a cancel-requested active stage by cancelling its durable job
      gatewayCall, // P3-E: approval-request outbox delivery
      logger,
    });
  } catch (error) {
    logger?.warn?.(`durable-jobs: workflow reconcile pass failed: ${error?.message ?? error}`);
  }
}

const WF_LINK_ID_RE = /^wf-[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const WF_LINK_STAGE_RE = /^\d{3}-[A-Za-z0-9_.-]{1,64}$/;
const WF_FROZEN_ROUTE_KINDS = new Set(["channel_root", "thread"]);

function wfLinkError(message) {
  const error = new Error(`WORKFLOW_LINK_INVALID: ${message}`);
  error.code = "WORKFLOW_LINK_INVALID";
  return error;
}

// #4: strict, pure validation of workflow-internal validatedExecution metadata (the frozen execution
// fingerprint the worker re-verifies before spawn). Rejects BEFORE any job/worker side effect. It is never
// exposed on the durable_job tool schema, so this is the only ingress; a standalone (non-workflow-linked)
// caller can never carry it, and every field is bounded to a canonical shape (no injected absolute path,
// oversized number, wrong runner, or cwd/worktree mismatch reaches the worker).
function veError(message) {
  const e = new Error(`WORKFLOW_VALIDATED_EXECUTION_INVALID: ${message}`);
  e.code = "WORKFLOW_VALIDATED_EXECUTION_INVALID";
  return e;
}
function validateValidatedExecution(metadata, { workflowLink, runnerType, runnerProfile, cwd }) {
  if (metadata == null) return null; // absent is always fine
  if (!workflowLink) throw veError("validatedExecution requires a valid workflowLink (not injectable standalone)");
  if (typeof metadata !== "object" || Array.isArray(metadata)) throw veError("must be a plain object");
  const allowedTop = new Set(["worktree", "worktreeAggregateHash", "fingerprint", "toolchain"]);
  for (const k of Object.keys(metadata)) if (!allowedTop.has(k)) throw veError(`unexpected field ${k}`);
  const isSha = (h) => typeof h === "string" && /^[0-9a-f]{64}$/.test(h);
  const isAbs = (p) => typeof p === "string" && path.isAbsolute(p);
  const isBoundedInt = (n) => Number.isInteger(n) && n >= 0 && n <= Number.MAX_SAFE_INTEGER;
  // worktree section (absolute, and equal to the job's forced cwd)
  if (metadata.worktree != null) {
    if (!isAbs(metadata.worktree)) throw veError("worktree must be an absolute path");
    if (path.resolve(metadata.worktree) !== path.resolve(cwd)) throw veError("worktree must equal the job cwd");
  }
  if (metadata.worktreeAggregateHash != null && !isSha(metadata.worktreeAggregateHash)) throw veError("worktreeAggregateHash must be a lowercase sha-256");
  // bounded fingerprint limits/timeouts
  if (metadata.fingerprint != null) {
    const fp = metadata.fingerprint;
    if (typeof fp !== "object" || Array.isArray(fp)) throw veError("fingerprint must be an object");
    for (const k of Object.keys(fp)) if (!["maxFiles", "maxBytes", "timeoutMs"].includes(k)) throw veError(`unexpected fingerprint field ${k}`);
    for (const k of ["maxFiles", "maxBytes", "timeoutMs"]) if (fp[k] != null && !isBoundedInt(fp[k])) throw veError(`fingerprint.${k} must be a bounded non-negative integer`);
  }
  // toolchain section (complete shape; a partial shape is rejected)
  if (metadata.toolchain != null) {
    const tc = metadata.toolchain;
    if (typeof tc !== "object" || Array.isArray(tc)) throw veError("toolchain must be an object");
    const allowedTc = new Set(["executableRealpath", "executableBasename", "executableContentHash", "executableSize", "aggregateHash", "runnerType", "runnerProfile"]);
    for (const k of Object.keys(tc)) if (!allowedTc.has(k)) throw veError(`unexpected toolchain field ${k}`);
    if (!isAbs(tc.executableRealpath)) throw veError("toolchain.executableRealpath must be an absolute path");
    if (!isSha(tc.aggregateHash)) throw veError("toolchain.aggregateHash must be a lowercase sha-256");
    if (tc.executableContentHash != null && !isSha(tc.executableContentHash)) throw veError("toolchain.executableContentHash must be a lowercase sha-256");
    if (!isBoundedInt(tc.executableSize)) throw veError("toolchain.executableSize must be a non-negative safe integer");
    if (tc.runnerType != null && tc.runnerType !== runnerType) throw veError("toolchain.runnerType must match the validated runner");
    if (tc.runnerProfile != null && tc.runnerProfile !== runnerProfile) throw veError("toolchain.runnerProfile must match the validated runner");
  }
  // require at least one verifiable section (a fully-empty metadata is meaningless)
  if (metadata.toolchain == null && metadata.worktreeAggregateHash == null) throw veError("must carry a toolchain and/or a worktree checkpoint");
  return metadata;
}

// Validate optional workflow-link metadata (internal; never set from the durable_job tool schema). Returns
// null when absent, a frozen shape when valid, and fail-closes on any malformed/inconsistent link: the ids
// must match their strict shapes and the activityIdempotencyKey must EXACTLY equal the deterministic key
// derived from workflowId/stageId/attempt (a caller cannot smuggle a mismatched key).
export function validateWorkflowLink(link) {
  if (link === undefined || link === null) return null;
  if (typeof link !== "object" || Array.isArray(link)) throw wfLinkError("workflowLink must be an object");
  const { workflowId, stageId, attempt, activityIdempotencyKey } = link;
  if (typeof workflowId !== "string" || !WF_LINK_ID_RE.test(workflowId)) throw wfLinkError(`invalid workflowId ${JSON.stringify(workflowId)}`);
  if (typeof stageId !== "string" || !WF_LINK_STAGE_RE.test(stageId)) throw wfLinkError(`invalid stageId ${JSON.stringify(stageId)}`);
  if (!Number.isInteger(attempt) || attempt < 1) throw wfLinkError(`attempt must be an integer >= 1 (got ${attempt})`);
  const expectedKey = `wf:${workflowId}:stage:${stageId}:attempt:${attempt}`;
  if (typeof activityIdempotencyKey !== "string" || activityIdempotencyKey !== expectedKey) {
    throw wfLinkError(`activityIdempotencyKey must equal ${JSON.stringify(expectedKey)}`);
  }
  return { workflowId, stageId, attempt, activityIdempotencyKey };
}

// Validate a caller-supplied PRE-FROZEN delivery route (workflow-linked jobs reuse the workflow's route).
// Only the frozen route shape is accepted; a malformed route fail-closes.
export function validateFrozenDeliveryRoute(route) {
  if (!route || typeof route !== "object" || Array.isArray(route)) throw wfLinkError("deliveryRoute must be a frozen route object");
  if (!WF_FROZEN_ROUTE_KINDS.has(route.routeKind)) throw wfLinkError(`deliveryRoute.routeKind must be one of ${[...WF_FROZEN_ROUTE_KINDS].join("|")}`);
  if (typeof route.channel !== "string" || !route.channel) throw wfLinkError("deliveryRoute.channel is required");
  if (typeof route.to !== "string" || !route.to) throw wfLinkError("deliveryRoute.to is required");
  return route;
}

export async function startJob(deps, ctx, params) {
  const { rootDir, config, gatewayCall, createFlow, spawnWorker } = deps;
  if (!ctx.agentId) throw new Error("durable_job requires a trusted tool context or a configured owner");
  const currentJobs = await listJobs(rootDir);
  const activeCount = currentJobs.filter((job) => ["QUEUED", "RUNNING"].includes(job.state)).length;
  if (activeCount >= config.maxConcurrent) {
    throw new Error(`durable job concurrency limit reached (${config.maxConcurrent})`);
  }
  const allowedRoots = [...(ctx.durableAllowedRoots ?? []), ctx.workspaceDir].filter(Boolean);
  const cwd = await resolveAllowedCwd(params.cwd, allowedRoots);
  await assertExecutable(params.command, cwd);
  // Resolve + VALIDATE effective runner metadata BEFORE any side effect (route freeze / flow / job /
  // worker). Explicit runnerType/runnerProfile is never blindly trusted: an incompatible combination
  // (e.g. an `agy` command forced to local, or model_agy on a non-agy command) throws
  // RUNNER_METADATA_INVALID here, so a mis-classified job that would bypass the provider evaluator can
  // never be created. activityType/resultProtocol are derived only from the validated profile.
  const runner = resolveRunnerMetadata({
    command: params.command,
    runnerType: params.runnerType,
    runnerProfile: params.runnerProfile,
  });
  // P3-C: a workflow-linked activity passes the workflow's already-frozen deliveryRoute (validated internal
  // metadata; never caller-supplied from the durable_job tool schema). This lets the workflow reconciler
  // submit a linked job with NO live session. `workflowLink` marks the job as linked (its stage verdict is
  // owned by the workflow reconciler; standalone P1 continuation is suppressed for it).
  const workflowLink = validateWorkflowLink(params.workflowLink);
  // #4: strict-validate validatedExecution BEFORE any side effect (route freeze / flow / job / worker) — throws
  // WORKFLOW_VALIDATED_EXECUTION_INVALID on a standalone injection, malformed hash/path/size, runner mismatch,
  // or cwd/worktree mismatch. params.cwd is the forced workflow worktree (== the job cwd). Kept before the
  // deliveryRoute freeze so a standalone injection is rejected as an invalid execution, not a missing route.
  const validatedExecution = validateValidatedExecution(params.validatedExecution ?? null, { workflowLink, runnerType: runner.runnerType, runnerProfile: runner.runnerProfile, cwd: params.cwd });
  // A pre-frozen deliveryRoute is ONLY accepted for a validated workflow-linked activity (which reuses the
  // workflow's frozen route), and only if it passes the frozen-route schema. A standalone job can never
  // inject a route — it must use the trusted/context-free freeze path below.
  if (params.deliveryRoute !== undefined && params.deliveryRoute !== null) {
    if (!workflowLink) throw wfLinkError("a pre-frozen deliveryRoute requires a valid workflowLink");
    validateFrozenDeliveryRoute(params.deliveryRoute);
  }
  // Freeze the delivery route once, now, before any job/flow/worker is created.
  //  0. Pre-frozen route supplied (workflow-linked activity): use it directly, no session read.
  //  A. Trusted context (session present): resolve from a single chat.history.
  //  B. Context-free (no session): freeze the owner's fixed config deliveryRoute.
  // Either throws DELIVERY_ROUTE_UNAVAILABLE before any side effect.
  const deliveryRoute = workflowLink && params.deliveryRoute
    ? params.deliveryRoute
    : ctx.sessionKey
      ? await resolveCreationRouteBestEffort(gatewayCall, ctx)
      : freezeOwnerConfigRoute(ctx.ownerDeliveryRoute, ctx);
  const id = createJobId();
  const createdAt = nowIso();
  const timeoutSeconds = Number.isInteger(params.timeoutSeconds)
    ? params.timeoutSeconds
    : config.defaultTimeoutSeconds;
  // TaskFlow and the optional ACP wakeup require a session; skip when absent.
  const flow = ctx.sessionKey
    ? createFlow(ctx, {
        jobId: id,
        goal: params.name,
        cwd,
        command: params.command,
        nextAction: params.nextAction ?? null,
      })
    : { flowId: null };
  const directory = jobDir(rootDir, id);
  // Provider-evaluator gating comes from the validated runner profile (above): model → agy-json,
  // local → none. A local activity's plain stdout (which may contain `{"status":"ERROR"}`) is never run
  // through the evaluator.
  const { runnerType, runnerProfile, activityType, resultProtocol } = runner;
  const job = {
    version: 1,
    id,
    name: params.name,
    state: "QUEUED",
    processState: "QUEUED",
    runnerType,
    runnerProfile,
    activityType,
    resultProtocol,
    // P2-A: freeze the effective observability policy for this profile (validated). The worker reads
    // heartbeatIntervalMs; the reconciler reads silenceBudgetMs/stallConfirmMs.
    observability: resolveObservabilityPolicy(config, runnerProfile),
    livenessState: null,
    livenessSince: null,
    cwd,
    command: params.command,
    timeoutSeconds,
    nextAction: params.nextAction ?? "inspect the terminal result and continue the original task",
    createdAt,
    updatedAt: createdAt,
    startedAt: null,
    endedAt: null,
    workerPid: null,
    childPid: null,
    exitCode: null,
    exitSignal: null,
    error: null,
    directory,
    agentId: ctx.agentId ?? null,
    sessionKey: ctx.sessionKey ?? null,
    sessionId: ctx.sessionId ?? null,
    requesterOrigin: ctx.deliveryContext ?? null,
    flowId: flow.flowId,
    // Additive normalized parent linkage (design §6, P0). The legacy top-level fields above are kept
    // unchanged; `parent` also marks this row as a new-format job (drives the new terminal wording).
    parent: {
      agentId: ctx.agentId ?? null,
      sessionKey: ctx.sessionKey ?? null,
      sessionId: ctx.sessionId ?? null,
      requesterOrigin: ctx.deliveryContext ?? null,
      flowId: flow.flowId,
    },
    deliveryRoute,
    // P3-C additive internal linkage (null on standalone jobs). Not exposed by publicJob (which copies only
    // an explicit field list), so the standalone durable_job surface never leaks it. Does NOT affect the
    // job's process/provider outcome.
    workflowLink,
    // P3-F #1/#4: frozen execution expectations the worker re-verifies in its own process right before spawning
    // the child. Strict-validated above (workflow-linked only; a standalone caller has no workflowLink so it is
    // forced null and un-injectable). Not copied by publicJob (internal realpath/hashes stay off the public
    // surface).
    validatedExecution,
    notification: {
      status: "pending",
      attempts: 0,
      idempotencyKey: `durable-job:${id}:terminal`,
      claimedAt: null,
      queuedAt: null,
      nextAttemptAt: null,
      lastError: null,
    },
  };
  job.delivery = initialOutbox(job, config.deliveryMaxAttempts);
  await createJob(rootDir, job);
  try {
    const workerPid = spawnWorker(rootDir, id);
    await updateJob(rootDir, id, (current) => {
      current.workerPid = workerPid;
      return current;
    });
  } catch (error) {
    await updateJob(rootDir, id, (current) => {
      current.state = "FAILED";
      current.endedAt = nowIso();
      current.error = error instanceof Error ? error.message : String(error);
      // Worker never spawned: command-layer failure (new-format jobs carry a parent block).
      if (current.parent) {
        current.processState = "FAILED_COMMAND";
        current.providerState = "UNKNOWN";
        current.jobOutcome = "FAILED_COMMAND";
      }
      current.notification.status = "pending";
      return current;
    });
    throw error;
  }
  return inspectJob(rootDir, id, false);
}

// Default worker launcher: detached child running worker.js against the job row.
export function spawnWorkerProcess(rootDir, id) {
  const worker = spawn(process.execPath, [workerPath, rootDir, id], {
    detached: true,
    stdio: "ignore",
    env: process.env,
  });
  worker.unref();
  return worker.pid;
}

// Default managed-flow creator, backed by the OpenClaw Task Flow runtime.
export function makeFlowCreator(api) {
  return (ctx, init) =>
    api.runtime.taskFlow.fromToolContext(ctx).createManaged({
      controllerId: CONTROLLER_ID,
      goal: init.goal,
      status: "running",
      notifyPolicy: "silent",
      currentStep: "command",
      stateJson: {
        jobId: init.jobId,
        cwd: init.cwd,
        command: init.command,
        nextAction: init.nextAction ?? null,
      },
    });
}

// Ownership for list: trusted context filters by the caller's session/agent; a
// context-free call must supply a workspace cwd selector, which selects the
// owner (sessionKey stays null). It can never enumerate another owner's jobs.
export function resolveListFilter(config, ctx, params = {}) {
  if (ctx.sessionKey) return { agentId: ctx.agentId, sessionKey: ctx.sessionKey };
  if (!params.cwd) {
    const error = new Error("context-free durable_job list requires a workspace cwd selector");
    error.code = "LIST_SELECTOR_REQUIRED";
    throw error;
  }
  const owner = resolveOwnerContext(config, ctx, { cwd: params.cwd });
  return { agentId: owner.agentId, sessionKey: owner.sessionKey };
}

// Ownership for status/cancel: a trusted call keeps the session+agent check; a
// context-free call authorizes from the JOB'S OWN stored cwd (never a
// caller-supplied cwd), then requires the job's agentId to match that owner.
export function authorizeJobAccess(config, ctx, job) {
  if (ctx.sessionKey) {
    assertJobOwner(job, ctx);
    return;
  }
  const owner = resolveOwnerContext(config, {}, { cwd: job.cwd });
  if (job.agentId !== owner.agentId) {
    throw new Error("durable job is not owned by the agent configured for its workspace");
  }
}

export async function cancelJob(rootDir, jobId) {
  const job = await readJob(rootDir, jobId);
  if (TERMINAL_STATES.has(job.state)) return inspectJob(rootDir, jobId, false);
  if (isProcessAlive(job.childPid)) signalProcessGroup(job.childPid, "SIGTERM");
  if (isProcessAlive(job.workerPid)) {
    try {
      process.kill(job.workerPid, "SIGTERM");
    } catch (error) {
      if (error?.code !== "ESRCH") throw error;
    }
  }
  await updateJob(rootDir, jobId, (current) => {
    if (TERMINAL_STATES.has(current.state)) return null;
    current.state = "CANCELLED";
    current.endedAt = nowIso();
    current.error = "cancelled by durable_job caller";
    // New-format jobs (with a parent block) get the separated outcome; CANCELLED → CANCELLED (verdict.js).
    if (current.parent) {
      current.processState = "CANCELLED";
      current.providerState = "UNKNOWN";
      current.jobOutcome = "CANCELLED";
    }
    current.notification.status = "pending";
    return current;
  });
  return inspectJob(rootDir, jobId, false);
}

