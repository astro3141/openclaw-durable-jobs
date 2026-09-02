// P1 continuation state machine (design §8), scoped to the conditions the live spike PROVED:
//   - new-format standalone job (has a parent block)
//   - explicit parent.sessionKey (incl. the canonical channel-default binding key)
//   - Gateway chat.send with deliver:false and a deterministic idempotencyKey
//
// It is deliberately conservative about the conditions the spike did NOT prove (abrupt backend loss /
// clean-closed-session auto-recreation): a chat.send that returns "started" is only DISPATCHED, never
// COMPLETED. Completion must be independently CORRELATED to THIS continuation's unique marker in
// chat.history within a bounded timeout; on timeout there is NO unbounded auto-retry — one dispatch by
// default, then a single structured Slack fallback notice inviting manual re-entry.
//
// This path is SEPARATE from the user-facing terminal notification (the delivery outbox) and from the
// legacy completionAcpWakeup path (which is preserved unchanged for legacy jobs).

import { randomUUID } from "node:crypto";
import { nowIso } from "./job-store.js";
import { terminalCompletionMessage } from "./completion-turn.js";

export const CONTINUATION_STATES = new Set([
  "PENDING",
  "DISPATCHING",
  "DISPATCHED",
  "COMPLETED",
  "FAILED",
  "TIMED_OUT",
  "MANUAL_FALLBACK",
]);

// States that need no further automatic continuation action.
export const CONTINUATION_RESOLVED = new Set(["COMPLETED", "FAILED", "TIMED_OUT", "MANUAL_FALLBACK"]);

// States for which a one-time structured Slack fallback notice is owed.
export const CONTINUATION_FALLBACK_STATES = new Set(["FAILED", "TIMED_OUT", "MANUAL_FALLBACK"]);

export function continuationIdFor(job) {
  return `durable-job:${job.id}:continuation`;
}

export function continuationFallbackIdFor(job) {
  return `durable-job:${job.id}:continuation-fallback`;
}

export function initialContinuation(job) {
  const ts = nowIso();
  return {
    idempotencyKey: continuationIdFor(job),
    state: "PENDING",
    attempts: 0,
    maxAttempts: 1, // conservative: one dispatch; on timeout → fallback, no unbounded re-dispatch
    marker: null, // unique per-dispatch correlation token (set at claim)
    sessionKey: null, // recorded at claim for correlation/audit
    runId: null,
    dispatchedAt: null,
    dispatchLeaseUntil: null,
    lastCheckedAt: null,
    lastCheckError: null,
    completedAt: null,
    reason: null, // NO_SESSION_KEY | DISPATCH_FAILED | TURN_TIMEOUT
    lastError: null,
    // Fallback notice sub-state — mirrors the delivery outbox's conservative posture (the gateway `send`
    // is NOT a proven exactly-once Slack dedup; see README delivery-guarantee section). A crash after a
    // send but before the DELIVERED record is AMBIGUOUS → parked DELIVERY_UNKNOWN, never blind-resent, so
    // the user can never receive a duplicate notice. A send that errors in-process (did not post) is
    // retried with bounded backoff.
    fallbackState: null, // null(PENDING) | SENDING | DELIVERED | DELIVERY_UNKNOWN | FAILED
    fallbackLeaseUntil: null,
    fallbackAttempts: 0,
    fallbackMaxAttempts: 5,
    fallbackNextAttemptAt: null,
    fallbackLastError: null,
    fallbackLastAttemptAt: null,
    fallbackAt: null,
    fallbackReason: null,
    createdAt: ts,
    updatedAt: ts,
  };
}

// The deterministic continuation turn injected into the Supervisor session, plus a UNIQUE per-dispatch
// marker so its completion can be correlated exactly (not by "some assistant reply exists"). Reuses the
// outcome-based terminal message so a FAILED_PROVIDER / COMPLETED_UNVERIFIED / etc. job all trigger review.
export function continuationMessage(job) {
  const base = terminalCompletionMessage(job);
  const marker = job.continuation?.marker;
  return marker ? `${base}\ncontinuation_marker=${marker}` : base;
}

// chat.send payload — sessionKey-targeted, deliver:false, deterministic idempotencyKey. No Slack post.
export function buildContinuationTurn(job) {
  const sessionKey = job.parent?.sessionKey ?? null;
  if (!sessionKey) return { ok: false, reason: "NO_SESSION_KEY" };
  const payload = {
    sessionKey,
    message: continuationMessage(job),
    deliver: false,
    idempotencyKey: continuationIdFor(job),
    ...(job.agentId ? { agentId: job.agentId } : job.parent?.agentId ? { agentId: job.parent.agentId } : {}),
  };
  return { ok: true, payload };
}

// One-time structured fallback notice, delivered to the FROZEN route (same shape as the outbox payload)
// with a DETERMINISTIC idempotencyKey so the gateway dedups even across a crash-retry.
export function buildContinuationFallbackPayload(job, reason) {
  const route = job.deliveryRoute;
  if (!route || !route.channel || !route.to) return { ok: false, reason: "DELIVERY_ROUTE_UNAVAILABLE" };
  if (route.routeKind === "thread" && !route.threadId) return { ok: false, reason: "DELIVERY_ROUTE_UNAVAILABLE" };
  if (route.routeKind !== "thread" && route.routeKind !== "channel_root") {
    return { ok: false, reason: "DELIVERY_ROUTE_UNAVAILABLE" };
  }
  const message = [
    "CONTINUATION_READY",
    `job_id=${job.id}`,
    `outcome=${job.jobOutcome ?? "unknown"}`,
    `reason=${reason}`,
    'next_action=Send "결과 리뷰" to resume Supervisor review.',
  ].join("\n");
  const payload = {
    to: route.to,
    channel: route.channel,
    message,
    idempotencyKey: continuationFallbackIdFor(job),
    ...(route.routeKind === "thread" && route.threadId ? { threadId: route.threadId } : {}),
    ...(route.accountId ? { accountId: route.accountId } : {}),
    ...(route.agentId ? { agentId: route.agentId } : {}),
  };
  return { ok: true, payload };
}

// Decide what a reconcile tick should do for this continuation record.
// "skip" | "resolved" | "manual_fallback_no_session" | "dispatch" | "check_completion" | "dispatch_stale".
export function classifyContinuationForTick(record, job, { now = Date.now() } = {}) {
  if (!record) return "skip";
  if (CONTINUATION_RESOLVED.has(record.state)) return "resolved";
  if (record.state === "DISPATCHING") {
    const leaseUntil = Date.parse(record.dispatchLeaseUntil ?? 0) || 0;
    return now >= leaseUntil ? "dispatch_stale" : "skip";
  }
  if (record.state === "PENDING") {
    if (!job.parent?.sessionKey) return "manual_fallback_no_session";
    return "dispatch";
  }
  if (record.state === "DISPATCHED") return "check_completion";
  return "skip";
}

// Claim PENDING → DISPATCHING with a bounded lease, minting the unique marker + recording the sessionKey.
export function claimDispatching(record, job, { leaseMs, now = Date.now() } = {}) {
  if (!record || record.state !== "PENDING") return null;
  return {
    ...record,
    state: "DISPATCHING",
    attempts: (record.attempts ?? 0) + 1,
    marker: record.marker ?? randomUUID(),
    sessionKey: job.parent?.sessionKey ?? null,
    dispatchLeaseUntil: new Date(now + leaseMs).toISOString(),
    updatedAt: nowIso(),
  };
}

// chat.send accepted the turn → DISPATCHED (NOT completed).
export function applyDispatched(record, result, { now = Date.now() } = {}) {
  return {
    ...record,
    state: "DISPATCHED",
    runId: result?.runId ?? record.runId ?? null,
    dispatchedAt: new Date(now).toISOString(),
    dispatchLeaseUntil: null,
    lastError: null,
    updatedAt: nowIso(),
  };
}

export function applyCompleted(record) {
  return { ...record, state: "COMPLETED", completedAt: nowIso(), updatedAt: nowIso() };
}

export function applyFailed(record, error, reason = "DISPATCH_FAILED") {
  return {
    ...record,
    state: "FAILED",
    reason,
    lastError: error instanceof Error ? error.message : String(error),
    dispatchLeaseUntil: null,
    updatedAt: nowIso(),
  };
}

export function applyTimedOut(record, reason = "TURN_TIMEOUT") {
  return { ...record, state: "TIMED_OUT", reason, dispatchLeaseUntil: null, updatedAt: nowIso() };
}

export function applyManualFallback(record, reason = "NO_SESSION_KEY") {
  return { ...record, state: "MANUAL_FALLBACK", reason, updatedAt: nowIso() };
}

// Record a transient completion-check outcome (history error or a not-yet-complete probe) without leaving
// DISPATCHED. Keeps the continuation alive until the bounded timeout.
export function recordCheck(record, { error = null, now = Date.now() } = {}) {
  return {
    ...record,
    lastCheckedAt: new Date(now).toISOString(),
    lastCheckError: error ? (error instanceof Error ? error.message : String(error)) : null,
    updatedAt: nowIso(),
  };
}

// Minimal message-text extractor (mirrors completion-turn.js, kept local to avoid coupling).
function messageText(message) {
  if (typeof message?.content === "string") return message.content;
  if (!Array.isArray(message?.content)) return "";
  return message.content
    .filter((part) => part?.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n");
}

// Correlate completion to THIS continuation's unique marker: find the user turn that carries the exact
// `continuation_marker=<marker>` token, then return the first assistant reply AFTER it (bounded by the
// next user turn). Unrelated replies, past markers, or a marked turn pushed out of the history window all
// correctly yield "not completed" rather than a false COMPLETED.
export function correlatedReply(history, marker) {
  if (!marker || !history || !Array.isArray(history.messages)) return undefined;
  const token = `continuation_marker=${marker}`;
  let userIndex = -1;
  for (let i = history.messages.length - 1; i >= 0; i -= 1) {
    const m = history.messages[i];
    if (m?.role === "user" && messageText(m).includes(token)) {
      userIndex = i;
      break;
    }
  }
  if (userIndex < 0) return undefined;
  for (let i = userIndex + 1; i < history.messages.length; i += 1) {
    const m = history.messages[i];
    if (m?.role === "user") break; // a later user turn closes this continuation's window
    if (m?.role === "assistant") {
      const text = messageText(m).trim();
      if (text) return text;
    }
  }
  return undefined;
}

export function continuationCompleted(history, record) {
  return typeof correlatedReply(history, record?.marker) === "string";
}

// Whether a DISPATCHED continuation has exceeded its bounded completion window.
export function continuationTimedOut(record, { timeoutMs, now = Date.now() } = {}) {
  const dispatchedAt = Date.parse(record.dispatchedAt ?? 0) || 0;
  return dispatchedAt > 0 && now - dispatchedAt >= timeoutMs;
}

// States that need no further fallback send.
export const FALLBACK_RESOLVED = new Set(["DELIVERED", "DELIVERY_UNKNOWN", "FAILED"]);

// Decide what a tick should do for the fallback notice:
// "resolved" | "skip" | "park_unknown" (ambiguous crash) | "send".
export function classifyFallbackForTick(record, { now = Date.now() } = {}) {
  if (!record) return "skip";
  const fs = record.fallbackState;
  if (FALLBACK_RESOLVED.has(fs)) return "resolved";
  if (fs === "SENDING") {
    const leaseUntil = Date.parse(record.fallbackLeaseUntil ?? 0) || 0;
    // A lease-expired SENDING means the process died around the send: the outcome is genuinely unknown.
    // Park it (never blind-resend) so the user cannot get a duplicate.
    return now >= leaseUntil ? "park_unknown" : "skip";
  }
  // null (never started) or "PENDING" (retry) — honour backoff.
  if (record.fallbackNextAttemptAt && Date.parse(record.fallbackNextAttemptAt) > now) return "skip";
  return "send";
}

// Claim → SENDING with a bounded lease (attempts++). Only call after classify returned "send".
export function claimFallbackSending(record, { leaseMs, reason, now = Date.now() } = {}) {
  return {
    ...record,
    fallbackState: "SENDING",
    fallbackReason: reason ?? record.fallbackReason ?? record.reason ?? null,
    fallbackLeaseUntil: new Date(now + leaseMs).toISOString(),
    fallbackAttempts: (record.fallbackAttempts ?? 0) + 1,
    fallbackLastAttemptAt: new Date(now).toISOString(),
    fallbackNextAttemptAt: null,
    updatedAt: nowIso(),
  };
}

export function applyFallbackDelivered(record) {
  return {
    ...record,
    fallbackState: "DELIVERED",
    fallbackLeaseUntil: null,
    fallbackLastError: null,
    fallbackAt: nowIso(),
    updatedAt: nowIso(),
  };
}

// A send error is only RETRYABLE when it is STRUCTURALLY PROVEN that the request was rejected before it
// could be processed (so it definitely did not post). A timeout, connection reset, interruption, or a
// response-parse failure leaves it UNKNOWN whether the gateway/Slack already handled the message — those
// (and any unclassified error) are parked DELIVERY_UNKNOWN and NEVER auto-resent, to avoid a duplicate
// user notice. The gateway-call layer does not surface such proof today, so in practice every send
// exception is ambiguous → DELIVERY_UNKNOWN; the retry path exists only for an explicit future signal.
// We never GUESS retryability from an error message.
export function isProvenPreSendRejection(error) {
  return error?.preSendRejected === true || error?.code === "PRE_SEND_REJECTED";
}

export function applyFallbackError(record, error) {
  const message = error instanceof Error ? error.message : String(error);
  if (!isProvenPreSendRejection(error)) {
    // Ambiguous transport failure — outcome unknown, park (no auto-resend).
    return {
      ...record,
      fallbackState: "DELIVERY_UNKNOWN",
      fallbackLeaseUntil: null,
      fallbackLastError: message,
      updatedAt: nowIso(),
    };
  }
  // Proven pre-send rejection → bounded retry.
  const attempts = record.fallbackAttempts ?? 1;
  const max = record.fallbackMaxAttempts ?? 5;
  if (attempts >= max) {
    return { ...record, fallbackState: "FAILED", fallbackLeaseUntil: null, fallbackLastError: message, updatedAt: nowIso() };
  }
  const backoff = Math.min(60_000, 2_000 * 2 ** Math.min(attempts - 1, 5));
  return {
    ...record,
    fallbackState: "PENDING",
    fallbackLeaseUntil: null,
    fallbackLastError: message,
    fallbackNextAttemptAt: new Date(Date.now() + backoff).toISOString(),
    updatedAt: nowIso(),
  };
}

// A lease-expired SENDING (ambiguous crash) → parked, never resent.
export function markFallbackUnknown(record) {
  return {
    ...record,
    fallbackState: "DELIVERY_UNKNOWN",
    fallbackLeaseUntil: null,
    fallbackLastError: "process restarted while a fallback send was in flight; delivery outcome is ambiguous",
    updatedAt: nowIso(),
  };
}
