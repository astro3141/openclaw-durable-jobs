import { randomUUID } from "node:crypto";
import { nowIso } from "./job-store.js";
import { OUTCOME_ICONS } from "./verdict.js";

// Persistent delivery outbox for durable-job terminal notifications.
//
// Guarantee level (see README):
//   Persistent best-effort delivery with retries for confirmed failures and
//   explicit parking of ambiguous outcomes. Not exactly-once and not guaranteed
//   at-least-once Slack delivery. A send whose result cannot be confirmed is
//   parked in DELIVERY_UNKNOWN and is NOT auto-resent (that would risk a
//   duplicate), so a genuine ambiguous crash can under-deliver.
//   The frozen delivery route captured at job creation is authoritative;
//   completion delivery never re-reads the session, session key, or
//   chat.history and never re-resolves the route.
//
// State machine:
//   PENDING -> SENDING -> GATEWAY_ACCEPTED_UNCONFIRMED
//                      \-> DELIVERED
// Error / ambiguous:
//   FAILED_RETRYABLE | FAILED_FINAL | DELIVERY_UNKNOWN

export const OUTBOX_STATES = new Set([
  "PENDING",
  "SENDING",
  "GATEWAY_ACCEPTED_UNCONFIRMED",
  "DELIVERED",
  "FAILED_RETRYABLE",
  "FAILED_FINAL",
  "DELIVERY_UNKNOWN",
]);

// States that require no further automatic action AND are genuinely done.
// GATEWAY_ACCEPTED_UNCONFIRMED is deliberately NOT here: it is a distinct,
// operator-visible state (runId only, no provider messageId) that must not be
// hidden as "resolved".
export const OUTBOX_RESOLVED = new Set(["DELIVERED", "FAILED_FINAL"]);

// Default lease for an in-flight send. Only a lease-expired SENDING record is
// treated as stale; a fresh SENDING is a concurrent attempt, never resent.
export const DEFAULT_SEND_LEASE_MS = 30_000;

const DELIVERY_ICONS = {
  SUCCEEDED: "✅", // ✅
  FAILED: "❌", // ❌
  TIMED_OUT: "⏱", // ⏱
  CANCELLED: "🚫", // 🚫
  LOST: "⚠️", // ⚠️
};

export function deliveryIdFor(job) {
  const terminalKey = job?.notification?.idempotencyKey ?? `durable-job:${job.id}:terminal`;
  return `${terminalKey}:delivery`;
}

function backoffMs(attempts) {
  const n = Math.max(1, attempts);
  return Math.min(60_000, 2_000 * 2 ** Math.min(n - 1, 5));
}

export function initialOutbox(job, maxAttempts) {
  const ts = nowIso();
  return {
    deliveryId: deliveryIdFor(job),
    idempotencyKey: deliveryIdFor(job),
    state: "PENDING",
    attempts: 0,
    maxAttempts,
    gatewayRunId: null,
    messageId: null,
    lastError: null,
    createdAt: ts,
    updatedAt: ts,
    nextAttemptAt: null,
    sentStartedAt: null,
  };
}

// Deterministic terminal message. Includes the Job ID and delivery ID so an
// operator (or a future Slack-history dedupe check) can recognise a resend.
export function buildTerminalDeliveryMessage(job) {
  // New-format jobs (P0, identified by a set job_outcome) report the separated outcome — never a raw
  // "SUCCEEDED". Legacy jobs (no job_outcome) keep the exact state-based wording unchanged.
  const isNewFormat = typeof job.jobOutcome === "string";
  const header = isNewFormat
    ? `${OUTCOME_ICONS[job.jobOutcome] ?? "ℹ️"} Durable job ${job.jobOutcome}: ${job.name}`
    : `${DELIVERY_ICONS[job.state] ?? "ℹ️"} Durable job ${job.state}: ${job.name}`; // ℹ️
  const lines = [header, `job_id=${job.id}`, `delivery_id=${deliveryIdFor(job)}`];
  if (isNewFormat) {
    lines.push(`process_state=${job.processState ?? "unknown"}`);
    lines.push(`provider_state=${job.providerState ?? "unknown"}`);
  }
  if (Number.isInteger(job.exitCode)) lines.push(`exit_code=${job.exitCode}`);
  if (job.exitSignal) lines.push(`signal=${job.exitSignal}`);
  if (job.error) lines.push(`error=${job.error}`);
  lines.push(`ended_at=${job.endedAt ?? "unknown"}`);
  lines.push(`Inspect job.json and logs directly at ${job.directory}`);
  lines.push("(runtime-generated durable-job completion notice)");
  return lines.join("\n");
}

// Build the `gateway call send` payload from the FROZEN route only.
// Returns { ok, payload } or { ok:false, reason } — never touches the session.
// The route kind (frozen at creation from trusted metadata) decides thread vs
// channel-root delivery; the model never chooses this at delivery time.
export function buildDeliveryPayload(job) {
  const route = job.deliveryRoute;
  const record = job.delivery;
  if (!route || !route.channel || !route.to) {
    return { ok: false, reason: "DELIVERY_ROUTE_UNAVAILABLE" };
  }
  if (route.routeKind === "thread") {
    if (!route.threadId) return { ok: false, reason: "DELIVERY_ROUTE_UNAVAILABLE" };
  } else if (route.routeKind !== "channel_root") {
    // "unknown" (or anything unexpected) is never deliverable.
    return { ok: false, reason: "DELIVERY_ROUTE_UNAVAILABLE" };
  }
  // The address is derived ONLY from the frozen route (channel, to, routeKind,
  // threadId, accountId, agentId) plus the deterministic idempotencyKey.
  // sessionKey is intentionally NOT part of the send payload: the deterministic
  // outbox must produce the same address even if the session key changes, is
  // dropped, or expires. sessionKey is used only for ownership/status, TaskFlow,
  // and the optional ACP follow-up.
  const payload = {
    to: route.to,
    channel: route.channel,
    message: buildTerminalDeliveryMessage(job),
    idempotencyKey: record?.idempotencyKey ?? deliveryIdFor(job),
    ...(route.routeKind === "thread" && route.threadId ? { threadId: route.threadId } : {}),
    ...(route.accountId ? { accountId: route.accountId } : {}),
    ...(route.agentId ? { agentId: route.agentId } : {}),
  };
  return { ok: true, payload };
}

// Classify a `gateway call send` result into a delivery confidence level.
export function classifySendResult(result) {
  if (result && result.messageId) return "DELIVERED";
  if (result && result.runId) return "GATEWAY_ACCEPTED";
  return "DELIVERY_UNKNOWN";
}

// Decide whether a reconcile tick may attempt a send for this record.
// Returns one of: "send" | "skip" | "stale_sending" | "resolved".
// A SENDING record is only "stale" once its lease has expired; a fresh SENDING
// is a concurrent in-flight attempt and is skipped (never resent, never marked
// unknown), which is what protects against overlapping reconcile ticks.
export function classifyForTick(record, { now = Date.now() } = {}) {
  if (!record) return "skip";
  if (OUTBOX_RESOLVED.has(record.state)) return "resolved";
  if (record.state === "GATEWAY_ACCEPTED_UNCONFIRMED") return "skip"; // exposed, no auto-resend
  if (record.state === "SENDING") {
    const leaseUntil = Date.parse(record.sendingLeaseUntil ?? 0) || 0;
    return now >= leaseUntil ? "stale_sending" : "skip";
  }
  if (record.state === "DELIVERY_UNKNOWN") return "skip"; // resend is an explicit policy
  // PENDING or FAILED_RETRYABLE
  if (record.nextAttemptAt && Date.parse(record.nextAttemptAt) > now) return "skip";
  return "send";
}

// Transition a record into SENDING (claim) with a bounded lease. Returns a new
// record or null if the record is not currently claimable.
export function claimSending(record, { leaseMs = DEFAULT_SEND_LEASE_MS, attemptId, now = Date.now() } = {}) {
  if (!record) return null;
  // DELIVERY_UNKNOWN is deliberately NOT claimable: an ambiguous outcome is a
  // manual, operator-verified case and is never auto-resent in this PoC.
  if (record.state !== "PENDING" && record.state !== "FAILED_RETRYABLE") {
    return null;
  }
  return {
    ...record,
    state: "SENDING",
    attempts: (record.attempts ?? 0) + 1,
    sendingStartedAt: new Date(now).toISOString(),
    sendingLeaseUntil: new Date(now + leaseMs).toISOString(),
    sendingAttemptId: attemptId ?? randomUUID(),
    nextAttemptAt: null,
    updatedAt: nowIso(),
  };
}

// Apply a successful `gateway call send` result to the record.
export function applySendResult(record, result) {
  const confidence = classifySendResult(result);
  const base = {
    ...record,
    gatewayRunId: result?.runId ?? record.gatewayRunId ?? null,
    messageId: result?.messageId ?? record.messageId ?? null,
    lastError: null,
    updatedAt: nowIso(),
  };
  if (confidence === "DELIVERED") {
    return { ...base, state: "DELIVERED", nextAttemptAt: null };
  }
  if (confidence === "GATEWAY_ACCEPTED") {
    // The Gateway accepted the send but returned no provider messageId, so
    // actual Slack delivery is unconfirmed. Kept visible, not auto-resent.
    return { ...base, state: "GATEWAY_ACCEPTED_UNCONFIRMED", nextAttemptAt: null };
  }
  return { ...base, state: "DELIVERY_UNKNOWN", nextAttemptAt: null };
}

// Apply a send failure. Retryable until maxAttempts, then FAILED_FINAL.
export function applySendError(record, error) {
  const message = error instanceof Error ? error.message : String(error);
  const attempts = record.attempts ?? 0;
  const exhausted = attempts >= (record.maxAttempts ?? 0);
  return {
    ...record,
    state: exhausted ? "FAILED_FINAL" : "FAILED_RETRYABLE",
    lastError: message,
    nextAttemptAt: exhausted ? null : new Date(Date.now() + backoffMs(attempts)).toISOString(),
    updatedAt: nowIso(),
  };
}

// A stale SENDING record means the process died mid-send: the outcome is
// genuinely unknown. Per the PoC policy we do NOT blindly resend; we mark it
// DELIVERY_UNKNOWN and leave any resend to an explicit operator policy.
export function markStaleSendingUnknown(record) {
  return {
    ...record,
    state: "DELIVERY_UNKNOWN",
    lastError: "process restarted while a send was in flight; delivery outcome is ambiguous",
    nextAttemptAt: null,
    updatedAt: nowIso(),
  };
}
