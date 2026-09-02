import assert from "node:assert/strict";
import test from "node:test";

import {
  applySendError,
  applySendResult,
  buildDeliveryPayload,
  buildTerminalDeliveryMessage,
  claimSending,
  classifyForTick,
  classifySendResult,
  deliveryIdFor,
  initialOutbox,
  markStaleSendingUnknown,
} from "../dist/delivery-outbox.js";

function baseJob(overrides = {}) {
  return {
    id: "job-12345678",
    name: "verify-suite",
    state: "SUCCEEDED",
    directory: "/state/durable-jobs/job-12345678",
    exitCode: 0,
    endedAt: "2026-08-03T10:00:00.000Z",
    sessionKey: "agent:infra-scanner-openclaw:acp:binding:slack:default:abc",
    notification: { idempotencyKey: "durable-job:job-12345678:terminal" },
    deliveryRoute: {
      routeKind: "thread",
      channel: "slack",
      to: "channel:C0EXAMPLE001",
      threadId: "1785661934.163229",
      accountId: "default",
      agentId: "infra-scanner-openclaw",
    },
    ...overrides,
  };
}

test("deliveryIdFor is deterministic per terminal job", () => {
  assert.equal(deliveryIdFor(baseJob()), "durable-job:job-12345678:terminal:delivery");
});

test("terminal message is deterministic and carries job + delivery ids", () => {
  const msg = buildTerminalDeliveryMessage(baseJob());
  assert.match(msg, /Durable job SUCCEEDED: verify-suite/);
  assert.match(msg, /job_id=job-12345678/);
  assert.match(msg, /delivery_id=durable-job:job-12345678:terminal:delivery/);
  assert.match(msg, /exit_code=0/);
  assert.equal(msg, buildTerminalDeliveryMessage(baseJob()));

  const failed = buildTerminalDeliveryMessage(
    baseJob({ state: "FAILED", exitCode: 1, error: "command exited with code 1" }),
  );
  assert.match(failed, /Durable job FAILED: verify-suite/);
  assert.match(failed, /error=command exited with code 1/);
});

test("initialOutbox starts PENDING with the deterministic idempotency key", () => {
  const outbox = initialOutbox(baseJob(), 8);
  assert.equal(outbox.state, "PENDING");
  assert.equal(outbox.attempts, 0);
  assert.equal(outbox.maxAttempts, 8);
  assert.equal(outbox.idempotencyKey, "durable-job:job-12345678:terminal:delivery");
  assert.equal(outbox.messageId, null);
});

test("buildDeliveryPayload uses the frozen thread route only", () => {
  const job = baseJob();
  job.delivery = initialOutbox(job, 8);
  const built = buildDeliveryPayload(job);
  assert.equal(built.ok, true);
  assert.equal(built.payload.to, "channel:C0EXAMPLE001");
  assert.equal(built.payload.channel, "slack");
  assert.equal(built.payload.threadId, "1785661934.163229");
  assert.equal(built.payload.accountId, "default");
  assert.equal(built.payload.idempotencyKey, "durable-job:job-12345678:terminal:delivery");
  assert.match(built.payload.message, /job_id=job-12345678/);
  // sessionKey must never appear in the send payload.
  assert.equal("sessionKey" in built.payload, false);
});

test("send payload is independent of sessionKey (changed, or null)", () => {
  const withKey = baseJob({ sessionKey: "agent:x:acp:binding:slack:default:AAA" });
  const otherKey = baseJob({ sessionKey: "agent:x:acp:binding:slack:default:ZZZ" });
  const noKey = baseJob({ sessionKey: null });
  const a = buildDeliveryPayload(withKey).payload;
  const b = buildDeliveryPayload(otherKey).payload;
  const c = buildDeliveryPayload(noKey).payload;
  assert.deepEqual(a, b);
  assert.deepEqual(a, c);
  for (const payload of [a, b, c]) assert.equal("sessionKey" in payload, false);
});

test("buildDeliveryPayload delivers a channel_root route without a threadId", () => {
  const rooted = baseJob({
    deliveryRoute: { routeKind: "channel_root", channel: "slack", to: "channel:C1", threadId: null },
  });
  const okBuilt = buildDeliveryPayload(rooted);
  assert.equal(okBuilt.ok, true);
  assert.equal("threadId" in okBuilt.payload, false);
  assert.equal(okBuilt.payload.to, "channel:C1");
});

test("buildDeliveryPayload rejects a thread route missing its threadId, or an unknown route", () => {
  const brokenThread = baseJob({
    deliveryRoute: { routeKind: "thread", channel: "slack", to: "channel:C1", threadId: null },
  });
  assert.equal(buildDeliveryPayload(brokenThread).ok, false);

  const unknown = baseJob({ deliveryRoute: { routeKind: "unknown", channel: "slack", to: "channel:C1" } });
  assert.equal(buildDeliveryPayload(unknown).ok, false);

  const noChannel = baseJob({ deliveryRoute: { routeKind: "thread", to: "channel:C1", threadId: "1.2" } });
  assert.equal(buildDeliveryPayload(noChannel).ok, false);
});

test("classifySendResult maps gateway result to a confidence level", () => {
  assert.equal(classifySendResult({ runId: "r", messageId: "1785.1" }), "DELIVERED");
  assert.equal(classifySendResult({ runId: "r" }), "GATEWAY_ACCEPTED");
  assert.equal(classifySendResult({}), "DELIVERY_UNKNOWN");
  assert.equal(classifySendResult(undefined), "DELIVERY_UNKNOWN");
});

test("claimSending sets a bounded lease and only claims claimable states", () => {
  const pending = initialOutbox(baseJob(), 8);
  const now = Date.parse("2026-08-03T12:00:00.000Z");
  const claimed = claimSending(pending, { leaseMs: 30_000, now, attemptId: "att-1" });
  assert.equal(claimed.state, "SENDING");
  assert.equal(claimed.attempts, 1);
  assert.equal(claimed.sendingStartedAt, "2026-08-03T12:00:00.000Z");
  assert.equal(claimed.sendingLeaseUntil, "2026-08-03T12:00:30.000Z");
  assert.equal(claimed.sendingAttemptId, "att-1");
  assert.equal(claimSending({ ...pending, state: "DELIVERED" }), null);
  assert.equal(claimSending({ ...pending, state: "GATEWAY_ACCEPTED_UNCONFIRMED" }), null);
  assert.equal(claimSending({ ...pending, state: "SENDING" }), null);
  // DELIVERY_UNKNOWN is never auto-resent.
  assert.equal(claimSending({ ...pending, state: "DELIVERY_UNKNOWN" }), null);
  // FAILED_RETRYABLE is claimable (retry of a confirmed failure).
  assert.equal(claimSending({ ...pending, state: "FAILED_RETRYABLE" })?.state, "SENDING");
});

test("applySendResult records DELIVERED / GATEWAY_ACCEPTED_UNCONFIRMED / DELIVERY_UNKNOWN", () => {
  const sending = claimSending(initialOutbox(baseJob(), 8));
  const delivered = applySendResult(sending, { runId: "r1", messageId: "1785.9" });
  assert.equal(delivered.state, "DELIVERED");
  assert.equal(delivered.messageId, "1785.9");
  assert.equal(delivered.gatewayRunId, "r1");

  assert.equal(applySendResult(sending, { runId: "r1" }).state, "GATEWAY_ACCEPTED_UNCONFIRMED");
  assert.equal(applySendResult(sending, {}).state, "DELIVERY_UNKNOWN");
});

test("applySendError retries with backoff until attempts are exhausted", () => {
  let record = claimSending(initialOutbox(baseJob(), 2)); // attempts=1, max=2
  record = applySendError(record, new Error("gateway timeout"));
  assert.equal(record.state, "FAILED_RETRYABLE");
  assert.equal(record.lastError, "gateway timeout");
  assert.equal(typeof record.nextAttemptAt, "string");

  record = claimSending(record); // attempts=2
  record = applySendError(record, new Error("gateway timeout again"));
  assert.equal(record.state, "FAILED_FINAL");
  assert.equal(record.nextAttemptAt, null);
});

test("classifyForTick disposes each state, keying SENDING off the lease", () => {
  const now = Date.parse("2026-08-03T12:00:00.000Z");
  assert.equal(classifyForTick({ state: "PENDING" }, { now }), "send");
  assert.equal(classifyForTick({ state: "DELIVERED" }, { now }), "resolved");
  assert.equal(classifyForTick({ state: "FAILED_FINAL" }, { now }), "resolved");
  // Unconfirmed is exposed, not hidden as resolved, and not auto-resent.
  assert.equal(classifyForTick({ state: "GATEWAY_ACCEPTED_UNCONFIRMED" }, { now }), "skip");
  assert.equal(classifyForTick({ state: "DELIVERY_UNKNOWN" }, { now }), "skip");
  // Fresh lease -> concurrent attempt -> skip. Expired lease -> stale.
  assert.equal(
    classifyForTick({ state: "SENDING", sendingLeaseUntil: "2026-08-03T12:00:30.000Z" }, { now }),
    "skip",
  );
  assert.equal(
    classifyForTick({ state: "SENDING", sendingLeaseUntil: "2026-08-03T11:59:59.000Z" }, { now }),
    "stale_sending",
  );
  assert.equal(
    classifyForTick({ state: "FAILED_RETRYABLE", nextAttemptAt: "2026-08-03T12:05:00.000Z" }, { now }),
    "skip",
  );
  assert.equal(
    classifyForTick({ state: "FAILED_RETRYABLE", nextAttemptAt: "2026-08-03T11:55:00.000Z" }, { now }),
    "send",
  );
});

test("stale SENDING becomes DELIVERY_UNKNOWN, never a blind resend", () => {
  const unknown = markStaleSendingUnknown({ state: "SENDING", attempts: 1 });
  assert.equal(unknown.state, "DELIVERY_UNKNOWN");
  assert.match(unknown.lastError, /ambiguous/);
  assert.equal(unknown.nextAttemptAt, null);
});
