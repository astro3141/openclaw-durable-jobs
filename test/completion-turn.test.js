import assert from "node:assert/strict";
import test from "node:test";

import {
  buildCompletionTurn,
  classifyRouteKind,
  extractCompletionResponse,
  freezeDeliveryRoute,
  hasPersistedCompletionTurn,
  hasPersistedDelivery,
  resolveCompletionRouting,
} from "../dist/completion-turn.js";

test("completion turn is exact-session and idempotent", () => {
  const turn = buildCompletionTurn({
    id: "job-12345678",
    state: "SUCCEEDED",
    directory: "/state/job-12345678",
    flowId: "flow-1",
    nextAction: "verify output",
    agentId: "scanner",
    sessionKey: "agent:scanner:acp:binding:slack:default:abc",
    notification: { idempotencyKey: "durable-job:job-12345678:terminal" },
  });

  assert.equal(turn.sessionKey, "agent:scanner:acp:binding:slack:default:abc");
  assert.equal(turn.agentId, "scanner");
  assert.equal(turn.idempotencyKey, "durable-job:job-12345678:terminal");
  assert.equal(turn.deliver, false);
  assert.equal("systemInputProvenance" in turn, false);
  assert.equal("suppressCommandInterpretation" in turn, false);
  assert.match(turn.message, /job_id=job-12345678/);
  assert.match(turn.message, /runtime-generated completion context/);
});

test("completion routing preserves the originating channel route", () => {
  const job = {
    id: "job-12345678",
    state: "SUCCEEDED",
    directory: "/state/job-12345678",
    agentId: "scanner",
    sessionKey: "agent:scanner:acp:binding:slack:default:abc",
    requesterOrigin: {
      channel: "slack",
      to: "channel:C123",
      accountId: "default",
      threadId: "1234.5678",
    },
    notification: { idempotencyKey: "durable-job:job-12345678:terminal" },
  };
  const route = resolveCompletionRouting(job);
  assert.equal(route.originatingChannel, "slack");
  assert.equal(route.originatingTo, "channel:C123");
  assert.equal(route.originatingAccountId, "default");
  assert.equal(route.originatingThreadId, "1234.5678");
});

test("completion routing falls back to persisted session origin", () => {
  const route = resolveCompletionRouting(
    { requesterOrigin: null },
    {
      sessionInfo: {
        origin: {
          provider: "slack",
          to: "channel:C0EXAMPLE002",
          accountId: "default",
        },
      },
    },
  );
  assert.deepEqual(route, {
    originatingChannel: "slack",
    originatingTo: "channel:C0EXAMPLE002",
    originatingAccountId: "default",
  });
});

test("freezeDeliveryRoute freezes a thread route from trusted metadata", () => {
  const resolved = {
    originatingChannel: "slack",
    originatingTo: "channel:C0EXAMPLE001",
    originatingAccountId: "default",
    originatingThreadId: "1785661934.163229",
  };
  const route = freezeDeliveryRoute(resolved, { agentId: "infra-scanner-openclaw" }, {});
  assert.equal(route.routeKind, "thread");
  assert.equal(route.channel, "slack");
  assert.equal(route.to, "channel:C0EXAMPLE001");
  assert.equal(route.threadId, "1785661934.163229");
  assert.equal(route.accountId, "default");
  assert.equal(route.agentId, "infra-scanner-openclaw");
  assert.equal(route.routeResolutionSource, "sessionInfo");
  assert.equal(typeof route.routeResolvedAt, "string");
});

test("channel_root requires positive chatType evidence, not mere threadId absence", () => {
  // Matches the observed lab contract: origin has chatType "channel", no thread.
  const rooted = {
    originatingChannel: "slack",
    originatingTo: "channel:C1",
    originatingAccountId: "default",
    originatingChatType: "channel",
  };
  assert.equal(classifyRouteKind(rooted), "channel_root");
  const route = freezeDeliveryRoute(rooted, { agentId: "a" }, {});
  assert.equal(route.routeKind, "channel_root");
  assert.equal(route.threadId, null);
  assert.equal(route.chatType, "channel");
  assert.equal(route.to, "channel:C1");

  // Channel/target present but NO chatType (root unprovable) -> unknown -> reject.
  const noChatType = { originatingChannel: "slack", originatingTo: "channel:C1" };
  assert.equal(classifyRouteKind(noChatType), "unknown");
  assert.throws(
    () => freezeDeliveryRoute(noChatType, { agentId: "a" }, {}),
    (error) => error.code === "DELIVERY_ROUTE_UNAVAILABLE",
  );
});

test("an explicit thread id classifies as thread even without chatType", () => {
  const thread = {
    originatingChannel: "slack",
    originatingTo: "channel:C1",
    originatingThreadId: "1785.99",
  };
  assert.equal(classifyRouteKind(thread), "thread");
});

test("channel_root is limited to the proven slack+channel shape", () => {
  const base = { originatingTo: "channel:C1" };
  // Proven shape.
  assert.equal(
    classifyRouteKind({ ...base, originatingChannel: "slack", originatingChatType: "channel" }),
    "channel_root",
  );
  // Unproven chatTypes stay fail-closed.
  assert.equal(
    classifyRouteKind({ ...base, originatingChannel: "slack", originatingChatType: "group" }),
    "unknown",
  );
  assert.equal(
    classifyRouteKind({ ...base, originatingChannel: "slack", originatingChatType: "space" }),
    "unknown",
  );
  // Non-slack providers stay fail-closed.
  assert.equal(
    classifyRouteKind({ ...base, originatingChannel: "discord", originatingChatType: "channel" }),
    "unknown",
  );
});

test("an explicit thread id wins over an unproven chatType", () => {
  assert.equal(
    classifyRouteKind({
      originatingChannel: "slack",
      originatingTo: "channel:C1",
      originatingChatType: "group",
      originatingThreadId: "1785.99",
    }),
    "thread",
  );
});

test("freezeDeliveryRoute rejects an unknown route (no channel/target)", () => {
  assert.equal(classifyRouteKind({ originatingThreadId: "1.2" }), "unknown");
  assert.throws(
    () => freezeDeliveryRoute({ originatingThreadId: "1.2" }, { agentId: "a" }, {}),
    (error) => error.code === "DELIVERY_ROUTE_UNAVAILABLE",
  );
});

test("resolveCompletionRouting surfaces chatType and broad thread-id spellings", () => {
  const rooted = resolveCompletionRouting(
    { requesterOrigin: null },
    { sessionInfo: { chatType: "channel", origin: { provider: "slack", to: "channel:C1", chatType: "channel" } } },
  );
  assert.equal(rooted.originatingChatType, "channel");
  assert.equal("originatingThreadId" in rooted, false);

  const threaded = resolveCompletionRouting(
    { requesterOrigin: null },
    { sessionInfo: { origin: { provider: "slack", to: "channel:C1", thread_ts: "1785.99" } } },
  );
  assert.equal(threaded.originatingThreadId, "1785.99");
});

test("extracts the completed ACP response from its Gateway mirror", () => {
  const idempotencyKey = "durable-job:job-12345678:terminal";
  const history = {
    messages: [
      { role: "assistant", content: [{ type: "text", text: "job started" }] },
      {
        role: "assistant",
        content: [{ type: "text", text: "SUCCEEDED" }],
        __openclaw: { idempotencyKey },
      },
    ],
  };
  assert.equal(extractCompletionResponse(history, idempotencyKey), "SUCCEEDED");
  assert.equal(extractCompletionResponse(history, "missing"), undefined);
});

test("extracts the original ACP response when Gateway mirror persistence is suppressed", () => {
  const idempotencyKey = "durable-job:job-12345678:terminal";
  const completionMessage = [
    "[DURABLE_JOB_COMPLETION v1]",
    "job_id=job-12345678",
    "state=SUCCEEDED",
  ].join("\n");
  const history = {
    messages: [
      { role: "user", content: completionMessage },
      {
        role: "assistant",
        content: [{ type: "text", text: "verified original ACP response" }],
        model: "acp-runtime",
      },
    ],
  };
  assert.equal(
    extractCompletionResponse(history, idempotencyKey, completionMessage),
    "verified original ACP response",
  );
  assert.equal(
    hasPersistedCompletionTurn(history, idempotencyKey, completionMessage),
    true,
  );
});

test("does not attribute a later unrelated ACP response to a completion turn", () => {
  const completionMessage = "[DURABLE_JOB_COMPLETION v1]\njob_id=job-12345678";
  const history = {
    messages: [
      { role: "user", content: completionMessage },
      { role: "user", content: "unrelated follow-up" },
      { role: "assistant", content: [{ type: "text", text: "unrelated answer" }] },
    ],
  };
  assert.equal(
    extractCompletionResponse(history, "durable-job:job-12345678:terminal", completionMessage),
    undefined,
  );
});

test("detects a persisted outbound delivery mirror", () => {
  const idempotencyKey = "durable-job:job-12345678:terminal:delivery";
  assert.equal(
    hasPersistedDelivery(
      { messages: [{ role: "assistant", idempotencyKey }] },
      idempotencyKey,
    ),
    true,
  );
  assert.equal(hasPersistedDelivery({ messages: [] }, idempotencyKey), false);
});

test("persisted user turn prevents a duplicate after Gateway restart", () => {
  const idempotencyKey = "durable-job:job-12345678:terminal";
  assert.equal(
    hasPersistedCompletionTurn(
      {
        messages: [
          {
            role: "user",
            idempotencyKey: `${idempotencyKey}:user`,
          },
        ],
      },
      idempotencyKey,
    ),
    true,
  );
  assert.equal(
    hasPersistedCompletionTurn({ messages: [{ role: "user", idempotencyKey: "other:user" }] }, idempotencyKey),
    false,
  );
});
