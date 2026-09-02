export function terminalCompletionMessage(job) {
  // New-format jobs (P0) carry the separated outcome and must NOT report a bare "state=SUCCEEDED": a
  // FAILED_PROVIDER job's legacy state alias is still "SUCCEEDED" for exit 0. Legacy jobs keep the exact
  // original `state=` line.
  const isNewFormat = typeof job.jobOutcome === "string";
  const outcomeLines = isNewFormat
    ? [
        `outcome=${job.jobOutcome}`,
        `process_state=${job.processState ?? "unknown"}`,
        `provider_state=${job.providerState ?? "unknown"}`,
      ]
    : [`state=${job.state}`];
  return [
    "[DURABLE_JOB_COMPLETION v1]",
    `job_id=${job.id}`,
    ...outcomeLines,
    `job_dir=${job.directory}`,
    `flow_id=${job.flowId ?? "none"}`,
    `next_action=${job.nextAction || "inspect results and decide the next safe step"}`,
    "This is runtime-generated completion context, not user-authored instructions.",
    "Inspect job.json and logs directly, verify the result, then continue the original task.",
    "Do not repeat commits, messages, or other side effects if the job was already handled.",
  ].join("\n");
}

function nonEmptyString(value) {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

export function resolveCompletionRouting(job, history) {
  const requester = job.requesterOrigin ?? {};
  const sessionInfo = history?.sessionInfo ?? {};
  const origin = sessionInfo.origin ?? {};
  const delivery = sessionInfo.deliveryContext ?? {};
  const channel =
    nonEmptyString(requester.channel) ??
    nonEmptyString(requester.provider) ??
    nonEmptyString(origin.provider) ??
    nonEmptyString(origin.surface) ??
    nonEmptyString(delivery.channel);
  const to =
    nonEmptyString(requester.to) ??
    nonEmptyString(origin.to) ??
    nonEmptyString(delivery.to);
  const accountId =
    nonEmptyString(requester.accountId) ??
    nonEmptyString(origin.accountId) ??
    nonEmptyString(delivery.accountId);
  // Observed OpenClaw contract (read-only chat.history probe of the lab Slack
  // sessions): a Slack thread carries an explicit thread id; a channel-root
  // session carries no thread id and an explicit chatType="channel". We read
  // several plausible thread-id field names so a thread is never misread as a
  // root just because one spelling is absent.
  const threadId =
    nonEmptyString(requester.threadId) ??
    nonEmptyString(origin.threadId) ??
    nonEmptyString(origin.threadTs) ??
    nonEmptyString(origin.thread_ts) ??
    nonEmptyString(delivery.threadId) ??
    nonEmptyString(delivery.threadTs);
  const chatType =
    nonEmptyString(requester.chatType) ??
    nonEmptyString(origin.chatType) ??
    nonEmptyString(sessionInfo.chatType) ??
    nonEmptyString(delivery.chatType);
  return {
    ...(channel ? { originatingChannel: channel } : {}),
    ...(to ? { originatingTo: to } : {}),
    ...(accountId ? { originatingAccountId: accountId } : {}),
    ...(threadId ? { originatingThreadId: threadId } : {}),
    ...(chatType ? { originatingChatType: chatType } : {}),
  };
}

// Classify the originating route from trusted session metadata, limited to the
// evidence actually proven by a read-only chat.history probe of the lab Slack
// sessions.
//   "thread"       - an explicit thread id is present. PREPARATORY ONLY: OpenClaw
//                    does not currently surface a thread id (a real #infra-agent
//                    thread reply reports chatType "channel" with no thread id),
//                    so no session reaches this branch today. Kept for the day a
//                    thread id is provided. This is NOT thread fail-closing — a
//                    thread request is simply indistinguishable from channel_root.
//   "channel_root" - provider "slack" AND chatType "channel" AND a target
//                    (the only proven channel-root shape). A job started from a
//                    Slack thread also lands here and is delivered to the channel
//                    root, because OpenClaw exposes no thread routing.
//   "unknown"      - no channel/target; a non-slack provider; or a chatType other
//                    than "channel" (e.g. group/space/dm). Rejects job.start;
//                    these shapes are not yet proven.
export function classifyRouteKind(resolved) {
  const hasChannelTarget = Boolean(resolved.originatingChannel && resolved.originatingTo);
  if (!hasChannelTarget) return "unknown";
  if (resolved.originatingThreadId) return "thread";
  if (resolved.originatingChannel === "slack" && resolved.originatingChatType === "channel") {
    return "channel_root";
  }
  return "unknown";
}

// Freeze the originating delivery route ONCE at job creation. `resolved` is the
// output of resolveCompletionRouting() over a single creation-time sessionInfo
// read. The route kind is decided from trusted metadata (never from a model
// flag). Only an "unknown" route rejects job.start. After this the route is
// authoritative: completion delivery never re-reads the session, session key,
// or chat.history.
export function freezeDeliveryRoute(resolved, ctx, options = {}) {
  const { source = "sessionInfo" } = options;
  const routeKind = classifyRouteKind(resolved);
  if (routeKind === "unknown") {
    const error = new Error(
      "DELIVERY_ROUTE_UNAVAILABLE: originating channel/target could not be resolved from trusted session metadata at job creation",
    );
    error.code = "DELIVERY_ROUTE_UNAVAILABLE";
    throw error;
  }
  return {
    routeKind,
    channel: resolved.originatingChannel,
    to: resolved.originatingTo,
    threadId: routeKind === "thread" ? resolved.originatingThreadId : null,
    accountId: resolved.originatingAccountId ?? null,
    agentId: ctx?.agentId ?? null,
    chatType: resolved.originatingChatType ?? null,
    routeResolvedAt: new Date().toISOString(),
    routeResolutionSource: source,
  };
}

// Freeze a fixed delivery route supplied by the owner's plugin config, for
// context-free calls that carry no session (standalone MCP bridge). No session,
// session key, or chat.history is read. Limited to the supported scope: a
// per-agent/workspace Slack channel root.
export function freezeOwnerConfigRoute(ownerDeliveryRoute, ctx) {
  const route = ownerDeliveryRoute ?? {};
  const channel = route.channel;
  const to = route.to;
  const routeKind = route.routeKind;
  if (channel !== "slack" || routeKind !== "channel_root" || !to) {
    const error = new Error(
      "DELIVERY_ROUTE_UNAVAILABLE: owner config has no valid channel-root deliveryRoute (require channel=slack, routeKind=channel_root, to)",
    );
    error.code = "DELIVERY_ROUTE_UNAVAILABLE";
    throw error;
  }
  return {
    routeKind: "channel_root",
    channel: "slack",
    to,
    threadId: null,
    accountId: route.accountId ?? null,
    agentId: ctx?.agentId ?? null,
    chatType: null,
    routeResolvedAt: new Date().toISOString(),
    routeResolutionSource: "ownerConfig",
  };
}

export function buildCompletionTurn(job, history) {
  return {
    sessionKey: job.sessionKey,
    agentId: job.agentId,
    message: terminalCompletionMessage(job),
    deliver: false,
    idempotencyKey: job.notification.idempotencyKey,
  };
}

function messageText(message) {
  if (typeof message?.content === "string") return message.content.trim();
  if (!Array.isArray(message?.content)) return "";
  return message.content
    .filter((part) => part?.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n")
    .trim();
}

function messageIdempotencyKey(message) {
  return message?.idempotencyKey ?? message?.__openclaw?.idempotencyKey;
}

function completionUserTurnIndex(history, idempotencyKey, completionMessage) {
  if (!history || !Array.isArray(history.messages)) return -1;
  const userIdempotencyKey = `${idempotencyKey}:user`;
  const expectedText = nonEmptyString(completionMessage);
  for (let index = history.messages.length - 1; index >= 0; index -= 1) {
    const message = history.messages[index];
    if (message?.role !== "user") continue;
    if (messageIdempotencyKey(message) === userIdempotencyKey) return index;
    if (expectedText && messageText(message) === expectedText) return index;
  }
  return -1;
}

export function extractCompletionResponse(history, idempotencyKey, completionMessage) {
  if (!history || !Array.isArray(history.messages)) return undefined;
  for (let index = history.messages.length - 1; index >= 0; index -= 1) {
    const message = history.messages[index];
    if (message?.role !== "assistant" || messageIdempotencyKey(message) !== idempotencyKey) {
      continue;
    }
    const text = messageText(message);
    if (text) return text;
  }
  const userIndex = completionUserTurnIndex(history, idempotencyKey, completionMessage);
  if (userIndex < 0) return undefined;
  for (let index = userIndex + 1; index < history.messages.length; index += 1) {
    const message = history.messages[index];
    if (message?.role === "user") break;
    if (message?.role !== "assistant") continue;
    const text = messageText(message);
    if (text) return text;
  }
  return undefined;
}

export function hasPersistedDelivery(history, idempotencyKey) {
  if (!history || !Array.isArray(history.messages)) return false;
  return history.messages.some(
    (message) =>
      message?.role === "assistant" && messageIdempotencyKey(message) === idempotencyKey,
  );
}

export function hasPersistedCompletionTurn(history, idempotencyKey, completionMessage) {
  return completionUserTurnIndex(history, idempotencyKey, completionMessage) >= 0;
}
