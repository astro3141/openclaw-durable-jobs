import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { processContinuation, readConfig, reconcileOnce } from "../dist/core.js";
import { createJob, createJobId, readJob } from "../dist/job-store.js";
import { buildContinuationTurn } from "../dist/continuation.js";

async function withStore(run) {
  const store = await mkdtemp(path.join(os.tmpdir(), "durable-cont-"));
  try {
    await run(store);
  } finally {
    await rm(store, { recursive: true, force: true });
  }
}

const cfg = {
  continuationEnabled: true,
  continuationTimeoutMs: 120_000,
  continuationDispatchLeaseMs: 60_000,
  continuationMaxAttempts: 1,
  completionAcpWakeup: false,
};

async function makeJob(store, over = {}) {
  const id = createJobId();
  const job = {
    version: 1,
    id,
    name: "delegate",
    state: "SUCCEEDED",
    processState: "COMPLETED",
    providerState: "OK",
    jobOutcome: "COMPLETED_UNVERIFIED",
    cwd: "/tmp",
    command: ["/opt/homebrew/bin/agy"],
    directory: path.join(store, id),
    agentId: "infra",
    nextAction: "review",
    parent: { agentId: "infra", sessionKey: "sk-1", sessionId: "s1", requesterOrigin: null, flowId: null },
    deliveryRoute: { routeKind: "channel_root", channel: "slack", to: "channel:C1", accountId: "default", agentId: "infra" },
    notification: { status: "pending", idempotencyKey: `durable-job:${id}:terminal` },
    ...over,
  };
  await createJob(store, job);
  return job;
}

// Mutable gateway stub: chat.history returns whatever `historyRef.value` currently holds.
function stubGateway(handlers = {}, historyRef = { value: { result: { messages: [] } } }) {
  const calls = [];
  const gc = async (method, params) => {
    calls.push({ method, params });
    if (method === "chat.history" && !handlers["chat.history"]) return historyRef.value;
    const h = handlers[method];
    const out = typeof h === "function" ? h(params, calls) : h;
    if (out instanceof Error) throw out;
    return out ?? { result: {} };
  };
  return { gc, calls, historyRef };
}

const userTurn = (marker, extra = "injected") => ({ role: "user", content: `${extra}\ncontinuation_marker=${marker}` });

// ---- Audit 1: default enablement ----

test("continuationEnabled defaults to FALSE (opt-in); explicit true honored", () => {
  assert.equal(readConfig({ pluginConfig: { stateSubdir: "x" } }).continuationEnabled, false);
  assert.equal(readConfig({ pluginConfig: { continuationEnabled: true } }).continuationEnabled, true);
  assert.equal(readConfig({ pluginConfig: { continuationEnabled: false } }).continuationEnabled, false);
});

test("with continuation disabled, reconcile does NOT create a continuation for a new-format job", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    const { gc } = stubGateway();
    await reconcileOnce({
      rootDir: store,
      config: { ...cfg, continuationEnabled: false, queuedGraceMs: 30_000, sendLeaseMs: 30_000, deliveryMaxAttempts: 8 },
      gatewayCall: gc,
      settleFlow: async () => {},
      logger: null,
    });
    assert.equal((await readJob(store, job.id)).continuation ?? null, null);
  });
});

// ---- Audit 2: completion correlation ----

test("live continuation: dispatch → DISPATCHED, marker-correlated reply → COMPLETED", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    const historyRef = { value: { result: { messages: [] } } };
    const { gc, calls } = stubGateway({ "chat.send": (p) => ({ result: { runId: p.idempotencyKey, status: "started" } }) }, historyRef);
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job);
    let j = await readJob(store, job.id);
    assert.equal(j.continuation.state, "DISPATCHED");
    assert.ok(j.continuation.marker, "unique marker minted");
    assert.equal(j.continuation.sessionKey, "sk-1");
    const send = calls.find((c) => c.method === "chat.send");
    assert.equal(send.params.deliver, false);
    assert.match(send.params.message, new RegExp(`continuation_marker=${j.continuation.marker}`));

    historyRef.value = { result: { messages: [userTurn(j.continuation.marker), { role: "assistant", content: "Reviewed." }] } };
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id));
    assert.equal((await readJob(store, job.id)).continuation.state, "COMPLETED");
  });
});

test("an unrelated assistant reply (to a later user turn) does NOT complete the continuation", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    const historyRef = { value: { result: { messages: [] } } };
    const { gc } = stubGateway({ "chat.send": (p) => ({ result: { runId: p.idempotencyKey, status: "started" } }) }, historyRef);
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job);
    const marker = (await readJob(store, job.id)).continuation.marker;
    // marked turn, then a DIFFERENT user turn, then a reply to that other turn
    historyRef.value = { result: { messages: [userTurn(marker), { role: "user", content: "something else" }, { role: "assistant", content: "reply to other" }] } };
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id));
    assert.equal((await readJob(store, job.id)).continuation.state, "DISPATCHED"); // not COMPLETED
  });
});

test("a PAST continuation marker's reply does not false-complete the current continuation", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    const historyRef = { value: { result: { messages: [] } } };
    const { gc } = stubGateway({ "chat.send": (p) => ({ result: { runId: p.idempotencyKey, status: "started" } }) }, historyRef);
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job);
    const marker = (await readJob(store, job.id)).continuation.marker;
    // an OLD marker with a reply, then the CURRENT marker with NO reply after it
    historyRef.value = { result: { messages: [userTurn("OLD-MARKER-1234", "old"), { role: "assistant", content: "old reply" }, userTurn(marker)] } };
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id));
    assert.equal((await readJob(store, job.id)).continuation.state, "DISPATCHED");
  });
});

test("marker pushed out of the history window keeps DISPATCHED (no false completion)", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    const historyRef = { value: { result: { messages: [] } } };
    const { gc } = stubGateway({ "chat.send": (p) => ({ result: { runId: p.idempotencyKey, status: "started" } }) }, historyRef);
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job);
    // history no longer contains our marker (trimmed), but has other traffic
    historyRef.value = { result: { messages: [{ role: "user", content: "later" }, { role: "assistant", content: "later reply" }] } };
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id));
    assert.equal((await readJob(store, job.id)).continuation.state, "DISPATCHED");
  });
});

test("duplicate ticks dispatch and mark exactly once", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    const { gc, calls } = stubGateway({ "chat.send": (p) => ({ result: { runId: p.idempotencyKey, status: "started" } }) });
    let firstMarker;
    for (let i = 0; i < 4; i++) {
      await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id));
      const m = (await readJob(store, job.id)).continuation.marker;
      firstMarker ??= m;
      assert.equal(m, firstMarker, "marker stable across ticks");
    }
    assert.equal(calls.filter((c) => c.method === "chat.send").length, 1);
  });
});

// ---- Audit 3: fallback crash safety ----

test("no sessionKey → MANUAL_FALLBACK + single fallback notice with deterministic key", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store, { parent: { agentId: "infra", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null } });
    const { gc, calls } = stubGateway({ send: { result: { messageId: "m" } } });
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job);
    const j = await readJob(store, job.id);
    assert.equal(j.continuation.state, "MANUAL_FALLBACK");
    assert.equal(j.continuation.fallbackState, "DELIVERED");
    const sends = calls.filter((c) => c.method === "send");
    assert.equal(sends.length, 1);
    assert.equal(sends[0].params.idempotencyKey, `durable-job:${job.id}:continuation-fallback`);
    assert.match(sends[0].params.message, /reason=NO_SESSION_KEY/);
  });
});

test("fallback notice sent exactly once across many ticks (DELIVERED claim blocks re-send)", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store, { parent: { agentId: "infra", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null } });
    const { gc, calls } = stubGateway({ send: { result: { messageId: "m" } } });
    for (let i = 0; i < 5; i++) await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id));
    assert.equal(calls.filter((c) => c.method === "send").length, 1);
  });
});

test("crash after send before DELIVERED: parked DELIVERY_UNKNOWN, NEVER blind-resent (no duplicate notice)", async () => {
  // The gateway `send` is not a proven exactly-once Slack dedup (README delivery-guarantee section), so an
  // ambiguous crash around the send must be parked, not re-sent — matching the delivery outbox.
  await withStore(async (store) => {
    const job = await makeJob(store, { parent: { agentId: "infra", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null } });
    const { gc, calls } = stubGateway({ send: { result: { messageId: "m" } } });
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job); // send #1 → DELIVERED
    // simulate a crash that lost the DELIVERED write: force SENDING with an EXPIRED lease
    const { updateJob } = await import("../dist/job-store.js");
    await updateJob(store, job.id, (j) => {
      j.continuation = { ...j.continuation, fallbackState: "SENDING", fallbackLeaseUntil: new Date(Date.now() - 1000).toISOString(), fallbackAt: null };
      return j;
    });
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id));
    assert.equal(calls.filter((c) => c.method === "send").length, 1, "no blind re-send after an ambiguous crash");
    assert.equal((await readJob(store, job.id)).continuation.fallbackState, "DELIVERY_UNKNOWN");
  });
});

// ---- Audit 2: fallback send error isolation ----

test("send timeout / ambiguous error → DELIVERY_UNKNOWN, and NO re-send on the next tick", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store, { parent: { agentId: "infra", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null } });
    const { gc, calls } = stubGateway({ send: () => new Error("send timeout") }); // ambiguous transport failure
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job);
    let j = await readJob(store, job.id);
    assert.equal(j.continuation.fallbackState, "DELIVERY_UNKNOWN", "ambiguous outcome is parked, not retried");
    assert.match(j.continuation.fallbackLastError, /send timeout/);
    assert.ok(j.continuation.fallbackLastAttemptAt);
    // next ticks must NOT resend
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id));
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id));
    assert.equal(calls.filter((c) => c.method === "send").length, 1, "no auto-resend after DELIVERY_UNKNOWN");
  });
});

test("connection-reset / unclassified exception is treated as ambiguous → DELIVERY_UNKNOWN (never guessed retryable)", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store, { parent: { agentId: "infra", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null } });
    const { gc, calls } = stubGateway({ send: () => Object.assign(new Error("ECONNRESET socket hang up"), { code: "ECONNRESET" }) });
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job);
    assert.equal((await readJob(store, job.id)).continuation.fallbackState, "DELIVERY_UNKNOWN");
    assert.equal(calls.filter((c) => c.method === "send").length, 1);
  });
});

test("only a STRUCTURALLY-PROVEN pre-send rejection is retried → PENDING → retry → DELIVERED", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store, { parent: { agentId: "infra", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null } });
    let failNext = true;
    const { gc, calls } = stubGateway({
      send: () => {
        if (failNext) { failNext = false; return Object.assign(new Error("rejected before dispatch"), { code: "PRE_SEND_REJECTED" }); }
        return { result: { messageId: "m" } };
      },
    });
    const cfgFast = { ...cfg, continuationDispatchLeaseMs: 1 };
    await processContinuation({ rootDir: store, config: cfgFast, gatewayCall: gc }, job); // proven pre-send rejection
    let j = await readJob(store, job.id);
    assert.equal(j.continuation.fallbackState, "PENDING", "proven pre-send rejection is retryable");
    const { updateJob } = await import("../dist/job-store.js");
    await updateJob(store, job.id, (x) => { x.continuation.fallbackNextAttemptAt = null; return x; });
    await processContinuation({ rootDir: store, config: cfgFast, gatewayCall: gc }, await readJob(store, job.id));
    j = await readJob(store, job.id);
    assert.equal(j.continuation.fallbackState, "DELIVERED");
    assert.equal(calls.filter((c) => c.method === "send").length, 2);
  });
});

test("a fallback send error does not abort reconcile over other jobs", async () => {
  await withStore(async (store) => {
    const a = await makeJob(store, { parent: { agentId: "infra", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null } });
    const b = await makeJob(store, { parent: { agentId: "infra", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null } });
    const gc = async (method) => {
      if (method === "send") throw new Error("send down");
      if (method === "chat.history") return { result: { messages: [] } };
      return { result: {} };
    };
    const deps = { rootDir: store, config: { ...cfg, queuedGraceMs: 30_000, sendLeaseMs: 30_000, deliveryMaxAttempts: 8 }, gatewayCall: gc, settleFlow: async () => {}, logger: null };
    await reconcileOnce(deps); // both go MANUAL_FALLBACK and both fallback sends throw — must not abort
    for (const j of [a, b]) {
      const row = await readJob(store, j.id);
      assert.equal(row.continuation.state, "MANUAL_FALLBACK");
      assert.match(row.continuation.fallbackLastError, /send down/);
    }
  });
});

test("consecutive PROVEN pre-send rejections are bounded → FAILED (no infinite retry)", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store, { parent: { agentId: "infra", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null } });
    const { gc } = stubGateway({ send: () => Object.assign(new Error("still refused"), { code: "PRE_SEND_REJECTED" }) });
    const cfgFast = { ...cfg, continuationDispatchLeaseMs: 1 };
    const { updateJob } = await import("../dist/job-store.js");
    for (let i = 0; i < 20; i++) {
      await processContinuation({ rootDir: store, config: cfgFast, gatewayCall: gc }, await readJob(store, job.id));
      await updateJob(store, job.id, (x) => { if (x.continuation.fallbackNextAttemptAt) x.continuation.fallbackNextAttemptAt = null; return x; });
    }
    assert.equal((await readJob(store, job.id)).continuation.fallbackState, "FAILED"); // bounded by fallbackMaxAttempts
  });
});

// ---- Audit 3: runtime disable with in-flight continuation ----

test("disabling continuation does not create a new continuation for a fresh terminal job", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    const { gc } = stubGateway();
    await processContinuation({ rootDir: store, config: { ...cfg, continuationEnabled: false }, gatewayCall: gc }, job);
    assert.equal((await readJob(store, job.id)).continuation ?? null, null);
  });
});

test("an in-flight DISPATCHED continuation still completes after the operator disables the feature", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    const historyRef = { value: { result: { messages: [] } } };
    const { gc } = stubGateway({ "chat.send": (p) => ({ result: { runId: p.idempotencyKey, status: "started" } }) }, historyRef);
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job); // enabled → DISPATCHED
    const marker = (await readJob(store, job.id)).continuation.marker;
    historyRef.value = { result: { messages: [userTurn(marker), { role: "assistant", content: "reviewed" }] } };
    // now DISABLED — the in-flight record must still be driven to COMPLETED
    await processContinuation({ rootDir: store, config: { ...cfg, continuationEnabled: false }, gatewayCall: gc }, await readJob(store, job.id));
    assert.equal((await readJob(store, job.id)).continuation.state, "COMPLETED");
  });
});

test("a PENDING continuation found while disabled → MANUAL_FALLBACK (not dispatched)", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    // create a PENDING record while enabled would dispatch immediately; instead seed one directly
    const { updateJob } = await import("../dist/job-store.js");
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: async () => ({ result: {} }), }, job); // enabled path
    // force it back to PENDING to represent "created, not yet dispatched"
    await updateJob(store, job.id, (j) => { j.continuation = { ...j.continuation, state: "PENDING", marker: null, dispatchedAt: null }; return j; });
    const { gc, calls } = stubGateway({ send: { result: { messageId: "m" } } });
    await processContinuation({ rootDir: store, config: { ...cfg, continuationEnabled: false }, gatewayCall: gc }, await readJob(store, job.id));
    const j = await readJob(store, job.id);
    assert.equal(j.continuation.state, "MANUAL_FALLBACK");
    assert.equal(j.continuation.reason, "CONTINUATION_DISABLED");
    assert.equal(calls.filter((c) => c.method === "chat.send").length, 0, "never dispatch while disabled");
    assert.match(calls.find((c) => c.method === "send").params.message, /reason=CONTINUATION_DISABLED/);
  });
});

// ---- Audit 4: config schema declares the continuation fields ----

test("plugin config schema declares the continuation.* fields (additionalProperties:false safe)", async () => {
  const { readFileSync } = await import("node:fs");
  const schema = JSON.parse(readFileSync(new URL("../openclaw.plugin.json", import.meta.url), "utf8")).configSchema;
  for (const key of ["continuationEnabled", "continuationTimeoutMs", "continuationDispatchLeaseMs", "continuationMaxAttempts"]) {
    assert.ok(schema.properties[key], `schema declares ${key}`);
  }
  assert.equal(schema.additionalProperties, false);
  assert.equal(schema.properties.continuationEnabled.default, false, "schema default is opt-in (false)");
});

// ---- Audit 4: history error isolation ----

test("first chat.history fails, second succeeds → COMPLETED (transient error is not DISPATCH_FAILED)", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    let failNext = false;
    const historyRef = { value: { result: { messages: [] } } };
    const { gc } = stubGateway({
      "chat.send": (p) => ({ result: { runId: p.idempotencyKey, status: "started" } }),
      "chat.history": () => {
        if (failNext) { failNext = false; throw new Error("history timeout"); }
        return historyRef.value;
      },
    });
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job); // dispatch
    const marker = (await readJob(store, job.id)).continuation.marker;
    failNext = true;
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id)); // history fails
    let j = await readJob(store, job.id);
    assert.equal(j.continuation.state, "DISPATCHED", "transient error keeps DISPATCHED");
    assert.match(j.continuation.lastCheckError, /history timeout/);
    historyRef.value = { result: { messages: [userTurn(marker), { role: "assistant", content: "ok" }] } };
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, await readJob(store, job.id)); // succeeds
    assert.equal((await readJob(store, job.id)).continuation.state, "COMPLETED");
  });
});

test("a chat.history error does not abort the reconcile pass over other jobs", async () => {
  await withStore(async (store) => {
    const a = await makeJob(store);
    const b = await makeJob(store);
    // pre-dispatch both so the next tick is a completion check
    const { gc } = stubGateway({ "chat.send": (p) => ({ result: { runId: p.idempotencyKey, status: "started" } }), "chat.history": () => { throw new Error("boom"); } });
    const deps = { rootDir: store, config: { ...cfg, queuedGraceMs: 30_000, sendLeaseMs: 30_000, deliveryMaxAttempts: 8 }, gatewayCall: gc, settleFlow: async () => {}, logger: null };
    await reconcileOnce(deps); // dispatch both
    await reconcileOnce(deps); // both hit history error — must not throw
    for (const j of [a, b]) {
      const row = await readJob(store, j.id);
      assert.equal(row.continuation.state, "DISPATCHED");
      assert.match(row.continuation.lastCheckError, /boom/);
    }
  });
});

test("consecutive history errors past the timeout → TIMED_OUT + one fallback", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    const zero = { ...cfg, continuationTimeoutMs: 0 };
    const { gc, calls } = stubGateway({ "chat.send": (p) => ({ result: { runId: p.idempotencyKey, status: "started" } }), "chat.history": () => { throw new Error("still down"); }, send: { result: { messageId: "m" } } });
    await processContinuation({ rootDir: store, config: zero, gatewayCall: gc }, job); // dispatch
    await processContinuation({ rootDir: store, config: zero, gatewayCall: gc }, await readJob(store, job.id)); // error + past timeout
    const j = await readJob(store, job.id);
    assert.equal(j.continuation.state, "TIMED_OUT");
    assert.equal(calls.filter((c) => c.method === "send").length, 1);
    assert.match(calls.find((c) => c.method === "send").params.message, /reason=TURN_TIMEOUT/);
  });
});

// ---- misc P1 semantics preserved ----

test("gateway dispatch error → FAILED → fallback (DISPATCH_FAILED)", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store);
    const { gc, calls } = stubGateway({ "chat.send": () => new Error("gateway down"), send: { result: { messageId: "m" } } });
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job);
    const j = await readJob(store, job.id);
    assert.equal(j.continuation.state, "FAILED");
    assert.equal(j.continuation.reason, "DISPATCH_FAILED");
    assert.match(calls.find((c) => c.method === "send").params.message, /reason=DISPATCH_FAILED/);
  });
});

test("FAILED_PROVIDER job still gets a continuation review dispatched", async () => {
  await withStore(async (store) => {
    const job = await makeJob(store, { jobOutcome: "FAILED_PROVIDER", providerState: "BLOCKED_QUOTA" });
    const { gc, calls } = stubGateway({ "chat.send": (p) => ({ result: { runId: p.idempotencyKey, status: "started" } }) });
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, job);
    assert.equal((await readJob(store, job.id)).continuation.state, "DISPATCHED");
    assert.equal(calls.filter((c) => c.method === "chat.send").length, 1);
  });
});

test("legacy job (no parent) is never given a continuation", async () => {
  await withStore(async (store) => {
    const legacy = await makeJob(store, { parent: undefined });
    delete legacy.parent;
    const { gc, calls } = stubGateway();
    await processContinuation({ rootDir: store, config: cfg, gatewayCall: gc }, legacy);
    assert.equal((await readJob(store, legacy.id)).continuation ?? null, null);
    assert.equal(calls.length, 0);
  });
});

test("buildContinuationTurn: deliver:false, deterministic key, sessionKey-targeted", () => {
  const job = { id: "job-z", agentId: "infra", parent: { sessionKey: "sk", agentId: "infra" }, jobOutcome: "COMPLETED_UNVERIFIED", directory: "/d", state: "SUCCEEDED" };
  const built = buildContinuationTurn(job);
  assert.equal(built.ok, true);
  assert.equal(built.payload.deliver, false);
  assert.equal(built.payload.sessionKey, "sk");
  assert.equal(built.payload.idempotencyKey, "durable-job:job-z:continuation");
  assert.equal(buildContinuationTurn({ id: "j", parent: { sessionKey: null } }).ok, false);
});
