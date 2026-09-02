import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  authorizeJobAccess,
  cancelJob,
  readConfig,
  reconcileOnce,
  resolveListFilter,
  selfLoadPluginConfig,
  settleFlowWithApi,
  startJob,
} from "../dist/core.js";
import { resolveOwnerContext } from "../dist/ownership.js";
import { freezeOwnerConfigRoute } from "../dist/completion-turn.js";
import { createJob, createJobId, listJobs, readJob } from "../dist/job-store.js";
import { initialOutbox } from "../dist/delivery-outbox.js";

const ROUTE = { routeKind: "channel_root", channel: "slack", to: "channel:C0EXAMPLE001", accountId: "default" };

function baseConfig(overrides = {}) {
  return {
    queuedGraceMs: 30_000,
    maxConcurrent: 4,
    defaultTimeoutSeconds: 0,
    deliveryMaxAttempts: 8,
    sendLeaseMs: 30_000,
    completionAcpWakeup: false,
    ...overrides,
  };
}

async function withDirs(run) {
  const store = await mkdtemp(path.join(os.tmpdir(), "durable-owner-store-"));
  const work = await mkdtemp(path.join(os.tmpdir(), "durable-owner-work-"));
  try {
    await run({ store, work });
  } finally {
    await Promise.all([rm(store, { recursive: true, force: true }), rm(work, { recursive: true, force: true })]);
  }
}

// ---- self-load (tests 1, 2, 12) ----

test("readConfig does NOT self-load when api.pluginConfig is present", () => {
  const cfg = readConfig(
    { pluginConfig: { stateSubdir: "given", owners: [{ agentId: "a", workspaceDir: "/w" }] } },
    { OPENCLAW_CONFIG_PATH: "/nonexistent/must-not-be-read.json" },
  );
  assert.equal(cfg.stateSubdir, "given");
  assert.equal(cfg.owners.length, 1);
});

test("readConfig self-loads only the durable-jobs section when api.pluginConfig is empty", async () => {
  await withDirs(async ({ store }) => {
    const file = path.join(store, "openclaw.json");
    await writeFile(
      file,
      JSON.stringify({
        secretSomewhereElse: "TOP-SECRET",
        plugins: { entries: { "durable-jobs": { config: { stateSubdir: "loaded", owners: [{ agentId: "infra", workspaceDir: "/repo", deliveryRoute: ROUTE }] } } } },
      }),
    );
    const cfg = readConfig({ pluginConfig: {} }, { OPENCLAW_CONFIG_PATH: file });
    assert.equal(cfg.stateSubdir, "loaded");
    assert.equal(cfg.owners[0].agentId, "infra");
    // Only the plugin section is returned; nothing else from the file.
    const section = selfLoadPluginConfig({ OPENCLAW_CONFIG_PATH: file });
    assert.equal("secretSomewhereElse" in section, false);
    assert.equal(section.stateSubdir, "loaded");
  });
});

test("self-load falls back to OPENCLAW_STATE_DIR/openclaw.json", async () => {
  await withDirs(async ({ store }) => {
    await writeFile(
      path.join(store, "openclaw.json"),
      JSON.stringify({ plugins: { entries: { "durable-jobs": { config: { stateSubdir: "viaStateDir" } } } } }),
    );
    const section = selfLoadPluginConfig({ OPENCLAW_STATE_DIR: store });
    assert.equal(section.stateSubdir, "viaStateDir");
  });
});

test("self-load is fail-closed on missing env, unreadable file, or missing section", async () => {
  await withDirs(async ({ store }) => {
    assert.throws(() => selfLoadPluginConfig({}), (e) => e.code === "PLUGIN_CONFIG_UNAVAILABLE");
    assert.throws(
      () => selfLoadPluginConfig({ OPENCLAW_CONFIG_PATH: path.join(store, "nope.json") }),
      (e) => e.code === "PLUGIN_CONFIG_UNAVAILABLE",
    );
    const noSection = path.join(store, "nosec.json");
    await writeFile(noSection, JSON.stringify({ plugins: { entries: {} } }));
    assert.throws(
      () => selfLoadPluginConfig({ OPENCLAW_CONFIG_PATH: noSection }),
      (e) => e.code === "PLUGIN_CONFIG_UNAVAILABLE",
    );
  });
});

// ---- owner resolution (tests 9, 11) ----

test("context-free call selects the single owner by cwd, sessionKey stays null", () => {
  const config = { owners: [{ agentId: "infra", workspaceDir: "/repo", allowedRoots: ["/repo"], deliveryRoute: ROUTE }] };
  const eff = resolveOwnerContext(config, {}, { cwd: "/repo" });
  assert.equal(eff.agentId, "infra");
  assert.equal(eff.sessionKey, null);
  assert.equal(eff.contextFree, true);
  assert.deepEqual(eff.ownerDeliveryRoute, ROUTE);
});

test("ambiguous owner match rejects with OWNER_AMBIGUOUS", () => {
  const config = {
    owners: [
      { agentId: "a", workspaceDir: "/repo", deliveryRoute: ROUTE },
      { agentId: "b", workspaceDir: "/repo", deliveryRoute: ROUTE },
    ],
  };
  assert.throws(() => resolveOwnerContext(config, {}, { cwd: "/repo" }), (e) => e.code === "OWNER_AMBIGUOUS");
});

test("cwd outside every owner workspace is rejected (OWNER_UNRESOLVED)", () => {
  const config = { owners: [{ agentId: "infra", workspaceDir: "/repo", deliveryRoute: ROUTE }] };
  assert.throws(() => resolveOwnerContext(config, {}, { cwd: "/somewhere/else" }), (e) => e.code === "OWNER_UNRESOLVED");
});

// ---- freezeOwnerConfigRoute (tests 4, 10) ----

test("freezeOwnerConfigRoute freezes a slack channel_root owner route", () => {
  const route = freezeOwnerConfigRoute(ROUTE, { agentId: "infra" });
  assert.equal(route.routeKind, "channel_root");
  assert.equal(route.channel, "slack");
  assert.equal(route.to, "channel:C0EXAMPLE001");
  assert.equal(route.threadId, null);
  assert.equal(route.accountId, "default");
  assert.equal(route.routeResolutionSource, "ownerConfig");
});

test("freezeOwnerConfigRoute rejects a missing/invalid owner route", () => {
  for (const bad of [null, {}, { routeKind: "thread", channel: "slack", to: "x" }, { routeKind: "channel_root", channel: "discord", to: "x" }, { routeKind: "channel_root", channel: "slack" }]) {
    assert.throws(() => freezeOwnerConfigRoute(bad, {}), (e) => e.code === "DELIVERY_ROUTE_UNAVAILABLE");
  }
});

// ---- startJob context-free (tests 3, 4, 5, 6, 7) ----

test("context-free startJob freezes ownerConfig route, no session, no chat.history, no flow", async () => {
  await withDirs(async ({ store, work }) => {
    const config = baseConfig();
    const ctx = resolveOwnerContext(
      { owners: [{ agentId: "infra", workspaceDir: work, allowedRoots: [work], deliveryRoute: ROUTE }] },
      {},
      { cwd: work },
    );
    let chatHistoryCalls = 0;
    let createFlowCalls = 0;
    const deps = {
      rootDir: store,
      config,
      gatewayCall: async (method) => {
        if (method === "chat.history") chatHistoryCalls += 1;
        throw new Error(`unexpected gateway method ${method}`);
      },
      createFlow: () => {
        createFlowCalls += 1;
        return { flowId: "should-not-happen" };
      },
      spawnWorker: () => 4242,
    };
    const created = await startJob(deps, ctx, { name: "cf-smoke", command: ["/bin/echo", "hi"], cwd: work });
    const job = await readJob(store, created.id);
    assert.equal(job.deliveryRoute.routeKind, "channel_root");
    assert.equal(job.deliveryRoute.to, "channel:C0EXAMPLE001");
    assert.equal(job.deliveryRoute.routeResolutionSource, "ownerConfig");
    assert.equal(job.sessionKey, null);
    assert.equal(job.flowId, null);
    assert.equal(createFlowCalls, 0);
    assert.equal(chatHistoryCalls, 0);
  });
});

test("context-free startJob without an owner route rejects before side effects", async () => {
  await withDirs(async ({ store, work }) => {
    const ctx = resolveOwnerContext(
      { owners: [{ agentId: "infra", workspaceDir: work, allowedRoots: [work] }] }, // no deliveryRoute
      {},
      { cwd: work },
    );
    let spawned = false;
    const deps = {
      rootDir: store,
      config: baseConfig(),
      gatewayCall: async () => {
        throw new Error("should not be called");
      },
      createFlow: () => ({ flowId: "x" }),
      spawnWorker: () => {
        spawned = true;
        return 1;
      },
    };
    await assert.rejects(
      startJob(deps, ctx, { name: "cf", command: ["/bin/echo", "hi"], cwd: work }),
      (e) => e.code === "DELIVERY_ROUTE_UNAVAILABLE",
    );
    assert.equal(spawned, false);
    assert.equal((await listJobs(store)).length, 0);
  });
});

test("settleFlowWithApi is a no-op when the job has no flow/session", async () => {
  let touched = false;
  const api = { runtime: { taskFlow: { bindSession: () => { touched = true; return {}; } } } };
  await settleFlowWithApi(api, { flowId: null, sessionKey: null, state: "SUCCEEDED" });
  assert.equal(touched, false);
});

// ---- context-free delivery (test 8) ----

test("a terminal context-free job is delivered once with no chat.history and no sessionKey in payload", async () => {
  await withDirs(async ({ store }) => {
    const id = createJobId();
    const now = new Date().toISOString();
    const job = {
      version: 1,
      id,
      name: "cf-terminal",
      state: "SUCCEEDED",
      cwd: "/tmp",
      command: ["/bin/echo", "hi"],
      exitCode: 0,
      endedAt: now,
      createdAt: now,
      updatedAt: now,
      directory: path.join(store, id),
      agentId: "infra",
      sessionKey: null,
      flowId: null,
      deliveryRoute: {
        routeKind: "channel_root",
        channel: "slack",
        to: "channel:C0EXAMPLE001",
        threadId: null,
        accountId: "default",
        agentId: "infra",
        routeResolutionSource: "ownerConfig",
      },
      notification: { status: "pending", idempotencyKey: `durable-job:${id}:terminal`, attempts: 0 },
    };
    job.delivery = initialOutbox(job, 8);
    await createJob(store, job);

    const calls = [];
    const gatewayCall = async (method, paramsArg) => {
      calls.push({ method, params: paramsArg });
      if (method === "send") return { result: { runId: "r1", messageId: "ts-cf" } };
      throw new Error(`unexpected ${method}`);
    };
    await reconcileOnce({ rootDir: store, config: baseConfig(), gatewayCall, settleFlow: async () => {}, logger: null });

    assert.deepEqual(calls.map((c) => c.method), ["send"]);
    assert.equal(calls[0].params.to, "channel:C0EXAMPLE001");
    assert.equal("sessionKey" in calls[0].params, false);
    const after = await readJob(store, id);
    assert.equal(after.delivery.state, "DELIVERED");
    assert.equal(after.delivery.messageId, "ts-cf");
  });
});

// ---- sessionless ACP block (Fix 1) ----

async function seedContextFreeTerminalJob(store) {
  const id = createJobId();
  const now = new Date().toISOString();
  const job = {
    version: 1,
    id,
    name: "cf",
    state: "SUCCEEDED",
    cwd: "/tmp",
    command: ["/bin/echo", "hi"],
    exitCode: 0,
    endedAt: now,
    createdAt: now,
    updatedAt: now,
    directory: path.join(store, id),
    agentId: "infra",
    sessionKey: null,
    flowId: null,
    deliveryRoute: {
      routeKind: "channel_root",
      channel: "slack",
      to: "channel:C0EXAMPLE001",
      threadId: null,
      accountId: "default",
      agentId: "infra",
      routeResolutionSource: "ownerConfig",
    },
    notification: { status: "pending", idempotencyKey: `durable-job:${id}:terminal`, attempts: 0 },
  };
  job.delivery = initialOutbox(job, 8);
  await createJob(store, job);
  return id;
}

test("completionAcpWakeup=true never touches a sessionKey=null job (no chat.history/chat.send)", async () => {
  await withDirs(async ({ store }) => {
    const id = await seedContextFreeTerminalJob(store);
    const calls = [];
    const gatewayCall = async (method) => {
      calls.push(method);
      if (method === "send") return { result: { runId: "r", messageId: "ts" } };
      throw new Error(`unexpected ${method}`);
    };
    await reconcileOnce({
      rootDir: store,
      config: baseConfig({ completionAcpWakeup: true }),
      gatewayCall,
      settleFlow: async () => {},
      logger: null,
    });
    // Only the deterministic outbox send; no legacy ACP RPCs.
    assert.deepEqual(calls, ["send"]);
    assert.equal(calls.includes("chat.history"), false);
    assert.equal(calls.includes("chat.send"), false);
    const after = await readJob(store, id);
    assert.equal(after.delivery.state, "DELIVERED");
    // Legacy notification state is left untouched (pending, never processed).
    assert.equal(after.notification.status, "pending");
  });
});

// ---- context-free list/status/cancel ownership (Fix 2) ----

const OWNERS = {
  owners: [
    { agentId: "infra", workspaceDir: "/repo/infra", allowedRoots: ["/repo/infra"], deliveryRoute: ROUTE },
    { agentId: "other", workspaceDir: "/repo/other", allowedRoots: ["/repo/other"], deliveryRoute: ROUTE },
  ],
};

test("resolveListFilter: trusted by session, context-free requires a cwd selector", () => {
  assert.deepEqual(resolveListFilter(OWNERS, { agentId: "a", sessionKey: "s" }, {}), {
    agentId: "a",
    sessionKey: "s",
  });
  assert.throws(() => resolveListFilter(OWNERS, {}, {}), (e) => e.code === "LIST_SELECTOR_REQUIRED");
  assert.deepEqual(resolveListFilter(OWNERS, {}, { cwd: "/repo/infra/x" }), {
    agentId: "infra",
    sessionKey: null,
  });
});

test("authorizeJobAccess (trusted) keeps the session+agent check", () => {
  const ctx = { agentId: "infra", sessionKey: "sess" };
  authorizeJobAccess(OWNERS, ctx, { agentId: "infra", sessionKey: "sess", cwd: "/repo/infra" });
  assert.throws(
    () => authorizeJobAccess(OWNERS, ctx, { agentId: "infra", sessionKey: "other", cwd: "/repo/infra" }),
    /owned by a different session/,
  );
});

test("authorizeJobAccess (context-free) authorizes from the job's own cwd, not caller cwd", () => {
  // Job legitimately owned by infra: its stored cwd resolves to the infra owner.
  authorizeJobAccess(OWNERS, {}, { agentId: "infra", sessionKey: null, cwd: "/repo/infra/sub" });
  // A job whose agentId disagrees with its workspace owner is rejected.
  assert.throws(
    () => authorizeJobAccess(OWNERS, {}, { agentId: "infra", sessionKey: null, cwd: "/repo/other/sub" }),
    /not owned by the agent configured for its workspace/,
  );
  // A job whose cwd is outside every owner is rejected fail-closed.
  assert.throws(
    () => authorizeJobAccess(OWNERS, {}, { agentId: "infra", sessionKey: null, cwd: "/tmp/elsewhere" }),
    (e) => e.code === "OWNER_UNRESOLVED",
  );
});

test("cancelJob cancels an active job without an ownership argument", async () => {
  await withDirs(async ({ store }) => {
    const id = createJobId();
    const now = new Date().toISOString();
    await createJob(store, {
      version: 1,
      id,
      name: "run",
      state: "RUNNING",
      cwd: "/repo/infra",
      command: ["/bin/sleep", "1"],
      createdAt: now,
      updatedAt: now,
      directory: path.join(store, id),
      agentId: "infra",
      sessionKey: null,
      workerPid: null,
      childPid: null,
      notification: { status: "pending" },
    });
    const res = await cancelJob(store, id);
    assert.equal(res.state, "CANCELLED");
  });
});
