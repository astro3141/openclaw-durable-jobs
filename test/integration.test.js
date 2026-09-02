import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import test from "node:test";

import {
  LEGACY_BLOCK_CODE,
  detectLegacyActiveJobs,
  makeLegacyBlockError,
  reconcileOnce,
  startJob,
} from "../dist/core.js";
import { createJob, createJobId, listJobs, readJob } from "../dist/job-store.js";
import { initialOutbox } from "../dist/delivery-outbox.js";

const execFileAsync = promisify(execFile);
const preflightPath = fileURLToPath(new URL("../dist/preflight.js", import.meta.url));

function testConfig(overrides = {}) {
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

function threadHistory() {
  return {
    result: {
      sessionInfo: {
        origin: { provider: "slack", to: "channel:C0EXAMPLE001", accountId: "default", threadId: "1785.99" },
      },
    },
  };
}

// Mirrors the observed lab contract: explicit chatType "channel", no thread id.
function channelRootHistory() {
  return {
    result: {
      sessionInfo: {
        chatType: "channel",
        origin: { provider: "slack", to: "channel:C0EXAMPLE001", accountId: "default", chatType: "channel" },
        deliveryContext: { channel: "slack" },
      },
    },
  };
}

async function withDirs(run) {
  const store = await mkdtemp(path.join(os.tmpdir(), "durable-int-store-"));
  const work = await mkdtemp(path.join(os.tmpdir(), "durable-int-work-"));
  try {
    await run({ store, work });
  } finally {
    await Promise.all([rm(store, { recursive: true, force: true }), rm(work, { recursive: true, force: true })]);
  }
}

function ownerCtx(work) {
  return {
    sessionKey: "agent:infra:acp:binding:slack:default:x",
    agentId: "infra",
    sessionId: "s1",
    workspaceDir: work,
    durableAllowedRoots: [work],
  };
}

async function seedTerminalJob(store, { routeKind = "thread", threadId = "1785.99", delivery } = {}) {
  const id = createJobId();
  const now = new Date().toISOString();
  const job = {
    version: 1,
    id,
    name: "seed",
    state: "SUCCEEDED",
    cwd: "/tmp",
    command: ["echo", "hi"],
    timeoutSeconds: 0,
    nextAction: "inspect",
    createdAt: now,
    updatedAt: now,
    startedAt: now,
    endedAt: now,
    workerPid: null,
    childPid: null,
    exitCode: 0,
    exitSignal: null,
    error: null,
    directory: path.join(store, id),
    agentId: "infra",
    sessionKey: "agent:infra:acp:binding:slack:default:x",
    sessionId: "s1",
    requesterOrigin: null,
    flowId: null,
    deliveryRoute: {
      routeKind,
      channel: "slack",
      to: "channel:C1",
      threadId: routeKind === "thread" ? threadId : null,
      accountId: "default",
      agentId: "infra",
    },
    notification: { status: "pending", idempotencyKey: `durable-job:${id}:terminal`, attempts: 0 },
  };
  job.delivery = delivery ?? initialOutbox(job, 8);
  await createJob(store, job);
  return job;
}

const noSettle = async () => {};

test("startJob resolves the route once via chat.history and freezes it before spawning", async () => {
  await withDirs(async ({ store, work }) => {
    const calls = [];
    const gatewayCall = async (method, params) => {
      calls.push({ method, params });
      if (method === "chat.history") return threadHistory();
      throw new Error(`unexpected gateway method ${method}`);
    };
    let spawnedId = null;
    const deps = {
      rootDir: store,
      config: testConfig(),
      gatewayCall,
      createFlow: () => ({ flowId: "flow-test" }),
      spawnWorker: (_root, id) => {
        spawnedId = id;
        return 4242;
      },
    };
    const created = await startJob(deps, ownerCtx(work), { name: "j", command: ["echo", "hi"], cwd: work });

    assert.equal(calls.filter((c) => c.method === "chat.history").length, 1);
    const job = await readJob(store, created.id);
    assert.equal(job.deliveryRoute.routeKind, "thread");
    assert.equal(job.deliveryRoute.threadId, "1785.99");
    assert.equal(job.deliveryRoute.to, "channel:C0EXAMPLE001");
    assert.equal(spawnedId, created.id);
    assert.equal(job.workerPid, 4242);
    assert.equal(job.delivery.state, "PENDING");
  });
});

test("an unknown route rejects job.start before any worker is spawned", async () => {
  await withDirs(async ({ store, work }) => {
    let spawnCalled = false;
    const deps = {
      rootDir: store,
      config: testConfig(),
      gatewayCall: async (method) => {
        if (method === "chat.history") return { result: { sessionInfo: { origin: {} } } };
        throw new Error(`unexpected ${method}`);
      },
      createFlow: () => ({ flowId: "flow-test" }),
      spawnWorker: () => {
        spawnCalled = true;
        return 1;
      },
    };
    await assert.rejects(
      startJob(deps, ownerCtx(work), { name: "j", command: ["echo", "hi"], cwd: work }),
      (error) => error.code === "DELIVERY_ROUTE_UNAVAILABLE",
    );
    assert.equal(spawnCalled, false);
    assert.equal((await listJobs(store)).length, 0);
  });
});

test("completion delivery uses only `send` with the frozen route — no chat.history/chat.send", async () => {
  await withDirs(async ({ store }) => {
    const job = await seedTerminalJob(store, { routeKind: "thread", threadId: "1785.99" });
    const calls = [];
    const gatewayCall = async (method, params) => {
      calls.push({ method, params });
      if (method === "send") return { result: { runId: "r1", messageId: "ts-1" } };
      throw new Error(`unexpected gateway method ${method}`);
    };
    await reconcileOnce({ rootDir: store, config: testConfig(), gatewayCall, settleFlow: noSettle, logger: null });

    assert.deepEqual(calls.map((c) => c.method), ["send"]);
    assert.equal(calls[0].params.channel, "slack");
    assert.equal(calls[0].params.to, "channel:C1");
    assert.equal(calls[0].params.threadId, "1785.99");
    // No session/sessionKey-based RPC and no sessionKey in the send payload.
    assert.equal("sessionKey" in calls[0].params, false);
    const after = await readJob(store, job.id);
    assert.equal(after.delivery.state, "DELIVERED");
    assert.equal(after.delivery.messageId, "ts-1");
  });
});

test("startJob freezes a channel_root route from chatType metadata", async () => {
  await withDirs(async ({ store, work }) => {
    const deps = {
      rootDir: store,
      config: testConfig(),
      gatewayCall: async (method) => {
        if (method === "chat.history") return channelRootHistory();
        throw new Error(`unexpected ${method}`);
      },
      createFlow: () => ({ flowId: "flow-test" }),
      spawnWorker: () => 4242,
    };
    const created = await startJob(deps, ownerCtx(work), { name: "j", command: ["echo", "hi"], cwd: work });
    const job = await readJob(store, created.id);
    assert.equal(job.deliveryRoute.routeKind, "channel_root");
    assert.equal(job.deliveryRoute.threadId, null);
    assert.equal(job.deliveryRoute.chatType, "channel");
  });
});

test("a channel/target with no chatType (root unprovable) rejects job.start before spawn", async () => {
  await withDirs(async ({ store, work }) => {
    let spawnCalled = false;
    const deps = {
      rootDir: store,
      config: testConfig(),
      gatewayCall: async (method) => {
        if (method === "chat.history") {
          return { result: { sessionInfo: { origin: { provider: "slack", to: "channel:C1" } } } };
        }
        throw new Error(`unexpected ${method}`);
      },
      createFlow: () => ({ flowId: "flow-test" }),
      spawnWorker: () => {
        spawnCalled = true;
        return 1;
      },
    };
    await assert.rejects(
      startJob(deps, ownerCtx(work), { name: "j", command: ["echo", "hi"], cwd: work }),
      (error) => error.code === "DELIVERY_ROUTE_UNAVAILABLE",
    );
    assert.equal(spawnCalled, false);
    assert.equal((await listJobs(store)).length, 0);
  });
});

test("makeLegacyBlockError carries the blocked job ids/state/createdAt", () => {
  const err = makeLegacyBlockError([
    { id: "job-aaaaaaaa", state: "RUNNING", createdAt: "2026-08-03T00:00:00.000Z" },
  ]);
  assert.equal(err.code, LEGACY_BLOCK_CODE);
  assert.equal(err.jobs.length, 1);
  assert.equal(err.jobs[0].id, "job-aaaaaaaa");
  assert.equal(err.jobs[0].state, "RUNNING");
  assert.match(err.message, /job-aaaaaaaa/);
  assert.match(err.message, /createdAt=2026-08-03/);
});

test("terminal legacy jobs are NOT blocked; only active ones are", async () => {
  await withDirs(async ({ store }) => {
    const now = new Date().toISOString();
    // Terminal legacy job (no route/outbox) — must not block.
    await createJob(store, {
      version: 1,
      id: createJobId(),
      name: "old-done",
      state: "SUCCEEDED",
      createdAt: now,
      updatedAt: now,
      directory: path.join(store, "x"),
      notification: { status: "pending" },
    });
    assert.equal((await detectLegacyActiveJobs(store)).length, 0);
  });
});

test("preflight.js exits 0 with no active legacy jobs and 1 when present", async () => {
  await withDirs(async ({ store }) => {
    // Clean store -> exit 0.
    const ok = await execFileAsync(process.execPath, [preflightPath, "--root-dir", store]);
    assert.match(ok.stdout, /preflight OK/);

    // Active legacy job -> exit 1.
    const legacyId = createJobId();
    const now = new Date().toISOString();
    await createJob(store, {
      version: 1,
      id: legacyId,
      name: "legacy",
      state: "RUNNING",
      createdAt: now,
      updatedAt: now,
      directory: path.join(store, legacyId),
      notification: { status: "pending" },
    });
    await assert.rejects(
      execFileAsync(process.execPath, [preflightPath, "--root-dir", store]),
      (error) => error.code === 1 && /preflight BLOCKED/.test(String(error.stderr)),
    );
  });
});

test("a channel_root route is delivered without a threadId", async () => {
  await withDirs(async ({ store }) => {
    const job = await seedTerminalJob(store, { routeKind: "channel_root" });
    const calls = [];
    const gatewayCall = async (method, params) => {
      calls.push({ method, params });
      if (method === "send") return { result: { runId: "r1", messageId: "ts-2" } };
      throw new Error(`unexpected ${method}`);
    };
    await reconcileOnce({ rootDir: store, config: testConfig(), gatewayCall, settleFlow: noSettle, logger: null });

    assert.equal("threadId" in calls[0].params, false);
    assert.equal((await readJob(store, job.id)).delivery.state, "DELIVERED");
  });
});

test("a runId without a provider messageId parks in GATEWAY_ACCEPTED_UNCONFIRMED", async () => {
  await withDirs(async ({ store }) => {
    const job = await seedTerminalJob(store);
    const gatewayCall = async (method) => {
      if (method === "send") return { result: { runId: "r1" } };
      throw new Error(`unexpected ${method}`);
    };
    await reconcileOnce({ rootDir: store, config: testConfig(), gatewayCall, settleFlow: noSettle, logger: null });
    assert.equal((await readJob(store, job.id)).delivery.state, "GATEWAY_ACCEPTED_UNCONFIRMED");
  });
});

test("overlapping reconciles run one send; the second neither resends nor marks unknown", async () => {
  await withDirs(async ({ store }) => {
    const job = await seedTerminalJob(store);
    let sendCount = 0;
    let release;
    const gate = new Promise((resolve) => {
      release = resolve;
    });
    const gatewayCall = async (method) => {
      if (method === "send") {
        sendCount += 1;
        await gate;
        return { result: { runId: "r1", messageId: "ts-9" } };
      }
      throw new Error(`unexpected ${method}`);
    };
    const deps = { rootDir: store, config: testConfig(), gatewayCall, settleFlow: noSettle, logger: null };

    const first = reconcileOnce(deps);
    await new Promise((r) => setTimeout(r, 50)); // let the first tick claim SENDING and enter send
    await reconcileOnce(deps); // second tick: sees a fresh-lease SENDING and skips

    const mid = await readJob(store, job.id);
    assert.equal(mid.delivery.state, "SENDING");
    assert.equal(sendCount, 1);

    release();
    await first;
    const after = await readJob(store, job.id);
    assert.equal(sendCount, 1);
    assert.equal(after.delivery.state, "DELIVERED");
  });
});

test("SENDING becomes DELIVERY_UNKNOWN only after its lease expires", async () => {
  await withDirs(async ({ store }) => {
    let sendCount = 0;
    const gatewayCall = async (method) => {
      if (method === "send") sendCount += 1;
      return { result: { runId: "r", messageId: "ts" } };
    };
    const deps = { rootDir: store, config: testConfig(), gatewayCall, settleFlow: noSettle, logger: null };

    // Fresh lease -> untouched.
    const fresh = await seedTerminalJob(store, {
      delivery: {
        state: "SENDING",
        attempts: 1,
        maxAttempts: 8,
        sendingLeaseUntil: new Date(Date.now() + 20_000).toISOString(),
      },
    });
    await reconcileOnce(deps);
    assert.equal((await readJob(store, fresh.id)).delivery.state, "SENDING");

    // Expired lease -> DELIVERY_UNKNOWN (no auto-resend).
    const stale = await seedTerminalJob(store, {
      delivery: {
        state: "SENDING",
        attempts: 1,
        maxAttempts: 8,
        sendingLeaseUntil: new Date(Date.now() - 1_000).toISOString(),
      },
    });
    await reconcileOnce(deps);
    assert.equal((await readJob(store, stale.id)).delivery.state, "DELIVERY_UNKNOWN");
    assert.equal(sendCount, 0);
  });
});

test("DELIVERY_UNKNOWN is never auto-resent on a later reconcile", async () => {
  await withDirs(async ({ store }) => {
    let sendCount = 0;
    const gatewayCall = async (method) => {
      if (method === "send") sendCount += 1;
      return { result: { runId: "r", messageId: "ts" } };
    };
    const deps = { rootDir: store, config: testConfig(), gatewayCall, settleFlow: noSettle, logger: null };
    const job = await seedTerminalJob(store, {
      delivery: { state: "DELIVERY_UNKNOWN", attempts: 1, maxAttempts: 8, lastError: "ambiguous" },
    });
    await reconcileOnce(deps);
    await reconcileOnce(deps);
    assert.equal(sendCount, 0);
    assert.equal((await readJob(store, job.id)).delivery.state, "DELIVERY_UNKNOWN");
  });
});

test("legacy active jobs (no route/outbox) are detected by preflight", async () => {
  await withDirs(async ({ store }) => {
    const legacyId = createJobId();
    const now = new Date().toISOString();
    await createJob(store, {
      version: 1,
      id: legacyId,
      name: "legacy",
      state: "RUNNING",
      createdAt: now,
      updatedAt: now,
      directory: path.join(store, legacyId),
      notification: { status: "pending" },
    });
    await seedTerminalJob(store); // modern terminal job — not flagged
    const modernActive = await seedTerminalJob(store);
    await createJob(store, {
      ...modernActive,
      id: createJobId(),
      state: "RUNNING", // modern active job WITH route/outbox — not flagged
    });

    const legacy = await detectLegacyActiveJobs(store);
    assert.equal(legacy.length, 1);
    assert.equal(legacy[0].id, legacyId);
  });
});
