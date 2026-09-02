import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { cancelJob, startJob } from "../dist/core.js";
import { createJob, createJobId, readJob } from "../dist/job-store.js";
import { buildTerminalDeliveryMessage, initialOutbox } from "../dist/delivery-outbox.js";

async function withDirs(run) {
  const store = await mkdtemp(path.join(os.tmpdir(), "durable-p0-store-"));
  const work = await mkdtemp(path.join(os.tmpdir(), "durable-p0-work-"));
  try {
    await run({ store, work });
  } finally {
    await Promise.all([
      rm(store, { recursive: true, force: true }),
      rm(work, { recursive: true, force: true }),
    ]);
  }
}

function ownerCtx(work) {
  return {
    sessionKey: "agent:infra:acp:binding:slack:default:x",
    agentId: "infra",
    sessionId: "s1",
    deliveryContext: { channel: "slack", to: "channel:C1", chatType: "channel" },
    workspaceDir: work,
    durableAllowedRoots: [work],
  };
}

function channelRootHistory() {
  return {
    result: {
      sessionInfo: {
        chatType: "channel",
        origin: { provider: "slack", to: "channel:C1", accountId: "default", chatType: "channel" },
      },
    },
  };
}

const testConfig = {
  queuedGraceMs: 30_000,
  maxConcurrent: 4,
  defaultTimeoutSeconds: 0,
  deliveryMaxAttempts: 8,
  sendLeaseMs: 30_000,
};

test("startJob normalizes an additive parent block and keeps the legacy fields", async () => {
  await withDirs(async ({ store, work }) => {
    const deps = {
      rootDir: store,
      config: testConfig,
      gatewayCall: async () => channelRootHistory(),
      createFlow: () => ({ flowId: "flow-1" }),
      spawnWorker: () => 4242,
    };
    const created = await startJob(deps, ownerCtx(work), { name: "j", command: ["echo", "hi"], cwd: work });
    const job = await readJob(store, created.id);
    // additive parent block
    assert.deepEqual(job.parent, {
      agentId: "infra",
      sessionKey: "agent:infra:acp:binding:slack:default:x",
      sessionId: "s1",
      requesterOrigin: { channel: "slack", to: "channel:C1", chatType: "channel" },
      flowId: "flow-1",
    });
    // legacy top-level fields still present (not removed)
    assert.equal(job.agentId, "infra");
    assert.equal(job.sessionKey, "agent:infra:acp:binding:slack:default:x");
    assert.equal(job.flowId, "flow-1");
  });
});

test("cancelJob sets CANCELLED process/outcome on a new-format job", async () => {
  await withDirs(async ({ store }) => {
    const id = createJobId();
    const now = new Date().toISOString();
    await createJob(store, {
      version: 1,
      id,
      name: "c",
      state: "RUNNING",
      cwd: "/tmp",
      command: ["sleep", "1"],
      timeoutSeconds: 0,
      createdAt: now,
      updatedAt: now,
      workerPid: null,
      childPid: null,
      parent: { agentId: "infra", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null },
      notification: { status: "pending", idempotencyKey: `durable-job:${id}:terminal` },
    });
    await cancelJob(store, id);
    const job = await readJob(store, id);
    assert.equal(job.state, "CANCELLED");
    assert.equal(job.processState, "CANCELLED");
    assert.equal(job.jobOutcome, "CANCELLED");
  });
});

test("terminal message: new-format reports job_outcome + separated states, never SUCCEEDED", () => {
  const job = {
    id: "job-x",
    name: "analyze",
    state: "SUCCEEDED", // legacy alias present, but must NOT drive the wording
    jobOutcome: "COMPLETED_UNVERIFIED",
    processState: "COMPLETED",
    providerState: "OK",
    exitCode: 0,
    endedAt: "2026-08-04T00:00:00Z",
    directory: "/d",
    notification: { idempotencyKey: "durable-job:job-x:terminal" },
  };
  const msg = buildTerminalDeliveryMessage(job);
  assert.match(msg, /Durable job COMPLETED_UNVERIFIED: analyze/);
  assert.match(msg, /process_state=COMPLETED/);
  assert.match(msg, /provider_state=OK/);
  assert.doesNotMatch(msg, /Durable job SUCCEEDED/);
});

test("terminal message: FAILED_PROVIDER new-format wording", () => {
  const job = {
    id: "job-y",
    name: "delegate",
    state: "SUCCEEDED",
    jobOutcome: "FAILED_PROVIDER",
    processState: "COMPLETED",
    providerState: "BLOCKED_QUOTA",
    exitCode: 0,
    endedAt: "t",
    directory: "/d",
    notification: { idempotencyKey: "durable-job:job-y:terminal" },
  };
  const msg = buildTerminalDeliveryMessage(job);
  assert.match(msg, /Durable job FAILED_PROVIDER: delegate/);
  assert.match(msg, /provider_state=BLOCKED_QUOTA/);
});

test("terminal message: legacy job (no jobOutcome) keeps the exact old state wording (regression)", () => {
  const legacy = {
    id: "job-legacy",
    name: "old",
    state: "SUCCEEDED",
    exitCode: 0,
    endedAt: "t",
    directory: "/d",
    notification: { idempotencyKey: "durable-job:job-legacy:terminal" },
  };
  const msg = buildTerminalDeliveryMessage(legacy);
  assert.match(msg, /Durable job SUCCEEDED: old/);
  assert.doesNotMatch(msg, /process_state=/);
  assert.doesNotMatch(msg, /provider_state=/);
});

test("legacy terminal states all render with the old wording (no new fields leak in)", () => {
  for (const state of ["SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED", "LOST"]) {
    const msg = buildTerminalDeliveryMessage({
      id: "j",
      name: "n",
      state,
      endedAt: "t",
      directory: "/d",
      notification: { idempotencyKey: "durable-job:j:terminal" },
    });
    assert.match(msg, new RegExp(`Durable job ${state}: n`));
    assert.doesNotMatch(msg, /process_state=/);
  }
});
