import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";

import { reconcileOnce, settleFlowWithApi, startJob } from "../dist/core.js";
import { classifyActivity } from "../dist/evaluator.js";
import { terminalCompletionMessage } from "../dist/completion-turn.js";
import { createJob, createJobId, readJob } from "../dist/job-store.js";

const workerPath = new URL("../dist/worker.js", import.meta.url).pathname;

async function withStore(run) {
  const store = await mkdtemp(path.join(os.tmpdir(), "durable-audit-"));
  try {
    await run(store);
  } finally {
    await rm(store, { recursive: true, force: true });
  }
}

function fakeFlowApi(status = "running") {
  const calls = { finish: [], fail: [] };
  const api = {
    runtime: {
      taskFlow: {
        bindSession: () => ({
          get: () => ({ flowId: "f1", revision: 1, status }),
          finish: (a) => calls.finish.push(a),
          fail: (a) => calls.fail.push(a),
        }),
      },
    },
  };
  return { api, calls };
}

const newFmtJob = (over = {}) => ({
  id: "job-a",
  flowId: "f1",
  sessionKey: "sk",
  state: "SUCCEEDED", // legacy alias for exit 0 — must NOT drive settlement
  jobOutcome: "FAILED_PROVIDER",
  processState: "COMPLETED",
  providerState: "BLOCKED_QUOTA",
  directory: "/d",
  ...over,
});

// ---- Audit 1: TaskFlow false-success ----

test("settleFlow: new-format FAILED_PROVIDER does NOT finish() the TaskFlow as success", async () => {
  const { api, calls } = fakeFlowApi();
  await settleFlowWithApi(api, newFmtJob());
  assert.equal(calls.finish.length, 0, "must never finish() a P0 job");
  assert.equal(calls.fail.length, 1);
  assert.match(calls.fail[0].blockedSummary, /FAILED_PROVIDER/);
});

test("settleFlow: new-format COMPLETED_UNVERIFIED is not settled as success either", async () => {
  const { api, calls } = fakeFlowApi();
  await settleFlowWithApi(api, newFmtJob({ jobOutcome: "COMPLETED_UNVERIFIED", providerState: "OK" }));
  assert.equal(calls.finish.length, 0);
  assert.equal(calls.fail.length, 1);
});

test("settleFlow: legacy job (no jobOutcome) keeps original finish-on-SUCCEEDED behaviour", async () => {
  const { api, calls } = fakeFlowApi();
  await settleFlowWithApi(api, { id: "j", flowId: "f1", sessionKey: "sk", state: "SUCCEEDED", directory: "/d" });
  assert.equal(calls.finish.length, 1);
  assert.equal(calls.fail.length, 0);
});

test("settleFlow: legacy FAILED job still fails", async () => {
  const { api, calls } = fakeFlowApi();
  await settleFlowWithApi(api, { id: "j", flowId: "f1", sessionKey: "sk", state: "FAILED", directory: "/d" });
  assert.equal(calls.finish.length, 0);
  assert.equal(calls.fail.length, 1);
});

// ---- Audit 1: completion-turn false-success ----

test("completion turn: new-format reports outcome, never a bare state=SUCCEEDED", () => {
  const msg = terminalCompletionMessage(newFmtJob());
  assert.match(msg, /outcome=FAILED_PROVIDER/);
  assert.match(msg, /provider_state=BLOCKED_QUOTA/);
  assert.doesNotMatch(msg, /state=SUCCEEDED/);
});

test("completion turn: legacy job keeps state= line", () => {
  const msg = terminalCompletionMessage({ id: "j", state: "SUCCEEDED", directory: "/d", flowId: null });
  assert.match(msg, /state=SUCCEEDED/);
  assert.doesNotMatch(msg, /outcome=/);
});

// ---- Audit 3: activity classification ----

test("classifyActivity: agy → model/agy-json; anything else → local/none", () => {
  assert.deepEqual(classifyActivity(["/opt/homebrew/bin/agy", "--print", "x"]), {
    activityType: "model",
    resultProtocol: "agy-json",
  });
  assert.deepEqual(classifyActivity(["agy", "--print"]), { activityType: "model", resultProtocol: "agy-json" });
  assert.deepEqual(classifyActivity(["node", "--test"]), { activityType: "local", resultProtocol: "none" });
  assert.deepEqual(classifyActivity(["/usr/bin/pytest"]), { activityType: "local", resultProtocol: "none" });
  assert.deepEqual(classifyActivity([]), { activityType: "local", resultProtocol: "none" });
});

// ---- Audit 2: processState lifecycle ----

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
const cfg = { queuedGraceMs: 30_000, maxConcurrent: 4, defaultTimeoutSeconds: 0, deliveryMaxAttempts: 8, sendLeaseMs: 30_000 };
const channelRootHistory = () => ({
  result: { sessionInfo: { chatType: "channel", origin: { provider: "slack", to: "channel:C1", chatType: "channel" } } },
});

test("processState: QUEUED at creation, and agy command → model/agy-json", async () => {
  await withStore(async (store) => {
    const work = await mkdtemp(path.join(os.tmpdir(), "durable-audit-work-"));
    const created = await startJob(
      {
        rootDir: store,
        config: cfg,
        gatewayCall: async () => channelRootHistory(),
        createFlow: () => ({ flowId: "f1" }),
        spawnWorker: () => 4242,
      },
      ownerCtx(work),
      { name: "j", command: ["/opt/homebrew/bin/agy", "--print", "x"], cwd: work },
    );
    const job = await readJob(store, created.id);
    assert.equal(job.processState, "QUEUED");
    assert.equal(job.activityType, "model");
    assert.equal(job.resultProtocol, "agy-json");
    await rm(work, { recursive: true, force: true });
  });
});

async function runWorker(store, id) {
  const child = spawn(process.execPath, [workerPath, store, id], { stdio: "ignore" });
  await new Promise((res) => child.once("exit", res));
}

test("processState: RUNNING is recorded mid-flight, then TIMED_OUT on timeout", async () => {
  await withStore(async (store) => {
    const id = createJobId();
    const now = new Date().toISOString();
    await createJob(store, {
      id,
      state: "QUEUED",
      processState: "QUEUED",
      resultProtocol: "none",
      command: [process.execPath, "-e", "setTimeout(()=>{}, 10000)"],
      cwd: store,
      timeoutSeconds: 1,
      createdAt: now,
      updatedAt: now,
      parent: { agentId: "a", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null },
      notification: { status: "pending" },
    });
    const child = spawn(process.execPath, [workerPath, store, id], { stdio: "ignore" });
    // poll for RUNNING
    let sawRunning = false;
    for (let i = 0; i < 50 && !sawRunning; i++) {
      const j = await readJob(store, id);
      if (j.processState === "RUNNING") sawRunning = true;
      else await new Promise((r) => setTimeout(r, 40));
    }
    assert.ok(sawRunning, "processState RUNNING must be observable mid-flight");
    await new Promise((res) => child.once("exit", res));
    const job = await readJob(store, id);
    assert.equal(job.processState, "TIMED_OUT");
    assert.equal(job.jobOutcome, "FAILED_COMMAND");
  });
});

test("processState: reconcile marks a dead-worker RUNNING job LOST (FAILED_COMMAND)", async () => {
  await withStore(async (store) => {
    const id = createJobId();
    const now = new Date().toISOString();
    await createJob(store, {
      id,
      state: "RUNNING",
      processState: "RUNNING",
      command: ["sleep", "9"],
      cwd: store,
      timeoutSeconds: 0,
      createdAt: now,
      updatedAt: now,
      workerPid: 2 ** 22, // not a live pid
      childPid: null,
      parent: { agentId: "a", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null },
      notification: { status: "pending" },
    });
    await reconcileOnce({ rootDir: store, config: cfg, gatewayCall: async () => ({ result: {} }), settleFlow: async () => {}, logger: null });
    const job = await readJob(store, id);
    assert.equal(job.state, "LOST");
    assert.equal(job.processState, "LOST");
    assert.equal(job.jobOutcome, "FAILED_COMMAND");
  });
});
