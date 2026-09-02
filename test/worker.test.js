import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";
import { createJob, createJobId, readJob } from "../dist/job-store.js";

const workerPath = new URL("../dist/worker.js", import.meta.url).pathname;

async function waitForTerminal(root, id, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = await readJob(root, id);
    if (["SUCCEEDED", "FAILED", "TIMED_OUT"].includes(job.state)) return job;
    await new Promise((resolve) => setTimeout(resolve, 30));
  }
  throw new Error(`worker did not reach terminal state: ${JSON.stringify(await readJob(root, id))}`);
}

async function fixture(root, command, timeoutSeconds = 0) {
  const id = createJobId();
  const now = new Date().toISOString();
  await createJob(root, {
    id,
    state: "QUEUED",
    command,
    cwd: root,
    timeoutSeconds,
    createdAt: now,
    updatedAt: now,
    notification: { status: "pending", attempts: 0, idempotencyKey: `test:${id}` },
  });
  return id;
}

test("worker records success and failure", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "durable-worker-test-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const successId = await fixture(root, ["/bin/sh", "-c", "printf success"]);
  const failureId = await fixture(root, ["/bin/sh", "-c", "printf failure >&2; exit 7"]);
  spawn(process.execPath, [workerPath, root, successId], { stdio: "ignore" });
  spawn(process.execPath, [workerPath, root, failureId], { stdio: "ignore" });
  assert.equal((await waitForTerminal(root, successId)).state, "SUCCEEDED");
  const failed = await waitForTerminal(root, failureId);
  assert.equal(failed.state, "FAILED");
  assert.equal(failed.exitCode, 7);
});

test("worker enforces timeout", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "durable-worker-timeout-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const id = await fixture(root, ["/bin/sh", "-c", "sleep 5"], 1);
  spawn(process.execPath, [workerPath, root, id], { stdio: "ignore" });
  const job = await waitForTerminal(root, id, 8000);
  assert.equal(job.state, "TIMED_OUT");
});
