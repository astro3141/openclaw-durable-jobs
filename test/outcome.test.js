import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";
import { createJob, createJobId, readJob } from "../dist/job-store.js";

const workerPath = new URL("../dist/worker.js", import.meta.url).pathname;

async function waitForTerminal(root, id, timeoutMs = 6000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = await readJob(root, id);
    if (["SUCCEEDED", "FAILED", "TIMED_OUT"].includes(job.state)) return job;
    await new Promise((r) => setTimeout(r, 30));
  }
  throw new Error(`worker did not terminate: ${JSON.stringify(await readJob(root, id))}`);
}

// Run the worker against a node -e program that writes an exact stdout and exit code.
// resultProtocol "agy-json" simulates a model activity (the evaluator runs); "none" is a local activity.
async function runOutcome(program, { exitCode = 0, resultProtocol = "agy-json" } = {}) {
  const root = await mkdtemp(path.join(os.tmpdir(), "durable-outcome-"));
  const id = createJobId();
  const now = new Date().toISOString();
  await createJob(root, {
    id,
    state: "QUEUED",
    processState: "QUEUED",
    activityType: resultProtocol === "agy-json" ? "model" : "local",
    resultProtocol,
    command: [process.execPath, "-e", program + (exitCode ? `;process.exit(${exitCode})` : "")],
    cwd: root,
    timeoutSeconds: 0,
    createdAt: now,
    updatedAt: now,
    // new-format marker so the worker + delivery treat it as a P0 job
    parent: { agentId: "a", sessionKey: null, sessionId: null, requesterOrigin: null, flowId: null },
    notification: { status: "pending" },
  });
  const child = spawn(process.execPath, [workerPath, root, id], { stdio: "ignore" });
  await new Promise((res) => child.once("exit", res));
  const job = await waitForTerminal(root, id);
  await rm(root, { recursive: true, force: true });
  return job;
}

test("exit 0 + AGY SUCCESS envelope → COMPLETED_UNVERIFIED (not SUCCEEDED semantics)", async () => {
  const job = await runOutcome('process.stdout.write(JSON.stringify({status:"SUCCESS",response:"OK"}))');
  assert.equal(job.processState, "COMPLETED");
  assert.equal(job.providerState, "OK");
  assert.equal(job.jobOutcome, "COMPLETED_UNVERIFIED");
  assert.equal(job.state, "SUCCEEDED"); // legacy alias preserved
});

test("exit 0 + AGY ERROR (unknown) envelope → FAILED_PROVIDER / ERROR_UNCLASSIFIED", async () => {
  const job = await runOutcome('process.stdout.write(JSON.stringify({status:"ERROR",error:"brand new failure"}))');
  assert.equal(job.processState, "COMPLETED");
  assert.equal(job.providerState, "ERROR_UNCLASSIFIED");
  assert.equal(job.jobOutcome, "FAILED_PROVIDER");
});

test("exit 0 + AGY ERROR timeout signature → FAILED_PROVIDER / TOOL_INTERRUPTED", async () => {
  const job = await runOutcome('process.stdout.write(JSON.stringify({status:"ERROR",error:"timeout waiting for response"}))');
  assert.equal(job.providerState, "TOOL_INTERRUPTED");
  assert.equal(job.jobOutcome, "FAILED_PROVIDER");
});

test("nonzero exit + no JSON envelope → FAILED_COMMAND", async () => {
  const job = await runOutcome('process.stdout.write("plain runner output\\nok 1")', { exitCode: 3 });
  assert.equal(job.processState, "FAILED_COMMAND");
  assert.equal(job.jobOutcome, "FAILED_COMMAND");
  assert.equal(job.state, "FAILED");
});

test("malformed envelope + exit 0 → COMPLETED_UNVERIFIED / UNKNOWN", async () => {
  const job = await runOutcome('process.stdout.write("{\\"status\\":\\"SUCCESS\\"")');
  assert.equal(job.processState, "COMPLETED");
  assert.equal(job.providerState, "UNKNOWN");
  assert.equal(job.jobOutcome, "COMPLETED_UNVERIFIED");
});

test("quota/error keywords in the SUCCESS envelope response do NOT flip it (no full-stdout scan)", async () => {
  const job = await runOutcome(
    'process.stdout.write(JSON.stringify({status:"SUCCESS",response:"quota exhausted error auth failed context limit"}))',
  );
  assert.equal(job.providerState, "OK");
  assert.equal(job.jobOutcome, "COMPLETED_UNVERIFIED");
});

test("LOCAL activity printing {\"status\":\"ERROR\"} + exit 0 is NOT misclassified as FAILED_PROVIDER", async () => {
  const job = await runOutcome('process.stdout.write(JSON.stringify({status:"ERROR",error:"a test assertion failed"}))', {
    resultProtocol: "none",
  });
  assert.equal(job.processState, "COMPLETED");
  assert.equal(job.providerState, "OK"); // local: no provider protocol, evaluator NOT applied
  assert.equal(job.jobOutcome, "COMPLETED_UNVERIFIED");
});

test("LOCAL activity nonzero exit → FAILED_COMMAND", async () => {
  const job = await runOutcome('process.stdout.write("1..1\\nnot ok 1")', { exitCode: 1, resultProtocol: "none" });
  assert.equal(job.processState, "FAILED_COMMAND");
  assert.equal(job.jobOutcome, "FAILED_COMMAND");
});
