import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";

import { resolveRunnerMetadata } from "../dist/evaluator.js";
import { startJob } from "../dist/core.js";
import { createJob, createJobId, listJobs, readJob } from "../dist/job-store.js";

// ---------- resolveRunnerMetadata (pure) ----------

test("no metadata + agy command → model / model_agy / agy-json", () => {
  assert.deepEqual(resolveRunnerMetadata({ command: ["agy", "--print", "x"] }), {
    runnerType: "model", runnerProfile: "model_agy", activityType: "model", resultProtocol: "agy-json",
  });
  // absolute path to agy resolves the same
  assert.equal(resolveRunnerMetadata({ command: ["/opt/homebrew/bin/agy"] }).runnerProfile, "model_agy");
});

test("no metadata + ordinary command → local / generic_local / none", () => {
  assert.deepEqual(resolveRunnerMetadata({ command: ["node", "--test"] }), {
    runnerType: "local", runnerProfile: "generic_local", activityType: "local", resultProtocol: "none",
  });
});

test("basename resolution: any absolute path ending in /agy → model_agy", () => {
  assert.equal(resolveRunnerMetadata({ command: ["/absolute/path/to/agy", "--print", "x"] }).runnerProfile, "model_agy");
});

test("basename resolution: `agy-wrapper` is NOT auto-guessed as model → generic_local (documented limitation)", () => {
  // Only the exact basename `agy` is a known model executable. A wrapper is NOT trusted as a model runner;
  // to run a wrapper as model it must be made an explicit model profile only if it truly is `agy`.
  assert.deepEqual(resolveRunnerMetadata({ command: ["agy-wrapper", "--print"] }), {
    runnerType: "local", runnerProfile: "generic_local", activityType: "local", resultProtocol: "none",
  });
  // and forcing a wrapper to model_agy is rejected (it is not the agy executable)
  rejects({ command: ["agy-wrapper"], runnerProfile: "model_agy" }, /requires the "agy" executable/);
});

test("explicit local_test + local command → allowed", () => {
  assert.deepEqual(resolveRunnerMetadata({ command: ["pytest"], runnerProfile: "local_test" }), {
    runnerType: "local", runnerProfile: "local_test", activityType: "local", resultProtocol: "none",
  });
});

test("explicit model_agy + agy command → allowed (agy-json)", () => {
  assert.equal(resolveRunnerMetadata({ command: ["agy"], runnerProfile: "model_agy" }).resultProtocol, "agy-json");
  assert.equal(resolveRunnerMetadata({ command: ["agy"], runnerType: "model", runnerProfile: "model_agy" }).runnerType, "model");
});

const rejects = (input, re = /RUNNER_METADATA_INVALID/) =>
  assert.throws(() => resolveRunnerMetadata(input), (e) => e.code === "RUNNER_METADATA_INVALID" && re.test(e.message));

test("agy command + runnerType=local → rejected (no model→local downgrade)", () => {
  rejects({ command: ["agy", "--print", "x"], runnerType: "local" }, /known model executable/);
});

test("agy command + generic_local → rejected", () => {
  rejects({ command: ["agy"], runnerProfile: "generic_local" }, /known model executable/);
});

test("ordinary command + model_agy → rejected (requires agy)", () => {
  rejects({ command: ["node", "-e", "1"], runnerProfile: "model_agy" }, /requires the "agy" executable/);
});

test("runnerType vs runnerProfile mismatch → rejected", () => {
  rejects({ command: ["node"], runnerType: "model", runnerProfile: "local_test" }, /conflicts with runnerProfile/);
  rejects({ command: ["node"], runnerType: "local", runnerProfile: "model_agy" }, /conflicts with runnerProfile/);
});

test("invalid runnerType / unknown runnerProfile → rejected", () => {
  rejects({ command: ["node"], runnerType: "gpu" });
  rejects({ command: ["node"], runnerProfile: "local_gpu" });
});

test("runnerType=model on a non-agy command → rejected", () => {
  rejects({ command: ["node", "-e", "1"], runnerType: "model" }, /requires the "agy" executable/);
});

test("caller cannot manipulate resultProtocol/providerState (ignored; derived from profile)", () => {
  // extra keys are ignored; resultProtocol comes only from the validated profile
  const out = resolveRunnerMetadata({ command: ["node"], runnerProfile: "local_test", resultProtocol: "agy-json", providerState: "OK" });
  assert.equal(out.resultProtocol, "none");
  const out2 = resolveRunnerMetadata({ command: ["agy"], runnerProfile: "model_agy", resultProtocol: "none" });
  assert.equal(out2.resultProtocol, "agy-json");
});

// ---------- startJob integration (metadata stored / rejected before side effects) ----------

async function withDirs(run) {
  const store = await mkdtemp(path.join(os.tmpdir(), "durable-p2b-store-"));
  const work = await mkdtemp(path.join(os.tmpdir(), "durable-p2b-work-"));
  try { await run({ store, work }); }
  finally { await Promise.all([rm(store, { recursive: true, force: true }), rm(work, { recursive: true, force: true })]); }
}

// context-free ctx (no gatewayCall route freeze needed) with a fixed owner deliveryRoute
function ctxFor(work) {
  return {
    sessionKey: null, agentId: "a", workspaceDir: work, durableAllowedRoots: [work],
    ownerDeliveryRoute: { routeKind: "channel_root", channel: "slack", to: "channel:C1" },
    deliveryContext: null,
  };
}
const deps = (store) => ({
  rootDir: store,
  config: { maxConcurrent: 4, defaultTimeoutSeconds: 0, deliveryMaxAttempts: 8, sendLeaseMs: 30_000 },
  gatewayCall: async () => ({ result: {} }),
  createFlow: () => ({ flowId: null }),
  spawnWorker: () => 4242,
});

test("startJob stores effective runner metadata (explicit model_agy + agy) and publicJob exposes it", async () => {
  await withDirs(async ({ store, work }) => {
    const created = await startJob(deps(store), ctxFor(work), {
      name: "j", command: ["agy", "--print", "x"], cwd: work, runnerProfile: "model_agy",
    });
    assert.equal(created.runnerType, "model");
    assert.equal(created.runnerProfile, "model_agy");
    assert.equal(created.resultProtocol, "agy-json");
    const job = await readJob(store, created.id);
    assert.equal(job.runnerType, "model");
    assert.equal(job.resultProtocol, "agy-json");
  });
});

test("startJob infers local/generic_local/none for an ordinary command", async () => {
  await withDirs(async ({ store, work }) => {
    const created = await startJob(deps(store), ctxFor(work), { name: "j", command: ["echo", "hi"], cwd: work });
    assert.equal(created.runnerType, "local");
    assert.equal(created.runnerProfile, "generic_local");
    assert.equal(created.resultProtocol, "none");
  });
});

test("startJob REJECTS an incompatible combo before creating any job", async () => {
  await withDirs(async ({ store, work }) => {
    await assert.rejects(
      () => startJob(deps(store), ctxFor(work), { name: "j", command: ["agy"], cwd: work, runnerType: "local" }),
      (e) => e.code === "RUNNER_METADATA_INVALID",
    );
    assert.equal((await listJobs(store)).length, 0, "no job row created on rejection");
  });
});

// ---------- worker evaluator gating driven by the resolved resultProtocol ----------

const workerPath = new URL("../dist/worker.js", import.meta.url).pathname;
async function runWithProtocol(program, resultProtocol, exitCode = 0) {
  const root = await mkdtemp(path.join(os.tmpdir(), "durable-p2b-run-"));
  const id = createJobId();
  const now = new Date().toISOString();
  await createJob(root, {
    id, state: "QUEUED", processState: "QUEUED", resultProtocol,
    command: [process.execPath, "-e", program + (exitCode ? `;process.exit(${exitCode})` : "")],
    cwd: root, timeoutSeconds: 0, createdAt: now, updatedAt: now,
    parent: { agentId: "a", sessionKey: null }, notification: { status: "pending" },
  });
  const child = spawn(process.execPath, [workerPath, root, id], { stdio: "ignore" });
  await new Promise((r) => child.once("exit", r));
  let job;
  for (let i = 0; i < 100; i++) { job = await readJob(root, id); if (["SUCCEEDED", "FAILED"].includes(job.state)) break; await new Promise((r) => setTimeout(r, 30)); }
  await rm(root, { recursive: true, force: true });
  return job;
}

test("local runner (resultProtocol none): {\"status\":\"ERROR\"} stdout is NOT a provider failure", async () => {
  const rp = resolveRunnerMetadata({ command: ["node", "-e", "1"], runnerProfile: "local_test" }).resultProtocol;
  assert.equal(rp, "none");
  const job = await runWithProtocol('process.stdout.write(JSON.stringify({status:"ERROR",error:"a test failed"}))', rp);
  assert.equal(job.providerState, "OK");
  assert.equal(job.jobOutcome, "COMPLETED_UNVERIFIED");
});

test("model_agy runner (resultProtocol agy-json): AGY ERROR envelope → FAILED_PROVIDER", async () => {
  const rp = resolveRunnerMetadata({ command: ["agy"], runnerProfile: "model_agy" }).resultProtocol;
  assert.equal(rp, "agy-json");
  const job = await runWithProtocol('process.stdout.write(JSON.stringify({status:"ERROR",error:"boom"}))', rp);
  assert.equal(job.providerState, "ERROR_UNCLASSIFIED");
  assert.equal(job.jobOutcome, "FAILED_PROVIDER");
});
