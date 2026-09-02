// Shared P3-C test helpers (not a test file itself). A fake startJob that persists a real job.json row via
// the durable-jobs store WITHOUT spawning a worker, so linkage/dedup/terminal-verdict paths are exercised
// deterministically. Also a helper to drive a stored job to a terminal outcome.
import { readFile, writeFile, stat } from "node:fs/promises";
import path from "node:path";
import { createJob, createJobId, jobDir, listJobs, readJob } from "../dist/job-store.js";

// Returns { startJob, calls, wfLockHeldDuringCall }. The fake mimics core.startJob's STORAGE (a QUEUED
// new-format job row carrying params.workflowLink + params.deliveryRoute) with no child process. It also
// records whether the workflow's .wf.lock was held at call time (must be false — startJob runs OUTSIDE the
// workflow lock).
export function makeFakeStartJob() {
  const calls = [];
  let wfLockHeldDuringCall = false;
  const startJob = async (startDeps, ctx, params) => {
    calls.push({ ctx, params });
    if (params.workflowLink?.workflowId) {
      const lockPath = path.join(startDeps.rootDir, "workflows", params.workflowLink.workflowId, ".wf.lock");
      if (await stat(lockPath).then(() => true).catch(() => false)) wfLockHeldDuringCall = true;
    }
    const id = createJobId();
    const now = new Date().toISOString();
    const job = {
      version: 1,
      id,
      name: params.name,
      state: "QUEUED",
      processState: "QUEUED",
      providerState: null,
      jobOutcome: null,
      runnerType: params.runnerType ?? null,
      runnerProfile: params.runnerProfile ?? null,
      cwd: params.cwd,
      command: params.command,
      timeoutSeconds: params.timeoutSeconds ?? 0,
      createdAt: now,
      updatedAt: now,
      startedAt: null,
      endedAt: null,
      agentId: ctx.agentId ?? null,
      sessionKey: ctx.sessionKey ?? null,
      parent: { agentId: ctx.agentId ?? null, sessionKey: ctx.sessionKey ?? null, flowId: null },
      flowId: null,
      deliveryRoute: params.deliveryRoute ?? null,
      workflowLink: params.workflowLink ?? null,
      directory: jobDir(startDeps.rootDir, id),
      // minimal fields so the real cancelJob (which touches notification) works on this fake row
      notification: { status: "pending", attempts: 0, idempotencyKey: `durable-job:${id}:terminal` },
      delivery: null,
    };
    await createJob(startDeps.rootDir, job);
    return { ...job };
  };
  return { startJob, calls, get wfLockHeldDuringCall() { return wfLockHeldDuringCall; } };
}

// Drive a stored job to a terminal outcome (new-format fields), as worker.js would at exit.
export async function setJobTerminal(rootDir, jobId, { processState, providerState = null, jobOutcome, state = "SUCCEEDED" }) {
  const file = path.join(jobDir(rootDir, jobId), "job.json");
  const job = JSON.parse(await readFile(file, "utf8"));
  job.state = state;
  job.processState = processState;
  job.providerState = providerState;
  job.jobOutcome = jobOutcome;
  job.endedAt = new Date().toISOString();
  job.updatedAt = job.endedAt;
  await writeFile(file, JSON.stringify(job, null, 2));
}

// P3-F: deterministic seams that make preflight PASS without a real Git worktree — a COMPLETE worktree
// fingerprint, a COMPLETE toolchain, and a READY provider probe. Non-fingerprint tests spread these so the
// (production) "new submission requires a COMPLETE Git fingerprint" policy does not block their fake jobs.
export function completeSeams({ worktreeHash = "WT", probe = "READY" } = {}) {
  return {
    captureFingerprint: async () => ({ fingerprintVersion: 1, status: "COMPLETE", aggregateHash: worktreeHash, capturedAt: new Date().toISOString() }),
    captureToolchain: async ({ runnerType, runnerProfile }) => ({ fingerprintVersion: 1, status: "COMPLETE", aggregateHash: "tc-" + runnerProfile, executableBasename: "x", executableRealpath: null, runnerType, runnerProfile }),
    providerProbe: async () => ({ status: probe }),
    providerConfigFingerprint: "test",
  };
}

export { listJobs, readJob };
