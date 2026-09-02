#!/usr/bin/env node
import { open, readFile, writeFile, stat } from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import {
  appendWorkerLog,
  readJob,
  signalProcessGroup,
  TERMINAL_STATES,
  updateJob,
} from "./job-store.js";
import { classifyProviderState } from "./evaluator.js";
import { computeJobOutcome, computeProcessState } from "./verdict.js";
import { fileSize, sampleChildCpuMs, scanLatestStep, writeHeartbeat } from "./heartbeat.js";
import { computeToolchainFingerprint, computeWorktreeFingerprint } from "./workflow-fingerprint.js";

// P3-F #1: for a workflow-linked job the reconciler freezes an execution fingerprint (executable
// realpath/content-hash/size + worktree aggregate hash). Re-verify it HERE, inside the worker's own process,
// immediately before spawning the child — the deepest TOCTOU close (the reconciler-side guard ran earlier, in
// a different process). Any drift ⇒ return the authoritative failure code so the caller records it and spawns
// NO child. A standalone durable_job has no validatedExecution and is unaffected.
async function verifyValidatedExecution(job) {
  const ve = job.validatedExecution;
  if (!ve) return null;
  if (ve.toolchain?.aggregateHash) {
    const t = ve.toolchain;
    const tc = await computeToolchainFingerprint({ argv: [t.executableRealpath ?? t.executableBasename], cwd: job.cwd, runnerType: t.runnerType, runnerProfile: t.runnerProfile });
    if (tc.status !== "COMPLETE"
      || tc.aggregateHash !== t.aggregateHash
      || (t.executableContentHash && tc.executableContentHash !== t.executableContentHash)
      || (t.executableSize != null && tc.executableSize !== t.executableSize)
      || (t.executableRealpath && tc.executableRealpath !== t.executableRealpath)) {
      return "WORKFLOW_TOOLCHAIN_CHANGED";
    }
  }
  if (ve.worktreeAggregateHash) {
    const wt = await computeWorktreeFingerprint(ve.worktree ?? job.cwd, ve.fingerprint ?? {});
    if (wt.status !== "COMPLETE" || wt.aggregateHash !== ve.worktreeAggregateHash) return "WORKFLOW_CHECKPOINT_CHANGED";
  }
  return null;
}

// Test-only spawn barrier (NEVER set in production): after the output files are prepared and just before the
// pre-spawn re-verify, signal readiness (`<file>.ready`) and block until released (`<file>.go`). This lets a
// smoke mutate the executable / worktree precisely in the verify→spawn window and prove the barrier rejects
// the changed execution with zero child spawns. Gated strictly on WF_SPAWN_BARRIER_FILE.
async function maybeSpawnBarrier() {
  const bf = process.env.WF_SPAWN_BARRIER_FILE;
  if (!bf) return;
  await writeFile(`${bf}.ready`, "1").catch(() => {});
  while (!(await stat(`${bf}.go`).then(() => true).catch(() => false))) {
    await new Promise((r) => setTimeout(r, 10));
  }
}

// Bounded read of the tail of a captured stdout file, for provider-envelope extraction only.
async function readStdoutTail(file, maxBytes = 65_536) {
  try {
    const contents = await readFile(file, "utf8");
    return contents.length <= maxBytes ? contents : contents.slice(contents.length - maxBytes);
  } catch {
    return "";
  }
}

async function run() {
  const [rootDir, jobId] = process.argv.slice(2);
  if (!rootDir || !jobId) throw new Error("usage: worker.js <rootDir> <jobId>");
  const directory = path.join(rootDir, jobId);
  const initial = await readJob(rootDir, jobId);
  if (TERMINAL_STATES.has(initial.state)) return;

  // #1: prepare the output files + spawn options FIRST (no state change yet); re-verify the frozen execution
  // fingerprint as the LAST step before spawn (with NO await between verify success and spawn — the tightest
  // possible TOCTOU window); record RUNNING + pids atomically only AFTER a real child exists (RUNNING is never
  // exposed without a live child).
  const stdoutPath = path.join(directory, "stdout.log");
  const stderrPath = path.join(directory, "stderr.log");
  const stdout = await open(stdoutPath, "a", 0o600);
  const stderr = await open(stderrPath, "a", 0o600);
  let child;
  let timedOut = false;
  let timer;
  let heartbeatTimer;
  // Heartbeat observation state (P2-A). observerHeartbeatAt marks the WORKER observer's liveness every
  // write; lastProgressAt marks observable CHILD progress (stdout/stderr byte growth OR a currentStep
  // change). A byte-size read failure keeps the last known value and never fails the heartbeat or child.
  const hb = { lastOutputAt: null, lastProgressAt: null, stdoutBytes: 0, stderrBytes: 0, currentStep: null };
  const policy = initial.observability ?? {};
  const heartbeatIntervalMs = Number.isInteger(policy.heartbeatIntervalMs) ? policy.heartbeatIntervalMs : 5000;
  const wantCpuConfirm = policy.stallConfirmSignal === "cpu";
  // Serialize heartbeat writes into a single chain so two writes never overlap; queued RUNNING writes
  // become no-ops once the child has exited so a late write can never overwrite the final EXITED sample.
  let hbChain = Promise.resolve();
  let hbStopped = false;
  async function writeOneHeartbeat(childProcessState) {
    try {
      const now = () => new Date().toISOString();
      const sb = await fileSize(stdoutPath);
      const eb = await fileSize(stderrPath);
      let grew = false;
      if (Number.isInteger(sb)) { if (sb > hb.stdoutBytes) { hb.lastOutputAt = now(); grew = true; } hb.stdoutBytes = sb; }
      if (Number.isInteger(eb)) { if (eb > hb.stderrBytes) { hb.lastOutputAt = now(); grew = true; } hb.stderrBytes = eb; }
      const step = await scanLatestStep([stdoutPath, stderrPath]);
      const stepChanged = step && step !== hb.currentStep;
      if (step) hb.currentStep = step; // sticky: never revert a known step to null
      if (grew || stepChanged) hb.lastProgressAt = now(); // CHILD progress signal (distinct from observer)
      const childCpuMs = wantCpuConfirm ? await sampleChildCpuMs(child?.pid) : null;
      await writeHeartbeat(directory, {
        observerHeartbeatAt: now(),
        lastOutputAt: hb.lastOutputAt,
        lastProgressAt: hb.lastProgressAt,
        stdoutBytes: hb.stdoutBytes,
        stderrBytes: hb.stderrBytes,
        childPid: child?.pid ?? null,
        childProcessState,
        currentStep: hb.currentStep,
        childCpuMs,
      });
    } catch {
      // A heartbeat write failure must never interrupt the child or its terminal result.
    }
  }
  function scheduleHeartbeat(state) {
    hbChain = hbChain.then(() => {
      if (hbStopped && state === "RUNNING") return undefined; // do not resurrect RUNNING after exit
      return writeOneHeartbeat(state);
    });
    return hbChain;
  }
  await maybeSpawnBarrier(); // test-only; no-op unless WF_SPAWN_BARRIER_FILE is set
  // #1: the final barrier — re-verify the frozen execution fingerprint (executable content/size/realpath +
  // worktree fingerprint) as the LAST thing before spawn. Drift ⇒ close the descriptors, settle terminal
  // FAILED (FAILED_COMMAND) with the authoritative code in .error, and spawn NO child. Nothing awaits between a
  // successful verify and the spawn below.
  const drift = await verifyValidatedExecution(initial);
  if (drift) {
    await Promise.allSettled([stdout.close(), stderr.close()]);
    await updateJob(rootDir, jobId, (job) => {
      if (TERMINAL_STATES.has(job.state)) return null;
      job.state = "FAILED";
      job.processState = "FAILED_COMMAND";
      job.providerState = "UNKNOWN";
      job.jobOutcome = "FAILED_COMMAND";
      job.error = drift;
      job.endedAt = new Date().toISOString();
      job.updatedAt = job.endedAt;
      return job;
    });
    await appendWorkerLog(directory, `execution re-verify failed (${drift}); no child spawned`);
    return;
  }
  try {
    child = spawn(initial.command[0], initial.command.slice(1), {
      cwd: initial.cwd,
      env: process.env,
      detached: true,
      stdio: ["ignore", stdout.fd, stderr.fd],
    });
    // Attach lifecycle listeners before any await. Very short commands can
    // exit while the job row is being updated; attaching afterwards loses the
    // one-shot exit event and leaves the wrapper waiting forever.
    const childResult = new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => resolve({ code, signal }));
    });
    // A real child now exists — record RUNNING + pids atomically (RUNNING was never exposed beforehand).
    await updateJob(rootDir, jobId, (job) => {
      if (TERMINAL_STATES.has(job.state)) return null;
      job.state = "RUNNING";
      job.processState = "RUNNING";
      job.startedAt = new Date().toISOString();
      job.workerPid = process.pid;
      job.childPid = child.pid;
      return job;
    });
    await appendWorkerLog(directory, `started child pid=${child.pid}`);

    // P2-A: periodic heartbeat, unref'd so it never keeps the worker alive on its own. Writes are
    // serialized via scheduleHeartbeat (no overlapping writes). Seed lastProgressAt with the start time so
    // a just-started job is HEALTHY until it has been silent for a whole silence budget.
    hb.lastProgressAt = new Date().toISOString();
    await scheduleHeartbeat("RUNNING"); // one immediate sample
    heartbeatTimer = setInterval(() => {
      void scheduleHeartbeat("RUNNING");
    }, heartbeatIntervalMs);
    heartbeatTimer.unref();

    if (initial.timeoutSeconds > 0) {
      timer = setTimeout(() => {
        timedOut = true;
        try {
          signalProcessGroup(child.pid, "SIGTERM");
        } catch {
          // Reconciliation still resolves the terminal state.
        }
        setTimeout(() => {
          try {
            signalProcessGroup(child.pid, "SIGKILL");
          } catch {
            // Already exited.
          }
        }, 5000).unref();
      }, initial.timeoutSeconds * 1000);
      timer.unref();
    }

    const result = await childResult;
    // Stop heartbeats deterministically: stop scheduling RUNNING writes, drain any in-flight write, then
    // write the final EXITED sample last so a late RUNNING write can never overwrite it.
    hbStopped = true;
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    heartbeatTimer = undefined;
    await hbChain; // wait for a possibly in-flight RUNNING write to finish
    await writeOneHeartbeat("EXITED"); // final, serialized after the drain
    const endedAt = new Date().toISOString();
    // Process outcome from the OS child (authoritative). Provider outcome from the model activity's
    // structured stdout envelope (NOT a full-stdout scan). job_outcome combines them — exit 0 alone yields
    // at most COMPLETED_UNVERIFIED, never a success.
    const processState = computeProcessState({ code: result.code, signal: result.signal, timedOut });
    // Provider evaluation is gated: ONLY a model activity that speaks the agy-json protocol is judged by
    // its stdout envelope. A local activity has no provider protocol → provider_state = OK by definition,
    // so its plain stdout (even a literal {"status":"ERROR"}) can never make it FAILED_PROVIDER.
    const providerState =
      initial.resultProtocol === "agy-json"
        ? classifyProviderState(await readStdoutTail(path.join(directory, "stdout.log"))).providerState
        : "OK";
    const jobOutcome = computeJobOutcome(processState, providerState);
    await updateJob(rootDir, jobId, (job) => {
      if (TERMINAL_STATES.has(job.state)) return null;
      job.endedAt = endedAt;
      job.exitCode = result.code;
      job.exitSignal = result.signal;
      // Legacy `state` alias, preserved verbatim for backward compatibility (reconcile/TERMINAL_STATES/
      // delivery-claim key). New separated fields drive the new terminal wording; `state` is NEVER read as
      // semantic success by the new logic.
      if (timedOut) {
        job.state = "TIMED_OUT";
        job.error = `command exceeded ${job.timeoutSeconds}s`;
      } else if (result.code === 0) {
        job.state = "SUCCEEDED";
      } else {
        job.state = "FAILED";
        job.error = `command exited with code ${result.code ?? "null"}${result.signal ? ` (${result.signal})` : ""}`;
      }
      // Additive P0 fields (separated state model).
      job.processState = processState;
      job.providerState = providerState;
      job.jobOutcome = jobOutcome;
      job.notification.status = "pending";
      return job;
    });
    await appendWorkerLog(
      directory,
      `finished code=${result.code ?? "null"} signal=${result.signal ?? "none"} timedOut=${timedOut}`,
    );
  } catch (error) {
    await appendWorkerLog(directory, `worker error: ${error?.stack ?? error}`);
    await updateJob(rootDir, jobId, (job) => {
      if (TERMINAL_STATES.has(job.state)) return null;
      job.state = "FAILED";
      job.error = error instanceof Error ? error.message : String(error);
      job.endedAt = new Date().toISOString();
      // The command never produced a usable result: command-layer failure, provider unknown.
      job.processState = "FAILED_COMMAND";
      job.providerState = "UNKNOWN";
      job.jobOutcome = "FAILED_COMMAND";
      job.notification.status = "pending";
      return job;
    });
  } finally {
    if (timer) clearTimeout(timer);
    if (heartbeatTimer) clearInterval(heartbeatTimer); // ensure cleanup on the error path too
    await Promise.allSettled([stdout.close(), stderr.close()]);
  }
}

run().catch(async (error) => {
  try {
    const [rootDir, jobId] = process.argv.slice(2);
    if (rootDir && jobId) {
      await appendWorkerLog(path.join(rootDir, jobId), `fatal worker error: ${error?.stack ?? error}`);
    }
  } finally {
    process.exitCode = 1;
  }
});
