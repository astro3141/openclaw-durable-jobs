// P2-A heartbeat & stall observability (design P2-0 §3). Pure helpers + a separate lock-free
// `heartbeat.json` per job (single writer = the worker). livenessState is an OBSERVATION only, distinct
// from processState: SUSPECTED_STALL / STALLED never kill, retry, or change processState. Only the
// existing hard `timeoutSeconds` kills; only a vanished worker with no terminal record is LOST.
//
// Observer vs child progress (audit fix): a fresh `observerHeartbeatAt` proves the WORKER observer is
// alive — NOT that the child is making progress. HEALTHY therefore requires observable CHILD progress
// (`lastProgressAt` = stdout/stderr byte growth or a currentStep change). A live observer with a stalled
// child (no progress) beyond the silence budget is SUSPECTED_STALL. STALLED is a stronger claim reached
// ONLY with a positive confirm signal (child CPU-time not increasing over the confirm window); profiles
// with no confirm signal cap at SUSPECTED_STALL. This is inherent: without a child-progress signal, a
// legitimately quiet process cannot be distinguished from a stalled one — so we stay conservative.

import { writeFile, rename, readFile, stat, open, unlink } from "node:fs/promises";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import path from "node:path";

export const LIVENESS_STATES = new Set(["HEALTHY", "SUSPECTED_STALL", "STALLED"]);

// Per-runnerProfile observability defaults. stallConfirmSignal is null by default → STALLED is never
// asserted (cap at SUSPECTED_STALL). Opt in per profile with "cpu" to enable CPU-flat STALLED confirmation.
export const DEFAULT_RUNNER_OBSERVABILITY = {
  model_agy: { heartbeatIntervalMs: 5000, silenceBudgetMs: 120000, stallConfirmMs: 120000, stallConfirmSignal: null },
  local_test: { heartbeatIntervalMs: 5000, silenceBudgetMs: 60000, stallConfirmMs: 60000, stallConfirmSignal: null },
  local_build: { heartbeatIntervalMs: 5000, silenceBudgetMs: 120000, stallConfirmMs: 120000, stallConfirmSignal: null },
  local_docker: { heartbeatIntervalMs: 5000, silenceBudgetMs: 300000, stallConfirmMs: 180000, stallConfirmSignal: null },
  generic_local: { heartbeatIntervalMs: 5000, silenceBudgetMs: 120000, stallConfirmMs: 120000, stallConfirmSignal: null },
};

function observabilityError(message) {
  const error = new Error(`OBSERVABILITY_CONFIG_INVALID: ${message}`);
  error.code = "OBSERVABILITY_CONFIG_INVALID";
  return error;
}

// Resolve the effective policy for a runnerProfile: profile default ← global heartbeat override ←
// per-profile config override. Validates the merged values (positive; heartbeatInterval < silenceBudget).
export function resolveObservabilityPolicy(config, runnerProfile) {
  const base = DEFAULT_RUNNER_OBSERVABILITY[runnerProfile] ?? DEFAULT_RUNNER_OBSERVABILITY.generic_local;
  const override = (config?.runnerObservability && config.runnerObservability[runnerProfile]) || {};
  const policy = {
    heartbeatIntervalMs: pickInt(override.heartbeatIntervalMs, config?.heartbeatIntervalMs, base.heartbeatIntervalMs),
    silenceBudgetMs: pickInt(override.silenceBudgetMs, base.silenceBudgetMs),
    stallConfirmMs: pickInt(override.stallConfirmMs, base.stallConfirmMs),
    stallConfirmSignal:
      override.stallConfirmSignal === "cpu" ? "cpu" : base.stallConfirmSignal === "cpu" ? "cpu" : null,
  };
  if (!(policy.heartbeatIntervalMs > 0)) throw observabilityError(`heartbeatIntervalMs must be > 0 (${runnerProfile})`);
  if (!(policy.silenceBudgetMs > 0)) throw observabilityError(`silenceBudgetMs must be > 0 (${runnerProfile})`);
  if (!(policy.stallConfirmMs > 0)) throw observabilityError(`stallConfirmMs must be > 0 (${runnerProfile})`);
  if (!(policy.heartbeatIntervalMs < policy.silenceBudgetMs)) {
    throw observabilityError(`heartbeatIntervalMs (${policy.heartbeatIntervalMs}) must be < silenceBudgetMs (${policy.silenceBudgetMs}) for ${runnerProfile}`);
  }
  return policy;
}

function pickInt(...values) {
  for (const v of values) if (Number.isInteger(v)) return v;
  return undefined;
}

// ---- progress marker: a COMPLETE line `##WF-STEP name=<safe-name>` updates currentStep ----
export const SAFE_STEP_NAME = /^[A-Za-z0-9_.:-]{1,64}$/;
const MARKER_LINE = /^##WF-STEP\s+name=(\S+)\s*$/;

export function parseProgressMarker(line) {
  if (typeof line !== "string") return null;
  const m = MARKER_LINE.exec(line.trim());
  if (!m) return null;
  return SAFE_STEP_NAME.test(m[1]) ? m[1] : null;
}

// ---- bounded tail read (real I/O bound: reads at most maxBytes from the END of the file) ----
// Returns { text, truncated }. `truncated` is true when the file was larger than maxBytes, so the caller
// must drop the (possibly partial) first line.
export async function readTailBytes(file, maxBytes = 8192) {
  let fh;
  try {
    fh = await open(file, "r");
    const { size } = await fh.stat();
    const start = Math.max(0, size - maxBytes);
    const len = size - start;
    if (len <= 0) return { text: "", truncated: false };
    const buf = Buffer.allocUnsafe(len);
    await fh.read(buf, 0, len, start);
    return { text: buf.toString("utf8"), truncated: start > 0 };
  } catch {
    return { text: "", truncated: false };
  } finally {
    if (fh) await fh.close().catch(() => {});
  }
}

// Scan the bounded tails of stdout AND stderr for the latest complete `##WF-STEP` marker. The first line
// of a truncated tail is dropped (it may be partial). Returns the last valid step name or null.
export async function scanLatestStep(files, maxBytes = 8192) {
  let step = null;
  for (const file of files) {
    const { text, truncated } = await readTailBytes(file, maxBytes);
    if (!text) continue;
    const lines = text.split("\n");
    if (truncated) lines.shift(); // ignore the partial first line
    for (const line of lines) {
      const name = parseProgressMarker(line);
      if (name) step = name;
    }
  }
  return step;
}

// ---- child CPU-time sample (optional STALLED confirm signal) ----
export function parseCpuTime(raw) {
  if (typeof raw !== "string") return null;
  const s = raw.trim();
  const m = /^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+)(?:\.(\d+))?$/.exec(s) || /^(\d+):(\d+)(?:\.(\d+))?$/.exec(s);
  if (!m) return null;
  // Normalise to [days?, hours?, minutes, seconds, frac?]
  let days = 0, hours = 0, minutes, seconds, frac = 0;
  if (m.length === 6) {
    days = Number(m[1] || 0); hours = Number(m[2] || 0); minutes = Number(m[3]); seconds = Number(m[4]); frac = Number(("0." + (m[5] || "0")));
  } else {
    minutes = Number(m[1]); seconds = Number(m[2]); frac = Number(("0." + (m[3] || "0")));
  }
  return (((days * 24 + hours) * 60 + minutes) * 60 + seconds + frac) * 1000;
}

export function sampleChildCpuMs(pid) {
  return new Promise((resolve) => {
    if (!Number.isInteger(pid) || pid <= 0) return resolve(null);
    execFile("ps", ["-o", "time=", "-p", String(pid)], { timeout: 2000 }, (err, stdout) => {
      if (err) return resolve(null);
      resolve(parseCpuTime(stdout));
    });
  });
}

// ---- heartbeat.json I/O (atomic write with a unique temp; partial-read-safe read) ----
export function heartbeatPath(directory) {
  return path.join(directory, "heartbeat.json");
}

export async function writeHeartbeat(directory, record) {
  const file = heartbeatPath(directory);
  const tmp = `${file}.${process.pid}.${randomUUID()}.tmp`;
  let renamed = false;
  try {
    await writeFile(tmp, JSON.stringify(record), "utf8");
    await rename(tmp, file); // atomic: a reader never sees a partial file; unique tmp avoids any collision
    renamed = true;
  } finally {
    // On any write/rename failure, best-effort remove the orphaned temp so repeated heartbeat failures do
    // not accumulate .tmp files. A cleanup failure is itself swallowed (never blocks the child/terminal).
    if (!renamed) await unlink(tmp).catch(() => {});
  }
}

export async function readHeartbeat(directory) {
  try {
    const raw = await readFile(heartbeatPath(directory), "utf8");
    const obj = JSON.parse(raw);
    return obj && typeof obj === "object" ? obj : null;
  } catch {
    return null; // missing (legacy) or (defensively) unparneable → treat as absent
  }
}

export async function fileSize(file) {
  try {
    return (await stat(file)).size;
  } catch {
    return null; // caller keeps the last known value
  }
}

// ---- liveness classification (pure) ----
// prev: { livenessState, livenessSince, suspectCpuMs } from job.json (may be empty → implicitly HEALTHY).
// heartbeat: { observerHeartbeatAt, lastProgressAt, childCpuMs } (from heartbeat.json).
// policy: { silenceBudgetMs, stallConfirmMs, stallConfirmSignal }. pidAlive: boolean. now: ms.
// Returns { livenessState, livenessSince, suspectCpuMs } or null when liveness does not apply.
export function classifyLiveness({ prev = {}, heartbeat, policy, pidAlive, now = Date.now() }) {
  if (!heartbeat) return null; // no heartbeat file (legacy) → no derivation
  const observerAt = Date.parse(heartbeat.observerHeartbeatAt ?? heartbeat.lastHeartbeatAt ?? 0) || 0;
  const progressAt = Date.parse(heartbeat.lastProgressAt ?? heartbeat.lastOutputAt ?? 0) || 0;
  const observerFresh = observerAt > 0 && now - observerAt <= policy.silenceBudgetMs;
  const progressFresh = progressAt > 0 && now - progressAt <= policy.silenceBudgetMs;

  // HEALTHY requires observable CHILD progress — a fresh observer heartbeat alone is NOT enough.
  if (progressFresh) {
    const since = prev.livenessState === "HEALTHY" && prev.livenessSince ? prev.livenessSince : isoNow(now);
    return { livenessState: "HEALTHY", livenessSince: since, suspectCpuMs: null };
  }
  // No fresh child progress. If the observer itself is stale (worker not sampling) or the pid is gone, we
  // lack current readings — keep the prior observation (worker death is handled by LOST, not here).
  if (!observerFresh || !pidAlive) {
    return prev.livenessState
      ? { livenessState: prev.livenessState, livenessSince: prev.livenessSince, suspectCpuMs: prev.suspectCpuMs ?? null }
      : null;
  }
  // Observer alive + child pid alive + no child progress beyond the silence budget → at least SUSPECTED.
  const wasStalling = prev.livenessState === "SUSPECTED_STALL" || prev.livenessState === "STALLED";
  const since = wasStalling && prev.livenessSince ? prev.livenessSince : isoNow(now);
  const suspectCpuMs = wasStalling && Number.isFinite(prev.suspectCpuMs)
    ? prev.suspectCpuMs
    : Number.isFinite(heartbeat.childCpuMs) ? heartbeat.childCpuMs : null;
  const elapsed = now - (Date.parse(since) || now);
  // STALLED only with a positive confirm signal that shows no progress over the confirm window. Without a
  // confirm signal (or if CPU could not be sampled) → cap at SUSPECTED_STALL.
  const cpuConfirm =
    policy.stallConfirmSignal === "cpu" &&
    Number.isFinite(heartbeat.childCpuMs) &&
    Number.isFinite(suspectCpuMs) &&
    heartbeat.childCpuMs <= suspectCpuMs;
  if (elapsed >= policy.stallConfirmMs && cpuConfirm) {
    return { livenessState: "STALLED", livenessSince: since, suspectCpuMs };
  }
  return { livenessState: "SUSPECTED_STALL", livenessSince: since, suspectCpuMs };
}

function isoNow(now) {
  return new Date(now).toISOString();
}
