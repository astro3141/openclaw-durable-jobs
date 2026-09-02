// Process-state and P0 job-outcome derivation (design §6).
//
// P0 deliberately produces NO semantic PASSED/SUCCEEDED and NO workflow success. The strongest positive
// P0 outcome is COMPLETED_UNVERIFIED ("the process finished and no provider failure was detected; the work
// is NOT asserted correct").

import { PROVIDER_FAILURE_STATES } from "./evaluator.js";

// Authoritative process outcome from the OS child (worker.js).
export const PROCESS_STATES = new Set([
  "QUEUED",
  "RUNNING",
  "COMPLETED",
  "FAILED_COMMAND",
  "INTERRUPTED",
  "TIMED_OUT",
  "CANCELLED",
  "LOST",
]);

// The only outcomes P0 may emit for a workflow-absent durable job.
export const JOB_OUTCOMES = new Set([
  "FAILED_COMMAND",
  "FAILED_PROVIDER",
  "COMPLETED_UNVERIFIED",
  "CANCELLED",
]);

// Process states that map to the FAILED_COMMAND outcome (a command/process-layer failure).
const COMMAND_FAILURE_PROCESS_STATES = new Set(["FAILED_COMMAND", "INTERRUPTED", "TIMED_OUT", "LOST"]);

// Derive process_state from a finished child. `timedOut` is true only when OUR timeout killed it.
export function computeProcessState({ code, signal, timedOut }) {
  if (timedOut) return "TIMED_OUT";
  if (code === 0) return "COMPLETED";
  if (signal) return "INTERRUPTED"; // killed by a signal we did not send (timedOut already excluded)
  return "FAILED_COMMAND"; // non-zero exit
}

// Map a terminal process_state + provider_state to the P0 job_outcome. Non-terminal → null.
export function computeJobOutcome(processState, providerState) {
  if (processState === "CANCELLED") return "CANCELLED";
  if (COMMAND_FAILURE_PROCESS_STATES.has(processState)) return "FAILED_COMMAND";
  if (processState === "COMPLETED") {
    // Provider failure on a process that exited 0 is the false-success case the fix targets.
    if (PROVIDER_FAILURE_STATES.has(providerState)) return "FAILED_PROVIDER";
    // provider_state ∈ {OK, UNKNOWN} → completed but not asserted correct.
    return "COMPLETED_UNVERIFIED";
  }
  return null; // QUEUED / RUNNING have no outcome yet
}

// Backward-compat read: map an old `state` value to a process_state for legacy job rows.
export function processStateFromLegacyState(state) {
  switch (state) {
    case "SUCCEEDED":
      return "COMPLETED";
    case "FAILED":
      return "FAILED_COMMAND";
    case "TIMED_OUT":
      return "TIMED_OUT";
    case "CANCELLED":
      return "CANCELLED";
    case "LOST":
      return "LOST";
    case "QUEUED":
      return "QUEUED";
    case "RUNNING":
      return "RUNNING";
    default:
      return null;
  }
}

// Human-facing outcome label for the terminal notice (new-format jobs only).
export const OUTCOME_ICONS = {
  COMPLETED_UNVERIFIED: "ℹ️",
  FAILED_PROVIDER: "❌",
  FAILED_COMMAND: "❌",
  CANCELLED: "🚫",
};
