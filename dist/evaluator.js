// Provider-state evaluator for durable-job model activities (P0).
//
// Authoritative source: the SINGLE AGY `--output-format json` result ENVELOPE written to stdout.
// This is grounded in the captured fixtures (test/fixtures/agy/*.json + MANIFEST.md): the envelope has a
// top-level `status` of "SUCCESS" | "ERROR" and, on error, a free-text top-level `error` string — there is
// NO structured error code/type field.
//
// Hard rules (design §6, §12):
//   - NEVER scan the full stdout for keywords. Only JSON.parse a candidate envelope and read its
//     top-level `status` and (size-bounded) `error`.
//   - Unproven quota/auth/context signatures are NOT implemented; a status:"ERROR" that is not matched by a
//     REAL captured signature is ERROR_UNCLASSIFIED, never a guessed subtype.

export const PROVIDER_STATES = new Set([
  "UNKNOWN",
  "OK",
  "ERROR_UNCLASSIFIED",
  "BLOCKED_QUOTA",
  "RATE_LIMITED",
  "AUTH_FAILED",
  "CONTEXT_LIMIT",
  "INTERNAL_ERROR",
  "TOOL_INTERRUPTED",
]);

// Provider states that mean the model activity did not do its work (→ FAILED_PROVIDER at the outcome layer).
export const PROVIDER_FAILURE_STATES = new Set([
  "ERROR_UNCLASSIFIED",
  "BLOCKED_QUOTA",
  "RATE_LIMITED",
  "AUTH_FAILED",
  "CONTEXT_LIMIT",
  "INTERNAL_ERROR",
  "TOOL_INTERRUPTED",
]);

// Upper bound on the error string we are willing to inspect (bounded, per design).
const ERROR_FIELD_MAX = 2000;

// ONLY signatures backed by a REAL capture may appear here. Per spike-agy-json-fixtures, the reproducible
// error was the print-mode wait timeout; quota/auth/context were NOT_REPRODUCIBLE and are intentionally
// absent (they classify as ERROR_UNCLASSIFIED until a real envelope is captured).
export const KNOWN_ERROR_SIGNATURES = [
  { state: "TOOL_INTERRUPTED", test: (error) => /timeout waiting for response/i.test(error) },
];

// Classify a durable activity by its command so the JSON-envelope evaluator is applied ONLY to model
// activities that speak the AGY result protocol. The only model runner in use is AGY
// (`command[0]` basename `agy`); everything else is a local activity with no provider protocol, so its
// plain stdout (which may legitimately contain `{"status":"ERROR"}`) is NEVER run through the evaluator.
// This is the simplest non-breaking gate (no tool-schema change, additive job fields only).
export function classifyActivity(command) {
  const exe = Array.isArray(command) && command.length > 0 ? String(command[0]) : "";
  const base = exe.split("/").pop();
  if (base === "agy") return { activityType: "model", resultProtocol: "agy-json" };
  return { activityType: "local", resultProtocol: "none" };
}

// P2-B runner profiles. Each profile's runnerType and resultProtocol are CANONICAL: the request never
// supplies resultProtocol/providerState directly — they are derived here from the (validated) profile.
export const RUNNER_PROFILES = {
  model_agy: { runnerType: "model", resultProtocol: "agy-json" },
  local_test: { runnerType: "local", resultProtocol: "none" },
  local_build: { runnerType: "local", resultProtocol: "none" },
  local_docker: { runnerType: "local", resultProtocol: "none" },
  generic_local: { runnerType: "local", resultProtocol: "none" },
};

// Executables that are known model runners and therefore MUST NOT be downgraded to a local activity
// (which would bypass the provider evaluator and let a real provider failure read as success).
const KNOWN_MODEL_EXECUTABLES = new Set(["agy"]);

function runnerMetadataError(message) {
  const error = new Error(`RUNNER_METADATA_INVALID: ${message}`);
  error.code = "RUNNER_METADATA_INVALID";
  return error;
}

// Resolve the EFFECTIVE runner metadata from the command + optional explicit runnerType/runnerProfile.
// Explicit metadata is validated (never blindly trusted); an incompatible combination throws before any
// job/side effect. activityType/resultProtocol are derived ONLY from the effective (validated) profile —
// the caller can never set them, and a known model executable (`agy`) can never be run as local.
export function resolveRunnerMetadata({ command, runnerType, runnerProfile } = {}) {
  const exe = Array.isArray(command) && command.length > 0 ? String(command[0]) : "";
  const base = exe.split("/").pop();
  const isModelExecutable = KNOWN_MODEL_EXECUTABLES.has(base);

  // (2) No explicit metadata → legacy inference from the command.
  if (!runnerType && !runnerProfile) {
    const inferred = classifyActivity(command);
    const profile = inferred.activityType === "model" ? "model_agy" : "generic_local";
    return {
      runnerType: inferred.activityType,
      runnerProfile: profile,
      activityType: inferred.activityType,
      resultProtocol: inferred.resultProtocol,
    };
  }

  // (3) Validate explicit values.
  if (runnerType && runnerType !== "model" && runnerType !== "local") {
    throw runnerMetadataError(`invalid runnerType "${runnerType}" (expected "model" | "local")`);
  }
  if (runnerProfile && !(runnerProfile in RUNNER_PROFILES)) {
    throw runnerMetadataError(`unknown runnerProfile "${runnerProfile}"`);
  }

  // Effective profile: explicit profile, else a default profile for the given runnerType.
  const profile = runnerProfile ?? (runnerType === "model" ? "model_agy" : "generic_local");
  const canonical = RUNNER_PROFILES[profile];

  // runnerType (if supplied) must agree with the profile's canonical type.
  if (runnerType && runnerType !== canonical.runnerType) {
    throw runnerMetadataError(
      `runnerType "${runnerType}" conflicts with runnerProfile "${profile}" (canonical type "${canonical.runnerType}")`,
    );
  }
  const effectiveType = canonical.runnerType;

  // (5,6) Command/profile compatibility.
  if (effectiveType === "model") {
    // The only model profile is model_agy, which requires the agy executable.
    if (profile !== "model_agy" || !isModelExecutable) {
      throw runnerMetadataError(
        `model runner (profile "${profile}") requires the "agy" executable, got "${base || "<none>"}"`,
      );
    }
  } else if (isModelExecutable) {
    // A known model executable must never be classified as local (evaluator-bypass guard).
    throw runnerMetadataError(`"${base}" is a known model executable and cannot run as a local activity`);
  }

  // (7) Derive activityType/resultProtocol ONLY from the effective profile.
  return {
    runnerType: effectiveType,
    runnerProfile: profile,
    activityType: effectiveType,
    resultProtocol: canonical.resultProtocol,
  };
}

// Extract the single JSON result envelope from captured stdout text, or null.
// Tries the whole trimmed stdout first, then the last non-empty line. Only objects with a string `status`
// count as an envelope. No keyword scanning; only JSON.parse of at most two candidates.
export function parseEnvelope(stdout) {
  if (typeof stdout !== "string") return null;
  const trimmed = stdout.trim();
  if (!trimmed || trimmed[0] === undefined) return null;
  const candidates = [];
  if (trimmed.startsWith("{")) candidates.push(trimmed);
  const nl = trimmed.lastIndexOf("\n");
  if (nl >= 0) {
    const lastLine = trimmed.slice(nl + 1).trim();
    if (lastLine.startsWith("{") && lastLine !== trimmed) candidates.push(lastLine);
  }
  for (const candidate of candidates) {
    try {
      const obj = JSON.parse(candidate);
      if (obj && typeof obj === "object" && !Array.isArray(obj) && typeof obj.status === "string") {
        return obj;
      }
    } catch {
      // Not this candidate; try the next.
    }
  }
  return null;
}

// Classify provider_state from captured stdout. Returns { providerState, envelope, signature }.
export function classifyProviderState(stdout) {
  const envelope = parseEnvelope(stdout);
  if (!envelope) return { providerState: "UNKNOWN", envelope: null, signature: null };
  if (envelope.status === "SUCCESS") return { providerState: "OK", envelope, signature: null };
  if (envelope.status === "ERROR") {
    const error = typeof envelope.error === "string" ? envelope.error.slice(0, ERROR_FIELD_MAX) : "";
    for (const signature of KNOWN_ERROR_SIGNATURES) {
      if (signature.test(error)) {
        return { providerState: signature.state, envelope, signature: signature.state };
      }
    }
    return { providerState: "ERROR_UNCLASSIFIED", envelope, signature: null };
  }
  // Envelope present but status is neither SUCCESS nor ERROR: we cannot assert success or failure.
  return { providerState: "UNKNOWN", envelope, signature: null };
}
