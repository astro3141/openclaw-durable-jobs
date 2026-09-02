import assert from "node:assert/strict";
import test from "node:test";
import {
  computeJobOutcome,
  computeProcessState,
  processStateFromLegacyState,
} from "../dist/verdict.js";

test("computeProcessState: exit 0 → COMPLETED", () => {
  assert.equal(computeProcessState({ code: 0, signal: null, timedOut: false }), "COMPLETED");
});

test("computeProcessState: non-zero exit → FAILED_COMMAND", () => {
  assert.equal(computeProcessState({ code: 2, signal: null, timedOut: false }), "FAILED_COMMAND");
});

test("computeProcessState: our timeout → TIMED_OUT (even though signalled)", () => {
  assert.equal(computeProcessState({ code: null, signal: "SIGTERM", timedOut: true }), "TIMED_OUT");
});

test("computeProcessState: signal we did not send → INTERRUPTED", () => {
  assert.equal(computeProcessState({ code: null, signal: "SIGKILL", timedOut: false }), "INTERRUPTED");
});

test("outcome: COMPLETED + OK → COMPLETED_UNVERIFIED (never a success)", () => {
  assert.equal(computeJobOutcome("COMPLETED", "OK"), "COMPLETED_UNVERIFIED");
});

test("outcome: COMPLETED + UNKNOWN → COMPLETED_UNVERIFIED", () => {
  assert.equal(computeJobOutcome("COMPLETED", "UNKNOWN"), "COMPLETED_UNVERIFIED");
});

test("outcome: exit 0 but provider ERROR_UNCLASSIFIED → FAILED_PROVIDER (the false-success fix)", () => {
  assert.equal(computeJobOutcome("COMPLETED", "ERROR_UNCLASSIFIED"), "FAILED_PROVIDER");
});

test("outcome: exit 0 but provider TOOL_INTERRUPTED → FAILED_PROVIDER", () => {
  assert.equal(computeJobOutcome("COMPLETED", "TOOL_INTERRUPTED"), "FAILED_PROVIDER");
});

test("outcome: every provider-failure state maps COMPLETED → FAILED_PROVIDER", () => {
  for (const p of [
    "ERROR_UNCLASSIFIED",
    "BLOCKED_QUOTA",
    "RATE_LIMITED",
    "AUTH_FAILED",
    "CONTEXT_LIMIT",
    "INTERNAL_ERROR",
    "TOOL_INTERRUPTED",
  ]) {
    assert.equal(computeJobOutcome("COMPLETED", p), "FAILED_PROVIDER", p);
  }
});

test("outcome: command-failure process states → FAILED_COMMAND (provider ignored)", () => {
  for (const s of ["FAILED_COMMAND", "INTERRUPTED", "TIMED_OUT", "LOST"]) {
    assert.equal(computeJobOutcome(s, "OK"), "FAILED_COMMAND", s);
  }
});

test("outcome: CANCELLED → CANCELLED", () => {
  assert.equal(computeJobOutcome("CANCELLED", "UNKNOWN"), "CANCELLED");
});

test("outcome: non-terminal process states have no outcome yet", () => {
  assert.equal(computeJobOutcome("QUEUED", "UNKNOWN"), null);
  assert.equal(computeJobOutcome("RUNNING", "OK"), null);
});

test("P0 never emits PASSED/SUCCEEDED", () => {
  const outcomes = [
    computeJobOutcome("COMPLETED", "OK"),
    computeJobOutcome("COMPLETED", "ERROR_UNCLASSIFIED"),
    computeJobOutcome("CANCELLED", "UNKNOWN"),
    computeJobOutcome("FAILED_COMMAND", "OK"),
  ];
  for (const o of outcomes) {
    assert.ok(!["PASSED", "SUCCEEDED", "SUCCESS"].includes(o), `unexpected success outcome: ${o}`);
  }
});

test("legacy state mapping (backward-compat read)", () => {
  assert.equal(processStateFromLegacyState("SUCCEEDED"), "COMPLETED");
  assert.equal(processStateFromLegacyState("FAILED"), "FAILED_COMMAND");
  assert.equal(processStateFromLegacyState("TIMED_OUT"), "TIMED_OUT");
  assert.equal(processStateFromLegacyState("CANCELLED"), "CANCELLED");
  assert.equal(processStateFromLegacyState("LOST"), "LOST");
});
