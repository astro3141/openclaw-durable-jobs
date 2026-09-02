import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { classifyProviderState, parseEnvelope } from "../dist/evaluator.js";

const fixture = (name) =>
  readFileSync(new URL(`./fixtures/agy/${name}.json`, import.meta.url), "utf8");

test("real success fixture → OK", () => {
  const { providerState, envelope } = classifyProviderState(fixture("success"));
  assert.equal(providerState, "OK");
  assert.equal(envelope.status, "SUCCESS");
});

test("real timeout fixture → TOOL_INTERRUPTED (only REAL-captured signature)", () => {
  const { providerState, signature } = classifyProviderState(fixture("error_timeout"));
  assert.equal(providerState, "TOOL_INTERRUPTED");
  assert.equal(signature, "TOOL_INTERRUPTED");
});

test("real invalid-model fixture → ERROR_UNCLASSIFIED (never mis-mapped to AUTH_FAILED)", () => {
  const { providerState } = classifyProviderState(fixture("error_invalid_model"));
  assert.equal(providerState, "ERROR_UNCLASSIFIED");
});

test("no envelope → UNKNOWN", () => {
  assert.equal(classifyProviderState("plain test runner output\nok 1 - foo\n").providerState, "UNKNOWN");
  assert.equal(classifyProviderState("").providerState, "UNKNOWN");
  assert.equal(classifyProviderState(undefined).providerState, "UNKNOWN");
});

test("malformed JSON envelope → UNKNOWN (parse failure)", () => {
  assert.equal(classifyProviderState('{"status":"SUCCESS"').providerState, "UNKNOWN");
  assert.equal(classifyProviderState("{not json}").providerState, "UNKNOWN");
});

test("status ERROR without a known signature → ERROR_UNCLASSIFIED", () => {
  const out = classifyProviderState('{"status":"ERROR","error":"some brand new failure mode"}');
  assert.equal(out.providerState, "ERROR_UNCLASSIFIED");
});

test("full-stdout keyword scan is NOT used: quota/error words in the RESPONSE do not change a SUCCESS", () => {
  const env = JSON.stringify({
    status: "SUCCESS",
    response: "I hit a quota limit earlier but recovered; error: none. auth failed? no.",
  });
  assert.equal(classifyProviderState(env).providerState, "OK");
});

test("envelope may be the last line among leading noise", () => {
  const stdout = `some warning line\nanother\n${fixture("success").trim()}`;
  assert.equal(classifyProviderState(stdout).providerState, "OK");
});

test("unrecognized status value → UNKNOWN (cannot assert success or failure)", () => {
  assert.equal(classifyProviderState('{"status":"WEIRD"}').providerState, "UNKNOWN");
});

test("parseEnvelope returns null when no top-level status string", () => {
  assert.equal(parseEnvelope('{"foo":1}'), null);
  assert.equal(parseEnvelope("[1,2,3]"), null);
});
