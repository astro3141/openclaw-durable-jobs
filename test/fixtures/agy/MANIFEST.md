# AGY `--output-format json` result fixtures (spike-agy-json-fixtures)

Real captures of the AGY (`/opt/homebrew/bin/agy`) print-mode JSON result envelope, collected for the P0
`evaluator.js` `provider_state` mapping (design §6, §12). **No tokens/credentials/PII** — `usage.*` values
are numeric token *counts* only; `conversation_id` is a per-run session UUID (not a secret).

## Envelope shape (observed, stable across all captures)

`agy --print "<prompt>" --model <id> --add-dir <dir> --output-format json --print-timeout <dur>`
always writes a single JSON object to **stdout** with:

```
conversation_id, status, response, duration_seconds, num_turns, usage{input_tokens,output_tokens,
thinking_tokens,cache_read_tokens,total_tokens}
```

- `status` ∈ { `"SUCCESS"`, `"ERROR"` } — **the authoritative provider signal.**
- On `status:"ERROR"` a top-level **`error`** string is added. There is **no** structured `error_code` /
  `error_type` field — only the free-text `error` message.
- A CLI **arg-parse** failure (e.g. `--print-timeout 800` without a unit) is different: it prints plain
  text to **stderr** and exits 2 **without** a JSON envelope. So: envelope-present ⇒ trust `status`;
  envelope-absent + exit≠0 ⇒ command-layer failure (process, not provider).

## Fixtures

| file | classification | argv (key parts) | model | exit | parse | status | error field |
|---|---|---|---|---|---|---|---|
| `success.json` | **CAPTURED_REAL** | `--print "Reply with exactly the word: OK" … --print-timeout 60s` | gemini-3.6-flash-low | 0 | OK | `SUCCESS` | (absent) |
| `error_invalid_model.json` | **CAPTURED_CLOSE_EQUIVALENT** (validation error, not a provider auth/quota event) | `--print "say OK" --model nonexistent-model-xyz … --print-timeout 30s` | (invalid) | 1 | OK | `ERROR` | `invalid model selection …` |
| `error_timeout.json` | **CAPTURED_CLOSE_EQUIVALENT** (closest reproducible to TOOL_INTERRUPTED) | `--print "<500-word essay>" … --print-timeout 1s` | gemini-3.6-flash-low | 1 | OK | `ERROR` | `timeout waiting for response` |

## Not reproducible safely (left UNKNOWN — not fabricated)

| provider sub-state | status | reason not captured |
|---|---|---|
| BLOCKED_QUOTA | NOT_REPRODUCIBLE | cannot exhaust real quota safely |
| AUTH_FAILED | NOT_REPRODUCIBLE | would require tampering with live `~/.gemini` credentials used by the running gateway |
| CONTEXT_LIMIT | NOT_REPRODUCIBLE | needs a very large prompt; consumes quota and is non-deterministic |

Per the design, `NOT_REPRODUCIBLE` sub-states are **not** mapped; the evaluator returns `UNKNOWN` for
them until a real sample is captured. Their `error`-string signatures must be added to the bounded
known-error table only from real envelopes, never invented.
