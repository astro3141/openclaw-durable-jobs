#!/usr/bin/env node
// Read-only install preflight. Exits non-zero if any active legacy job (no
// frozen route / no delivery outbox) is present, zero otherwise.
//
//   node dist/preflight.js --state-dir <lab-state-dir> [--state-subdir durable-jobs]
//   node dist/preflight.js --root-dir  <durable-jobs-dir>
import path from "node:path";
import { detectLegacyActiveJobs } from "./core.js";

function parseArgs(argv) {
  const args = { stateSubdir: "durable-jobs" };
  for (let i = 0; i < argv.length; i += 1) {
    const flag = argv[i];
    if (flag === "--state-dir") args.stateDir = argv[(i += 1)];
    else if (flag === "--state-subdir") args.stateSubdir = argv[(i += 1)];
    else if (flag === "--root-dir") args.rootDir = argv[(i += 1)];
    else if (flag === "-h" || flag === "--help") args.help = true;
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const rootDir =
    args.rootDir ?? (args.stateDir ? path.join(args.stateDir, args.stateSubdir) : null);
  if (args.help || !rootDir) {
    process.stderr.write(
      "usage: node dist/preflight.js --state-dir <lab-state-dir> [--state-subdir durable-jobs]\n" +
        "       node dist/preflight.js --root-dir <durable-jobs-dir>\n",
    );
    process.exit(args.help ? 0 : 2);
  }
  const legacy = await detectLegacyActiveJobs(rootDir);
  if (legacy.length === 0) {
    process.stdout.write(`durable-jobs preflight OK: no active legacy jobs in ${rootDir}\n`);
    process.exit(0);
  }
  process.stderr.write(
    `durable-jobs preflight BLOCKED: ${legacy.length} active legacy job(s) in ${rootDir}\n`,
  );
  for (const job of legacy) {
    process.stderr.write(
      `  ${job.id}\tstate=${job.state}\tcreatedAt=${job.createdAt ?? "unknown"}\n`,
    );
  }
  process.exit(1);
}

main().catch((error) => {
  process.stderr.write(`durable-jobs preflight ERROR: ${error?.stack ?? error}\n`);
  process.exit(2);
});
