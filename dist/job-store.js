import { constants as fsConstants } from "node:fs";
import {
  access,
  appendFile,
  mkdir,
  open,
  readFile,
  readdir,
  realpath,
  rename,
  rm,
  stat,
} from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";

export const TERMINAL_STATES = new Set([
  "SUCCEEDED",
  "FAILED",
  "TIMED_OUT",
  "CANCELLED",
  "LOST",
]);

const JOB_ID_PATTERN = /^[a-z0-9][a-z0-9-]{7,80}$/;
const LOCK_STALE_MS = 30_000;

export function nowIso() {
  return new Date().toISOString();
}

export function createJobId() {
  return `job-${randomUUID()}`;
}

export function assertJobId(jobId) {
  if (!JOB_ID_PATTERN.test(jobId)) {
    throw new Error(`invalid job id: ${jobId}`);
  }
}

export function jobDir(rootDir, jobId) {
  assertJobId(jobId);
  return path.join(rootDir, jobId);
}

export function jobFile(rootDir, jobId) {
  return path.join(jobDir(rootDir, jobId), "job.json");
}

export async function ensureStore(rootDir) {
  await mkdir(rootDir, { recursive: true, mode: 0o700 });
}

export async function atomicWriteJson(file, value) {
  const parent = path.dirname(file);
  await mkdir(parent, { recursive: true, mode: 0o700 });
  const temporary = path.join(parent, `.job.${process.pid}.${randomUUID()}.tmp`);
  const handle = await open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(value, null, 2)}\n`, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporary, file);
}

async function acquireLock(directory, timeoutMs = 10_000) {
  const lockPath = path.join(directory, ".lock");
  const deadline = Date.now() + timeoutMs;
  while (true) {
    try {
      await mkdir(lockPath, { mode: 0o700 });
      return async () => rm(lockPath, { recursive: true, force: true });
    } catch (error) {
      if (error?.code !== "EEXIST") throw error;
      try {
        const info = await stat(lockPath);
        if (Date.now() - info.mtimeMs > LOCK_STALE_MS) {
          await rm(lockPath, { recursive: true, force: true });
          continue;
        }
      } catch (statError) {
        if (statError?.code !== "ENOENT") throw statError;
        continue;
      }
      if (Date.now() >= deadline) {
        throw new Error(`timed out acquiring job lock: ${directory}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 40));
    }
  }
}

export async function readJob(rootDir, jobId) {
  const contents = await readFile(jobFile(rootDir, jobId), "utf8");
  return JSON.parse(contents);
}

export async function updateJob(rootDir, jobId, updater) {
  const directory = jobDir(rootDir, jobId);
  const release = await acquireLock(directory);
  try {
    const current = await readJob(rootDir, jobId);
    const next = await updater(structuredClone(current));
    if (!next) return current;
    next.updatedAt = nowIso();
    await atomicWriteJson(jobFile(rootDir, jobId), next);
    return next;
  } finally {
    await release();
  }
}

export async function createJob(rootDir, job) {
  const directory = jobDir(rootDir, job.id);
  await mkdir(directory, { recursive: false, mode: 0o700 });
  await atomicWriteJson(path.join(directory, "job.json"), job);
  return job;
}

export async function listJobs(rootDir) {
  await ensureStore(rootDir);
  const entries = await readdir(rootDir, { withFileTypes: true });
  const jobs = [];
  for (const entry of entries) {
    if (!entry.isDirectory() || !JOB_ID_PATTERN.test(entry.name)) continue;
    try {
      jobs.push(await readJob(rootDir, entry.name));
    } catch {
      // A partially created or externally damaged row is ignored here and
      // surfaced in the service log during reconciliation.
    }
  }
  return jobs.sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
}

export async function resolveAllowedCwd(cwd, allowedRoots) {
  if (!cwd || typeof cwd !== "string") throw new Error("cwd is required");
  const resolvedCwd = await realpath(cwd);
  const info = await stat(resolvedCwd);
  if (!info.isDirectory()) throw new Error(`cwd is not a directory: ${cwd}`);

  const resolvedRoots = [];
  for (const root of allowedRoots) {
    if (!root) continue;
    try {
      resolvedRoots.push(await realpath(root));
    } catch {
      // Missing configured roots never broaden access.
    }
  }
  if (resolvedRoots.length === 0) {
    throw new Error("no allowed working roots are configured for durable jobs");
  }
  const allowed = resolvedRoots.some(
    (root) => resolvedCwd === root || resolvedCwd.startsWith(`${root}${path.sep}`),
  );
  if (!allowed) {
    throw new Error(`cwd is outside allowed roots: ${resolvedCwd}`);
  }
  return resolvedCwd;
}

export async function assertExecutable(command, cwd) {
  if (!Array.isArray(command) || command.length === 0) {
    throw new Error("command must contain at least one argv item");
  }
  if (!command.every((value) => typeof value === "string" && value.length > 0)) {
    throw new Error("every command argv item must be a non-empty string");
  }
  if (command[0].includes(path.sep)) {
    const candidate = path.resolve(cwd, command[0]);
    await access(candidate, fsConstants.X_OK);
  }
}

export async function appendWorkerLog(directory, line) {
  await appendFile(path.join(directory, "worker.log"), `${nowIso()} ${line}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
}

export async function tailFile(file, maxBytes = 5000) {
  try {
    const contents = await readFile(file, "utf8");
    if (contents.length <= maxBytes) return contents;
    return contents.slice(contents.length - maxBytes);
  } catch (error) {
    if (error?.code === "ENOENT") return "";
    throw error;
  }
}

export function isProcessAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code === "EPERM";
  }
}

export function signalProcessGroup(pid, signal) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(-pid, signal);
    return true;
  } catch (error) {
    if (error?.code === "ESRCH") return false;
    throw error;
  }
}
