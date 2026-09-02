import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, realpath, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  atomicWriteJson,
  createJob,
  createJobId,
  listJobs,
  readJob,
  resolveAllowedCwd,
  updateJob,
} from "../dist/job-store.js";

test("creates and atomically updates one job", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "durable-jobs-test-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const id = createJobId();
  await createJob(root, {
    id,
    state: "QUEUED",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  });
  await updateJob(root, id, (job) => {
    job.state = "RUNNING";
    return job;
  });
  assert.equal((await readJob(root, id)).state, "RUNNING");
  assert.equal((await listJobs(root)).length, 1);
});

test("allowed cwd check fails closed outside configured roots", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "durable-jobs-root-"));
  const outside = await mkdtemp(path.join(os.tmpdir(), "durable-jobs-outside-"));
  t.after(() => Promise.all([rm(root, { recursive: true, force: true }), rm(outside, { recursive: true, force: true })]));
  const inside = path.join(root, "repo");
  await mkdir(inside);
  // resolveAllowedCwd canonicalises via realpath; on macOS os.tmpdir() lives
  // under the /var -> /private/var symlink, so compare against the realpath.
  assert.equal(await resolveAllowedCwd(inside, [root]), await realpath(inside));
  await assert.rejects(resolveAllowedCwd(outside, [root]), /outside allowed roots/);
});

test("atomic writer always leaves valid JSON", async (t) => {
  const root = await mkdtemp(path.join(os.tmpdir(), "durable-jobs-json-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const file = path.join(root, "state.json");
  await atomicWriteJson(file, { ok: true, count: 1 });
  assert.deepEqual(JSON.parse(await (await import("node:fs/promises")).readFile(file, "utf8")), {
    ok: true,
    count: 1,
  });
});
