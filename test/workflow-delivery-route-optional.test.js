// Delivery-route resolution is best-effort enrichment for a trusted
// workflow.start: the H1 core contract (record creation + parent identity
// freeze + ownership) must not depend on gateway chat.history availability.
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { startJob } from "../dist/core.js";
import { readJob } from "../dist/job-store.js";
import { resolveOwnerContext } from "../dist/ownership.js";

async function withStore(run) {
  const store = await mkdtemp(path.join(os.tmpdir(), "durable-route-optional-"));
  try {
    await run(store);
  } finally {
    await rm(store, { recursive: true, force: true });
  }
}

const cfg = {
  queuedGraceMs: 30_000,
  maxConcurrent: 4,
  defaultTimeoutSeconds: 0,
  deliveryMaxAttempts: 8,
  sendLeaseMs: 30_000,
};
const SESSION_KEY = "agent:claude:acp:binding:slack:default:x";
function trustedCtx(work) {
  return {
    sessionKey: SESSION_KEY,
    agentId: "claude",
    sessionId: "s1",
    deliveryContext: { channel: "slack", to: "channel:C1", chatType: "channel" },
    workspaceDir: work,
    durableAllowedRoots: [work],
  };
}
const okHistory = () => ({
  result: {
    sessionInfo: { chatType: "channel", origin: { provider: "slack", to: "channel:C1", chatType: "channel" } },
  },
});
const deps = (gatewayCall) => ({
  rootDir: undefined,
  config: cfg,
  gatewayCall,
  createFlow: () => ({ flowId: "f1" }),
  spawnWorker: () => 4242,
});

test("trusted workflow.start with a resolvable route freezes deliveryRoute and parent identity", async () => {
  await withStore(async (store) => {
    const work = await mkdtemp(path.join(os.tmpdir(), "route-work-"));
    const created = await startJob(
      { ...deps(async () => okHistory()), rootDir: store },
      trustedCtx(work),
      { name: "p3h-h1-parent-freeze", command: ["/usr/bin/true"], cwd: work },
    );
    const job = await readJob(store, created.id);
    assert.notEqual(job.deliveryRoute, null, "route resolved and frozen");
    assert.equal(job.parent.agentId, "claude");
    assert.equal(job.parent.sessionKey, SESSION_KEY);
    await rm(work, { recursive: true, force: true });
  });
});

test("trusted workflow.start still creates the record + freezes parent when chat.history requires gateway credentials", async () => {
  await withStore(async (store) => {
    const work = await mkdtemp(path.join(os.tmpdir(), "route-work-"));
    const gatewayCall = async () => {
      const err = new Error("gateway chat.history requires credentials before opening a websocket");
      err.name = "GatewayCredentialsRequiredError";
      throw err;
    };
    const created = await startJob({ ...deps(gatewayCall), rootDir: store }, trustedCtx(work), {
      name: "p3h-h1-parent-freeze",
      command: ["/usr/bin/true"],
      cwd: work,
    });
    const job = await readJob(store, created.id);
    assert.equal(job.deliveryRoute, null, "delivery route deferred to null, creation still succeeds");
    assert.equal(job.parent.agentId, "claude");
    assert.equal(job.parent.sessionKey, SESSION_KEY);
    await rm(work, { recursive: true, force: true });
  });
});

test("an unexpected (non-deferrable) gatewayCall error still fails workflow.start", async () => {
  await withStore(async (store) => {
    const work = await mkdtemp(path.join(os.tmpdir(), "route-work-"));
    const gatewayCall = async () => {
      throw new Error("boom: unexpected programming error");
    };
    await assert.rejects(
      startJob({ ...deps(gatewayCall), rootDir: store }, trustedCtx(work), {
        name: "p3h-h1-parent-freeze",
        command: ["/usr/bin/true"],
        cwd: work,
      }),
      /boom: unexpected/,
    );
    await rm(work, { recursive: true, force: true });
  });
});

test("ownership regression: a trusted-session context with no workspaceDir and no configured owner throws OWNER_UNRESOLVED", () => {
  assert.throws(
    () => resolveOwnerContext({}, { agentId: "claude", sessionKey: SESSION_KEY }, {}),
    (err) => err.code === "OWNER_UNRESOLVED",
  );
});
