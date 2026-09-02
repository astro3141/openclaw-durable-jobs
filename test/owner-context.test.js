import assert from "node:assert/strict";
import test from "node:test";

import { assertJobOwner, resolveOwnerContext } from "../dist/ownership.js";

const ROUTE = { routeKind: "channel_root", channel: "slack", to: "channel:C1", accountId: "default" };

const configured = {
  ownerAgentId: "scanner",
  ownerSessionKey: "agent:scanner:acp:binding:test",
  workspaceDir: "/work/scanner",
  deliveryRoute: ROUTE,
};

test("context-free MCP calls resolve the owner but stay sessionKey-less", () => {
  assert.deepEqual(resolveOwnerContext(configured, {}), {
    agentId: "scanner",
    sessionKey: null,
    workspaceDir: "/work/scanner",
    durableAllowedRoots: [],
    ownerDeliveryRoute: ROUTE,
    contextFree: true,
  });
});

test("rejects an agent or session that differs from the configured owner", () => {
  assert.throws(() => resolveOwnerContext(configured, { agentId: "other" }), /not authorized for this agent/);
  assert.throws(
    () => resolveOwnerContext(configured, { sessionKey: "agent:other:main" }),
    /not authorized for this session/,
  );
});

test("job access is scoped to agent + exact (possibly null) session", () => {
  const owner = resolveOwnerContext(configured, {});
  const job = { agentId: owner.agentId, sessionKey: owner.sessionKey };
  assert.equal(assertJobOwner(job, owner), job);
  assert.throws(
    () => assertJobOwner({ ...job, sessionKey: "agent:scanner:other" }, owner),
    /owned by a different session/,
  );
});

test("selects the configured context-free owner from cwd (sessionKey null)", () => {
  const config = {
    owners: [
      { agentId: "test-agent", workspaceDir: "/work/test", allowedRoots: ["/work/test"], deliveryRoute: ROUTE },
      { agentId: "infra-agent", workspaceDir: "/repos/infra", allowedRoots: ["/repos/infra"], deliveryRoute: ROUTE },
    ],
  };
  assert.deepEqual(resolveOwnerContext(config, {}, { cwd: "/repos/infra/tests" }), {
    agentId: "infra-agent",
    sessionKey: null,
    workspaceDir: "/repos/infra",
    durableAllowedRoots: ["/repos/infra"],
    ownerDeliveryRoute: ROUTE,
    contextFree: true,
  });
  // No cwd + multiple owners is ambiguous.
  assert.throws(() => resolveOwnerContext(config, {}), (e) => e.code === "OWNER_AMBIGUOUS");
});

test("rejects cwd outside every configured owner (OWNER_UNRESOLVED)", () => {
  assert.throws(
    () =>
      resolveOwnerContext(
        {
          owners: [
            { agentId: "a", workspaceDir: "/work/a", deliveryRoute: ROUTE },
            { agentId: "b", workspaceDir: "/work/b", deliveryRoute: ROUTE },
          ],
        },
        {},
        { cwd: "/tmp/outside" },
      ),
    (e) => e.code === "OWNER_UNRESOLVED",
  );
});
