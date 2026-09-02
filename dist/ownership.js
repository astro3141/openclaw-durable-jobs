import path from "node:path";

function legacyOwner(config) {
  if (!config.ownerAgentId && !config.ownerSessionKey && !config.workspaceDir && !config.deliveryRoute) {
    return undefined;
  }
  return {
    agentId: config.ownerAgentId,
    sessionKey: config.ownerSessionKey,
    workspaceDir: config.workspaceDir,
    allowedRoots: config.allowedRoots ?? [],
    deliveryRoute: config.deliveryRoute,
  };
}

function configuredOwners(config) {
  if (Array.isArray(config.owners) && config.owners.length > 0) return config.owners;
  const owner = legacyOwner(config);
  return owner ? [owner] : [];
}

function ownerKey(owner) {
  return `${owner.agentId ?? ""}::${path.resolve(owner.workspaceDir ?? "")}`;
}

function isWithin(candidate, root) {
  if (!candidate || !root) return false;
  const resolvedCandidate = path.resolve(candidate);
  const resolvedRoot = path.resolve(root);
  return resolvedCandidate === resolvedRoot || resolvedCandidate.startsWith(`${resolvedRoot}${path.sep}`);
}

function ambiguousOwnerError() {
  const error = new Error("OWNER_AMBIGUOUS: cwd matches more than one configured owner");
  error.code = "OWNER_AMBIGUOUS";
  return error;
}

function selectOwner(config, ctx, cwd) {
  const owners = configuredOwners(config);
  if (owners.length === 0) return undefined;
  // Trusted context: match the exact owner by the caller-provided identity.
  const exact = owners.filter(
    (owner) =>
      (!ctx.agentId || ctx.agentId === owner.agentId) &&
      (!ctx.sessionKey || ctx.sessionKey === owner.sessionKey),
  );
  if ((ctx.agentId || ctx.sessionKey) && exact.length === 1) return exact[0];
  // Context-free: select the single owner whose workspace/allowedRoots contains
  // cwd, preferring the most specific (longest) root. Ties are ambiguous.
  const ownerCwd = cwd ?? ctx.workspaceDir;
  if (ownerCwd) {
    const matches = owners
      .flatMap((owner) =>
        [owner.workspaceDir, ...(owner.allowedRoots ?? [])]
          .filter((root) => isWithin(ownerCwd, root))
          .map((root) => ({ owner, rootLength: path.resolve(root).length })),
      )
      .sort((a, b) => b.rootLength - a.rootLength);
    if (matches.length > 0) {
      const best = matches[0];
      const tied = new Set(
        matches.filter((match) => match.rootLength === best.rootLength).map((match) => ownerKey(match.owner)),
      );
      if (tied.size === 1) return best.owner;
      throw ambiguousOwnerError();
    }
    // cwd given but inside no owner workspace: do not fall back to a lone owner.
    return undefined;
  }
  if (owners.length === 1) return owners[0];
  throw ambiguousOwnerError();
}

export function resolveOwnerContext(config, ctx, hints = {}) {
  const owner = selectOwner(config, ctx, hints.cwd);
  const trusted = Boolean(ctx.sessionKey);
  const effective = {
    ...ctx,
    agentId: ctx.agentId ?? owner?.agentId,
    // A context-free call never adopts a static owner sessionKey (it would rot
    // on rotation). It stays sessionKey-less and freezes the owner's fixed
    // deliveryRoute instead.
    sessionKey: ctx.sessionKey ?? null,
    workspaceDir: ctx.workspaceDir ?? owner?.workspaceDir,
    durableAllowedRoots: owner?.allowedRoots ?? config.allowedRoots ?? [],
    ownerDeliveryRoute: owner?.deliveryRoute ?? null,
    contextFree: !trusted,
  };
  if (!effective.agentId || !effective.workspaceDir) {
    const error = new Error(
      "durable_job requires a trusted tool context (agentId + workspaceDir) or a configured owner selected by cwd",
    );
    error.code = "OWNER_UNRESOLVED";
    throw error;
  }
  if (owner?.agentId && effective.agentId !== owner.agentId) {
    throw new Error("durable_job is not authorized for this agent");
  }
  if (trusted && owner?.sessionKey && ctx.sessionKey !== owner.sessionKey) {
    throw new Error("durable_job is not authorized for this session");
  }
  return effective;
}

export function assertJobOwner(job, ctx) {
  if ((job.sessionKey ?? null) !== (ctx.sessionKey ?? null) || job.agentId !== ctx.agentId) {
    throw new Error("durable job is owned by a different session");
  }
  return job;
}
