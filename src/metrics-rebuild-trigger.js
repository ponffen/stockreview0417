/**
 * 写路径：成交响应前同步打标 rebuilding，freeze 在 waitUntil 内同实例执行。
 */
const { upsertUserMetricsMeta, getUserMetricsMeta } = require("./db");
const {
  partitionHintDates,
  resolveFrozenThroughForUser,
  refreshLiveMetricsOnly,
  earliestFromHints,
} = require("./metrics-rebuild-service");

async function runFreezeJobDirect(payload) {
  const { runDailyFreeze } = require("./eod-freeze-service");
  return runDailyFreeze({
    force: payload.force !== false,
    syncDailyClose: !!payload.syncDailyClose,
    userIds: payload.userIds || [],
    rebuildFromDate: payload.rebuildFromDate || null,
    fullRebuild: !!payload.fullRebuild,
    logger: console,
  });
}

function normalizeDispatchBody(payload) {
  return {
    userIds: payload.userIds || [],
    force: payload.force !== false,
    syncDailyClose: !!payload.syncDailyClose,
    rebuildFromDate: payload.rebuildFromDate || undefined,
    fullRebuild: !!payload.fullRebuild,
  };
}

async function runAndVerifyFreeze(body) {
  const userIdsLabel = body.userIds.join(",");
  const t0 = Date.now();
  const data = await runFreezeJobDirect(body);
  const failed = (data?.users || []).filter(
    (row) => row?.skipped && row?.reason !== "already-up-to-date",
  );
  if (failed.length) {
    const reasons = failed.map((row) => `${row.userId || "?"}:${row.reason || "skipped"}`).join(",");
    throw new Error(`freeze failed: ${reasons}`);
  }
  for (const uid of body.userIds) {
    const meta = await getUserMetricsMeta(uid, { light: true });
    if (meta.rebuilding) {
      console.warn("[metrics-rebuild-trigger] rebuilding still true after freeze, force clear", uid);
      await upsertUserMetricsMeta(uid, {
        rebuilding: false,
        rebuildFrom: null,
        dataVersion: (meta.dataVersion || 0) + 1,
      });
    }
  }
  console.log(
    "[metrics-rebuild-trigger] freeze ok userIds=%s rebuildFromDate=%s wallMs=%s serviceMs=%s",
    userIdsLabel,
    body.rebuildFromDate || "-",
    Date.now() - t0,
    data?.elapsedMs ?? "-",
  );
  return data;
}

async function dispatchFreezeEodJobAsync(payload) {
  const body = normalizeDispatchBody(payload);
  if (!body.userIds.length) {
    return;
  }
  try {
    await runAndVerifyFreeze(body);
  } catch (e) {
    console.warn(
      "[metrics-rebuild-trigger] freeze failed userIds=%s rebuildFromDate=%s %s",
      body.userIds.join(","),
      body.rebuildFromDate || "-",
      e?.message || e,
    );
    console.warn(
      "[metrics-rebuild-trigger] freeze gave up userIds=%s; rebuilding stays true",
      body.userIds.join(","),
    );
  }
}

/**
 * 同步阶段：判断是否需要 freeze，需要则立即写 rebuilding=true。
 * @returns {Promise<{ payload?: object, skip?: boolean, reason?: string }>}
 */
async function prepareLedgerMetricsFreeze(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { skip: true, reason: "missing-user" };
  }
  const fullRebuild = !!opts.fullRebuild;
  const hintDates = opts.hintDates || [];

  if (!fullRebuild) {
    if (!hintDates.length) {
      return { skip: true, reason: "no-hint-dates" };
    }
    const frozenThrough = await resolveFrozenThroughForUser(uid);
    const { freezeHints, liveOnlyHints } = partitionHintDates(hintDates, frozenThrough);
    if (!freezeHints.length) {
      if (liveOnlyHints.length) {
        await refreshLiveMetricsOnly(uid);
      }
      return { skip: true, reason: "live-only-after-frozen-through" };
    }
    const rebuildFromDate = earliestFromHints(freezeHints, false);
    await upsertUserMetricsMeta(uid, {
      rebuilding: true,
      rebuildFrom: rebuildFromDate || null,
    });
    return {
      payload: {
        userIds: [uid],
        force: true,
        rebuildFromDate: rebuildFromDate || undefined,
        fullRebuild: false,
      },
      rebuildFromDate,
    };
  }

  await upsertUserMetricsMeta(uid, {
    rebuilding: true,
    rebuildFrom: null,
  });
  return {
    payload: {
      userIds: [uid],
      force: true,
      fullRebuild: true,
    },
    fullRebuild: true,
  };
}

/** debounce flush 仍走此入口（内部 await freeze）。 */
async function triggerLedgerMetricsFreeze(userId, opts = {}) {
  const prepared = await prepareLedgerMetricsFreeze(userId, opts);
  if (!prepared.payload) {
    return { ok: true, skip: true, reason: prepared.reason || "no-op" };
  }
  await dispatchFreezeEodJobAsync(prepared.payload);
  return {
    ok: true,
    dispatched: true,
    rebuildFromDate: prepared.rebuildFromDate,
    fullRebuild: prepared.fullRebuild,
  };
}

module.exports = {
  prepareLedgerMetricsFreeze,
  dispatchFreezeEodJobAsync,
  triggerLedgerMetricsFreeze,
};
