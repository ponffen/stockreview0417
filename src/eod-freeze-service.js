const { listAllUserIds, setSnapshotWatermark, backfillTradeAmountShareRatiosForUser } = require("./db");
const { toDateKey } = require("../scripts/lib/market-fetch");
const { shouldSkipScheduledFreezeCron } = require("./metrics/freeze-calendar");

function addCalendarDays(dateKey, days) {
  const d = new Date(`${String(dateKey || "").slice(0, 10)}T12:00:00+08:00`);
  if (Number.isNaN(d.getTime())) {
    return toDateKey(new Date());
  }
  d.setDate(d.getDate() + Number(days || 0));
  return toDateKey(d);
}

function getShanghaiWallClockParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);
  const get = (type) => parts.find((p) => p.type === type)?.value;
  return {
    y: Number(get("year")),
    m: Number(get("month")),
    d: Number(get("day")),
    h: Number(get("hour")),
    min: Number(get("minute")),
  };
}

function getTradingDateKey(baseDate = new Date()) {
  const { y, m, d, h, min } = getShanghaiWallClockParts(baseDate);
  const current = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  if (h < 8 || (h === 8 && min < 30)) {
    return addCalendarDays(current, -1);
  }
  return current;
}

/** 冻结快照重建后，回填该用户成交的 amount_share_ratio（仅写该字段，best-effort，不影响冻结结果）。 */
async function backfillAmountShareRatioAfterFreeze(uid, logger) {
  try {
    await backfillTradeAmountShareRatiosForUser(uid, { logger });
  } catch (error) {
    logger?.warn?.(`[amount-share-backfill] user=${uid} failed: ${error?.message || error}`);
  }
}

function resolveFrozenDate(input) {
  if (input) {
    return toDateKey(input);
  }
  const tradingDate = getTradingDateKey(new Date());
  return addCalendarDays(tradingDate, -1);
}

async function freezeUserToDate(userId, frozenDate, options = {}) {
  const logger = options.logger || console;
  const uid = String(userId || "").trim();
  if (!uid) {
    return { userId: "", skipped: true, reason: "missing-user" };
  }
  const fd = frozenDate ? resolveFrozenDate(frozenDate) : resolveFrozenDate();
  const rebuildFromDate = options.rebuildFromDate
    ? String(options.rebuildFromDate).slice(0, 10)
    : null;
  const fullRebuild = options.fullRebuild === true;
  const useFreezeV3 = fullRebuild || !!rebuildFromDate;

  if (useFreezeV3) {
    const {
      getUserMetricsMeta,
      upsertUserMetricsMeta,
      getLatestAnalysisSnapshotDate,
    } = require("./db");
    const { capFrozenThroughToSnapshot } = require("./metrics/freeze-calendar");
    try {
      const { runFreezeV3ForUser } = require("./metrics/freeze-v3");
      await runFreezeV3ForUser(uid, {
        frozenDate: fd,
        force: true,
        syncDailyClose: options.syncDailyClose === true,
        rebuildFromDate: rebuildFromDate || undefined,
        fullRebuild,
        logger,
      });
      const meta = await getUserMetricsMeta(uid, { light: true });
      const latestSnap = await getLatestAnalysisSnapshotDate(uid, "all");
      const frozenThrough =
        capFrozenThroughToSnapshot(fd, latestSnap) || latestSnap || meta.frozenThrough || fd;
      await upsertUserMetricsMeta(uid, {
        rebuilding: false,
        rebuildFrom: null,
        dataVersion: (meta.dataVersion || 0) + 1,
        frozenThrough,
      });
      await backfillAmountShareRatioAfterFreeze(uid, logger);
      return {
        userId: uid,
        skipped: false,
        frozenDate: frozenThrough,
        mode: "freeze-v3",
      };
    } catch (error) {
      await upsertUserMetricsMeta(uid, { rebuilding: false, rebuildFrom: null }).catch(() => {});
      return { userId: uid, skipped: true, reason: error?.message || "freeze-v3-failed" };
    }
  }

  const { runFreezeIncrementalForUser } = require("./metrics/freeze-incremental");
  const result = await runFreezeIncrementalForUser(uid, {
    frozenDate: fd,
    force: options.force === true,
    syncDailyClose: options.syncDailyClose === true,
    fullRebuild: options.fullRebuild === true,
    logger,
  });
  if (!result.ok) {
    return { userId: uid, skipped: true, reason: result.reason || "freeze-failed" };
  }
  if (result.skipped) {
    return { userId: uid, skipped: true, reason: result.reason, frozenDate: result.frozenDate };
  }
  await backfillAmountShareRatioAfterFreeze(uid, logger);
  return {
    userId: uid,
    skipped: false,
    frozenDate: result.frozenDate,
    analysisRowsWritten: result.timing?.accountRows || 0,
    symbolRowsWritten: result.timing?.symbolRows || 0,
    timing: result.timing,
  };
}

async function runDailyFreeze(options = {}) {
  const logger = options.logger || console;
  const frozenDate = resolveFrozenDate(options.frozenDate);
  const force = options.force === true;
  if (options.fromCron && !force && shouldSkipScheduledFreezeCron()) {
    logger.info?.("[freeze-eod] skipped: Sun/Mon morning (Asia/Shanghai)");
    return {
      ok: true,
      skipped: true,
      reason: "cron-skipped-sun-mon-morning",
      frozenDate,
      startedAt: Date.now(),
      finishedAt: Date.now(),
      elapsedMs: 0,
      users: [],
    };
  }
  const syncDailyClose = options.syncDailyClose === true;
  const userIdsInput = Array.isArray(options.userIds) ? options.userIds : [];
  const rebuildFromDate = options.rebuildFromDate
    ? String(options.rebuildFromDate).slice(0, 10)
    : null;
  const fullRebuild = options.fullRebuild === true;
  const userIds = userIdsInput.length
    ? [...new Set(userIdsInput.map((u) => String(u || "").trim()).filter(Boolean))]
    : await listAllUserIds();
  const startedAt = Date.now();
  const results = [];
  try {
    for (const uid of userIds) {
      const one = await freezeUserToDate(uid, frozenDate, {
        logger,
        force,
        syncDailyClose,
        rebuildFromDate,
        fullRebuild,
      });
      results.push(one);
    }
    const successCount = results.filter((r) => !r.skipped).length;
    await setSnapshotWatermark({
      frozenDate,
      status: "success",
      message: `users=${userIds.length}, updated=${successCount}`,
    });
    return {
      ok: true,
      frozenDate,
      startedAt,
      finishedAt: Date.now(),
      elapsedMs: Date.now() - startedAt,
      users: results,
    };
  } catch (error) {
    await setSnapshotWatermark({
      frozenDate,
      status: "failed",
      message: String(error?.message || error || "freeze failed"),
    });
    throw error;
  }
}

module.exports = {
  runDailyFreeze,
  resolveFrozenDate,
  freezeUserToDate,
};
