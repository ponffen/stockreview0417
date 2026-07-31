const { listAllUserIds, setSnapshotWatermark } = require("./db");
const { toDateKey } = require("../scripts/lib/market-fetch");
const { shouldSkipScheduledFreezeCron } = require("./metrics/freeze-calendar");
const { listLagUserIds, alignFrozenThroughForScope } = require("./metrics/freeze-lag");

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
  const { y, m, d, h } = getShanghaiWallClockParts(baseDate);
  const current = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  if (h < 8) {
    return addCalendarDays(current, -1);
  }
  return current;
}

function resolveFrozenDate(input) {
  if (input) {
    return toDateKey(input);
  }
  const tradingDate = getTradingDateKey(new Date());
  return addCalendarDays(tradingDate, -1);
}

function freezeLagMaxRounds() {
  return Math.max(1, Math.min(10, Number(process.env.FREEZE_LAG_MAX_ROUNDS || 5)));
}

function mergeFreezeResult(map, row) {
  if (!row?.userId) {
    return;
  }
  const prev = map.get(row.userId);
  map.set(row.userId, {
    ...prev,
    ...row,
    attempts: (prev?.attempts || 0) + 1,
  });
}

async function freezeUserToDate(userId, frozenDate, options = {}) {
  const logger = options.logger || console;
  const uid = String(userId || "").trim();
  if (!uid) {
    return { userId: "", skipped: true, reason: "missing-user" };
  }
  const { runFreezeIncrementalForUser } = require("./metrics/freeze-incremental");
  const fd = frozenDate ? resolveFrozenDate(frozenDate) : resolveFrozenDate();
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
  return {
    userId: uid,
    skipped: false,
    frozenDate: result.frozenDate,
    analysisRowsWritten: result.timing?.accountRows || 0,
    symbolRowsWritten: result.timing?.symbolRows || 0,
    timing: result.timing,
  };
}

async function freezeUsersBatch(userIds, frozenDate, options, resultsByUser, phase) {
  const logger = options.logger || console;
  const list = [...new Set((userIds || []).map((id) => String(id || "").trim()).filter(Boolean))];
  for (const uid of list) {
    const one = await freezeUserToDate(uid, frozenDate, options);
    mergeFreezeResult(resultsByUser, { ...one, phase });
    if (one.skipped && one.reason && one.reason !== "already-up-to-date" && one.reason !== "no-trades") {
      logger.warn?.("[freeze-eod]", phase, uid, one.reason);
    }
  }
  return list.length;
}

function buildWatermarkMessage({ frozenDate, scopeCount, results, catchUpRounds, lagRemaining, aligned }) {
  const successCount = results.filter((r) => !r.skipped).length;
  const lagPart = lagRemaining.length ? `, lag=${lagRemaining.length}` : "";
  const alignPart = aligned.length ? `, metaAligned=${aligned.length}` : "";
  return `target=${frozenDate}, scope=${scopeCount}, updated=${successCount}, catchUpRounds=${catchUpRounds}${lagPart}${alignPart}`;
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
      lagRemaining: [],
      catchUpRounds: 0,
    };
  }
  const syncDailyClose = options.syncDailyClose === true;
  const fullRebuild = options.fullRebuild === true;
  const userIdsInput = Array.isArray(options.userIds) ? options.userIds : [];
  const scopeUserIds = userIdsInput.length
    ? [...new Set(userIdsInput.map((u) => String(u || "").trim()).filter(Boolean))]
    : await listAllUserIds();
  const freezeOpts = { logger, force, syncDailyClose, fullRebuild };
  const startedAt = Date.now();
  const resultsByUser = new Map();
  const maxRounds = freezeLagMaxRounds();
  let catchUpRounds = 0;
  let lagRemaining = [];
  let aligned = [];
  let caughtError = null;

  try {
    logger.info?.("[freeze-eod] pass1 scope=%s target=%s", scopeUserIds.length, frozenDate);
    await freezeUsersBatch(scopeUserIds, frozenDate, freezeOpts, resultsByUser, "pass1");

    while (catchUpRounds < maxRounds) {
      const lagUserIds = await listLagUserIds(frozenDate, scopeUserIds);
      if (!lagUserIds.length) {
        break;
      }
      catchUpRounds += 1;
      logger.info?.(
        "[freeze-eod] catch-up round=%s lag=%s target=%s",
        catchUpRounds,
        lagUserIds.length,
        frozenDate,
      );
      await freezeUsersBatch(lagUserIds, frozenDate, freezeOpts, resultsByUser, `catch-up-${catchUpRounds}`);
    }

    lagRemaining = await listLagUserIds(frozenDate, scopeUserIds);
    aligned = await alignFrozenThroughForScope(frozenDate, scopeUserIds);
    if (aligned.length) {
      logger.info?.("[freeze-eod] meta aligned count=%s", aligned.length);
    }
    if (lagRemaining.length) {
      logger.warn?.("[freeze-eod] lag remaining count=%s ids=%s", lagRemaining.length, lagRemaining.join(","));
    }
  } catch (error) {
    caughtError = error;
    logger.error?.("[freeze-eod] error", error?.message || error);
    try {
      lagRemaining = await listLagUserIds(frozenDate, scopeUserIds);
    } catch (lagErr) {
      logger.warn?.("[freeze-eod] lag check after error failed", lagErr?.message || lagErr);
    }
  }

  const results = [...resultsByUser.values()];
  const finishedAt = Date.now();
  const watermarkStatus = caughtError ? "failed" : lagRemaining.length ? "partial_failed" : "success";
  const watermarkMessage = caughtError
    ? `${String(caughtError?.message || caughtError).slice(0, 320)}${lagRemaining.length ? `; lag=${lagRemaining.length}` : ""}`
    : buildWatermarkMessage({
        frozenDate,
        scopeCount: scopeUserIds.length,
        results,
        catchUpRounds,
        lagRemaining,
        aligned,
      });

  await setSnapshotWatermark({
    frozenDate,
    status: watermarkStatus,
    message: watermarkMessage,
  });

  const payload = {
    ok: !caughtError && lagRemaining.length === 0,
    partial: !caughtError && lagRemaining.length > 0,
    frozenDate,
    startedAt,
    finishedAt,
    elapsedMs: finishedAt - startedAt,
    catchUpRounds,
    lagRemaining,
    metaAligned: aligned,
    users: results,
    watermark: { status: watermarkStatus, message: watermarkMessage },
  };

  if (caughtError) {
    throw caughtError;
  }
  return payload;
}

/** Vercel 定时：先全局 sync 日 K，再日终冻结（周二～六 08:00 北京）。 */
async function runScheduledEodPipeline(options = {}) {
  const logger = options.logger || console;
  const pipelineStartedAt = Date.now();
  const { runDailyCloseSync } = require("./daily-close-sync-service");

  let dailyClose = null;
  let dailyCloseError = null;
  try {
    logger.info?.("[eod-pipeline] daily-close-sync start");
    dailyClose = await runDailyCloseSync({
      asOfDate: options.asOfDate,
      symbols: options.symbols,
      logger,
    });
    logger.info?.(
      "[eod-pipeline] daily-close-sync done symbolsSynced=%s rowsWritten=%s failed=%s",
      dailyClose?.symbolsSynced,
      dailyClose?.rowsWritten,
      dailyClose?.symbolsFailed,
    );
  } catch (error) {
    dailyCloseError = error;
    logger.error?.("[eod-pipeline] daily-close-sync failed", error?.message || error);
  }

  const freeze = await runDailyFreeze({
    frozenDate: options.frozenDate,
    force: options.force === true,
    userIds: options.userIds,
    fromCron: true,
    rebuildFromDate: options.rebuildFromDate,
    fullRebuild: options.fullRebuild === true,
    syncDailyClose: false,
    logger,
  });

  const pipelineElapsedMs = Date.now() - pipelineStartedAt;
  const dailyCloseFailed = !!dailyCloseError;
  return {
    ...freeze,
    ok: freeze.ok && !dailyCloseFailed,
    partial: freeze.partial || dailyCloseFailed,
    pipelineElapsedMs,
    dailyClose,
    dailyCloseError: dailyCloseError ? String(dailyCloseError?.message || dailyCloseError) : null,
  };
}

module.exports = {
  runDailyFreeze,
  runScheduledEodPipeline,
  resolveFrozenDate,
  freezeUserToDate,
};
