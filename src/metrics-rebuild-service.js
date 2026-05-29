/**
 * 写路径静默重算：从 rebuildFrom 日起逐日 freeze 到昨日，完成后 data_version++。
 */
const {
  getTrades,
  getCashTransfers,
  upsertUserMetricsMeta,
  getUserMetricsMeta,
  deletePerformanceSeriesCacheForUser,
} = require("./db");
const { freezeUserToDate, resolveFrozenDate } = require("./eod-freeze-service");
const { addCalendarDays } = require("./metrics/stages");

const queueByUser = new Map();

function earliestAffectedDateFromTradesCash(trades, cash, hintDate) {
  let minD = hintDate ? String(hintDate).slice(0, 10) : null;
  for (const t of trades || []) {
    const d = String(t.date || "").slice(0, 10);
    if (d && (!minD || d < minD)) {
      minD = d;
    }
  }
  for (const c of cash || []) {
    const d = String(c.date || "").slice(0, 10);
    if (d && (!minD || d < minD)) {
      minD = d;
    }
  }
  return minD;
}

function enumerateDatesInclusive(start, end) {
  const out = [];
  let cur = String(start).slice(0, 10);
  const e = String(end).slice(0, 10);
  while (cur <= e) {
    out.push(cur);
    cur = addCalendarDays(cur, 1);
  }
  return out;
}

async function runMetricsRebuildForUser(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { ok: false };
  }
  const hint = opts.hintDate ? String(opts.hintDate).slice(0, 10) : null;
  const trades = await getTrades(uid);
  const cash = await getCashTransfers(uid);
  const earliest = earliestAffectedDateFromTradesCash(trades, cash, hint);
  if (!earliest) {
    await upsertUserMetricsMeta(uid, { rebuilding: false });
    return { ok: true, skip: true };
  }
  const rebuildFrom = addCalendarDays(earliest, -1);
  const frozenEnd = resolveFrozenDate();
  const dates = enumerateDatesInclusive(rebuildFrom, frozenEnd);
  await upsertUserMetricsMeta(uid, { rebuilding: true, rebuildFrom });
  await deletePerformanceSeriesCacheForUser(uid);
  try {
    const { runFreezeV3ForUser } = require("./metrics/freeze-v3");
    await runFreezeV3ForUser(uid, {
      frozenDate: frozenEnd,
      force: true,
      syncDailyClose: false,
      logger: console,
    });
    const meta = await getUserMetricsMeta(uid);
    await upsertUserMetricsMeta(uid, {
      rebuilding: false,
      dataVersion: (meta.dataVersion || 0) + 1,
      frozenThrough: frozenEnd,
    });
    return { ok: true, dates: dates.length };
  } catch (error) {
    await upsertUserMetricsMeta(uid, { rebuilding: false });
    throw error;
  }
}

function scheduleMetricsRebuildForUser(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  if (queueByUser.has(uid)) {
    return;
  }
  const p = runMetricsRebuildForUser(uid, opts)
    .catch((e) => {
      console.warn("[metrics-rebuild]", uid, e?.message || e);
    })
    .finally(() => {
      queueByUser.delete(uid);
    });
  queueByUser.set(uid, p);
}

module.exports = {
  scheduleMetricsRebuildForUser,
  runMetricsRebuildForUser,
};
