/**
 * 写路径静默重算：从本次受影响最早日（前推 1 天）逐日 freeze 到昨日。
 */
const {
  getTrades,
  upsertUserMetricsMeta,
  getUserMetricsMeta,
  getLatestAnalysisSnapshotDate,
  deletePerformanceSeriesCacheForUser,
} = require("./db");
const { resolveFrozenDate } = require("./eod-freeze-service");
const { addCalendarDays } = require("./metrics/stages");
const { minDateKey, normDateKey } = require("./metrics/date-keys");
const { capFrozenThroughToSnapshot } = require("./metrics/freeze-calendar");

const DEBOUNCE_MS = Math.max(
  1000,
  Math.min(120_000, Number(process.env.METRICS_REBUILD_DEBOUNCE_MS) || 10_000),
);
const MAX_RETRIES = Math.max(1, Math.min(5, Number(process.env.METRICS_REBUILD_RETRIES) || 3));
const RETRY_BASE_MS = Math.max(500, Number(process.env.METRICS_REBUILD_RETRY_MS) || 2000);

const queueByUser = new Map();
const pendingByUser = new Map();

function earliestFromHints(hintDates, fullRebuild) {
  if (fullRebuild) {
    return null;
  }
  const minHint = minDateKey(hintDates);
  if (!minHint) {
    return null;
  }
  return addCalendarDays(minHint, -1);
}

async function resolveFrozenThroughForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return "";
  }
  const meta = await getUserMetricsMeta(uid, { light: true });
  const latest = await getLatestAnalysisSnapshotDate(uid, "all");
  return capFrozenThroughToSnapshot(meta.frozenThrough, latest) || latest || "";
}

/** 晚于 frozen_through 的流水只走实时；其余才冻历史快照。 */
function partitionHintDates(hintDates, frozenThrough) {
  const ft = normDateKey(frozenThrough);
  const freezeHints = [];
  const liveOnlyHints = [];
  for (const raw of hintDates || []) {
    const dk = normDateKey(raw);
    if (!dk) {
      continue;
    }
    if (ft && dk > ft) {
      liveOnlyHints.push(dk);
    } else {
      freezeHints.push(dk);
    }
  }
  return { freezeHints, liveOnlyHints };
}

async function refreshLiveMetricsOnly(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { ok: false };
  }
  await deletePerformanceSeriesCacheForUser(uid);
  const meta = await getUserMetricsMeta(uid, { light: true });
  if (meta.rebuilding) {
    await upsertUserMetricsMeta(uid, { rebuilding: false, rebuildFrom: null });
  }
  return { ok: true, mode: "live-only" };
}

function mergePending(uid, hintDates, fullRebuild) {
  const cur = pendingByUser.get(uid) || { hintDates: [], fullRebuild: false, timer: null };
  if (fullRebuild) {
    cur.fullRebuild = true;
  }
  for (const d of hintDates || []) {
    const dk = normDateKey(d);
    if (dk) cur.hintDates.push(dk);
  }
  pendingByUser.set(uid, cur);
  return cur;
}

async function runMetricsRebuildForUser(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { ok: false };
  }
  const fullRebuild = !!opts.fullRebuild;
  const trades = await getTrades(uid);
  if (!trades?.length) {
    await upsertUserMetricsMeta(uid, { rebuilding: false });
    return { ok: true, skip: true, reason: "no-trades" };
  }

  const rebuildFromDate = fullRebuild ? null : earliestFromHints(opts.hintDates, false);
  if (!fullRebuild && !rebuildFromDate) {
    await upsertUserMetricsMeta(uid, { rebuilding: false });
    return { ok: true, skip: true, reason: "no-hint" };
  }

  const frozenEnd = resolveFrozenDate();
  await upsertUserMetricsMeta(uid, {
    rebuilding: true,
    rebuildFrom: rebuildFromDate || null,
  });
  await deletePerformanceSeriesCacheForUser(uid);

  const freezeOpts = {
    frozenDate: frozenEnd,
    force: true,
    syncDailyClose: false,
    logger: console,
    rebuildFromDate: rebuildFromDate || undefined,
    fullRebuild,
  };

  let lastErr = null;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      const { runFreezeV3ForUser } = require("./metrics/freeze-v3");
      await runFreezeV3ForUser(uid, freezeOpts);
      const meta = await getUserMetricsMeta(uid);
      const { getLatestAnalysisSnapshotDate } = require("./db");
      const latestSnap = await getLatestAnalysisSnapshotDate(uid, "all");
      const { capFrozenThroughToSnapshot } = require("./metrics/freeze-calendar");
      const frozenThrough = capFrozenThroughToSnapshot(frozenEnd, latestSnap) || latestSnap || meta.frozenThrough || frozenEnd;
      await upsertUserMetricsMeta(uid, {
        rebuilding: false,
        dataVersion: (meta.dataVersion || 0) + 1,
        frozenThrough,
        rebuildFrom: null,
      });
      return { ok: true, rebuildFromDate, frozenEnd, attempt };
    } catch (error) {
      lastErr = error;
      console.warn("[metrics-rebuild]", uid, `attempt ${attempt}/${MAX_RETRIES}`, error?.message || error);
      if (attempt < MAX_RETRIES) {
        await new Promise((r) => setTimeout(r, RETRY_BASE_MS * attempt));
      }
    }
  }
  await upsertUserMetricsMeta(uid, { rebuilding: false, rebuildFrom: null });
  throw lastErr || new Error("metrics rebuild failed");
}

function flushPendingRebuild(uid) {
  const pending = pendingByUser.get(uid);
  if (!pending) {
    return;
  }
  if (pending.timer) {
    clearTimeout(pending.timer);
  }
  pendingByUser.delete(uid);
  const hintDates = [...(pending.hintDates || [])];
  const fullRebuild = !!pending.fullRebuild;
  if (!hintDates.length && !fullRebuild) {
    return;
  }
  if (queueByUser.has(uid)) {
    mergePending(uid, hintDates, fullRebuild);
    return;
  }
  const p = flushPendingRebuildWork(uid, hintDates, fullRebuild)
    .catch((e) => {
      console.warn("[metrics-rebuild] failed", uid, e?.message || e);
    })
    .finally(() => {
      queueByUser.delete(uid);
      const again = pendingByUser.get(uid);
      if (again && (again.hintDates?.length || again.fullRebuild)) {
        scheduleMetricsRebuildForUser(uid, {
          hintDates: again.hintDates,
          fullRebuild: again.fullRebuild,
        });
      }
    });
  queueByUser.set(uid, p);
}

async function flushPendingRebuildWork(uid, hintDates, fullRebuild) {
  if (!fullRebuild) {
    const frozenThrough = await resolveFrozenThroughForUser(uid);
    const { freezeHints, liveOnlyHints } = partitionHintDates(hintDates, frozenThrough);
    if (!freezeHints.length) {
      if (liveOnlyHints.length) {
        await refreshLiveMetricsOnly(uid);
      }
      return { ok: true, skip: true, reason: "live-only-after-frozen-through" };
    }
    return runMetricsRebuildForUser(uid, { hintDates: freezeHints, fullRebuild: false });
  }
  return runMetricsRebuildForUser(uid, { hintDates, fullRebuild: true });
}

function isVercelRuntime() {
  return String(process.env.VERCEL || "").trim() === "1";
}

function scheduleMetricsRebuildForUser(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  const pending = mergePending(uid, opts.hintDates || [], !!opts.fullRebuild);
  if (pending.timer) {
    clearTimeout(pending.timer);
    pending.timer = null;
  }
  const debounceMs = isVercelRuntime() ? 400 : DEBOUNCE_MS;
  if (isVercelRuntime() && !queueByUser.has(uid)) {
    pending.timer = setTimeout(() => flushPendingRebuild(uid), debounceMs);
    return;
  }
  pending.timer = setTimeout(() => flushPendingRebuild(uid), debounceMs);
}

/** Vercel：在当次 Serverless 调用内尽快开跑（避免仅 setTimeout 后实例被回收）。 */
function kickMetricsRebuildNow(userId) {
  const uid = String(userId || "").trim();
  if (!uid || !isVercelRuntime()) {
    return;
  }
  const pending = pendingByUser.get(uid);
  if (pending?.timer) {
    clearTimeout(pending.timer);
    pending.timer = null;
  }
  setImmediate(() => flushPendingRebuild(uid));
}

module.exports = {
  scheduleMetricsRebuildForUser,
  runMetricsRebuildForUser,
  kickMetricsRebuildNow,
  partitionHintDates,
  resolveFrozenThroughForUser,
  refreshLiveMetricsOnly,
};
