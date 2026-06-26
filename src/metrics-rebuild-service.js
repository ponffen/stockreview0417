/**
 * 写路径：debounce 合并 hint 后异步调 freeze-eod（见 metrics-rebuild-trigger.js）。
 */
const { upsertUserMetricsMeta, getUserMetricsMeta, getLatestAnalysisSnapshotDate } = require("./db");
const { addCalendarDays } = require("./metrics/stages");
const { minDateKey, normDateKey } = require("./metrics/date-keys");
const { capFrozenThroughToSnapshot } = require("./metrics/freeze-calendar");

const DEBOUNCE_MS = Math.max(
  1000,
  Math.min(120_000, Number(process.env.METRICS_REBUILD_DEBOUNCE_MS) || 10_000),
);

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
  const { triggerLedgerMetricsFreeze } = require("./metrics-rebuild-trigger");
  if (!fullRebuild) {
    const frozenThrough = await resolveFrozenThroughForUser(uid);
    const { freezeHints, liveOnlyHints } = partitionHintDates(hintDates, frozenThrough);
    if (!freezeHints.length) {
      if (liveOnlyHints.length) {
        await refreshLiveMetricsOnly(uid);
      }
      return { ok: true, skip: true, reason: "live-only-after-frozen-through" };
    }
    return triggerLedgerMetricsFreeze(uid, { hintDates: freezeHints, fullRebuild: false });
  }
  return triggerLedgerMetricsFreeze(uid, { hintDates, fullRebuild: true });
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
  pending.timer = setTimeout(() => flushPendingRebuild(uid), debounceMs);
}

/** 保存后立即触发 freeze-eod（不等 debounce）。 */
function kickMetricsRebuildNow(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
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
  kickMetricsRebuildNow,
  partitionHintDates,
  resolveFrozenThroughForUser,
  refreshLiveMetricsOnly,
  earliestFromHints,
};
