/**
 * 日终冻结漏跑检测与 meta 对齐（freeze-eod 主任务验收）。
 */
const {
  getUserMetricsMeta,
  getLatestAnalysisSnapshotDate,
  upsertUserMetricsMeta,
  listFreezeLagUserIds,
} = require("../db");
const {
  capFrozenThroughToSnapshot,
  normDateKey,
  isWeekendDateKey,
  previousSessionDate,
} = require("./freeze-calendar");

function effectiveFreezeTarget(targetDate) {
  let d = normDateKey(targetDate);
  while (d && isWeekendDateKey(d)) {
    d = previousSessionDate(d);
  }
  return d || normDateKey(targetDate);
}

function normTargetDate(targetDate) {
  return effectiveFreezeTarget(targetDate);
}

async function listLagUserIds(targetDate, scopeUserIds = null) {
  const target = normTargetDate(targetDate);
  const opts =
    Array.isArray(scopeUserIds) && scopeUserIds.length
      ? { scopeUserIds }
      : {};
  return listFreezeLagUserIds(target, opts);
}

/** 快照已到 target 但 frozen_through 偏旧时对齐 meta（不写新快照行）。 */
async function alignFrozenThroughForScope(targetDate, scopeUserIds) {
  const target = normTargetDate(targetDate);
  const list = Array.isArray(scopeUserIds) ? scopeUserIds : [];
  const aligned = [];
  for (const uid of list) {
    const id = String(uid || "").trim();
    if (!id) {
      continue;
    }
    const latest = await getLatestAnalysisSnapshotDate(id, "all");
    if (!latest || latest < target) {
      continue;
    }
    const meta = await getUserMetricsMeta(id, { light: true });
    const next = capFrozenThroughToSnapshot(meta.frozenThrough, latest) || latest;
    if (next && next !== meta.frozenThrough) {
      await upsertUserMetricsMeta(id, { frozenThrough: next });
      aligned.push({ userId: id, from: meta.frozenThrough, to: next });
    }
  }
  return aligned;
}

module.exports = {
  listLagUserIds,
  alignFrozenThroughForScope,
};
