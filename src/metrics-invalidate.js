/**
 * 成交 / 银证写路径：收集受影响日期；晚于 frozen_through 仅清缓存走实时，否则区间重算。
 */
const { hintDatesForRebuild } = require("./metrics/freeze-calendar");

/** 新增/修改成交：合并改前、改后日期 */
function hintDatesFromTradeMutation(priorTrade, nextTrade) {
  return hintDatesForRebuild([priorTrade?.date, nextTrade?.date].filter(Boolean));
}

/** 新增/修改银证 */
function hintDatesFromCashMutation(priorCash, nextCash) {
  const pd = priorCash?.date || priorCash?.transferDate;
  const nd = nextCash?.date || nextCash?.transferDate;
  return hintDatesForRebuild([pd, nd].filter(Boolean));
}

function hintDatesFromImportRows(rows, dateField = "date") {
  const raw = (rows || []).map((r) => r?.[dateField] || r?.transferDate || r?.transfer_date).filter(Boolean);
  return hintDatesForRebuild(raw);
}

/**
 * @param {string} userId
 * @param {{ hintDates?: string[], fullRebuild?: boolean }} opts
 */
function notifyLedgerMutation(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) return;
  const { scheduleMetricsRebuildForUser, kickMetricsRebuildNow } = require("./metrics-rebuild-service");
  scheduleMetricsRebuildForUser(uid, {
    hintDates: opts.hintDates || [],
    fullRebuild: !!opts.fullRebuild,
  });
  kickMetricsRebuildNow(uid);
}

module.exports = {
  hintDatesFromTradeMutation,
  hintDatesFromCashMutation,
  hintDatesFromImportRows,
  notifyLedgerMutation,
};
