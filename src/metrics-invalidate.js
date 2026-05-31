/**
 * 成交 / 银证写路径：收集受影响日期并触发 metrics 区间重算。
 */

/** 新增/修改成交：合并改前、改后日期 */
function hintDatesFromTradeMutation(priorTrade, nextTrade) {
  const out = [];
  if (priorTrade?.date) out.push(priorTrade.date);
  if (nextTrade?.date) out.push(nextTrade.date);
  return out;
}

/** 新增/修改银证 */
function hintDatesFromCashMutation(priorCash, nextCash) {
  const out = [];
  const pd = priorCash?.date || priorCash?.transferDate;
  const nd = nextCash?.date || nextCash?.transferDate;
  if (pd) out.push(pd);
  if (nd) out.push(nd);
  return out;
}

function hintDatesFromImportRows(rows, dateField = "date") {
  return (rows || []).map((r) => r?.[dateField] || r?.transferDate || r?.transfer_date).filter(Boolean);
}

function clearUserMetricsCaches(userId) {
  void userId;
}

/**
 * @param {string} userId
 * @param {{ hintDates?: string[], fullRebuild?: boolean }} opts
 */
function notifyLedgerMutation(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) return;
  clearUserMetricsCaches(uid);
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
