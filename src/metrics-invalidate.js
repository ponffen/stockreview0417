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
 * 成交响应前同步打标；freeze 在 waitUntil 后台执行。
 * @param {string} userId
 * @param {{ hintDates?: string[], fullRebuild?: boolean }} opts
 */
async function notifyLedgerMutation(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  const { prepareLedgerMetricsFreeze, dispatchFreezeEodJobAsync } = require("./metrics-rebuild-trigger");
  const { runInBackground } = require("./background-task");

  const prepared = await prepareLedgerMetricsFreeze(uid, {
    hintDates: opts.hintDates || [],
    fullRebuild: !!opts.fullRebuild,
  });

  if (!prepared.payload) {
    console.log(
      "[metrics-invalidate] skip userId=%s reason=%s hints=%s",
      uid,
      prepared.reason || "no-op",
      (opts.hintDates || []).join(",") || "-",
    );
    return;
  }

  console.log(
    "[metrics-invalidate] freeze queued userId=%s rebuildFromDate=%s fullRebuild=%s",
    uid,
    prepared.rebuildFromDate || "-",
    !!prepared.fullRebuild,
  );
  const freezeTask = () => dispatchFreezeEodJobAsync(prepared.payload);
  if (String(process.env.VERCEL || "").trim() === "1") {
    // Vercel：同请求内 await freeze（maxDuration=300s），避免 waitUntil 未执行导致假完成
    await freezeTask();
    return;
  }
  runInBackground(freezeTask);
}

module.exports = {
  hintDatesFromTradeMutation,
  hintDatesFromCashMutation,
  hintDatesFromImportRows,
  notifyLedgerMutation,
};
