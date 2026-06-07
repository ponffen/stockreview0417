/**
 * 分析 Tab / 个股分析：区间 chip → stage、symbol_daily_pnl stage 收益字段。
 */
const { sortTradeAsc } = require("./stock-rank-period");

const STOCK_RECORD_RANGE_TO_STAGE = {
  7: "last_7d",
  30: "last_30d",
  90: "last_90d",
  mtd: "mtd",
  ytd: "ytd",
  all: "inception",
};

function stockRecordRangeChipToStage(rangeChip) {
  const raw = String(rangeChip || "30").trim().toLowerCase();
  return STOCK_RECORD_RANGE_TO_STAGE[raw] || "last_30d";
}

/** 个股 pnl 行按 stage 取累计收益（本币），对齐 stageProfitCnyFromSnapshotRow。 */
function stageProfitFromSymbolPnlRow(row, stageKey) {
  const st = String(stageKey || "last_30d").trim() || "last_30d";
  if (!row) {
    return 0;
  }
  if (st === "mtd") {
    return Number(row.stageMtdProfit ?? 0);
  }
  if (st === "ytd") {
    return Number(row.stageYtdProfit ?? 0);
  }
  if (st === "inception") {
    return Number(row.stageInceptionProfit ?? 0);
  }
  if (st === "last_7d") {
    return Number(row.stageLast7dProfit ?? 0);
  }
  if (st === "last_30d") {
    return Number(row.stageLast30dProfit ?? 0);
  }
  if (st === "last_90d") {
    return Number(row.stageLast90dProfit ?? 0);
  }
  return Number(row.stageLast30dProfit ?? row.stageInceptionProfit ?? 0);
}

function firstTradeDateFromTrades(trades, fallback) {
  const asOf = String(fallback || "").slice(0, 10);
  if (!Array.isArray(trades) || !trades.length) {
    return asOf;
  }
  return [...trades].sort(sortTradeAsc)[0].date;
}

module.exports = {
  STOCK_RECORD_RANGE_TO_STAGE,
  stockRecordRangeChipToStage,
  stageProfitFromSymbolPnlRow,
  firstTradeDateFromTrades,
};
