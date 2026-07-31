/**
 * Home 冻结账户标量：子账户=记账币(book)，全部账户=all=人民币(CNY)。
 * analysis_daily_snapshot 同时存 book 列与 *_cny 列；读路径须按 scope 区分，勿混用。
 */

function isAggregateAccountScope(accountId) {
  const id = String(accountId || "all").trim() || "all";
  return id === "all";
}

function pickBookOrCny(row, bookKey, cnyKey, aggregate) {
  if (!row) {
    return 0;
  }
  if (aggregate) {
    return Number(row[cnyKey] ?? row[bookKey] ?? 0);
  }
  return Number(row[bookKey] ?? 0);
}

/** 从 analysis_daily_snapshot 行映射 scope 计价金额/收益率（非真 CNY 列）。 */
function scopedScalarsFromAnalysisRow(row) {
  const accountId = String(row?.account_id || row?.accountId || "all");
  const aggregate = isAggregateAccountScope(accountId);
  return {
    accountId,
    aggregate,
    monthProfit: pickBookOrCny(row, "stage_mtd_profit", "stage_mtd_profit_cny", aggregate),
    monthRateTwr: pickBookOrCny(row, "stage_mtd_rate_twr", "stage_mtd_rate_twr_cny", aggregate),
    monthRateMwr: Number(row?.stage_mtd_rate_mwr ?? 0),
    ytdProfit: pickBookOrCny(row, "stage_ytd_profit", "stage_ytd_profit_cny", aggregate),
    ytdRateTwr: pickBookOrCny(row, "stage_ytd_rate_twr", "stage_ytd_rate_twr_cny", aggregate),
    ytdRateMwr: Number(row?.stage_ytd_rate_mwr ?? 0),
    totalProfit: pickBookOrCcyInception(row, aggregate),
    totalRateTwr: pickBookOrCny(row, "stage_inception_rate_twr", "stage_inception_rate_twr_cny", aggregate),
    totalRateMwr: Number(row?.stage_inception_rate_mwr ?? 0),
    eodTotalAssets: pickBookOrCny(row, "total_assets", "total_assets_cny", aggregate),
    eodMarketValue: Number(row?.market_value ?? 0),
    eodCash: Number(row?.cash ?? 0),
    eodPrincipal: Number(row?.principal ?? 0),
    monthProfitCny: Number(row?.stage_mtd_profit_cny ?? 0),
    ytdProfitCny: Number(row?.stage_ytd_profit_cny ?? 0),
    totalProfitCny: Number(row?.stage_inception_profit_cny ?? 0),
    eodTotalAssetsCny: Number(row?.total_assets_cny ?? 0),
  };
}

function pickBookOrCcyInception(row, aggregate) {
  if (aggregate) {
    return Number(row?.stage_inception_profit_cny ?? row?.stage_inception_profit ?? 0);
  }
  return Number(row?.stage_inception_profit ?? 0);
}

/** 从 mapAnalysisRowToHomeAccount 产物读取 scope 计价 EOD 标量。 */
function homeAccountEod(acc) {
  const a = acc || {};
  const book = String(a.book_currency || "CNY").toUpperCase();
  const aggregate = isAggregateAccountScope(a.account_id || a.account_scope);
  let totalAssets = Number(a.eod_total_assets);
  if (!Number.isFinite(totalAssets) || totalAssets <= 0) {
    if (aggregate || book === "CNY") {
      totalAssets = Number(a.eod_total_assets_cny) || 0;
    } else {
      totalAssets = 0;
    }
  }
  const marketValue =
    Number(a.eod_market_value ?? a.last_market_value ?? a.last_market_value_cny ?? 0) || 0;
  let cash = Number(a.eod_cash);
  if (!Number.isFinite(cash)) {
    cash = aggregate || book === "CNY" ? Number(a.eod_cash_cny ?? 0) : 0;
  }
  let principal = Number(a.eod_principal);
  if (!Number.isFinite(principal)) {
    principal = aggregate || book === "CNY" ? Number(a.eod_principal_cny ?? 0) : 0;
  }
  return {
    totalAssets,
    marketValue,
    cash,
    principal,
    cashRatioPct: Number(a.eod_cash_ratio ?? 0) || 0,
  };
}

/** 从 home 账户映射读取 stage 累计收益/收益率（scope 计价）。 */
function homeAccountStageMetrics(acc) {
  const a = acc || {};
  const book = String(a.book_currency || "CNY").toUpperCase();
  const aggregate = isAggregateAccountScope(a.account_id || a.account_scope);
  const profitFallback = (bookKey, cnyKey) => {
    const bookVal = Number(a[bookKey]);
    if (Number.isFinite(bookVal)) {
      return bookVal;
    }
    if (aggregate || book === "CNY") {
      return Number(a[cnyKey] ?? 0) || 0;
    }
    return 0;
  };
  const eod = homeAccountEod(a);
  return {
    monthProfit: profitFallback("month_profit", "month_profit_cny"),
    monthRateTwr: Number(a.month_rate_twr ?? 0) || 0,
    monthRateMwr: Number(a.month_rate_mwr ?? 0) || 0,
    ytdProfit: profitFallback("ytd_profit", "ytd_profit_cny"),
    ytdRateTwr: Number(a.ytd_rate_twr ?? 0) || 0,
    ytdRateMwr: Number(a.ytd_rate_mwr ?? 0) || 0,
    totalProfit: profitFallback("total_profit", "total_profit_cny"),
    totalRateTwr: Number(a.total_rate_twr ?? 0) || 0,
    totalRateMwr: Number(a.total_rate_mwr ?? 0) || 0,
    eodTotalAssets: eod.totalAssets,
    eodMarketValue: eod.marketValue,
    eodCash: eod.cash,
  };
}

module.exports = {
  isAggregateAccountScope,
  scopedScalarsFromAnalysisRow,
  homeAccountEod,
  homeAccountStageMetrics,
};
