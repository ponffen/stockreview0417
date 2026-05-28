/**
 * 冻结日 EOD（account_home_summary / analysis 快照）+ 盘中实时增量。
 */
const { computeLedgerCashCnyUpToDate, externalFlowCnyForDate } = require("../ledger-metrics");

function chainTwrRate(frozenRate, todayRate) {
  const f = Number(frozenRate) || 0;
  const t = Number(todayRate) || 0;
  return (1 + f) * (1 + t) - 1;
}

/** 账户日 TWR：相对冻结日总资产，剔除当日银证出入金。 */
function accountDailyTwrReturn(frozenTotalAssetsCny, liveTotalAssetsCny, externalFlowTodayCny) {
  const base = Number(frozenTotalAssetsCny) || 0;
  const flow = Number(externalFlowTodayCny) || 0;
  const live = Number(liveTotalAssetsCny) || 0;
  const denom = base + Math.max(flow, 0);
  if (denom <= 0) {
    return 0;
  }
  return (live - base - flow) / denom;
}

/** 今日收益（账户）：当前总资产 − 冻结日总资产 − 今日银证净额 */
function todayProfitCnyFromTotals(live) {
  if (!live?.tradingDay) {
    return 0;
  }
  const frozenTa = Number(live.eodTotalAssetsCny) || 0;
  const flow = Number(live.externalFlowTodayCny) || 0;
  const ta = Number(live.totalAssetsCny) || 0;
  return ta - frozenTa - flow;
}

/** 个股日 TWR：市值=总资产，当日买卖=出入金 */
function positionDailyTwrReturn(startMarketValueNat, liveMarketValueNat, todayProfitNative, todayTradeFlowNative) {
  const base = Number(startMarketValueNat) || 0;
  const flow = Number(todayTradeFlowNative) || 0;
  const profit = Number(todayProfitNative) || 0;
  const denom = base + Math.max(flow, 0);
  if (denom <= 0) {
    return 0;
  }
  return profit / denom;
}

/**
 * 总资产 = 冻结市值 + (实时市值-冻结市值) + 冻结现金 + (账本现金_live-账本现金_冻结)。
 * 与全量重放 ledger 到 live 日区分，避免与 EOD 快照现金不一致。
 */
function applyEodPlusLiveTotals({
  homeAcc,
  frozenThrough,
  liveDate,
  liveMarketValueCny,
  ledgerCashAtLive,
  trades,
  cashTransfers,
  accounts,
  scope,
  fxUsdMap,
  fxHkdMap,
}) {
  const eodTa = Number(homeAcc?.eod_total_assets_cny) || 0;
  const eodMv = Number(homeAcc?.eod_market_value_cny) || 0;
  const eodCash = Number(homeAcc?.eod_cash_cny) || 0;
  const ft = String(frozenThrough || "").slice(0, 10);
  if (!ft || !(eodTa > 0)) {
    return null;
  }
  const cashFrozen = computeLedgerCashCnyUpToDate(
    trades,
    cashTransfers,
    accounts,
    scope,
    fxUsdMap,
    fxHkdMap,
    ft,
  );
  const mvCny = eodMv + (Number(liveMarketValueCny) - eodMv);
  const cashCny = eodCash + (Number(ledgerCashAtLive) - cashFrozen);
  const totalAssetsCny = mvCny + cashCny;
  const externalFlowTodayCny = externalFlowCnyForDate(
    cashTransfers,
    accounts,
    scope,
    fxUsdMap,
    fxHkdMap,
    liveDate,
  );
  return {
    eodTotalAssetsCny: eodTa,
    eodMarketValueCny: eodMv,
    eodCashCny: eodCash,
    liveMarketValueCny: mvCny,
    cashCny,
    totalAssetsCny,
    externalFlowTodayCny,
    cashRatio: totalAssetsCny > 0 ? cashCny / totalAssetsCny : 0,
  };
}

module.exports = {
  chainTwrRate,
  accountDailyTwrReturn,
  todayProfitCnyFromTotals,
  positionDailyTwrReturn,
  applyEodPlusLiveTotals,
};
