/**
 * 冻结日 EOD（analysis_daily_snapshot v3）+ 盘中实时增量。
 */
const { normalizeSymbol } = require("../db");
const { hasOpenPositionQuantity } = require("./holdings-active-symbols");
const { computeLedgerCashBookUpToDate, externalFlowBookForRange } = require("../ledger-metrics");
const { shouldCountTodayPositionPnlFromQuote } = require("../position-today-pnl");

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

/**
 * 账户「今日」是否与个股一致：至少一只持仓的行情日 = 当前交易日期（08:30 北京）。
 * 无持仓或行情均未切到今日 → 不计账户今日收益。
 */
function shouldCountAccountTodayPnl({ positions, quoteBySymbol, now = new Date(), ledgerSessionKey = null }) {
  const quotes = quoteBySymbol && typeof quoteBySymbol === "object" ? quoteBySymbol : {};
  for (const p of positions || []) {
    const qty = Number(p.quantity) || 0;
    if (!hasOpenPositionQuantity(qty)) {
      if (Math.abs(Number(p.todayProfitCny) || 0) > 1e-6) {
        return true;
      }
      continue;
    }
    const sym = normalizeSymbol(p.symbol);
    const quote = quotes[sym] ?? quotes[p.symbol];
    if (shouldCountTodayPositionPnlFromQuote(quote, now, ledgerSessionKey)) {
      return true;
    }
  }
  return false;
}

/** 账户今日收益：门控通过后为各持仓 todayProfitCny 之和；否则 0（不用总资产差，避免现金/汇率假增量）。 */
function resolveAccountTodayProfitCny(live, positions, quoteBySymbol, now = new Date()) {
  if (!live?.tradingDay) {
    return 0;
  }
  const ledgerSessionKey = String(live.liveDate || "").slice(0, 10) || null;
  if (!shouldCountAccountTodayPnl({ positions, quoteBySymbol, now, ledgerSessionKey })) {
    return 0;
  }
  return (positions || []).reduce((s, p) => s + (Number(p.todayProfitCny) || 0), 0);
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
  if (!ft) {
    return null;
  }
  const cashFrozen = computeLedgerCashBookUpToDate(
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
  // 外部流入须覆盖「冻结日之后 → 今天」整段（含周末/节假日），与 liveTA−frozenTA 的口径对齐，
  // 否则空档期的银证转入（如周末转入）会被当日 TWR 误算成投资收益。
  const externalFlowTodayCny = externalFlowBookForRange(
    cashTransfers,
    accounts,
    scope,
    fxUsdMap,
    fxHkdMap,
    ft,
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
  shouldCountAccountTodayPnl,
  resolveAccountTodayProfitCny,
  positionDailyTwrReturn,
  applyEodPlusLiveTotals,
};
