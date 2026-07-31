/**
 * 将 analysis_daily_snapshot / symbol_daily_pnl（v3）映射为原 home_summary 读路径形状。
 */
const { addCalendarDays } = require("./stages");
const { capFrozenThroughToSnapshot, normDateKey } = require("./freeze-calendar");
const { scopedScalarsFromAnalysisRow } = require("./home-account-scalars");

function mapAnalysisRowToHomeAccount(row, frozenThrough, firstTradeDate) {
  if (!row) {
    return null;
  }
  const ft = String(frozenThrough || row.date || "").slice(0, 10);
  const ratio = Number(row.cash_ratio ?? row.cashRatio ?? 0);
  const s = scopedScalarsFromAnalysisRow(row);
  return {
    account_scope: String(row.account_id || row.accountId || "all"),
    account_id: s.accountId,
    frozen_through: ft,
    first_trade_date: String(firstTradeDate || ft).slice(0, 10),
    book_currency: row.book_currency || "CNY",
    // scope 计价（子账户=记账币，all=CNY）
    month_profit: s.monthProfit,
    month_rate_twr: s.monthRateTwr,
    month_rate_mwr: s.monthRateMwr,
    ytd_profit: s.ytdProfit,
    ytd_rate_twr: s.ytdRateTwr,
    ytd_rate_mwr: s.ytdRateMwr,
    total_profit: s.totalProfit,
    total_rate_twr: s.totalRateTwr,
    total_rate_mwr: s.totalRateMwr,
    eod_total_assets: s.eodTotalAssets,
    eod_market_value: s.eodMarketValue,
    eod_cash: s.eodCash,
    eod_principal: s.eodPrincipal,
    last_market_value: s.eodMarketValue,
    // 真 CNY（仅 all 汇总或人民币展示；子账户 TWR 勿读）
    month_profit_cny: s.monthProfitCny,
    ytd_profit_cny: s.ytdProfitCny,
    total_profit_cny: s.totalProfitCny,
    eod_total_assets_cny: s.eodTotalAssetsCny,
    eod_cash_ratio: ratio <= 1 ? ratio * 100 : ratio,
    eod_fx_usd_cny: Number(row.fx_usd_cny ?? 0),
    eod_fx_hkd_cny: Number(row.fx_hkd_cny ?? 0),
  };
}

function mapSymbolRowToHomeSummary(row, frozenThrough) {
  if (!row) {
    return null;
  }
  const ft = String(frozenThrough || row.date || "").slice(0, 10);
  return {
    symbol: String(row.symbol || ""),
    account_scope: String(row.account_id || row.accountId || "all"),
    frozen_through: ft,
    currency: String(row.currency || row.book_currency || "CNY").toUpperCase(),
    month_profit_native: Number(row.stage_mtd_profit ?? 0),
    month_profit_cny: Number(row.stage_mtd_profit_cny ?? 0),
    ytd_profit_native: Number(row.stage_ytd_profit ?? 0),
    ytd_profit_cny: Number(row.stage_ytd_profit_cny ?? 0),
    total_profit_native: Number(row.stage_inception_profit ?? 0),
    total_profit_cny: Number(row.stage_inception_profit_cny ?? 0),
    total_rate_twr: Number(row.stage_inception_rate_twr ?? 0),
    total_rate_mwr: Number(row.stage_inception_rate_mwr ?? 0),
  };
}

function resolveFrozenThrough(umRow, analysisRow) {
  const meta = normDateKey(umRow?.frozen_through || umRow?.frozenThrough);
  const snap = normDateKey(analysisRow?.date);
  return capFrozenThroughToSnapshot(meta, snap) || meta || snap || "";
}

/** 账户 scope 下最早成交日；trades 已按 trade_date 升序时取首条匹配即可。 */
function minFirstTradeDateForScope(trades, accountScope, fallback = "") {
  const scope = String(accountScope || "all").trim() || "all";
  const fb = String(fallback || "").slice(0, 10);
  if (!Array.isArray(trades) || !trades.length) {
    return fb;
  }
  const tradeDate = (t) => String(t.date || t.trade_date || "").slice(0, 10);
  const accountId = (t) => String(t.accountId || t.account_id || "default");
  if (scope === "all") {
    const d = tradeDate(trades[0]);
    return d || fb;
  }
  for (const t of trades) {
    if (accountId(t) === scope) {
      const d = tradeDate(t);
      if (d) {
        return d;
      }
    }
  }
  return fb;
}

module.exports = {
  mapAnalysisRowToHomeAccount,
  mapSymbolRowToHomeSummary,
  resolveFrozenThrough,
  minFirstTradeDateForScope,
  addCalendarDays,
};
