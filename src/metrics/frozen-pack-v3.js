/**
 * 将 analysis_daily_snapshot / symbol_daily_pnl（v3）映射为原 home_summary 读路径形状。
 */
const { addCalendarDays } = require("./stages");
const { capFrozenThroughToSnapshot, normDateKey } = require("./freeze-calendar");

function mapAnalysisRowToHomeAccount(row, frozenThrough, firstTradeDate) {
  if (!row) {
    return null;
  }
  const ft = String(frozenThrough || row.date || "").slice(0, 10);
  const ratio = Number(row.cash_ratio ?? row.cashRatio ?? 0);
  return {
    account_scope: String(row.account_id || row.accountId || "all"),
    account_id: String(row.account_id || row.accountId || "all"),
    frozen_through: ft,
    first_trade_date: String(firstTradeDate || ft).slice(0, 10),
    month_profit_cny: Number(row.stage_mtd_profit_cny ?? row.stage_mtd_profit ?? 0),
    month_rate_twr: Number(row.stage_mtd_rate_twr_cny ?? row.stage_mtd_rate_twr ?? 0),
    month_rate_mwr: Number(row.stage_mtd_rate_mwr ?? 0),
    ytd_profit_cny: Number(row.stage_ytd_profit_cny ?? row.stage_ytd_profit ?? 0),
    ytd_rate_twr: Number(row.stage_ytd_rate_twr_cny ?? row.stage_ytd_rate_twr ?? 0),
    ytd_rate_mwr: Number(row.stage_ytd_rate_mwr ?? 0),
    total_profit_cny: Number(row.stage_inception_profit_cny ?? row.stage_inception_profit ?? 0),
    total_rate_twr: Number(row.stage_inception_rate_twr_cny ?? row.stage_inception_rate_twr ?? 0),
    total_rate_mwr: Number(row.stage_inception_rate_mwr ?? 0),
    last_market_value_cny: Number(row.market_value ?? 0),
    eod_total_assets_cny: Number(row.total_assets_cny ?? row.total_assets ?? 0),
    eod_market_value_cny: Number(row.market_value ?? 0),
    eod_cash_cny: Number(row.cash ?? 0),
    eod_cash_ratio: ratio <= 1 ? ratio * 100 : ratio,
    eod_principal_cny: Number(row.principal ?? 0),
    eod_fx_usd_cny: Number(row.fx_usd_cny ?? 0),
    eod_fx_hkd_cny: Number(row.fx_hkd_cny ?? 0),
    book_currency: row.book_currency || "CNY",
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
    month_profit_cny: Number(row.stage_mtd_profit_cny ?? row.stage_mtd_profit ?? 0),
    ytd_profit_native: Number(row.stage_ytd_profit ?? 0),
    ytd_profit_cny: Number(row.stage_ytd_profit_cny ?? row.stage_ytd_profit ?? 0),
    total_profit_native: Number(row.stage_inception_profit ?? 0),
    total_profit_cny: Number(row.stage_inception_profit_cny ?? row.stage_inception_profit ?? 0),
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
