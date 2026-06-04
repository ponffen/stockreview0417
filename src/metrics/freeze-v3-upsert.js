/**
 * Metrics v3 冻结写库：analysis_daily_snapshot / symbol_daily_pnl 批量 upsert。
 * 独立模块，供 freeze-v3 与 freeze-incremental 共用，避免循环依赖。
 */
const { METRICS_SOURCE_VERSION } = require("./schema-v3");

async function upsertAnalysisBatchV3(client, uid, rows) {
  const now = Date.now();
  for (const r of rows) {
    await client.query(
      `INSERT INTO analysis_daily_snapshot (
         user_id, account_id, date, book_currency, source_version,
         daily_profit, daily_rate_twr, daily_external_flow, daily_cash_delta, tw_r_cumulative,
         market_value, total_assets, cash, cash_ratio, principal,
         fx_hkd_cny, fx_usd_cny,
         stage_mtd_profit, stage_mtd_rate_twr, stage_mtd_rate_mwr,
         stage_ytd_profit, stage_ytd_rate_twr, stage_ytd_rate_mwr,
         stage_inception_profit, stage_inception_rate_twr, stage_inception_rate_mwr,
         stage_last_7d_profit, stage_last_7d_rate_twr, stage_last_7d_rate_mwr,
         stage_last_30d_profit, stage_last_30d_rate_twr, stage_last_30d_rate_mwr,
         stage_last_90d_profit, stage_last_90d_rate_twr, stage_last_90d_rate_mwr,
         profit_cny, tw_r_daily, external_flow_cny, cash_cny,
         created_at, updated_at
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
         $18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,
         $36,$37,$38,$39,$40,$41
       )
       ON CONFLICT (user_id, account_id, date) DO UPDATE SET
         book_currency=EXCLUDED.book_currency, source_version=EXCLUDED.source_version,
         daily_profit=EXCLUDED.daily_profit, daily_rate_twr=EXCLUDED.daily_rate_twr,
         daily_external_flow=EXCLUDED.daily_external_flow, daily_cash_delta=EXCLUDED.daily_cash_delta,
         tw_r_cumulative=EXCLUDED.tw_r_cumulative,
         market_value=EXCLUDED.market_value, total_assets=EXCLUDED.total_assets,
         cash=EXCLUDED.cash, cash_ratio=EXCLUDED.cash_ratio, principal=EXCLUDED.principal,
         fx_hkd_cny=EXCLUDED.fx_hkd_cny, fx_usd_cny=EXCLUDED.fx_usd_cny,
         stage_mtd_profit=EXCLUDED.stage_mtd_profit, stage_mtd_rate_twr=EXCLUDED.stage_mtd_rate_twr, stage_mtd_rate_mwr=EXCLUDED.stage_mtd_rate_mwr,
         stage_ytd_profit=EXCLUDED.stage_ytd_profit, stage_ytd_rate_twr=EXCLUDED.stage_ytd_rate_twr, stage_ytd_rate_mwr=EXCLUDED.stage_ytd_rate_mwr,
         stage_inception_profit=EXCLUDED.stage_inception_profit, stage_inception_rate_twr=EXCLUDED.stage_inception_rate_twr, stage_inception_rate_mwr=EXCLUDED.stage_inception_rate_mwr,
         stage_last_7d_profit=EXCLUDED.stage_last_7d_profit, stage_last_7d_rate_twr=EXCLUDED.stage_last_7d_rate_twr, stage_last_7d_rate_mwr=EXCLUDED.stage_last_7d_rate_mwr,
         stage_last_30d_profit=EXCLUDED.stage_last_30d_profit, stage_last_30d_rate_twr=EXCLUDED.stage_last_30d_rate_twr, stage_last_30d_rate_mwr=EXCLUDED.stage_last_30d_rate_mwr,
         stage_last_90d_profit=EXCLUDED.stage_last_90d_profit, stage_last_90d_rate_twr=EXCLUDED.stage_last_90d_rate_twr, stage_last_90d_rate_mwr=EXCLUDED.stage_last_90d_rate_mwr,
         profit_cny=EXCLUDED.profit_cny, tw_r_daily=EXCLUDED.tw_r_daily,
         external_flow_cny=EXCLUDED.external_flow_cny, cash_cny=EXCLUDED.cash_cny,
         updated_at=EXCLUDED.updated_at`,
      [
        uid,
        r.accountId,
        r.date,
        r.bookCurrency,
        METRICS_SOURCE_VERSION,
        r.dailyProfit,
        r.dailyRateTwr,
        r.dailyExternalFlow,
        r.dailyCashDelta,
        r.twRCumulative,
        r.marketValue,
        r.totalAssets,
        r.cash,
        r.cashRatio,
        r.principal,
        r.fxHkdCny,
        r.fxUsdCny,
        r.stageMtdProfit,
        r.stageMtdRateTwr,
        r.stageMtdRateMwr,
        r.stageYtdProfit,
        r.stageYtdRateTwr,
        r.stageYtdRateMwr,
        r.stageInceptionProfit,
        r.stageInceptionRateTwr,
        r.stageInceptionRateMwr,
        r.stageLast7dProfit,
        r.stageLast7dRateTwr,
        r.stageLast7dRateMwr,
        r.stageLast30dProfit,
        r.stageLast30dRateTwr,
        r.stageLast30dRateMwr,
        r.stageLast90dProfit,
        r.stageLast90dRateTwr,
        r.stageLast90dRateMwr,
        r.dailyProfit,
        r.dailyRateTwr,
        r.dailyExternalFlow,
        r.cash,
        now,
        now,
      ],
    );
  }
}

async function upsertSymbolBatchV3(client, uid, rows) {
  const now = Date.now();
  for (const r of rows) {
    await client.query(
      `INSERT INTO symbol_daily_pnl (
         user_id, account_id, symbol, date, book_currency, source_version,
         daily_profit, daily_trade_qty, daily_trade_amount, daily_trade_flow, daily_rate_twr,
         eod_shares, eod_price, eod_market_value_native, position_weight,
         stage_mtd_profit, stage_mtd_rate_twr, stage_mtd_rate_mwr,
         stage_ytd_profit, stage_ytd_rate_twr, stage_ytd_rate_mwr,
         stage_inception_profit, stage_inception_rate_twr, stage_inception_rate_mwr,
         stage_last_7d_profit, stage_last_7d_rate_twr, stage_last_7d_rate_mwr,
         stage_last_30d_profit, stage_last_30d_rate_twr, stage_last_30d_rate_mwr,
         stage_last_90d_profit, stage_last_90d_rate_twr, stage_last_90d_rate_mwr,
         day_trade_qty, day_trade_amount, day_trade_flow_native, day_close_price, day_pnl_native, currency,
         created_at, updated_at
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
         $16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,
         $34,$35,$36,$37,$38,$39,$40,$41
       )
       ON CONFLICT (user_id, account_id, symbol, date) DO UPDATE SET
         book_currency=EXCLUDED.book_currency, source_version=EXCLUDED.source_version,
         currency=EXCLUDED.currency,
         daily_profit=EXCLUDED.daily_profit, daily_trade_qty=EXCLUDED.daily_trade_qty,
         daily_trade_amount=EXCLUDED.daily_trade_amount, daily_trade_flow=EXCLUDED.daily_trade_flow,
         daily_rate_twr=EXCLUDED.daily_rate_twr,
         eod_shares=EXCLUDED.eod_shares, eod_price=EXCLUDED.eod_price,
         eod_market_value_native=EXCLUDED.eod_market_value_native, position_weight=EXCLUDED.position_weight,
         stage_mtd_profit=EXCLUDED.stage_mtd_profit, stage_mtd_rate_twr=EXCLUDED.stage_mtd_rate_twr, stage_mtd_rate_mwr=EXCLUDED.stage_mtd_rate_mwr,
         stage_ytd_profit=EXCLUDED.stage_ytd_profit, stage_ytd_rate_twr=EXCLUDED.stage_ytd_rate_twr, stage_ytd_rate_mwr=EXCLUDED.stage_ytd_rate_mwr,
         stage_inception_profit=EXCLUDED.stage_inception_profit, stage_inception_rate_twr=EXCLUDED.stage_inception_rate_twr, stage_inception_rate_mwr=EXCLUDED.stage_inception_rate_mwr,
         stage_last_7d_profit=EXCLUDED.stage_last_7d_profit, stage_last_7d_rate_twr=EXCLUDED.stage_last_7d_rate_twr, stage_last_7d_rate_mwr=EXCLUDED.stage_last_7d_rate_mwr,
         stage_last_30d_profit=EXCLUDED.stage_last_30d_profit, stage_last_30d_rate_twr=EXCLUDED.stage_last_30d_rate_twr, stage_last_30d_rate_mwr=EXCLUDED.stage_last_30d_rate_mwr,
         stage_last_90d_profit=EXCLUDED.stage_last_90d_profit, stage_last_90d_rate_twr=EXCLUDED.stage_last_90d_rate_twr, stage_last_90d_rate_mwr=EXCLUDED.stage_last_90d_rate_mwr,
         day_pnl_native=EXCLUDED.daily_profit, day_trade_flow_native=EXCLUDED.daily_trade_flow,
         day_close_price=EXCLUDED.eod_price,
         updated_at=EXCLUDED.updated_at`,
      [
        uid,
        r.accountId,
        r.symbol,
        r.date,
        r.bookCurrency,
        METRICS_SOURCE_VERSION,
        r.dailyProfit,
        r.dailyTradeQty,
        r.dailyTradeAmount,
        r.dailyTradeFlow,
        r.dailyRateTwr,
        r.eodShares,
        r.eodPrice,
        r.eodMarketValueNative,
        r.positionWeight,
        r.stageMtdProfit,
        r.stageMtdRateTwr,
        r.stageMtdRateMwr,
        r.stageYtdProfit,
        r.stageYtdRateTwr,
        r.stageYtdRateMwr,
        r.stageInceptionProfit,
        r.stageInceptionRateTwr,
        r.stageInceptionRateMwr,
        r.stageLast7dProfit,
        r.stageLast7dRateTwr,
        r.stageLast7dRateMwr,
        r.stageLast30dProfit,
        r.stageLast30dRateTwr,
        r.stageLast30dRateMwr,
        r.stageLast90dProfit,
        r.stageLast90dRateTwr,
        r.stageLast90dRateMwr,
        r.dailyTradeQty,
        r.dailyTradeAmount,
        r.dailyTradeFlow,
        r.eodPrice,
        r.dailyProfit,
        r.currency,
        now,
        now,
      ],
    );
  }
}

module.exports = {
  upsertAnalysisBatchV3,
  upsertSymbolBatchV3,
};
