/**
 * metrics v3 DDL：analysis_daily_snapshot / symbol_daily_pnl 扩展列。
 */
const { runSchemaDdl } = require("../db");

const METRICS_SOURCE_VERSION = "3";

const ANALYSIS_STAGE_COLS = [
  "stage_mtd_profit",
  "stage_mtd_rate_twr",
  "stage_mtd_rate_mwr",
  "stage_ytd_profit",
  "stage_ytd_rate_twr",
  "stage_ytd_rate_mwr",
  "stage_inception_profit",
  "stage_inception_rate_twr",
  "stage_inception_rate_mwr",
  "stage_last_7d_profit",
  "stage_last_7d_rate_twr",
  "stage_last_7d_rate_mwr",
  "stage_last_30d_profit",
  "stage_last_30d_rate_twr",
  "stage_last_30d_rate_mwr",
  "stage_last_90d_profit",
  "stage_last_90d_rate_twr",
  "stage_last_90d_rate_mwr",
];

let schemaV3Promise = null;

async function ensureMetricsSchemaV3() {
  if (schemaV3Promise) {
    return schemaV3Promise;
  }
  schemaV3Promise = (async () => {
    const { ensurePerformanceSchemaV2 } = require("../db");
    await ensurePerformanceSchemaV2();

    const analysisAdds = [
      `book_currency TEXT NOT NULL DEFAULT 'CNY'`,
      `source_version TEXT NOT NULL DEFAULT '3'`,
      `daily_profit DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `daily_rate_twr DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `daily_external_flow DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `daily_cash_delta DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `cash DOUBLE PRECISION NOT NULL DEFAULT 0`,
      ...ANALYSIS_STAGE_COLS.map((c) => `${c} DOUBLE PRECISION NOT NULL DEFAULT 0`),
    ];
    for (const def of analysisAdds) {
      const col = def.split(" ")[0];
      await runSchemaDdl(`ALTER TABLE analysis_daily_snapshot ADD COLUMN IF NOT EXISTS ${def}`).catch(() => {});
    }

    const symbolAdds = [
      `book_currency TEXT NOT NULL DEFAULT 'CNY'`,
      `source_version TEXT NOT NULL DEFAULT '3'`,
      `daily_profit DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `daily_trade_qty DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `daily_trade_amount DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `daily_trade_flow DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `daily_rate_twr DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `eod_price DOUBLE PRECISION`,
      `eod_market_value_native DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `position_weight DOUBLE PRECISION NOT NULL DEFAULT 0`,
      ...ANALYSIS_STAGE_COLS.map((c) => `${c} DOUBLE PRECISION NOT NULL DEFAULT 0`),
    ];
    for (const def of symbolAdds) {
      await runSchemaDdl(`ALTER TABLE symbol_daily_pnl ADD COLUMN IF NOT EXISTS ${def}`).catch(() => {});
    }

    await runSchemaDdl(`
      UPDATE analysis_daily_snapshot SET
        daily_profit = COALESCE(NULLIF(daily_profit, 0), profit_cny),
        daily_rate_twr = COALESCE(NULLIF(daily_rate_twr, 0), tw_r_daily),
        daily_external_flow = COALESCE(NULLIF(daily_external_flow, 0), external_flow_cny),
        cash = COALESCE(NULLIF(cash, 0), cash_cny)
      WHERE daily_profit = 0 AND profit_cny <> 0
    `).catch(() => {});

    await runSchemaDdl(`
      UPDATE symbol_daily_pnl SET
        daily_profit = COALESCE(NULLIF(daily_profit, 0), day_pnl_native),
        daily_trade_flow = COALESCE(NULLIF(daily_trade_flow, 0), day_trade_flow_native),
        daily_trade_qty = COALESCE(NULLIF(daily_trade_qty, 0), day_trade_qty),
        daily_trade_amount = COALESCE(NULLIF(daily_trade_amount, 0), day_trade_amount),
        eod_price = COALESCE(eod_price, day_close_price)
      WHERE daily_profit = 0 AND day_pnl_native <> 0
    `).catch(() => {});
  })();
  return schemaV3Promise;
}

module.exports = {
  ensureMetricsSchemaV3,
  METRICS_SOURCE_VERSION,
  ANALYSIS_STAGE_COLS,
};
