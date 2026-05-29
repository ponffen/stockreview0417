#!/usr/bin/env node
/** 仅重算 symbol_daily_pnl（冻结日一行），保留 analysis_daily_snapshot */
const { getCliUserId } = require("../src/db");
const { runSymbolsOnlyV3ForUser } = require("../src/metrics/freeze-v3");
const { resolveFrozenDate } = require("../src/eod-freeze-service");

async function main() {
  const phoneArg = process.argv[2] || process.env.STOCKREVIEW_PHONE;
  if (phoneArg) {
    process.env.STOCKREVIEW_PHONE = String(phoneArg).trim();
  }
  const uid = await getCliUserId();
  const t0 = Date.now();
  const result = await runSymbolsOnlyV3ForUser(uid, {
    frozenDate: resolveFrozenDate(),
    syncDailyClose: true,
    logger: console,
  });
  console.log("[symbols-only]", JSON.stringify({ ...result, wallMs: Date.now() - t0 }, null, 2));
  if (!result.ok) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
