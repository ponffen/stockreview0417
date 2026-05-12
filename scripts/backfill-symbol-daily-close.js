#!/usr/bin/env node
/**
 * 按全体用户持仓生命周期规则回填 symbol_daily_close。
 * 用法：node scripts/backfill-symbol-daily-close.js
 */
const path = require("node:path");

const { runDailyCloseSync } = require(path.join(__dirname, "..", "src", "daily-close-sync-service"));

async function main() {
  const asOfDate = process.argv[2] ? String(process.argv[2]).slice(0, 10) : "";
  const result = await runDailyCloseSync({
    asOfDate: asOfDate || undefined,
    logger: console,
  });
  console.log(
    `[daily-close] asOf=${result.asOfDate} planned=${result.symbolsPlanned} synced=${result.symbolsSynced} skipped=${result.symbolsSkipped} failed=${result.symbolsFailed} rowsFetched=${result.rowsFetched} rowsWritten=${result.rowsWritten}`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
