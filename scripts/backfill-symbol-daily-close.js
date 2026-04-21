#!/usr/bin/env node
/**
 * 按成交区间从东财+新浪回填日收盘价到 symbol_daily_close。
 * 用法：node scripts/backfill-symbol-daily-close.js
 */
const path = require("node:path");

const {
  getTradeWindowForDailyClose,
  upsertSymbolDailyCloseBatch,
} = require(path.join(__dirname, "..", "src", "db"));
const { fetchRemoteDailyClosesForSymbol } = require(path.join(__dirname, "..", "src", "daily-close-backfill"));

async function main() {
  const w = getTradeWindowForDailyClose();
  if (!w.symbols.length) {
    console.log("[daily-close] 无成交，退出。");
    process.exit(0);
  }
  console.log(`[daily-close] 区间 ${w.from} ~ ${w.to}，共 ${w.symbols.length} 个标的`);
  const counts = {};
  for (let i = 0; i < w.symbols.length; i += 1) {
    const sym = w.symbols[i];
    process.stdout.write(`  [${i + 1}/${w.symbols.length}] ${sym} … `);
    try {
      const rows = await fetchRemoteDailyClosesForSymbol(sym, w.from, w.to);
      upsertSymbolDailyCloseBatch(rows);
      counts[sym] = rows.length;
      console.log(rows.length, "行");
    } catch (e) {
      console.log("失败", e.message || e);
      counts[sym] = 0;
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  console.log("[daily-close] 完成", counts);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
