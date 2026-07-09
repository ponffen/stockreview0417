#!/usr/bin/env node
/**
 * 强制重拉指定日期区间的日 K 并 upsert 到 symbol_daily_close。
 * 范围：全站曾持仓标的 + 外汇（buildGlobalDailyClosePlan）。
 *
 * 用法:
 *   node scripts/backfill-daily-close-date-range.js 2026-07-07 2026-07-08
 */
require("dotenv").config();
const path = require("node:path");
const { buildGlobalDailyClosePlan } = require(path.join(__dirname, "..", "src", "daily-close-sync-service"));
const { fetchRemoteDailyClosesForSymbol } = require(path.join(__dirname, "..", "src", "daily-close-backfill"));
const {
  initPool,
  upsertSymbolDailyCloseBatch,
  closeDatabase,
  normalizeSymbol,
} = require(path.join(__dirname, "..", "src", "db"));

const THROTTLE_MS = 120;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  const fromDate = String(process.argv[2] || "").slice(0, 10);
  const toDate = String(process.argv[3] || "").slice(0, 10);
  if (!fromDate || !toDate || fromDate > toDate) {
    console.error("Usage: node scripts/backfill-daily-close-date-range.js <from> <to>");
    process.exit(1);
  }

  await initPool();
  const plan = await buildGlobalDailyClosePlan(toDate);
  const symbols = [...new Set(plan.map((item) => normalizeSymbol(item.symbol)).filter(Boolean))].sort();
  console.log(`[daily-close-range] from=${fromDate} to=${toDate} symbols=${symbols.length}`);

  let rowsFetched = 0;
  let rowsWritten = 0;
  let symbolsOk = 0;
  let symbolsEmpty = 0;
  let symbolsFailed = 0;

  for (let i = 0; i < symbols.length; i += 1) {
    const sym = symbols[i];
    try {
      const rows = await fetchRemoteDailyClosesForSymbol(sym, fromDate, toDate);
      const filtered = (rows || []).filter((row) => {
        const d = String(row.date || "").slice(0, 10);
        return d >= fromDate && d <= toDate && Number.isFinite(Number(row.close)) && Number(row.close) > 0;
      });
      rowsFetched += filtered.length;
      if (!filtered.length) {
        symbolsEmpty += 1;
        console.warn(`[skip-empty] ${sym}`);
      } else {
        const written = await upsertSymbolDailyCloseBatch(
          filtered.map((row) => ({
            symbol: sym,
            date: String(row.date).slice(0, 10),
            close: Number(row.close),
            source: String(row.source || "sina"),
          })),
        );
        rowsWritten += written;
        symbolsOk += 1;
        console.log(
          `[ok] ${sym} (${i + 1}/${symbols.length})`,
          filtered.map((row) => `${row.date}:${row.close}`).join(", "),
        );
      }
    } catch (error) {
      symbolsFailed += 1;
      console.error(`[fail] ${sym}`, error?.message || error);
    }
    if (i < symbols.length - 1) {
      await sleep(THROTTLE_MS);
    }
  }

  console.log(
    JSON.stringify({
      fromDate,
      toDate,
      symbolsPlanned: symbols.length,
      symbolsOk,
      symbolsEmpty,
      symbolsFailed,
      rowsFetched,
      rowsWritten,
    }),
  );
  await closeDatabase?.();
}

main().catch(async (error) => {
  console.error(error);
  try {
    const { closeDatabase } = require(path.join(__dirname, "..", "src", "db"));
    await closeDatabase?.();
  } catch {
    // ignore
  }
  process.exit(1);
});
