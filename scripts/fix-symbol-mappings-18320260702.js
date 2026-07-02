#!/usr/bin/env node
/**
 * Fix wrong symbol mappings for phone 18320260702.
 * Usage: node scripts/fix-symbol-mappings-18320260702.js [--dry-run]
 */
require("dotenv").config();
const path = require("node:path");
const {
  initPool,
  findUserByPhone,
  upsertSymbolNameMapBatch,
  backfillTradeAmountShareRatiosForUser,
  upsertSymbolDailyCloseBatch,
  getTrades,
  closeDatabase,
} = require("../src/db");
const { runFreezeV3ForUser } = require(path.join(__dirname, "..", "src", "metrics", "freeze-v3"));
const { resolveFrozenDate } = require(path.join(__dirname, "..", "src", "eod-freeze-service"));
const { fetchRemoteDailyClosesForSymbol } = require(path.join(__dirname, "..", "src", "daily-close-backfill"));

const PHONE = "18320260702";

/** oldSymbol → { symbol, nameCn } */
const SYMBOL_FIXES = [
  { oldSymbol: "cienn", symbol: "cohr", nameCn: "相干" },
  { oldSymbol: "hk07750", symbol: "hk07709", nameCn: "XL二南方海力士" },
  { oldSymbol: "spacex", symbol: "spcx", nameCn: "SpaceX" },
  { oldSymbol: "hk02513", symbol: "hk02513", nameCn: "智谱" },
];

const STALE_MAP_SYMBOLS = ["cienn", "hk07750", "spacex"];

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const user = await findUserByPhone(PHONE);
  if (!user?.id) {
    throw new Error(`User not found: ${PHONE}`);
  }
  const uid = user.id;
  const pool = await initPool();
  const nowMs = Date.now();

  console.log(`[fix-symbols] user=${uid} phone=${PHONE} dryRun=${dryRun}`);

  for (const fix of SYMBOL_FIXES) {
    if (fix.oldSymbol === fix.symbol) {
      const { rows } = await pool.query(
        `SELECT COUNT(*)::int c FROM trades WHERE user_id = $1 AND symbol = $2`,
        [uid, fix.symbol],
      );
      console.log(`[trades] ${fix.symbol} name→${fix.nameCn} (${rows[0].c} rows, name only)`);
      if (!dryRun && rows[0].c > 0) {
        await pool.query(`UPDATE trades SET name = $1, updated_at = $2 WHERE user_id = $3 AND symbol = $4`, [
          fix.nameCn,
          nowMs,
          uid,
          fix.symbol,
        ]);
      }
      continue;
    }
    const { rows } = await pool.query(
      `SELECT COUNT(*)::int c FROM trades WHERE user_id = $1 AND symbol = $2`,
      [uid, fix.oldSymbol],
    );
    console.log(`[trades] ${fix.oldSymbol} → ${fix.symbol} name=${fix.nameCn} (${rows[0].c} rows)`);
    if (!dryRun && rows[0].c > 0) {
      await pool.query(
        `UPDATE trades SET symbol = $1, name = $2, updated_at = $3 WHERE user_id = $4 AND symbol = $5`,
        [fix.symbol, fix.nameCn, nowMs, uid, fix.oldSymbol],
      );
    }
  }

  if (!dryRun) {
    await upsertSymbolNameMapBatch(
      SYMBOL_FIXES.map((f) => ({ symbol: f.symbol, nameCn: f.nameCn, source: "import-fix" })),
    );
    for (const stale of STALE_MAP_SYMBOLS) {
      await pool.query(`DELETE FROM symbol_name_map WHERE symbol = $1`, [stale]);
    }
    console.log("[symbol_name_map] upserted fixes, removed stale:", STALE_MAP_SYMBOLS.join(", "));
  }

  const frozenDate = resolveFrozenDate();
  const trades = await getTrades(uid);
  const symbolsToSync = [...new Set(SYMBOL_FIXES.map((f) => f.symbol))];
  const minDate = trades.map((t) => t.date).sort()[0] || "2026-01-01";

  for (const sym of symbolsToSync) {
    console.log(`[kline] fetching ${sym} ${minDate}..${frozenDate}`);
    if (dryRun) {
      continue;
    }
    const rows = await fetchRemoteDailyClosesForSymbol(sym, minDate, frozenDate);
    if (rows.length) {
      await upsertSymbolDailyCloseBatch(
        rows.map((r) => ({ symbol: sym, date: r.date, close: r.close, source: r.source || "sina" })),
      );
    }
    console.log(`[kline] ${sym} written=${rows.length}`);
  }

  if (dryRun) {
    console.log("[dry-run] skip freeze + amountShareRatio");
    return;
  }

  console.log("[freeze] full rebuild...");
  const freezeResult = await runFreezeV3ForUser(uid, {
    frozenDate,
    force: true,
    fullRebuild: true,
    syncDailyClose: true,
    logger: console,
  });
  console.log("[freeze] done", JSON.stringify(freezeResult, null, 2));

  const ratioResult = await backfillTradeAmountShareRatiosForUser(uid, { logger: console });
  console.log("[amountShareRatio]", ratioResult);

  await closeDatabase?.();
}

main().catch(async (e) => {
  console.error(e);
  try {
    await closeDatabase?.();
  } catch {
    // ignore
  }
  process.exit(1);
});
