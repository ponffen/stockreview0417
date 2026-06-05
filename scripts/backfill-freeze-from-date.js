#!/usr/bin/env node
/**
 * 从指定交易日起重跑增量冻结（analysis_daily_snapshot + symbol_daily_pnl）。
 * 用法: DATABASE_URL=... node scripts/backfill-freeze-from-date.js 2026-06-02
 */
require("dotenv").config();
const {
  listAllUserIds,
  initPool,
  upsertUserMetricsMeta,
  closeDatabase,
} = require("../src/db");
const { previousSessionDate } = require("../src/metrics/freeze-calendar");
const { freezeUserToDate, resolveFrozenDate } = require("../src/eod-freeze-service");

async function main() {
  const fromDate = String(process.argv[2] || "2026-06-02").slice(0, 10);
  const anchor = previousSessionDate(fromDate);
  const end = resolveFrozenDate();
  const pool = await initPool();

  const userIds = process.argv[3]
    ? [String(process.argv[3]).trim()]
    : await listAllUserIds();

  console.log("[backfill-freeze]", { fromDate, anchor, end, users: userIds.length });

  for (const uid of userIds) {
    const t0 = Date.now();
    try {
      await pool.query(
        "DELETE FROM analysis_daily_snapshot WHERE user_id = $1 AND date >= $2",
        [uid, fromDate],
      );
      await pool.query("DELETE FROM symbol_daily_pnl WHERE user_id = $1 AND date >= $2", [
        uid,
        fromDate,
      ]);
      if (anchor) {
        await upsertUserMetricsMeta(uid, { frozenThrough: anchor });
      }
      const result = await freezeUserToDate(uid, end, {
        logger: console,
        force: false,
        syncDailyClose: false,
      });
      console.log("[done]", uid, { ...result, wallMs: Date.now() - t0 });
      if (!result.skipped && result.frozenDate) {
        await upsertUserMetricsMeta(uid, { frozenThrough: result.frozenDate, rebuilding: false });
      }
    } catch (e) {
      console.error("[fail]", uid, e?.message || e);
      await upsertUserMetricsMeta(uid, { rebuilding: false }).catch(() => {});
    }
  }

  await closeDatabase?.();
  console.log("[backfill-freeze] finished");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
