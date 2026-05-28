#!/usr/bin/env node
/**
 * 历史回填：清空当前 CLI 用户的 symbol_daily_pnl、analysis_daily_snapshot 后，
 * 调用与线上相同的日终冻结逻辑（TWR/出入金、业绩缓存）。
 * 需 DATABASE_URL；默认用户见 STOCKREVIEW_PHONE / db.getCliUserId。
 */
const path = require("node:path");

const {
  deleteAllSymbolDailyPnl,
  deleteAllAnalysisDailySnapshot,
  getCliUserId,
  ensurePerformanceSchemaV2,
} = require(path.join(__dirname, "..", "src", "db"));
const { runDailyFreeze, resolveFrozenDate } = require(path.join(__dirname, "..", "src", "eod-freeze-service"));
const { deletePerformanceSeriesForUser } = require(path.join(__dirname, "..", "src", "performance-cache-service"));

async function main() {
  await ensurePerformanceSchemaV2();
  const phoneArg = process.argv[2] || process.env.STOCKREVIEW_PHONE;
  if (phoneArg) {
    process.env.STOCKREVIEW_PHONE = String(phoneArg).trim();
  }
  const uid = await getCliUserId();
  console.log("[backfill] user", uid, "phone", process.env.STOCKREVIEW_PHONE || "(cli default)");
  await deletePerformanceSeriesForUser(uid);
  await deleteAllSymbolDailyPnl(uid);
  await deleteAllAnalysisDailySnapshot(uid);
  const frozenDate = resolveFrozenDate();
  const data = await runDailyFreeze({
    frozenDate,
    force: true,
    syncDailyClose: true,
    userIds: [uid],
    logger: console,
  });
  console.log("[backfill] done", JSON.stringify(data, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
