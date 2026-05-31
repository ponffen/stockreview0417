#!/usr/bin/env node
/**
 * Metrics v3 历史回填：analysis_daily_snapshot 全历史；symbol_daily_pnl 按日终仍有持仓逐日一行。
 * Usage: STOCKREVIEW_PHONE=18310270720 node scripts/backfill-daily-tables.js
 */
const path = require("node:path");

const { getCliUserId } = require(path.join(__dirname, "..", "src", "db"));
const { runFreezeV3ForUser } = require(path.join(__dirname, "..", "src", "metrics", "freeze-v3"));
const { resolveFrozenDate } = require(path.join(__dirname, "..", "src", "eod-freeze-service"));

async function main() {
  const phoneArg = process.argv[2] || process.env.STOCKREVIEW_PHONE;
  if (phoneArg) {
    process.env.STOCKREVIEW_PHONE = String(phoneArg).trim();
  }
  const uid = await getCliUserId();
  const frozenDate = resolveFrozenDate();
  console.log("[backfill-v3] user", uid, "phone", process.env.STOCKREVIEW_PHONE || "", "frozen", frozenDate);
  const t0 = Date.now();
  const result = await runFreezeV3ForUser(uid, {
    frozenDate,
    force: true,
    syncDailyClose: true,
    logger: console,
  });
  const wallMs = Date.now() - t0;
  console.log("[backfill-v3] done", JSON.stringify({ ...result, wallMs }, null, 2));
  if (!result.ok) {
    process.exit(1);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
