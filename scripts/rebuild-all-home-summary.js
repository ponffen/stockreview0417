#!/usr/bin/env node
/**
 * @deprecated 使用 metrics v3：node scripts/backfill-daily-tables.js
 */
const path = require("node:path");
const { listAllUserIds } = require(path.join(__dirname, "..", "src", "db"));
const { runFreezeV3ForUser } = require(path.join(__dirname, "..", "src", "metrics/freeze-v3"));
const { resolveFrozenDate } = require(path.join(__dirname, "..", "src", "eod-freeze-service"));

async function main() {
  const uids = await listAllUserIds();
  const frozenDate = resolveFrozenDate();
  for (const uid of uids) {
    try {
      const r = await runFreezeV3ForUser(uid, { frozenDate, force: true, syncDailyClose: true, logger: console });
      console.log("[rebuild-metrics-v3]", uid, r.skipped ? "skip" : "ok", r.frozenDate || r.reason);
    } catch (e) {
      console.error("[rebuild-metrics-v3]", uid, e?.message || e);
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
