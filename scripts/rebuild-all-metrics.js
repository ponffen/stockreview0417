#!/usr/bin/env node
/**
 * 全量重算：从首笔成交日至昨日 freeze + home_summary。
 * 用法: DATABASE_URL=... node scripts/rebuild-all-metrics.js
 */
require("dotenv").config();
const { listAllUserIds, upsertUserMetricsMeta } = require("../src/db");
const { freezeUserToDate, resolveFrozenDate } = require("../src/eod-freeze-service");

async function rebuildUser(uid) {
  const end = resolveFrozenDate();
  console.log("[user]", uid, "->", end);
  await upsertUserMetricsMeta(uid, { rebuilding: true });
  const result = await freezeUserToDate(uid, end, { logger: console, force: true, syncDailyClose: false });
  if (result.skipped) {
    console.log("[skip]", uid, result.reason);
  }
  await upsertUserMetricsMeta(uid, {
    rebuilding: false,
    frozenThrough: end,
    dataVersion: 1,
  });
  console.log("[done]", uid, end);
}

async function main() {
  const userIds = await listAllUserIds();
  console.log("[rebuild-all-metrics] users=", userIds.length);
  for (const uid of userIds) {
    try {
      await rebuildUser(uid);
    } catch (e) {
      console.error("[fail]", uid, e?.message || e);
      await upsertUserMetricsMeta(uid, { rebuilding: false }).catch(() => {});
    }
  }
  console.log("[rebuild-all-metrics] finished");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
