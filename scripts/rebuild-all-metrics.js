#!/usr/bin/env node
/**
 * 全量重算：从首笔成交日至昨日逐日 freeze + home_summary。
 * 用法: DATABASE_URL=... node scripts/rebuild-all-metrics.js
 */
require("dotenv").config();
const { listAllUserIds, getTrades, upsertUserMetricsMeta } = require("../src/db");
const { freezeUserToDate, resolveFrozenDate } = require("../src/eod-freeze-service");
const { rebuildHomeSummaryForUser } = require("../src/home-summary-service");
const { addCalendarDays } = require("../src/metrics/stages");

function enumerateDatesInclusive(start, end) {
  const out = [];
  let cur = String(start).slice(0, 10);
  const e = String(end).slice(0, 10);
  while (cur <= e) {
    out.push(cur);
    cur = addCalendarDays(cur, 1);
  }
  return out;
}

async function rebuildUser(uid) {
  const trades = await getTrades(uid);
  if (!trades.length) {
    console.log("[skip] no trades", uid);
    return;
  }
  const first = [...trades].sort((a, b) => String(a.date).localeCompare(String(b.date)))[0].date;
  const start = addCalendarDays(String(first).slice(0, 10), -1);
  const end = resolveFrozenDate();
  const dates = enumerateDatesInclusive(start, end);
  console.log("[user]", uid, "dates", dates.length, start, "->", end);
  await upsertUserMetricsMeta(uid, { rebuilding: true, rebuildFrom: start });
  for (const d of dates) {
    await freezeUserToDate(uid, d, { logger: console, force: true, syncDailyClose: false });
  }
  const hr = await rebuildHomeSummaryForUser(uid);
  await upsertUserMetricsMeta(uid, {
    rebuilding: false,
    frozenThrough: end,
    dataVersion: 1,
  });
  console.log("[done]", uid, hr?.frozenThrough || end);
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
