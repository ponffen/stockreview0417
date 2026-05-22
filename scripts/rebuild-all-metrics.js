#!/usr/bin/env node
/**
 * 全量重算：从首笔成交日至昨日逐日 freeze + home_summary。
 * 用法:
 *   DATABASE_URL=... node scripts/rebuild-all-metrics.js
 *   DATABASE_URL=... REBUILD_USER_ID=<uuid> node scripts/rebuild-all-metrics.js
 */
require("dotenv").config();
const { listAllUserIds, getTrades, upsertUserMetricsMeta } = require("../src/db");
const { freezeUserToDate, resolveFrozenDate } = require("../src/eod-freeze-service");
const { rebuildHomeSummaryForUser } = require("../src/home-summary-service");
const { addCalendarDays } = require("../src/metrics/stages");

const USER_DELAY_MS = Math.max(0, Number(process.env.REBUILD_USER_DELAY_MS || 8000));
const FREEZE_RETRY_MAX = Math.max(1, Number(process.env.REBUILD_FREEZE_RETRY_MAX || 4));
const FREEZE_RETRY_BASE_MS = Math.max(500, Number(process.env.REBUILD_FREEZE_RETRY_BASE_MS || 2000));

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isTransientDbError(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return (
    msg.includes("conn crashed") ||
    msg.includes("connect failed") ||
    msg.includes("server login has been failing") ||
    msg.includes("connection terminated") ||
    msg.includes("econnreset") ||
    msg.includes("timeout")
  );
}

async function withRetries(label, fn) {
  let last;
  for (let i = 0; i < FREEZE_RETRY_MAX; i += 1) {
    try {
      return await fn();
    } catch (e) {
      last = e;
      if (!isTransientDbError(e) || i >= FREEZE_RETRY_MAX - 1) {
        throw e;
      }
      const wait = FREEZE_RETRY_BASE_MS * (i + 1);
      console.warn(`[retry] ${label} attempt=${i + 1} wait=${wait}ms`, e?.message || e);
      await sleep(wait);
    }
  }
  throw last;
}

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
  let done = 0;
  for (const d of dates) {
    await withRetries(`freeze ${uid} ${d}`, () =>
      freezeUserToDate(uid, d, { logger: console, force: true, syncDailyClose: false })
    );
    done += 1;
    if (done % 50 === 0) {
      console.log("[progress]", uid, `${done}/${dates.length}`, d);
    }
  }
  const hr = await withRetries(`home-summary ${uid}`, () => rebuildHomeSummaryForUser(uid));
  await upsertUserMetricsMeta(uid, {
    rebuilding: false,
    frozenThrough: end,
    dataVersion: 1,
  });
  console.log("[done]", uid, hr?.frozenThrough || end);
}

async function main() {
  const only = String(process.env.REBUILD_USER_ID || "").trim();
  let userIds = await listAllUserIds();
  if (only) {
    userIds = userIds.filter((id) => id === only);
    if (!userIds.length) {
      console.error("[rebuild-all-metrics] user not found:", only);
      process.exit(1);
    }
  }
  console.log("[rebuild-all-metrics] users=", userIds.length, only ? `(only ${only})` : "");
  for (let i = 0; i < userIds.length; i += 1) {
    const uid = userIds[i];
    try {
      await rebuildUser(uid);
    } catch (e) {
      console.error("[fail]", uid, e?.message || e);
      await upsertUserMetricsMeta(uid, { rebuilding: false }).catch(() => {});
    }
    if (i < userIds.length - 1 && USER_DELAY_MS > 0) {
      console.log("[rebuild-all-metrics] pause", USER_DELAY_MS, "ms before next user");
      await sleep(USER_DELAY_MS);
    }
  }
  console.log("[rebuild-all-metrics] finished");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
