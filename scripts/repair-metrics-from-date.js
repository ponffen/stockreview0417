#!/usr/bin/env node
/**
 * 从指定交易日起修复指标：日 K → 日冻结 → amount_share_ratio。
 *
 * 用法:
 *   node scripts/repair-metrics-from-date.js 2026-07-15
 *   node scripts/repair-metrics-from-date.js 2026-07-15 2026-07-22
 */
require("dotenv").config();
const path = require("node:path");
const { spawn } = require("node:child_process");
const {
  resolveBatchMetricsUserIds,
  initPool,
  upsertUserMetricsMeta,
  closeDatabase,
  backfillTradeAmountShareRatiosForUser,
} = require(path.join(__dirname, "..", "src", "db"));
const { metricsUserScopeFromArgv } = require("./lib/metrics-user-scope");
const { previousSessionDate } = require(path.join(__dirname, "..", "src", "metrics", "freeze-calendar"));
const { freezeUserToDate, resolveFrozenDate } = require(path.join(__dirname, "..", "src", "eod-freeze-service"));
const { liveDateKeyShanghai } = require(path.join(__dirname, "..", "src", "metrics", "trading-calendar"));

function runNodeScript(scriptRel, args = []) {
  const scriptPath = path.join(__dirname, scriptRel);
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [scriptPath, ...args], {
      stdio: "inherit",
      env: process.env,
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`${scriptRel} exited with code ${code}`));
      }
    });
  });
}

async function refreezeAllUsers(fromDate) {
  const anchor = previousSessionDate(fromDate);
  const end = resolveFrozenDate();
  const pool = await initPool();
  const userIds = await resolveBatchMetricsUserIds({ allUsers: metricsUserScopeFromArgv() });
  console.log("[repair-freeze]", { fromDate, anchor, end, users: userIds.length });

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
      console.log("[freeze-done]", uid, { ...result, wallMs: Date.now() - t0 });
      if (!result.skipped && result.frozenDate) {
        await upsertUserMetricsMeta(uid, { frozenThrough: result.frozenDate, rebuilding: false });
      }
    } catch (error) {
      console.error("[freeze-fail]", uid, error?.message || error);
      await upsertUserMetricsMeta(uid, { rebuilding: false }).catch(() => {});
      throw error;
    }
  }
}

async function backfillAmountShareFromDate(fromDate) {
  const userIds = await resolveBatchMetricsUserIds({ allUsers: metricsUserScopeFromArgv() });
  let trades = 0;
  let updated = 0;
  let nullCount = 0;
  console.log("[repair-amount-share]", { fromDate, users: userIds.length });
  for (const uid of userIds) {
    const r = await backfillTradeAmountShareRatiosForUser(uid, { logger: console, fromDate });
    trades += r.trades;
    updated += r.updated;
    nullCount += r.nullCount;
  }
  console.log(
    JSON.stringify({
      step: "amount-share",
      fromDate,
      users: userIds.length,
      trades,
      updated,
      nullCount,
    }),
  );
}

async function main() {
  const fromDate = String(process.argv[2] || "").slice(0, 10);
  const toDate = String(process.argv[3] || liveDateKeyShanghai()).slice(0, 10);
  if (!fromDate || !toDate || fromDate > toDate) {
    console.error("Usage: node scripts/repair-metrics-from-date.js <from> [to]");
    process.exit(1);
  }

  console.log("[repair-metrics] start", { fromDate, toDate });
  await runNodeScript("backfill-daily-close-date-range.js", [fromDate, toDate]);
  await refreezeAllUsers(fromDate);
  await initPool();
  await backfillAmountShareFromDate(fromDate);
  await closeDatabase?.();
  console.log("[repair-metrics] finished", { fromDate, toDate });
}

main().catch(async (error) => {
  console.error(error);
  try {
    await closeDatabase?.();
  } catch {
    // ignore
  }
  process.exit(1);
});
