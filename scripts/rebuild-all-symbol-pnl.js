#!/usr/bin/env node
/**
 * 全用户重算 symbol_daily_pnl（全历史逐日 + book_currency / 新列），保留 analysis_daily_snapshot。
 * Usage: DATABASE_URL=... node scripts/rebuild-all-symbol-pnl.js
 */
require("dotenv").config();
const { listAllUserIds } = require("../src/db");
const { runSymbolsFullRebuildForUser } = require("../src/metrics/freeze-v3");
const { resolveFrozenDate } = require("../src/eod-freeze-service");

async function rebuildUser(uid) {
  const frozenDate = resolveFrozenDate();
  console.log("[symbol-pnl]", uid, "frozen", frozenDate);
  const t0 = Date.now();
  const result = await runSymbolsFullRebuildForUser(uid, {
    frozenDate,
    syncDailyClose: true,
    logger: console,
  });
  console.log("[symbol-pnl-done]", uid, JSON.stringify({ ...result, wallMs: Date.now() - t0 }));
  if (!result.ok) {
    throw new Error(result.reason || "rebuild failed");
  }
}

async function main() {
  const userIds = await listAllUserIds();
  console.log("[rebuild-all-symbol-pnl] users=", userIds.length);
  let ok = 0;
  let fail = 0;
  for (const uid of userIds) {
    try {
      await rebuildUser(uid);
      ok += 1;
    } catch (e) {
      fail += 1;
      console.error("[fail]", uid, e?.message || e);
    }
  }
  console.log("[rebuild-all-symbol-pnl] finished ok=", ok, "fail=", fail);
  if (fail > 0) {
    process.exit(1);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
