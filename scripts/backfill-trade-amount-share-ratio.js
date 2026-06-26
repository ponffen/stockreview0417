#!/usr/bin/env node
/**
 * 全量回填 trades.amount_share_ratio（按每条成交 trade_date 历史时点快照）。
 * 用法：DATABASE_URL=... node scripts/backfill-trade-amount-share-ratio.js
 *
 * 与日冻结（增量 / 全量重算）共用 db.backfillTradeAmountShareRatiosForUser，
 * 口径一致：account_id='all'、成交日当天或之前最近一个冻结日的总资产；仅写 amount_share_ratio。
 */
const path = require("node:path");

async function main() {
  const db = require(path.join(__dirname, "..", "src", "db"));
  await db.initPool();
  await db.ensurePerformanceSchemaV2();

  const userIds = await db.listAllUserIds();
  let trades = 0;
  let updated = 0;
  let nullCount = 0;
  for (const uid of userIds) {
    const r = await db.backfillTradeAmountShareRatiosForUser(uid, { logger: console });
    trades += r.trades;
    updated += r.updated;
    nullCount += r.nullCount;
  }

  console.log(
    `[backfill-trade-amount-share-ratio] users=${userIds.length} trades=${trades} withRatio=${updated} null=${nullCount}`,
  );
  await db.closeDatabase();
}

main().catch(async (e) => {
  console.error(e);
  try {
    const db = require(path.join(__dirname, "..", "src", "db"));
    await db.closeDatabase();
  } catch {
    // ignore
  }
  process.exit(1);
});
