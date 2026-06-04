#!/usr/bin/env node
/**
 * 全量回填 trades.amount_share_ratio（按每条成交 trade_date 历史时点快照）。
 * 用法：DATABASE_URL=... node scripts/backfill-trade-amount-share-ratio.js
 */
const path = require("node:path");

const { computeTradeAmountShareRatio } = require(path.join(
  __dirname,
  "..",
  "src",
  "trade-amount-share-ratio.js"
));

async function main() {
  const db = require(path.join(__dirname, "..", "src", "db"));
  await db.initPool();
  await db.ensurePerformanceSchemaV2();

  const trades = await db.listTradesForAmountShareBackfill();
  const snapCache = new Map();
  let updated = 0;
  let nullCount = 0;

  async function snapshotFor(userId, asOfDate) {
    const key = `${userId}|${asOfDate}`;
    if (snapCache.has(key)) {
      return snapCache.get(key);
    }
    const snap = await db.selectAnalysisSnapshotAllAccountOnOrBefore(userId, asOfDate);
    snapCache.set(key, snap);
    return snap;
  }

  for (const row of trades) {
    const uid = String(row.user_id || "").trim();
    const asOf = String(row.trade_date || "").slice(0, 10);
    let ratio = null;
    if (uid && asOf) {
      const snap = await snapshotFor(uid, asOf);
      if (snap) {
        ratio = computeTradeAmountShareRatio({
          amount: Number(row.amount),
          symbol: row.symbol,
          totalAssetsCny: snap.totalAssets,
          fxUsdCny: snap.fxUsdCny,
          fxHkdCny: snap.fxHkdCny,
        });
      }
    }
    if (ratio == null) {
      nullCount += 1;
    } else {
      updated += 1;
    }
    await db.setTradeAmountShareRatio(row.id, ratio);
  }

  console.log(
    `[backfill-trade-amount-share-ratio] trades=${trades.length} withRatio=${updated} null=${nullCount} snapCacheKeys=${snapCache.size}`
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
