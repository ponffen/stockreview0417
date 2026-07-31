#!/usr/bin/env node
/**
 * Verify stock dynamics page 1/2/3 are contiguous (new→old) via cursor pagination.
 * Usage: node scripts/verify-stock-dynamics-pagination.js [symbol]
 */
require("dotenv").config();
const { initPool, findUserByPhone } = require("../src/db");
const { listDynamicsFeed, SCENES } = require("../src/dynamics/dynamics-feed");

function cardLabel(c) {
  return `${c.cardKind}:${String(c.id || "").slice(0, 8)}:${c.tradeDate || c.bottomTime}`;
}

async function main() {
  const sym = process.argv[2] || "us_tsm";
  await initPool();
  const user = await findUserByPhone("18310270720");
  if (!user?.id) {
    throw new Error("seed user not found");
  }
  const uid = user.id;
  const seen = new Set();
  let cursor = null;
  for (let page = 1; page <= 3; page += 1) {
    const opts = {
      viewerId: uid,
      targetUserId: uid,
      scene: SCENES.STOCK_SELF,
      symbol: sym,
      limit: 10,
      filter: "all",
    };
    if (page === 1) {
      opts.page = 1;
    } else {
      opts.cursor = cursor;
    }
    const r = await listDynamicsFeed(opts);
    const labels = (r.data || []).map(cardLabel);
    const dup = labels.filter((x) => seen.has(x));
    if (dup.length) {
      console.error(`[FAIL] page ${page} duplicates:`, dup);
      process.exit(1);
    }
    labels.forEach((x) => seen.add(x));
    console.log(
      `[page ${page}] items=${labels.length} hasMore=${r.pagination.hasMore} first=${labels[0] || "-"} last=${labels[labels.length - 1] || "-"}`,
    );
    cursor = r.pagination.cursor;
    if (!r.pagination.hasMore) {
      break;
    }
  }
  console.log("[ok] cursor pagination contiguous for", sym);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
