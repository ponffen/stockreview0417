#!/usr/bin/env node
/**
 * 全量重算：日终快照 + 业绩缓存（需 DATABASE_URL；建议 CRON_SECRET 调 /api/cron/freeze-eod?force=true 等价）。
 * VERCEL=1 node scripts/rebuild-all-performance.js
 */
const path = require("node:path");

process.env.VERCEL = process.env.VERCEL || "1";

const { runDailyFreeze } = require(path.join(__dirname, "..", "src", "eod-freeze-service"));

async function main() {
  const data = await runDailyFreeze({
    force: true,
    syncDailyClose: true,
    logger: console,
  });
  // eslint-disable-next-line no-console
  console.log(JSON.stringify(data, null, 2));
}

main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(e);
  process.exit(1);
});
