/**
 * 按当前代码全量重算所有用户的日快照、业绩缓存与首页汇总（需 DATABASE_URL）。
 * 用法: node scripts/recompute-all-analysis-snapshots.js
 * 可选: STOCKREVIEW_FORCE_FREEZE=1（默认已 force）
 */
require("dotenv").config();
const { listAllUserIds } = require("../src/db");
const { freezeUserToDate, resolveFrozenDate } = require("../src/eod-freeze-service");

async function main() {
  const frozenDate = resolveFrozenDate();
  const userIds = await listAllUserIds();
  console.log("[recompute] users=", userIds.length, "frozenDate=", frozenDate);
  let ok = 0;
  let skipped = 0;
  for (const uid of userIds) {
    try {
      const r = await freezeUserToDate(uid, frozenDate, { force: true, syncDailyClose: false, logger: console });
      if (r.skipped) {
        skipped += 1;
        console.log("[recompute] skip", uid, r.reason || r);
      } else {
        ok += 1;
        console.log("[recompute] ok", uid, "analysisRows=", r.analysisRowsWritten);
      }
    } catch (e) {
      console.error("[recompute] fail", uid, e?.message || e);
    }
  }
  console.log("[recompute] done ok=", ok, "skipped=", skipped);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
