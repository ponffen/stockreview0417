#!/usr/bin/env node
/**
 * 审计并回填 symbol_daily_close：自各标的首次持仓前一日至今，补齐远端行情有而本地缺的交易日。
 *
 * 用法：
 *   node scripts/audit-and-backfill-daily-close.js
 *   node scripts/audit-and-backfill-daily-close.js --audit-only
 *   node scripts/audit-and-backfill-daily-close.js --symbol hk00700
 */
require("dotenv").config();
const path = require("node:path");
const { toDateKey } = require(path.join(__dirname, "lib", "market-fetch"));
const {
  buildGlobalDailyClosePlan,
  auditDailyCloseGapsForPlan,
  runDailyCloseSync,
} = require(path.join(__dirname, "..", "src", "daily-close-sync-service"));

function parseArgs(argv) {
  const out = { auditOnly: false, symbols: [] };
  for (let i = 2; i < argv.length; i += 1) {
    const arg = String(argv[i] || "").trim();
    if (arg === "--audit-only") {
      out.auditOnly = true;
    } else if (arg === "--symbol" && argv[i + 1]) {
      out.symbols.push(String(argv[++i]).trim());
    }
  }
  return out;
}

function printAuditTable(title, audits) {
  console.log(`\n=== ${title} ===`);
  const withIssues = audits.filter((a) => a.missingCount > 0 || a.error);
  if (!withIssues.length) {
    console.log("无缺口。");
    return { symbolsWithGaps: 0, totalMissing: 0 };
  }
  let totalMissing = 0;
  for (const row of withIssues) {
    totalMissing += Number(row.missingCount) || 0;
    const ranges = (row.missingRanges || [])
      .slice(0, 3)
      .map((r) => `${r.from}~${r.to}(${r.count}d)`)
      .join(", ");
    console.log(
      [
        row.symbol,
        `local=${row.localCount}`,
        `remote=${row.remoteCount}`,
        `missing=${row.missingCount}`,
        row.error ? `error=${row.error}` : `ranges=${ranges || "—"}`,
      ].join(" | "),
    );
  }
  console.log(`合计：${withIssues.length} 只标的缺日 K，共 ${totalMissing} 个交易日。`);
  return { symbolsWithGaps: withIssues.length, totalMissing };
}

async function main() {
  const args = parseArgs(process.argv);
  const asOfDate = toDateKey(new Date());
  const plan = await buildGlobalDailyClosePlan(asOfDate);
  const targets =
    args.symbols.length > 0
      ? plan.filter((item) => args.symbols.includes(item.symbol))
      : plan;

  if (!targets.length) {
    console.error("未找到需处理的标的。");
    process.exit(1);
  }

  console.log(`[daily-close-audit] asOf=${asOfDate} symbols=${targets.length}`);
  const before = await auditDailyCloseGapsForPlan(targets, { onlyMissing: true });
  const beforeStats = printAuditTable("回填前缺口", before);

  if (args.auditOnly) {
    return;
  }

  console.log("\n[daily-close-audit] 开始回填…");
  const sync = await runDailyCloseSync({
    asOfDate,
    symbols: targets.map((t) => t.symbol),
    logger: console,
  });
  console.log(
    `[daily-close-audit] synced=${sync.symbolsSynced} skipped=${sync.symbolsSkipped} failed=${sync.symbolsFailed} rowsWritten=${sync.rowsWritten}`,
  );

  const after = await auditDailyCloseGapsForPlan(targets, { onlyMissing: true });
  const afterStats = printAuditTable("回填后缺口", after);

  const filled = Math.max(0, beforeStats.totalMissing - afterStats.totalMissing);
  console.log(
    `\n汇总：回填前缺 ${beforeStats.totalMissing} 日 / ${beforeStats.symbolsWithGaps} 标的；写入 ${sync.rowsWritten} 条；回填后仍缺 ${afterStats.totalMissing} 日 / ${afterStats.symbolsWithGaps} 标的；净补 ${filled} 日。`,
  );

  const failed = (sync.plan || []).filter((p) => p.reason === "failed");
  if (failed.length) {
    console.log("\n失败标的：");
    for (const row of failed) {
      console.log(`  ${row.symbol}: ${row.error || "unknown"}`);
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
