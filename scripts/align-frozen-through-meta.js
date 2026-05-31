#!/usr/bin/env node
/**
 * 将 user_metrics_meta.frozen_through 对齐为最后一条 analysis 快照日（修复周六 meta 超前）。
 * 用法: DATABASE_URL=... node scripts/align-frozen-through-meta.js
 */
require("dotenv").config();
const {
  initPool,
  listAllUserIds,
  getUserMetricsMeta,
  upsertUserMetricsMeta,
  getLatestAnalysisSnapshotDate,
} = require("../src/db");
const { capFrozenThroughToSnapshot } = require("../src/metrics/freeze-calendar");

async function main() {
  await initPool();
  const userIds = await listAllUserIds();
  for (const uid of userIds) {
    const meta = await getUserMetricsMeta(uid, { light: true });
    const latest = await getLatestAnalysisSnapshotDate(uid, "all");
    if (!latest) {
      console.log("[skip]", uid, "no-snapshot");
      continue;
    }
    const aligned = capFrozenThroughToSnapshot(meta.frozenThrough, latest) || latest;
    if (aligned === meta.frozenThrough) {
      console.log("[ok]", uid, aligned);
      continue;
    }
    await upsertUserMetricsMeta(uid, { frozenThrough: aligned });
    console.log("[fix]", uid, meta.frozenThrough, "->", aligned);
  }
  console.log("[align-frozen-through-meta] done");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
