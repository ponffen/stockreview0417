#!/usr/bin/env node
/** 仅执行 metrics v3 DDL（不改数据逻辑） */
const { ensureMetricsSchemaV3 } = require("../src/metrics/schema-v3");

async function main() {
  await ensureMetricsSchemaV3();
  console.log("[migrate] metrics schema v3 ok");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
