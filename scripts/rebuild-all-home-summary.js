#!/usr/bin/env node
/**
 * 为所有用户重算 account_home_summary / symbol_home_summary（需 DATABASE_URL）。
 */
process.env.VERCEL = process.env.VERCEL || "1";

const { listAllUserIds } = require("../src/db");
const { rebuildHomeSummaryForUser } = require("../src/home-summary-service");

async function main() {
  const ids = await listAllUserIds();
  let ok = 0;
  let skip = 0;
  let fail = 0;
  for (const uid of ids) {
    try {
      const r = await rebuildHomeSummaryForUser(uid);
      if (r?.ok) ok += 1;
      else if (r?.skip) skip += 1;
      else fail += 1;
    } catch (e) {
      console.error("[rebuild-home-summary]", uid, e?.message || e);
      fail += 1;
    }
  }
  console.log(JSON.stringify({ users: ids.length, ok, skip, fail }));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
