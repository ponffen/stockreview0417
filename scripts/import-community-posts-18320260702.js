#!/usr/bin/env node
/**
 * One-off import opinion posts for phone 18320260702.
 * Usage:
 *   node scripts/import-community-posts-18320260702.js --dry-run
 *   node scripts/import-community-posts-18320260702.js --import
 */
require("dotenv").config();
const fs = require("node:fs");
const path = require("node:path");
const { findUserByPhone, initPool, dbQuery, closeDatabase } = require("../src/db");

const PHONE = "18320260702";
const ID_PREFIX = "import-post-18320260702";
const TSV_PATH = path.join(__dirname, "../data/import-posts-18320260702.tsv");

function parseTsvRows(raw) {
  const rows = [];
  for (const line of String(raw || "").split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }
    const tab = trimmed.indexOf("\t");
    if (tab <= 0) {
      throw new Error(`invalid line (missing tab): ${trimmed.slice(0, 80)}`);
    }
    const date = trimmed.slice(0, tab).trim();
    const content = trimmed.slice(tab + 1).trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      throw new Error(`invalid date: ${date}`);
    }
    if (!content) {
      throw new Error(`empty content for date ${date}`);
    }
    rows.push({ date, content });
  }
  return rows;
}

function assignSameDaySeq(rows) {
  const countByDate = new Map();
  return rows.map((row) => {
    const n = (countByDate.get(row.date) || 0) + 1;
    countByDate.set(row.date, n);
    return { ...row, seq: n };
  });
}

function shanghaiEndOfDayMs(dateStr, seq, totalOnDay) {
  const m = String(dateStr).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) {
    throw new Error(`bad date ${dateStr}`);
  }
  const offsetSec = totalOnDay > 1 ? totalOnDay - seq : 0;
  const sec = 59 - offsetSec;
  const ms = 0;
  const iso = `${m[1]}-${m[2]}-${m[3]}T23:59:${String(sec).padStart(2, "0")}.${String(ms).padStart(3, "0")}+08:00`;
  return new Date(iso).getTime();
}

function buildImportRows(parsed) {
  const withSeq = assignSameDaySeq(parsed);
  const totalByDate = new Map();
  for (const row of withSeq) {
    totalByDate.set(row.date, (totalByDate.get(row.date) || 0) + 1);
  }
  return withSeq.map((row) => {
    const totalOnDay = totalByDate.get(row.date) || 1;
    const createdAt = shanghaiEndOfDayMs(row.date, row.seq, totalOnDay);
    const id = `${ID_PREFIX}-${row.date.replace(/-/g, "")}-${String(row.seq).padStart(2, "0")}`;
    return {
      id,
      content: row.content,
      createdAt,
      updatedAt: createdAt,
      symbols: "[]",
      imageUrls: "[]",
    };
  });
}

async function importRows(userId, rows) {
  let inserted = 0;
  for (const row of rows) {
    await dbQuery(
      `INSERT INTO community_posts (id, user_id, content, image_urls, symbols, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (id) DO UPDATE SET
         content = EXCLUDED.content,
         image_urls = EXCLUDED.image_urls,
         symbols = EXCLUDED.symbols,
         created_at = EXCLUDED.created_at,
         updated_at = EXCLUDED.updated_at`,
      [row.id, userId, row.content, row.imageUrls, row.symbols, row.createdAt, row.updatedAt],
    );
    inserted += 1;
  }
  return inserted;
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const doImport = process.argv.includes("--import");
  if (!dryRun && !doImport) {
    console.log("Usage: node scripts/import-community-posts-18320260702.js --dry-run|--import");
    process.exit(1);
  }

  const raw = fs.readFileSync(TSV_PATH, "utf8");
  const parsed = parseTsvRows(raw);
  const rows = buildImportRows(parsed);

  await initPool();
  const user = await findUserByPhone(PHONE);
  if (!user?.id) {
    throw new Error(`user not found for phone ${PHONE}`);
  }

  const maxLen = Math.max(...rows.map((r) => r.content.length));
  const over2k = rows.filter((r) => r.content.length > 2000);

  console.log(`[import-posts] phone=${PHONE} user_id=${user.id}`);
  console.log(`[import-posts] rows=${rows.length} maxContentLen=${maxLen} over2000=${over2k.length}`);

  if (over2k.length) {
    console.warn("[import-posts] warning: some rows exceed app UI limit 2000 chars (ok for direct DB import)");
    for (const r of over2k) {
      console.warn(`  - ${r.id} len=${r.content.length}`);
    }
  }

  console.log("[import-posts] preview (first 3):");
  for (const row of rows.slice(0, 3)) {
    console.log(
      `  ${row.id} createdAt=${row.createdAt} (${new Date(row.createdAt).toISOString()}) len=${row.content.length}`,
    );
    console.log(`    ${row.content.slice(0, 60)}...`);
  }

  if (dryRun) {
    console.log("[import-posts] dry-run complete, no writes");
    await closeDatabase();
    return;
  }

  const count = await importRows(user.id, rows);
  const { rows: verifyRows } = await dbQuery(
    `SELECT COUNT(*)::int AS c FROM community_posts WHERE user_id = $1 AND id LIKE $2`,
    [user.id, `${ID_PREFIX}-%`],
  );
  console.log(`[import-posts] imported/updated ${count} rows`);
  console.log(`[import-posts] verify count=${verifyRows[0]?.c ?? 0}`);
  await closeDatabase();
}

main().catch(async (e) => {
  console.error(e);
  try {
    await closeDatabase();
  } catch {
    // ignore
  }
  process.exit(1);
});
