#!/usr/bin/env node
/**
 * Backfill symbol_name_map: name_cn, market_tag via Tencent realtime quotes.
 * Usage: node scripts/backfill-symbol-name-map.js [--force-all]
 */
require("dotenv").config();

const { initPool, dbQuery, upsertSymbolNameMapBatch, normalizeSymbol, getSymbolMetaMap } = require("../src/db");
const { fetchTencentQuoteMetaForSymbols } = require("../src/tencent-quote-meta");
const { isUsTickerSymbol } = require("../src/db-pure");

const CHUNK = 50;
const forceAll = process.argv.includes("--force-all");

async function collectSymbolsFromDb() {
  const set = new Set();
  const add = (s) => {
    const sym = normalizeSymbol(s);
    if (sym) {
      set.add(sym);
    }
  };

  const { rows: tradeRows } = await dbQuery(
    `SELECT DISTINCT symbol FROM trades WHERE symbol IS NOT NULL AND trim(symbol) <> ''`
  );
  for (const r of tradeRows) {
    add(r.symbol);
  }

  const { rows: postRows } = await dbQuery(`SELECT symbols FROM community_posts WHERE symbols IS NOT NULL`);
  for (const r of postRows) {
    let list = [];
    try {
      list = JSON.parse(String(r.symbols || "[]"));
    } catch {
      list = [];
    }
    for (const sym of list) {
      add(sym);
    }
  }

  const { rows: closeRows } = await dbQuery(
    `SELECT DISTINCT symbol FROM symbol_daily_close WHERE symbol IS NOT NULL AND trim(symbol) <> ''`
  );
  for (const r of closeRows) {
    add(r.symbol);
  }

  const { rows: mapRows } = await dbQuery(`SELECT symbol FROM symbol_name_map`);
  for (const r of mapRows) {
    add(r.symbol);
  }

  return [...set].sort();
}

async function loadExistingMap(symbols) {
  if (!symbols.length) {
    return new Map();
  }
  const { rows } = await dbQuery(
    `SELECT symbol, name_cn, market_tag
     FROM symbol_name_map
     WHERE symbol = ANY($1::text[])`,
    [symbols]
  );
  const out = new Map();
  for (const r of rows || []) {
    out.set(normalizeSymbol(r.symbol), r);
  }
  return out;
}

function needsBackfill(sym, existing) {
  if (forceAll) {
    return true;
  }
  const row = existing.get(sym);
  if (!row) {
    return true;
  }
  const name = String(row.name_cn || "").trim();
  const tag = String(row.market_tag || "").trim().toLowerCase();
  if (!name || name === "-") {
    return true;
  }
  if (!tag || tag === "ot") {
    return true;
  }
  if (sym.startsWith("sh") || sym.startsWith("sz")) {
    return tag !== "cn";
  }
  if (isUsTickerSymbol(sym)) {
    return tag !== "us";
  }
  return false;
}

async function main() {
  if (!process.env.DATABASE_URL) {
    console.error("DATABASE_URL required");
    process.exit(1);
  }
  await initPool();
  await getSymbolMetaMap([]);
  const fixed = await dbQuery(
    `UPDATE symbol_name_map
     SET market_tag = 'cn', updated_at = $1
     WHERE lower(symbol) ~ '^(sh|sz)' AND lower(coalesce(market_tag, '')) = 'ot'`,
    [Date.now()]
  );
  console.log(`[backfill] fixed sh/sz ot→cn rows: ${fixed.rowCount ?? 0}`);
  const { rows: otRows } = await dbQuery(
    `SELECT symbol FROM symbol_name_map WHERE lower(coalesce(market_tag, '')) = 'ot'`
  );
  const usFix = (otRows || [])
    .map((r) => normalizeSymbol(r.symbol))
    .filter((sym) => sym && isUsTickerSymbol(sym));
  if (usFix.length) {
    const usFixed = await dbQuery(
      `UPDATE symbol_name_map
       SET market_tag = 'us', updated_at = $2
       WHERE symbol = ANY($1::text[])`,
      [usFix, Date.now()]
    );
    console.log(`[backfill] fixed us-ticker ot→us rows: ${usFixed.rowCount ?? 0}`, usFix);
  }
  const pltrFixed = await dbQuery(
    `UPDATE symbol_name_map SET market_tag = 'us', updated_at = $1 WHERE symbol = 'pltr'`,
    [Date.now()]
  );
  console.log(`[backfill] pltr→us rows: ${pltrFixed.rowCount ?? 0}`);
  const all = await collectSymbolsFromDb();
  console.log(`[backfill] collected ${all.length} unique symbols`);
  const existing = await loadExistingMap(all);
  const todo = all.filter((sym) => needsBackfill(sym, existing));
  console.log(`[backfill] ${todo.length} symbols need backfill${forceAll ? " (force-all)" : ""}`);
  if (!todo.length) {
    return;
  }

  let upserted = 0;
  let failed = 0;
  for (let i = 0; i < todo.length; i += CHUNK) {
    const chunk = todo.slice(i, i + CHUNK);
    const fetched = await fetchTencentQuoteMetaForSymbols(chunk);
    const batch = [];
    for (const sym of chunk) {
      const hit = fetched.get(sym);
      if (!hit?.name) {
        failed += 1;
        console.warn(`[backfill] tencent miss: ${sym}`);
        continue;
      }
      batch.push({
        symbol: sym,
        nameCn: hit.name,
        marketTag: hit.marketTag || "ot",
        source: "backfill",
      });
    }
    if (batch.length) {
      upserted += await upsertSymbolNameMapBatch(batch);
      console.log(`[backfill] chunk ${Math.floor(i / CHUNK) + 1}: upserted ${batch.length}`);
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  console.log(`[backfill] done upserted=${upserted} tencent_miss=${failed}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
