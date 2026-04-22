#!/usr/bin/env node
/**
 * 将指定账户的 analysis_daily_snapshot 与某标的 symbol_daily_close 按日期左连接，导出 .xlsx。
 * 用法: node scripts/export-analysis-snapshot-with-close.js [accountId] [symbol] [outPath]
 */
const path = require("node:path");
const fs = require("node:fs");
const Database = require("better-sqlite3");
const XLSX = require("xlsx");

const root = path.join(__dirname, "..");
const dbPath = path.join(root, "data", "app.db");

const accountId = process.argv[2] || "acc_1776821439184_507";
const symbol = (process.argv[3] || "hk02259").toLowerCase().trim();
const outArg = process.argv[4];
const outPath =
  outArg ||
  path.join(root, "exports", `analysis_daily_snapshot_${accountId}_${symbol}_merged.xlsx`);

if (!/^[a-z0-9._-]+$/i.test(symbol)) {
  console.error("[export] symbol 仅允许字母数字 ._-");
  process.exit(1);
}

const closeCol = `${symbol}_close`;

const db = new Database(dbPath, { readonly: true, fileMustExist: true });

const raw = db
  .prepare(
    `SELECT s.account_id AS account_id,
            s.date AS date,
            s.profit_cny,
            s.rate_cost,
            s.rate_twr,
            s.rate_dietz,
            s.total_profit,
            s.total_rate_cost,
            s.total_rate_twr,
            s.total_rate_dietz,
            s.principal,
            s.market_value,
            s.fx_hkd_cny,
            s.fx_usd_cny,
            s.created_at,
            s.updated_at,
            c.close AS _merge_close
     FROM analysis_daily_snapshot s
     LEFT JOIN symbol_daily_close c
       ON c.symbol = ? AND c.date = s.date
     WHERE s.account_id = ?
     ORDER BY s.date ASC`
  )
  .all(symbol, accountId);

const rows = raw.map((r) => ({
  account_id: r.account_id,
  date: r.date,
  [closeCol]: r._merge_close,
  profit_cny: r.profit_cny,
  rate_cost: r.rate_cost,
  rate_twr: r.rate_twr,
  rate_dietz: r.rate_dietz,
  total_profit: r.total_profit,
  total_rate_cost: r.total_rate_cost,
  total_rate_twr: r.total_rate_twr,
  total_rate_dietz: r.total_rate_dietz,
  principal: r.principal,
  market_value: r.market_value,
  fx_hkd_cny: r.fx_hkd_cny,
  fx_usd_cny: r.fx_usd_cny,
  created_at: r.created_at,
  updated_at: r.updated_at,
}));

db.close();

fs.mkdirSync(path.dirname(outPath), { recursive: true });
const ws = XLSX.utils.json_to_sheet(rows);
const wb = XLSX.utils.book_new();
XLSX.utils.book_append_sheet(wb, ws, "merged");
XLSX.writeFile(wb, outPath);
console.log(`[export] ${rows.length} 行 → ${outPath}`);
