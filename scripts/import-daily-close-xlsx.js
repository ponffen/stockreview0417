#!/usr/bin/env node
/**
 * 将手工整理的日 K 收盘价 Excel 导入 symbol_daily_close。
 *
 * 表头约定：股票代码、日期、收盘价
 * 代码示例：us_AAPL、cn_sh510300、hk_hk00700（与「全量日K收盘价_最终版」一致）
 *
 * 用法：
 *   node scripts/import-daily-close-xlsx.js [路径.xlsx]
 *   npm run import:daily-close:xlsx
 */
const path = require("node:path");
const XLSX = require("xlsx");
const { upsertSymbolDailyCloseBatch, normalizeSymbol } = require(path.join(__dirname, "..", "src", "db"));

/** Excel 股票代码列 → 与本库 normalizeSymbol 一致的写法 */
function excelCodeToNormalizedSymbol(raw) {
  const s = String(raw || "").trim();
  if (!s) {
    return "";
  }
  const lower = s.toLowerCase();
  if (lower.startsWith("us_")) {
    return lower.slice(3);
  }
  if (lower.startsWith("cn_")) {
    return lower.slice(3);
  }
  if (lower.startsWith("hk_hk")) {
    const rest = lower.slice("hk_hk".length).replace(/\D/g, "");
    if (!rest) {
      return "";
    }
    return `hk${rest.padStart(5, "0").slice(-5)}`;
  }
  return lower;
}

function cellToDateKey(v) {
  if (v == null || v === "") {
    return "";
  }
  if (v instanceof Date) {
    const y = v.getFullYear();
    const m = String(v.getMonth() + 1).padStart(2, "0");
    const d = String(v.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }
  const t = String(v).trim().slice(0, 10).replace(/\//g, "-");
  return t.length >= 10 ? t : "";
}

function main() {
  const fileArg = process.argv[2];
  const xlsxPath = path.resolve(
    __dirname,
    "..",
    fileArg || "全量日K收盘价_最终版.xlsx",
  );
  // eslint-disable-next-line no-console
  console.log("[import-daily-close]", xlsxPath);

  const wb = XLSX.readFile(xlsxPath, { cellDates: true, raw: false });
  const sheetName = wb.SheetNames[0];
  if (!sheetName) {
    // eslint-disable-next-line no-console
    console.error("工作簿无工作表");
    process.exit(1);
  }
  const rows = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { defval: "" });
  const out = [];
  const bad = [];
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i] || {};
    const codeRaw = row["股票代码"] ?? row.stock ?? row.symbol ?? row["代码"];
    const dateRaw = row["日期"] ?? row.date ?? row["Date"];
    const closeRaw = row["收盘价"] ?? row.close ?? row["收盘"];
    const mapped = excelCodeToNormalizedSymbol(codeRaw);
    const sym = normalizeSymbol(mapped || codeRaw);
    const date = cellToDateKey(dateRaw);
    const close = Number(String(closeRaw).replace(/,/g, ""));
    if (!sym || !date || !Number.isFinite(close) || close <= 0) {
      bad.push({ line: i + 2, codeRaw, date, close });
      continue;
    }
    out.push({
      symbol: sym,
      date,
      close,
      source: "xlsx_manual",
    });
  }

  if (bad.length) {
    // eslint-disable-next-line no-console
    console.warn("[import-daily-close] 跳过无效行", bad.length, "条（示例首条）", bad[0]);
  }

  const n = upsertSymbolDailyCloseBatch(out);
  // eslint-disable-next-line no-console
  console.log("[import-daily-close] 已 upsert", out.length, "条有效记录（批次数", n, "）");
}

main();
