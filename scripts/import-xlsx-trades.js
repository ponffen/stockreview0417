/**
 * Import trades from an Excel export (.xlsx).
 * Expected columns (first row): market, share, cost, trade_date, type, preclose, payment, description, symbol, sub_market
 *
 * Usage:
 *   npm run import:xlsx -- --file /path/to/交易记录.xlsx [--mode append|replace]
 */
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const XLSX = require("xlsx");
const { normalizeTrade, importTrades } = require("../src/db");

const DEFAULT_ACCOUNT = "default";

function printUsage() {
  // eslint-disable-next-line no-console
  console.log("Usage: npm run import:xlsx -- --file <path.xlsx> [--mode append|replace]");
}

function parseArgs(argv) {
  const args = { file: "", mode: "append" };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === "--file") {
      args.file = argv[i + 1] || "";
      i += 1;
    } else if (token === "--mode") {
      args.mode = argv[i + 1] || "";
      i += 1;
    }
  }
  return args;
}

function cellToDateKey(value) {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    const y = value.getFullYear();
    const m = String(value.getMonth() + 1).padStart(2, "0");
    const d = String(value.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const utc = Math.round((value - 25569) * 86400 * 1000);
    const dt = new Date(utc);
    if (!Number.isNaN(dt.getTime())) {
      return cellToDateKey(dt);
    }
  }
  const s = String(value || "").trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
    return s.slice(0, 10);
  }
  return s.slice(0, 10);
}

function isDividendRow(description) {
  return String(description || "").includes("分红");
}

function rowToRawTrade(row, index) {
  const symbol = String(row.symbol || "").trim();
  if (!symbol) {
    return null;
  }
  const dateKey = cellToDateKey(row.trade_date);
  if (!dateKey || dateKey.length < 8) {
    return null;
  }
  const description = String(row.description || "").trim();
  const dividend = isDividendRow(description);
  const excelSide = String(row.type || "").trim().toLowerCase();
  const payment = Number(row.payment);
  const cost = Number(row.cost);
  const share = Number(row.share);

  if (dividend) {
    return {
      id: randomUUID(),
      accountId: DEFAULT_ACCOUNT,
      type: "dividend",
      symbol,
      name: symbol.toUpperCase(),
      side: "sell",
      price: 0,
      quantity: 0,
      amount: Math.abs(Number.isFinite(payment) ? payment : 0),
      date: dateKey,
      note: description,
      createdAt: Date.now() - index,
    };
  }

  const side = excelSide === "sell" ? "sell" : "buy";
  return {
    id: randomUUID(),
    accountId: DEFAULT_ACCOUNT,
    type: "trade",
    symbol,
    name: symbol.toUpperCase(),
    side,
    price: Number.isFinite(cost) ? cost : 0,
    quantity: Math.abs(Number.isFinite(share) ? share : 0),
    amount: Math.abs(Number.isFinite(payment) ? payment : 0),
    date: dateKey,
    note: description,
    createdAt: Date.now() - index,
  };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.file) {
    printUsage();
    process.exit(1);
  }
  const mode = args.mode === "replace" ? "replace" : "append";
  const absPath = path.isAbsolute(args.file) ? args.file : path.join(process.cwd(), args.file);

  const wb = XLSX.readFile(absPath, { cellDates: true });
  const sheetName = wb.SheetNames[0];
  if (!sheetName) {
    throw new Error("Workbook has no sheets");
  }
  const ws = wb.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: "" });
  const rawTrades = [];
  let skipped = 0;
  for (let i = 0; i < rows.length; i += 1) {
    const raw = rowToRawTrade(rows[i], i);
    if (!raw) {
      skipped += 1;
      continue;
    }
    rawTrades.push(normalizeTrade(raw));
  }

  const all = importTrades(rawTrades, mode);
  // eslint-disable-next-line no-console
  console.log(
    `Imported ${rawTrades.length} trades from ${absPath} (sheet: ${sheetName}, mode: ${mode}). Skipped ${skipped} empty/invalid rows. Total in DB: ${all.length}.`
  );
}

main();
