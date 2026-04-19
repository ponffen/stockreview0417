const path = require("node:path");
const { randomUUID } = require("node:crypto");
const Database = require("better-sqlite3");

const DB_PATH = process.env.DB_PATH || path.join(__dirname, "..", "data", "app.db");
const db = new Database(DB_PATH);

db.pragma("journal_mode = WAL");

db.exec(`
CREATE TABLE IF NOT EXISTS trades (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  symbol TEXT NOT NULL,
  name TEXT NOT NULL,
  side TEXT NOT NULL,
  price REAL NOT NULL DEFAULT 0,
  quantity REAL NOT NULL DEFAULT 0,
  amount REAL NOT NULL DEFAULT 0,
  trade_date TEXT NOT NULL,
  note TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_trade_date_created_at
  ON trades (trade_date ASC, created_at ASC);

CREATE TABLE IF NOT EXISTS app_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
`);

const DEFAULT_SETTINGS = {
  route: "earning",
  useDemoData: true,
  algoMode: "cost",
  benchmark: "none",
  stageRange: "month",
  rangeDays: 7,
  analysisRangeMode: "preset",
  customRangeStart: "",
  customRangeEnd: "",
  capitalTrendMode: "both",
  capitalAmount: 0,
};

const UPSERT_TRADE_STMT = db.prepare(`
INSERT INTO trades (
  id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at, updated_at
) VALUES (
  @id, @type, @symbol, @name, @side, @price, @quantity, @amount, @trade_date, @note, @created_at, @updated_at
)
ON CONFLICT(id) DO UPDATE SET
  type = excluded.type,
  symbol = excluded.symbol,
  name = excluded.name,
  side = excluded.side,
  price = excluded.price,
  quantity = excluded.quantity,
  amount = excluded.amount,
  trade_date = excluded.trade_date,
  note = excluded.note,
  updated_at = excluded.updated_at
`);

const DELETE_ALL_TRADES_STMT = db.prepare("DELETE FROM trades");
const DELETE_TRADE_STMT = db.prepare("DELETE FROM trades WHERE id = ?");
const SELECT_ALL_TRADES_STMT = db.prepare(`
SELECT id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at
FROM trades
ORDER BY trade_date ASC, created_at ASC
`);

const UPSERT_SETTING_STMT = db.prepare(`
INSERT INTO app_settings (key, value, updated_at)
VALUES (@key, @value, @updated_at)
ON CONFLICT(key) DO UPDATE SET
  value = excluded.value,
  updated_at = excluded.updated_at
`);
const SELECT_ALL_SETTINGS_STMT = db.prepare("SELECT key, value FROM app_settings");

function nowMs() {
  return Date.now();
}

function toDateKey(input) {
  const date = input ? new Date(input) : new Date();
  if (Number.isNaN(date.getTime())) {
    const fallback = new Date();
    return `${fallback.getFullYear()}-${String(fallback.getMonth() + 1).padStart(2, "0")}-${String(
      fallback.getDate()
    ).padStart(2, "0")}`;
  }
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(
    date.getDate()
  ).padStart(2, "0")}`;
}

function validNumber(...values) {
  for (const value of values) {
    const num = Number(value);
    if (Number.isFinite(num)) {
      return num;
    }
  }
  return 0;
}

function normalizeSymbol(rawSymbol) {
  const value = String(rawSymbol || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
  if (!value) {
    return "";
  }
  if (
    value.startsWith("sh") ||
    value.startsWith("sz") ||
    value.startsWith("hk") ||
    value.startsWith("rt_hk") ||
    value.startsWith("gb_")
  ) {
    return value;
  }
  if (/^\d{6}$/.test(value)) {
    if (["5", "6", "9"].includes(value[0])) {
      return `sh${value}`;
    }
    return `sz${value}`;
  }
  if (/^\d{5}$/.test(value)) {
    return `hk${value}`;
  }
  return value;
}

function normalizedSide(type, side) {
  if (type === "dividend" || type === "merge") {
    return "sell";
  }
  if (type === "bonus" || type === "split") {
    return "buy";
  }
  return side === "sell" ? "sell" : "buy";
}

function parseSide(raw, fallbackType) {
  const value = String(raw || "").trim().toLowerCase();
  if (value === "buy" || value === "b" || value.includes("买")) {
    return "buy";
  }
  if (value === "sell" || value === "s" || value.includes("卖")) {
    return "sell";
  }
  return normalizedSide(fallbackType, "buy");
}

function parseType(rawType) {
  const value = String(rawType || "").trim().toLowerCase();
  if (!value) {
    return "trade";
  }
  if (value === "dividend" || value.includes("分红")) {
    return "dividend";
  }
  if (value === "bonus" || value.includes("送股")) {
    return "bonus";
  }
  if (value === "split" || value.includes("拆股")) {
    return "split";
  }
  if (value === "merge" || value.includes("合股")) {
    return "merge";
  }
  return "trade";
}

function normalizeTrade(input) {
  const raw = input || {};
  const type = parseType(raw.type || raw.tradeType || raw["类型"]);
  const symbol = normalizeSymbol(
    raw.symbol || raw.code || raw.stockCode || raw.ts_code || raw["证券代码"] || raw["代码"]
  );
  const name = String(
    raw.name || raw.stockName || raw.securityName || raw["证券名称"] || raw["名称"] || symbol || "unknown"
  ).trim();
  const side = parseSide(
    raw.side || raw.direction || raw.action || raw["方向"] || raw["买卖"] || raw.type,
    type
  );
  const price = validNumber(
    raw.price,
    raw.cost,
    raw.tradePrice,
    raw.dealPrice,
    raw.avgPrice,
    raw["成交价"],
    raw["价格"],
    0
  );
  const quantity = validNumber(
    raw.quantity,
    raw.share,
    raw.qty,
    raw.volume,
    raw.shares,
    raw["数量"],
    raw["成交数量"],
    0
  );
  const amount = Math.abs(
    validNumber(
      raw.amount,
      raw.payment,
      raw.tradeAmount,
      raw.turnover,
      raw["发生金额"],
      raw["成交金额"],
      price * quantity
    )
  );
  const date = toDateKey(
    raw.date || raw.trade_date || raw.tradeDate || raw.dealDate || raw["日期"] || raw["成交日期"]
  );
  const note = String(raw.note || raw.remark || raw["备注"] || "").trim();
  const createdAt = validNumber(raw.createdAt, raw.created_at, raw.timestamp, Date.parse(date), nowMs());

  return {
    id: String(raw.id || raw.tradeId || raw.ts_id || randomUUID()),
    type,
    symbol,
    name,
    side,
    price,
    quantity,
    amount,
    date,
    note,
    createdAt,
  };
}

function tradeToRow(trade) {
  const safe = normalizeTrade(trade);
  const updatedAt = nowMs();
  return {
    id: safe.id,
    type: safe.type,
    symbol: safe.symbol,
    name: safe.name,
    side: safe.side,
    price: safe.price,
    quantity: safe.quantity,
    amount: safe.amount,
    trade_date: safe.date,
    note: safe.note,
    created_at: safe.createdAt,
    updated_at: updatedAt,
  };
}

function rowToTrade(row) {
  return {
    id: row.id,
    type: row.type,
    symbol: row.symbol,
    name: row.name,
    side: row.side,
    price: Number(row.price),
    quantity: Number(row.quantity),
    amount: Number(row.amount),
    date: row.trade_date,
    note: row.note || "",
    createdAt: Number(row.created_at),
  };
}

function getTrades() {
  return SELECT_ALL_TRADES_STMT.all().map(rowToTrade);
}

function upsertTrade(trade) {
  const row = tradeToRow(trade);
  UPSERT_TRADE_STMT.run(row);
  return normalizeTrade({ ...trade, id: row.id });
}

const replaceTradesTx = db.transaction((trades) => {
  DELETE_ALL_TRADES_STMT.run();
  for (const trade of trades) {
    UPSERT_TRADE_STMT.run(tradeToRow(trade));
  }
});

const appendTradesTx = db.transaction((trades) => {
  for (const trade of trades) {
    UPSERT_TRADE_STMT.run(tradeToRow(trade));
  }
});

function importTrades(trades, mode = "append") {
  const list = Array.isArray(trades) ? trades : [];
  if (mode === "replace") {
    replaceTradesTx(list);
  } else {
    appendTradesTx(list);
  }
  return getTrades();
}

function deleteTradeById(tradeId) {
  const res = DELETE_TRADE_STMT.run(String(tradeId || ""));
  return res.changes > 0;
}

function getSettings() {
  const settings = { ...DEFAULT_SETTINGS };
  const rows = SELECT_ALL_SETTINGS_STMT.all();
  for (const row of rows) {
    if (!(row.key in settings)) {
      continue;
    }
    try {
      settings[row.key] = JSON.parse(row.value);
    } catch {
      settings[row.key] = row.value;
    }
  }
  return settings;
}

function setSettings(partial) {
  if (!partial || typeof partial !== "object") {
    return getSettings();
  }
  const updatedAt = nowMs();
  for (const [key, value] of Object.entries(partial)) {
    if (!(key in DEFAULT_SETTINGS)) {
      continue;
    }
    UPSERT_SETTING_STMT.run({
      key,
      value: JSON.stringify(value),
      updated_at: updatedAt,
    });
  }
  return getSettings();
}

function getState() {
  return {
    ...getSettings(),
    trades: getTrades(),
  };
}

module.exports = {
  DEFAULT_SETTINGS,
  DB_PATH,
  normalizeTrade,
  getTrades,
  upsertTrade,
  importTrades,
  deleteTradeById,
  getSettings,
  setSettings,
  getState,
};
