const path = require("node:path");
const { randomUUID } = require("node:crypto");
const Database = require("better-sqlite3");
const {
  hashPassword,
  verifyPassword,
  isValidPhone,
  isValidPasswordDigits,
} = require("./password");

const DB_PATH = process.env.DB_PATH || path.join(__dirname, "..", "data", "app.db");
const db = new Database(DB_PATH);

db.pragma("journal_mode = WAL");

db.exec(`
CREATE TABLE IF NOT EXISTS trades (
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL DEFAULT 'default',
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

CREATE TABLE IF NOT EXISTS accounts (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  currency TEXT NOT NULL DEFAULT 'CNY',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_accounts_created_at ON accounts (created_at ASC);

CREATE TABLE IF NOT EXISTS daily_returns (
  account_id TEXT NOT NULL,
  date TEXT NOT NULL,
  profit REAL NOT NULL DEFAULT 0,
  return_rate REAL NOT NULL DEFAULT 0,
  total_asset REAL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (account_id, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_returns_date ON daily_returns (date ASC);

CREATE TABLE IF NOT EXISTS symbol_daily_pnl (
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  date TEXT NOT NULL,
  eod_shares REAL NOT NULL DEFAULT 0,
  day_trade_qty REAL NOT NULL DEFAULT 0,
  day_trade_amount REAL NOT NULL DEFAULT 0,
  day_close_price REAL,
  day_pnl_native REAL NOT NULL DEFAULT 0,
  currency TEXT NOT NULL DEFAULT 'CNY',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (account_id, symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_symbol_daily_pnl_date ON symbol_daily_pnl (date ASC);

CREATE TABLE IF NOT EXISTS analysis_daily_snapshot (
  account_id TEXT NOT NULL,
  date TEXT NOT NULL,
  profit_cny REAL NOT NULL DEFAULT 0,
  rate_cost REAL NOT NULL DEFAULT 0,
  rate_twr REAL NOT NULL DEFAULT 0,
  rate_dietz REAL NOT NULL DEFAULT 0,
  total_profit REAL NOT NULL DEFAULT 0,
  total_rate_cost REAL NOT NULL DEFAULT 0,
  total_rate_twr REAL NOT NULL DEFAULT 0,
  total_rate_dietz REAL NOT NULL DEFAULT 0,
  principal REAL NOT NULL DEFAULT 0,
  market_value REAL NOT NULL DEFAULT 0,
  fx_hkd_cny REAL,
  fx_usd_cny REAL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (account_id, date)
);

CREATE INDEX IF NOT EXISTS idx_analysis_daily_snapshot_date ON analysis_daily_snapshot (date ASC);

CREATE TABLE IF NOT EXISTS symbol_daily_close (
  symbol TEXT NOT NULL,
  date TEXT NOT NULL,
  close REAL NOT NULL,
  source TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_symbol_daily_close_date ON symbol_daily_close (date ASC);

CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  phone TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
`);

const SEED_USER_PHONE = "18310270720";
const SEED_USER_PASSWORD = "123456";

const tradeColumns = db.prepare("PRAGMA table_info(trades)").all();
if (!tradeColumns.some((col) => col.name === "account_id")) {
  db.exec("ALTER TABLE trades ADD COLUMN account_id TEXT NOT NULL DEFAULT 'default'");
}

function tradesHasUserIdColumn() {
  return db.prepare("PRAGMA table_info(trades)").all().some((c) => c.name === "user_id");
}

function ensureSeedUserRow() {
  const row = db.prepare("SELECT id FROM users WHERE phone = ?").get(SEED_USER_PHONE);
  if (row) {
    return row.id;
  }
  const id = randomUUID();
  const now = Date.now();
  db.prepare(
    "INSERT INTO users (id, phone, password_hash, created_at, updated_at) VALUES (?,?,?,?,?)"
  ).run(id, SEED_USER_PHONE, hashPassword(SEED_USER_PASSWORD), now, now);
  return id;
}

function migratePerUserSchemaIfNeeded() {
  if (tradesHasUserIdColumn()) {
    ensureSeedUserRow();
    return;
  }
  const seedId = ensureSeedUserRow();
  const tx = db.transaction(() => {
    db.exec(`
CREATE TABLE accounts_mg (
  user_id TEXT NOT NULL,
  id TEXT NOT NULL,
  name TEXT NOT NULL,
  currency TEXT NOT NULL DEFAULT 'CNY',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (user_id, id)
);`);
    db.prepare(
      `INSERT INTO accounts_mg (user_id, id, name, currency, created_at, updated_at)
       SELECT ?, id, name, currency, created_at, updated_at FROM accounts`
    ).run(seedId);
    db.exec(`DROP TABLE accounts; ALTER TABLE accounts_mg RENAME TO accounts;`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_accounts_created_at ON accounts (created_at ASC);`);

    db.exec(`
CREATE TABLE daily_returns_mg (
  user_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  date TEXT NOT NULL,
  profit REAL NOT NULL DEFAULT 0,
  return_rate REAL NOT NULL DEFAULT 0,
  total_asset REAL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (user_id, account_id, date)
);`);
    db.prepare(
      `INSERT INTO daily_returns_mg (user_id, account_id, date, profit, return_rate, total_asset, created_at, updated_at)
       SELECT ?, account_id, date, profit, return_rate, total_asset, created_at, updated_at FROM daily_returns`
    ).run(seedId);
    db.exec(`DROP TABLE daily_returns; ALTER TABLE daily_returns_mg RENAME TO daily_returns;`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_daily_returns_date ON daily_returns (date ASC);`);

    db.exec(`
CREATE TABLE symbol_daily_pnl_mg (
  user_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  date TEXT NOT NULL,
  eod_shares REAL NOT NULL DEFAULT 0,
  day_trade_qty REAL NOT NULL DEFAULT 0,
  day_trade_amount REAL NOT NULL DEFAULT 0,
  day_close_price REAL,
  day_pnl_native REAL NOT NULL DEFAULT 0,
  currency TEXT NOT NULL DEFAULT 'CNY',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (user_id, account_id, symbol, date)
);`);
    db.prepare(
      `INSERT INTO symbol_daily_pnl_mg (
         user_id, account_id, symbol, date, eod_shares, day_trade_qty, day_trade_amount,
         day_close_price, day_pnl_native, currency, created_at, updated_at
       )
       SELECT ?, account_id, symbol, date, eod_shares, day_trade_qty, day_trade_amount,
         day_close_price, day_pnl_native, currency, created_at, updated_at FROM symbol_daily_pnl`
    ).run(seedId);
    db.exec(`DROP TABLE symbol_daily_pnl; ALTER TABLE symbol_daily_pnl_mg RENAME TO symbol_daily_pnl;`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_symbol_daily_pnl_date ON symbol_daily_pnl (date ASC);`);

    db.exec(`
CREATE TABLE analysis_daily_snapshot_mg (
  user_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  date TEXT NOT NULL,
  profit_cny REAL NOT NULL DEFAULT 0,
  rate_cost REAL NOT NULL DEFAULT 0,
  rate_twr REAL NOT NULL DEFAULT 0,
  rate_dietz REAL NOT NULL DEFAULT 0,
  total_profit REAL NOT NULL DEFAULT 0,
  total_rate_cost REAL NOT NULL DEFAULT 0,
  total_rate_twr REAL NOT NULL DEFAULT 0,
  total_rate_dietz REAL NOT NULL DEFAULT 0,
  principal REAL NOT NULL DEFAULT 0,
  market_value REAL NOT NULL DEFAULT 0,
  fx_hkd_cny REAL,
  fx_usd_cny REAL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (user_id, account_id, date)
);`);
    db.prepare(
      `INSERT INTO analysis_daily_snapshot_mg (
         user_id, account_id, date, profit_cny, rate_cost, rate_twr, rate_dietz,
         total_profit, total_rate_cost, total_rate_twr, total_rate_dietz,
         principal, market_value, fx_hkd_cny, fx_usd_cny, created_at, updated_at
       )
       SELECT ?, account_id, date, profit_cny, rate_cost, rate_twr, rate_dietz,
         total_profit, total_rate_cost, total_rate_twr, total_rate_dietz,
         principal, market_value, fx_hkd_cny, fx_usd_cny, created_at, updated_at
       FROM analysis_daily_snapshot`
    ).run(seedId);
    db.exec(`DROP TABLE analysis_daily_snapshot; ALTER TABLE analysis_daily_snapshot_mg RENAME TO analysis_daily_snapshot;`);
    db.exec(
      `CREATE INDEX IF NOT EXISTS idx_analysis_daily_snapshot_date ON analysis_daily_snapshot (date ASC);`
    );

    db.exec(`
CREATE TABLE app_settings_mg (
  user_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (user_id, key)
);`);
    db.prepare(
      `INSERT INTO app_settings_mg (user_id, key, value, updated_at) SELECT ?, key, value, updated_at FROM app_settings`
    ).run(seedId);
    db.exec(`DROP TABLE app_settings; ALTER TABLE app_settings_mg RENAME TO app_settings;`);

    db.exec(`ALTER TABLE trades ADD COLUMN user_id TEXT;`);
    db.prepare(`UPDATE trades SET user_id = ? WHERE user_id IS NULL`).run(seedId);
  });
  tx();
}

migratePerUserSchemaIfNeeded();

function ensureCommunitySchema() {
  const ucols = db.prepare("PRAGMA table_info(users)").all();
  if (!ucols.some((c) => c.name === "nickname")) {
    db.exec("ALTER TABLE users ADD COLUMN nickname TEXT");
  }
  if (!ucols.some((c) => c.name === "community_public")) {
    db.exec("ALTER TABLE users ADD COLUMN community_public INTEGER NOT NULL DEFAULT 1");
  }
  db.exec(`
    CREATE TABLE IF NOT EXISTS community_follows (
      follower_id TEXT NOT NULL,
      followee_id TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      PRIMARY KEY (follower_id, followee_id)
    );
    CREATE INDEX IF NOT EXISTS idx_community_follows_followee ON community_follows (followee_id);
    CREATE TABLE IF NOT EXISTS community_leaderboard_cache (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      payload TEXT NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);
  db.exec(
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_nickname_nonnull ON users(nickname) WHERE nickname IS NOT NULL AND length(trim(nickname)) > 0"
  );
}

ensureCommunitySchema();

const UPSERT_ACCOUNT_STMT = db.prepare(`
INSERT INTO accounts (user_id, id, name, currency, created_at, updated_at)
VALUES (@user_id, @id, @name, @currency, @created_at, @updated_at)
ON CONFLICT(user_id, id) DO UPDATE SET
  name = excluded.name,
  currency = excluded.currency,
  updated_at = excluded.updated_at
`);

const SELECT_ALL_ACCOUNTS_STMT = db.prepare(`
SELECT id, name, currency, created_at FROM accounts WHERE user_id = ? ORDER BY created_at ASC, id ASC
`);

const DELETE_ACCOUNT_STMT = db.prepare("DELETE FROM accounts WHERE user_id = ? AND id = ?");

const SELECT_ALL_DAILY_RETURNS_STMT = db.prepare(`
SELECT account_id, date, profit, return_rate, total_asset, created_at
FROM daily_returns
WHERE user_id = ?
ORDER BY account_id ASC, date ASC
`);

const SELECT_DAILY_RETURNS_RANGE_STMT = db.prepare(`
SELECT account_id, date, profit, return_rate, total_asset, created_at
FROM daily_returns
WHERE user_id = @user_id
  AND (@account_id = '' OR account_id = @account_id)
  AND date >= @from
  AND date <= @to
ORDER BY date ASC
`);

const UPSERT_DAILY_RETURN_STMT = db.prepare(`
INSERT INTO daily_returns (user_id, account_id, date, profit, return_rate, total_asset, created_at, updated_at)
VALUES (@user_id, @account_id, @date, @profit, @return_rate, @total_asset, @created_at, @updated_at)
ON CONFLICT(user_id, account_id, date) DO UPDATE SET
  profit = excluded.profit,
  return_rate = excluded.return_rate,
  total_asset = excluded.total_asset,
  updated_at = excluded.updated_at
`);

const DELETE_DAILY_RETURN_STMT = db.prepare(
  "DELETE FROM daily_returns WHERE user_id = ? AND account_id = ? AND date = ?"
);

const DELETE_ALL_DAILY_RETURNS_STMT = db.prepare("DELETE FROM daily_returns WHERE user_id = ?");

const DEFAULT_SETTINGS = {
  route: "earning",
  useDemoData: true,
  algoMode: "cost",
  benchmark: "none",
  stageRange: "month",
  rangeDays: 30,
  analysisRangeMode: "preset",
  analysisPreset: null,
  customRangeStart: "",
  customRangeEnd: "",
  capitalTrendMode: "principal",
  capitalAmount: 0,
  accounts: [{ id: "default", name: "默认账户", currency: "CNY", createdAt: 0 }],
  selectedAccountId: "all",
  tradeFilterAccountId: "all",
  stockSortKey: "default",
  stockSortOrder: "default",
  /** native=港股美元按原币种展示金额；cny=金额列用人民币并加 ¥（持仓与汇总仍以人民币计算） */
  stockAmountDisplay: "native",
};

const UPSERT_TRADE_STMT = db.prepare(`
INSERT INTO trades (
  id, user_id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at, updated_at
) VALUES (
  @id, @user_id, @account_id, @type, @symbol, @name, @side, @price, @quantity, @amount, @trade_date, @note, @created_at, @updated_at
)
ON CONFLICT(id) DO UPDATE SET
  user_id = excluded.user_id,
  account_id = excluded.account_id,
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

const DELETE_ALL_TRADES_STMT = db.prepare("DELETE FROM trades WHERE user_id = ?");
const DELETE_TRADE_STMT = db.prepare("DELETE FROM trades WHERE user_id = ? AND id = ?");
const SELECT_ALL_TRADES_STMT = db.prepare(`
SELECT id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at
FROM trades
WHERE user_id = ?
ORDER BY trade_date ASC, created_at ASC
`);

const UPSERT_SETTING_STMT = db.prepare(`
INSERT INTO app_settings (user_id, key, value, updated_at)
VALUES (@user_id, @key, @value, @updated_at)
ON CONFLICT(user_id, key) DO UPDATE SET
  value = excluded.value,
  updated_at = excluded.updated_at
`);
const SELECT_ALL_SETTINGS_STMT = db.prepare("SELECT key, value FROM app_settings WHERE user_id = ?");

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

function normalizeAccountRecords(rawList) {
  const seen = new Set();
  const base = [];
  const input = Array.isArray(rawList) ? rawList : [];
  for (const raw of input) {
    const id = String(raw?.id || "").trim();
    if (!id || seen.has(id)) {
      continue;
    }
    const name = String(raw?.name || "").trim() || "未命名账户";
    const currency = ["CNY", "USD", "HKD"].includes(String(raw?.currency || "CNY").toUpperCase())
      ? String(raw.currency).toUpperCase()
      : "CNY";
    const createdAt = validNumber(raw?.createdAt, raw?.created_at, Date.now());
    base.push({ id, name, currency, createdAt });
    seen.add(id);
  }
  if (!seen.has("default")) {
    base.unshift({ id: "default", name: "默认账户", currency: "CNY", createdAt: 0 });
  } else {
    const idx = base.findIndex((item) => item.id === "default");
    base[idx] = {
      ...base[idx],
      name: "默认账户",
      currency: base[idx].currency || "CNY",
      createdAt: base[idx].createdAt || 0,
    };
  }
  base.sort((a, b) => {
    if (a.id === "default") {
      return -1;
    }
    if (b.id === "default") {
      return 1;
    }
    return Number(a.createdAt) - Number(b.createdAt);
  });
  return base;
}

function migrateAccountsIfEmptyForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  const { c } = db.prepare("SELECT COUNT(*) AS c FROM accounts WHERE user_id = ?").get(uid);
  if (c > 0) {
    return;
  }
  let list = [{ id: "default", name: "默认账户", currency: "CNY", createdAt: 0 }];
  const row = db.prepare("SELECT value FROM app_settings WHERE user_id = ? AND key = ?").get(uid, "accounts");
  if (row && row.value) {
    try {
      const parsed = JSON.parse(row.value);
      if (Array.isArray(parsed) && parsed.length) {
        list = parsed;
      }
    } catch {
      // ignore invalid legacy JSON
    }
  }
  const now = nowMs();
  const tx = db.transaction(() => {
    for (const acc of normalizeAccountRecords(list)) {
      UPSERT_ACCOUNT_STMT.run({
        user_id: uid,
        id: acc.id,
        name: acc.name,
        currency: acc.currency,
        created_at: acc.createdAt,
        updated_at: now,
      });
    }
  });
  tx();
}

function migrateAllUsersAccountsIfEmpty() {
  const rows = db.prepare("SELECT id FROM users").all();
  for (const r of rows) {
    migrateAccountsIfEmptyForUser(r.id);
  }
}

migrateAllUsersAccountsIfEmpty();

function getCliUserId() {
  const phone = String(process.env.STOCKREVIEW_PHONE || SEED_USER_PHONE).trim();
  const row = db.prepare("SELECT id FROM users WHERE phone = ?").get(phone);
  if (!row) {
    throw new Error(`No user for phone ${phone}; open app once to seed database.`);
  }
  return row.id;
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
  if (/^\d{1,4}$/.test(value)) {
    return `hk${value.padStart(5, "0")}`;
  }
  return value;
}

function migrateTradeSymbolsToNormalized() {
  try {
    const rows = db.prepare("SELECT id, user_id, symbol FROM trades").all();
    const stmt = db.prepare("UPDATE trades SET symbol = ?, updated_at = ? WHERE user_id = ? AND id = ?");
    const now = nowMs();
    let updated = 0;
    for (const row of rows) {
      const next = normalizeSymbol(row.symbol);
      if (next && next !== row.symbol) {
        stmt.run(next, now, row.user_id, row.id);
        updated += 1;
      }
    }
    if (typeof process !== "undefined" && process.env.STOCKREVIEW_SILENT_DB_LOG !== "1") {
      // eslint-disable-next-line no-console
      console.log(
        `[db] trade symbol migration: ${updated} row(s) updated, ${rows.length} trade(s) checked.`
      );
    }
  } catch {
    // ignore migration failures
  }
}

migrateTradeSymbolsToNormalized();

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
    accountId: String(raw.accountId || raw.account_id || raw.account || "default").trim() || "default",
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

function tradeToRow(trade, userId) {
  const safe = normalizeTrade(trade);
  const updatedAt = nowMs();
  return {
    id: safe.id,
    user_id: String(userId || "").trim(),
    account_id: safe.accountId || "default",
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
    accountId: row.account_id || "default",
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

function getTrades(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  return SELECT_ALL_TRADES_STMT.all(uid).map(rowToTrade);
}

function upsertTrade(trade, userId) {
  const row = tradeToRow(trade, userId);
  if (!row.user_id) {
    throw new Error("userId required");
  }
  UPSERT_TRADE_STMT.run(row);
  return normalizeTrade({ ...trade, id: row.id });
}

const replaceTradesTx = db.transaction((trades, userId) => {
  DELETE_ALL_TRADES_STMT.run(userId);
  for (const trade of trades) {
    UPSERT_TRADE_STMT.run(tradeToRow(trade, userId));
  }
});

const appendTradesTx = db.transaction((trades, userId) => {
  for (const trade of trades) {
    UPSERT_TRADE_STMT.run(tradeToRow(trade, userId));
  }
});

function importTrades(trades, mode = "append", userId = null) {
  const uid = String(userId || getCliUserId()).trim();
  const list = Array.isArray(trades) ? trades : [];
  if (mode === "replace") {
    replaceTradesTx(list, uid);
  } else {
    appendTradesTx(list, uid);
  }
  return getTrades(uid);
}

function deleteTradeById(tradeId, userId) {
  const uid = String(userId || "").trim();
  const res = DELETE_TRADE_STMT.run(uid, String(tradeId || ""));
  return res.changes > 0;
}

function rowToAccount(row) {
  return {
    id: row.id,
    name: row.name,
    currency: row.currency,
    createdAt: Number(row.created_at),
  };
}

function getAccounts(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  return SELECT_ALL_ACCOUNTS_STMT.all(uid).map(rowToAccount);
}

function replaceAccountsFromList(accounts, userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  const list = normalizeAccountRecords(accounts);
  const now = nowMs();
  const tx = db.transaction(() => {
    const ids = new Set(list.map((a) => a.id));
    for (const a of list) {
      UPSERT_ACCOUNT_STMT.run({
        user_id: uid,
        id: a.id,
        name: a.name,
        currency: a.currency,
        created_at: a.createdAt,
        updated_at: now,
      });
    }
    const allIds = db.prepare("SELECT id FROM accounts WHERE user_id = ?").all(uid).map((r) => r.id);
    for (const id of allIds) {
      if (ids.has(id)) {
        continue;
      }
      const { c } = db.prepare("SELECT COUNT(*) AS c FROM trades WHERE user_id = ? AND account_id = ?").get(
        uid,
        id
      );
      if (Number(c) === 0 && id !== "default") {
        DELETE_ACCOUNT_STMT.run(uid, id);
      }
    }
    db.prepare("DELETE FROM app_settings WHERE user_id = ? AND key = 'accounts'").run(uid);
  });
  tx();
}

function normalizeDailyReturn(input) {
  const raw = input || {};
  const accountId = String(raw.accountId || raw.account_id || "default").trim() || "default";
  const date = toDateKey(raw.date || raw.day);
  const profit = validNumber(raw.profit, raw.pnl, raw.earning, 0);
  const returnRate = validNumber(raw.returnRate, raw.return_rate, raw.rate, 0);
  let totalAsset = null;
  if (raw.totalAsset != null || raw.total_asset != null) {
    const n = Number(raw.totalAsset != null ? raw.totalAsset : raw.total_asset);
    totalAsset = Number.isFinite(n) ? n : null;
  }
  const createdAt = validNumber(raw.createdAt, raw.created_at, nowMs());
  return {
    accountId,
    date,
    profit,
    returnRate,
    totalAsset,
    createdAt,
  };
}

function rowToDailyReturn(row) {
  const totalAsset = row.total_asset == null ? null : Number(row.total_asset);
  return {
    accountId: row.account_id,
    date: row.date,
    profit: Number(row.profit),
    returnRate: Number(row.return_rate),
    totalAsset: Number.isFinite(totalAsset) ? totalAsset : null,
    createdAt: Number(row.created_at),
  };
}

function getDailyReturns(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const from = query.from != null && String(query.from).trim() ? String(query.from).trim() : "";
  const to = query.to != null && String(query.to).trim() ? String(query.to).trim() : "";
  if (!accountId && !from && !to) {
    return SELECT_ALL_DAILY_RETURNS_STMT.all(uid).map(rowToDailyReturn);
  }
  const fromBound = from || "1970-01-01";
  const toBound = to || "9999-12-31";
  return SELECT_DAILY_RETURNS_RANGE_STMT.all({
    user_id: uid,
    account_id: accountId,
    from: fromBound,
    to: toBound,
  }).map(rowToDailyReturn);
}

function upsertDailyReturn(input, userId) {
  const uid = String(userId || "").trim();
  const safe = normalizeDailyReturn(input);
  const updatedAt = nowMs();
  UPSERT_DAILY_RETURN_STMT.run({
    user_id: uid,
    account_id: safe.accountId,
    date: safe.date,
    profit: safe.profit,
    return_rate: safe.returnRate,
    total_asset: safe.totalAsset,
    created_at: safe.createdAt,
    updated_at: updatedAt,
  });
  return safe;
}

const replaceAllDailyReturnsTx = db.transaction((rows, userId) => {
  const uid = String(userId || "").trim();
  DELETE_ALL_DAILY_RETURNS_STMT.run(uid);
  const updatedAt = nowMs();
  for (const raw of rows) {
    const safe = normalizeDailyReturn(raw);
    UPSERT_DAILY_RETURN_STMT.run({
      user_id: uid,
      account_id: safe.accountId,
      date: safe.date,
      profit: safe.profit,
      return_rate: safe.returnRate,
      total_asset: safe.totalAsset,
      created_at: safe.createdAt,
      updated_at: updatedAt,
    });
  }
});

function importDailyReturns(rows, mode = "append", userId = null) {
  const uid = String(userId || getCliUserId()).trim();
  const list = Array.isArray(rows) ? rows.map(normalizeDailyReturn) : [];
  if (mode === "replace") {
    replaceAllDailyReturnsTx(list, uid);
  } else {
    const updatedAt = nowMs();
    for (const safe of list) {
      UPSERT_DAILY_RETURN_STMT.run({
        user_id: uid,
        account_id: safe.accountId,
        date: safe.date,
        profit: safe.profit,
        return_rate: safe.returnRate,
        total_asset: safe.totalAsset,
        created_at: safe.createdAt,
        updated_at: updatedAt,
      });
    }
  }
  return getDailyReturns({}, uid);
}

function deleteDailyReturn(accountId, date, userId) {
  const uid = String(userId || "").trim();
  const res = DELETE_DAILY_RETURN_STMT.run(uid, String(accountId || ""), String(date || ""));
  return res.changes > 0;
}

function getSettings(userId) {
  const uid = String(userId || "").trim();
  const settings = { ...DEFAULT_SETTINGS };
  if (!uid) {
    settings.accounts = [];
    return settings;
  }
  const rows = SELECT_ALL_SETTINGS_STMT.all(uid);
  for (const row of rows) {
    if (row.key === "accounts") {
      continue;
    }
    if (!(row.key in settings)) {
      continue;
    }
    try {
      settings[row.key] = JSON.parse(row.value);
    } catch {
      settings[row.key] = row.value;
    }
  }
  settings.accounts = getAccounts(uid);
  return settings;
}

function setSettings(partial, userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return getSettings("");
  }
  if (!partial || typeof partial !== "object") {
    return getSettings(uid);
  }
  const updatedAt = nowMs();
  for (const [key, value] of Object.entries(partial)) {
    if (!(key in DEFAULT_SETTINGS)) {
      continue;
    }
    if (key === "accounts") {
      if (Array.isArray(value)) {
        replaceAccountsFromList(value, uid);
      }
      continue;
    }
    UPSERT_SETTING_STMT.run({
      user_id: uid,
      key,
      value: JSON.stringify(value),
      updated_at: updatedAt,
    });
  }
  return getSettings(uid);
}

const SELECT_SYMBOL_DAILY_RANGE_STMT = db.prepare(`
SELECT account_id, symbol, date, eod_shares, day_trade_qty, day_trade_amount, day_close_price, day_pnl_native, currency, created_at
FROM symbol_daily_pnl
WHERE user_id = @user_id
  AND (@account_id = '' OR account_id = @account_id)
  AND date >= @from
  AND date <= @to
  AND (@symbol = '' OR symbol = @symbol)
ORDER BY date ASC, symbol ASC
`);

const UPSERT_SYMBOL_DAILY_STMT = db.prepare(`
INSERT INTO symbol_daily_pnl (
  user_id, account_id, symbol, date, eod_shares, day_trade_qty, day_trade_amount, day_close_price, day_pnl_native, currency, created_at, updated_at
) VALUES (
  @user_id, @account_id, @symbol, @date, @eod_shares, @day_trade_qty, @day_trade_amount, @day_close_price, @day_pnl_native, @currency, @created_at, @updated_at
)
ON CONFLICT(user_id, account_id, symbol, date) DO UPDATE SET
  eod_shares = excluded.eod_shares,
  day_trade_qty = excluded.day_trade_qty,
  day_trade_amount = excluded.day_trade_amount,
  day_close_price = excluded.day_close_price,
  day_pnl_native = excluded.day_pnl_native,
  currency = excluded.currency,
  updated_at = excluded.updated_at
`);

const SELECT_ANALYSIS_DAILY_RANGE_STMT = db.prepare(`
SELECT account_id, date, profit_cny, rate_cost, rate_twr, rate_dietz,
  total_profit, total_rate_cost, total_rate_twr, total_rate_dietz,
  principal, market_value, fx_hkd_cny, fx_usd_cny, created_at
FROM analysis_daily_snapshot
WHERE user_id = @user_id
  AND (@account_id = '' OR account_id = @account_id)
  AND date >= @from
  AND date <= @to
ORDER BY date ASC
`);

const UPSERT_ANALYSIS_DAILY_STMT = db.prepare(`
INSERT INTO analysis_daily_snapshot (
  user_id, account_id, date, profit_cny, rate_cost, rate_twr, rate_dietz,
  total_profit, total_rate_cost, total_rate_twr, total_rate_dietz,
  principal, market_value, fx_hkd_cny, fx_usd_cny, created_at, updated_at
) VALUES (
  @user_id, @account_id, @date, @profit_cny, @rate_cost, @rate_twr, @rate_dietz,
  @total_profit, @total_rate_cost, @total_rate_twr, @total_rate_dietz,
  @principal, @market_value, @fx_hkd_cny, @fx_usd_cny, @created_at, @updated_at
)
ON CONFLICT(user_id, account_id, date) DO UPDATE SET
  profit_cny = excluded.profit_cny,
  rate_cost = excluded.rate_cost,
  rate_twr = excluded.rate_twr,
  rate_dietz = excluded.rate_dietz,
  total_profit = excluded.total_profit,
  total_rate_cost = excluded.total_rate_cost,
  total_rate_twr = excluded.total_rate_twr,
  total_rate_dietz = excluded.total_rate_dietz,
  principal = excluded.principal,
  market_value = excluded.market_value,
  fx_hkd_cny = excluded.fx_hkd_cny,
  fx_usd_cny = excluded.fx_usd_cny,
  updated_at = excluded.updated_at
`);

const UPSERT_SYMBOL_DAILY_CLOSE_STMT = db.prepare(`
INSERT INTO symbol_daily_close (symbol, date, close, source, updated_at)
VALUES (@symbol, @date, @close, @source, @updated_at)
ON CONFLICT(symbol, date) DO UPDATE SET
  close = excluded.close,
  source = excluded.source,
  updated_at = excluded.updated_at
`);

const SELECT_SYMBOL_DAILY_CLOSE_RANGE_STMT = db.prepare(`
SELECT date, close, source FROM symbol_daily_close
WHERE symbol = ? AND date >= ? AND date <= ?
ORDER BY date ASC
`);

function addCalendarDays(dateStr, delta) {
  const base = String(dateStr || "").slice(0, 10);
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(base);
  if (!m) {
    return base;
  }
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  d.setDate(d.getDate() + Number(delta) || 0);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function upsertSymbolDailyCloseBatch(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) {
    return 0;
  }
  const now = nowMs();
  const tx = db.transaction(() => {
    for (const raw of list) {
      const r = raw || {};
      const sym = normalizeSymbol(r.symbol);
      if (!sym || !Number.isFinite(Number(r.close))) {
        continue;
      }
      UPSERT_SYMBOL_DAILY_CLOSE_STMT.run({
        symbol: sym,
        date: toDateKey(r.date),
        close: validNumber(r.close, 0),
        source: String(r.source || "").slice(0, 64),
        updated_at: now,
      });
    }
  });
  tx();
  return list.length;
}

function getSymbolDailyCloseRange(symbol, fromDate, toDate) {
  const sym = normalizeSymbol(symbol);
  if (!sym) {
    return [];
  }
  const from = fromDate && String(fromDate).trim() ? String(fromDate).trim() : "1970-01-01";
  const to = toDate && String(toDate).trim() ? String(toDate).trim() : "9999-12-31";
  return SELECT_SYMBOL_DAILY_CLOSE_RANGE_STMT.all(sym, from, to).map((row) => ({
    date: row.date,
    close: Number(row.close),
    source: row.source || "",
  }));
}

/** 全部成交里出现过的标的：首日−1 ～ max(末次+1, 今天) */
function getTradeWindowForDailyClose(userId) {
  const trades = getTrades(userId);
  if (!trades.length) {
    return { symbols: [], from: null, to: null };
  }
  let minD = trades[0].date;
  let maxD = trades[0].date;
  const set = new Set();
  for (const t of trades) {
    if (t.date < minD) {
      minD = t.date;
    }
    if (t.date > maxD) {
      maxD = t.date;
    }
    const s = normalizeSymbol(t.symbol);
    if (s) {
      set.add(s);
    }
  }
  const today = toDateKey(new Date());
  const from = addCalendarDays(minD, -1);
  let to = addCalendarDays(maxD, 1);
  if (to < today) {
    to = today;
  }
  return { symbols: [...set].sort(), from, to };
}

function getSymbolDailyPnl(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const from = query.from != null && String(query.from).trim() ? String(query.from).trim() : "1970-01-01";
  const to = query.to != null && String(query.to).trim() ? String(query.to).trim() : "9999-12-31";
  const symbol =
    query.symbol != null && String(query.symbol).trim()
      ? normalizeSymbol(String(query.symbol).trim())
      : "";
  return SELECT_SYMBOL_DAILY_RANGE_STMT.all({
    user_id: uid,
    account_id: accountId,
    from,
    to,
    symbol,
  }).map((row) => ({
    accountId: row.account_id,
    symbol: row.symbol,
    date: row.date,
    eodShares: Number(row.eod_shares),
    dayTradeQty: Number(row.day_trade_qty),
    dayTradeAmount: Number(row.day_trade_amount),
    dayClosePrice: row.day_close_price == null ? null : Number(row.day_close_price),
    dayPnlNative: Number(row.day_pnl_native),
    currency: row.currency,
    createdAt: Number(row.created_at),
  }));
}

function upsertSymbolDailyPnlBatch(rows, userId = null) {
  const uid = String(userId || getCliUserId()).trim();
  const list = Array.isArray(rows) ? rows : [];
  const now = nowMs();
  const tx = db.transaction(() => {
    for (const raw of list) {
      const r = raw || {};
      UPSERT_SYMBOL_DAILY_STMT.run({
        user_id: uid,
        account_id: String(r.accountId || r.account_id || "default").trim() || "default",
        symbol: String(r.symbol || "").trim().toLowerCase(),
        date: toDateKey(r.date),
        eod_shares: validNumber(r.eodShares, r.eod_shares, 0),
        day_trade_qty: validNumber(r.dayTradeQty, r.day_trade_qty, 0),
        day_trade_amount: validNumber(r.dayTradeAmount, r.day_trade_amount, 0),
        day_close_price:
          r.dayClosePrice != null || r.day_close_price != null
            ? validNumber(r.dayClosePrice, r.day_close_price, 0)
            : null,
        day_pnl_native: validNumber(r.dayPnlNative, r.day_pnl_native, 0),
        currency: String(r.currency || "CNY").toUpperCase().slice(0, 3) || "CNY",
        created_at: validNumber(r.createdAt, r.created_at, now),
        updated_at: now,
      });
    }
  });
  tx();
}

function getAnalysisDailySnapshots(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const from = query.from != null && String(query.from).trim() ? String(query.from).trim() : "1970-01-01";
  const to = query.to != null && String(query.to).trim() ? String(query.to).trim() : "9999-12-31";
  return SELECT_ANALYSIS_DAILY_RANGE_STMT.all({ user_id: uid, account_id: accountId, from, to }).map((row) => ({
    accountId: row.account_id,
    date: row.date,
    profitCny: Number(row.profit_cny),
    rateCost: Number(row.rate_cost),
    rateTwr: Number(row.rate_twr),
    rateDietz: Number(row.rate_dietz),
    totalProfit: Number(row.total_profit),
    totalRateCost: Number(row.total_rate_cost),
    totalRateTwr: Number(row.total_rate_twr),
    totalRateDietz: Number(row.total_rate_dietz),
    principal: Number(row.principal),
    marketValue: Number(row.market_value),
    fxHkdCny: row.fx_hkd_cny == null ? null : Number(row.fx_hkd_cny),
    fxUsdCny: row.fx_usd_cny == null ? null : Number(row.fx_usd_cny),
    createdAt: Number(row.created_at),
  }));
}

function upsertAnalysisDailySnapshot(input, userId = null) {
  const uid = String(userId || getCliUserId()).trim();
  const r = input || {};
  const now = nowMs();
  const row = {
    user_id: uid,
    account_id: String(r.accountId || r.account_id || "default").trim() || "default",
    date: toDateKey(r.date),
    profit_cny: validNumber(r.profitCny, r.profit_cny, 0),
    rate_cost: validNumber(r.rateCost, r.rate_cost, 0),
    rate_twr: validNumber(r.rateTwr, r.rate_twr, 0),
    rate_dietz: validNumber(r.rateDietz, r.rate_dietz, 0),
    total_profit: validNumber(r.totalProfit, r.total_profit, 0),
    total_rate_cost: validNumber(r.totalRateCost, r.total_rate_cost, 0),
    total_rate_twr: validNumber(r.totalRateTwr, r.total_rate_twr, 0),
    total_rate_dietz: validNumber(r.totalRateDietz, r.total_rate_dietz, 0),
    principal: validNumber(r.principal, 0),
    market_value: validNumber(r.marketValue, r.market_value, 0),
    fx_hkd_cny: r.fxHkdCny != null || r.fx_hkd_cny != null ? validNumber(r.fxHkdCny, r.fx_hkd_cny) : null,
    fx_usd_cny: r.fxUsdCny != null || r.fx_usd_cny != null ? validNumber(r.fxUsdCny, r.fx_usd_cny) : null,
    created_at: validNumber(r.createdAt, r.created_at, now),
    updated_at: now,
  };
  UPSERT_ANALYSIS_DAILY_STMT.run(row);
  return row;
}

function getState(userId) {
  const uid = String(userId || "").trim();
  return {
    ...getSettings(uid),
    trades: getTrades(uid),
    dailyReturns: getDailyReturns({}, uid),
  };
}

function closeDatabase() {
  db.close();
}

function deleteAllSymbolDailyPnl(userId = null) {
  const uid = String(userId || getCliUserId()).trim();
  db.prepare("DELETE FROM symbol_daily_pnl WHERE user_id = ?").run(uid);
}

function deleteAllAnalysisDailySnapshot(userId = null) {
  const uid = String(userId || getCliUserId()).trim();
  db.prepare("DELETE FROM analysis_daily_snapshot WHERE user_id = ?").run(uid);
}

function findUserByPhone(phone) {
  const p = String(phone || "").trim();
  if (!p) {
    return null;
  }
  return db.prepare("SELECT id, phone, created_at FROM users WHERE phone = ?").get(p) || null;
}

function verifyUserLogin(phone, passwordPlain) {
  const p = String(phone || "").trim();
  if (!isValidPhone(p) || !isValidPasswordDigits(passwordPlain)) {
    return null;
  }
  const row = db.prepare("SELECT id, password_hash FROM users WHERE phone = ?").get(p);
  if (!row || !verifyPassword(passwordPlain, row.password_hash)) {
    return null;
  }
  return { id: row.id, phone: p };
}

function createRegisteredUser(phone, passwordPlain) {
  const p = String(phone || "").trim();
  if (!isValidPhone(p) || !isValidPasswordDigits(passwordPlain)) {
    throw new Error("invalid phone or password");
  }
  if (findUserByPhone(p)) {
    throw new Error("phone already registered");
  }
  const id = randomUUID();
  const now = nowMs();
  db.prepare(
    "INSERT INTO users (id, phone, password_hash, created_at, updated_at) VALUES (?,?,?,?,?)"
  ).run(id, p, hashPassword(passwordPlain), now, now);
  migrateAccountsIfEmptyForUser(id);
  return { id, phone: p };
}

function updateUserPassword(userId, newPasswordPlain) {
  const uid = String(userId || "").trim();
  if (!uid || !isValidPasswordDigits(newPasswordPlain)) {
    throw new Error("invalid password");
  }
  const now = nowMs();
  const res = db
    .prepare("UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?")
    .run(hashPassword(newPasswordPlain), now, uid);
  return res.changes > 0;
}

function verifyUserPasswordById(userId, passwordPlain) {
  const uid = String(userId || "").trim();
  const row = db.prepare("SELECT password_hash FROM users WHERE id = ?").get(uid);
  if (!row) {
    return false;
  }
  return verifyPassword(passwordPlain, row.password_hash);
}

function getUserPhone(userId) {
  const uid = String(userId || "").trim();
  const row = db.prepare("SELECT phone FROM users WHERE id = ?").get(uid);
  return row ? String(row.phone) : "";
}

const SELECT_USER_COMMUNITY_STMT = db.prepare(`
SELECT id, phone, nickname, community_public FROM users WHERE id = ?
`);

const UPDATE_USER_COMMUNITY_STMT = db.prepare(`
UPDATE users SET nickname = ?, community_public = ?, updated_at = ? WHERE id = ?
`);

const INSERT_FOLLOW_STMT = db.prepare(`
INSERT INTO community_follows (follower_id, followee_id, created_at) VALUES (?, ?, ?)
`);

const DELETE_FOLLOW_STMT = db.prepare(`
DELETE FROM community_follows WHERE follower_id = ? AND followee_id = ?
`);

const SELECT_FOLLOWEES_STMT = db.prepare(`
SELECT followee_id FROM community_follows WHERE follower_id = ?
`);

const SELECT_LEADERBOARD_CACHE_STMT = db.prepare(`
SELECT payload, updated_at FROM community_leaderboard_cache WHERE id = 1
`);

const UPSERT_LEADERBOARD_CACHE_STMT = db.prepare(`
INSERT INTO community_leaderboard_cache (id, payload, updated_at) VALUES (1, ?, ?)
  ON CONFLICT(id) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at
`);

function getUserCommunityRow(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return null;
  }
  return SELECT_USER_COMMUNITY_STMT.get(uid) || null;
}

function updateUserCommunityProfile(userId, { nickname, communityPublic }) {
  const uid = String(userId || "").trim();
  if (!uid) {
    throw new Error("userId required");
  }
  const row = getUserCommunityRow(uid);
  if (!row) {
    throw new Error("user not found");
  }
  const now = nowMs();
  let nick = row.nickname;
  if (nickname !== undefined) {
    const t = String(nickname || "").trim();
    if (t.length > 20) {
      throw new Error("nickname too long");
    }
    nick = t.length ? t : null;
    if (nick) {
      const clash = db.prepare("SELECT id FROM users WHERE nickname = ? AND id != ?").get(nick, uid);
      if (clash) {
        throw new Error("nickname taken");
      }
    }
  }
  let pub = row.community_public != null ? Number(row.community_public) : 1;
  if (communityPublic !== undefined) {
    pub = communityPublic ? 1 : 0;
  }
  UPDATE_USER_COMMUNITY_STMT.run(nick, pub, now, uid);
  db.prepare("DELETE FROM community_leaderboard_cache").run();
  return getUserCommunityRow(uid);
}

function setCommunityFollow(followerId, followeeId) {
  const a = String(followerId || "").trim();
  const b = String(followeeId || "").trim();
  if (!a || !b || a === b) {
    return false;
  }
  const now = nowMs();
  try {
    INSERT_FOLLOW_STMT.run(a, b, now);
    return true;
  } catch {
    return false;
  }
}

function removeCommunityFollow(followerId, followeeId) {
  const a = String(followerId || "").trim();
  const b = String(followeeId || "").trim();
  return DELETE_FOLLOW_STMT.run(a, b).changes > 0;
}

function listCommunityFolloweeIds(followerId) {
  const a = String(followerId || "").trim();
  if (!a) {
    return [];
  }
  return SELECT_FOLLOWEES_STMT.all(a).map((r) => r.followee_id);
}

function isCommunityFollowing(followerId, followeeId) {
  const row = db
    .prepare("SELECT 1 AS x FROM community_follows WHERE follower_id = ? AND followee_id = ?")
    .get(String(followerId), String(followeeId));
  return Boolean(row);
}

function getCommunityLeaderboardCache() {
  return SELECT_LEADERBOARD_CACHE_STMT.get();
}

function setCommunityLeaderboardCache(payloadJson, updatedAt) {
  UPSERT_LEADERBOARD_CACHE_STMT.run(payloadJson, updatedAt);
}

function selectAnalysisSnapshotsFrom(userId, accountId, fromDate) {
  const uid = String(userId || "").trim();
  const acc = String(accountId || "all");
  const from = String(fromDate || "1970-01-01");
  return db
    .prepare(
      `SELECT date, total_rate_twr, profit_cny, market_value
       FROM analysis_daily_snapshot
       WHERE user_id = ? AND account_id = ? AND date >= ?
       ORDER BY date ASC`
    )
    .all(uid, acc, from);
}

function selectLatestSymbolDailyDate(userId, accountId) {
  const uid = String(userId || "").trim();
  const acc = String(accountId || "all");
  const row = db
    .prepare(
      `SELECT MAX(date) AS d FROM symbol_daily_pnl WHERE user_id = ? AND account_id = ?`
    )
    .get(uid, acc);
  return row?.d ? String(row.d) : null;
}

function selectTopSymbolDailyByDate(userId, accountId, date, limit) {
  const uid = String(userId || "").trim();
  const acc = String(accountId || "all");
  const dk = String(date || "");
  const lim = Math.min(20, Math.max(1, Number(limit) || 3));
  return db
    .prepare(
      `SELECT symbol, eod_shares, day_close_price, currency, day_pnl_native
       FROM symbol_daily_pnl
       WHERE user_id = ? AND account_id = ? AND date = ?
       ORDER BY abs(eod_shares * COALESCE(day_close_price, 0)) DESC
       LIMIT ?`
    )
    .all(uid, acc, dk, lim);
}

function getCommunityFeedTradesRecent(_viewerId, limit = 50) {
  const lim = Math.min(2000, Math.max(1, Number(limit) || 50));
  const rows = db
    .prepare(
      `SELECT t.id, t.user_id, t.symbol, t.name, t.price, t.quantity, t.amount, t.trade_date, t.note, t.side, t.created_at
       FROM trades t
       INNER JOIN users u ON u.id = t.user_id
       WHERE IFNULL(u.community_public, 1) = 1
         AND t.type = 'trade'
       ORDER BY t.trade_date DESC, t.created_at DESC
       LIMIT ?`
    )
    .all(lim);
  return rows.map((row) => ({
    id: row.id,
    userId: row.user_id,
    symbol: row.symbol,
    name: row.name,
    price: Number(row.price),
    quantity: Number(row.quantity),
    amount: Number(row.amount),
    date: row.trade_date,
    note: row.note || "",
    side: row.side,
    createdAt: Number(row.created_at),
  }));
}

function listPublicCommunityUserIds() {
  return db
    .prepare(
      `SELECT id FROM users WHERE IFNULL(community_public, 1) = 1`
    )
    .all()
    .map((r) => r.id);
}

function selectSymbolDailyPositionsOnDate(userId, accountId, date) {
  const uid = String(userId || "").trim();
  const acc = String(accountId || "all");
  const dk = String(date || "");
  return db
    .prepare(
      `SELECT symbol, eod_shares, day_close_price, currency, day_pnl_native
       FROM symbol_daily_pnl
       WHERE user_id = ? AND account_id = ? AND date = ? AND eod_shares > 0.0001
       ORDER BY abs(eod_shares * COALESCE(day_close_price, 0)) DESC`
    )
    .all(uid, acc, dk);
}

module.exports = {
  DEFAULT_SETTINGS,
  DB_PATH,
  SEED_USER_PHONE,
  normalizeSymbol,
  normalizeTrade,
  normalizeAccountRecords,
  normalizeDailyReturn,
  getTrades,
  upsertTrade,
  importTrades,
  deleteTradeById,
  getAccounts,
  replaceAccountsFromList,
  getDailyReturns,
  upsertDailyReturn,
  importDailyReturns,
  deleteDailyReturn,
  getSettings,
  setSettings,
  getState,
  getSymbolDailyPnl,
  upsertSymbolDailyPnlBatch,
  getAnalysisDailySnapshots,
  upsertAnalysisDailySnapshot,
  deleteAllSymbolDailyPnl,
  deleteAllAnalysisDailySnapshot,
  upsertSymbolDailyCloseBatch,
  getSymbolDailyCloseRange,
  getTradeWindowForDailyClose,
  addCalendarDays,
  closeDatabase,
  getCliUserId,
  findUserByPhone,
  verifyUserLogin,
  createRegisteredUser,
  updateUserPassword,
  verifyUserPasswordById,
  getUserPhone,
  isValidPhone,
  isValidPasswordDigits,
  getUserCommunityRow,
  updateUserCommunityProfile,
  setCommunityFollow,
  removeCommunityFollow,
  listCommunityFolloweeIds,
  isCommunityFollowing,
  getCommunityLeaderboardCache,
  setCommunityLeaderboardCache,
  selectAnalysisSnapshotsFrom,
  selectLatestSymbolDailyDate,
  selectTopSymbolDailyByDate,
  getCommunityFeedTradesRecent,
  listPublicCommunityUserIds,
  selectSymbolDailyPositionsOnDate,
};
