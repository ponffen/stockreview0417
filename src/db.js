const { Pool } = require("pg");
const {
  hashPassword,
  verifyPassword,
  isValidPhone,
  isValidPasswordDigits,
} = require("./password");
const {
  SEED_USER_PHONE,
  DEFAULT_SETTINGS,
  randomUUID,
  nowMs,
  toDateKey,
  validNumber,
  normalizeAccountRecords,
  normalizeSymbol,
  normalizeTrade,
  tradeToRow,
  rowToTrade,
  rowToAccount,
  normalizeDailyReturn,
  rowToDailyReturn,
  normalizeCashTransfer,
  cashTransferToRow,
  rowToCashTransfer,
  addCalendarDays,
} = require("./db-pure");

/** Vercel Marketplace / Neon 可能注入 POSTGRES_URL；统一取连接串 */
function getDatabaseUrl() {
  return (
    process.env.DATABASE_URL ||
    process.env.POSTGRES_URL ||
    process.env.POSTGRES_PRISMA_URL ||
    ""
  );
}

const DB_PATH = getDatabaseUrl() ? "[postgresql]" : "";

let pool;
let initPromise;
let postInitTasksStarted = false;
let isBootstrapping = false;

function getSslOption() {
  if (process.env.DATABASE_SSL === "0") {
    return false;
  }
  const u = String(getDatabaseUrl() || "");
  if (/localhost|127\.0\.0\.1/.test(u)) {
    return false;
  }
  return { rejectUnauthorized: false };
}

const DDL = [
  `CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    phone TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    nickname TEXT,
    community_public INTEGER NOT NULL DEFAULT 1
  )`,
  `CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    account_id TEXT NOT NULL DEFAULT 'default',
    type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    side TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL DEFAULT 0,
    quantity DOUBLE PRECISION NOT NULL DEFAULT 0,
    amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    trade_date TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_trades_user_id ON trades (user_id)`,
  `CREATE INDEX IF NOT EXISTS idx_trades_trade_date_created_at ON trades (trade_date ASC, created_at ASC)`,
  `CREATE TABLE IF NOT EXISTS app_settings (
    user_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, key)
  )`,
  `CREATE TABLE IF NOT EXISTS accounts (
    user_id TEXT NOT NULL,
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    currency TEXT NOT NULL DEFAULT 'CNY',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, id)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_accounts_created_at ON accounts (created_at ASC)`,
  `CREATE TABLE IF NOT EXISTS daily_returns (
    user_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    date TEXT NOT NULL,
    profit DOUBLE PRECISION NOT NULL DEFAULT 0,
    return_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_asset DOUBLE PRECISION,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, account_id, date)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_daily_returns_date ON daily_returns (date ASC)`,
  `CREATE TABLE IF NOT EXISTS symbol_daily_pnl (
    user_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    eod_shares DOUBLE PRECISION NOT NULL DEFAULT 0,
    day_trade_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
    day_trade_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    day_close_price DOUBLE PRECISION,
    day_pnl_native DOUBLE PRECISION NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'CNY',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, account_id, symbol, date)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_symbol_daily_pnl_date ON symbol_daily_pnl (date ASC)`,
  `CREATE TABLE IF NOT EXISTS analysis_daily_snapshot (
    user_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    date TEXT NOT NULL,
    profit_cny DOUBLE PRECISION NOT NULL DEFAULT 0,
    rate_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    rate_twr DOUBLE PRECISION NOT NULL DEFAULT 0,
    rate_dietz DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_profit DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_rate_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_rate_twr DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_rate_dietz DOUBLE PRECISION NOT NULL DEFAULT 0,
    principal DOUBLE PRECISION NOT NULL DEFAULT 0,
    market_value DOUBLE PRECISION NOT NULL DEFAULT 0,
    fx_hkd_cny DOUBLE PRECISION,
    fx_usd_cny DOUBLE PRECISION,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, account_id, date)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_analysis_daily_snapshot_date ON analysis_daily_snapshot (date ASC)`,
  `CREATE TABLE IF NOT EXISTS symbol_daily_close (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    source TEXT,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (symbol, date)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_symbol_daily_close_date ON symbol_daily_close (date ASC)`,
  `CREATE TABLE IF NOT EXISTS community_follows (
    follower_id TEXT NOT NULL,
    followee_id TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (follower_id, followee_id)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_community_follows_followee ON community_follows (followee_id)`,
  `CREATE TABLE IF NOT EXISTS community_leaderboard_cache (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload TEXT NOT NULL,
    updated_at BIGINT NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS cash_transfers (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    transfer_date TEXT NOT NULL,
    direction TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    note TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_cash_transfers_user_date ON cash_transfers (user_id, transfer_date ASC, created_at ASC)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_users_nickname_nonnull
   ON users (nickname)
   WHERE nickname IS NOT NULL AND length(trim(nickname)) > 0`,
];

async function initPool() {
  if (initPromise) {
    return initPromise;
  }
  const dbUrl = getDatabaseUrl();
  if (!dbUrl) {
    throw new Error(
      "Database URL is required: set DATABASE_URL or connect Postgres in Vercel (POSTGRES_URL is used automatically when present)."
    );
  }
  initPromise = (async () => {
    isBootstrapping = true;
    pool = new Pool({
      connectionString: dbUrl,
      max: 20,
      ssl: getSslOption(),
      connectionTimeoutMillis: 15_000,
      idleTimeoutMillis: 60_000,
    });
    const c = await pool.connect();
    try {
      for (const sql of DDL) {
        await c.query(sql);
      }
      await ensureSeedUserRowWithClient(c);
    } finally {
      c.release();
      isBootstrapping = false;
    }
    startPostInitTasks();
    return pool;
  })();
  return initPromise;
}

function startPostInitTasks() {
  if (postInitTasksStarted) {
    return;
  }
  postInitTasksStarted = true;
  // 冷启动首个请求（含登录/注册）只做最小必要初始化，重迁移改为后台执行，避免函数长时间 pending。
  setImmediate(async () => {
    try {
      await migrateAllUsersAccountsIfEmpty();
      await migrateTradeSymbolsToNormalized();
    } catch {
      // ignore background migration errors to keep API responsive
    }
  });
}

async function q(text, params = []) {
  if (isBootstrapping && pool) {
    return pool.query(text, params);
  }
  const p = await initPool();
  return p.query(text, params);
}

const SEED_USER_PASSWORD = "123456";

async function ensureSeedUserRowWithClient(client) {
  const check = await client.query("SELECT id FROM users WHERE phone = $1", [SEED_USER_PHONE]);
  if (check.rows.length) {
    return check.rows[0].id;
  }
  const id = randomUUID();
  const now = nowMs();
  await client.query(
    `INSERT INTO users (id, phone, password_hash, created_at, updated_at) VALUES ($1,$2,$3,$4,$5)`,
    [id, SEED_USER_PHONE, hashPassword(SEED_USER_PASSWORD), now, now]
  );
  return id;
}

async function ensureSeedUserRow() {
  const { rows: existing } = await q("SELECT id FROM users WHERE phone = $1", [SEED_USER_PHONE]);
  if (existing.length) {
    return existing[0].id;
  }
  const id = randomUUID();
  const now = nowMs();
  await q(
    `INSERT INTO users (id, phone, password_hash, created_at, updated_at) VALUES ($1,$2,$3,$4,$5)`,
    [id, SEED_USER_PHONE, hashPassword(SEED_USER_PASSWORD), now, now]
  );
  return id;
}

async function migrateAccountsIfEmptyForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  const { rows: cRows } = await q("SELECT COUNT(*)::int AS c FROM accounts WHERE user_id = $1", [uid]);
  if (Number(cRows[0].c) > 0) {
    return;
  }
  let list = [{ id: "default", name: "默认账户", currency: "CNY", createdAt: 0 }];
  const { rows: sRows } = await q("SELECT value FROM app_settings WHERE user_id = $1 AND key = $2", [
    uid,
    "accounts",
  ]);
  if (sRows[0] && sRows[0].value) {
    try {
      const parsed = JSON.parse(sRows[0].value);
      if (Array.isArray(parsed) && parsed.length) {
        list = parsed;
      }
    } catch {
      // ignore
    }
  }
  const now = nowMs();
  const p = await initPool();
  const cl = await p.connect();
  try {
    await cl.query("BEGIN");
    for (const acc of normalizeAccountRecords(list)) {
      await cl.query(
        `INSERT INTO accounts (user_id, id, name, currency, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (user_id, id) DO UPDATE SET
           name = EXCLUDED.name, currency = EXCLUDED.currency, updated_at = EXCLUDED.updated_at`,
        [uid, acc.id, acc.name, acc.currency, acc.createdAt, now]
      );
    }
    await cl.query("COMMIT");
  } catch (e) {
    await cl.query("ROLLBACK");
    throw e;
  } finally {
    cl.release();
  }
}

async function migrateAllUsersAccountsIfEmpty() {
  const { rows } = await q("SELECT id FROM users");
  for (const r of rows) {
    await migrateAccountsIfEmptyForUser(r.id);
  }
}

async function migrateTradeSymbolsToNormalized() {
  try {
    const { rows } = await q("SELECT id, user_id, symbol FROM trades");
    const now = nowMs();
    let updated = 0;
    for (const row of rows) {
      const next = normalizeSymbol(row.symbol);
      if (next && next !== row.symbol) {
        await q("UPDATE trades SET symbol = $1, updated_at = $2 WHERE user_id = $3 AND id = $4", [
          next,
          now,
          row.user_id,
          row.id,
        ]);
        updated += 1;
      }
    }
    if (process.env.STOCKREVIEW_SILENT_DB_LOG !== "1") {
      // eslint-disable-next-line no-console
      console.log(`[db] trade symbol migration: ${updated} row(s) updated, ${rows.length} trade(s) checked.`);
    }
  } catch {
    // ignore
  }
}

async function getTrades(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const { rows } = await q(
    `SELECT id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at
     FROM trades WHERE user_id = $1
     ORDER BY trade_date ASC, created_at ASC`,
    [uid]
  );
  return rows.map(rowToTrade);
}

async function upsertTrade(trade, userId) {
  const row = tradeToRow(trade, userId);
  if (!row.user_id) {
    throw new Error("userId required");
  }
  await q(
    `INSERT INTO trades (
      id, user_id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at, updated_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
    ON CONFLICT (id) DO UPDATE SET
      user_id = EXCLUDED.user_id, account_id = EXCLUDED.account_id, type = EXCLUDED.type, symbol = EXCLUDED.symbol,
      name = EXCLUDED.name, side = EXCLUDED.side, price = EXCLUDED.price, quantity = EXCLUDED.quantity,
      amount = EXCLUDED.amount, trade_date = EXCLUDED.trade_date, note = EXCLUDED.note, updated_at = EXCLUDED.updated_at`,
    [
      row.id,
      row.user_id,
      row.account_id,
      row.type,
      row.symbol,
      row.name,
      row.side,
      row.price,
      row.quantity,
      row.amount,
      row.trade_date,
      row.note,
      row.created_at,
      row.updated_at,
    ]
  );
  return normalizeTrade({ ...trade, id: row.id });
}

async function importTrades(trades, mode = "append", userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  const list = Array.isArray(trades) ? trades : [];
  const p = await initPool();
  const client = await p.connect();
  try {
    await client.query("BEGIN");
    if (mode === "replace") {
      await client.query("DELETE FROM trades WHERE user_id = $1", [uid]);
    }
    for (const trade of list) {
      const row = tradeToRow(trade, uid);
      await client.query(
        `INSERT INTO trades (
          id, user_id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (id) DO UPDATE SET
          user_id = EXCLUDED.user_id, account_id = EXCLUDED.account_id, type = EXCLUDED.type, symbol = EXCLUDED.symbol,
          name = EXCLUDED.name, side = EXCLUDED.side, price = EXCLUDED.price, quantity = EXCLUDED.quantity,
          amount = EXCLUDED.amount, trade_date = EXCLUDED.trade_date, note = EXCLUDED.note, updated_at = EXCLUDED.updated_at`,
        [
          row.id,
          row.user_id,
          row.account_id,
          row.type,
          row.symbol,
          row.name,
          row.side,
          row.price,
          row.quantity,
          row.amount,
          row.trade_date,
          row.note,
          row.created_at,
          row.updated_at,
        ]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
  return getTrades(uid);
}

async function deleteTradeById(tradeId, userId) {
  const uid = String(userId || "").trim();
  const { rowCount } = await q("DELETE FROM trades WHERE user_id = $1 AND id = $2", [uid, String(tradeId || "")]);
  return rowCount > 0;
}

async function getCashTransfers(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const { rows } = await q(
    `SELECT id, account_id, transfer_date, direction, amount, note, created_at
     FROM cash_transfers WHERE user_id = $1
     ORDER BY transfer_date DESC, created_at DESC`,
    [uid]
  );
  return rows.map(rowToCashTransfer);
}

async function upsertCashTransfer(record, userId) {
  const uid = String(userId || "").trim();
  const row = cashTransferToRow(record, userId);
  if (!row.user_id) {
    throw new Error("userId required");
  }
  await q(
    `INSERT INTO cash_transfers (id, user_id, account_id, transfer_date, direction, amount, note, created_at, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (id) DO UPDATE SET
       user_id = EXCLUDED.user_id, account_id = EXCLUDED.account_id, transfer_date = EXCLUDED.transfer_date,
       direction = EXCLUDED.direction, amount = EXCLUDED.amount, note = EXCLUDED.note, updated_at = EXCLUDED.updated_at`,
    [
      row.id,
      row.user_id,
      row.account_id,
      row.transfer_date,
      row.direction,
      row.amount,
      row.note,
      row.created_at,
      row.updated_at,
    ]
  );
  const all = await getCashTransfers(uid);
  return all.find((x) => x.id === row.id) || all[all.length - 1];
}

async function importCashTransfers(rows, mode = "append", userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  const list = Array.isArray(rows) ? rows.map(normalizeCashTransfer) : [];
  const p = await initPool();
  const client = await p.connect();
  try {
    await client.query("BEGIN");
    if (mode === "replace") {
      await client.query("DELETE FROM cash_transfers WHERE user_id = $1", [uid]);
    }
    for (const r of list) {
      const row = cashTransferToRow(r, uid);
      await client.query(
        `INSERT INTO cash_transfers (id, user_id, account_id, transfer_date, direction, amount, note, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
         ON CONFLICT (id) DO UPDATE SET
           user_id = EXCLUDED.user_id, account_id = EXCLUDED.account_id, transfer_date = EXCLUDED.transfer_date,
           direction = EXCLUDED.direction, amount = EXCLUDED.amount, note = EXCLUDED.note, updated_at = EXCLUDED.updated_at`,
        [
          row.id,
          row.user_id,
          row.account_id,
          row.transfer_date,
          row.direction,
          row.amount,
          row.note,
          row.created_at,
          row.updated_at,
        ]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
  return getCashTransfers(uid);
}

async function deleteCashTransferById(cashId, userId) {
  const uid = String(userId || "").trim();
  const { rowCount } = await q("DELETE FROM cash_transfers WHERE user_id = $1 AND id = $2", [uid, String(cashId || "")]);
  return rowCount > 0;
}

async function getAccounts(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const { rows } = await q(
    "SELECT id, name, currency, created_at FROM accounts WHERE user_id = $1 ORDER BY created_at ASC, id ASC",
    [uid]
  );
  return rows.map(rowToAccount);
}

async function replaceAccountsFromList(accounts, userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  const list = normalizeAccountRecords(accounts);
  const now = nowMs();
  const p = await initPool();
  const client = await p.connect();
  try {
    await client.query("BEGIN");
    const ids = new Set(list.map((a) => a.id));
    for (const a of list) {
      await client.query(
        `INSERT INTO accounts (user_id, id, name, currency, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (user_id, id) DO UPDATE SET
           name = EXCLUDED.name, currency = EXCLUDED.currency, updated_at = EXCLUDED.updated_at`,
        [uid, a.id, a.name, a.currency, a.createdAt, now]
      );
    }
    const { rows: allIdsRows } = await client.query("SELECT id FROM accounts WHERE user_id = $1", [uid]);
    for (const r of allIdsRows) {
      if (ids.has(r.id)) {
        continue;
      }
      const cRes = await client.query(
        "SELECT COUNT(*)::int AS c FROM trades WHERE user_id = $1 AND account_id = $2",
        [uid, r.id]
      );
      if (Number(cRes.rows[0].c) === 0 && r.id !== "default") {
        await client.query("DELETE FROM accounts WHERE user_id = $1 AND id = $2", [uid, r.id]);
      }
    }
    await client.query("DELETE FROM app_settings WHERE user_id = $1 AND key = 'accounts'", [uid]);
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function getDailyReturns(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const from = query.from != null && String(query.from).trim() ? String(query.from).trim() : "";
  const to = query.to != null && String(query.to).trim() ? String(query.to).trim() : "";
  if (!accountId && !from && !to) {
    const { rows } = await q(
      "SELECT account_id, date, profit, return_rate, total_asset, created_at FROM daily_returns WHERE user_id = $1 ORDER BY account_id ASC, date ASC",
      [uid]
    );
    return rows.map(rowToDailyReturn);
  }
  const fromBound = from || "1970-01-01";
  const toBound = to || "9999-12-31";
  const { rows } = await q(
    `SELECT account_id, date, profit, return_rate, total_asset, created_at
     FROM daily_returns
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND date >= $3 AND date <= $4
     ORDER BY date ASC`,
    [uid, accountId, fromBound, toBound]
  );
  return rows.map(rowToDailyReturn);
}

async function upsertDailyReturn(input, userId) {
  const uid = String(userId || "").trim();
  const safe = normalizeDailyReturn(input);
  const updatedAt = nowMs();
  await q(
    `INSERT INTO daily_returns (user_id, account_id, date, profit, return_rate, total_asset, created_at, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (user_id, account_id, date) DO UPDATE SET
       profit = EXCLUDED.profit, return_rate = EXCLUDED.return_rate, total_asset = EXCLUDED.total_asset, updated_at = EXCLUDED.updated_at`,
    [uid, safe.accountId, safe.date, safe.profit, safe.returnRate, safe.totalAsset, safe.createdAt, updatedAt]
  );
  return safe;
}

async function importDailyReturns(rows, mode = "append", userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  const list = Array.isArray(rows) ? rows.map(normalizeDailyReturn) : [];
  const p = await initPool();
  const client = await p.connect();
  const updatedAt = nowMs();
  try {
    await client.query("BEGIN");
    if (mode === "replace") {
      await client.query("DELETE FROM daily_returns WHERE user_id = $1", [uid]);
    }
    for (const safe of list) {
      await client.query(
        `INSERT INTO daily_returns (user_id, account_id, date, profit, return_rate, total_asset, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (user_id, account_id, date) DO UPDATE SET
           profit = EXCLUDED.profit, return_rate = EXCLUDED.return_rate, total_asset = EXCLUDED.total_asset, updated_at = EXCLUDED.updated_at`,
        [uid, safe.accountId, safe.date, safe.profit, safe.returnRate, safe.totalAsset, safe.createdAt, updatedAt]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
  return getDailyReturns({}, uid);
}

async function deleteDailyReturn(accountId, date, userId) {
  const uid = String(userId || "").trim();
  const { rowCount } = await q("DELETE FROM daily_returns WHERE user_id = $1 AND account_id = $2 AND date = $3", [
    uid,
    String(accountId || ""),
    String(date || ""),
  ]);
  return rowCount > 0;
}

async function getSettings(userId) {
  const uid = String(userId || "").trim();
  const settings = { ...DEFAULT_SETTINGS };
  if (!uid) {
    settings.accounts = [];
    return settings;
  }
  const { rows } = await q("SELECT key, value FROM app_settings WHERE user_id = $1", [uid]);
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
  settings.accounts = await getAccounts(uid);
  return settings;
}

async function setSettings(partial, userId) {
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
        await replaceAccountsFromList(value, uid);
      }
      continue;
    }
    await q(
      `INSERT INTO app_settings (user_id, key, value, updated_at) VALUES ($1,$2,$3,$4)
       ON CONFLICT (user_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at`,
      [uid, key, JSON.stringify(value), updatedAt]
    );
  }
  return getSettings(uid);
}

async function getState(userId) {
  const uid = String(userId || "").trim();
  return {
    ...(await getSettings(uid)),
    trades: await getTrades(uid),
    dailyReturns: await getDailyReturns({}, uid),
    cashTransfers: await getCashTransfers(uid),
  };
}

async function getSymbolDailyPnl(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const from = query.from != null && String(query.from).trim() ? String(query.from).trim() : "1970-01-01";
  const to = query.to != null && String(query.to).trim() ? String(query.to).trim() : "9999-12-31";
  const symbol =
    query.symbol != null && String(query.symbol).trim() ? normalizeSymbol(String(query.symbol).trim()) : "";
  const { rows } = await q(
    `SELECT account_id, symbol, date, eod_shares, day_trade_qty, day_trade_amount, day_close_price, day_pnl_native, currency, created_at
     FROM symbol_daily_pnl
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND date >= $3 AND date <= $4
       AND ($5 = '' OR symbol = $5)
     ORDER BY date ASC, symbol ASC`,
    [uid, accountId, from, to, symbol]
  );
  return rows.map((row) => ({
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

async function upsertSymbolDailyPnlBatch(rows, userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  const list = Array.isArray(rows) ? rows : [];
  const now = nowMs();
  const p = await initPool();
  const client = await p.connect();
  try {
    await client.query("BEGIN");
    for (const raw of list) {
      const r = raw || {};
      await client.query(
        `INSERT INTO symbol_daily_pnl (
           user_id, account_id, symbol, date, eod_shares, day_trade_qty, day_trade_amount, day_close_price, day_pnl_native, currency, created_at, updated_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
         ON CONFLICT (user_id, account_id, symbol, date) DO UPDATE SET
           eod_shares = EXCLUDED.eod_shares, day_trade_qty = EXCLUDED.day_trade_qty, day_trade_amount = EXCLUDED.day_trade_amount,
           day_close_price = EXCLUDED.day_close_price, day_pnl_native = EXCLUDED.day_pnl_native, currency = EXCLUDED.currency, updated_at = EXCLUDED.updated_at`,
        [
          uid,
          String(r.accountId || r.account_id || "default").trim() || "default",
          String(r.symbol || "").trim().toLowerCase(),
          toDateKey(r.date),
          validNumber(r.eodShares, r.eod_shares, 0),
          validNumber(r.dayTradeQty, r.day_trade_qty, 0),
          validNumber(r.dayTradeAmount, r.day_trade_amount, 0),
          r.dayClosePrice != null || r.day_close_price != null ? validNumber(r.dayClosePrice, r.day_close_price, 0) : null,
          validNumber(r.dayPnlNative, r.day_pnl_native, 0),
          String(r.currency || "CNY").toUpperCase().slice(0, 3) || "CNY",
          validNumber(r.createdAt, r.created_at, now),
          now,
        ]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
  return list.length;
}

async function getAnalysisDailySnapshots(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const from = query.from != null && String(query.from).trim() ? String(query.from).trim() : "1970-01-01";
  const to = query.to != null && String(query.to).trim() ? String(query.to).trim() : "9999-12-31";
  const { rows } = await q(
    `SELECT account_id, date, profit_cny, rate_cost, rate_twr, rate_dietz,
      total_profit, total_rate_cost, total_rate_twr, total_rate_dietz, principal, market_value, fx_hkd_cny, fx_usd_cny, created_at
     FROM analysis_daily_snapshot
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND date >= $3 AND date <= $4
     ORDER BY date ASC`,
    [uid, accountId, from, to]
  );
  return rows.map((row) => ({
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

async function upsertAnalysisDailySnapshot(input, userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
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
  await q(
    `INSERT INTO analysis_daily_snapshot (
       user_id, account_id, date, profit_cny, rate_cost, rate_twr, rate_dietz, total_profit, total_rate_cost, total_rate_twr, total_rate_dietz,
       principal, market_value, fx_hkd_cny, fx_usd_cny, created_at, updated_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
     ON CONFLICT (user_id, account_id, date) DO UPDATE SET
       profit_cny = EXCLUDED.profit_cny, rate_cost = EXCLUDED.rate_cost, rate_twr = EXCLUDED.rate_twr, rate_dietz = EXCLUDED.rate_dietz,
       total_profit = EXCLUDED.total_profit, total_rate_cost = EXCLUDED.total_rate_cost, total_rate_twr = EXCLUDED.total_rate_twr, total_rate_dietz = EXCLUDED.total_rate_dietz,
       principal = EXCLUDED.principal, market_value = EXCLUDED.market_value, fx_hkd_cny = EXCLUDED.fx_hkd_cny, fx_usd_cny = EXCLUDED.fx_usd_cny, updated_at = EXCLUDED.updated_at`,
    [
      row.user_id,
      row.account_id,
      row.date,
      row.profit_cny,
      row.rate_cost,
      row.rate_twr,
      row.rate_dietz,
      row.total_profit,
      row.total_rate_cost,
      row.total_rate_twr,
      row.total_rate_dietz,
      row.principal,
      row.market_value,
      row.fx_hkd_cny,
      row.fx_usd_cny,
      row.created_at,
      row.updated_at,
    ]
  );
  return row;
}

async function deleteAllSymbolDailyPnl(userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  await q("DELETE FROM symbol_daily_pnl WHERE user_id = $1", [uid]);
}

async function deleteAllAnalysisDailySnapshot(userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  await q("DELETE FROM analysis_daily_snapshot WHERE user_id = $1", [uid]);
}

async function deleteAllDataForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  const p = await initPool();
  const c = await p.connect();
  try {
    await c.query("BEGIN");
    await c.query("DELETE FROM trades WHERE user_id = $1", [uid]);
    await c.query("DELETE FROM cash_transfers WHERE user_id = $1", [uid]);
    await c.query("DELETE FROM symbol_daily_pnl WHERE user_id = $1", [uid]);
    await c.query("DELETE FROM analysis_daily_snapshot WHERE user_id = $1", [uid]);
    await c.query("DELETE FROM daily_returns WHERE user_id = $1", [uid]);
    await c.query("DELETE FROM app_settings WHERE user_id = $1", [uid]);
    await c.query("DELETE FROM accounts WHERE user_id = $1", [uid]);
    await c.query("COMMIT");
  } catch (e) {
    await c.query("ROLLBACK");
    throw e;
  } finally {
    c.release();
  }
}

async function upsertSymbolDailyCloseBatch(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) {
    return 0;
  }
  const now = nowMs();
  const p = await initPool();
  const c = await p.connect();
  try {
    await c.query("BEGIN");
    for (const raw of list) {
      const r = raw || {};
      const sym = normalizeSymbol(r.symbol);
      if (!sym || !Number.isFinite(Number(r.close))) {
        continue;
      }
      await c.query(
        `INSERT INTO symbol_daily_close (symbol, date, close, source, updated_at) VALUES ($1,$2,$3,$4,$5)
         ON CONFLICT (symbol, date) DO UPDATE SET close = EXCLUDED.close, source = EXCLUDED.source, updated_at = EXCLUDED.updated_at`,
        [sym, toDateKey(r.date), validNumber(r.close, 0), String(r.source || "").slice(0, 64), now]
      );
    }
    await c.query("COMMIT");
  } catch (e) {
    await c.query("ROLLBACK");
    throw e;
  } finally {
    c.release();
  }
  return list.length;
}

async function getSymbolDailyCloseRange(symbol, fromDate, toDate) {
  const sym = normalizeSymbol(symbol);
  if (!sym) {
    return [];
  }
  const from = fromDate && String(fromDate).trim() ? String(fromDate).trim() : "1970-01-01";
  const to = toDate && String(toDate).trim() ? String(toDate).trim() : "9999-12-31";
  const { rows } = await q(
    "SELECT date, close, source FROM symbol_daily_close WHERE symbol = $1 AND date >= $2 AND date <= $3 ORDER BY date ASC",
    [sym, from, to]
  );
  return rows.map((row) => ({
    date: row.date,
    close: Number(row.close),
    source: row.source || "",
  }));
}

async function getLatestSymbolDailyClose(symbol) {
  const sym = normalizeSymbol(symbol);
  if (!sym) {
    return null;
  }
  const { rows } = await q(
    "SELECT close, date FROM symbol_daily_close WHERE symbol = $1 ORDER BY date DESC LIMIT 1",
    [sym]
  );
  if (!rows[0] || rows[0].close == null) {
    return null;
  }
  return { close: Number(rows[0].close), date: String(rows[0].date) };
}

async function getTradeWindowForDailyClose(userId) {
  const trades = await getTrades(userId);
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

function closeDatabase() {
  if (pool) {
    return pool.end();
  }
  return Promise.resolve();
}

async function getCliUserId() {
  const phone = String(process.env.STOCKREVIEW_PHONE || SEED_USER_PHONE).trim();
  const { rows } = await q("SELECT id FROM users WHERE phone = $1", [phone]);
  if (!rows[0]) {
    throw new Error(`No user for phone ${phone}; open app once to seed database.`);
  }
  return rows[0].id;
}

async function findUserByPhone(phone) {
  const p = String(phone || "").trim();
  if (!p) {
    return null;
  }
  const { rows } = await q("SELECT id, phone, created_at FROM users WHERE phone = $1", [p]);
  return rows[0] || null;
}

async function verifyUserLogin(phone, passwordPlain) {
  const p = String(phone || "").trim();
  if (!isValidPhone(p) || !isValidPasswordDigits(passwordPlain)) {
    return null;
  }
  const { rows } = await q("SELECT id, password_hash FROM users WHERE phone = $1", [p]);
  if (!rows[0] || !verifyPassword(passwordPlain, rows[0].password_hash)) {
    return null;
  }
  return { id: rows[0].id, phone: p };
}

async function createRegisteredUser(phone, passwordPlain) {
  const p = String(phone || "").trim();
  if (!isValidPhone(p) || !isValidPasswordDigits(passwordPlain)) {
    throw new Error("invalid phone or password");
  }
  if (await findUserByPhone(p)) {
    throw new Error("phone already registered");
  }
  const id = randomUUID();
  const now = nowMs();
  await q(
    "INSERT INTO users (id, phone, password_hash, created_at, updated_at) VALUES ($1,$2,$3,$4,$5)",
    [id, p, hashPassword(passwordPlain), now, now]
  );
  await migrateAccountsIfEmptyForUser(id);
  return { id, phone: p };
}

async function updateUserPassword(userId, newPasswordPlain) {
  const uid = String(userId || "").trim();
  if (!uid || !isValidPasswordDigits(newPasswordPlain)) {
    throw new Error("invalid password");
  }
  const now = nowMs();
  const { rowCount } = await q("UPDATE users SET password_hash = $1, updated_at = $2 WHERE id = $3", [
    hashPassword(newPasswordPlain),
    now,
    uid,
  ]);
  return rowCount > 0;
}

async function verifyUserPasswordById(userId, passwordPlain) {
  const uid = String(userId || "").trim();
  const { rows } = await q("SELECT password_hash FROM users WHERE id = $1", [uid]);
  if (!rows[0]) {
    return false;
  }
  return verifyPassword(passwordPlain, rows[0].password_hash);
}

async function getUserPhone(userId) {
  const uid = String(userId || "").trim();
  const { rows } = await q("SELECT phone FROM users WHERE id = $1", [uid]);
  return rows[0] ? String(rows[0].phone) : "";
}

async function getUserCommunityRow(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return null;
  }
  const { rows } = await q("SELECT id, phone, nickname, community_public FROM users WHERE id = $1", [uid]);
  return rows[0] || null;
}

async function updateUserCommunityProfile(userId, { nickname, communityPublic }) {
  const uid = String(userId || "").trim();
  if (!uid) {
    throw new Error("userId required");
  }
  const row = await getUserCommunityRow(uid);
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
      const { rows: clash } = await q("SELECT id FROM users WHERE nickname = $1 AND id != $2", [nick, uid]);
      if (clash[0]) {
        throw new Error("nickname taken");
      }
    }
  }
  let pub = row.community_public != null ? Number(row.community_public) : 1;
  if (communityPublic !== undefined) {
    pub = communityPublic ? 1 : 0;
  }
  await q("UPDATE users SET nickname = $1, community_public = $2, updated_at = $3 WHERE id = $4", [
    nick,
    pub,
    now,
    uid,
  ]);
  await q("DELETE FROM community_leaderboard_cache");
  return getUserCommunityRow(uid);
}

async function setCommunityFollow(followerId, followeeId) {
  const a = String(followerId || "").trim();
  const b = String(followeeId || "").trim();
  if (!a || !b || a === b) {
    return false;
  }
  const now = nowMs();
  try {
    await q("INSERT INTO community_follows (follower_id, followee_id, created_at) VALUES ($1,$2,$3)", [a, b, now]);
    return true;
  } catch (e) {
    if (e && e.code === "23505") {
      return false;
    }
    throw e;
  }
}

async function removeCommunityFollow(followerId, followeeId) {
  const a = String(followerId || "").trim();
  const b = String(followeeId || "").trim();
  const { rowCount } = await q("DELETE FROM community_follows WHERE follower_id = $1 AND followee_id = $2", [a, b]);
  return rowCount > 0;
}

async function listCommunityFolloweeIds(followerId) {
  const a = String(followerId || "").trim();
  if (!a) {
    return [];
  }
  const { rows } = await q("SELECT followee_id FROM community_follows WHERE follower_id = $1", [a]);
  return rows.map((r) => r.followee_id);
}

async function isCommunityFollowing(followerId, followeeId) {
  const { rows } = await q(
    "SELECT 1 AS x FROM community_follows WHERE follower_id = $1 AND followee_id = $2",
    [String(followerId), String(followeeId)]
  );
  return Boolean(rows[0]);
}

async function getCommunityLeaderboardCache() {
  const { rows } = await q("SELECT payload, updated_at FROM community_leaderboard_cache WHERE id = 1");
  return rows[0] || null;
}

async function setCommunityLeaderboardCache(payloadJson, updatedAt) {
  await q(
    `INSERT INTO community_leaderboard_cache (id, payload, updated_at) VALUES (1, $1, $2)
     ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at`,
    [payloadJson, updatedAt]
  );
}

async function selectAnalysisSnapshotsFrom(userId, accountId, fromDate) {
  const uid = String(userId || "").trim();
  const acc = String(accountId || "all");
  const from = String(fromDate || "1970-01-01");
  const { rows } = await q(
    `SELECT date, total_rate_twr, total_rate_cost, profit_cny, market_value, fx_hkd_cny, fx_usd_cny
     FROM analysis_daily_snapshot
     WHERE user_id = $1 AND account_id = $2 AND date >= $3
     ORDER BY date ASC`,
    [uid, acc, from]
  );
  return rows;
}

async function selectAnalysisSnapshotsForPublicMetrics(userId) {
  const uid = String(userId || "").trim();
  const tryIds = [];
  const seen = new Set();
  const push = (id) => {
    const a = String(id || "all").trim() || "all";
    if (seen.has(a)) {
      return;
    }
    seen.add(a);
    tryIds.push(a);
  };
  push("all");
  try {
    for (const ac of await getAccounts(uid)) {
      push(ac?.id);
    }
  } catch {
    // ignore
  }
  for (const t of await getTrades(uid)) {
    push(t.accountId);
  }
  for (const acc of tryIds) {
    const rows = await selectAnalysisSnapshotsFrom(uid, acc, "2000-01-01");
    if (rows.length) {
      return rows;
    }
  }
  return [];
}

async function selectLatestSymbolDailyDate(userId, accountId) {
  const uid = String(userId || "").trim();
  const acc = String(accountId || "all");
  const { rows } = await q(
    "SELECT MAX(date) AS d FROM symbol_daily_pnl WHERE user_id = $1 AND account_id = $2",
    [uid, acc]
  );
  return rows[0]?.d ? String(rows[0].d) : null;
}

async function selectTopSymbolDailyByDate(userId, accountId, date, limit) {
  const uid = String(userId || "").trim();
  const acc = String(accountId || "all");
  const dk = String(date || "");
  const lim = Math.min(20, Math.max(1, Number(limit) || 3));
  const { rows } = await q(
    `SELECT symbol, eod_shares, day_close_price, currency, day_pnl_native
     FROM symbol_daily_pnl
     WHERE user_id = $1 AND account_id = $2 AND date = $3
     ORDER BY abs(eod_shares * COALESCE(day_close_price, 0)) DESC
     LIMIT $4`,
    [uid, acc, dk, lim]
  );
  return rows;
}

async function getCommunityFeedTradesRecent(_viewerId, limit = 50) {
  const lim = Math.min(2000, Math.max(1, Number(limit) || 50));
  const { rows } = await q(
    `SELECT t.id, t.user_id, t.symbol, t.name, t.price, t.quantity, t.amount, t.trade_date, t.note, t.side, t.created_at
     FROM trades t
     INNER JOIN users u ON u.id = t.user_id
     WHERE COALESCE(u.community_public, 1) = 1
       AND t.type = 'trade'
     ORDER BY t.trade_date DESC, t.created_at DESC
     LIMIT $1`,
    [lim]
  );
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

async function listPublicCommunityUserIds() {
  const { rows } = await q("SELECT id FROM users WHERE COALESCE(community_public, 1) = 1");
  return rows.map((r) => r.id);
}

async function selectSymbolDailyPositionsOnDate(userId, accountId, date) {
  const uid = String(userId || "").trim();
  const acc = String(accountId || "all");
  const dk = String(date || "");
  const { rows } = await q(
    `SELECT symbol, eod_shares, day_close_price, currency, day_pnl_native
     FROM symbol_daily_pnl
     WHERE user_id = $1 AND account_id = $2 AND date = $3 AND eod_shares > 0.0001
     ORDER BY abs(eod_shares * COALESCE(day_close_price, 0)) DESC`,
    [uid, acc, dk]
  );
  return rows;
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
  normalizeCashTransfer,
  getCashTransfers,
  upsertCashTransfer,
  importCashTransfers,
  deleteCashTransferById,
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
  deleteAllDataForUser,
  upsertSymbolDailyCloseBatch,
  getSymbolDailyCloseRange,
  getLatestSymbolDailyClose,
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
  selectAnalysisSnapshotsForPublicMetrics,
  selectLatestSymbolDailyDate,
  selectTopSymbolDailyByDate,
  getCommunityFeedTradesRecent,
  listPublicCommunityUserIds,
  selectSymbolDailyPositionsOnDate,
};
