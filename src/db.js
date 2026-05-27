if (!process.env.VERCEL) {
  require("dotenv").config();
}
const { neonConfig, Pool, Client } = require("@neondatabase/serverless");
const ws = require("ws");
neonConfig.webSocketConstructor = ws; // 必需：让 neon 使用 ws 库在 Node.js 中建立连接

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
  isUsTickerSymbol,
  getLegacyUsAlias,
  formatSymbolForDisplay,
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
const { toDateKey: shanghaiCalendarDateKey } = require("../scripts/lib/market-fetch");

/** Vercel Marketplace / Neon 可能注入 POSTGRES_URL；统一取连接串 */
function getDatabaseUrl() {
  // 使用 @neondatabase/serverless 驱动时，必须走 -pooler host（WebSocket 通道）
  // 优先使用带 -pooler 的变量：DATABASE_URL / POSTGRES_URL
  const url = process.env.DATABASE_URL ||
    process.env.POSTGRES_URL ||
    process.env.POSTGRES_PRISMA_URL ||
    process.env.DATABASE_URL_UNPOOLED ||
    process.env.POSTGRES_URL_NON_POOLING ||
    "";

  let cleanUrl = url;

  // 确保在 Vercel 线上连接 Neon 时一定带有 sslmode=require
  if (process.env.VERCEL && cleanUrl && !cleanUrl.includes('sslmode=')) {
    cleanUrl += (cleanUrl.includes('?') ? '&' : '?') + 'sslmode=require';
  }

  return cleanUrl;
}

/**
 * Neon 控制台有时会带上 channel_binding=require；在部分 Node/pg + serverless 环境里会在握手阶段长时间卡住。
 * 去掉该参数不影响 sslmode=require 的加密连接。
 */
function sanitizeNeonConnectionString(url) {
  let u = String(url || "").trim();
  if (!u) {
    return u;
  }
  // 如果 ?channel_binding= 在查询参数的开头，处理它以及它后面的 &
  u = u.replace(/\?channel_binding=[^&]*&?/gi, "?");
  // 如果 &channel_binding= 在查询参数的中间
  u = u.replace(/&channel_binding=[^&]*/gi, "");
  // 清理尾部多余的 ? 或 &
  u = u.replace(/[?&]$/g, "");
  return u;
}

function getPgConnectionString() {
  return sanitizeNeonConnectionString(getDatabaseUrl());
}

const DB_PATH = getDatabaseUrl() ? "[postgresql]" : "";

let pool;
let initPromise;
let postInitTasksStarted = false;
let isBootstrapping = false;
let symbolNameMapTableReadyPromise = null;
let symbolNameMapCanonicalizePromise = null;
let symbolNameMapCanonicalizedAt = 0;
let snapshotWatermarkTableReadyPromise = null;

const SYMBOL_MAP_CANONICALIZE_TTL_MS = 5 * 60 * 1000;

/** Vercel：限制单实例同时打开的 Neon 连接数，避免 burst 时 pooler 排队导致全站 API pending */
const VERCEL_DB_SLOT_MAX = Math.min(
  16,
  Math.max(1, Number(process.env.VERCEL_DB_SLOT_MAX || (process.env.VERCEL ? 6 : 99)))
);
let vercelDbSlotsInUse = 0;
const vercelDbSlotWaiters = [];

const VERCEL_DB_SLOT_WAIT_MS = Math.max(
  1000,
  Math.min(25_000, Number(process.env.VERCEL_DB_SLOT_WAIT_MS || 12_000))
);

function acquireVercelDbSlot() {
  if (!process.env.VERCEL) {
    return Promise.resolve();
  }
  if (vercelDbSlotsInUse < VERCEL_DB_SLOT_MAX) {
    vercelDbSlotsInUse += 1;
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`vercel db slot wait timeout after ${VERCEL_DB_SLOT_WAIT_MS}ms`));
    }, VERCEL_DB_SLOT_WAIT_MS);
    vercelDbSlotWaiters.push(() => {
      clearTimeout(timer);
      vercelDbSlotsInUse += 1;
      resolve();
    });
  });
}

function releaseVercelDbSlot() {
  if (!process.env.VERCEL) {
    return;
  }
  vercelDbSlotsInUse -= 1;
  const next = vercelDbSlotWaiters.shift();
  if (next) {
    next();
  }
}
function isServerlessRuntime() {
  return !!(process.env.VERCEL || process.env.VERCEL_ENV || process.env.AWS_LAMBDA_FUNCTION_NAME);
}

async function withOneShotDbClient(fn) {
  if (isServerlessRuntime()) {
    return withVercelDbClient(fn);
  }
  const dbUrl = getPgConnectionString();
  if (!dbUrl) {
    throw new Error("Database URL is not configured (DATABASE_URL / POSTGRES_URL)");
  }
  const connectMs = Number(process.env.DATABASE_CONNECT_TIMEOUT_MS || 8000);
  const client = new Client({
    connectionString: dbUrl,
    ssl: getSslOption(),
    connectionTimeoutMillis: connectMs,
    query_timeout: connectMs,
  });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end().catch(() => {});
  }
}

async function withVercelDbClient(fn) {
  if (!process.env.VERCEL) {
    throw new Error("withVercelDbClient only for VERCEL");
  }
  const dbUrl = getPgConnectionString();
  if (!dbUrl) {
    throw new Error("Database URL is not configured (DATABASE_URL / POSTGRES_URL)");
  }
  const connectMs = Number(process.env.DATABASE_CONNECT_TIMEOUT_MS || 8000);
  const hardTimeoutMs = Math.max(1000, Number(process.env.DATABASE_PING_HARD_TIMEOUT_MS || connectMs + 2000));
  await acquireVercelDbSlot();
  const client = new Client({
    connectionString: dbUrl,
    ssl: getSslOption(),
    connectionTimeoutMillis: connectMs,
    query_timeout: connectMs,
  });
  const withHardTimeout = (promise, stage) => {
    let timerId = null;
    return new Promise((resolve, reject) => {
      timerId = setTimeout(() => {
        reject(new Error(`database ${stage} timeout after ${hardTimeoutMs}ms`));
      }, hardTimeoutMs);
      promise.then(
        (v) => {
          if (timerId) clearTimeout(timerId);
          resolve(v);
        },
        (err) => {
          if (timerId) clearTimeout(timerId);
          reject(err);
        }
      );
    });
  };
  try {
    await withHardTimeout(client.connect(), "connect");
    return await fn(client);
  } finally {
    try {
      await client.end().catch(() => {});
    } finally {
      releaseVercelDbSlot();
    }
  }
}

function applyAppSettingsRows(settings, rows) {
  for (const row of rows || []) {
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
}

async function ensureSnapshotWatermarkTable() {
  if (snapshotWatermarkTableReadyPromise) {
    return snapshotWatermarkTableReadyPromise;
  }
  snapshotWatermarkTableReadyPromise = (async () => {
    await q(
      `CREATE TABLE IF NOT EXISTS snapshot_watermark (
         id INTEGER PRIMARY KEY CHECK (id = 1),
         frozen_date TEXT NOT NULL,
         status TEXT NOT NULL,
         message TEXT,
         updated_at BIGINT NOT NULL
       )`
    );
  })().finally(() => {
    snapshotWatermarkTableReadyPromise = null;
  });
  return snapshotWatermarkTableReadyPromise;
}

function getSslOption() {
  if (process.env.DATABASE_SSL === "0" || process.env.DATABASE_SSL === "false") {
    return false;
  }
  const u = String(getPgConnectionString() || "");
  if (/localhost|127\.0\.0\.1/.test(u)) {
    return false;
  }
  // Vercel 部署时连接 Neon 必须带 SSL
  return { rejectUnauthorized: false };
}

function symbolQueryCandidates(symbol) {
  const normalized = normalizeSymbol(symbol);
  if (!normalized) {
    return [];
  }
  const out = [normalized];
  const legacyUs = getLegacyUsAlias(normalized);
  if (legacyUs && !out.includes(legacyUs)) {
    out.push(legacyUs);
  }
  return out;
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
    day_trade_flow_native DOUBLE PRECISION NOT NULL DEFAULT 0,
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
    tw_r_daily DOUBLE PRECISION NOT NULL DEFAULT 0,
    tw_r_cumulative DOUBLE PRECISION NOT NULL DEFAULT 0,
    external_flow_cny DOUBLE PRECISION NOT NULL DEFAULT 0,
    external_flow_native DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_profit DOUBLE PRECISION NOT NULL DEFAULT 0,
    principal DOUBLE PRECISION NOT NULL DEFAULT 0,
    market_value DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_assets DOUBLE PRECISION NOT NULL DEFAULT 0,
    cash_cny DOUBLE PRECISION NOT NULL DEFAULT 0,
    cash_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    fx_hkd_cny DOUBLE PRECISION,
    fx_usd_cny DOUBLE PRECISION,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, account_id, date)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_analysis_daily_snapshot_date ON analysis_daily_snapshot (date ASC)`,
  `CREATE TABLE IF NOT EXISTS performance_series_cache (
    user_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    preset TEXT NOT NULL,
    algo TEXT NOT NULL,
    period_return DOUBLE PRECISION NOT NULL DEFAULT 0,
    series_json TEXT,
    source_frozen_through TEXT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, account_id, as_of_date, preset, algo)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_perf_series_user_asof ON performance_series_cache (user_id, as_of_date DESC)`,
  `CREATE TABLE IF NOT EXISTS symbol_daily_close (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    source TEXT,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (symbol, date)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_symbol_daily_close_date ON symbol_daily_close (date ASC)`,
  `CREATE TABLE IF NOT EXISTS symbol_name_map (
    symbol TEXT PRIMARY KEY,
    name_cn TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'unknown',
    updated_at BIGINT NOT NULL,
    last_seen_at BIGINT NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_symbol_name_map_updated_at ON symbol_name_map (updated_at DESC)`,
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
  `CREATE TABLE IF NOT EXISTS snapshot_watermark (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    frozen_date TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT,
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
  const dbUrl = getPgConnectionString();
  if (!dbUrl) {
    throw new Error(
      "Database URL is required. Set DATABASE_URL in a local .env file (example: DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/stockreview) or connect Postgres in Vercel (POSTGRES_URL is used automatically when present)."
    );
  }
  const connectMs = Number(process.env.DATABASE_CONNECT_TIMEOUT_MS || 3000); // Fail very fast
  initPromise = (async () => {
    isBootstrapping = true;
    const poolMax = Number(process.env.PG_POOL_MAX || (process.env.VERCEL ? 2 : 20)); // Reduce connections in Vercel to avoid exhaustion
    pool = new Pool({
      connectionString: dbUrl,
      max: Number.isFinite(poolMax) && poolMax > 0 ? poolMax : 2,
      ssl: getSslOption(),
      connectionTimeoutMillis: connectMs,
      idleTimeoutMillis: 10000, // Close idle connections much faster in Serverless
    });
    let c;
    try {
      console.log("[db] Attempting Postgres connect with timeout", connectMs, "ms to URL starting with:", dbUrl.split('@')[1] ? dbUrl.split('@')[1].split('/')[0] : 'hidden');
      
      // IMPORTANT: In Vercel Edge/Serverless with neon ws driver, Promise.race with setTimeout 
      // can sometimes cause the event loop to freeze if the promise resolves too quickly or the timeout hangs.
      // Removing the hard JS timeout to let neon native ws handle the timeout.
      c = await pool.connect();
      console.log("[db] Postgres connect successful");
    } catch (e) {
      console.error("[db] Postgres connect failed:", e?.message || e);
      isBootstrapping = false;
      initPromise = null; // Reset promise so next request can retry
      throw e;
    }
    try {
      console.log("[db] Checking Vercel DDL skip...");
      if (!process.env.VERCEL) {
        console.log("[db] Not in Vercel, starting DDL execution...");
        for (let i = 0; i < DDL.length; i++) {
          await c.query(DDL[i]);
        }
        console.log("[db] DDL execution completed successfully.");
        console.log("[db] Ensuring seed user...");
        await ensureSeedUserRowWithClient(c);
        console.log("[db] Seed user ensured.");
      } else {
        console.log("[db] Skipped DDL and Seed execution entirely in Vercel");
      }
    } catch (e) {
      console.error("[db] Postgres init tasks failed:", e?.message || e);
      // Do not throw here if we are just ensuring seed user, let the app start
      if (!process.env.VERCEL) throw e; 
    } finally {
      c.release();
      isBootstrapping = false;
    }
    console.log("[db] initPool completed");
    return pool;
  })();
  return initPromise;
}

async function q(text, params = []) {
  // Vercel Serverless：绝对不复用长生命周期 Pool。
  // 同一个函数实例会被 freeze/thaw，冻结期间 WebSocket 往往被中间层悄悄断开，
  // 复用时写入新请求会永久挂起直到 300s 函数超时。
  // 因此每次查询都临时新建 Client，用完立刻 end()。
  if (process.env.VERCEL) {
    await acquireVercelDbSlot();
    const dbUrl = getPgConnectionString();
    if (!dbUrl) {
      releaseVercelDbSlot();
      throw new Error("Database URL is not configured (DATABASE_URL / POSTGRES_URL)");
    }
    const connectMs = Number(process.env.DATABASE_CONNECT_TIMEOUT_MS || 8000);
    const hardTimeoutMs = Math.max(1000, Number(process.env.DATABASE_PING_HARD_TIMEOUT_MS || connectMs + 2000));
    const client = new Client({
      connectionString: dbUrl,
      ssl: getSslOption(),
      connectionTimeoutMillis: connectMs,
      query_timeout: connectMs,
    });
    const withHardTimeout = (promise, stage) => {
      let timerId = null;
      return new Promise((resolve, reject) => {
        timerId = setTimeout(() => {
          reject(new Error(`database ${stage} timeout after ${hardTimeoutMs}ms`));
        }, hardTimeoutMs);
        promise.then(
          (v) => {
            if (timerId) clearTimeout(timerId);
            resolve(v);
          },
          (err) => {
            if (timerId) clearTimeout(timerId);
            reject(err);
          }
        );
      });
    };
    try {
      console.log("[db.q.vercel] build=v2 stage=connect-start textHead=%s", String(text).substring(0, 40));
      await withHardTimeout(client.connect(), "connect");
      console.log("[db.q.vercel] stage=connected, running query...");
      const result = await withHardTimeout(client.query(text, params), "query");
      console.log("[db.q.vercel] stage=query-done rows=%d", result?.rowCount ?? -1);
      return result;
    } catch (e) {
      console.error("[db.q.vercel] stage=error msg=%s textHead=%s", e?.message || e, String(text).substring(0, 80));
      throw e;
    } finally {
      try {
        await client.end().catch(() => {});
      } finally {
        releaseVercelDbSlot();
      }
    }
  }

  if (isBootstrapping && pool) {
    return pool.query(text, params);
  }
  const p = await initPool();
  try {
    return await p.query(text, params);
  } catch (e) {
    console.error("[db] query error:", e?.message || e, text.substring(0, 50));
    throw e;
  }
}

/**
 * 运维探活：单次短连接 + 一条只读 SQL。
 * 不走 initPool / DDL：冷启动若在这里触发完整建表，容易超过 Vercel 函数默认时限，浏览器会一直 pending、0 bytes。
 */
async function pingDatabase() {
  const dbUrl = getPgConnectionString();
  if (!dbUrl) {
    throw new Error("Database URL is not configured (DATABASE_URL / POSTGRES_URL)");
  }
  try {
    const hostPart = dbUrl.split("@")[1]?.split("/")[0] || "hidden";
    const hasPooler = hostPart.includes("-pooler");
    console.log(
      "[db.ping] connecting host=%s pooler=%s driver=@neondatabase/serverless",
      hostPart,
      hasPooler
    );
  } catch (_) {}
  const connectMs = Number(process.env.DATABASE_CONNECT_TIMEOUT_MS || 8000);
  const hardTimeoutMs = Math.max(1000, Number(process.env.DATABASE_PING_HARD_TIMEOUT_MS || connectMs + 2000));
  const client = new Client({
    connectionString: dbUrl,
    ssl: getSslOption(),
    connectionTimeoutMillis: connectMs,
    query_timeout: connectMs,
  });
  let timeoutId = null;
  const withHardTimeout = (promise, stage) =>
    new Promise((resolve, reject) => {
      timeoutId = setTimeout(() => {
        reject(new Error(`database ${stage} timeout after ${hardTimeoutMs}ms`));
      }, hardTimeoutMs);
      promise.then(resolve, reject);
    });
  try {
    await withHardTimeout(client.connect(), "connect");
    const { rows } = await withHardTimeout(
      client.query("SELECT current_database() AS db, current_schema() AS schema, NOW() AS server_time"),
      "query"
    );
    return rows[0] || {};
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
    await client.end().catch(() => {});
  }
}

const SEED_USER_PASSWORD = "123456";

async function ensureSeedUserRowWithClient(client) {
  const check = await client.query("SELECT id FROM users WHERE phone = $1", [SEED_USER_PHONE]);
  if (check.rows.length) {
    return check.rows[0].id;
  }
  const id = randomUUID();
  const now = nowMs();
  // 增加插入前的日志
  console.log("[db] Inserting seed user...");
  await client.query(
    `INSERT INTO users (id, phone, password_hash, created_at, updated_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (phone) DO NOTHING`,
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
  // Reset clearing flags so the next cron re-evaluates this user/account
  const tradeNow = nowMs();
  await q(
    `UPDATE user_metrics_meta SET is_cleared = FALSE, updated_at = $2 WHERE user_id = $1`,
    [row.user_id, tradeNow]
  ).catch(() => {});
  await q(
    `INSERT INTO account_metrics_meta (user_id, account_id, is_cleared, updated_at)
     VALUES ($1, $2, FALSE, $3)
     ON CONFLICT (user_id, account_id) DO UPDATE SET is_cleared = FALSE, updated_at = EXCLUDED.updated_at`,
    [row.user_id, row.account_id, tradeNow]
  ).catch(() => {});
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
  const tid = String(tradeId || "");
  // Get account_id before delete so we can reset that account's clearing flag
  let deletedAccountId = null;
  try {
    const { rows: tr } = await q("SELECT account_id FROM trades WHERE user_id = $1 AND id = $2", [uid, tid]);
    deletedAccountId = tr[0]?.account_id || null;
  } catch { /* ignore */ }
  const { rowCount } = await q("DELETE FROM trades WHERE user_id = $1 AND id = $2", [uid, tid]);
  if (rowCount > 0) {
    const delNow = nowMs();
    await q(`UPDATE user_metrics_meta SET is_cleared = FALSE, updated_at = $2 WHERE user_id = $1`, [uid, delNow]).catch(() => {});
    if (deletedAccountId) {
      await q(
        `INSERT INTO account_metrics_meta (user_id, account_id, is_cleared, updated_at)
         VALUES ($1, $2, FALSE, $3)
         ON CONFLICT (user_id, account_id) DO UPDATE SET is_cleared = FALSE, updated_at = EXCLUDED.updated_at`,
        [uid, deletedAccountId, delNow]
      ).catch(() => {});
    }
  }
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
  const allHistory = query.allHistory === true || query.allHistory === 1;
  const limitN = Number(query.limit);
  const offsetN = Number(query.offset);
  const limit = Number.isFinite(limitN) ? Math.min(50000, Math.max(0, Math.floor(limitN))) : 0;
  const offset = Number.isFinite(offsetN) ? Math.max(0, Math.floor(offsetN)) : 0;
  const defaultCapDays = Math.min(5000, Math.max(60, Number(process.env.DAILY_RETURNS_DEFAULT_CAP_DAYS) || 800));

  const mapRows = (rows) => rows.map(rowToDailyReturn);

  if (accountId || from || to) {
    const fromBound = from || "1970-01-01";
    const toBound = to || "9999-12-31";
    let sql = `SELECT account_id, date, profit, return_rate, total_asset, created_at
     FROM daily_returns
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND date >= $3 AND date <= $4
     ORDER BY date ASC`;
    const params = [uid, accountId, fromBound, toBound];
    if (limit > 0) {
      sql += ` LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
      params.push(limit, offset);
    }
    const { rows } = await q(sql, params);
    return mapRows(rows);
  }

  if (allHistory) {
    let sql =
      "SELECT account_id, date, profit, return_rate, total_asset, created_at FROM daily_returns WHERE user_id = $1 ORDER BY account_id ASC, date ASC";
    const params = [uid];
    if (limit > 0) {
      sql += ` LIMIT $2 OFFSET $3`;
      params.push(limit, offset);
    }
    const { rows } = await q(sql, params);
    return mapRows(rows);
  }

  const today = toDateKey(new Date());
  const minD = addCalendarDays(today, -defaultCapDays);
  let sql = `SELECT account_id, date, profit, return_rate, total_asset, created_at
     FROM daily_returns
     WHERE user_id = $1 AND date >= $2
     ORDER BY account_id ASC, date ASC`;
  const params = [uid, minD];
  if (limit > 0) {
    sql += ` LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
    params.push(limit, offset);
  }
  const { rows } = await q(sql, params);
  return mapRows(rows);
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
  return getDailyReturns({ allHistory: true }, uid);
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

async function getState(userId, opts = {}) {
  const uid = String(userId || "").trim();
  const base = await getSettings(uid);
  if (opts.lite === true) {
    return {
      ...base,
      trades: [],
      dailyReturns: [],
      cashTransfers: [],
    };
  }
  const [trades, dailyReturns, cashTransfers] = await Promise.all([
    getTrades(uid),
    getDailyReturns({ allHistory: true }, uid),
    getCashTransfers(uid),
  ]);
  return {
    ...base,
    trades,
    dailyReturns,
    cashTransfers,
  };
}

/** 账户级 KPI 展示账本：全部=CNY；单账户=该账户 settings 记账币种。 */
function resolveBookCurrencyForAccountScope(settings, accountScope) {
  const sc = String(accountScope || "all").trim() || "all";
  if (sc === "all") {
    return "CNY";
  }
  const accs = Array.isArray(settings?.accounts) ? settings.accounts : [];
  const hit = accs.find((a) => String(a.id || "") === sc);
  const c = String(hit?.currency || "CNY").toUpperCase();
  if (c === "USD" || c === "HKD" || c === "CNY") {
    return c;
  }
  return "CNY";
}

/** 供 HTTP：单 scope 的账户 KPI 展示态（依赖 account_home_summary 已由定时任务写入）。 */
async function buildAccountKpiSurfaceForScope(userId, accountScope = "all") {
  const uid = String(userId || "").trim();
  if (!uid) {
    return null;
  }
  const sc = String(accountScope || "all").trim() || "all";
  const settings = await getSettings(uid);
  const { rows } = await q("SELECT * FROM account_home_summary WHERE user_id = $1 AND account_scope = $2", [uid, sc]);
  const row = rows[0];
  if (!row) {
    return null;
  }
  const { buildAccountKpiSurfacePayload } = require("./account-kpi-surface");
  const book = resolveBookCurrencyForAccountScope(settings, sc);
  const algo = String(settings?.algoMode || "twr").toLowerCase() === "mwr" ? "mwr" : "twr";
  return buildAccountKpiSurfacePayload(row, book, algo);
}

let metricsOpsSchemaPromise = null;

async function ensureMetricsOpsTables() {
  if (metricsOpsSchemaPromise) return metricsOpsSchemaPromise;
  metricsOpsSchemaPromise = (async () => {
    await q(`CREATE TABLE IF NOT EXISTS user_metrics_meta (
        user_id TEXT PRIMARY KEY, data_version INTEGER NOT NULL DEFAULT 0,
        rebuilding BOOLEAN NOT NULL DEFAULT FALSE, frozen_through TEXT,
        rebuild_from TEXT, updated_at BIGINT NOT NULL DEFAULT 0,
        is_cleared BOOLEAN NOT NULL DEFAULT FALSE, cleared_at TEXT)`).catch(() => {});
    await q(`ALTER TABLE user_metrics_meta ADD COLUMN IF NOT EXISTS is_cleared BOOLEAN NOT NULL DEFAULT FALSE`).catch(() => {});
    await q(`ALTER TABLE user_metrics_meta ADD COLUMN IF NOT EXISTS cleared_at TEXT`).catch(() => {});
    await q(`CREATE TABLE IF NOT EXISTS account_metrics_meta (
        user_id TEXT NOT NULL, account_id TEXT NOT NULL,
        is_cleared BOOLEAN NOT NULL DEFAULT FALSE, cleared_at TEXT,
        frozen_through TEXT, updated_at BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (user_id, account_id))`).catch(() => {});
    await q(`CREATE TABLE IF NOT EXISTS cron_job_run (
        id BIGSERIAL PRIMARY KEY, job_name TEXT NOT NULL, started_at BIGINT NOT NULL,
        finished_at BIGINT, ok BOOLEAN NOT NULL DEFAULT FALSE, error_message TEXT,
        meta_json TEXT, created_at BIGINT NOT NULL)`).catch(() => {});
  })();
  return metricsOpsSchemaPromise;
}

async function getUserMetricsMeta(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) return { dataVersion: 0, rebuilding: false, frozenThrough: null };
  const light = opts.light === true;
  if (!light) {
    await ensureMetricsOpsTables();
  }
  const { rows } = await q(
    `SELECT data_version, rebuilding, frozen_through, is_cleared, cleared_at FROM user_metrics_meta WHERE user_id = $1`,
    [uid]
  );
  const row = rows[0];
  if (!row) {
    if (light) {
      return { dataVersion: 0, rebuilding: false, frozenThrough: null, isCleared: false, clearedAt: null };
    }
    const frozen = await getLatestAnalysisSnapshotDate(uid, "all");
    return { dataVersion: 0, rebuilding: false, frozenThrough: frozen || null, isCleared: false, clearedAt: null };
  }
  return {
    dataVersion: Number(row.data_version) || 0,
    rebuilding: row.rebuilding === true,
    frozenThrough: row.frozen_through || null,
    isCleared: row.is_cleared === true,
    clearedAt: row.cleared_at || null,
  };
}

/** cron / 首次写入前：home_summary + metrics 运维表幂等建表（勿放在首屏 bootstrap）。 */
async function ensureAppDerivedTables() {
  await Promise.all([ensureHomeSummaryTables(), ensureMetricsOpsTables()]);
}

async function upsertUserMetricsMeta(userId, patch = {}) {
  const uid = String(userId || "").trim();
  if (!uid) return;
  await ensureMetricsOpsTables();
  const cur = await getUserMetricsMeta(uid, { light: true });
  const dataVersion = patch.dataVersion != null ? Number(patch.dataVersion) : cur.dataVersion;
  const rebuilding = patch.rebuilding != null ? !!patch.rebuilding : cur.rebuilding;
  const frozenThrough = patch.frozenThrough !== undefined ? patch.frozenThrough : cur.frozenThrough;
  const rebuildFrom = patch.rebuildFrom !== undefined ? patch.rebuildFrom : null;
  const isCleared = patch.isCleared !== undefined ? !!patch.isCleared : (cur.isCleared || false);
  const clearedAt = patch.clearedAt !== undefined ? (patch.clearedAt || null) : (cur.clearedAt || null);
  await q(
    `INSERT INTO user_metrics_meta (user_id, data_version, rebuilding, frozen_through, rebuild_from, is_cleared, cleared_at, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (user_id) DO UPDATE SET
       data_version = EXCLUDED.data_version, rebuilding = EXCLUDED.rebuilding,
       frozen_through = EXCLUDED.frozen_through, rebuild_from = EXCLUDED.rebuild_from,
       is_cleared = EXCLUDED.is_cleared, cleared_at = EXCLUDED.cleared_at,
       updated_at = EXCLUDED.updated_at`,
    [uid, dataVersion, rebuilding, frozenThrough, rebuildFrom, isCleared, clearedAt, nowMs()],
  );
}

async function upsertAccountMetricsMeta(userId, accountId, patch = {}) {
  const uid = String(userId || "").trim();
  const acc = String(accountId || "default").trim() || "default";
  if (!uid || !acc) return;
  await ensureMetricsOpsTables();
  const isCleared = patch.isCleared !== undefined ? !!patch.isCleared : false;
  const clearedAt = patch.clearedAt !== undefined ? (patch.clearedAt || null) : null;
  const frozenThrough = patch.frozenThrough !== undefined ? (patch.frozenThrough || null) : null;
  await q(
    `INSERT INTO account_metrics_meta (user_id, account_id, is_cleared, cleared_at, frozen_through, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6)
     ON CONFLICT (user_id, account_id) DO UPDATE SET
       is_cleared = EXCLUDED.is_cleared,
       cleared_at = EXCLUDED.cleared_at,
       frozen_through = COALESCE(EXCLUDED.frozen_through, account_metrics_meta.frozen_through),
       updated_at = EXCLUDED.updated_at`,
    [uid, acc, isCleared, clearedAt, frozenThrough, nowMs()]
  );
}

async function getAccountMetricsMetaForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) return [];
  await ensureMetricsOpsTables();
  const { rows } = await q(
    `SELECT account_id, is_cleared, cleared_at, frozen_through, updated_at
     FROM account_metrics_meta WHERE user_id = $1`,
    [uid]
  );
  return rows.map((r) => ({
    accountId: String(r.account_id),
    isCleared: r.is_cleared === true,
    clearedAt: r.cleared_at || null,
    frozenThrough: r.frozen_through || null,
    updatedAt: Number(r.updated_at) || 0,
  }));
}

async function getLastEodSharesForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) return [];
  const { rows } = await q(
    `SELECT DISTINCT ON (account_id, symbol) account_id, symbol, eod_shares, date
     FROM symbol_daily_pnl WHERE user_id = $1
     ORDER BY account_id, symbol, date DESC`,
    [uid]
  );
  return rows.map((r) => ({
    accountId: String(r.account_id),
    symbol: String(r.symbol),
    eodShares: Number(r.eod_shares),
    date: String(r.date),
  }));
}

async function insertCronJobRun(row) {
  await ensureMetricsOpsTables();
  const r = row || {};
  await q(
    `INSERT INTO cron_job_run (job_name, started_at, finished_at, ok, error_message, meta_json, created_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [String(r.jobName || ""), Number(r.startedAt) || nowMs(), r.finishedAt != null ? Number(r.finishedAt) : null,
     r.ok === true, r.errorMessage != null ? String(r.errorMessage).slice(0, 2000) : null,
     r.metaJson != null ? String(r.metaJson).slice(0, 8000) : null, nowMs()],
  );
}

async function listCronJobRuns(limit = 50) {
  await ensureMetricsOpsTables();
  const n = Math.min(200, Math.max(1, Number(limit) || 50));
  const { rows } = await q(
    `SELECT id, job_name, started_at, finished_at, ok, error_message, meta_json, created_at
     FROM cron_job_run ORDER BY id DESC LIMIT $1`, [n]);
  return rows.map((row) => ({
    id: row.id, jobName: row.job_name, startedAt: Number(row.started_at),
    finishedAt: row.finished_at != null ? Number(row.finished_at) : null,
    ok: row.ok === true, errorMessage: row.error_message, metaJson: row.meta_json,
    createdAt: Number(row.created_at),
  }));
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
    `SELECT account_id, symbol, date, eod_shares, day_trade_qty, day_trade_amount, day_trade_flow_native, day_close_price, day_pnl_native, currency, created_at
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
    dayTradeFlowNative: Number(row.day_trade_flow_native),
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
           user_id, account_id, symbol, date, eod_shares, day_trade_qty, day_trade_amount, day_trade_flow_native, day_close_price, day_pnl_native, currency, created_at, updated_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
         ON CONFLICT (user_id, account_id, symbol, date) DO UPDATE SET
           eod_shares = EXCLUDED.eod_shares, day_trade_qty = EXCLUDED.day_trade_qty, day_trade_amount = EXCLUDED.day_trade_amount,
           day_trade_flow_native = EXCLUDED.day_trade_flow_native,
           day_close_price = EXCLUDED.day_close_price, day_pnl_native = EXCLUDED.day_pnl_native, currency = EXCLUDED.currency, updated_at = EXCLUDED.updated_at`,
        [
          uid,
          String(r.accountId || r.account_id || "default").trim() || "default",
          String(r.symbol || "").trim().toLowerCase(),
          toDateKey(r.date),
          validNumber(r.eodShares, r.eod_shares, 0),
          validNumber(r.dayTradeQty, r.day_trade_qty, 0),
          validNumber(r.dayTradeAmount, r.day_trade_amount, 0),
          validNumber(r.dayTradeFlowNative, r.day_trade_flow_native, 0),
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
    `SELECT account_id, date, profit_cny, tw_r_daily, tw_r_cumulative, external_flow_cny, external_flow_native,
      total_profit, principal, market_value, total_assets, cash_cny, cash_ratio, fx_hkd_cny, fx_usd_cny, created_at
     FROM analysis_daily_snapshot
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND date >= $3 AND date <= $4
     ORDER BY date ASC`,
    [uid, accountId, from, to]
  );
  return rows.map((row) => ({
    accountId: row.account_id,
    date: row.date == null ? "" : shanghaiCalendarDateKey(row.date),
    profitCny: Number(row.profit_cny),
    twRDaily: Number(row.tw_r_daily),
    twRCumulative: Number(row.tw_r_cumulative),
    externalFlowCny: Number(row.external_flow_cny),
    externalFlowNative: Number(row.external_flow_native),
    totalProfit: Number(row.total_profit),
    principal: Number(row.principal),
    marketValue: Number(row.market_value),
    totalAssets: Number(row.total_assets ?? row.totalAssets ?? 0),
    cash: Number(row.cash_cny ?? row.cash ?? 0),
    cashRatio: Number(row.cash_ratio ?? row.cashRatio ?? 0),
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
    tw_r_daily: validNumber(r.twRDaily, r.tw_r_daily, 0),
    tw_r_cumulative: validNumber(r.twRCumulative, r.tw_r_cumulative, 0),
    external_flow_cny: validNumber(r.externalFlowCny, r.external_flow_cny, 0),
    external_flow_native: validNumber(r.externalFlowNative, r.external_flow_native, 0),
    total_profit: validNumber(r.totalProfit, r.total_profit, 0),
    principal: 0,
    market_value: validNumber(r.marketValue, r.market_value, 0),
    total_assets: validNumber(r.totalAssets, r.total_assets, 0),
    cash_cny: validNumber(r.cash, r.cash_cny, 0),
    cash_ratio: validNumber(r.cashRatio, r.cash_ratio, 0),
    fx_hkd_cny: r.fxHkdCny != null || r.fx_hkd_cny != null ? validNumber(r.fxHkdCny, r.fx_hkd_cny) : null,
    fx_usd_cny: r.fxUsdCny != null || r.fx_usd_cny != null ? validNumber(r.fxUsdCny, r.fx_usd_cny) : null,
    created_at: validNumber(r.createdAt, r.created_at, now),
    updated_at: now,
  };
  await q(
    `INSERT INTO analysis_daily_snapshot (
       user_id, account_id, date, profit_cny, tw_r_daily, tw_r_cumulative, external_flow_cny, external_flow_native,
       total_profit, principal, market_value, total_assets, cash_cny, cash_ratio, fx_hkd_cny, fx_usd_cny, created_at, updated_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
     ON CONFLICT (user_id, account_id, date) DO UPDATE SET
       profit_cny = EXCLUDED.profit_cny, tw_r_daily = EXCLUDED.tw_r_daily, tw_r_cumulative = EXCLUDED.tw_r_cumulative,
       external_flow_cny = EXCLUDED.external_flow_cny, external_flow_native = EXCLUDED.external_flow_native,
       total_profit = EXCLUDED.total_profit, principal = EXCLUDED.principal, market_value = EXCLUDED.market_value,
       total_assets = EXCLUDED.total_assets, cash_cny = EXCLUDED.cash_cny, cash_ratio = EXCLUDED.cash_ratio,
       fx_hkd_cny = EXCLUDED.fx_hkd_cny, fx_usd_cny = EXCLUDED.fx_usd_cny, updated_at = EXCLUDED.updated_at`,
    [
      row.user_id,
      row.account_id,
      row.date,
      row.profit_cny,
      row.tw_r_daily,
      row.tw_r_cumulative,
      row.external_flow_cny,
      row.external_flow_native,
      row.total_profit,
      row.principal,
      row.market_value,
      row.total_assets,
      row.cash_cny,
      row.cash_ratio,
      row.fx_hkd_cny,
      row.fx_usd_cny,
      row.created_at,
      row.updated_at,
    ]
  );
  return row;
}

async function upsertAnalysisDailySnapshotBatch(rows, userId = null) {
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
        `INSERT INTO analysis_daily_snapshot (
           user_id, account_id, date, profit_cny, tw_r_daily, tw_r_cumulative, external_flow_cny, external_flow_native,
           total_profit, principal, market_value, total_assets, cash_cny, cash_ratio, fx_hkd_cny, fx_usd_cny, created_at, updated_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
         ON CONFLICT (user_id, account_id, date) DO UPDATE SET
           profit_cny = EXCLUDED.profit_cny, tw_r_daily = EXCLUDED.tw_r_daily, tw_r_cumulative = EXCLUDED.tw_r_cumulative,
           external_flow_cny = EXCLUDED.external_flow_cny, external_flow_native = EXCLUDED.external_flow_native,
           total_profit = EXCLUDED.total_profit, principal = EXCLUDED.principal, market_value = EXCLUDED.market_value,
           total_assets = EXCLUDED.total_assets, cash_cny = EXCLUDED.cash_cny, cash_ratio = EXCLUDED.cash_ratio,
           fx_hkd_cny = EXCLUDED.fx_hkd_cny, fx_usd_cny = EXCLUDED.fx_usd_cny, updated_at = EXCLUDED.updated_at`,
        [
          uid,
          String(r.accountId || r.account_id || "default").trim() || "default",
          toDateKey(r.date),
          validNumber(r.profitCny, r.profit_cny, 0),
          validNumber(r.twRDaily, r.tw_r_daily, 0),
          validNumber(r.twRCumulative, r.tw_r_cumulative, 0),
          validNumber(r.externalFlowCny, r.external_flow_cny, 0),
          validNumber(r.externalFlowNative, r.external_flow_native, 0),
          validNumber(r.totalProfit, r.total_profit, 0),
          0,
          validNumber(r.marketValue, r.market_value, 0),
          validNumber(r.totalAssets, r.total_assets, 0),
          validNumber(r.cash, r.cash_cny, 0),
          validNumber(r.cashRatio, r.cash_ratio, 0),
          r.fxHkdCny != null || r.fx_hkd_cny != null ? validNumber(r.fxHkdCny, r.fx_hkd_cny) : null,
          r.fxUsdCny != null || r.fx_usd_cny != null ? validNumber(r.fxUsdCny, r.fx_usd_cny) : null,
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

async function deleteAllSymbolDailyPnl(userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  await q("DELETE FROM symbol_daily_pnl WHERE user_id = $1", [uid]);
}

async function deleteAllAnalysisDailySnapshot(userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  await q("DELETE FROM analysis_daily_snapshot WHERE user_id = $1", [uid]);
}

async function deletePerformanceSeriesCacheForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  await q("DELETE FROM performance_series_cache WHERE user_id = $1", [uid]);
}

let homeSummarySchemaPromise = null;

/** 首页「截止到昨日日终」汇总表（幂等建表）。 */
async function ensureHomeSummaryTables() {
  if (homeSummarySchemaPromise) {
    return homeSummarySchemaPromise;
  }
  homeSummarySchemaPromise = (async () => {
    await Promise.all([
      q(
        `CREATE TABLE IF NOT EXISTS account_home_summary (
        user_id TEXT NOT NULL,
        account_scope TEXT NOT NULL,
        frozen_through TEXT NOT NULL,
        first_trade_date TEXT,
        last_market_value_cny DOUBLE PRECISION NOT NULL DEFAULT 0,
        month_profit_cny DOUBLE PRECISION NOT NULL DEFAULT 0,
        month_rate_twr DOUBLE PRECISION NOT NULL DEFAULT 0,
        month_rate_mwr DOUBLE PRECISION NOT NULL DEFAULT 0,
        ytd_profit_cny DOUBLE PRECISION NOT NULL DEFAULT 0,
        ytd_rate_twr DOUBLE PRECISION NOT NULL DEFAULT 0,
        ytd_rate_mwr DOUBLE PRECISION NOT NULL DEFAULT 0,
        total_profit_cny DOUBLE PRECISION NOT NULL DEFAULT 0,
        total_rate_twr DOUBLE PRECISION NOT NULL DEFAULT 0,
        total_rate_mwr DOUBLE PRECISION NOT NULL DEFAULT 0,
        source_version TEXT NOT NULL DEFAULT '',
        computed_at BIGINT NOT NULL,
        PRIMARY KEY (user_id, account_scope)
      )`
      ).catch(() => {}),
      q(
        `CREATE TABLE IF NOT EXISTS symbol_home_summary (
        user_id TEXT NOT NULL,
        account_scope TEXT NOT NULL,
        symbol TEXT NOT NULL,
        frozen_through TEXT NOT NULL,
        first_trade_date TEXT,
        month_profit_native DOUBLE PRECISION NOT NULL DEFAULT 0,
        ytd_profit_native DOUBLE PRECISION NOT NULL DEFAULT 0,
        total_profit_native DOUBLE PRECISION NOT NULL DEFAULT 0,
        total_rate_twr DOUBLE PRECISION NOT NULL DEFAULT 0,
        total_rate_mwr DOUBLE PRECISION NOT NULL DEFAULT 0,
        currency TEXT NOT NULL DEFAULT 'CNY',
        source_version TEXT NOT NULL DEFAULT '',
        computed_at BIGINT NOT NULL,
        PRIMARY KEY (user_id, account_scope, symbol)
      )`
      ).catch(() => {}),
    ]);
    for (const ddl of [
      `ALTER TABLE account_home_summary ADD COLUMN IF NOT EXISTS eod_total_assets_cny DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `ALTER TABLE account_home_summary ADD COLUMN IF NOT EXISTS eod_market_value_cny DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `ALTER TABLE account_home_summary ADD COLUMN IF NOT EXISTS eod_cash_cny DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `ALTER TABLE account_home_summary ADD COLUMN IF NOT EXISTS eod_cash_ratio DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `ALTER TABLE account_home_summary ADD COLUMN IF NOT EXISTS eod_principal_cny DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `ALTER TABLE account_home_summary ADD COLUMN IF NOT EXISTS eod_fx_usd_cny DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `ALTER TABLE account_home_summary ADD COLUMN IF NOT EXISTS eod_fx_hkd_cny DOUBLE PRECISION NOT NULL DEFAULT 0`,
    ]) {
      await q(ddl).catch(() => {});
    }
  })();
  return homeSummarySchemaPromise;
}

async function deleteSymbolHomeSummaryForScope(userId, accountScope = "all") {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  const sc = String(accountScope || "all").trim() || "all";
  await q("DELETE FROM symbol_home_summary WHERE user_id = $1 AND account_scope = $2", [uid, sc]);
}

async function deleteHomeSummaryForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  await q("DELETE FROM account_home_summary WHERE user_id = $1", [uid]);
  await q("DELETE FROM symbol_home_summary WHERE user_id = $1", [uid]);
}

async function upsertAccountHomeSummaryRow(input, userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  const r = input || {};
  const now = validNumber(r.computedAt, r.computed_at, nowMs());
  await ensureHomeSummaryTables();
  await q(
    `INSERT INTO account_home_summary (
       user_id, account_scope, frozen_through, first_trade_date, last_market_value_cny,
       month_profit_cny, month_rate_twr, month_rate_mwr,
       ytd_profit_cny, ytd_rate_twr, ytd_rate_mwr,
       total_profit_cny, total_rate_twr, total_rate_mwr,
       eod_total_assets_cny, eod_market_value_cny, eod_cash_cny, eod_cash_ratio, eod_principal_cny,
       eod_fx_usd_cny, eod_fx_hkd_cny,
       source_version, computed_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
     ON CONFLICT (user_id, account_scope) DO UPDATE SET
       frozen_through = EXCLUDED.frozen_through,
       first_trade_date = EXCLUDED.first_trade_date,
       last_market_value_cny = EXCLUDED.last_market_value_cny,
       month_profit_cny = EXCLUDED.month_profit_cny,
       month_rate_twr = EXCLUDED.month_rate_twr,
       month_rate_mwr = EXCLUDED.month_rate_mwr,
       ytd_profit_cny = EXCLUDED.ytd_profit_cny,
       ytd_rate_twr = EXCLUDED.ytd_rate_twr,
       ytd_rate_mwr = EXCLUDED.ytd_rate_mwr,
       total_profit_cny = EXCLUDED.total_profit_cny,
       total_rate_twr = EXCLUDED.total_rate_twr,
       total_rate_mwr = EXCLUDED.total_rate_mwr,
       eod_total_assets_cny = EXCLUDED.eod_total_assets_cny,
       eod_market_value_cny = EXCLUDED.eod_market_value_cny,
       eod_cash_cny = EXCLUDED.eod_cash_cny,
       eod_cash_ratio = EXCLUDED.eod_cash_ratio,
       eod_principal_cny = EXCLUDED.eod_principal_cny,
       eod_fx_usd_cny = EXCLUDED.eod_fx_usd_cny,
       eod_fx_hkd_cny = EXCLUDED.eod_fx_hkd_cny,
       source_version = EXCLUDED.source_version,
       computed_at = EXCLUDED.computed_at`,
    [
      uid,
      String(r.accountScope || r.account_scope || "all").trim() || "all",
      toDateKey(r.frozenThrough || r.frozen_through),
      r.firstTradeDate != null || r.first_trade_date != null ? String(r.firstTradeDate || r.first_trade_date).slice(0, 10) : null,
      validNumber(r.lastMarketValueCny, r.last_market_value_cny, 0),
      validNumber(r.monthProfitCny, r.month_profit_cny, 0),
      validNumber(r.monthRateTwr, r.month_rate_twr, 0),
      validNumber(r.monthRateMwr, r.month_rate_mwr, 0),
      validNumber(r.ytdProfitCny, r.ytd_profit_cny, 0),
      validNumber(r.ytdRateTwr, r.ytd_rate_twr, 0),
      validNumber(r.ytdRateMwr, r.ytd_rate_mwr, 0),
      validNumber(r.totalProfitCny, r.total_profit_cny, 0),
      validNumber(r.totalRateTwr, r.total_rate_twr, 0),
      validNumber(r.totalRateMwr, r.total_rate_mwr, 0),
      validNumber(r.eodTotalAssetsCny, r.eod_total_assets_cny, 0),
      validNumber(r.eodMarketValueCny, r.eod_market_value_cny, 0),
      validNumber(r.eodCashCny, r.eod_cash_cny, 0),
      validNumber(r.eodCashRatio, r.eod_cash_ratio, 0),
      validNumber(r.eodPrincipalCny, r.eod_principal_cny, 0),
      validNumber(r.eodFxUsdCny, r.eod_fx_usd_cny, 0),
      validNumber(r.eodFxHkdCny, r.eod_fx_hkd_cny, 0),
      String(r.sourceVersion || r.source_version || "").slice(0, 64),
      now,
    ]
  );
}

async function upsertSymbolHomeSummaryBatch(rows, userId = null, accountScope = "all") {
  const uid = String(userId || (await getCliUserId())).trim();
  const list = Array.isArray(rows) ? rows : [];
  const sc = String(accountScope || "all").trim() || "all";
  if (!uid || !list.length) {
    return 0;
  }
  await ensureHomeSummaryTables();
  const p = await initPool();
  const c = await p.connect();
  try {
    await c.query("BEGIN");
    for (const raw of list) {
      const r = raw || {};
      const now = validNumber(r.computedAt, r.computed_at, nowMs());
      await c.query(
        `INSERT INTO symbol_home_summary (
           user_id, account_scope, symbol, frozen_through, first_trade_date,
           month_profit_native, ytd_profit_native, total_profit_native,
           total_rate_twr, total_rate_mwr, currency, source_version, computed_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
         ON CONFLICT (user_id, account_scope, symbol) DO UPDATE SET
           frozen_through = EXCLUDED.frozen_through,
           first_trade_date = EXCLUDED.first_trade_date,
           month_profit_native = EXCLUDED.month_profit_native,
           ytd_profit_native = EXCLUDED.ytd_profit_native,
           total_profit_native = EXCLUDED.total_profit_native,
           total_rate_twr = EXCLUDED.total_rate_twr,
           total_rate_mwr = EXCLUDED.total_rate_mwr,
           currency = EXCLUDED.currency,
           source_version = EXCLUDED.source_version,
           computed_at = EXCLUDED.computed_at`,
        [
          uid,
          sc,
          normalizeSymbol(String(r.symbol || "").trim()),
          toDateKey(r.frozenThrough || r.frozen_through),
          r.firstTradeDate != null || r.first_trade_date != null ? String(r.firstTradeDate || r.first_trade_date).slice(0, 10) : null,
          validNumber(r.monthProfitNative, r.month_profit_native, 0),
          validNumber(r.ytdProfitNative, r.ytd_profit_native, 0),
          validNumber(r.totalProfitNative, r.total_profit_native, 0),
          validNumber(r.totalRateTwr, r.total_rate_twr, 0),
          validNumber(r.totalRateMwr, r.total_rate_mwr, 0),
          String(r.currency || "CNY").toUpperCase().slice(0, 3) || "CNY",
          String(r.sourceVersion || r.source_version || "").slice(0, 64),
          now,
        ]
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

async function getHomeSummaryForUser(userId, accountScope = "all") {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { account: null, symbols: [] };
  }
  const sc = String(accountScope || "all").trim() || "all";
  const { rows: ar } = await q("SELECT * FROM account_home_summary WHERE user_id = $1 AND account_scope = $2", [uid, sc]);
  const { rows: sr } = await q(
    "SELECT * FROM symbol_home_summary WHERE user_id = $1 AND account_scope = $2 ORDER BY symbol ASC",
    [uid, sc]
  );
  return { account: ar[0] || null, symbols: sr };
}

function parseAppSettingsFromRows(rows, accounts = []) {
  const settings = { ...DEFAULT_SETTINGS };
  for (const row of rows || []) {
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
  settings.accounts = accounts;
  return settings;
}

function mapUserMetricsMetaRow(row) {
  if (!row) {
    return { dataVersion: 0, rebuilding: false, frozenThrough: null, isCleared: false, clearedAt: null };
  }
  return {
    dataVersion: Number(row.data_version) || 0,
    rebuilding: row.rebuilding === true,
    frozenThrough: row.frozen_through || null,
    isCleared: row.is_cleared === true,
    clearedAt: row.cleared_at || null,
  };
}

/**
 * Vercel：一次 Neon 连接拉齐首页冻结包，避免每表单独 q() 重复 connect（8 次可达 60s+）。
 * 非 Vercel 返回 null，由调用方走常规多 q() 路径。
 */
async function fetchHomeBundleFrozenPack(userId, accountScope = "all") {
  const uid = String(userId || "").trim();
  const scope = String(accountScope || "all").trim() || "all";
  if (!uid) {
    return null;
  }
  return withOneShotDbClient(async (client) => {
    const cq = (text, params) => client.query(text, params);
    const settingsRes = await cq("SELECT key, value FROM app_settings WHERE user_id = $1", [uid]);
    const homeAccRes = await cq(
      "SELECT * FROM account_home_summary WHERE user_id = $1 AND account_scope = $2",
      [uid, scope],
    );
    const symRes = await cq(
      "SELECT * FROM symbol_home_summary WHERE user_id = $1 AND account_scope = $2 ORDER BY symbol ASC",
      [uid, scope],
    );
    const umRes = await cq(
      "SELECT data_version, rebuilding, frozen_through, is_cleared, cleared_at FROM user_metrics_meta WHERE user_id = $1",
      [uid],
    );
    const accountsRes = await cq(
      "SELECT id, name, currency, created_at, updated_at FROM accounts WHERE user_id = $1 ORDER BY created_at ASC",
      [uid],
    );
    let accountMetaList = [];
    try {
      const accMetaRes = await cq(
        "SELECT account_id, is_cleared, cleared_at, frozen_through, updated_at FROM account_metrics_meta WHERE user_id = $1",
        [uid],
      );
      accountMetaList = (accMetaRes.rows || []).map((r) => ({
        accountId: String(r.account_id),
        isCleared: r.is_cleared === true,
        clearedAt: r.cleared_at || null,
        frozenThrough: r.frozen_through || null,
        updatedAt: Number(r.updated_at) || 0,
      }));
    } catch {
      accountMetaList = [];
    }
    const accounts = (accountsRes.rows || []).map((r) => ({
      id: String(r.id),
      name: String(r.name || ""),
      currency: String(r.currency || "CNY").toUpperCase(),
      createdAt: Number(r.created_at) || 0,
      updatedAt: Number(r.updated_at) || 0,
    }));
    const homeAccount = homeAccRes.rows[0] || null;
    const frozenThrough = String(homeAccount?.frozen_through || "").slice(0, 10);
    let lastEodRows = [];
    if (frozenThrough && String(process.env.HOME_BUNDLE_SKIP_EOD || "").trim() !== "1") {
      const from = addCalendarDays(frozenThrough, -14);
      const eodSql =
        scope === "all"
          ? `SELECT DISTINCT ON (account_id, symbol) account_id, symbol, eod_shares, date
             FROM symbol_daily_pnl WHERE user_id = $1 AND date >= $2 AND date <= $3
             ORDER BY account_id, symbol, date DESC`
          : `SELECT DISTINCT ON (account_id, symbol) account_id, symbol, eod_shares, date
             FROM symbol_daily_pnl WHERE user_id = $1 AND account_id = $2 AND date >= $3 AND date <= $4
             ORDER BY account_id, symbol, date DESC`;
      const eodParams = scope === "all" ? [uid, from, frozenThrough] : [uid, scope, from, frozenThrough];
      try {
        const eodRes = await cq(eodSql, eodParams);
        lastEodRows = (eodRes.rows || []).map((r) => ({
          accountId: String(r.account_id),
          symbol: String(r.symbol),
          eodShares: Number(r.eod_shares),
          date: String(r.date),
        }));
      } catch {
        lastEodRows = [];
      }
    }
    return {
      scope,
      settings: parseAppSettingsFromRows(settingsRes.rows, accounts),
      home: { account: homeAccount, symbols: symRes.rows || [] },
      um: mapUserMetricsMetaRow(umRes.rows[0]),
      accountMetaList,
      lastEodRows,
      singleConnection: true,
    };
  });
}

const PERFORMANCE_PRESET_KEYS = new Set(["last_7d", "last_30d", "last_90d", "mtd", "ytd", "inception"]);

/**
 * 读取某冻结日上的业绩曲线缓存（twr + mwr 两行）。as_of 缺省时取该用户该账户该 preset 下最大的 as_of_date。
 * @returns {Promise<{ asOfDate: string, twr: object|null, mwr: object|null }|null>}
 */
async function getPerformancePresetSnapshot(userId, accountId, preset, asOfDateOpt) {
  const uid = String(userId || "").trim();
  const acc = String(accountId || "all").trim() || "all";
  const presetKey = String(preset || "").trim();
  if (!uid || !PERFORMANCE_PRESET_KEYS.has(presetKey)) {
    return null;
  }
  let asOf = String(asOfDateOpt || "").trim().slice(0, 10);
  if (!asOf) {
    const { rows: mx } = await q(
      `SELECT MAX(as_of_date) AS d FROM performance_series_cache
       WHERE user_id = $1 AND account_id = $2 AND preset = $3`,
      [uid, acc, presetKey]
    );
    asOf = mx[0]?.d ? String(mx[0].d).slice(0, 10) : "";
  }
  if (!asOf) {
    return null;
  }
  const { rows } = await q(
    `SELECT algo, period_return, series_json, rule_version, as_of_date
     FROM performance_series_cache
     WHERE user_id = $1 AND account_id = $2 AND preset = $3 AND as_of_date = $4
       AND algo IN ('twr','mwr')`,
    [uid, acc, presetKey, asOf]
  );
  const out = { asOfDate: asOf, twr: null, mwr: null };
  for (const row of rows) {
    const algo = String(row.algo || "").trim();
    const slot = {
      periodReturn: validNumber(row.period_return, 0),
      ruleVersion: Number.isFinite(Number(row.rule_version)) ? Math.trunc(Number(row.rule_version)) : 1,
      seriesJson: row.series_json == null ? null : String(row.series_json),
      asOfDate: String(row.as_of_date || asOf).slice(0, 10),
    };
    if (algo === "twr") {
      out.twr = slot;
    } else if (algo === "mwr") {
      out.mwr = slot;
    }
  }
  return out;
}

async function upsertPerformanceSeriesCacheRow(input) {
  const r = input || {};
  const now = nowMs();
  const rv = Number.isFinite(Number(r.rule_version)) ? Math.trunc(Number(r.rule_version)) : 1;
  await q(
    `INSERT INTO performance_series_cache (
       user_id, account_id, as_of_date, preset, algo, period_return, series_json, source_frozen_through, rule_version, updated_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
     ON CONFLICT (user_id, account_id, as_of_date, preset, algo) DO UPDATE SET
       period_return = EXCLUDED.period_return,
       series_json = EXCLUDED.series_json,
       source_frozen_through = EXCLUDED.source_frozen_through,
       rule_version = EXCLUDED.rule_version,
       updated_at = EXCLUDED.updated_at`,
    [
      String(r.user_id || "").trim(),
      String(r.account_id || "default").trim() || "default",
      String(r.as_of_date || "").slice(0, 10),
      String(r.preset || "").trim(),
      String(r.algo || "").trim(),
      validNumber(r.period_return, 0),
      r.series_json == null ? null : String(r.series_json),
      String(r.source_frozen_through || "").slice(0, 10),
      rv,
      now,
    ]
  );
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
    await c.query("DELETE FROM performance_series_cache WHERE user_id = $1", [uid]);
    await c.query("DELETE FROM account_home_summary WHERE user_id = $1", [uid]);
    await c.query("DELETE FROM symbol_home_summary WHERE user_id = $1", [uid]);
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
  const candidates = symbolQueryCandidates(symbol);
  if (!candidates.length) {
    return [];
  }
  const primary = candidates[0];
  const from = fromDate && String(fromDate).trim() ? String(fromDate).trim() : "1970-01-01";
  const to = toDate && String(toDate).trim() ? String(toDate).trim() : "9999-12-31";
  const { rows } = await q(
    `SELECT symbol, date, close, source, updated_at
     FROM symbol_daily_close
     WHERE symbol = ANY($1::text[])
       AND date >= $2
       AND date <= $3
     ORDER BY date ASC,
              CASE WHEN symbol = $4 THEN 0 ELSE 1 END ASC,
              updated_at DESC`,
    [candidates, from, to, primary]
  );
  const dedupByDate = new Map();
  for (const row of rows) {
    const date = String(row.date || "").slice(0, 10);
    if (!date || dedupByDate.has(date)) {
      continue;
    }
    dedupByDate.set(date, {
      date,
      close: Number(row.close),
      source: row.source || "",
    });
  }
  return [...dedupByDate.values()];
}

async function getLatestSymbolDailyClose(symbol) {
  const candidates = symbolQueryCandidates(symbol);
  if (!candidates.length) {
    return null;
  }
  const primary = candidates[0];
  const { rows } = await q(
    `SELECT symbol, close, date
     FROM symbol_daily_close
     WHERE symbol = ANY($1::text[])
     ORDER BY date DESC,
              CASE WHEN symbol = $2 THEN 0 ELSE 1 END ASC,
              updated_at DESC
     LIMIT 1`,
    [candidates, primary]
  );
  if (!rows[0] || rows[0].close == null) {
    return null;
  }
  return { close: Number(rows[0].close), date: String(rows[0].date) };
}

async function getSymbolDailyCloseBounds(symbol, fromDate, toDate) {
  const candidates = symbolQueryCandidates(symbol);
  if (!candidates.length) {
    return null;
  }
  const primary = candidates[0];
  const from = fromDate && String(fromDate).trim() ? String(fromDate).trim() : "1970-01-01";
  const to = toDate && String(toDate).trim() ? String(toDate).trim() : "9999-12-31";
  const { rows } = await q(
    `SELECT MIN(d.date) AS min_date, MAX(d.date) AS max_date, COUNT(*)::int AS c
     FROM (
       SELECT DISTINCT ON (date) date
       FROM symbol_daily_close
       WHERE symbol = ANY($1::text[])
         AND date >= $2
         AND date <= $3
       ORDER BY date ASC,
                CASE WHEN symbol = $4 THEN 0 ELSE 1 END ASC,
                updated_at DESC
     ) AS d`,
    [candidates, from, to, primary]
  );
  const row = rows[0];
  if (!row || Number(row.c) <= 0) {
    return null;
  }
  return {
    minDate: String(row.min_date || "").slice(0, 10),
    maxDate: String(row.max_date || "").slice(0, 10),
    count: Number(row.c) || 0,
  };
}

async function ensureSymbolNameMapTable() {
  if (symbolNameMapTableReadyPromise) {
    return symbolNameMapTableReadyPromise;
  }
  symbolNameMapTableReadyPromise = (async () => {
    try {
      const { rows } = await q("SELECT to_regclass('public.symbol_name_map') AS t");
      if (rows[0]?.t) {
        return;
      }
    } catch {
      // 探活失败则继续走 DDL
    }
    const ddl = `
      SET lock_timeout = '3000ms';
      SET statement_timeout = '8000ms';
      CREATE TABLE IF NOT EXISTS symbol_name_map (
        symbol TEXT PRIMARY KEY,
        name_cn TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'unknown',
        updated_at BIGINT NOT NULL,
        last_seen_at BIGINT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_symbol_name_map_updated_at ON symbol_name_map (updated_at DESC);
    `;
    await Promise.race([
      q(ddl),
      new Promise((_, reject) => {
        setTimeout(() => reject(new Error("symbol_name_map ddl timeout")), 12_000);
      }),
    ]);
  })().catch((error) => {
    symbolNameMapTableReadyPromise = null;
    throw error;
  });
  return symbolNameMapTableReadyPromise;
}

/**
 * 将历史 gb_ 前缀美股别名并入小写 symbol（维护用）。
 * 已从 symbol_name_map 读写热路径移除；可在一键脚本或管理端按需 force 调用。
 */
async function canonicalizeSymbolNameMapUsAliases(force = false) {
  const now = nowMs();
  if (!force && now - symbolNameMapCanonicalizedAt < SYMBOL_MAP_CANONICALIZE_TTL_MS) {
    return;
  }
  if (symbolNameMapCanonicalizePromise) {
    return symbolNameMapCanonicalizePromise;
  }
  symbolNameMapCanonicalizePromise = (async () => {
    await q(
      `INSERT INTO symbol_name_map (symbol, name_cn, source, updated_at, last_seen_at)
       SELECT lower(substr(symbol, 4)), name_cn, source, updated_at, last_seen_at
       FROM symbol_name_map
       WHERE symbol ~* '^gb_[a-z0-9._-]+$'
       ON CONFLICT (symbol) DO UPDATE SET
         name_cn = CASE
           WHEN length(trim(EXCLUDED.name_cn)) > 0 THEN EXCLUDED.name_cn
           ELSE symbol_name_map.name_cn
         END,
         source = CASE
           WHEN length(trim(EXCLUDED.name_cn)) > 0 THEN EXCLUDED.source
           ELSE symbol_name_map.source
         END,
         updated_at = GREATEST(symbol_name_map.updated_at, EXCLUDED.updated_at),
         last_seen_at = GREATEST(symbol_name_map.last_seen_at, EXCLUDED.last_seen_at)`
    );
    await q("DELETE FROM symbol_name_map WHERE symbol ~* '^gb_[a-z0-9._-]+$'");
    symbolNameMapCanonicalizedAt = nowMs();
  })().finally(() => {
    symbolNameMapCanonicalizePromise = null;
  });
  return symbolNameMapCanonicalizePromise;
}

async function createSymbolNameMapTableNow() {
  const dbUrl = getPgConnectionString();
  if (!dbUrl) {
    throw new Error("Database URL is not configured (DATABASE_URL / POSTGRES_URL)");
  }
  const connectMs = Number(process.env.DATABASE_CONNECT_TIMEOUT_MS || 8000);
  const hardTimeoutMs = Math.max(1000, Number(process.env.DATABASE_PING_HARD_TIMEOUT_MS || connectMs + 4000));
  const client = new Client({
    connectionString: dbUrl,
    ssl: getSslOption(),
    connectionTimeoutMillis: connectMs,
    query_timeout: connectMs,
  });
  let timeoutId = null;
  const withHardTimeout = (promise, stage) =>
    new Promise((resolve, reject) => {
      timeoutId = setTimeout(() => reject(new Error(`symbol_name_map ${stage} timeout after ${hardTimeoutMs}ms`)), hardTimeoutMs);
      promise.then(resolve, reject);
    });
  try {
    await withHardTimeout(client.connect(), "connect");
    await withHardTimeout(
      client.query(`
        SET lock_timeout = '3000ms';
        SET statement_timeout = '8000ms';
        CREATE TABLE IF NOT EXISTS symbol_name_map (
          symbol TEXT PRIMARY KEY,
          name_cn TEXT NOT NULL,
          source TEXT NOT NULL DEFAULT 'unknown',
          updated_at BIGINT NOT NULL,
          last_seen_at BIGINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_symbol_name_map_updated_at ON symbol_name_map (updated_at DESC);
      `),
      "ddl"
    );
    const { rows } = await withHardTimeout(
      client.query("SELECT to_regclass('public.symbol_name_map') AS table_name, to_regclass('public.idx_symbol_name_map_updated_at') AS index_name"),
      "verify"
    );
    return {
      table: rows?.[0]?.table_name || null,
      index: rows?.[0]?.index_name || null,
    };
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
    await client.end().catch(() => {});
  }
}

function normalizeSymbolNameEntry(entry = {}) {
  const symbol = normalizeSymbol(entry.symbol);
  let nameCn = String(entry.nameCn ?? entry.name ?? "").trim();
  const source = String(entry.source || "unknown").trim().slice(0, 32) || "unknown";
  if (!symbol || !nameCn) {
    return null;
  }
  if (nameCn.length > 64) {
    nameCn = nameCn.slice(0, 64);
  }
  const loweredName = nameCn.toLowerCase();
  if (loweredName === symbol.toLowerCase()) {
    return null;
  }
  if (
    /^(sh|sz)\d{6}$/i.test(loweredName) ||
    /^hk\d{5}$/i.test(loweredName) ||
    /^gb_[a-z0-9._-]+$/i.test(loweredName) ||
    /^us_[a-z0-9._-]+$/i.test(loweredName)
  ) {
    return null;
  }
  return { symbol, nameCn, source };
}

async function upsertSymbolNameMapBatch(rows = []) {
  const list = Array.isArray(rows) ? rows.map(normalizeSymbolNameEntry).filter(Boolean) : [];
  if (!list.length) {
    return 0;
  }
  await ensureSymbolNameMapTable();
  /** 写入路径不再跑 gb_ 整表整理，避免与读路径/其它实例争用 symbol_name_map 锁导致全站 pending */
  const latestBySymbol = new Map();
  for (const item of list) {
    latestBySymbol.set(item.symbol, item);
  }
  const deduped = [...latestBySymbol.values()];
  const now = nowMs();
  await q(
    `INSERT INTO symbol_name_map (symbol, name_cn, source, updated_at, last_seen_at)
     SELECT src.symbol, src.name_cn, src.source, $4, $4
     FROM UNNEST($1::text[], $2::text[], $3::text[]) AS src(symbol, name_cn, source)
     ON CONFLICT (symbol) DO UPDATE SET
       name_cn = CASE
         WHEN EXCLUDED.name_cn IS NOT NULL AND length(trim(EXCLUDED.name_cn)) > 0 THEN EXCLUDED.name_cn
         ELSE symbol_name_map.name_cn
       END,
       source = CASE
         WHEN EXCLUDED.name_cn IS NOT NULL AND length(trim(EXCLUDED.name_cn)) > 0 THEN EXCLUDED.source
         ELSE symbol_name_map.source
       END,
       updated_at = CASE
         WHEN EXCLUDED.name_cn IS NOT NULL AND length(trim(EXCLUDED.name_cn)) > 0 THEN EXCLUDED.updated_at
         ELSE symbol_name_map.updated_at
       END,
       last_seen_at = EXCLUDED.last_seen_at`,
    [
      deduped.map((x) => x.symbol),
      deduped.map((x) => x.nameCn),
      deduped.map((x) => x.source),
      now,
    ]
  );
  return deduped.length;
}

async function getSymbolNameMap(symbols = []) {
  await ensureSymbolNameMapTable();
  /** 读路径不跑 gb_ 别名整理：该步骤会扫表 INSERT/DELETE，多 Serverless 实例并发时易锁表，导致 symbol-name-map / home-summary / 行情等大面积 pending。整理仍在 upsertSymbolNameMapBatch 写入时执行。 */
  const uniq = [...new Set((symbols || []).map((s) => normalizeSymbol(String(s || ""))).filter(Boolean))];
  if (!uniq.length) {
    return {};
  }
  const candidates = [...new Set(uniq.flatMap((symbol) => symbolQueryCandidates(symbol)))];
  const { rows } = await q(
    `SELECT symbol, name_cn, updated_at
     FROM symbol_name_map
     WHERE symbol = ANY($1::text[])
     ORDER BY updated_at DESC`,
    [candidates]
  );
  const out = {};
  for (const row of rows || []) {
    const symbol = normalizeSymbol(row.symbol);
    const nameCn = String(row.name_cn || "").trim();
    if (!symbol || !nameCn) {
      continue;
    }
    if (!out[symbol]) {
      out[symbol] = nameCn;
    }
  }
  return out;
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

let perfSchemaV2Promise = null;
/** 已有库升级到 TWR 新列、performance_series_cache、symbol 日净流入列（幂等）。 */
async function ensurePerformanceSchemaV2() {
  if (perfSchemaV2Promise) {
    return perfSchemaV2Promise;
  }
  perfSchemaV2Promise = (async () => {
    await q(`CREATE TABLE IF NOT EXISTS performance_series_cache (
        user_id TEXT NOT NULL,
        account_id TEXT NOT NULL,
        as_of_date TEXT NOT NULL,
        preset TEXT NOT NULL,
        algo TEXT NOT NULL,
        period_return DOUBLE PRECISION NOT NULL DEFAULT 0,
        series_json TEXT,
        source_frozen_through TEXT NOT NULL,
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (user_id, account_id, as_of_date, preset, algo)
      )`).catch(() => {});
    await q(
      `CREATE INDEX IF NOT EXISTS idx_perf_series_user_asof ON performance_series_cache (user_id, as_of_date DESC)`
    ).catch(() => {});
    await q(
      `ALTER TABLE performance_series_cache ADD COLUMN IF NOT EXISTS rule_version INTEGER NOT NULL DEFAULT 1`
    ).catch(() => {});
    await q(
      `ALTER TABLE symbol_daily_pnl ADD COLUMN IF NOT EXISTS day_trade_flow_native DOUBLE PRECISION NOT NULL DEFAULT 0`
    ).catch(() => {});
    await q(
      `ALTER TABLE analysis_daily_snapshot ADD COLUMN IF NOT EXISTS total_assets DOUBLE PRECISION NOT NULL DEFAULT 0`
    ).catch(() => {});
    await q(
      `ALTER TABLE analysis_daily_snapshot ADD COLUMN IF NOT EXISTS cash_cny DOUBLE PRECISION NOT NULL DEFAULT 0`
    ).catch(() => {});
    await q(
      `ALTER TABLE analysis_daily_snapshot ADD COLUMN IF NOT EXISTS cash_ratio DOUBLE PRECISION NOT NULL DEFAULT 0`
    ).catch(() => {});

    const { rows } = await q(
      `SELECT 1 FROM information_schema.columns
       WHERE table_schema = 'public' AND table_name = 'analysis_daily_snapshot' AND column_name = 'tw_r_daily' LIMIT 1`
    );
    if (rows.length) {
      return;
    }
    const stmts = [
      `ALTER TABLE analysis_daily_snapshot ADD COLUMN IF NOT EXISTS tw_r_daily DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `ALTER TABLE analysis_daily_snapshot ADD COLUMN IF NOT EXISTS tw_r_cumulative DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `ALTER TABLE analysis_daily_snapshot ADD COLUMN IF NOT EXISTS external_flow_cny DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `ALTER TABLE analysis_daily_snapshot ADD COLUMN IF NOT EXISTS external_flow_native DOUBLE PRECISION NOT NULL DEFAULT 0`,
      `ALTER TABLE analysis_daily_snapshot DROP COLUMN IF EXISTS rate_cost`,
      `ALTER TABLE analysis_daily_snapshot DROP COLUMN IF EXISTS rate_dietz`,
      `ALTER TABLE analysis_daily_snapshot DROP COLUMN IF EXISTS total_rate_cost`,
      `ALTER TABLE analysis_daily_snapshot DROP COLUMN IF EXISTS total_rate_dietz`,
      `ALTER TABLE analysis_daily_snapshot DROP COLUMN IF EXISTS rate_twr`,
      `ALTER TABLE analysis_daily_snapshot DROP COLUMN IF EXISTS total_rate_twr`,
    ];
    for (const sql of stmts) {
      try {
        await q(sql);
      } catch (e) {
        console.error("[db] ensurePerformanceSchemaV2:", sql.slice(0, 80), e?.message || e);
      }
    }
  })();
  return perfSchemaV2Promise;
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
  if (!a || !b) {
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
    `SELECT date, tw_r_cumulative, profit_cny, market_value, fx_hkd_cny, fx_usd_cny
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

async function getLatestAnalysisSnapshotDate(userId, accountId = "all") {
  const uid = String(userId || "").trim();
  if (!uid) {
    return null;
  }
  const acc = String(accountId || "all").trim() || "all";
  const { rows } = await q(
    "SELECT MAX(date) AS d FROM analysis_daily_snapshot WHERE user_id = $1 AND account_id = $2",
    [uid, acc]
  );
  return rows[0]?.d ? String(rows[0].d) : null;
}

async function listAllUserIds() {
  const { rows } = await q("SELECT id FROM users ORDER BY created_at ASC");
  return rows.map((row) => String(row.id || "").trim()).filter(Boolean);
}

async function getSnapshotWatermark() {
  await ensureSnapshotWatermarkTable();
  const { rows } = await q("SELECT frozen_date, status, message, updated_at FROM snapshot_watermark WHERE id = 1");
  if (!rows[0]) {
    return null;
  }
  return {
    frozenDate: String(rows[0].frozen_date || ""),
    status: String(rows[0].status || ""),
    message: rows[0].message == null ? "" : String(rows[0].message),
    updatedAt: Number(rows[0].updated_at) || 0,
  };
}

async function setSnapshotWatermark(input = {}) {
  await ensureSnapshotWatermarkTable();
  const now = nowMs();
  const frozenDate = toDateKey(input.frozenDate || new Date());
  const status = String(input.status || "success").trim().slice(0, 32) || "success";
  const message = String(input.message || "").trim().slice(0, 400) || null;
  await q(
    `INSERT INTO snapshot_watermark (id, frozen_date, status, message, updated_at)
     VALUES (1, $1, $2, $3, $4)
     ON CONFLICT (id) DO UPDATE SET
       frozen_date = EXCLUDED.frozen_date,
       status = EXCLUDED.status,
       message = EXCLUDED.message,
       updated_at = EXCLUDED.updated_at`,
    [frozenDate, status, message, now]
  );
  return {
    frozenDate,
    status,
    message: message || "",
    updatedAt: now,
  };
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

async function getCommunityFeedTradesRecent(viewerId, limit = 50) {
  const vid = String(viewerId || "").trim();
  if (!vid) {
    return [];
  }
  const lim = Math.min(2000, Math.max(1, Number(limit) || 50));
  const { rows } = await q(
    `SELECT t.id, t.user_id, t.symbol, t.name, t.price, t.quantity, t.amount, t.trade_date, t.note, t.side, t.created_at
     FROM trades t
     INNER JOIN users u ON u.id = t.user_id
     INNER JOIN community_follows f ON f.followee_id = t.user_id AND f.follower_id = $2
     WHERE COALESCE(u.community_public, 1) = 1
       AND t.type = 'trade'
     ORDER BY t.trade_date DESC, t.created_at DESC
     LIMIT $1`,
    [lim, vid]
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
  /** 与 `initPool` 建表语句一致；供 `scripts/migrate-sqlite-to-postgres.js` 等离线工具复用 */
  schemaDdl: DDL,
  SEED_USER_PHONE,
  normalizeSymbol,
  isUsTickerSymbol,
  formatSymbolForDisplay,
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
  getUserMetricsMeta,
  upsertUserMetricsMeta,
  upsertAccountMetricsMeta,
  getAccountMetricsMetaForUser,
  getLastEodSharesForUser,
  insertCronJobRun,
  listCronJobRuns,
  buildAccountKpiSurfaceForScope,
  getSymbolDailyPnl,
  upsertSymbolDailyPnlBatch,
  getAnalysisDailySnapshots,
  upsertAnalysisDailySnapshot,
  upsertAnalysisDailySnapshotBatch,
  deleteAllSymbolDailyPnl,
  deleteAllAnalysisDailySnapshot,
  deletePerformanceSeriesCacheForUser,
  getPerformancePresetSnapshot,
  upsertPerformanceSeriesCacheRow,
  ensureHomeSummaryTables,
  ensureMetricsOpsTables,
  ensureAppDerivedTables,
  deleteHomeSummaryForUser,
  deleteSymbolHomeSummaryForScope,
  upsertAccountHomeSummaryRow,
  upsertSymbolHomeSummaryBatch,
  getHomeSummaryForUser,
  fetchHomeBundleFrozenPack,
  resolveBookCurrencyForAccountScope,
  deleteAllDataForUser,
  upsertSymbolDailyCloseBatch,
  getSymbolDailyCloseRange,
  getLatestSymbolDailyClose,
  getSymbolDailyCloseBounds,
  getSymbolNameMap,
  upsertSymbolNameMapBatch,
  createSymbolNameMapTableNow,
  getTradeWindowForDailyClose,
  addCalendarDays,
  ensurePerformanceSchemaV2,
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
  getLatestAnalysisSnapshotDate,
  selectLatestSymbolDailyDate,
  selectTopSymbolDailyByDate,
  getCommunityFeedTradesRecent,
  listPublicCommunityUserIds,
  listAllUserIds,
  selectSymbolDailyPositionsOnDate,
  getSnapshotWatermark,
  setSnapshotWatermark,
  pingDatabase,
};
