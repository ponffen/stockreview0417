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
  inferMarketTagFromSymbol,
  resolveMarketTagForSymbol,
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
const { computeTradeAmountShareRatio } = require("./trade-amount-share-ratio");
const { loadFxRatesOnDate } = require("./metrics/fx-maps");
const { toDateKey: shanghaiCalendarDateKey } = require("../scripts/lib/market-fetch");
const {
  LEGACY_USER_VALID_UNTIL,
  normalizeValidUntilDate,
  computeNewUserValidUntil,
  isSubscriptionExpired,
} = require("./user-subscription");

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
    notes TEXT,
    community_public INTEGER NOT NULL DEFAULT 1,
    valid_until TEXT
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
    market_tag TEXT NOT NULL DEFAULT 'ot',
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
  `CREATE TABLE IF NOT EXISTS community_posts (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    image_urls TEXT NOT NULL DEFAULT '[]',
    symbols TEXT NOT NULL DEFAULT '[]',
    post_type TEXT NOT NULL DEFAULT 'viewpoint',
    extra TEXT NOT NULL DEFAULT '{}',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_community_posts_user_created ON community_posts (user_id, created_at DESC)`,
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
        await ensureUserSubscriptionSchemaWithClient(c);
        await ensureUserNotesSchemaWithClient(c);
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
    `SELECT id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at, amount_share_ratio, image_urls
     FROM trades WHERE user_id = $1
     ORDER BY trade_date ASC, created_at ASC`,
    [uid]
  );
  return rows.map(rowToTrade);
}

function ledgerListAccountFilterClause(accountId, params) {
  const aid = String(accountId || "").trim();
  if (!aid || aid === "all") {
    return "";
  }
  params.push(aid);
  return ` AND account_id = $${params.length}`;
}

/**
 * 单标的全部成交（新→旧）。无 limit 时返回全量（兼容旧调用）。
 */
async function getTradesForSymbol(userId, symbol, opts = {}) {
  const uid = String(userId || "").trim();
  const sym = normalizeSymbol(symbol);
  if (!uid || !sym) {
    return [];
  }
  const params = [uid, sym];
  const accountClause = ledgerListAccountFilterClause(opts.accountId, params);
  const { rows } = await q(
    `SELECT id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at, amount_share_ratio, image_urls
     FROM trades WHERE user_id = $1 AND symbol = $2${accountClause}
     ORDER BY trade_date DESC, created_at DESC`,
    params
  );
  return rows.map(rowToTrade);
}

/**
 * 单标的成交分页（新→旧），供个股记录页懒加载。
 */
async function getTradesPageForSymbol(userId, symbol, opts = {}) {
  const uid = String(userId || "").trim();
  const sym = normalizeSymbol(symbol);
  if (!uid || !sym) {
    return { data: [], pagination: { limit: 10, offset: 0, total: 0, hasMore: false } };
  }
  const limit = Math.min(100, Math.max(1, Number(opts.limit) || 10));
  const offset = Math.max(0, Number(opts.offset) || 0);
  const params = [uid, sym];
  const accountClause = ledgerListAccountFilterClause(opts.accountId, params);
  const where = `user_id = $1 AND symbol = $2${accountClause}`;
  const { rows: countRows } = await q(`SELECT COUNT(*)::int AS n FROM trades WHERE ${where}`, params);
  const total = Number(countRows[0]?.n) || 0;
  const dataParams = [...params, limit, offset];
  const { rows } = await q(
    `SELECT id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at, amount_share_ratio, image_urls
     FROM trades WHERE ${where}
     ORDER BY trade_date DESC, created_at DESC
     LIMIT $${dataParams.length - 1} OFFSET $${dataParams.length}`,
    dataParams
  );
  const data = rows.map(rowToTrade);
  return {
    data,
    pagination: {
      limit,
      offset,
      total,
      hasMore: offset + data.length < total,
    },
  };
}

/**
 * 交易列表分页（新→旧），供交易记录页按需加载。
 */
async function getTradesPage(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { data: [], pagination: { limit: 10, offset: 0, total: 0, hasMore: false } };
  }
  const limit = Math.min(100, Math.max(1, Number(opts.limit) || 10));
  const offset = Math.max(0, Number(opts.offset) || 0);
  const params = [uid];
  const accountClause = ledgerListAccountFilterClause(opts.accountId, params);
  const where = `user_id = $1${accountClause}`;
  const { rows: countRows } = await q(`SELECT COUNT(*)::int AS n FROM trades WHERE ${where}`, params);
  const total = Number(countRows[0]?.n) || 0;
  const dataParams = [...params, limit, offset];
  const { rows } = await q(
    `SELECT id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at, amount_share_ratio, image_urls
     FROM trades WHERE ${where}
     ORDER BY trade_date DESC, created_at DESC
     LIMIT $${dataParams.length - 1} OFFSET $${dataParams.length}`,
    dataParams
  );
  const data = rows.map(rowToTrade);
  return {
    data,
    pagination: {
      limit,
      offset,
      total,
      hasMore: offset + data.length < total,
    },
  };
}

async function selectAnalysisSnapshotAllAccountOnOrBefore(userId, asOfDate) {
  const uid = String(userId || "").trim();
  const asOf = String(asOfDate || "").slice(0, 10);
  if (!uid || !asOf) {
    return null;
  }
  const { rows } = await q(
    `SELECT date, total_assets, fx_hkd_cny, fx_usd_cny
     FROM analysis_daily_snapshot
     WHERE user_id = $1 AND account_id = 'all' AND date <= $2
     ORDER BY date DESC
     LIMIT 1`,
    [uid, asOf],
  );
  const row = rows[0];
  if (!row) {
    return null;
  }
  return {
    date: String(row.date || "").slice(0, 10),
    totalAssets: Number(row.total_assets),
    fxHkdCny: row.fx_hkd_cny == null ? null : Number(row.fx_hkd_cny),
    fxUsdCny: row.fx_usd_cny == null ? null : Number(row.fx_usd_cny),
  };
}

async function resolveAmountShareRatioForTrade(userId, trade) {
  const safe = normalizeTrade(trade);
  if (safe.type !== "trade") {
    return null;
  }
  const asOf = String(safe.date || "").slice(0, 10);
  if (!asOf) {
    return null;
  }
  const snap = await selectAnalysisSnapshotAllAccountOnOrBefore(userId, asOf);
  if (!snap) {
    return null;
  }
  return computeTradeAmountShareRatio({
    amount: safe.amount,
    symbol: safe.symbol,
    totalAssetsCny: snap.totalAssets,
    fxUsdCny: snap.fxUsdCny,
    fxHkdCny: snap.fxHkdCny,
  });
}

async function upsertTrade(trade, userId) {
  const safe = normalizeTrade(trade);
  const { normalizeStoredImageUrls } = require("./dynamics/blob-images");
  safe.imageUrls = normalizeStoredImageUrls(safe.imageUrls);
  const amountShareRatio =
    safe.type === "trade" ? await resolveAmountShareRatioForTrade(userId, safe) : null;
  const row = tradeToRow({ ...safe, amountShareRatio }, userId);
  if (!row.user_id) {
    throw new Error("userId required");
  }
  let priorImageUrls = [];
  if (safe.id) {
    const { rows: priorRows } = await q(
      `SELECT image_urls FROM trades WHERE user_id = $1 AND id = $2 LIMIT 1`,
      [row.user_id, row.id],
    );
    if (priorRows.length) {
      const { parseImageUrlsField } = require("./dynamics/blob-images");
      priorImageUrls = parseImageUrlsField(priorRows[0].image_urls);
    }
  }
  await q(
    `INSERT INTO trades (
      id, user_id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at, updated_at, amount_share_ratio, image_urls
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
    ON CONFLICT (id) DO UPDATE SET
      user_id = EXCLUDED.user_id, account_id = EXCLUDED.account_id, type = EXCLUDED.type, symbol = EXCLUDED.symbol,
      name = EXCLUDED.name, side = EXCLUDED.side, price = EXCLUDED.price, quantity = EXCLUDED.quantity,
      amount = EXCLUDED.amount, trade_date = EXCLUDED.trade_date, note = EXCLUDED.note, updated_at = EXCLUDED.updated_at,
      amount_share_ratio = EXCLUDED.amount_share_ratio, image_urls = EXCLUDED.image_urls`,
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
      row.amount_share_ratio,
      row.image_urls,
    ],
  );
  if (priorImageUrls.length) {
    const { diffRemovedImageUrls, deleteBlobUrls } = require("./dynamics/blob-images");
    const removed = diffRemovedImageUrls(priorImageUrls, safe.imageUrls);
    if (removed.length) {
      await deleteBlobUrls(removed);
    }
  }
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
  return normalizeTrade({ ...safe, id: row.id, amountShareRatio });
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
      const safe = normalizeTrade(trade);
      const { normalizeStoredImageUrls } = require("./dynamics/blob-images");
      safe.imageUrls = normalizeStoredImageUrls(safe.imageUrls);
      const amountShareRatio =
        safe.type === "trade" ? await resolveAmountShareRatioForTrade(uid, safe) : null;
      const row = tradeToRow({ ...safe, amountShareRatio }, uid);
      await client.query(
        `INSERT INTO trades (
          id, user_id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at, updated_at, amount_share_ratio, image_urls
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        ON CONFLICT (id) DO UPDATE SET
          user_id = EXCLUDED.user_id, account_id = EXCLUDED.account_id, type = EXCLUDED.type, symbol = EXCLUDED.symbol,
          name = EXCLUDED.name, side = EXCLUDED.side, price = EXCLUDED.price, quantity = EXCLUDED.quantity,
          amount = EXCLUDED.amount, trade_date = EXCLUDED.trade_date, note = EXCLUDED.note, updated_at = EXCLUDED.updated_at,
          amount_share_ratio = EXCLUDED.amount_share_ratio, image_urls = EXCLUDED.image_urls`,
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
          row.amount_share_ratio,
          row.image_urls,
        ],
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

async function getTradeByIdForUser(tradeId, userId) {
  const uid = String(userId || "").trim();
  const tid = String(tradeId || "");
  if (!uid || !tid) {
    return null;
  }
  const { rows } = await q(
    `SELECT id, account_id, symbol, trade_date::text AS date FROM trades WHERE user_id = $1 AND id = $2 LIMIT 1`,
    [uid, tid],
  );
  if (!rows.length) {
    return null;
  }
  const r = rows[0];
  return {
    id: r.id,
    accountId: r.account_id,
    symbol: r.symbol,
    date: String(r.date || "").slice(0, 10),
  };
}

/** MCP 防重：同账户/标的/方向/量价金额在邻近交易日已存在则视为重复。 */
async function findLikelyDuplicateTrade(userId, trade, { excludeId = null, windowDays = 7 } = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return null;
  }
  const safe = normalizeTrade(trade);
  const date = String(safe.date || "").slice(0, 10);
  if (!date) {
    return null;
  }
  const win = Math.max(1, Math.min(30, Number(windowDays) || 7));
  const from = addCalendarDays(date, -win);
  const to = addCalendarDays(date, win);
  const { rows } = await q(
    `SELECT id, account_id, symbol, side, type, price, quantity, amount, trade_date::text AS date, created_at
     FROM trades
     WHERE user_id = $1
       AND account_id = $2
       AND symbol = $3
       AND side = $4
       AND type = $5
       AND trade_date >= $6
       AND trade_date <= $7
       AND abs(price - $8) < 0.001
       AND abs(quantity - $9) < 0.0001
       AND abs(amount - $10) < 0.01
       AND ($11::text IS NULL OR id <> $11)
     ORDER BY trade_date ASC, created_at ASC
     LIMIT 1`,
    [
      uid,
      safe.accountId || "default",
      safe.symbol,
      safe.side,
      safe.type,
      from,
      to,
      Number(safe.price) || 0,
      Number(safe.quantity) || 0,
      Number(safe.amount) || 0,
      excludeId ? String(excludeId) : null,
    ],
  );
  if (!rows.length) {
    return null;
  }
  const r = rows[0];
  return {
    id: r.id,
    accountId: r.account_id,
    symbol: r.symbol,
    side: r.side,
    type: r.type,
    price: Number(r.price),
    quantity: Number(r.quantity),
    amount: Number(r.amount),
    date: String(r.date || "").slice(0, 10),
    createdAt: Number(r.created_at) || 0,
  };
}

async function deleteTradeById(tradeId, userId) {
  const uid = String(userId || "").trim();
  const tid = String(tradeId || "");
  const { rows: priorRows } = await q(
    `SELECT account_id, trade_date::text AS date, image_urls FROM trades WHERE user_id = $1 AND id = $2 LIMIT 1`,
    [uid, tid],
  );
  const prior = priorRows[0] || null;
  let deletedAccountId = prior?.account_id || null;
  const { rowCount } = await q("DELETE FROM trades WHERE user_id = $1 AND id = $2", [uid, tid]);
  if (rowCount > 0) {
    const { deleteBlobUrls, parseImageUrlsField } = require("./dynamics/blob-images");
    await deleteBlobUrls(parseImageUrlsField(prior?.image_urls));
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
  return { deleted: rowCount > 0, date: prior?.date || null };
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

/**
 * 银证记录分页（新→旧），供资金记录页按需加载。
 */
async function getCashTransfersPage(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { data: [], pagination: { limit: 10, offset: 0, total: 0, hasMore: false } };
  }
  const limit = Math.min(100, Math.max(1, Number(opts.limit) || 10));
  const offset = Math.max(0, Number(opts.offset) || 0);
  const params = [uid];
  const accountClause = ledgerListAccountFilterClause(opts.accountId, params);
  const where = `user_id = $1${accountClause}`;
  const { rows: countRows } = await q(`SELECT COUNT(*)::int AS n FROM cash_transfers WHERE ${where}`, params);
  const total = Number(countRows[0]?.n) || 0;
  const dataParams = [...params, limit, offset];
  const { rows } = await q(
    `SELECT id, account_id, transfer_date, direction, amount, note, created_at
     FROM cash_transfers WHERE ${where}
     ORDER BY transfer_date DESC, created_at DESC
     LIMIT $${dataParams.length - 1} OFFSET $${dataParams.length}`,
    dataParams
  );
  const data = rows.map(rowToCashTransfer);
  return {
    data,
    pagination: {
      limit,
      offset,
      total,
      hasMore: offset + data.length < total,
    },
  };
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

async function getCashTransferByIdForUser(cashId, userId) {
  const uid = String(userId || "").trim();
  const cid = String(cashId || "");
  if (!uid || !cid) {
    return null;
  }
  const { rows } = await q(
    `SELECT id, account_id, transfer_date::text AS date FROM cash_transfers WHERE user_id = $1 AND id = $2 LIMIT 1`,
    [uid, cid],
  );
  if (!rows.length) {
    return null;
  }
  const r = rows[0];
  return { id: r.id, accountId: r.account_id, date: String(r.date || "").slice(0, 10) };
}

async function deleteCashTransferById(cashId, userId) {
  const uid = String(userId || "").trim();
  const cid = String(cashId || "");
  const prior = await getCashTransferByIdForUser(cid, uid);
  const { rowCount } = await q("DELETE FROM cash_transfers WHERE user_id = $1 AND id = $2", [uid, cid]);
  return { deleted: rowCount > 0, date: prior?.date || null };
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

async function getUserLedgerCounts(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { trades: 0, cashTransfers: 0 };
  }
  const [tradesRes, cashRes] = await Promise.all([
    q("SELECT COUNT(*)::int AS n FROM trades WHERE user_id = $1", [uid]),
    q("SELECT COUNT(*)::int AS n FROM cash_transfers WHERE user_id = $1", [uid]),
  ]);
  return {
    trades: Number(tradesRes.rows[0]?.n) || 0,
    cashTransfers: Number(cashRes.rows[0]?.n) || 0,
  };
}

/** 新注册用户默认关注种子用户「西坡」（SEED_USER_PHONE）。 */
async function ensureDefaultCommunityFollowForUser(followerId) {
  const follower = String(followerId || "").trim();
  if (!follower) {
    return;
  }
  const target = await findUserByPhone(SEED_USER_PHONE);
  if (!target?.id || target.id === follower) {
    return;
  }
  await setCommunityFollow(follower, target.id);
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
  settings.ledgerCounts = await getUserLedgerCounts(uid);
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
    await q(`CREATE TABLE IF NOT EXISTS user_cache_epoch (
        user_id TEXT PRIMARY KEY,
        ledger_epoch INTEGER NOT NULL DEFAULT 0,
        dynamics_epoch INTEGER NOT NULL DEFAULT 0,
        follow_epoch INTEGER NOT NULL DEFAULT 0,
        updated_at BIGINT NOT NULL DEFAULT 0)`).catch(() => {});
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

/** cron / 首次写入前：metrics 运维表幂等建表（勿放在首屏 bootstrap）。 */
async function ensureAppDerivedTables() {
  await ensureMetricsOpsTables();
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

/** 指定冻结日 symbol_daily_pnl 的 EOD 股数/价（用于持仓股数、冻结市值）。 */
async function getSymbolDailyEodRowsAtDate(userId, accountScope, dateKey) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const scope = String(accountScope || "all").trim() || "all";
  const d =
    (await resolveMetricsSnapshotDate(uid, scope, dateKey)) || String(dateKey || "").slice(0, 10);
  if (!d) {
    return [];
  }
  const params = scope === "all" ? [uid, d] : [uid, d, scope];
  const sql =
    scope === "all"
      ? `SELECT account_id, symbol, date, eod_shares,
           COALESCE(NULLIF(eod_price, 0), day_close_price, 0) AS eod_price
         FROM symbol_daily_pnl
         WHERE user_id = $1 AND date = $2
         ORDER BY account_id ASC, symbol ASC`
      : `SELECT account_id, symbol, date, eod_shares,
           COALESCE(NULLIF(eod_price, 0), day_close_price, 0) AS eod_price
         FROM symbol_daily_pnl
         WHERE user_id = $1 AND date = $2 AND account_id = $3
         ORDER BY symbol ASC`;
  const { rows } = await q(sql, params);
  return rows.map((r) => ({
    accountId: String(r.account_id),
    symbol: String(r.symbol),
    date: r.date == null ? d : shanghaiCalendarDateKey(r.date),
    eodShares: Number(r.eod_shares) || 0,
    eodPrice: Number(r.eod_price) || 0,
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

async function getSymbolDailyPnlChartSeries(query = {}, userId = null) {
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
    `SELECT account_id, symbol, date, eod_shares, eod_price, eod_market_value_native, position_weight,
            stage_inception_profit, currency, book_currency, day_close_price
     FROM symbol_daily_pnl
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND date >= $3 AND date <= $4
       AND ($5 = '' OR symbol = $5)
     ORDER BY date ASC`,
    [uid, accountId, from, to, symbol]
  );
  return rows.map((row) => ({
    accountId: row.account_id,
    symbol: row.symbol,
    date: row.date,
    eodShares: Number(row.eod_shares),
    eodPrice: row.eod_price == null ? null : Number(row.eod_price),
    eodMarketValueNative:
      row.eod_market_value_native == null
        ? null
        : Number(row.eod_market_value_native),
    positionWeight: row.position_weight == null ? null : Number(row.position_weight),
    stageInceptionProfit:
      row.stage_inception_profit == null ? null : Number(row.stage_inception_profit),
    stageInceptionRateTwr:
      row.stage_inception_rate_twr == null ? null : Number(row.stage_inception_rate_twr),
    currency: row.currency,
    bookCurrency: row.book_currency,
    dayClosePrice: row.day_close_price == null ? null : Number(row.day_close_price),
  }));
}

function mapSymbolDailyPnlChartRow(row) {
  return {
    accountId: row.account_id,
    symbol: row.symbol,
    date: row.date,
    eodShares: Number(row.eod_shares),
    eodPrice: row.eod_price == null ? null : Number(row.eod_price),
    eodMarketValueNative:
      row.eod_market_value_native == null
        ? null
        : Number(row.eod_market_value_native),
    positionWeight: row.position_weight == null ? null : Number(row.position_weight),
    stageMtdProfit: row.stage_mtd_profit == null ? null : Number(row.stage_mtd_profit),
    stageYtdProfit: row.stage_ytd_profit == null ? null : Number(row.stage_ytd_profit),
    stageInceptionProfit:
      row.stage_inception_profit == null ? null : Number(row.stage_inception_profit),
    stageInceptionRateTwr:
      row.stage_inception_rate_twr == null ? null : Number(row.stage_inception_rate_twr),
    stageLast7dProfit: row.stage_last_7d_profit == null ? null : Number(row.stage_last_7d_profit),
    stageLast30dProfit: row.stage_last_30d_profit == null ? null : Number(row.stage_last_30d_profit),
    stageLast90dProfit: row.stage_last_90d_profit == null ? null : Number(row.stage_last_90d_profit),
    stageMtdProfitBook:
      row.stage_mtd_profit_book == null ? null : Number(row.stage_mtd_profit_book ?? row.stage_mtd_profit),
    stageYtdProfitBook:
      row.stage_ytd_profit_book == null ? null : Number(row.stage_ytd_profit_book ?? row.stage_ytd_profit),
    stageInceptionProfitBook:
      row.stage_inception_profit_book == null
        ? null
        : Number(row.stage_inception_profit_book ?? row.stage_inception_profit),
    stageLast7dProfitBook:
      row.stage_last_7d_profit_book == null ? null : Number(row.stage_last_7d_profit_book ?? row.stage_last_7d_profit),
    stageLast30dProfitBook:
      row.stage_last_30d_profit_book == null ? null : Number(row.stage_last_30d_profit_book ?? row.stage_last_30d_profit),
    stageLast90dProfitBook:
      row.stage_last_90d_profit_book == null ? null : Number(row.stage_last_90d_profit_book ?? row.stage_last_90d_profit),
    stageMtdProfitCny:
      row.stage_mtd_profit_cny == null ? null : Number(row.stage_mtd_profit_cny ?? row.stage_mtd_profit),
    stageYtdProfitCny:
      row.stage_ytd_profit_cny == null ? null : Number(row.stage_ytd_profit_cny ?? row.stage_ytd_profit),
    stageInceptionProfitCny:
      row.stage_inception_profit_cny == null
        ? null
        : Number(row.stage_inception_profit_cny ?? row.stage_inception_profit),
    stageLast7dProfitCny:
      row.stage_last_7d_profit_cny == null ? null : Number(row.stage_last_7d_profit_cny ?? row.stage_last_7d_profit),
    stageLast30dProfitCny:
      row.stage_last_30d_profit_cny == null ? null : Number(row.stage_last_30d_profit_cny ?? row.stage_last_30d_profit),
    stageLast90dProfitCny:
      row.stage_last_90d_profit_cny == null ? null : Number(row.stage_last_90d_profit_cny ?? row.stage_last_90d_profit),
    dailyTradeCount: row.daily_trade_count == null ? null : Number(row.daily_trade_count),
    stageMtdTradeCount: row.stage_mtd_trade_count == null ? null : Number(row.stage_mtd_trade_count),
    stageYtdTradeCount: row.stage_ytd_trade_count == null ? null : Number(row.stage_ytd_trade_count),
    stageInceptionTradeCount:
      row.stage_inception_trade_count == null ? null : Number(row.stage_inception_trade_count),
    stageLast7dTradeCount: row.stage_last_7d_trade_count == null ? null : Number(row.stage_last_7d_trade_count),
    stageLast30dTradeCount: row.stage_last_30d_trade_count == null ? null : Number(row.stage_last_30d_trade_count),
    stageLast90dTradeCount: row.stage_last_90d_trade_count == null ? null : Number(row.stage_last_90d_trade_count),
    currency: row.currency,
    bookCurrency: row.book_currency,
    dayClosePrice: row.day_close_price == null ? null : Number(row.day_close_price),
  };
}

/** 个股图分页：从 endDate 往历史 DESC，再于调用方 reverse 为 ASC。 */
async function getSymbolDailyPnlChartSeriesPage(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const to = query.to != null && String(query.to).trim() ? String(query.to).trim() : "9999-12-31";
  const symbol =
    query.symbol != null && String(query.symbol).trim() ? normalizeSymbol(String(query.symbol).trim()) : "";
  if (!symbol) {
    return [];
  }
  const offset = Math.max(0, Math.floor(Number(query.offset) || 0));
  const limit = Math.max(1, Math.min(200, Math.floor(Number(query.limit) || 30)));
  const { rows } = await q(
    `SELECT account_id, symbol, date, eod_shares, eod_price, eod_market_value_native, position_weight,
            stage_mtd_profit, stage_ytd_profit, stage_inception_profit,
            stage_last_7d_profit, stage_last_30d_profit, stage_last_90d_profit,
            currency, book_currency, day_close_price
     FROM symbol_daily_pnl
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND symbol = $3
       AND date <= $4
     ORDER BY date DESC
     OFFSET $5 LIMIT $6`,
    [uid, accountId, symbol, to, offset, limit],
  );
  return rows.map(mapSymbolDailyPnlChartRow);
}

/** 个股图：按日期闭区间 ASC（mtd/ytd 等）。 */
async function getSymbolDailyPnlChartSeriesDateRange(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const from = query.from != null && String(query.from).trim() ? String(query.from).trim() : "1970-01-01";
  const to = query.to != null && String(query.to).trim() ? String(query.to).trim() : "9999-12-31";
  const symbol =
    query.symbol != null && String(query.symbol).trim() ? normalizeSymbol(String(query.symbol).trim()) : "";
  if (!symbol) {
    return [];
  }
  const { rows } = await q(
    `SELECT account_id, symbol, date, eod_shares, eod_price, eod_market_value_native, position_weight,
            stage_mtd_profit, stage_ytd_profit, stage_inception_profit,
            stage_last_7d_profit, stage_last_30d_profit, stage_last_90d_profit,
            currency, book_currency, day_close_price
     FROM symbol_daily_pnl
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND symbol = $3
       AND date >= $4 AND date <= $5
     ORDER BY date ASC`,
    [uid, accountId, symbol, from, to],
  );
  return rows.map(mapSymbolDailyPnlChartRow);
}

/** 个股图：该账户+标的 symbol_daily_pnl 最早日期（range=all 起点）。 */
async function getEarliestSymbolDailyPnlDate(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return null;
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const symbol =
    query.symbol != null && String(query.symbol).trim() ? normalizeSymbol(String(query.symbol).trim()) : "";
  if (!symbol) {
    return null;
  }
  const { rows } = await q(
    `SELECT MIN(date)::text AS min_date
     FROM symbol_daily_pnl
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND symbol = $3`,
    [uid, accountId, symbol],
  );
  const minDate = rows[0]?.min_date;
  return minDate ? String(minDate).slice(0, 10) : null;
}

/** 账户 scope 下 symbol_daily_pnl 最早日期（排行 inception 起点）。 */
async function getMinSymbolDailyPnlDateForAccount(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return null;
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "all";
  const { rows } = await q(
    `SELECT MIN(date)::text AS min_date
     FROM symbol_daily_pnl
     WHERE user_id = $1 AND account_id = $2`,
    [uid, accountId || "all"],
  );
  const minDate = rows[0]?.min_date;
  return minDate ? String(minDate).slice(0, 10) : null;
}

/** 各标的在 beforeDate 之前最近一行 eod_shares（阶段初持仓结转）。 */
async function getSymbolEodCarryBeforeDate(userId, accountId, beforeDate) {
  const uid = String(userId || "").trim();
  const aid = String(accountId || "all").trim() || "all";
  const before = String(beforeDate || "").slice(0, 10);
  if (!uid || !before) {
    return [];
  }
  const { rows } = await q(
    `SELECT DISTINCT ON (symbol) symbol, eod_shares, date
     FROM symbol_daily_pnl
     WHERE user_id = $1 AND account_id = $2 AND date < $3
     ORDER BY symbol, date DESC`,
    [uid, aid, before],
  );
  return rows.map((r) => ({
    symbol: String(r.symbol),
    eodShares: Number(r.eod_shares) || 0,
    date: String(r.date).slice(0, 10),
  }));
}

async function hasSymbolDailyPnlBeforeDate(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return false;
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const symbol =
    query.symbol != null && String(query.symbol).trim() ? normalizeSymbol(String(query.symbol).trim()) : "";
  const before = query.before != null && String(query.before).trim() ? String(query.before).trim() : "";
  if (!symbol || !before) {
    return false;
  }
  const { rows } = await q(
    `SELECT 1 FROM symbol_daily_pnl
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND symbol = $3
       AND date < $4
     LIMIT 1`,
    [uid, accountId, symbol, before],
  );
  return rows.length > 0;
}

const SYMBOL_DAILY_PNL_ROW_ON_OR_BEFORE_COLS = `account_id, symbol, date, eod_shares, eod_price, eod_market_value_native, position_weight,
            stage_mtd_profit, stage_ytd_profit, stage_inception_profit, stage_inception_rate_twr,
            stage_last_7d_profit, stage_last_30d_profit, stage_last_90d_profit,
            stage_mtd_profit_book, stage_ytd_profit_book, stage_inception_profit_book,
            stage_last_7d_profit_book, stage_last_30d_profit_book, stage_last_90d_profit_book,
            stage_mtd_profit_cny, stage_ytd_profit_cny, stage_inception_profit_cny,
            stage_last_7d_profit_cny, stage_last_30d_profit_cny, stage_last_90d_profit_cny,
            daily_trade_count, stage_mtd_trade_count, stage_ytd_trade_count,
            stage_inception_trade_count, stage_last_7d_trade_count,
            stage_last_30d_trade_count, stage_last_90d_trade_count,
            currency, book_currency, day_close_price`;

/** Live 日 stage profit：取 asOf 及之前最近一行 symbol_daily_pnl（含 stage_*_profit）。 */
async function getSymbolDailyPnlRowOnOrBefore(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return null;
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const symbol =
    query.symbol != null && String(query.symbol).trim() ? normalizeSymbol(String(query.symbol).trim()) : "";
  const asOf = query.asOf != null && String(query.asOf).trim() ? String(query.asOf).trim() : "";
  if (!symbol || !asOf) {
    return null;
  }
  const { rows } = await q(
    `SELECT ${SYMBOL_DAILY_PNL_ROW_ON_OR_BEFORE_COLS}
     FROM symbol_daily_pnl
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND symbol = $3
       AND date <= $4
     ORDER BY date DESC
     LIMIT 1`,
    [uid, accountId, symbol, asOf],
  );
  return rows.length ? mapSymbolDailyPnlChartRow(rows[0]) : null;
}

/** 批量取多标的 asOf 及之前最近一行 symbol_daily_pnl，单次查询。返回 Map(symbol → row)。 */
async function getSymbolDailyPnlRowsOnOrBefore(query = {}, userId = null) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return new Map();
  }
  const accountId = query.accountId != null ? String(query.accountId).trim() : "";
  const asOf = query.asOf != null && String(query.asOf).trim() ? String(query.asOf).trim() : "";
  const symbols = [
    ...new Set((query.symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean)),
  ];
  if (!asOf || !symbols.length) {
    return new Map();
  }
  const { rows } = await q(
    `SELECT DISTINCT ON (symbol) ${SYMBOL_DAILY_PNL_ROW_ON_OR_BEFORE_COLS}
     FROM symbol_daily_pnl
     WHERE user_id = $1
       AND ($2 = '' OR account_id = $2)
       AND symbol = ANY($3::text[])
       AND date <= $4
     ORDER BY symbol, date DESC`,
    [uid, accountId, symbols, asOf],
  );
  const out = new Map();
  for (const row of rows) {
    const sym = normalizeSymbol(row.symbol);
    if (sym) {
      out.set(sym, mapSymbolDailyPnlChartRow(row));
    }
  }
  return out;
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
    `SELECT account_id, date,
      COALESCE(NULLIF(daily_profit, 0), profit_cny) AS profit_cny,
      COALESCE(NULLIF(daily_rate_twr, 0), tw_r_daily) AS tw_r_daily,
      tw_r_cumulative,
      COALESCE(NULLIF(daily_external_flow, 0), external_flow_cny) AS external_flow_cny,
      external_flow_native,
      total_profit, principal, market_value, total_assets,
      COALESCE(NULLIF(cash, 0), cash_cny) AS cash_cny,
      cash_ratio, fx_hkd_cny, fx_usd_cny,
      daily_profit, daily_rate_twr, daily_external_flow, daily_cash_delta, cash, book_currency,
      stage_mtd_profit, stage_mtd_rate_twr, stage_mtd_rate_mwr,
      stage_ytd_profit, stage_ytd_rate_twr, stage_ytd_rate_mwr,
      stage_inception_profit, stage_inception_rate_twr, stage_inception_rate_mwr,
      stage_last_7d_profit, stage_last_7d_rate_twr, stage_last_7d_rate_mwr,
      stage_last_30d_profit, stage_last_30d_rate_twr, stage_last_30d_rate_mwr,
      stage_last_90d_profit, stage_last_90d_rate_twr, stage_last_90d_rate_mwr,
      created_at
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
    dailyProfit: Number(row.daily_profit ?? row.profit_cny),
    twRDaily: Number(row.tw_r_daily),
    dailyRateTwr: Number(row.daily_rate_twr ?? row.tw_r_daily),
    twRCumulative: Number(row.tw_r_cumulative),
    externalFlowCny: Number(row.external_flow_cny),
    dailyExternalFlow: Number(row.daily_external_flow ?? row.external_flow_cny),
    externalFlowNative: Number(row.external_flow_native),
    dailyCashDelta: Number(row.daily_cash_delta ?? 0),
    totalProfit: Number(row.total_profit),
    principal: Number(row.principal),
    marketValue: Number(row.market_value),
    totalAssets: Number(row.total_assets ?? row.totalAssets ?? 0),
    cash: Number(row.cash ?? row.cash_cny ?? 0),
    cashRatio: Number(row.cash_ratio ?? row.cashRatio ?? 0),
    bookCurrency: row.book_currency || "CNY",
    stageMtdProfit: Number(row.stage_mtd_profit ?? 0),
    stageMtdRateTwr: Number(row.stage_mtd_rate_twr ?? 0),
    stageMtdRateMwr: Number(row.stage_mtd_rate_mwr ?? 0),
    stageYtdProfit: Number(row.stage_ytd_profit ?? 0),
    stageYtdRateTwr: Number(row.stage_ytd_rate_twr ?? 0),
    stageYtdRateMwr: Number(row.stage_ytd_rate_mwr ?? 0),
    stageInceptionProfit: Number(row.stage_inception_profit ?? 0),
    stageInceptionRateTwr: Number(row.stage_inception_rate_twr ?? 0),
    stageInceptionRateMwr: Number(row.stage_inception_rate_mwr ?? 0),
    stageLast7dProfit: Number(row.stage_last_7d_profit ?? 0),
    stageLast7dRateTwr: Number(row.stage_last_7d_rate_twr ?? 0),
    stageLast7dRateMwr: Number(row.stage_last_7d_rate_mwr ?? 0),
    stageLast30dProfit: Number(row.stage_last_30d_profit ?? 0),
    stageLast30dRateTwr: Number(row.stage_last_30d_rate_twr ?? 0),
    stageLast30dRateMwr: Number(row.stage_last_30d_rate_mwr ?? 0),
    stageLast90dProfit: Number(row.stage_last_90d_profit ?? 0),
    stageLast90dRateTwr: Number(row.stage_last_90d_rate_twr ?? 0),
    stageLast90dRateMwr: Number(row.stage_last_90d_rate_mwr ?? 0),
    fxHkdCny: row.fx_hkd_cny == null ? null : Number(row.fx_hkd_cny),
    fxUsdCny: row.fx_usd_cny == null ? null : Number(row.fx_usd_cny),
    createdAt: Number(row.created_at),
  }));
}

async function deleteAllSymbolDailyPnl(userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  await q("DELETE FROM symbol_daily_pnl WHERE user_id = $1", [uid]);
}

async function deleteAllAnalysisDailySnapshot(userId = null) {
  const uid = String(userId || (await getCliUserId())).trim();
  await q("DELETE FROM analysis_daily_snapshot WHERE user_id = $1", [uid]);
}

async function getHomeSummaryForUser(userId, accountScope = "all") {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { account: null, symbols: [] };
  }
  const sc = String(accountScope || "all").trim() || "all";
  const { mapAnalysisRowToHomeAccount, mapSymbolRowToHomeSummary, resolveFrozenThrough } = require("./metrics/frozen-pack-v3");
  const { rows: umRows } = await q(
    "SELECT frozen_through FROM user_metrics_meta WHERE user_id = $1",
    [uid],
  );
  const ftMeta = umRows[0]?.frozen_through || null;
  const { rows: ar } = await q(
    `SELECT * FROM analysis_daily_snapshot WHERE user_id = $1 AND account_id = $2
     ORDER BY date DESC LIMIT 1`,
    [uid, sc],
  );
  const frozenThrough = resolveFrozenThrough({ frozen_through: ftMeta }, ar[0]);
  const snapshotDate = await resolveMetricsSnapshotDate(uid, sc, frozenThrough);
  const firstTradeSql =
    sc === "all"
      ? `SELECT MIN(trade_date)::text AS d FROM trades WHERE user_id = $1`
      : `SELECT MIN(trade_date)::text AS d FROM trades WHERE user_id = $1 AND account_id = $2`;
  const firstTradeParams = sc === "all" ? [uid] : [uid, sc];
  const { rows: tr } = await q(firstTradeSql, firstTradeParams);
  const firstTrade = tr[0]?.d ? String(tr[0].d).slice(0, 10) : snapshotDate || frozenThrough;
  const homeAccount = mapAnalysisRowToHomeAccount(ar[0], snapshotDate || frozenThrough, firstTrade);
  let symRows = [];
  if (snapshotDate) {
    const { rows: sr } = await q(
      `SELECT * FROM symbol_daily_pnl WHERE user_id = $1 AND account_id = $2 AND date = $3 ORDER BY symbol ASC`,
      [uid, sc, snapshotDate],
    );
    symRows = sr.map((r) => mapSymbolRowToHomeSummary(r, snapshotDate));
  }
  return { account: homeAccount, symbols: symRows };
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
 * 单次连接拉齐首页所需库表（含 trades/cash），供 home-bundle 实时路径复用。
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
    const {
      mapAnalysisRowToHomeAccount,
      mapSymbolRowToHomeSummary,
      resolveFrozenThrough,
      minFirstTradeDateForScope,
    } = require("./metrics/frozen-pack-v3");
    const homeAccRes = await cq(
      `SELECT * FROM analysis_daily_snapshot WHERE user_id = $1 AND account_id = $2 ORDER BY date DESC LIMIT 1`,
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
    const tradesRes = await cq(
      `SELECT id, account_id, type, symbol, name, side, price, quantity, amount, trade_date, note, created_at
       FROM trades WHERE user_id = $1 ORDER BY trade_date ASC, created_at ASC`,
      [uid],
    );
    const cashRes = await cq(
      `SELECT id, account_id, transfer_date, direction, amount, note, created_at
       FROM cash_transfers WHERE user_id = $1 ORDER BY transfer_date DESC, created_at DESC`,
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
    const umRow = umRes.rows[0] || null;
    const analysisRow = homeAccRes.rows[0] || null;
    const frozenThrough = resolveFrozenThrough(umRow, analysisRow);
    const snapshotDate = await resolveMetricsSnapshotDate(uid, scope, frozenThrough);
    const packTrades = (tradesRes.rows || []).map(rowToTrade);
    const firstTrade = minFirstTradeDateForScope(packTrades, scope, snapshotDate || frozenThrough);
    const homeAccount = mapAnalysisRowToHomeAccount(analysisRow, snapshotDate || frozenThrough, firstTrade);
    const symRes =
      snapshotDate && analysisRow
        ? await cq(
            scope === "all"
              ? `SELECT * FROM symbol_daily_pnl WHERE user_id = $1 AND date = $2 ORDER BY account_id ASC, symbol ASC`
              : `SELECT * FROM symbol_daily_pnl WHERE user_id = $1 AND account_id = $2 AND date = $3 ORDER BY symbol ASC`,
            scope === "all" ? [uid, snapshotDate] : [uid, scope, snapshotDate],
          )
        : { rows: [] };
    const symbolRows = (symRes.rows || []).map((r) => mapSymbolRowToHomeSummary(r, snapshotDate));
    const frozenSymbolEodRows = (symRes.rows || []).map((r) => ({
      accountId: String(r.account_id),
      symbol: String(r.symbol),
      date: snapshotDate,
      eodShares: Number(r.eod_shares) || 0,
      eodPrice: Number(r.eod_price) || Number(r.day_close_price) || 0,
    }));
    let lastEodRows = [];
    if (snapshotDate && String(process.env.HOME_BUNDLE_SKIP_EOD || "").trim() !== "1") {
      const from = addCalendarDays(snapshotDate, -14);
      const eodSql =
        scope === "all"
          ? `SELECT DISTINCT ON (account_id, symbol) account_id, symbol, eod_shares, date
             FROM symbol_daily_pnl WHERE user_id = $1 AND date >= $2 AND date <= $3
             ORDER BY account_id, symbol, date DESC`
          : `SELECT DISTINCT ON (account_id, symbol) account_id, symbol, eod_shares, date
             FROM symbol_daily_pnl WHERE user_id = $1 AND account_id = $2 AND date >= $3 AND date <= $4
             ORDER BY account_id, symbol, date DESC`;
      const eodParams = scope === "all" ? [uid, from, snapshotDate] : [uid, scope, from, snapshotDate];
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
      accounts,
      home: { account: homeAccount, symbols: symbolRows },
      um: mapUserMetricsMetaRow(umRes.rows[0]),
      accountMetaList,
      lastEodRows,
      frozenSymbolEodRows,
      trades: packTrades,
      cashTransfers: (cashRes.rows || []).map(rowToCashTransfer),
      singleConnection: true,
    };
  });
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

async function ensureSymbolNameMapColumns() {
  await q(
    `ALTER TABLE symbol_name_map ADD COLUMN IF NOT EXISTS market_tag TEXT NOT NULL DEFAULT 'ot'`
  );
  await q(`ALTER TABLE symbol_name_map DROP COLUMN IF EXISTS display_code`);
}

async function ensureSymbolNameMapTable() {
  if (symbolNameMapTableReadyPromise) {
    return symbolNameMapTableReadyPromise;
  }
  symbolNameMapTableReadyPromise = (async () => {
    try {
      const { rows } = await q("SELECT to_regclass('public.symbol_name_map') AS t");
      if (rows[0]?.t) {
        await ensureSymbolNameMapColumns();
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
        market_tag TEXT NOT NULL DEFAULT 'ot',
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

function normalizeMarketTagStored(raw) {
  const v = String(raw || "").trim().toLowerCase();
  if (v === "cn" || v === "hk" || v === "us" || v === "ot") {
    return v;
  }
  if (v === "a股" || v === "a") {
    return "cn";
  }
  if (v === "港股" || v === "hk") {
    return "hk";
  }
  if (v === "美股" || v === "us") {
    return "us";
  }
  return "";
}

function normalizeSymbolMetaEntry(entry = {}) {
  const symbol = normalizeSymbol(entry.symbol);
  if (!symbol) {
    return null;
  }
  let nameCn = String(entry.nameCn ?? entry.name ?? "").trim();
  const source = String(entry.source || "unknown").trim().slice(0, 32) || "unknown";
  const marketTag = resolveMarketTagForSymbol(
    symbol,
    normalizeMarketTagStored(entry.marketTag ?? entry.market_tag ?? "")
  );
  if (nameCn.length > 64) {
    nameCn = nameCn.slice(0, 64);
  }
  const loweredName = nameCn.toLowerCase();
  if (
    nameCn &&
    (loweredName === symbol.toLowerCase() ||
      /^(sh|sz)\d{6}$/i.test(loweredName) ||
      /^hk\d{5}$/i.test(loweredName) ||
      /^gb_[a-z0-9._-]+$/i.test(loweredName) ||
      /^us_[a-z0-9._-]+$/i.test(loweredName))
  ) {
    nameCn = "";
  }
  if (!nameCn && !marketTag) {
    return null;
  }
  return {
    symbol,
    nameCn: nameCn || "-",
    marketTag: marketTag || "ot",
    source,
  };
}

function normalizeSymbolNameEntry(entry = {}) {
  const parsed = normalizeSymbolMetaEntry(entry);
  if (!parsed || !parsed.nameCn || parsed.nameCn === "-") {
    return null;
  }
  return parsed;
}

async function upsertSymbolNameMapBatch(rows = []) {
  const list = Array.isArray(rows) ? rows.map(normalizeSymbolMetaEntry).filter(Boolean) : [];
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
    `INSERT INTO symbol_name_map (symbol, name_cn, market_tag, source, updated_at, last_seen_at)
     SELECT src.symbol, src.name_cn, src.market_tag, src.source, $5, $5
     FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[]) AS src(symbol, name_cn, market_tag, source)
     ON CONFLICT (symbol) DO UPDATE SET
       name_cn = CASE
         WHEN EXCLUDED.name_cn IS NOT NULL AND length(trim(EXCLUDED.name_cn)) > 0 AND EXCLUDED.name_cn <> '-' THEN EXCLUDED.name_cn
         ELSE symbol_name_map.name_cn
       END,
       market_tag = CASE
         WHEN EXCLUDED.market_tag IS NOT NULL AND length(trim(EXCLUDED.market_tag)) > 0 AND EXCLUDED.market_tag <> 'ot' THEN EXCLUDED.market_tag
         WHEN symbol_name_map.market_tag IS NULL OR symbol_name_map.market_tag = '' OR symbol_name_map.market_tag = 'ot' THEN EXCLUDED.market_tag
         ELSE symbol_name_map.market_tag
       END,
       source = CASE
         WHEN EXCLUDED.name_cn IS NOT NULL AND length(trim(EXCLUDED.name_cn)) > 0 AND EXCLUDED.name_cn <> '-' THEN EXCLUDED.source
         WHEN EXCLUDED.market_tag IS NOT NULL AND EXCLUDED.market_tag <> 'ot' THEN EXCLUDED.source
         ELSE symbol_name_map.source
       END,
       updated_at = $5,
       last_seen_at = $5`,
    [
      deduped.map((x) => x.symbol),
      deduped.map((x) => x.nameCn),
      deduped.map((x) => x.marketTag),
      deduped.map((x) => x.source),
      now,
    ]
  );
  return deduped.length;
}

async function hasSymbolNameMapEntry(symbol) {
  await ensureSymbolNameMapTable();
  const sym = normalizeSymbol(symbol);
  if (!sym) {
    return false;
  }
  const candidates = [...new Set(symbolQueryCandidates(sym))];
  const { rows } = await q(
    `SELECT 1 FROM symbol_name_map WHERE symbol = ANY($1::text[]) LIMIT 1`,
    [candidates]
  );
  return (rows || []).length > 0;
}

async function getSymbolMetaMap(symbols = []) {
  await ensureSymbolNameMapTable();
  const uniq = [...new Set((symbols || []).map((s) => normalizeSymbol(String(s || ""))).filter(Boolean))];
  if (!uniq.length) {
    return {};
  }
  const candidates = [...new Set(uniq.flatMap((symbol) => symbolQueryCandidates(symbol)))];
  const { rows } = await q(
    `SELECT symbol, name_cn, market_tag, updated_at
     FROM symbol_name_map
     WHERE symbol = ANY($1::text[])
     ORDER BY updated_at DESC`,
    [candidates]
  );
  const out = {};
  for (const row of rows || []) {
    const symbol = normalizeSymbol(row.symbol);
    if (!symbol || out[symbol]) {
      continue;
    }
    out[symbol] = {
      nameCn: String(row.name_cn || "").trim() || "-",
      marketTag: resolveMarketTagForSymbol(symbol, normalizeMarketTagStored(row.market_tag) || "ot"),
    };
  }
  return out;
}

async function getSymbolNameMap(symbols = []) {
  const metaMap = await getSymbolMetaMap(symbols);
  const out = {};
  for (const [symbol, meta] of Object.entries(metaMap)) {
    const name = String(meta?.nameCn || "").trim();
    if (name) {
      out[symbol] = name;
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
/** 已有库升级到 TWR 新列、symbol 日净流入列（幂等）；废弃 performance_series_cache 则 DROP。 */
async function ensurePerformanceSchemaV2() {
  if (perfSchemaV2Promise) {
    return perfSchemaV2Promise;
  }
  perfSchemaV2Promise = (async () => {
    await q(`DROP TABLE IF EXISTS performance_series_cache`).catch(() => {});
    await q(`DROP TABLE IF EXISTS account_home_summary`).catch(() => {});
    await q(`DROP TABLE IF EXISTS symbol_home_summary`).catch(() => {});
    await q(`DROP TABLE IF EXISTS daily_returns`).catch(() => {});
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
    await q(`ALTER TABLE trades ADD COLUMN IF NOT EXISTS amount_share_ratio DOUBLE PRECISION`).catch(() => {});
    await q(`ALTER TABLE trades ADD COLUMN IF NOT EXISTS image_urls TEXT NOT NULL DEFAULT '[]'`).catch(() => {});
    await q(`
      CREATE TABLE IF NOT EXISTS community_posts (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        content TEXT NOT NULL DEFAULT '',
        image_urls TEXT NOT NULL DEFAULT '[]',
        symbols TEXT NOT NULL DEFAULT '[]',
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL
      )
    `).catch(() => {});
    await q(`
      CREATE INDEX IF NOT EXISTS idx_community_posts_user_created
        ON community_posts (user_id, created_at DESC)
    `).catch(() => {});
    await q(`ALTER TABLE community_posts ADD COLUMN IF NOT EXISTS post_type TEXT NOT NULL DEFAULT 'viewpoint'`).catch(
      () => {},
    );
    await q(`ALTER TABLE community_posts ADD COLUMN IF NOT EXISTS extra TEXT NOT NULL DEFAULT '{}'`).catch(() => {});
    await q(`
      CREATE INDEX IF NOT EXISTS idx_community_posts_user_type_created
        ON community_posts (user_id, post_type, created_at DESC)
    `).catch(() => {});

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
    const { ensureMetricsSchemaV3 } = require("./metrics/schema-v3");
    await ensureMetricsSchemaV3();
  })();
  return perfSchemaV2Promise;
}

function closeDatabase() {
  if (pool) {
    return pool.end();
  }
  return Promise.resolve();
}

async function listTradesForAmountShareBackfill() {
  const { rows } = await q(
    `SELECT id, user_id, symbol, amount, trade_date
     FROM trades
     WHERE type = 'trade'
     ORDER BY user_id ASC, trade_date ASC, created_at ASC`,
  );
  return rows;
}

async function setTradeAmountShareRatio(tradeId, ratio) {
  await q(`UPDATE trades SET amount_share_ratio = $2 WHERE id = $1`, [tradeId, ratio]);
}

/**
 * 回填某用户全部成交的 amount_share_ratio（按每笔成交 trade_date 的历史时点快照）。
 * 总资产口径：analysis_daily_snapshot 中 account_id='all'、成交日当天或之前最近一个冻结日的 total_assets，
 * 与新增成交时 resolveAmountShareRatioForTrade 完全一致。仅写 amount_share_ratio，不动其他字段。
 * 供日冻结（增量 / 全量重算）在快照重建后调用。
 */
async function backfillTradeAmountShareRatiosForUser(userId, options = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { trades: 0, updated: 0, nullCount: 0 };
  }
  const logger = options.logger || null;
  const fromDate = String(options.fromDate || "").slice(0, 10);
  const { rows } = await q(
    `SELECT id, symbol, amount, trade_date
     FROM trades
     WHERE user_id = $1 AND type = 'trade'
       ${fromDate ? "AND trade_date >= $2" : ""}
     ORDER BY trade_date ASC, created_at ASC`,
    fromDate ? [uid, fromDate] : [uid],
  );
  const snapCache = new Map();
  let updated = 0;
  let nullCount = 0;
  for (const row of rows) {
    const asOf = String(row.trade_date || "").slice(0, 10);
    let ratio = null;
    if (asOf) {
      let snap = snapCache.get(asOf);
      if (snap === undefined) {
        snap = await selectAnalysisSnapshotAllAccountOnOrBefore(uid, asOf);
        snapCache.set(asOf, snap);
      }
      if (snap) {
        let fxUsdCny = snap.fxUsdCny;
        let fxHkdCny = snap.fxHkdCny;
        if (!(Number(fxUsdCny) > 0) || !(Number(fxHkdCny) > 0)) {
          const dbFx = await loadFxRatesOnDate(asOf);
          if (!(Number(fxUsdCny) > 0)) {
            fxUsdCny = dbFx.USD;
          }
          if (!(Number(fxHkdCny) > 0)) {
            fxHkdCny = dbFx.HKD;
          }
        }
        ratio = computeTradeAmountShareRatio({
          amount: Number(row.amount),
          symbol: row.symbol,
          totalAssetsCny: snap.totalAssets,
          fxUsdCny,
          fxHkdCny,
        });
      }
    }
    await setTradeAmountShareRatio(row.id, ratio);
    if (ratio == null) {
      nullCount += 1;
    } else {
      updated += 1;
    }
  }
  if (logger && typeof logger.log === "function") {
    logger.log(
      `[amount-share-backfill] user=${uid} trades=${rows.length} withRatio=${updated} null=${nullCount}`,
    );
  }
  return { trades: rows.length, updated, nullCount };
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

let userSubscriptionSchemaPromise = null;

async function ensureUserSubscriptionSchemaWithClient(client) {
  await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS valid_until TEXT`);
  await client.query(
    `UPDATE users SET valid_until = $1 WHERE valid_until IS NULL OR TRIM(valid_until) = ''`,
    [LEGACY_USER_VALID_UNTIL]
  );
}

async function ensureUserSubscriptionSchema() {
  if (userSubscriptionSchemaPromise) {
    return userSubscriptionSchemaPromise;
  }
  userSubscriptionSchemaPromise = (async () => {
    await q(`ALTER TABLE users ADD COLUMN IF NOT EXISTS valid_until TEXT`).catch(() => {});
    await q(`UPDATE users SET valid_until = $1 WHERE valid_until IS NULL OR TRIM(valid_until) = ''`, [
      LEGACY_USER_VALID_UNTIL,
    ]).catch(() => {});
  })();
  return userSubscriptionSchemaPromise;
}

let userNotesSchemaPromise = null;

async function ensureUserNotesSchemaWithClient(client) {
  await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS notes TEXT`);
  await client.query(`
    DO $$ BEGIN
      ALTER TABLE users ADD CONSTRAINT users_notes_len CHECK (notes IS NULL OR length(notes) <= 1000);
    EXCEPTION WHEN duplicate_object THEN NULL;
    END $$
  `);
  await client.query(`
    UPDATE users
    SET notes = TRIM(nickname)
    WHERE (notes IS NULL OR TRIM(notes) = '')
      AND nickname IS NOT NULL AND TRIM(nickname) != ''
  `);
}

async function ensureUserNotesSchema() {
  if (userNotesSchemaPromise) {
    return userNotesSchemaPromise;
  }
  userNotesSchemaPromise = (async () => {
    await q(`ALTER TABLE users ADD COLUMN IF NOT EXISTS notes TEXT`).catch(() => {});
    await q(`
      DO $$ BEGIN
        ALTER TABLE users ADD CONSTRAINT users_notes_len CHECK (notes IS NULL OR length(notes) <= 1000);
      EXCEPTION WHEN duplicate_object THEN NULL;
      END $$
    `).catch(() => {});
    await q(`
      UPDATE users
      SET notes = TRIM(nickname)
      WHERE (notes IS NULL OR TRIM(notes) = '')
        AND nickname IS NOT NULL AND TRIM(nickname) != ''
    `).catch(() => {});
  })();
  return userNotesSchemaPromise;
}

async function getUserValidUntil(userId) {
  await ensureUserSubscriptionSchema();
  const uid = String(userId || "").trim();
  if (!uid) {
    return LEGACY_USER_VALID_UNTIL;
  }
  const { rows } = await q("SELECT valid_until FROM users WHERE id = $1", [uid]);
  return normalizeValidUntilDate(rows[0]?.valid_until);
}

async function getAuthSessionUserPayload(userId) {
  await ensureUserSubscriptionSchema();
  await ensureUserNotesSchema();
  const uid = String(userId || "").trim();
  const phone = await getUserPhone(uid);
  const validUntil = await getUserValidUntil(uid);
  const row = await getUserCommunityRow(uid);
  const { maskPhone, displayNameForUser } = require("./community-service");
  return {
    id: uid,
    phone,
    phoneMasked: maskPhone(phone),
    nickname: row?.nickname != null && String(row.nickname).trim() ? String(row.nickname).trim() : null,
    communityPublic: row?.community_public != null ? !!Number(row.community_public) : true,
    displayName: row ? displayNameForUser(row) : maskPhone(phone),
    validUntil,
    expired: isSubscriptionExpired(validUntil),
  };
}

async function createRegisteredUser(phone, passwordPlain) {
  const p = String(phone || "").trim();
  if (!isValidPhone(p) || !isValidPasswordDigits(passwordPlain)) {
    throw new Error("invalid phone or password");
  }
  if (await findUserByPhone(p)) {
    throw new Error("phone already registered");
  }
  await ensureUserSubscriptionSchema();
  await ensureUserNotesSchema();
  const id = randomUUID();
  const now = nowMs();
  const validUntil = computeNewUserValidUntil();
  await q(
    "INSERT INTO users (id, phone, password_hash, created_at, updated_at, valid_until) VALUES ($1,$2,$3,$4,$5,$6)",
    [id, p, hashPassword(passwordPlain), now, now, validUntil]
  );
  await migrateAccountsIfEmptyForUser(id);
  try {
    await ensureDefaultCommunityFollowForUser(id);
  } catch (err) {
    console.warn("[createRegisteredUser] default community follow failed:", err?.message || err);
  }
  return { id, phone: p, validUntil };
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
  const { bumpFollowEpoch } = require("./cache-epoch");
  await bumpFollowEpoch(uid);
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
    const { bumpFollowEpoch } = require("./cache-epoch");
    await bumpFollowEpoch(a);
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
  if (rowCount > 0) {
    const { bumpFollowEpoch } = require("./cache-epoch");
    await bumpFollowEpoch(a);
  }
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

/** 不晚于 hint 的最近 symbol_daily_pnl 日期（scope=all 时跨账户取 MAX）。 */
async function selectLatestSymbolDailyDateOnOrBefore(userId, accountScope, hint) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return null;
  }
  const scope = String(accountScope || "all").trim() || "all";
  const h = String(hint || "").slice(0, 10);
  const sql =
    scope === "all"
      ? `SELECT MAX(date) AS d FROM symbol_daily_pnl WHERE user_id = $1 AND ($2 = '' OR date <= $2)`
      : `SELECT MAX(date) AS d FROM symbol_daily_pnl WHERE user_id = $1 AND account_id = $2 AND ($3 = '' OR date <= $3)`;
  const params = scope === "all" ? [uid, h] : [uid, scope, h];
  const { rows } = await q(sql, params);
  return rows[0]?.d ? String(rows[0].d).slice(0, 10) : null;
}

/** 读路径：frozen_through 与 analysis / symbol 最后快照日对齐。 */
async function resolveMetricsSnapshotDate(userId, accountScope, hint) {
  const { capFrozenThroughToSnapshot } = require("./metrics/freeze-calendar");
  const scope = String(accountScope || "all").trim() || "all";
  const acc = scope === "all" ? "all" : scope;
  const h = String(hint || "").slice(0, 10);
  const analysisDate = await getLatestAnalysisSnapshotDate(userId, acc);
  let d = capFrozenThroughToSnapshot(h, analysisDate);
  const symDate = await selectLatestSymbolDailyDateOnOrBefore(userId, scope, d || h);
  d = capFrozenThroughToSnapshot(d, symDate);
  return d || symDate || analysisDate || h || null;
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

/** 有成交、未清仓，且 all 账户快照日早于 target 的用户（日终 cron 漏跑检测）。 */
async function listFreezeLagUserIds(targetDate, options = {}) {
  const { normDateKey, isWeekendDateKey, previousSessionDate } = require("./metrics/freeze-calendar");
  let target = normDateKey(targetDate);
  if (!target) {
    return [];
  }
  while (target && isWeekendDateKey(target)) {
    target = previousSessionDate(target);
  }
  if (!target) {
    return [];
  }
  const scope = Array.isArray(options.scopeUserIds)
    ? [...new Set(options.scopeUserIds.map((id) => String(id || "").trim()).filter(Boolean))]
    : null;
  const params = [target];
  let scopeClause = "";
  if (scope?.length) {
    params.push(scope);
    scopeClause = `AND u.id::text = ANY($${params.length}::text[])`;
  }
  const { rows } = await q(
    `SELECT u.id::text AS id
     FROM users u
     WHERE EXISTS (SELECT 1 FROM trades t WHERE t.user_id = u.id LIMIT 1)
       AND NOT EXISTS (
         SELECT 1 FROM user_metrics_meta um
         WHERE um.user_id = u.id AND um.is_cleared IS TRUE
       )
       AND COALESCE(
         (SELECT MAX(a.date) FROM analysis_daily_snapshot a
          WHERE a.user_id = u.id AND a.account_id = 'all'),
         '1970-01-01'
       ) < $1
       ${scopeClause}
     ORDER BY u.created_at ASC`,
    params,
  );
  return rows.map((row) => String(row.id || "").trim()).filter(Boolean);
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
    `SELECT t.id, t.user_id, t.symbol, t.name, t.price, t.quantity, t.amount, t.trade_date, t.note, t.side, t.created_at,
            t.amount_share_ratio, t.image_urls, t.account_id, u.nickname, u.phone
     FROM trades t
     INNER JOIN users u ON u.id = t.user_id
     INNER JOIN community_follows f ON f.followee_id = t.user_id AND f.follower_id = $2
     WHERE COALESCE(u.community_public, 1) = 1
       AND t.type = 'trade'
     ORDER BY t.trade_date DESC, t.created_at DESC
     LIMIT $1`,
    [lim, vid]
  );
  return rows.map((row) => {
    const ratioRaw = row.amount_share_ratio;
    const amountShareRatio =
      ratioRaw == null || ratioRaw === "" ? null : Number(ratioRaw);
    return {
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
      amountShareRatio: Number.isFinite(amountShareRatio) ? amountShareRatio : null,
      nickname: row.nickname,
      phone: row.phone,
    };
  });
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
     WHERE user_id = $1 AND account_id = $2 AND date = $3 AND abs(eod_shares) > 0.0001
     ORDER BY abs(eod_shares * COALESCE(day_close_price, 0)) DESC`,
    [uid, acc, dk]
  );
  return rows;
}

async function runSchemaDdl(sql) {
  await q(sql);
}

module.exports = {
  DEFAULT_SETTINGS,
  DB_PATH,
  runSchemaDdl,
  schemaDdl: DDL,
  SEED_USER_PHONE,
  normalizeSymbol,
  inferMarketTagFromSymbol,
  resolveMarketTagForSymbol,
  isUsTickerSymbol,
  formatSymbolForDisplay,
  normalizeTrade,
  normalizeAccountRecords,
  normalizeDailyReturn,
  getTrades,
  getTradesForSymbol,
  getTradesPageForSymbol,
  getTradesPage,
  upsertTrade,
  getTradeByIdForUser,
  findLikelyDuplicateTrade,
  importTrades,
  deleteTradeById,
  normalizeCashTransfer,
  getCashTransfers,
  getCashTransfersPage,
  upsertCashTransfer,
  getCashTransferByIdForUser,
  importCashTransfers,
  deleteCashTransferById,
  getAccounts,
  replaceAccountsFromList,
  getSettings,
  setSettings,
  getUserMetricsMeta,
  upsertUserMetricsMeta,
  upsertAccountMetricsMeta,
  getAccountMetricsMetaForUser,
  getLastEodSharesForUser,
  getSymbolDailyEodRowsAtDate,
  insertCronJobRun,
  listCronJobRuns,
  getSymbolDailyPnl,
  getSymbolDailyPnlChartSeries,
  getSymbolDailyPnlChartSeriesPage,
  getSymbolDailyPnlChartSeriesDateRange,
  getEarliestSymbolDailyPnlDate,
  getMinSymbolDailyPnlDateForAccount,
  getSymbolEodCarryBeforeDate,
  hasSymbolDailyPnlBeforeDate,
  getSymbolDailyPnlRowOnOrBefore,
  getSymbolDailyPnlRowsOnOrBefore,
  upsertSymbolDailyPnlBatch,
  getAnalysisDailySnapshots,
  deleteAllSymbolDailyPnl,
  deleteAllAnalysisDailySnapshot,
  ensureMetricsOpsTables,
  ensureAppDerivedTables,
  getHomeSummaryForUser,
  fetchHomeBundleFrozenPack,
  resolveBookCurrencyForAccountScope,
  deleteAllDataForUser,
  upsertSymbolDailyCloseBatch,
  getSymbolDailyCloseRange,
  getLatestSymbolDailyClose,
  getSymbolDailyCloseBounds,
  getSymbolNameMap,
  getSymbolMetaMap,
  hasSymbolNameMapEntry,
  upsertSymbolNameMapBatch,
  createSymbolNameMapTableNow,
  getTradeWindowForDailyClose,
  addCalendarDays,
  ensurePerformanceSchemaV2,
  listTradesForAmountShareBackfill,
  setTradeAmountShareRatio,
  backfillTradeAmountShareRatiosForUser,
  selectAnalysisSnapshotAllAccountOnOrBefore,
  closeDatabase,
  getCliUserId,
  findUserByPhone,
  verifyUserLogin,
  createRegisteredUser,
  ensureUserSubscriptionSchema,
  getUserValidUntil,
  getAuthSessionUserPayload,
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
  selectLatestSymbolDailyDateOnOrBefore,
  resolveMetricsSnapshotDate,
  selectTopSymbolDailyByDate,
  getCommunityFeedTradesRecent,
  listPublicCommunityUserIds,
  listAllUserIds,
  listFreezeLagUserIds,
  selectSymbolDailyPositionsOnDate,
  getSnapshotWatermark,
  setSnapshotWatermark,
  pingDatabase,
  initPool,
  dbQuery: q,
};
