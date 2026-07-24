/**
 * Per-user cache epoch domains + global quote epoch for client-side page cache invalidation.
 */
const { nowMs } = require("./db-pure");
const { dbQuery: q, getUserMetricsMeta } = require("./db");

let cacheEpochSchemaPromise = null;

async function ensureCacheEpochTables() {
  if (cacheEpochSchemaPromise) {
    return cacheEpochSchemaPromise;
  }
  cacheEpochSchemaPromise = (async () => {
    await q(`CREATE TABLE IF NOT EXISTS user_cache_epoch (
      user_id TEXT PRIMARY KEY,
      ledger_epoch INTEGER NOT NULL DEFAULT 0,
      dynamics_epoch INTEGER NOT NULL DEFAULT 0,
      follow_epoch INTEGER NOT NULL DEFAULT 0,
      updated_at BIGINT NOT NULL DEFAULT 0
    )`).catch(() => {});
  })();
  return cacheEpochSchemaPromise;
}

let globalQuoteEpoch = 1;
let globalQuoteTime = null;
let lastQuoteTouchAt = 0;
const QUOTE_TOUCH_MIN_MS = 60 * 1000;

function touchGlobalQuoteEpoch(quoteTime) {
  const now = Date.now();
  if (now - lastQuoteTouchAt < QUOTE_TOUCH_MIN_MS) {
    if (quoteTime) {
      globalQuoteTime = String(quoteTime);
    }
    return globalQuoteEpoch;
  }
  lastQuoteTouchAt = now;
  globalQuoteEpoch += 1;
  if (quoteTime) {
    globalQuoteTime = String(quoteTime);
  }
  return globalQuoteEpoch;
}

function getGlobalQuoteEpoch() {
  return { epoch: globalQuoteEpoch, quoteTime: globalQuoteTime };
}

async function getUserCacheEpochRow(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { ledgerEpoch: 0, dynamicsEpoch: 0, followEpoch: 0 };
  }
  await ensureCacheEpochTables();
  try {
    const { rows } = await q(
      `SELECT ledger_epoch, dynamics_epoch, follow_epoch FROM user_cache_epoch WHERE user_id = $1`,
      [uid],
    );
    const row = rows[0];
    if (!row) {
      return { ledgerEpoch: 0, dynamicsEpoch: 0, followEpoch: 0 };
    }
    return {
      ledgerEpoch: Number(row.ledger_epoch) || 0,
      dynamicsEpoch: Number(row.dynamics_epoch) || 0,
      followEpoch: Number(row.follow_epoch) || 0,
    };
  } catch {
    return { ledgerEpoch: 0, dynamicsEpoch: 0, followEpoch: 0 };
  }
}

async function bumpUserCacheEpoch(userId, field) {
  const uid = String(userId || "").trim();
  if (!uid || !field) {
    return;
  }
  const col =
    field === "ledger"
      ? "ledger_epoch"
      : field === "dynamics"
        ? "dynamics_epoch"
        : field === "follow"
          ? "follow_epoch"
          : null;
  if (!col) {
    return;
  }
  await ensureCacheEpochTables();
  const ts = nowMs();
  await q(
    `INSERT INTO user_cache_epoch (user_id, ledger_epoch, dynamics_epoch, follow_epoch, updated_at)
     VALUES ($1, 0, 0, 0, $2)
     ON CONFLICT (user_id) DO UPDATE SET
       ${col} = user_cache_epoch.${col} + 1,
       updated_at = EXCLUDED.updated_at`,
    [uid, ts],
  );
}

async function bumpLedgerEpoch(userId) {
  await bumpUserCacheEpoch(userId, "ledger");
}

async function bumpDynamicsEpoch(userId) {
  await bumpUserCacheEpoch(userId, "dynamics");
}

async function bumpFollowEpoch(userId) {
  await bumpUserCacheEpoch(userId, "follow");
}

async function getCacheMeta(userId) {
  const uid = String(userId || "").trim();
  try {
    const [epochs, um] = await Promise.all([
      getUserCacheEpochRow(uid),
      getUserMetricsMeta(uid, { light: true }),
    ]);
    const quote = getGlobalQuoteEpoch();
    return {
      ledgerEpoch: epochs.ledgerEpoch,
      metricsEpoch: Number(um?.dataVersion) || 0,
      dynamicsEpoch: epochs.dynamicsEpoch,
      followEpoch: epochs.followEpoch,
      quoteEpoch: quote.epoch,
      quoteTime: quote.quoteTime,
      rebuilding: !!um?.rebuilding,
      frozenThrough: um?.frozenThrough || null,
    };
  } catch (error) {
    const quote = getGlobalQuoteEpoch();
    return {
      ledgerEpoch: 0,
      metricsEpoch: 0,
      dynamicsEpoch: 0,
      followEpoch: 0,
      quoteEpoch: quote.epoch,
      quoteTime: quote.quoteTime,
      rebuilding: false,
      frozenThrough: null,
      degraded: true,
      error: String(error?.message || error || "cache-meta degraded"),
    };
  }
}

module.exports = {
  touchGlobalQuoteEpoch,
  getGlobalQuoteEpoch,
  bumpLedgerEpoch,
  bumpDynamicsEpoch,
  bumpFollowEpoch,
  getCacheMeta,
};
