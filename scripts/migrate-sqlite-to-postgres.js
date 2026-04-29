#!/usr/bin/env node
/**
 * 将旧版 SQLite（data/app.db）中的业务表整库 upsert 到当前 DATABASE_URL 指向的 Postgres。
 *
 * 依赖: better-sqlite3（devDependency）、pg、.env 中的 DATABASE_URL
 *
 * 用法:
 *   npm run migrate:sqlite-to-pg
 *   SQLITE_PATH=/path/to/app.db npm run migrate:sqlite-to-pg
 *   npm run migrate:sqlite-to-pg -- --wipe-pg   # 先清空 Postgres 再写入（慎用）
 */
require("dotenv").config();
const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("pg");

const root = path.join(__dirname, "..");
const SQLITE_DEFAULT = path.join(root, "data", "app.db");

function getPgUrl() {
  return (
    process.env.DATABASE_URL ||
    process.env.POSTGRES_URL ||
    process.env.POSTGRES_PRISMA_URL ||
    ""
  );
}

function getSslOption(url) {
  if (process.env.DATABASE_SSL === "0") {
    return false;
  }
  if (/localhost|127\.0\.0\.1/.test(String(url || ""))) {
    return false;
  }
  return { rejectUnauthorized: false };
}

function tableExistsSqlite(db, name) {
  const r = db
    .prepare("SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = ?")
    .get(name);
  return !!r;
}

function sqliteColumnSet(db, table) {
  if (!tableExistsSqlite(db, table)) {
    return null;
  }
  const rows = db.prepare(`PRAGMA table_info(${table})`).all();
  return new Set(rows.map((r) => r.name));
}

function pick(row, cols, defaults = {}) {
  const out = [];
  for (const c of cols) {
    if (Object.prototype.hasOwnProperty.call(row, c) && row[c] !== undefined) {
      out.push(row[c]);
    } else if (Object.prototype.hasOwnProperty.call(defaults, c)) {
      out.push(defaults[c]);
    } else {
      out.push(null);
    }
  }
  return out;
}

async function runDdl(client, ddlList) {
  for (const sql of ddlList) {
    await client.query(sql);
  }
}

async function truncatePostgresAppTables(client) {
  await client.query(`
    TRUNCATE TABLE
      community_follows,
      cash_transfers,
      trades,
      app_settings,
      accounts,
      daily_returns,
      symbol_daily_pnl,
      analysis_daily_snapshot,
      symbol_daily_close,
      community_leaderboard_cache,
      users
    RESTART IDENTITY
  `);
}

async function main() {
  const argv = process.argv.slice(2);
  const shouldWipePg = argv.includes("--wipe-pg");

  const sqlitePath = String(process.env.SQLITE_PATH || SQLITE_DEFAULT).trim();
  if (!fs.existsSync(sqlitePath)) {
    console.error(`找不到 SQLite 文件: ${sqlitePath}`);
    console.error("可设置 SQLITE_PATH 指向你的 app.db。");
    process.exit(1);
  }

  const dbUrl = getPgUrl();
  if (!dbUrl) {
    console.error("请设置 DATABASE_URL（或 POSTGRES_URL）指向 Postgres。");
    process.exit(1);
  }

  let Database;
  try {
    // eslint-disable-next-line global-require, import/no-extraneous-dependencies
    Database = require("better-sqlite3");
  } catch (e) {
    console.error("缺少依赖 better-sqlite3。请在项目根目录执行: npm install");
    console.error(e?.message || e);
    process.exit(1);
  }

  const { schemaDdl } = require("../src/db");
  const sqlite = new Database(sqlitePath, { readonly: true, fileMustExist: true });

  const client = new Client({
    connectionString: dbUrl,
    ssl: getSslOption(dbUrl),
  });
  await client.connect();

  try {
    await client.query("BEGIN");
    await runDdl(client, schemaDdl);
    if (shouldWipePg) {
      await truncatePostgresAppTables(client);
      console.log("已按 --wipe-pg 清空 Postgres 业务表。");
    }

    const specs = [
      {
        table: "users",
        columns: [
          "id",
          "phone",
          "password_hash",
          "created_at",
          "updated_at",
          "nickname",
          "community_public",
        ],
        defaults: { nickname: null, community_public: 1 },
        conflict: "(id) DO UPDATE SET phone = EXCLUDED.phone, password_hash = EXCLUDED.password_hash, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, nickname = EXCLUDED.nickname, community_public = EXCLUDED.community_public",
      },
      {
        table: "accounts",
        columns: ["user_id", "id", "name", "currency", "created_at", "updated_at"],
        defaults: { currency: "CNY" },
        conflict:
          "(user_id, id) DO UPDATE SET name = EXCLUDED.name, currency = EXCLUDED.currency, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at",
      },
      {
        table: "trades",
        columns: [
          "id",
          "user_id",
          "account_id",
          "type",
          "symbol",
          "name",
          "side",
          "price",
          "quantity",
          "amount",
          "trade_date",
          "note",
          "created_at",
          "updated_at",
        ],
        defaults: { account_id: "default", note: "" },
        conflict:
          "(id) DO UPDATE SET user_id = EXCLUDED.user_id, account_id = EXCLUDED.account_id, type = EXCLUDED.type, symbol = EXCLUDED.symbol, name = EXCLUDED.name, side = EXCLUDED.side, price = EXCLUDED.price, quantity = EXCLUDED.quantity, amount = EXCLUDED.amount, trade_date = EXCLUDED.trade_date, note = EXCLUDED.note, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at",
      },
      {
        table: "app_settings",
        columns: ["user_id", "key", "value", "updated_at"],
        conflict: "(user_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
      },
      {
        table: "daily_returns",
        columns: [
          "user_id",
          "account_id",
          "date",
          "profit",
          "return_rate",
          "total_asset",
          "created_at",
          "updated_at",
        ],
        defaults: { profit: 0, return_rate: 0 },
        conflict:
          "(user_id, account_id, date) DO UPDATE SET profit = EXCLUDED.profit, return_rate = EXCLUDED.return_rate, total_asset = EXCLUDED.total_asset, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at",
      },
      {
        table: "symbol_daily_pnl",
        columns: [
          "user_id",
          "account_id",
          "symbol",
          "date",
          "eod_shares",
          "day_trade_qty",
          "day_trade_amount",
          "day_close_price",
          "day_pnl_native",
          "currency",
          "created_at",
          "updated_at",
        ],
        defaults: {
          eod_shares: 0,
          day_trade_qty: 0,
          day_trade_amount: 0,
          day_pnl_native: 0,
          currency: "CNY",
        },
        conflict:
          "(user_id, account_id, symbol, date) DO UPDATE SET eod_shares = EXCLUDED.eod_shares, day_trade_qty = EXCLUDED.day_trade_qty, day_trade_amount = EXCLUDED.day_trade_amount, day_close_price = EXCLUDED.day_close_price, day_pnl_native = EXCLUDED.day_pnl_native, currency = EXCLUDED.currency, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at",
      },
      {
        table: "analysis_daily_snapshot",
        columns: [
          "user_id",
          "account_id",
          "date",
          "profit_cny",
          "rate_cost",
          "rate_twr",
          "rate_dietz",
          "total_profit",
          "total_rate_cost",
          "total_rate_twr",
          "total_rate_dietz",
          "principal",
          "market_value",
          "fx_hkd_cny",
          "fx_usd_cny",
          "created_at",
          "updated_at",
        ],
        defaults: {
          profit_cny: 0,
          rate_cost: 0,
          rate_twr: 0,
          rate_dietz: 0,
          total_profit: 0,
          total_rate_cost: 0,
          total_rate_twr: 0,
          total_rate_dietz: 0,
          principal: 0,
          market_value: 0,
        },
        conflict:
          "(user_id, account_id, date) DO UPDATE SET profit_cny = EXCLUDED.profit_cny, rate_cost = EXCLUDED.rate_cost, rate_twr = EXCLUDED.rate_twr, rate_dietz = EXCLUDED.rate_dietz, total_profit = EXCLUDED.total_profit, total_rate_cost = EXCLUDED.total_rate_cost, total_rate_twr = EXCLUDED.total_rate_twr, total_rate_dietz = EXCLUDED.total_rate_dietz, principal = EXCLUDED.principal, market_value = EXCLUDED.market_value, fx_hkd_cny = EXCLUDED.fx_hkd_cny, fx_usd_cny = EXCLUDED.fx_usd_cny, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at",
      },
      {
        table: "symbol_daily_close",
        columns: ["symbol", "date", "close", "source", "updated_at"],
        conflict:
          "(symbol, date) DO UPDATE SET close = EXCLUDED.close, source = EXCLUDED.source, updated_at = EXCLUDED.updated_at",
      },
      {
        table: "community_follows",
        columns: ["follower_id", "followee_id", "created_at"],
        conflict: "(follower_id, followee_id) DO UPDATE SET created_at = EXCLUDED.created_at",
      },
      {
        table: "community_leaderboard_cache",
        columns: ["id", "payload", "updated_at"],
        conflict: "(id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at",
      },
      {
        table: "cash_transfers",
        columns: [
          "id",
          "user_id",
          "account_id",
          "transfer_date",
          "direction",
          "amount",
          "note",
          "created_at",
          "updated_at",
        ],
        defaults: { note: "" },
        conflict:
          "(id) DO UPDATE SET user_id = EXCLUDED.user_id, account_id = EXCLUDED.account_id, transfer_date = EXCLUDED.transfer_date, direction = EXCLUDED.direction, amount = EXCLUDED.amount, note = EXCLUDED.note, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at",
      },
    ];

    let total = 0;
    for (const spec of specs) {
      const colset = sqliteColumnSet(sqlite, spec.table);
      if (!colset) {
        console.log(`[skip] SQLite 无表 ${spec.table}`);
        continue;
      }
      const activeCols = spec.columns.filter((c) => colset.has(c));
      if (!activeCols.length) {
        console.log(`[skip] ${spec.table} 无可用列`);
        continue;
      }
      const placeholders = activeCols.map((_, i) => `$${i + 1}`).join(", ");
      const sql = `INSERT INTO ${spec.table} (${activeCols.join(", ")}) VALUES (${placeholders}) ON CONFLICT ${spec.conflict}`;
      const stmt = sqlite.prepare(`SELECT ${activeCols.map((c) => `"${c}"`).join(", ")} FROM ${spec.table}`);
      const rows = stmt.all();
      for (const row of rows) {
        const merged = { ...row };
        for (const [k, v] of Object.entries(spec.defaults || {})) {
          if (activeCols.includes(k) && (merged[k] === undefined || merged[k] === null)) {
            merged[k] = v;
          }
        }
        const params = pick(merged, activeCols, spec.defaults);
        await client.query(sql, params);
      }
      console.log(`[ok] ${spec.table}: ${rows.length} 行`);
      total += rows.length;
    }

    await client.query("COMMIT");
    console.log(`\n完成。共写入/更新 ${total} 行（含各表重复 upsert）。SQLite: ${sqlitePath}`);
    if (!shouldWipePg) {
      console.log(
        "若出现手机号唯一冲突，可对空库重跑并加 --wipe-pg，或先手工删掉 Postgres 里冲突用户后再迁移。"
      );
    }
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    sqlite.close();
    await client.end();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
