#!/usr/bin/env node
/**
 * 按手机号白名单删除用户及全部 per-user 数据。
 * 用法:
 *   node scripts/delete-users-by-phone.js --dry-run
 *   node scripts/delete-users-by-phone.js --execute
 */
require("dotenv").config();
const { initPool, closeDatabase } = require("../src/db");

const PHONE_WHITELIST = ["13216933724", "13800138099", "13800138001", "19900001111"];

const USER_TABLES = [
  "trades",
  "cash_transfers",
  "symbol_daily_pnl",
  "analysis_daily_snapshot",
  "account_home_summary",
  "symbol_home_summary",
  "daily_returns",
  "app_settings",
  "accounts",
  "user_metrics_meta",
  "account_metrics_meta",
];

async function countRows(pool, table, uid) {
  const safe = String(table).replace(/[^a-z0-9_]/gi, "");
  if (safe !== table) return null;
  const r = await pool.query(`SELECT COUNT(*)::bigint AS c FROM ${safe} WHERE user_id = $1`, [uid]);
  return Number(r.rows[0].c) || 0;
}

async function countFollows(pool, uid) {
  const r = await pool.query(
    `SELECT COUNT(*)::bigint AS c FROM community_follows WHERE follower_id = $1 OR followee_id = $1`,
    [uid],
  );
  return Number(r.rows[0].c) || 0;
}

async function deleteUserAllData(client, uid, phone) {
  const tables = [
    "trades",
    "cash_transfers",
    "symbol_daily_pnl",
    "analysis_daily_snapshot",
    "account_home_summary",
    "symbol_home_summary",
    "daily_returns",
    "app_settings",
    "accounts",
    "user_metrics_meta",
    "account_metrics_meta",
  ];
  for (const table of tables) {
    await client.query(`DELETE FROM ${table} WHERE user_id = $1`, [uid]);
  }
  await client.query("DELETE FROM community_follows WHERE follower_id = $1 OR followee_id = $1", [uid]);
  const delUser = await client.query("DELETE FROM users WHERE id = $1 AND phone = $2", [uid, phone]);
  if (delUser.rowCount !== 1) {
    throw new Error(`users delete expected 1 row, got ${delUser.rowCount}`);
  }
}

async function main() {
  const execute = process.argv.includes("--execute");
  const dryRun = process.argv.includes("--dry-run") || !execute;
  if (!process.env.DATABASE_URL) {
    console.error("DATABASE_URL is required");
    process.exit(1);
  }

  const pool = await initPool();
  const { rows: users } = await pool.query(
    `SELECT id, phone, nickname FROM users WHERE phone = ANY($1::text[]) ORDER BY phone`,
    [PHONE_WHITELIST],
  );

  if (users.length !== PHONE_WHITELIST.length) {
    console.error("[abort] expected", PHONE_WHITELIST.length, "users, got", users.length);
    console.error(
      "found:",
      users.map((u) => ({ id: u.id, phone: u.phone, nickname: u.nickname })),
    );
    const foundPhones = new Set(users.map((u) => u.phone));
    const missing = PHONE_WHITELIST.filter((p) => !foundPhones.has(p));
    if (missing.length) console.error("missing phones:", missing);
    process.exit(1);
  }

  const otherUsers = await pool.query(
    `SELECT COUNT(*)::bigint AS c FROM users WHERE phone <> ALL($1::text[])`,
    [PHONE_WHITELIST],
  );
  const keepCount = Number(otherUsers.rows[0].c) || 0;

  console.log("[mode]", dryRun ? "dry-run" : "execute");
  console.log("[keep] other users in DB:", keepCount);

  for (const u of users) {
    const counts = {};
    let total = 0;
    for (const t of USER_TABLES) {
      const n = await countRows(pool, t, u.id);
      counts[t] = n;
      total += n;
    }
    counts.community_follows = await countFollows(pool, u.id);
    total += counts.community_follows;
    counts.users_row = 1;
    total += 1;
    console.log("[user]", { phone: u.phone, id: u.id, nickname: u.nickname, totalRows: total, counts });

    if (!dryRun) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await deleteUserAllData(client, u.id, u.phone);
        await client.query("COMMIT");
        console.log("[deleted]", u.phone, u.id);
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      } finally {
        client.release();
      }
    }
  }

  if (!dryRun) {
    const verify = await pool.query(`SELECT phone FROM users WHERE phone = ANY($1::text[])`, [PHONE_WHITELIST]);
    if (verify.rows.length) {
      console.error("[verify-fail] users still present:", verify.rows);
      process.exit(1);
    }
    const keepAfter = await pool.query(`SELECT COUNT(*)::bigint AS c FROM users`);
    console.log("[verify] deleted phones gone; users remaining:", keepAfter.rows[0].c);
  }

  await closeDatabase?.();
  console.log(dryRun ? "[dry-run] done — pass --execute to delete" : "[execute] done");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
