#!/usr/bin/env node
/**
 * 将「源手机号」在库里的 per-user 数据划给「目标手机号」对应用户。
 * - 对源有数据的表：先删目标同表行，再把源行 user_id 改为目标（避免主键冲突）。
 * - 若源有成交：会清空目标在 symbol_daily_pnl / analysis_daily_snapshot / daily_returns
 *   下的行（与旧成交不一致，需按目标号重跑 backfill）。
 *
 * 用法:
 *   node scripts/migrate-user-data-between-phones.js <源手机> <目标手机>
 *   SOURCE_PHONE=13216933724 DEST_PHONE=18310270720 node scripts/migrate-user-data-between-phones.js
 *
 * 环境: DB_PATH 可选，默认 ../data/app.db
 */
const path = require("node:path");
const Database = require("better-sqlite3");

const DB_PATH = process.env.DB_PATH || path.join(__dirname, "..", "data", "app.db");

const TABLES_REWRITABLE = [
  "trades",
  "accounts",
  "daily_returns",
  "symbol_daily_pnl",
  "analysis_daily_snapshot",
  "app_settings",
];

function main() {
  const srcPhone = String(process.argv[2] || process.env.SOURCE_PHONE || "").trim();
  const destPhone = String(process.argv[3] || process.env.DEST_PHONE || "").trim();
  if (!srcPhone || !destPhone) {
    console.error(
      "用法: node scripts/migrate-user-data-between-phones.js <源手机> <目标手机>\n" +
        "示例: node scripts/migrate-user-data-between-phones.js 13216933724 18310270720"
    );
    process.exit(1);
  }
  if (srcPhone === destPhone) {
    console.error("源与目标相同，无需迁移");
    process.exit(0);
  }

  const db = new Database(DB_PATH);
  const findUser = db.prepare("SELECT id, phone FROM users WHERE phone = ?");

  const src = findUser.get(srcPhone);
  const dest = findUser.get(destPhone);
  if (!src) {
    console.error(`未找到用户: ${srcPhone}`);
    process.exit(1);
  }
  if (!dest) {
    console.error(`未找到用户: ${destPhone}（请先在应用里注册该号）`);
    process.exit(1);
  }

  const srcId = src.id;
  const destId = dest.id;

  console.log(`源: ${srcPhone}  user_id=${srcId}`);
  console.log(`目标: ${destPhone}  user_id=${destId}`);

  const counts = (uid) => {
    const out = {};
    for (const t of TABLES_REWRITABLE) {
      const row = db.prepare(`SELECT COUNT(*) AS c FROM ${t} WHERE user_id = ?`).get(uid);
      out[t] = Number(row?.c || 0);
    }
    return out;
  };

  console.log("迁移前 源行数:", counts(srcId));
  console.log("迁移前 目标行数:", counts(destId));

  const DERIVED_RECOMPUTE = ["symbol_daily_pnl", "analysis_daily_snapshot", "daily_returns"];

  const tx = db.transaction(() => {
    const srcTradeN = Number(
      db.prepare(`SELECT COUNT(*) AS c FROM trades WHERE user_id = ?`).get(srcId).c || 0
    );
    if (srcTradeN > 0) {
      for (const t of DERIVED_RECOMPUTE) {
        const del = db.prepare(`DELETE FROM ${t} WHERE user_id = ?`).run(destId);
        if (del.changes) {
          console.log(
            `  源侧有成交：已清空目标在 ${t} 的 ${del.changes} 行（与迁入成交对齐，请对目标号执行 backfill）`
          );
        }
      }
    }
    for (const t of TABLES_REWRITABLE) {
      const { c: srcRows } = db.prepare(`SELECT COUNT(*) AS c FROM ${t} WHERE user_id = ?`).get(srcId);
      if (Number(srcRows) === 0) {
        continue;
      }
      const del = db.prepare(`DELETE FROM ${t} WHERE user_id = ?`).run(destId);
      if (del.changes) {
        console.log(`  已删除目标在 ${t} 的 ${del.changes} 行（为迁入源数据让路）`);
      }
      const info = db.prepare(`UPDATE ${t} SET user_id = ? WHERE user_id = ?`).run(destId, srcId);
      if (info.changes) {
        console.log(`  已将 ${t} ${info.changes} 行 user_id ${srcId.slice(0, 8)}… → ${destId.slice(0, 8)}…`);
      }
    }
  });

  tx();

  const srcAfter = db.prepare("SELECT COUNT(*) AS c FROM trades WHERE user_id = ?").get(srcId);
  if (Number(srcAfter?.c) > 0) {
    console.warn("警告: 源 user_id 仍有残留行，请检查表结构或手工处理");
  }

  console.log("迁移后 源行数:", counts(srcId));
  console.log("迁移后 目标行数:", counts(destId));
  console.log(
    `完成。源账号登录后库内应无上述业务数据。请清除浏览器里源号的 Local Storage（earning-clone-state-v2::${srcPhone}）以免旧缓存干扰。`
  );
  console.log(
    `若目标号日级快照/日收益为空，请在项目根目录执行（需网络拉汇率/K 线）:\n  STOCKREVIEW_PHONE=${destPhone} node scripts/backfill-daily-tables.js`
  );

  db.close();
}

main();
