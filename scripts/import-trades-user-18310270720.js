#!/usr/bin/env node
/**
 * 将 scripts/import-trades-18310270720.json 导入手机 18310270720 对应用户（全部写入默认账户）。
 *
 * 使用 importTrades(..., "append")：只新增/更新本 JSON 中的记录（按 id upsert），
 * 不会清空或删除该用户原有的其它交易。不要用 "replace" 模式。
 *
 * 用法（需 DATABASE_URL 或 POSTGRES_URL）：
 *   node scripts/import-trades-user-18310270720.js
 */
const fs = require("node:fs");
const path = require("node:path");

const {
  findUserByPhone,
  createRegisteredUser,
  importTrades,
  getTrades,
  closeDatabase,
  normalizeTrade,
} = require(path.join(__dirname, "..", "src", "db.js"));

const PHONE = "18310270720";
const TRADES_JSON = path.join(__dirname, "import-trades-18310270720.json");

async function main() {
  const url =
    process.env.DATABASE_URL ||
    process.env.POSTGRES_URL ||
    process.env.POSTGRES_PRISMA_URL ||
    "";
  if (!url.trim()) {
    throw new Error("缺少 DATABASE_URL（或 POSTGRES_URL），无法连接数据库。");
  }

  let user = await findUserByPhone(PHONE);
  if (!user) {
    await createRegisteredUser(PHONE, "123456");
    user = await findUserByPhone(PHONE);
  }
  const uid = user.id;

  const raw = JSON.parse(fs.readFileSync(TRADES_JSON, "utf8"));
  const trades = raw.map((item) => normalizeTrade(item));

  /** append：保留已有交易，仅插入或按 id 覆盖 JSON 内这 21 条 */
  await importTrades(trades, "append", uid);

  const list = await getTrades(uid);
  // eslint-disable-next-line no-console
  console.log(
    `OK 已导入 ${trades.length} 条交易到 ${PHONE}（user ${uid}，默认账户）。该用户当前共 ${list.length} 条交易记录。`,
  );

  await closeDatabase();
}

main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(e);
  process.exit(1);
});
