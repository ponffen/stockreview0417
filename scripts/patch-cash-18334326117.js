#!/usr/bin/env node
/**
 * 仅更新 18334326117 三个股票账户的银证入金（不删交易）。
 * 需要环境变量 DATABASE_URL（PostgreSQL）。
 * node scripts/patch-cash-18334326117.js
 */
const path = require("node:path");
const { randomUUID } = require("node:crypto");

const {
  findUserByPhone,
  importCashTransfers,
  getCashTransfers,
  closeDatabase,
} = require(path.join(__dirname, "..", "src", "db.js"));

const PHONE = "18334326117";
const AS_OF = "2026-04-22";

function row(accountId, amount, note) {
  return {
    id: randomUUID(),
    accountId,
    date: AS_OF,
    direction: "in",
    amount,
    note: String(note || ""),
    createdAt: Date.now(),
  };
}

async function main() {
  const u = await findUserByPhone(PHONE);
  if (!u) {
    throw new Error(`用户 ${PHONE} 不存在`);
  }
  const uid = u.id;

  await importCashTransfers(
    [
      row("yingtou", 94452, "银证入金 USD"),
      row("longbridge", 35312.49, "银证入金 USD"),
      row("haitong", 443978.43, "银证入金 CNY"),
    ],
    "replace",
    uid
  );

  const list = await getCashTransfers(uid);
  // eslint-disable-next-line no-console
  console.log(
    "OK 入金已更新",
    list.map((r) => `${r.accountId}=${r.amount}`).join(" | "),
  );
  await closeDatabase();
}

main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(e);
  process.exit(1);
});
