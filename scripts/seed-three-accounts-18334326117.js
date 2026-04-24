#!/usr/bin/env node
/**
 * 手机 18334326117：先清空该用户全部数据，再按三账户截图/约定写入账户、买入、银证入金。
 * node scripts/seed-three-accounts-18334326117.js
 */
const path = require("node:path");
const { randomUUID } = require("node:crypto");

const {
  findUserByPhone,
  createRegisteredUser,
  importTrades,
  importCashTransfers,
  replaceAccountsFromList,
  deleteAllDataForUser,
  getTrades,
  getCashTransfers,
  getAccounts,
  closeDatabase,
} = require(path.join(__dirname, "..", "src", "db.js"));

const PHONE = "18334326117";
const AS_OF = "2026-04-22";

const ACC_IB = "yingtou";
const ACC_LB = "longbridge";
const ACC_HT = "haitong";

// —— 盈透：建仓合计 89,168.19 USD；银证入金 94,452 USD
// —— 长桥：两笔港元；银证入金 35,312.49 USD
// —— 海通：A/沪/深人民币 + 泡泡 155.6 HKD×400；银证入金 443,978.43 CNY

const POP_HK_QTY = 400;
const POP_HK_PX = 155.6;
const POP_HK_AMT = POP_HK_QTY * POP_HK_PX; // 62,240 HKD

const FUND_YINGTOU_USD = 94452;
const FUND_LONGBRIDGE_USD = 35312.49;
const FUND_HAITONG_CNY = 443978.43;

const PX = {
  tsm: 36195 / 95,
  goog: 335,
  futu: 18803.19 / 119,
  jt: 8.99,
  popHk: POP_HK_PX,
  nd: 438.9,
  zj: 34.4,
  sp: 2.369,
};

function t(accountId, symbol, name, price, quantity, amount, note) {
  return {
    id: randomUUID(),
    accountId,
    type: "trade",
    symbol,
    name,
    side: "buy",
    price,
    quantity,
    amount,
    date: AS_OF,
    note: String(note || "建仓"),
    createdAt: Date.now(),
  };
}

function c(accountId, amount, note) {
  return {
    id: randomUUID(),
    accountId,
    date: AS_OF,
    direction: "in",
    amount,
    note: String(note || "银证转入"),
  };
}

async function main() {
  let u = await findUserByPhone(PHONE);
  if (!u) {
    await createRegisteredUser(PHONE, "123456");
    u = await findUserByPhone(PHONE);
  }
  const uid = u.id;

  await deleteAllDataForUser(uid);

  const now = Date.now();
  await replaceAccountsFromList(
    [
      { id: "default", name: "默认账户", currency: "CNY", createdAt: 0 },
      { id: ACC_IB, name: "盈透", currency: "USD", createdAt: now - 2 },
      { id: ACC_LB, name: "长桥综合账户(1843)", currency: "USD", createdAt: now - 1 },
      { id: ACC_HT, name: "海通", currency: "CNY", createdAt: now },
    ],
    uid
  );

  const trades = [
    t(ACC_IB, "gb_tsm", "台积电 TSM", PX.tsm, 95, 36195, "盈透"),
    t(ACC_IB, "gb_goog", "Alphabet GOOG", PX.goog, 102, 34170, "盈透"),
    t(ACC_IB, "gb_futu", "富途 FUTU", PX.futu, 119, 18803.19, "盈透 发生额小计对齐 89,168.19"),
    t(ACC_LB, "hk02228", "晶泰控股", PX.jt, 9000, 80910, "长桥 HKD"),
    t(ACC_LB, "hk09992", "泡泡玛特", PX.popHk, 400, 62240, "长桥 HKD"),
    t(ACC_HT, "sz300750", "宁德时代", PX.nd, 100, 43890, "海通"),
    t(ACC_HT, "sh601899", "紫金矿业", PX.zj, 1300, 44720, "海通"),
    t(ACC_HT, "hk09992", "泡泡玛特", PX.popHk, POP_HK_QTY, POP_HK_AMT, "海通 原币港元，同长桥 155.6×400"),
    t(ACC_HT, "sh513500", "标普500ETF", PX.sp, 63000, 149247, "海通"),
  ];

  const cash = [
    c(ACC_IB, FUND_YINGTOU_USD, "银证入金 USD"),
    c(ACC_LB, FUND_LONGBRIDGE_USD, "银证入金 USD"),
    c(ACC_HT, FUND_HAITONG_CNY, "银证入金 CNY"),
  ];

  await importTrades(trades, "replace", uid);
  await importCashTransfers(cash, "replace", uid);

  const trList = await getTrades(uid);
  const sumBuy = (aid) => trList.filter((x) => x.accountId === aid).reduce((s, x) => s + x.amount, 0);
  // eslint-disable-next-line no-console
  console.log("OK", PHONE, uid);
  // eslint-disable-next-line no-console
  console.log("accounts", (await getAccounts(uid)).map((a) => `${a.id}:${a.currency}`).join(" | "));
  // eslint-disable-next-line no-console
  console.log(
    "入金(银证) USD 盈透/长桥 | CNY 海通",
    (await getCashTransfers(uid))
      .map((r) => `${r.accountId}=${r.amount}`)
      .join(" "),
  );
  // eslint-disable-next-line no-console
  console.log("Σ买款 盈透", sumBuy(ACC_IB), "期望 89168.19", Math.abs(sumBuy(ACC_IB) - 89168.19) < 0.01 ? "OK" : "CHECK");
  // eslint-disable-next-line no-console
  console.log("Σ买款(原币) 长桥", sumBuy(ACC_LB), "期望 HKD 143150");
  // eslint-disable-next-line no-console
  console.log(
    "海通 原币加总(不混加)",
    "A+ETF",
    43890 + 44720 + 149247,
    "泡泡HKD",
    POP_HK_AMT,
    "银证CNY",
    FUND_HAITONG_CNY,
  );

  await closeDatabase();
}

main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(e);
  process.exit(1);
});
