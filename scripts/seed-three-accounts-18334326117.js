#!/usr/bin/env node
/**
 * 为手机 18334326117 创建三股票账户、持仓（买入）与银证转入，与约定总资产/现金/市值一致。
 * 运行：node scripts/seed-three-accounts-18334326117.js
 */
const path = require("node:path");
const { randomUUID } = require("node:crypto");

const {
  findUserByPhone,
  createRegisteredUser,
  importTrades,
  importCashTransfers,
  replaceAccountsFromList,
  closeDatabase,
} = require(path.join(__dirname, "..", "src", "db.js"));

const PHONE = "18334326117";
const AS_OF = "2026-04-22";

const ACC_IB = "yingtou";
const ACC_LB = "longbridge";
const ACC_HT = "haitong";

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
    note: String(note || "期初建仓（导入）"),
    createdAt: Date.now(),
  };
}

function c(accountId, amountUsdOrCny, note) {
  return {
    id: randomUUID(),
    accountId,
    date: AS_OF,
    direction: "in",
    amount: amountUsdOrCny,
    note: String(note || "银证转入（与券商总资产对齐）"),
  };
}

function main() {
  let u = findUserByPhone(PHONE);
  if (!u) {
    createRegisteredUser(PHONE, "123456");
    u = findUserByPhone(PHONE);
  }
  const uid = u.id;

  // 1) 清空该用户成交与资金记录，再写账户
  importTrades([], "replace", uid);
  importCashTransfers([], "replace", uid);

  const now = Date.now();
  replaceAccountsFromList(
    [
      { id: "default", name: "默认账户", currency: "CNY", createdAt: 0 },
      { id: ACC_IB, name: "盈透", currency: "USD", createdAt: now - 2 },
      { id: ACC_LB, name: "长桥综合账户(1843)", currency: "USD", createdAt: now - 1 },
      { id: ACC_HT, name: "海通", currency: "CNY", createdAt: now },
    ],
    uid
  );

  // 2) 交易（发生额=市值口径；长桥两只为港股，金额/单价为港元）
  const trades = [
    // 盈透：总市值 89,168.19；现金=总资产-市值=5,283.81
    t(ACC_IB, "gb_tsm", "台积电 TSM", 36195 / 95, 95, 36195, "盈透"),
    t(ACC_IB, "gb_goog", "Alphabet GOOG", 34170 / 102, 102, 34170, "盈透"),
    t(ACC_IB, "gb_futu", "富途 FUTU", 18803 / 119, 119, 18803, "盈透"),

    // 长桥：港股市值合计 HKD 143,150；USD 持仓 18,274.52
    t(ACC_LB, "hk02228", "晶泰控股", 80910 / 9000, 9000, 80910, "长桥 港元"),
    t(ACC_LB, "hk09992", "泡泡玛特", 62240 / 400, 400, 62240, "长桥 港元"),

    // 海通：市值 292,038.16；现金 151,940.27；总资产 443,978.43
    t(ACC_HT, "sz300750", "宁德时代", 438.9, 100, 43890, "海通"),
    t(ACC_HT, "sh601899", "紫金矿业", 34.4, 1300, 44720, "海通"),
    // 港股在应用内以 HKD 计发生额；使折人民币约 54,181（σ 与 292,038.16 对齐；汇率按约 0.92）
    t(ACC_HT, "hk09992", "泡泡玛特", 58892 / 400, 400, 58892, "海通 港股（港港元）"),
    t(ACC_HT, "sh513500", "标普500ETF", 2.369, 63000, 149247, "海通 标普500场内"),
  ];

  // 3) 银证：转入 = 该账户下 Σ买款 + 目标现金（同币种账户侧）
  const cash = [
    c(ACC_IB, 94452, "与总资产 94,452 USD 对齐（含市值+现金）"),
    c(ACC_LB, 35312.49, "与总资产 35,312.49 USD 对齐"),
    c(ACC_HT, 443978.43, "与账户资产 443,978.43 对齐"),
  ];

  importTrades(trades, "replace", uid);
  importCashTransfers(cash, "replace", uid);

  // eslint-disable-next-line no-console
  console.log(`OK: user ${PHONE} (${uid}): 3 操作账户 + 默认账户, ${trades.length} 笔买入, ${cash.length} 笔银证转入。默认登录密码如为新注册: 123456`);
  closeDatabase();
}

main();
