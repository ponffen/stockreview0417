#!/usr/bin/env node
/**
 * 子账户 scope 计价：冻结映射 + TWR 不应混用 CNY 列。
 */
const assert = require("node:assert/strict");
const {
  scopedScalarsFromAnalysisRow,
  homeAccountEod,
  homeAccountStageMetrics,
} = require("../src/metrics/home-account-scalars");
const { mapAnalysisRowToHomeAccount } = require("../src/metrics/frozen-pack-v3");
const { accountDailyTwrReturn, positionTodayProfitForScope, resolveAccountTodayProfitCny } = require("../src/metrics/snapshot-plus-live");

const usdRow = {
  account_id: "acc_1778565511579_482",
  book_currency: "USD",
  date: "2026-07-30",
  total_assets: 23776,
  total_assets_cny: 160614,
  market_value: 22000,
  cash: 1776,
  principal: 20000,
  stage_mtd_profit: 486,
  stage_mtd_profit_cny: 2518,
  stage_mtd_rate_twr: 0.0209,
  stage_mtd_rate_twr_cny: 0.15,
};

const scoped = scopedScalarsFromAnalysisRow(usdRow);
assert.equal(scoped.monthProfit, 486, "sub-account mtd profit = book");
assert.equal(scoped.eodTotalAssets, 23776, "sub-account eod TA = book");
assert.equal(scoped.monthProfitCny, 2518, "true CNY column preserved");
assert.equal(scoped.eodTotalAssetsCny, 160614, "true CNY TA preserved");

const homeAcc = mapAnalysisRowToHomeAccount(usdRow, "2026-07-30", "2024-01-01");
assert.equal(homeAcc.month_profit, 486);
assert.equal(homeAcc.eod_total_assets, 23776);
assert.equal(homeAcc.eod_total_assets_cny, 160614);

const eod = homeAccountEod(homeAcc);
assert.equal(eod.totalAssets, 23776, "homeAccountEod uses book for USD sub-account");
assert.equal(eod.cash, 1776);

const stage = homeAccountStageMetrics(homeAcc);
assert.equal(stage.monthProfit, 486);
assert.equal(stage.eodTotalAssets, 23776);

// Bug regression: frozen TA must not be CNY while live TA is USD
const liveTaUsd = 24000;
const twr = accountDailyTwrReturn(eod.totalAssets, liveTaUsd, 0);
assert.ok(twr > -0.1 && twr < 0.2, `mtd daily TWR sane: ${twr}`);
const badTwr = accountDailyTwrReturn(homeAcc.eod_total_assets_cny, liveTaUsd, 0);
assert.ok(badTwr < -0.5, `old bug path still ~-85%: ${badTwr}`);

const usdPositions = [
  { todayProfitNative: -100, todayProfitBook: -100, todayProfitCny: -720 },
  { todayProfitNative: -50, todayProfitBook: -50, todayProfitCny: -360 },
];
const subToday = resolveAccountTodayProfitCny({ tradingDay: true, liveDate: "2026-08-12" }, usdPositions, {}, new Date(), usdRow.account_id);
assert.equal(subToday, -150, "sub-account today = sum book/native, not cny");
const allToday = resolveAccountTodayProfitCny({ tradingDay: true, liveDate: "2026-08-12" }, usdPositions, {}, new Date(), "all");
assert.equal(allToday, -1080, "all-account today = sum cny track");
assert.equal(positionTodayProfitForScope(usdPositions[0], usdRow.account_id), -100);
assert.equal(positionTodayProfitForScope(usdPositions[0], "all"), -720);

async function verifyLiveBundle() {
  if (!process.env.DATABASE_URL) {
    console.log("verify-home-account-scalars: unit ok (skip DB — no DATABASE_URL)");
    return;
  }
  require("dotenv").config();
  const uid = "d175359f-a856-478d-a45d-3112c10227fa";
  const accountId = "acc_1778565511579_482";
  const { getMetricsAnalysisBundle } = require("../src/metrics-api-service");
  const bundle = await getMetricsAnalysisBundle(uid, accountId, "mtd");
  const rateStr = bundle?.returns?.rate;
  const profitStr = bundle?.returns?.profit;
  assert.ok(rateStr, "analysis bundle returns.rate present");
  const rateNum = parseFloat(String(rateStr).replace("%", ""));
  assert.ok(
    rateNum > -20 && rateNum < 50,
    `长桥 mtd rate should not cliff: got ${rateStr} profit=${profitStr}`,
  );
  const { getMetricsHomeBundle } = require("../src/metrics-api-service");
  const home = await getMetricsHomeBundle(uid, accountId, "today");
  const todayHeader = parseFloat(String(home?.returns?.stages?.today?.profit || "0").replace(/,/g, ""));
  const todayRows = (home?.holdings?.rows || []).reduce((s, r) => {
    return s + parseFloat(String(r.todayProfit || "0").replace(/,/g, ""));
  }, 0);
  assert.ok(
    Math.abs(todayHeader - todayRows) < 0.05,
    `today header should match holdings sum: header=${todayHeader} rows=${todayRows}`,
  );
  console.log(`verify-home-account-scalars: DB ok mtd rate=${rateStr} profit=${profitStr} today=${todayHeader}`);
}

verifyLiveBundle()
  .then(() => {
    console.log("verify-home-account-scalars: ok");
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
