#!/usr/bin/env node
const assert = require("node:assert/strict");
const { computeTradeAmountShareRatio } = require("../src/trade-amount-share-ratio");

const FX_USD_TEST = 6.5;
const FX_HKD_TEST = 0.88;

assert.equal(
  computeTradeAmountShareRatio({
    amount: 14600,
    symbol: "sz300750",
    totalAssetsCny: 1_000_000,
    fxUsdCny: FX_USD_TEST,
    fxHkdCny: FX_HKD_TEST,
  }),
  0.0146
);

assert.equal(
  computeTradeAmountShareRatio({
    amount: 1000,
    symbol: "gb_aapl",
    totalAssetsCny: 10_000 * FX_USD_TEST,
    fxUsdCny: FX_USD_TEST,
    fxHkdCny: FX_HKD_TEST,
  }),
  0.1
);

assert.equal(
  computeTradeAmountShareRatio({
    amount: 100,
    symbol: "hk00700",
    totalAssetsCny: 100 * FX_HKD_TEST * 100,
    fxUsdCny: FX_USD_TEST,
    fxHkdCny: FX_HKD_TEST,
  }),
  0.01
);

assert.equal(
  computeTradeAmountShareRatio({
    amount: 100,
    symbol: "sz000001",
    totalAssetsCny: 0,
  }),
  null
);

console.log("verify-trade-amount-share-ratio: ok");
