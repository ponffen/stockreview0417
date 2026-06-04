#!/usr/bin/env node
const assert = require("node:assert/strict");
const { computeTradeAmountShareRatio } = require("../src/trade-amount-share-ratio");

assert.equal(
  computeTradeAmountShareRatio({
    amount: 14600,
    symbol: "sz300750",
    totalAssetsCny: 1_000_000,
    fxUsdCny: 7.2,
    fxHkdCny: 0.92,
  }),
  0.0146
);

assert.equal(
  computeTradeAmountShareRatio({
    amount: 1000,
    symbol: "gb_aapl",
    totalAssetsCny: 72000,
    fxUsdCny: 7.2,
    fxHkdCny: 0.92,
  }),
  0.1
);

assert.equal(
  computeTradeAmountShareRatio({
    amount: 100,
    symbol: "hk00700",
    totalAssetsCny: 9200,
    fxUsdCny: 7.2,
    fxHkdCny: 0.92,
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
