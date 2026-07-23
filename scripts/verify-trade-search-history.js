#!/usr/bin/env node
const assert = require("node:assert/strict");
const { mergeTradeSearchHistoryItems } = require("../src/trade-search-history");

const tradeLatestTs = new Map([
  ["sz600519", 1000],
  ["hk00700", 500],
]);
const postSymbolTs = new Map([
  ["hk00700", 2000],
  ["gb_aapl", 1500],
]);

const items = mergeTradeSearchHistoryItems({
  holdingSymbols: ["sz600519", "hk00700"],
  postSymbolTs,
  tradeLatestTs,
});

assert.equal(items.length, 3);
assert.equal(items[0].symbol, "hk00700");
assert.equal(items[0].holding, true);
assert.deepEqual(items[0].sources, ["holding", "post"]);
assert.equal(items[0].lastSeenAt, 2000);
assert.equal(items[1].symbol, "sz600519");
assert.equal(items[1].holding, true);
assert.equal(items[2].symbol, "gb_aapl");
assert.equal(items[2].holding, false);

console.log("verify-trade-search-history: ok");
