#!/usr/bin/env node
const assert = require("node:assert/strict");

function formatLowEstimateChange(lowPrice, current) {
  const low = Number(lowPrice);
  const c = Number(current);
  if (!Number.isFinite(low) || low <= 0 || !Number.isFinite(c) || c <= 0) {
    return "";
  }
  const rate = c / low - 1;
  const safe = Number.isFinite(rate) ? rate : 0;
  const num = (safe * 100).toFixed(2);
  return `${safe > 0 ? "+" : ""}${num}%`;
}

assert.equal(formatLowEstimateChange(100, 120), "+20.00%");
assert.equal(formatLowEstimateChange(100, 80), "-20.00%");
assert.equal(formatLowEstimateChange(100, 100), "0.00%");
assert.equal(formatLowEstimateChange(0, 100), "");

console.log("verify-low-estimate-change: ok");
