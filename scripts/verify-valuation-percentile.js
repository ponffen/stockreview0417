#!/usr/bin/env node
const assert = require("node:assert/strict");
const { formatValuationPercentile } = require("../src/dynamics/valuation-format");

assert.equal(formatValuationPercentile(100, 200, 150), "50%");
assert.equal(formatValuationPercentile(100, 200, 100), "0%");
assert.equal(formatValuationPercentile(100, 200, 200), "100%");
assert.equal(formatValuationPercentile(100, 200, 80), "-20%");
assert.equal(formatValuationPercentile(100, 200, 220), "120%");
assert.equal(formatValuationPercentile(100, 200, 0), "—");
assert.equal(formatValuationPercentile(0, 200, 150), "—");
assert.equal(formatValuationPercentile(100, 100, 150), "—");
assert.equal(formatValuationPercentile(null, 200, 150), "—");

console.log("verify-valuation-percentile: ok");
