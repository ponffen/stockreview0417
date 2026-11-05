#!/usr/bin/env node
const assert = require("node:assert/strict");
const { formatPositionPercentile } = require("../src/dynamics/position-band-format");

assert.equal(formatPositionPercentile(0.05, 0.12, 0.085), "50%");
assert.equal(formatPositionPercentile(0.05, 0.12, 0.05), "0%");
assert.equal(formatPositionPercentile(0.05, 0.12, 0.12), "100%");
assert.equal(formatPositionPercentile(0.05, 0.12, 0.03), "-29%");
assert.equal(formatPositionPercentile(0.05, 0.12, 0.15), "143%");
assert.equal(formatPositionPercentile(0.05, 0.12, NaN), "—");
assert.equal(formatPositionPercentile(0.05, 0.05, 0.1), "—");
assert.equal(formatPositionPercentile(null, 0.12, 0.08), "—");

console.log("verify-position-percentile: ok");
