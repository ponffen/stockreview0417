#!/usr/bin/env node
const assert = require("node:assert/strict");
const { HOLDING_ROW_PUBLIC_KEYS } = require("../src/metrics/public-home-bundle-redact");
const { POSITION_BAND_HOLDING_KEYS, formatHoldingsForMcp } = require("../src/mcp/holdings-service");

for (const key of POSITION_BAND_HOLDING_KEYS) {
  assert.ok(HOLDING_ROW_PUBLIC_KEYS.has(key), `public holdings whitelist missing ${key}`);
}

const formatted = formatHoldingsForMcp(
  {
    rows: [
      {
        symbol: "SH600519",
        positionMin: "5.00%",
        positionMinChange: "+1.00%",
        positionMax: "12.00%",
        positionMaxChange: "-2.00%",
        positionPercentile: "40%",
      },
    ],
  },
  "private",
);
assert.equal(formatted.rows[0].positionPercentile, "40%");
assert.ok(formatted.positionBandHints.positionMin);

console.log("verify-mcp-holdings-position-band: ok");
