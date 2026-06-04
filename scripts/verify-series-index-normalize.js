#!/usr/bin/env node
/**
 * Smoke checks for src/metrics/series-index-normalize.js (public analysis index).
 */
const {
  normalizeSeriesToIndex,
  normalizeMetricTimeSeries,
  normalizeHeadlineFromSeries,
} = require("../src/metrics/series-index-normalize");

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    // eslint-disable-next-line no-console
    console.error("FAIL:", msg);
    failed += 1;
  }
}

const rising = normalizeMetricTimeSeries(
  [
    { date: "2024-01-02", totalAssets: "¥1,000,000" },
    { date: "2024-06-01", totalAssets: "¥1,200,000" },
  ],
  "totalAssets",
);
assert(rising[0].totalAssets === "1.0000" && rising[1].totalAssets === "1.2000", "monotonic series");

const lateStart = normalizeMetricTimeSeries(
  [
    { date: "2024-01-02", cash: "0" },
    { date: "2024-01-03", cash: "¥50,000" },
    { date: "2024-01-04", cash: "¥75,000" },
  ],
  "cash",
);
assert(lateStart[1].cash === "1.0000" && lateStart[2].cash === "1.5000", "first nonzero base");

const headline = normalizeHeadlineFromSeries(
  [
    { date: "2024-01-02", profit: "¥10,000" },
    { date: "2024-06-01", profit: "¥15,000" },
  ],
  "profit",
);
assert(headline === "1.5000", "headline = last index point");

const core = normalizeSeriesToIndex([null, 0, 100, 150]);
assert(core.baseIndex === 2 && core.values[3] === 1.5, "core math");

if (failed) {
  process.exitCode = 1;
  // eslint-disable-next-line no-console
  console.error(`${failed} assertion(s) failed`);
} else {
  // eslint-disable-next-line no-console
  console.log("verify-series-index-normalize: ok");
}
