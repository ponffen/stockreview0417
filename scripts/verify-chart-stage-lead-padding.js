#!/usr/bin/env node
/**
 * Smoke tests for chart-stage-lead-padding (no DB).
 */
const {
  resolveChartLeadStart,
  leadSessionDates,
  padAnalysisSeriesBundle,
  padBenchmarkLead,
  padStockRecordChartPointsLead,
} = require("../src/metrics/chart-stage-lead-padding");

function assert(cond, msg) {
  if (!cond) {
    throw new Error(msg);
  }
}

// YTD: stage start Jan 1, first data Mar 3 → lead includes Jan-Feb weekdays
const ytdStart = resolveChartLeadStart("ytd", "2026-07-01", "2026-03-03");
assert(ytdStart === "2026-01-01", `ytd start ${ytdStart}`);

const lead = leadSessionDates("2026-01-01", "2026-03-03");
assert(lead.length > 0 && lead[0] === "2026-01-01", "ytd lead dates");
assert(!lead.includes("2026-03-03"), "lead excludes first data day");

// Inception: day before first trade
const incStart = resolveChartLeadStart("inception", "2026-07-01", "2026-03-03");
assert(incStart === "2026-03-02", `inception lead start ${incStart}`);

// Analysis padding
const series = padAnalysisSeriesBundle(
  {
    stageProfit: [{ date: "2026-03-03", profit: "+100.00" }],
    stageRate: [{ date: "2026-03-03", rate: "+1.00%" }],
    totalAssets: [{ date: "2026-03-03", totalAssets: "1,000.00" }],
    marketValue: [{ date: "2026-03-03", marketValue: "800.00" }],
    cash: [{ date: "2026-03-03", cash: "200.00" }],
    cashRatio: [{ date: "2026-03-03", cashRatio: "20.00%" }],
    principal: [{ date: "2026-03-03", principal: "900.00" }],
  },
  "2026-01-01",
  { scope: "all", bookCurrency: "CNY", fxUsdCny: 7.2, fxHkdCny: 0.92 },
);
assert(series.stageProfit[0].date === "2026-01-01", "profit lead date");
assert(series.stageProfit[0].profit === "0.00", `profit zero ${series.stageProfit[0].profit}`);
assert(series.stageRate[0].rate === "0.00%", "rate zero");

// Benchmark padding
const bench = padBenchmarkLead(
  { symbol: "sh000001", stage: "ytd", points: [{ date: "2026-03-03", rate: 0.01, rateDisplay: "+1.00%" }] },
  "2026-01-01",
);
assert(bench.points[0].date === "2026-01-01", "bench lead");

// Stock: close uses lookup, others zero
const stockPts = padStockRecordChartPointsLead(
  [{ date: "2026-03-03", close: "12.000", shares: "100", marketValueNative: "1,200.00", weight: "10.00%", profit: "+50.00" }],
  "2026-01-01",
  new Map([
    ["2026-01-02", 10.5],
    ["2026-03-02", 11.2],
  ]),
);
assert(stockPts[0].date === "2026-01-02", `stock first lead ${stockPts[0].date}`);
assert(stockPts[0].close === "10.500", `stock close ${stockPts[0].close}`);
assert(stockPts[0].shares === "0", "stock shares zero");
assert(stockPts.some((p) => p.date === "2026-03-02"), "stock includes day before first data");

console.log("verify-chart-stage-lead-padding: ok");
