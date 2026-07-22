/**
 * Chart-only: prepend zero-valued lead points from stage visual start through day before first data.
 * Does not affect KPI headlines, freeze math, or public redaction rules.
 */
const { addCalendarDays, resolveStageRange } = require("./stages");
const { enumerateFreezeSessionDates, previousSessionDate } = require("./freeze-calendar");
const {
  fmtPercentRatio,
  fmtSignedPercentRatio,
  fmtPlainAmount,
  fmtPlainSignedAmount,
} = require("../account-kpi-surface");
const {
  formatSignedProfitForScope,
  formatPlainAssetForScope,
} = require("./account-book-metrics");

function formatClosePrice(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return v.toFixed(3);
}

function formatShares(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return String(Math.round(v));
}

function resolveChartLeadStart(stage, asOf, firstTradeDate, customRange = null) {
  const st = String(stage || "").trim();
  if (!st || st === "today") {
    return null;
  }
  const firstTrade = String(firstTradeDate || "").slice(0, 10);
  const { start } = resolveStageRange(st, asOf, firstTrade, customRange);
  if (st === "inception" && firstTrade) {
    return previousSessionDate(firstTrade) || start;
  }
  return start;
}

function sortedByDate(points) {
  return [...(points || [])].sort((a, b) => String(a.date).localeCompare(String(b.date)));
}

function firstSeriesDate(points) {
  const sorted = sortedByDate(points);
  return sorted.length ? String(sorted[0].date || "").slice(0, 10) : "";
}

function leadSessionDates(chartStart, firstDataDate) {
  const a = String(chartStart || "").slice(0, 10);
  if (!a) {
    return [];
  }
  const b = String(firstDataDate || "").slice(0, 10);
  if (!b) {
    return enumerateFreezeSessionDates(a, a);
  }
  if (b <= a) {
    return [];
  }
  const prev = addCalendarDays(b, -1);
  if (!prev || prev < a) {
    return [];
  }
  return enumerateFreezeSessionDates(a, prev);
}

function shouldSkipLeadPadding(points, chartStart) {
  if (!chartStart) {
    return true;
  }
  const first = firstSeriesDate(points);
  if (!first) {
    return false;
  }
  return first <= String(chartStart).slice(0, 10);
}

function mergePointsByDate(existing, leadPoints) {
  const byDate = new Map();
  for (const p of leadPoints || []) {
    byDate.set(p.date, p);
  }
  for (const p of existing || []) {
    byDate.set(p.date, p);
  }
  return [...byDate.values()].sort((a, b) => String(a.date).localeCompare(String(b.date)));
}

function padPointsLead(points, chartStart, makeZeroPoint) {
  if (!chartStart || typeof makeZeroPoint !== "function") {
    return sortedByDate(points);
  }
  const sorted = sortedByDate(points);
  if (shouldSkipLeadPadding(sorted, chartStart)) {
    return sorted;
  }
  const leadDates = leadSessionDates(chartStart, firstSeriesDate(sorted));
  if (!leadDates.length) {
    return sorted;
  }
  const lead = leadDates.map((date) => makeZeroPoint(date));
  return mergePointsByDate(sorted, lead);
}

function padAnalysisSeriesBundle(series, chartStart, scopeCtx) {
  if (!series || !chartStart) {
    return series;
  }
  const scope = scopeCtx?.scope;
  const book = scopeCtx?.bookCurrency;
  const fxU = scopeCtx?.fxUsdCny;
  const fxH = scopeCtx?.fxHkdCny;
  return {
    stageProfit: padPointsLead(series.stageProfit, chartStart, (date) => ({
      date,
      profit: formatSignedProfitForScope(0, scope, book, fxU, fxH),
    })),
    stageRate: padPointsLead(series.stageRate, chartStart, (date) => ({
      date,
      rate: fmtSignedPercentRatio(0),
    })),
    totalAssets: padPointsLead(series.totalAssets, chartStart, (date) => ({
      date,
      totalAssets: formatPlainAssetForScope(0, scope, book, fxU, fxH),
    })),
    marketValue: padPointsLead(series.marketValue, chartStart, (date) => ({
      date,
      marketValue: formatPlainAssetForScope(0, scope, book, fxU, fxH),
    })),
    cash: padPointsLead(series.cash, chartStart, (date) => ({
      date,
      cash: formatPlainAssetForScope(0, scope, book, fxU, fxH),
    })),
    cashRatio: padPointsLead(series.cashRatio, chartStart, (date) => ({
      date,
      cashRatio: fmtPercentRatio(0),
    })),
    principal: padPointsLead(series.principal, chartStart, (date) => ({
      date,
      principal: formatPlainAssetForScope(0, scope, book, fxU, fxH),
    })),
  };
}

function padBenchmarkLead(benchmark, chartStart) {
  if (!benchmark || !chartStart) {
    return benchmark;
  }
  const points = padPointsLead(benchmark.points, chartStart, (date) => ({
    date,
    rate: 0,
    rateDisplay: fmtSignedPercentRatio(0),
  }));
  return { ...benchmark, points };
}

function padStockRecordChartPointsLead(points, chartStart, closeLookup) {
  if (!chartStart) {
    return sortedByDate(points);
  }
  const sorted = sortedByDate(points);
  if (shouldSkipLeadPadding(sorted, chartStart)) {
    return sorted;
  }
  const leadDates = leadSessionDates(chartStart, firstSeriesDate(sorted));
  const lead = [];
  for (const dk of leadDates) {
    const close = closeLookup?.closeOn?.(dk);
    if (!(close > 0)) {
      continue;
    }
    lead.push({
      date: dk,
      close: formatClosePrice(close),
      shares: formatShares(0),
      marketValueNative: fmtPlainAmount(0),
      weight: fmtPercentRatio(0),
      profit: fmtPlainSignedAmount(0),
    });
  }
  if (!lead.length) {
    return sorted;
  }
  return mergePointsByDate(sorted, lead);
}

module.exports = {
  resolveChartLeadStart,
  leadSessionDates,
  padAnalysisSeriesBundle,
  padBenchmarkLead,
  padStockRecordChartPointsLead,
};
