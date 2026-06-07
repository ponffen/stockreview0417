/**
 * Public community stock-record bundle: normalize shares / market value / profit to index.
 */

const { finalizeMetricsBundlePayload } = require("./bundle-payload");
const { normalizeMetricTimeSeries } = require("./series-index-normalize");

function redactChartPoints(points) {
  const list = Array.isArray(points) ? points : [];
  if (!list.length) {
    return [];
  }
  const withProfit = list.map((p) => ({ ...p, profit: p.profit ?? p.totalProfit }));
  const shares = normalizeMetricTimeSeries(list, "shares");
  const marketValue = normalizeMetricTimeSeries(list, "marketValueNative");
  const profitSeries = normalizeMetricTimeSeries(withProfit, "profit");
  return list.map((p, i) => ({
    date: p.date,
    close: p.close,
    shares: shares[i]?.shares,
    marketValueNative: marketValue[i]?.marketValueNative,
    weight: p.weight,
    profit: profitSeries[i]?.profit,
  }));
}

function redactPublicStockRecordBundle(bundle) {
  if (!bundle || typeof bundle !== "object") {
    return bundle;
  }
  const charts = bundle.charts || {};
  const series = {
    ...charts,
    points: redactChartPoints(charts.points),
  };
  const meta = bundle.meta ? { ...bundle.meta } : {};
  delete meta.bookCurrency;
  const value = {
    metricsArchitecture: bundle.metricsArchitecture,
    meta,
    headline: bundle.headline,
    charts: series,
  };
  return finalizeMetricsBundlePayload(value);
}

module.exports = {
  redactPublicStockRecordBundle,
};
