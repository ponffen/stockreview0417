/**
 * Public community analysis tab: same analysis-bundle shape as private,
 * strip sensitive fields and normalize amount fields to index (base day = 1.0000).
 */

const { finalizeMetricsBundlePayload } = require("./bundle-payload");
const {
  normalizeMetricTimeSeries,
  normalizeHeadlineFromSeries,
  parsePlainAmountText,
  formatIndexValue,
} = require("./series-index-normalize");

const STOCK_RANK_PUBLIC_KEYS = new Set([
  "rank",
  "symbol",
  "name",
  "pxChange",
  "heldDays",
  "profitShare",
  "holdIntervalsLabel",
]);

function redactAssets(assets, series) {
  const src = assets || {};
  const out = {
    cashRatio: src.cashRatio,
    stockRatio: src.stockRatio,
  };
  if (src.totalAssets != null) {
    out.totalAssets = normalizeHeadlineFromSeries(series?.totalAssets, "totalAssets");
  }
  if (src.marketValue != null) {
    out.marketValue = normalizeHeadlineFromSeries(series?.marketValue, "marketValue");
  }
  if (src.cash != null) {
    out.cash = normalizeHeadlineFromSeries(series?.cash, "cash");
  }
  if (src.principal != null) {
    out.principal = normalizeHeadlineFromSeries(series?.principal, "principal");
  }
  return out;
}

function redactReturns(returns, stageProfitSeries) {
  const row = returns || {};
  const out = { rate: row.rate };
  if (row.profit != null) {
    out.profit = normalizeHeadlineFromSeries(stageProfitSeries, "profit");
  }
  return out;
}

function redactSeries(series) {
  const s = series || {};
  const stageProfitSrc = s.stageProfit || s.dailyProfit || [];
  const stageRateSrc = s.stageRate || s.dailyTwr || [];
  const totalAssets = normalizeMetricTimeSeries(s.totalAssets, "totalAssets");
  const marketValue = normalizeMetricTimeSeries(s.marketValue, "marketValue");
  const cash = normalizeMetricTimeSeries(s.cash, "cash");
  const principalSrc = s.principal;
  const out = {
    stageRate: stageRateSrc.map((p) => ({ date: p.date, rate: p.rate })),
    stageProfit: normalizeMetricTimeSeries(stageProfitSrc, "profit"),
    totalAssets,
    marketValue,
    cash,
    cashRatio: (s.cashRatio || []).map((p) => ({ date: p.date, cashRatio: p.cashRatio })),
  };
  if (Array.isArray(principalSrc) && principalSrc.length) {
    out.principal = normalizeMetricTimeSeries(principalSrc, "principal");
  }
  return out;
}

function formatProfitShareForPublic(v) {
  if (v == null || v === "") {
    return v;
  }
  if (typeof v === "string" && v.includes("%")) {
    return v;
  }
  const n = Number(v);
  if (!Number.isFinite(n)) {
    return String(v);
  }
  const pct = Math.abs(n) <= 1 && n !== 0 ? n * 100 : n;
  const sign = pct >= 0 ? "+" : "";
  return `${sign}${pct.toFixed(2)}%`;
}

function formatPxChangeForPublic(v) {
  if (v == null || v === "") {
    return v;
  }
  if (typeof v === "string" && v.includes("%")) {
    return v;
  }
  const n = Number(v);
  if (!Number.isFinite(n)) {
    return String(v);
  }
  const pct = Math.abs(n) <= 1 && Math.abs(n) > 0.0001 ? n * 100 : n;
  const sign = pct >= 0 ? "+" : "";
  return `${sign}${pct.toFixed(2)}%`;
}

function redactStockRank(stockRank) {
  if (!stockRank || typeof stockRank !== "object") {
    return stockRank;
  }
  const rows = Array.isArray(stockRank.rows) ? stockRank.rows : [];
  return {
    stage: stockRank.stage,
    periodStart: stockRank.periodStart,
    periodEnd: stockRank.periodEnd,
    rows: rows.map((row) => {
      const out = {};
      for (const key of STOCK_RANK_PUBLIC_KEYS) {
        if (row && row[key] != null) {
          out[key] = row[key];
        }
      }
      if (out.profitShare != null) {
        out.profitShare = formatProfitShareForPublic(row.profitShare);
      }
      if (out.pxChange != null) {
        out.pxChange = formatPxChangeForPublic(row.pxChange);
      }
      return out;
    }),
  };
}

function redactPublicAnalysisBundle(bundle) {
  if (!bundle || typeof bundle !== "object") {
    return bundle;
  }
  const series = redactSeries(bundle.series);
  const value = {
    metricsArchitecture: bundle.metricsArchitecture,
    meta: bundle.meta ? { ...bundle.meta } : {},
    returns: redactReturns(bundle.returns, series.stageProfit),
    assets: redactAssets(bundle.assets, series),
    series,
    stockRank: redactStockRank(bundle.stockRank),
  };
  if (bundle._diag) {
    value._diag = bundle._diag;
  }
  return finalizeMetricsBundlePayload(value);
}

module.exports = {
  redactPublicAnalysisBundle,
  STOCK_RANK_PUBLIC_KEYS,
  parsePlainAmountText,
  formatNormalizedIndex: formatIndexValue,
};
