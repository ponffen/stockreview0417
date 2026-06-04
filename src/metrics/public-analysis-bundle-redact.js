/**
 * Public community analysis tab: same analysis-bundle shape as private,
 * strip sensitive fields and normalize amount fields to index (base day = 1.0000).
 */

const { finalizeMetricsBundlePayload } = require("./bundle-payload");

const STOCK_RANK_PUBLIC_KEYS = new Set([
  "rank",
  "symbol",
  "name",
  "pxChange",
  "heldDays",
  "profitShare",
  "holdIntervalsLabel",
]);

const ASSET_AMOUNT_KEYS = ["totalAssets", "marketValue", "cash", "principal"];

function parsePlainAmountText(text) {
  let t = String(text ?? "").trim().replace(/,/g, "");
  if (!t || t === "–" || t === "—" || t === "-") {
    return null;
  }
  t = t.replace(/^¥\s*/i, "");
  const neg = t.startsWith("-") || t.startsWith("−");
  const n = parseFloat(t.replace(/^[+−-]/, ""));
  if (!Number.isFinite(n)) {
    return null;
  }
  return neg ? -n : n;
}

function formatNormalizedIndex(n) {
  if (!Number.isFinite(n)) {
    return "—";
  }
  return (Number(n) || 0).toFixed(4);
}

function normalizeSeriesPoints(points, valueKey) {
  const list = Array.isArray(points) ? points : [];
  if (!list.length) {
    return [];
  }
  const nums = list.map((p) => parsePlainAmountText(p?.[valueKey]));
  let baseIdx = -1;
  for (let i = 0; i < nums.length; i++) {
    if (nums[i] != null && Math.abs(nums[i]) > 1e-12) {
      baseIdx = i;
      break;
    }
  }
  if (baseIdx < 0) {
    const first = nums.find((n) => n != null);
    const base = first != null && Math.abs(first) > 1e-12 ? Math.abs(first) : 1;
    return list.map((p, i) => ({
      date: p.date,
      [valueKey]: formatNormalizedIndex(nums[i] != null ? nums[i] / base : null),
    }));
  }
  const base = nums[baseIdx];
  const baseAbs = Math.abs(base) > 1e-12 ? base : 1;
  return list.map((p, i) => {
    const raw = nums[i];
    const index = raw == null ? null : raw / baseAbs;
    return {
      date: p.date,
      [valueKey]: formatNormalizedIndex(index),
    };
  });
}

function normalizeHeadlineAmount(text, seriesPoints, valueKey) {
  const pts = normalizeSeriesPoints(seriesPoints, valueKey);
  if (!pts.length) {
    return "—";
  }
  return pts[pts.length - 1][valueKey];
}

function redactAssets(assets, series) {
  const src = assets || {};
  const out = {
    cashRatio: src.cashRatio,
    stockRatio: src.stockRatio,
  };
  if (src.totalAssets != null) {
    out.totalAssets = normalizeHeadlineAmount(
      src.totalAssets,
      series?.totalAssets,
      "totalAssets",
    );
  }
  if (src.marketValue != null) {
    out.marketValue = normalizeHeadlineAmount(
      src.marketValue,
      series?.marketValue,
      "marketValue",
    );
  }
  if (src.cash != null) {
    out.cash = normalizeHeadlineAmount(src.cash, series?.cash, "cash");
  }
  if (src.principal != null) {
    out.principal = normalizeHeadlineAmount(src.principal, series?.principal, "principal");
  }
  return out;
}

function redactReturns(returns, stageProfitSeries) {
  const row = returns || {};
  const out = { rate: row.rate };
  if (row.profit != null) {
    out.profit = normalizeHeadlineAmount(row.profit, stageProfitSeries, "profit");
  }
  return out;
}

function redactSeries(series) {
  const s = series || {};
  const stageProfitSrc = s.stageProfit || s.dailyProfit || [];
  const stageRateSrc = s.stageRate || s.dailyTwr || [];
  const totalAssets = normalizeSeriesPoints(s.totalAssets, "totalAssets");
  const marketValue = normalizeSeriesPoints(s.marketValue, "marketValue");
  const cash = normalizeSeriesPoints(s.cash, "cash");
  const principalSrc = s.principal;
  const out = {
    stageRate: stageRateSrc.map((p) => ({ date: p.date, rate: p.rate })),
    stageProfit: normalizeSeriesPoints(stageProfitSrc, "profit"),
    totalAssets,
    marketValue,
    cash,
    cashRatio: (s.cashRatio || []).map((p) => ({ date: p.date, cashRatio: p.cashRatio })),
  };
  if (Array.isArray(principalSrc) && principalSrc.length) {
    out.principal = normalizeSeriesPoints(principalSrc, "principal");
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
  formatNormalizedIndex,
};
