/**
 * Public community earning tab: same home-bundle shape, strip absolute amounts.
 */

const HOLDING_ROW_PUBLIC_KEYS = new Set([
  "symbol",
  "name",
  "marketTag",
  "stockCode",
  "market",
  "currency",
  "isCnyStock",
  "price",
  "dayChange",
  "weight",
  "cost",
  "monthWeight",
  "yearWeight",
  "totalWeight",
  "totalRate",
  "regret",
  "lastTradeDate",
  "lastTradeSide",
  "lowEstimate",
  "lowEstimateChange",
  "highEstimate",
  "highEstimateChange",
  "valuationPercentile",
]);

const ASSET_PUBLIC_KEYS = new Set(["cashRatio", "stockRatio"]);

function redactReturnStages(stages) {
  const out = {};
  for (const [key, row] of Object.entries(stages || {})) {
    if (!row || typeof row !== "object") {
      continue;
    }
    out[key] = { rate: row.rate };
  }
  return out;
}

function redactAssets(assets) {
  const out = {};
  for (const key of ASSET_PUBLIC_KEYS) {
    if (assets && assets[key] != null) {
      out[key] = assets[key];
    }
  }
  return out;
}

function redactHoldingRow(row) {
  const out = {};
  for (const key of HOLDING_ROW_PUBLIC_KEYS) {
    if (row && row[key] != null) {
      out[key] = row[key];
    }
  }
  return out;
}

function redactPublicHomeBundle(bundle) {
  if (!bundle || typeof bundle !== "object") {
    return bundle;
  }
  const value = {
    metricsArchitecture: bundle.metricsArchitecture,
    meta: bundle.meta ? { ...bundle.meta } : {},
    returns: {
      stages: redactReturnStages(bundle.returns?.stages),
    },
    assets: redactAssets(bundle.assets),
    holdings: {
      rows: (Array.isArray(bundle.holdings?.rows) ? bundle.holdings.rows : []).map(redactHoldingRow),
    },
  };
  if (bundle._diag) {
    value._diag = bundle._diag;
  }
  return value;
}

module.exports = {
  redactPublicHomeBundle,
  HOLDING_ROW_PUBLIC_KEYS,
};
