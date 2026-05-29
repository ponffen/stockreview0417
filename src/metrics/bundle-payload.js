/**
 * home-bundle / analysis-bundle：去掉 *Display 字段名，展示值写入对应短名（profit、ratePct、totalAssets…）。
 * 数值字段（profitCny、rate、*Num）保留供排序/着色/图表。
 */
const METRICS_V3_TABLES = ["analysis_daily_snapshot", "symbol_daily_pnl"];

/** Display 键 → bundle 对外键（避免与已有 number 字段冲突） */
const DISPLAY_KEY_ALIASES = {
  rateDisplay: "ratePct",
  rateTwrDisplay: "rateTwrPct",
  rateMwrDisplay: "rateMwrPct",
  valueDisplay: "valueFmt",
  pxChangeDisplay: "pxChangePct",
  profitShareDisplay: "profitShare",
};

function bundleKeyFromDisplayKey(key) {
  if (DISPLAY_KEY_ALIASES[key]) {
    return DISPLAY_KEY_ALIASES[key];
  }
  if (!key.includes("Display")) {
    return null;
  }
  return key.replace(/Display/g, "");
}

function hoistDisplayFields(value) {
  if (value == null || typeof value !== "object") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(hoistDisplayFields);
  }
  const out = {};
  for (const [k, v] of Object.entries(value)) {
    if (k.includes("Display")) {
      continue;
    }
    out[k] = hoistDisplayFields(v);
  }
  for (const [k, v] of Object.entries(value)) {
    if (!k.includes("Display")) {
      continue;
    }
    const target = bundleKeyFromDisplayKey(k);
    if (!target) {
      continue;
    }
    out[target] =
      v != null && typeof v === "object" && !Array.isArray(v) ? hoistDisplayFields(v) : v;
  }
  return out;
}

function finalizeMetricsBundlePayload(payload) {
  const base =
    payload && typeof payload === "object"
      ? {
          metricsArchitecture: "v3",
          metricsTables: [...METRICS_V3_TABLES],
          ...payload,
        }
      : payload;
  return hoistDisplayFields(base);
}

module.exports = {
  METRICS_V3_TABLES,
  bundleKeyFromDisplayKey,
  hoistDisplayFields,
  finalizeMetricsBundlePayload,
};
