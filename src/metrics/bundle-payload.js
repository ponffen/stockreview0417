/**
 * home-bundle / analysis-bundle 响应：去掉 *Display，标注 v3 数据来源。
 */
const METRICS_V3_TABLES = ["analysis_daily_snapshot", "symbol_daily_pnl"];

function stripDisplayFields(value) {
  if (value == null || typeof value !== "object") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(stripDisplayFields);
  }
  const out = {};
  for (const [k, v] of Object.entries(value)) {
    if (k.endsWith("Display")) {
      continue;
    }
    out[k] = stripDisplayFields(v);
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
  return stripDisplayFields(base);
}

module.exports = {
  METRICS_V3_TABLES,
  stripDisplayFields,
  finalizeMetricsBundlePayload,
};
