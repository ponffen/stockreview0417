/**
 * home-bundle / analysis-bundle 响应元数据（v3 表来源标注）。
 * 展示字段 *Display 由服务端格式化，前端直接渲染。
 */
const METRICS_V3_TABLES = ["analysis_daily_snapshot", "symbol_daily_pnl"];

function finalizeMetricsBundlePayload(payload) {
  if (!payload || typeof payload !== "object") {
    return payload;
  }
  return {
    metricsArchitecture: "v3",
    metricsTables: [...METRICS_V3_TABLES],
    ...payload,
  };
}

module.exports = {
  METRICS_V3_TABLES,
  finalizeMetricsBundlePayload,
};
