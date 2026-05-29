/**
 * home-bundle / analysis-bundle：仅标注架构版本，不做字段映射。
 */
function finalizeMetricsBundlePayload(payload) {
  if (!payload || typeof payload !== "object") {
    return payload;
  }
  return {
    metricsArchitecture: "v3",
    ...payload,
  };
}

module.exports = {
  finalizeMetricsBundlePayload,
};
