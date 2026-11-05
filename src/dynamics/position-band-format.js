/**
 * 仓位带展示格式化（权重为小数，UI 输入为百分数）。
 */

const { fmtPercentRatio, fmtSignedPercentRatio } = require("../account-kpi-surface");

function formatPositionWeightDisplay(weightRatio) {
  const w = Number(weightRatio);
  if (!Number.isFinite(w) || w < 0) {
    return "";
  }
  return fmtPercentRatio(w);
}

/** (当前仓位-最低)/(最高-最低)*100%，取整，可小于 0% 或大于 100%。 */
function formatPositionPercentile(minWeight, maxWeight, currentWeight) {
  const min = Number(minWeight);
  const max = Number(maxWeight);
  const cur = Number(currentWeight);
  if (!Number.isFinite(min) || min <= 0 || !Number.isFinite(max) || max <= min) {
    return "—";
  }
  if (!Number.isFinite(cur) || cur < 0) {
    return "—";
  }
  const pct = Math.round(((cur - min) / (max - min)) * 100);
  return `${pct}%`;
}

function formatPositionMinWeightChange(minWeight, currentWeight) {
  const min = Number(minWeight);
  const cur = Number(currentWeight);
  if (!Number.isFinite(min) || min <= 0 || !Number.isFinite(cur) || cur < 0) {
    return "";
  }
  return fmtSignedPercentRatio(cur / min - 1);
}

function formatPositionMaxWeightChange(maxWeight, currentWeight) {
  const max = Number(maxWeight);
  const cur = Number(currentWeight);
  if (!Number.isFinite(max) || max <= 0 || !Number.isFinite(cur) || cur < 0) {
    return "";
  }
  return fmtSignedPercentRatio(max / cur - 1);
}

function formatPositionBandExtraDisplay(extra) {
  const raw = extra && typeof extra === "object" ? extra : {};
  const out = {};
  const min = formatPositionWeightDisplay(raw.minWeight);
  const max = formatPositionWeightDisplay(raw.maxWeight);
  if (min) {
    out.minWeight = min;
  }
  if (max) {
    out.maxWeight = max;
  }
  return out;
}

module.exports = {
  formatPositionWeightDisplay,
  formatPositionPercentile,
  formatPositionMinWeightChange,
  formatPositionMaxWeightChange,
  formatPositionBandExtraDisplay,
};
