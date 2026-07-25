/**
 * 个股估值展示格式化（API 直出字符串，前端直接渲染）。
 */

function formatValuationPrice(n) {
  const v = Number(n);
  if (!Number.isFinite(v) || v <= 0) {
    return "";
  }
  return v.toLocaleString("zh-CN", { minimumFractionDigits: 3, maximumFractionDigits: 3 });
}

/** 将 DB/CRUD 中的数值 extra 格式化为 Feed 展示用字符串。 */
function formatValuationExtraDisplay(extra) {
  const raw = extra && typeof extra === "object" ? extra : {};
  const out = {};
  const low = formatValuationPrice(raw.lowPrice);
  const high = formatValuationPrice(raw.highPrice);
  if (low) {
    out.lowPrice = low;
  }
  if (high) {
    out.highPrice = high;
  }
  return out;
}

module.exports = {
  formatValuationPrice,
  formatValuationExtraDisplay,
};
