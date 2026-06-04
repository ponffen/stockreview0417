/**
 * Public amount series → rebased index (first non-zero |v| in window = 1.0000).
 * Used by community analysis tab redaction; change algorithm here only.
 */

const DEFAULT_ALGORITHM = "first-nonzero-abs";
const DEFAULT_EPSILON = 1e-12;
const DEFAULT_DECIMALS = 4;
const DEFAULT_NULL_DISPLAY = "—";

function parsePlainAmountText(text) {
  let t = String(text ?? "")
    .trim()
    .replace(/,/g, "");
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

function parseSeriesRawValues(points, valueKey) {
  const list = Array.isArray(points) ? points : [];
  return list.map((p) => {
    const cell = p?.[valueKey];
    if (cell == null || cell === "") {
      return null;
    }
    if (typeof cell === "number") {
      return Number.isFinite(cell) ? cell : null;
    }
    return parsePlainAmountText(cell);
  });
}

function formatIndexValue(n, options = {}) {
  const nullDisplay = options.nullDisplay ?? DEFAULT_NULL_DISPLAY;
  const decimals = options.decimals ?? DEFAULT_DECIMALS;
  if (!Number.isFinite(n)) {
    return nullDisplay;
  }
  return (Number(n) || 0).toFixed(decimals);
}

/**
 * Core index math. values[i] = raw_i / |base|, base = first |v| > epsilon.
 */
function normalizeSeriesToIndex(rawValues, options = {}) {
  const algorithm = options.algorithm ?? DEFAULT_ALGORITHM;
  const epsilon = options.epsilon ?? DEFAULT_EPSILON;
  const nums = Array.isArray(rawValues) ? rawValues : [];

  if (algorithm !== DEFAULT_ALGORITHM) {
    throw new Error(`unsupported series index algorithm: ${algorithm}`);
  }

  if (!nums.length) {
    return {
      values: [],
      baseIndex: -1,
      baseValue: null,
      meta: { algorithm, epsilon, decimals: options.decimals ?? DEFAULT_DECIMALS },
    };
  }

  let baseIdx = -1;
  for (let i = 0; i < nums.length; i++) {
    if (nums[i] != null && Math.abs(nums[i]) > epsilon) {
      baseIdx = i;
      break;
    }
  }

  if (baseIdx < 0) {
    const first = nums.find((n) => n != null);
    const baseAbs = first != null && Math.abs(first) > epsilon ? Math.abs(first) : 1;
    return {
      values: nums.map((raw) => (raw == null ? null : raw / baseAbs)),
      baseIndex: -1,
      baseValue: first,
      meta: { algorithm, epsilon, decimals: options.decimals ?? DEFAULT_DECIMALS, fallbackBase: baseAbs },
    };
  }

  const baseValue = nums[baseIdx];
  const baseAbs = Math.abs(baseValue) > epsilon ? Math.abs(baseValue) : 1;
  return {
    values: nums.map((raw) => (raw == null ? null : raw / baseAbs)),
    baseIndex: baseIdx,
    baseValue,
    meta: { algorithm, epsilon, decimals: options.decimals ?? DEFAULT_DECIMALS },
  };
}

function normalizeMetricTimeSeries(points, valueKey, options = {}) {
  const list = Array.isArray(points) ? points : [];
  if (!list.length) {
    return [];
  }
  const rawValues = parseSeriesRawValues(list, valueKey);
  const { values } = normalizeSeriesToIndex(rawValues, options);
  return list.map((p, i) => ({
    date: p.date,
    [valueKey]: formatIndexValue(values[i], options),
  }));
}

function normalizeHeadlineFromSeries(seriesPoints, valueKey, options = {}) {
  const pts = normalizeMetricTimeSeries(seriesPoints, valueKey, options);
  if (!pts.length) {
    return options.nullDisplay ?? DEFAULT_NULL_DISPLAY;
  }
  return pts[pts.length - 1][valueKey];
}

module.exports = {
  DEFAULT_ALGORITHM,
  DEFAULT_EPSILON,
  DEFAULT_DECIMALS,
  DEFAULT_NULL_DISPLAY,
  parsePlainAmountText,
  formatIndexValue,
  normalizeSeriesToIndex,
  normalizeMetricTimeSeries,
  normalizeHeadlineFromSeries,
  // Legacy export names
  formatNormalizedIndex: formatIndexValue,
};
