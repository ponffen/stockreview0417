/**
 * 行情记录与通用工具（长桥 / 腾讯编排层共用）。
 */
const { parseQuoteTimeToDateKey, isExtendedQuoteSession } = require("../position-today-pnl");

function parsePriceField(v) {
  const n = Number(String(v ?? "").replace(/,/g, ""));
  return Number.isFinite(n) ? n : NaN;
}

function decimalToNum(d) {
  if (d == null) {
    return NaN;
  }
  if (typeof d === "number") {
    return Number.isFinite(d) ? d : NaN;
  }
  if (typeof d === "string") {
    return parsePriceField(d);
  }
  if (typeof d.toNumber === "function") {
    return d.toNumber();
  }
  return parsePriceField(String(d));
}

function formatTimestampBeijing(ts) {
  if (!ts) {
    return "--";
  }
  const d = ts instanceof Date ? ts : new Date(Number(ts) > 1e12 ? ts : Number(ts) * 1000);
  if (!Number.isFinite(d.getTime())) {
    return "--";
  }
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(d);
  const get = (type) => parts.find((p) => p.type === type)?.value || "";
  return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")}`;
}

function quoteTimeSortKey(timeStr) {
  if (!timeStr || typeof timeStr !== "string") {
    return 0;
  }
  const raw = String(timeStr).trim();
  if (!raw || raw === "--") {
    return 0;
  }
  const digits = raw.replace(/\D/g, "");
  if (digits.length >= 14) {
    return Number(digits.slice(0, 14)) || 0;
  }
  if (digits.length >= 8) {
    return Number(`${digits.slice(0, 8)}000000`) || 0;
  }
  return 0;
}

function pickLatestQuoteTime(times) {
  const list = Array.isArray(times) ? times : [];
  let best = "";
  let bestKey = 0;
  for (const item of list) {
    const time = String(item || "").trim();
    const key = quoteTimeSortKey(time);
    if (key > bestKey) {
      best = time;
      bestKey = key;
    }
  }
  return best || null;
}

/**
 * @typedef {object} QuoteRecord
 * @property {string} symbol
 * @property {string} [name]
 * @property {number} current
 * @property {number} prevClose
 * @property {string} time
 * @property {string} [rawTime]
 * @property {string|null} [marketDate]
 * @property {string|null} [quoteDate]
 * @property {string|null} [session]
 * @property {string|null} [sessionLabel]
 * @property {string} source
 * @property {boolean} [delayed]
 */

function buildQuoteRecord({
  symbol,
  name,
  current,
  prevClose,
  time,
  rawTime,
  session = null,
  sessionLabel = null,
  source,
  delayed = false,
}) {
  const cur = Number(current);
  const prev = Number(prevClose);
  const t = String(time || "--").trim() || "--";
  const raw = String(rawTime || t).trim() || t;
  const marketDate = parseQuoteTimeToDateKey(raw) || parseQuoteTimeToDateKey(t);
  if (!Number.isFinite(cur) || cur <= 0) {
    return null;
  }
  const pc = Number.isFinite(prev) && prev > 0 ? prev : cur;
  return {
    symbol: String(symbol || ""),
    name: name ? String(name) : undefined,
    current: cur,
    prevClose: pc,
    time: t,
    rawTime: raw,
    marketDate,
    quoteDate: marketDate,
    session: session || null,
    sessionLabel: sessionLabel || null,
    source: String(source || "unknown"),
    delayed: !!delayed,
  };
}

function resolveQuoteSource(sources) {
  const set = new Set((sources || []).filter(Boolean));
  if (!set.size) {
    return "";
  }
  if (set.size === 1) {
    return [...set][0];
  }
  return "mixed";
}

module.exports = {
  parsePriceField,
  decimalToNum,
  formatTimestampBeijing,
  quoteTimeSortKey,
  pickLatestQuoteTime,
  buildQuoteRecord,
  resolveQuoteSource,
  isExtendedQuoteSession,
};
