/**
 * 腾讯行情时间：美股美东 → 北京时间（与 app.js parseTencentQuoteRecord 展示口径一致）。
 */
const { normalizeSymbol } = require("./db-pure");

function parseQuoteTimeParts(timeStr) {
  if (!timeStr || typeof timeStr !== "string") {
    return null;
  }
  const raw = String(timeStr).trim();
  if (!raw || raw === "--") {
    return null;
  }
  const compact = raw.replace(/\D/g, "");
  if (compact.length >= 14) {
    return {
      year: compact.slice(0, 4),
      month: compact.slice(4, 6),
      day: compact.slice(6, 8),
      hour: compact.slice(8, 10),
      minute: compact.slice(10, 12),
      second: compact.slice(12, 14),
    };
  }
  const iso = /^(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})(?:\D+(\d{1,2})[:：](\d{1,2})(?:[:：](\d{1,2}))?)?/.exec(raw);
  if (iso) {
    return {
      year: iso[1],
      month: String(Number(iso[2])).padStart(2, "0"),
      day: String(Number(iso[3])).padStart(2, "0"),
      hour: String(Number(iso[4] || 0)).padStart(2, "0"),
      minute: String(Number(iso[5] || 0)).padStart(2, "0"),
      second: String(Number(iso[6] || 0)).padStart(2, "0"),
    };
  }
  return null;
}

function nthWeekdayOfMonth(year, month, weekday, nth) {
  const firstDow = new Date(Date.UTC(year, month - 1, 1)).getUTCDay();
  return 1 + ((7 + weekday - firstDow) % 7) + (Math.max(1, nth) - 1) * 7;
}

function isUsEasternDstByLocalParts(parts) {
  if (!parts) {
    return false;
  }
  const year = Number(parts.year);
  const month = Number(parts.month);
  const day = Number(parts.day);
  const hour = Number(parts.hour || 0);
  if (![year, month, day, hour].every(Number.isFinite)) {
    return false;
  }
  if (month < 3 || month > 11) {
    return false;
  }
  if (month > 3 && month < 11) {
    return true;
  }
  const secondSundayInMarch = nthWeekdayOfMonth(year, 3, 0, 2);
  const firstSundayInNovember = nthWeekdayOfMonth(year, 11, 0, 1);
  if (month === 3) {
    if (day > secondSundayInMarch) {
      return true;
    }
    if (day < secondSundayInMarch) {
      return false;
    }
    return hour >= 2;
  }
  if (day < firstSundayInNovember) {
    return true;
  }
  if (day > firstSundayInNovember) {
    return false;
  }
  return hour < 2;
}

function isUsTickerSymbol(symbol) {
  const s = String(symbol || "").trim().toLowerCase();
  if (!s) {
    return false;
  }
  if (
    s.startsWith("sh") ||
    s.startsWith("sz") ||
    s.startsWith("hk") ||
    s.startsWith("rt_hk") ||
    s.startsWith("fx_") ||
    /^wh(usd|hkd)cny$/.test(s) ||
    s === "usdcny" ||
    s === "hkdcny"
  ) {
    return false;
  }
  return /^[a-z][a-z0-9._-]*$/i.test(s);
}

function convertUsEasternTimeToBeijing(timeStr) {
  const parts = parseQuoteTimeParts(timeStr);
  if (!parts) {
    return "";
  }
  const year = Number(parts.year);
  const month = Number(parts.month);
  const day = Number(parts.day);
  const hour = Number(parts.hour || 0);
  const minute = Number(parts.minute || 0);
  const second = Number(parts.second || 0);
  if (![year, month, day, hour, minute, second].every(Number.isFinite)) {
    return "";
  }
  const diffHours = isUsEasternDstByLocalParts(parts) ? 12 : 13;
  const baseMs = Date.UTC(year, month - 1, day, hour, minute, second);
  const bj = new Date(baseMs + diffHours * 60 * 60 * 1000);
  return [
    String(bj.getUTCFullYear()).padStart(4, "0"),
    String(bj.getUTCMonth() + 1).padStart(2, "0"),
    String(bj.getUTCDate()).padStart(2, "0"),
    String(bj.getUTCHours()).padStart(2, "0"),
    String(bj.getUTCMinutes()).padStart(2, "0"),
    String(bj.getUTCSeconds()).padStart(2, "0"),
  ].join("");
}

function normalizeQuoteTimeToBeijingBySymbol(timeStr, symbol) {
  const raw = String(timeStr || "").trim();
  if (!raw || raw === "--") {
    return "--";
  }
  const normalized = normalizeSymbol(symbol || "");
  if (!isUsTickerSymbol(normalized)) {
    return raw;
  }
  return convertUsEasternTimeToBeijing(raw) || raw;
}

module.exports = {
  parseQuoteTimeParts,
  normalizeQuoteTimeToBeijingBySymbol,
  isUsTickerSymbol,
};
