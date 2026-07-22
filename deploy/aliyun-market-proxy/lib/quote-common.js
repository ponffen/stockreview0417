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
    session: session || null,
    sessionLabel: sessionLabel || null,
    source: String(source || "unknown"),
    delayed: !!delayed,
  };
}

function isUsTickerSymbol(sym) {
  const s = String(sym || "").trim().toLowerCase();
  return /^us[a-z0-9._-]+$/.test(s) || /^gb_[a-z0-9._-]+$/.test(s) || /^[a-z][a-z0-9._-]*$/.test(s);
}

module.exports = {
  parsePriceField,
  decimalToNum,
  formatTimestampBeijing,
  buildQuoteRecord,
  isUsTickerSymbol,
};
