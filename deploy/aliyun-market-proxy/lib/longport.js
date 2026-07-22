const {
  decimalToNum,
  formatTimestampBeijing,
  buildQuoteRecord,
  isUsTickerSymbol,
} = require("./quote-common");
const { headerValue } = require("./http");
const { fetchQuotesOverWs } = require("./longport-ws");

const LONGPORT_CHUNK_SIZE = 450;
const LONGPORT_FETCH_TIMEOUT_MS = Math.max(
  3000,
  Math.min(30_000, Number(process.env.LONGPORT_QUOTE_TIMEOUT_MS || 15_000)),
);

function pickCredsValue(creds, ...keys) {
  for (const key of keys) {
    const v = String(creds?.[key] || "").trim();
    if (v) {
      return v;
    }
  }
  return "";
}

function longportCredsFromHeaders(headers) {
  const appKey = headerValue(headers, "x-longport-app-key") || headerValue(headers, "x-longbridge-app-key");
  const appSecret =
    headerValue(headers, "x-longport-app-secret") || headerValue(headers, "x-longbridge-app-secret");
  const accessToken =
    headerValue(headers, "x-longport-access-token") || headerValue(headers, "x-longbridge-access-token");
  if (!appKey && !appSecret && !accessToken) {
    return null;
  }
  return {
    appKey,
    appSecret,
    accessToken,
    httpUrl: headerValue(headers, "x-longport-http-url") || headerValue(headers, "x-longbridge-http-url"),
    enableOvernight:
      headerValue(headers, "x-longport-enable-overnight") ||
      headerValue(headers, "x-longbridge-enable-overnight"),
  };
}

function normalizeSymbol(rawSymbol) {
  const value = String(rawSymbol || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
  if (!value) {
    return "";
  }
  if (/^sh\d{6}$/.test(value) || /^sz\d{6}$/.test(value) || /^hk\d{5}$/.test(value)) {
    return value;
  }
  if (/^\d{6}$/.test(value)) {
    return ["5", "6", "9"].includes(value[0]) ? `sh${value}` : `sz${value}`;
  }
  if (/^\d{1,5}$/.test(value)) {
    return `hk${value.padStart(5, "0")}`;
  }
  if (/^us[a-z0-9._-]+$/i.test(value)) {
    return value.toLowerCase();
  }
  return value;
}

function toLongportSymbol(rawSymbol) {
  const sym = normalizeSymbol(rawSymbol);
  if (!sym) {
    return "";
  }
  if (/^hk\d{5}$/.test(sym)) {
    const code = String(parseInt(sym.slice(2), 10));
    return `${code}.HK`;
  }
  if (/^sh\d{6}$/.test(sym)) {
    return `${sym.slice(2)}.SH`;
  }
  if (/^sz\d{6}$/.test(sym)) {
    return `${sym.slice(2)}.SZ`;
  }
  let ticker = sym;
  if (/^us[a-z0-9._-]+$/i.test(sym) && !/^us_/i.test(sym)) {
    ticker = sym.replace(/^us/i, "");
  } else if (/^gb_/i.test(sym)) {
    ticker = sym.slice(3);
  }
  ticker = String(ticker || "")
    .replace(/\.(OQ|N)$/i, "")
    .toUpperCase();
  if (!ticker) {
    return "";
  }
  return `${ticker}.US`;
}

function longportSymbolToInternal(lpSymbol, fallbackSym) {
  const raw = String(lpSymbol || "").trim();
  const fb = normalizeSymbol(fallbackSym);
  if (!raw) {
    return fb;
  }
  const [ticker, region] = raw.split(".");
  const reg = String(region || "").toUpperCase();
  if (reg === "HK") {
    return `hk${String(ticker || "").replace(/\D/g, "").padStart(5, "0")}`;
  }
  if (reg === "SH") {
    return `sh${String(ticker || "").padStart(6, "0")}`;
  }
  if (reg === "SZ") {
    return `sz${String(ticker || "").padStart(6, "0")}`;
  }
  if (reg === "US") {
    return `us${String(ticker || "").toUpperCase()}`;
  }
  return fb || raw.toLowerCase();
}

async function fetchLongportQuotes(symbols, creds) {
  const symList = [...new Set((symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))];
  const lpToInternal = new Map();
  for (const sym of symList) {
    const lp = toLongportSymbol(sym);
    if (lp) {
      lpToInternal.set(lp, sym);
    }
  }
  const lpSymbols = [...lpToInternal.keys()];
  if (!lpSymbols.length) {
    return { ok: false, quotes: {}, delayed: false, error: "no mappable symbols" };
  }

  const quotes = {};
  let delayed = false;
  let lastError = "";

  try {
    for (let i = 0; i < lpSymbols.length; i += LONGPORT_CHUNK_SIZE) {
      const chunk = lpSymbols.slice(i, i + LONGPORT_CHUNK_SIZE);
      const chunkQuotes = await fetchQuotesOverWs(chunk, lpToInternal, creds, LONGPORT_FETCH_TIMEOUT_MS);
      for (const [sym, rec] of Object.entries(chunkQuotes)) {
        quotes[normalizeSymbol(sym)] = rec;
      }
    }
  } catch (error) {
    lastError = error?.message || "longport quote failed";
    delayed = true;
  }

  const got = Object.keys(quotes).length;
  if (got > 0 && got < symList.length) {
    delayed = true;
  }

  return {
    ok: got > 0,
    quotes,
    delayed,
    error: got ? "" : lastError || "longport quote returned no data",
    source: "longport",
  };
}

module.exports = {
  fetchLongportQuotes,
  longportCredsFromHeaders,
  normalizeSymbol,
};
