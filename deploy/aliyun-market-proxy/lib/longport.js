const {
  decimalToNum,
  formatTimestampBeijing,
  buildQuoteRecord,
  isUsTickerSymbol,
} = require("./quote-common");
const { headerValue } = require("./http");

const LONGPORT_CHUNK_SIZE = 450;
const LONGPORT_FETCH_TIMEOUT_MS = Math.max(
  3000,
  Math.min(30_000, Number(process.env.LONGPORT_QUOTE_TIMEOUT_MS || 15_000)),
);

const SESSION_LABELS = {
  pre: "盘前",
  post: "盘后",
  overnight: "夜盘",
};

const quoteContextCache = new Map();

function pickCredsValue(creds, ...keys) {
  for (const key of keys) {
    const v = String(creds?.[key] || "").trim();
    if (v) {
      return v;
    }
  }
  return "";
}

function isOvernightEnabled(creds) {
  const raw = pickCredsValue(creds, "enableOvernight");
  return raw === "1" || /^true$/i.test(raw);
}

function credsCacheKey(creds) {
  const appKey = pickCredsValue(creds, "appKey");
  const accessToken = pickCredsValue(creds, "accessToken");
  return `${appKey}::${accessToken}`;
}

async function getQuoteContext(creds) {
  const appKey = pickCredsValue(creds, "appKey");
  const appSecret = pickCredsValue(creds, "appSecret");
  const accessToken = pickCredsValue(creds, "accessToken");
  if (!appKey || !appSecret || !accessToken) {
    throw new Error("longport credentials missing");
  }
  const cacheKey = credsCacheKey(creds);
  if (quoteContextCache.has(cacheKey)) {
    return quoteContextCache.get(cacheKey);
  }
  const promise = (async () => {
    const { Config, QuoteContext } = require("longbridge");
    const httpUrl = pickCredsValue(creds, "httpUrl");
    const extra = { enableOvernight: isOvernightEnabled(creds) };
    if (httpUrl) {
      extra.httpUrl = httpUrl;
    }
    const config = Config.fromApikey(appKey, appSecret, accessToken, extra);
    return QuoteContext.new(config);
  })();
  quoteContextCache.set(cacheKey, promise);
  return promise;
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

function sessionCandidate(session, sessionLabel, block) {
  if (!block) {
    return null;
  }
  const current = decimalToNum(block.lastDone);
  const prevClose = decimalToNum(block.prevClose);
  const ts = block.timestamp instanceof Date ? block.timestamp.getTime() : 0;
  if (!Number.isFinite(current) || current <= 0 || !ts) {
    return null;
  }
  return { session, sessionLabel, current, prevClose, ts, time: formatTimestampBeijing(block.timestamp) };
}

function resolveUsActiveSession(quote) {
  const regular = sessionCandidate("regular", null, {
    lastDone: quote.lastDone,
    prevClose: quote.prevClose,
    timestamp: quote.timestamp,
  });
  const candidates = [
    sessionCandidate("overnight", SESSION_LABELS.overnight, quote.overnightQuote),
    sessionCandidate("post", SESSION_LABELS.post, quote.postMarketQuote),
    sessionCandidate("pre", SESSION_LABELS.pre, quote.preMarketQuote),
    regular,
  ].filter(Boolean);
  if (!candidates.length) {
    return null;
  }
  candidates.sort((a, b) => b.ts - a.ts);
  return candidates[0];
}

function parseLongportQuote(lpSymbol, quote, internalSym) {
  const sym = longportSymbolToInternal(lpSymbol, internalSym);
  if (!quote) {
    return null;
  }
  let session = "regular";
  let sessionLabel = null;
  let current = decimalToNum(quote.lastDone);
  let prevClose = decimalToNum(quote.prevClose);
  let time = formatTimestampBeijing(quote.timestamp);

  if (isUsTickerSymbol(sym)) {
    const active = resolveUsActiveSession(quote);
    if (active) {
      session = active.session;
      sessionLabel = active.sessionLabel;
      current = active.current;
      prevClose = active.prevClose;
      time = active.time;
    }
  }

  return buildQuoteRecord({
    symbol: sym,
    current,
    prevClose,
    time,
    rawTime: time,
    session,
    sessionLabel,
    source: "longport",
  });
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
    const ctx = await getQuoteContext(creds);
    for (let i = 0; i < lpSymbols.length; i += LONGPORT_CHUNK_SIZE) {
      const chunk = lpSymbols.slice(i, i + LONGPORT_CHUNK_SIZE);
      const rows = await Promise.race([
        ctx.quote(chunk),
        new Promise((_, reject) => {
          setTimeout(() => reject(new Error("longport quote timeout")), LONGPORT_FETCH_TIMEOUT_MS);
        }),
      ]);
      for (const q of rows || []) {
        const lpSym = String(q?.symbol || "");
        const internal = lpToInternal.get(lpSym);
        const rec = parseLongportQuote(lpSym, q, internal);
        if (rec) {
          quotes[normalizeSymbol(rec.symbol)] = rec;
        }
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
    error: got ? "" : lastError,
    source: "longport",
  };
}

module.exports = {
  fetchLongportQuotes,
  longportCredsFromHeaders,
  normalizeSymbol,
};
