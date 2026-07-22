/**
 * 长桥 OpenAPI 实时行情（主数据源）。
 */
const { normalizeSymbol } = require("../db");
const { isUsTickerSymbol } = require("../tencent-quote-time");
const {
  decimalToNum,
  formatTimestampBeijing,
  buildQuoteRecord,
} = require("./quote-common");

const LONGPORT_CHUNK_SIZE = 450;
const LONGPORT_FETCH_TIMEOUT_MS = Math.max(
  3000,
  Math.min(20_000, Number(process.env.LONGPORT_QUOTE_TIMEOUT_MS || 12_000)),
);

const SESSION_LABELS = {
  pre: "盘前",
  post: "盘后",
  overnight: "夜盘",
};

let quoteContextPromise = null;

function envValue(...names) {
  for (const name of names) {
    const v = String(process.env[name] || "").trim();
    if (v) {
      return v;
    }
  }
  return "";
}

function isOvernightEnabled() {
  const raw = envValue("LONGPORT_ENABLE_OVERNIGHT", "LONGBRIDGE_ENABLE_OVERNIGHT");
  return raw === "1" || /^true$/i.test(raw);
}

async function getQuoteContext() {
  if (quoteContextPromise) {
    return quoteContextPromise;
  }
  quoteContextPromise = (async () => {
    const appKey = envValue("LONGPORT_APP_KEY", "LONGBRIDGE_APP_KEY");
    const appSecret = envValue("LONGPORT_APP_SECRET", "LONGBRIDGE_APP_SECRET");
    const accessToken = envValue("LONGPORT_ACCESS_TOKEN", "LONGBRIDGE_ACCESS_TOKEN");
    if (!appKey || !appSecret || !accessToken) {
      throw new Error("longport credentials missing");
    }
    const { Config, QuoteContext } = require("longbridge");
    const httpUrl = envValue("LONGPORT_HTTP_URL", "LONGBRIDGE_HTTP_URL");
    const extra = { enableOvernight: isOvernightEnabled() };
    if (httpUrl) {
      extra.httpUrl = httpUrl;
    }
    const config = Config.fromApikey(appKey, appSecret, accessToken, extra);
    return QuoteContext.new(config);
  })();
  return quoteContextPromise;
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

async function fetchLongportQuoteMap(symbols) {
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
    return { ok: false, map: new Map(), delayed: false, error: "no mappable symbols" };
  }

  const out = new Map();
  let delayed = false;
  let lastError = "";

  try {
    const ctx = await getQuoteContext();
    for (let i = 0; i < lpSymbols.length; i += LONGPORT_CHUNK_SIZE) {
      const chunk = lpSymbols.slice(i, i + LONGPORT_CHUNK_SIZE);
      const quotes = await Promise.race([
        ctx.quote(chunk),
        new Promise((_, reject) => {
          setTimeout(() => reject(new Error("longport quote timeout")), LONGPORT_FETCH_TIMEOUT_MS);
        }),
      ]);
      for (const q of quotes || []) {
        const lpSym = String(q?.symbol || "");
        const internal = lpToInternal.get(lpSym);
        const rec = parseLongportQuote(lpSym, q, internal);
        if (rec) {
          out.set(normalizeSymbol(rec.symbol), rec);
        }
      }
    }
  } catch (error) {
    lastError = error?.message || "longport quote failed";
    delayed = true;
  }

  if (out.size < symList.length && out.size > 0) {
    delayed = true;
  }

  return {
    ok: out.size > 0,
    map: out,
    delayed,
    error: out.size ? "" : lastError,
    source: "longport",
  };
}

module.exports = {
  toLongportSymbol,
  fetchLongportQuoteMap,
  getQuoteContext,
};
