/**
 * 服务端实时盈亏：与 /api/realtime/patch 同口径，供 metrics a/b/f 共用。
 */
const {
  getTrades,
  getCashTransfers,
  getAccounts,
  getLatestAnalysisSnapshotDate,
  getAnalysisDailySnapshots,
  getSymbolDailyCloseRange,
  batchLatestSymbolDailyCloseOnOrBefore,
  normalizeSymbol,
  addCalendarDays,
} = require("./db");
const { computeLedgerCashCnyUpToDate, principalCnyUpToDate } = require("./ledger-metrics");
const { toTencentQuoteKey } = require("./tencent-quote-meta");
const { shouldEmitTodayLivePoint, liveDateKeyShanghai } = require("./metrics/trading-calendar");

const FX_FALLBACK = { USD: 7.2, HKD: 0.92 };
const quoteMem = new Map();
const QUOTE_CHUNK_SIZE = 55;
const QUOTE_FETCH_TIMEOUT_MS = 8_000;
const QUOTE_PROXY_BASE =
  String(process.env.ALIYUN_QUOTE_PROXY_BASE_URL || "").trim().replace(/\/+$/, "") ||
  "https://market-oxy-http-market-proxy-pbftovdfne.cn-hangzhou.fcapp.run";
const LIVE_METRICS_DEDUP_MS = Math.max(
  1000,
  Math.min(8000, Number(process.env.LIVE_METRICS_DEDUP_MS || 4000)),
);
const liveMetricsInflight = new Map();
const liveMetricsRecent = new Map();

function pickLatestQuoteTime(times) {
  let best = "";
  let bestKey = 0;
  for (const item of Array.isArray(times) ? times : []) {
    const t = String(item || "").trim();
    const digits = t.replace(/\D/g, "");
    const key =
      digits.length >= 14
        ? Number(digits.slice(0, 14)) || 0
        : digits.length >= 8
          ? Number(`${digits.slice(0, 8)}000000`) || 0
          : 0;
    if (key > bestKey) {
      best = t;
      bestKey = key;
    }
  }
  return best || null;
}

function parseQuoteTimeToDateKey(timeStr) {
  if (!timeStr || typeof timeStr !== "string") {
    return null;
  }
  const compact = /^(\d{4})(\d{2})(\d{2})/.exec(String(timeStr).trim().replace(/\s/g, ""));
  return compact ? `${compact[1]}-${compact[2]}-${compact[3]}` : null;
}

function getTradingDateKeyBy0830(baseDate = new Date()) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(baseDate);
  const get = (type) => parts.find((p) => p.type === type)?.value;
  const y = Number(get("year"));
  const m = Number(get("month"));
  const d = Number(get("day"));
  const h = Number(get("hour"));
  const min = Number(get("minute"));
  const current = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  if (h < 8 || (h === 8 && min < 30)) {
    return addCalendarDays(current, -1);
  }
  return current;
}

function shouldCountTodayPositionPnlFromQuote(quote, now = new Date()) {
  const tradingKey = getTradingDateKeyBy0830(now);
  const quoteKey =
    (quote && quote.marketDate) ||
    (quote && quote.quoteDate) ||
    (quote && parseQuoteTimeToDateKey(quote.rawTime)) ||
    (quote && parseQuoteTimeToDateKey(quote.time)) ||
    null;
  return !!quoteKey && quoteKey === tradingKey;
}

function parseTencentPriceField(v) {
  const n = Number(String(v || "").replace(/,/g, ""));
  return Number.isFinite(n) ? n : NaN;
}

function parseTencentQuoteRecord(symbol, rawText) {
  if (!rawText || typeof rawText !== "string") {
    return null;
  }
  const parts = rawText.split("~");
  if (parts.length < 6) {
    return null;
  }
  const current = parseTencentPriceField(parts[3]);
  const prevClose = parseTencentPriceField(parts[4]);
  const rawTime = String(parts[30] || parts[31] || "--").trim();
  const marketDate = parseQuoteTimeToDateKey(rawTime);
  if (!Number.isFinite(current) || current <= 0) {
    return null;
  }
  return {
    name: String(parts[1] || "").trim() || symbol,
    current,
    prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
    time: rawTime || "--",
    rawTime,
    marketDate,
    quoteDate: marketDate,
  };
}

function parseTencentQuoteTextToMap(text) {
  const map = new Map();
  for (const chunk of String(text || "").split(";")) {
    const m = /v_(\w+)="([^"]*)"/.exec(chunk);
    if (!m) {
      continue;
    }
    const key = String(m[1]).toLowerCase();
    const rec = parseTencentQuoteRecord(key, m[2]);
    if (rec) {
      map.set(key, rec);
    }
  }
  return map;
}

async function fetchTencentQuoteChunk(keys) {
  const url = `${QUOTE_PROXY_BASE}/api/quote/tencent?q=${encodeURIComponent(keys.join(","))}`;
  const r = await fetch(url, {
    signal: AbortSignal.timeout(QUOTE_FETCH_TIMEOUT_MS),
  });
  if (!r.ok) {
    return { ok: false, map: new Map() };
  }
  const text = await r.text();
  return { ok: true, map: parseTencentQuoteTextToMap(text) };
}

async function fetchTencentQuotePayloadMap(reqKeys) {
  const keys = [...new Set((reqKeys || []).map((s) => String(s || "").trim()).filter(Boolean))];
  if (!keys.length) {
    return { ok: false, payloadMap: new Map(), delayed: true };
  }
  const payloadMap = new Map();
  let delayed = false;
  const chunks = [];
  for (let i = 0; i < keys.length; i += QUOTE_CHUNK_SIZE) {
    chunks.push(keys.slice(i, i + QUOTE_CHUNK_SIZE));
  }
  try {
    const parts = await Promise.all(chunks.map((c) => fetchTencentQuoteChunk(c)));
    for (const part of parts) {
      if (!part.ok) {
        delayed = true;
        continue;
      }
      for (const [k, payload] of part.map.entries()) {
        quoteMem.set(k, payload);
        payloadMap.set(k, payload);
      }
    }
  } catch {
    delayed = true;
  }
  for (const key of keys) {
    const k = String(key).toLowerCase();
    if (!payloadMap.has(k) && quoteMem.has(k)) {
      payloadMap.set(k, quoteMem.get(k));
      delayed = true;
    }
  }
  return { ok: payloadMap.size > 0, payloadMap, delayed };
}

function parseTencentForexFromPayload(raw) {
  if (!raw) {
    return null;
  }
  if (typeof raw === "object" && Number(raw.current) > 0) {
    return raw;
  }
  if (typeof raw !== "string") {
    return null;
  }
  const parts = raw.split("~");
  const current = parseTencentPriceField(parts[3]);
  if (!Number.isFinite(current) || current <= 0) {
    return null;
  }
  const prevClose = parseTencentPriceField(parts[4]);
  return { current, prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current };
}

async function lastCloseForSymbol(sym, asOf) {
  const rows = await getSymbolDailyCloseRange(sym, addCalendarDays(asOf, -10), asOf);
  let best = null;
  for (const r of rows || []) {
    const d = String(r.date || "").slice(0, 10);
    if (d <= asOf && (!best || d > best.date)) {
      best = r;
    }
  }
  const c = Number(best?.close);
  return Number.isFinite(c) && c > 0 ? c : null;
}

async function batchLastCloseForSymbols(symbols, asOf) {
  try {
    return await batchLatestSymbolDailyCloseOnOrBefore(symbols, asOf);
  } catch {
    return new Map();
  }
}


function getSymbolCurrency(symbol) {
  const s = String(symbol || "");
  if (s.startsWith("hk") || s.startsWith("rt_hk")) {
    return "HKD";
  }
  if (s.startsWith("sh") || s.startsWith("sz")) {
    return "CNY";
  }
  return "USD";
}

function holdingsSymbolsFromTrades(trades, accountScope) {
  const wanted = String(accountScope || "all").trim() || "all";
  const list =
    wanted === "all" ? trades : trades.filter((t) => String(t.accountId || "default") === wanted);
  const holdings = new Map();
  for (const trade of list.sort((a, b) => {
    const ad = new Date(a.date).getTime();
    const bd = new Date(b.date).getTime();
    if (ad !== bd) {
      return ad - bd;
    }
    return Number(a.createdAt || 0) - Number(b.createdAt || 0);
  })) {
    const symbol = normalizeSymbol(trade.symbol);
    if (!symbol) {
      continue;
    }
    holdings.set(
      symbol,
      (holdings.get(symbol) || 0) +
        (trade.side === "buy" ? Number(trade.quantity || 0) : -Number(trade.quantity || 0)),
    );
  }
  return [...holdings.entries()].filter(([, q]) => q > 1e-6).map(([s]) => s);
}

function liveMetricsCacheKey(userId, scope) {
  return `${String(userId || "").trim()}|${String(scope || "all").trim() || "all"}`;
}

async function computeLiveMetrics(userId, accountScope = "all") {
  const uid = String(userId || "").trim();
  const scope = String(accountScope || "all").trim() || "all";
  const tradingDay = shouldEmitTodayLivePoint();
  const liveDate = liveDateKeyShanghai();
  const frozenThrough = await getLatestAnalysisSnapshotDate(uid, scope);
  const baseRows = frozenThrough
    ? await getAnalysisDailySnapshots({ accountId: scope, from: frozenThrough, to: frozenThrough }, uid)
    : [];
  const lastSnap = baseRows[baseRows.length - 1] || null;
  const fxUsdFrozen = Number(lastSnap?.fxUsdCny ?? lastSnap?.fx_usd_cny) || FX_FALLBACK.USD;
  const fxHkdFrozen = Number(lastSnap?.fxHkdCny ?? lastSnap?.fx_hkd_cny) || FX_FALLBACK.HKD;
  const fxUsdMap = fxUsdFrozen > 0 ? { [String(frozenThrough || liveDate)]: fxUsdFrozen } : {};
  const fxHkdMap = fxHkdFrozen > 0 ? { [String(frozenThrough || liveDate)]: fxHkdFrozen } : {};
  const lastMarketValueCny =
    Number(lastSnap?.marketValue ?? lastSnap?.market_value) ||
    Number(lastSnap?.totalAssets ?? lastSnap?.total_assets) ||
    0;

  const trades = await getTrades(uid);
  const cashTransfers = await getCashTransfers(uid);
  const accounts = await getAccounts(uid);

  if (!tradingDay) {
    const cashCny = frozenThrough
      ? computeLedgerCashCnyUpToDate(
          trades,
          cashTransfers,
          accounts,
          scope === "all" ? "all" : scope,
          fxUsdMap,
          fxHkdMap,
          frozenThrough,
        )
      : 0;
    const ta = Number(lastSnap?.totalAssets ?? lastSnap?.total_assets ?? 0) || 0;
    const mv = Number(lastSnap?.marketValue ?? lastSnap?.market_value ?? 0) || 0;
    return {
      tradingDay: false,
      liveDate: null,
      frozenThrough: frozenThrough || null,
      delayed: false,
      quoteTime: null,
      todayProfitCny: 0,
      liveMarketValueCny: mv,
      lastMarketValueCny,
      cashCny,
      totalAssetsCny: ta || mv + cashCny,
      cashRatio: ta > 0 ? cashCny / ta : 0,
      principalCny: frozenThrough
        ? principalCnyUpToDate(cashTransfers, accounts, scope, fxUsdMap, fxHkdMap, frozenThrough)
        : 0,
      positions: [],
      fxUsdCny: fxUsdFrozen,
      fxHkdCny: fxHkdFrozen,
    };
  }

  const symbols = holdingsSymbolsFromTrades(trades, scope);
  const closeFallback = await batchLastCloseForSymbols(symbols, liveDate);
  const quoteReq = await fetchTencentQuotePayloadMap([
    ...symbols.map((s) => toTencentQuoteKey(s)).filter(Boolean),
    "whUSDCNY",
    "whHKDCNY",
  ]);
  const fxSpot = { CNY: 1, USD: FX_FALLBACK.USD, HKD: FX_FALLBACK.HKD };
  const payloadMap = quoteReq.payloadMap || new Map();
  const usdP = parseTencentForexFromPayload(payloadMap.get("whusdcny"));
  const hkdP = parseTencentForexFromPayload(payloadMap.get("whhkdcny"));
  if (usdP?.current > 0) {
    fxSpot.USD = usdP.current;
  }
  if (hkdP?.current > 0) {
    fxSpot.HKD = hkdP.current;
  }
  const fxRate = (ccy) => (ccy === "CNY" ? 1 : ccy === "USD" ? fxSpot.USD : ccy === "HKD" ? fxSpot.HKD : 1);

  const quoteMap = {};
  for (const sym of symbols) {
    const key = toTencentQuoteKey(sym);
    let q = payloadMap.get(String(key).toLowerCase());
    if (typeof q === "string") {
      q = parseTencentQuoteRecord(sym, q);
    }
    if (!q) {
      const close = closeFallback.get(sym);
      if (close != null) {
        q = { current: close, prevClose: close, marketDate: liveDate };
        if (!quoteReq.ok) {
          quoteReq.delayed = true;
        }
      }
    }
    if (q) {
      quoteMap[sym] = q;
    }
  }

  const scoped =
    scope === "all" ? trades : trades.filter((t) => String(t.accountId || "default") === scope);
  const holdings = new Map();
  for (const trade of scoped.sort((a, b) => {
    const ad = new Date(a.date).getTime();
    const bd = new Date(b.date).getTime();
    if (ad !== bd) {
      return ad - bd;
    }
    return Number(a.createdAt || 0) - Number(b.createdAt || 0);
  })) {
    const symbol = normalizeSymbol(trade.symbol);
    if (!symbol) {
      continue;
    }
    holdings.set(
      symbol,
      (holdings.get(symbol) || 0) +
        (trade.side === "buy" ? Number(trade.quantity || 0) : -Number(trade.quantity || 0)),
    );
  }

  let liveMarketValue = 0;
  let todayProfitCny = 0;
  const positions = [];
  for (const [symbol, qty] of holdings.entries()) {
    if (!(qty > 1e-6)) {
      continue;
    }
    const quote = quoteMap[symbol];
    if (!quote) {
      continue;
    }
    const current = Number(quote.current) || 0;
    const prevClose = Number(quote.prevClose) || current;
    const rate = fxRate(getSymbolCurrency(symbol));
    const mv = qty * current * rate;
    liveMarketValue += mv;
    const todayP = shouldCountTodayPositionPnlFromQuote(quote) ? qty * (current - prevClose) * rate : 0;
    todayProfitCny += todayP;
    positions.push({ symbol, quantity: qty, current, prevClose, todayProfitCny: todayP, marketValueCny: mv });
  }

  const cashCny = computeLedgerCashCnyUpToDate(trades, cashTransfers, accounts, scope, fxUsdMap, fxHkdMap, liveDate);
  const principalCny = principalCnyUpToDate(cashTransfers, accounts, scope, fxUsdMap, fxHkdMap, liveDate);
  const totalAssetsCny = liveMarketValue + cashCny;

  return {
    tradingDay: true,
    liveDate,
    frozenThrough: frozenThrough || null,
    delayed: !!quoteReq.delayed,
    quoteTime: pickLatestQuoteTime(Object.values(quoteMap).map((q) => q?.time)),
    todayProfitCny,
    liveMarketValueCny: liveMarketValue,
    lastMarketValueCny,
    cashCny,
    totalAssetsCny,
    cashRatio: totalAssetsCny > 0 ? cashCny / totalAssetsCny : 0,
    principalCny,
    positions,
    fxUsdCny: fxSpot.USD,
    fxHkdCny: fxSpot.HKD,
  };
}

async function getComputeLiveMetrics(userId, accountScope = "all") {
  const uid = String(userId || "").trim();
  const scope = String(accountScope || "all").trim() || "all";
  const key = liveMetricsCacheKey(uid, scope);
  const now = Date.now();
  const cached = liveMetricsRecent.get(key);
  if (cached && now - cached.at < LIVE_METRICS_DEDUP_MS) {
    return cached.value;
  }
  if (liveMetricsInflight.has(key)) {
    return liveMetricsInflight.get(key);
  }
  const task = computeLiveMetrics(uid, scope)
    .then((value) => {
      liveMetricsRecent.set(key, { at: Date.now(), value });
      return value;
    })
    .finally(() => {
      liveMetricsInflight.delete(key);
    });
  liveMetricsInflight.set(key, task);
  return task;
}

module.exports = {
  computeLiveMetrics,
  getComputeLiveMetrics,
  fetchTencentQuotePayloadMap,
  toTencentQuoteKey,
};
