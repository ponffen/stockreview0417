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
  normalizeSymbol,
  addCalendarDays,
} = require("./db");
const { computeLedgerCashCnyUpToDate, principalCnyUpToDate } = require("./ledger-metrics");
const { toTencentQuoteKey } = require("./tencent-quote-meta");
const { shouldEmitTodayLivePoint, liveDateKeyShanghai } = require("./metrics/trading-calendar");
const { holdingsSymbolsFromTrades } = require("./metrics/holdings-active-symbols");
const {
  parseQuoteTimeToDateKey,
  todayProfitCnyForHolding,
} = require("./position-today-pnl");


const FX_FALLBACK = { USD: 7.2, HKD: 0.92 };
const quoteMem = new Map();
const QUOTE_CHUNK_SIZE = 55;
const QUOTE_FETCH_TIMEOUT_MS = 5_000;
const QUOTE_TOTAL_BUDGET_MS = Math.max(
  2000,
  Math.min(12_000, Number(process.env.QUOTE_TOTAL_BUDGET_MS || 7000)),
);
const QUOTE_CHUNK_CONCURRENCY = Math.max(1, Math.min(3, Number(process.env.QUOTE_CHUNK_CONCURRENCY || 2)));
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

async function mapPool(items, limit, fn) {
  const n = items.length;
  if (!n) {
    return [];
  }
  const out = new Array(n);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, n) }, async () => {
    while (next < n) {
      const i = next++;
      out[i] = await fn(items[i], i);
    }
  });
  await Promise.all(workers);
  return out;
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

async function fetchTencentQuotePayloadMap(reqKeys, budgetMs = QUOTE_TOTAL_BUDGET_MS) {
  const keys = [...new Set((reqKeys || []).map((s) => String(s || "").trim()).filter(Boolean))];
  if (!keys.length) {
    return { ok: false, payloadMap: new Map(), delayed: true };
  }
  const budget = Math.max(1000, Number(budgetMs) || QUOTE_TOTAL_BUDGET_MS);
  const payloadMap = new Map();
  let delayed = false;
  const chunks = [];
  for (let i = 0; i < keys.length; i += QUOTE_CHUNK_SIZE) {
    chunks.push(keys.slice(i, i + QUOTE_CHUNK_SIZE));
  }
  const work = (async () => {
    const parts = await mapPool(chunks, QUOTE_CHUNK_CONCURRENCY, (c) => fetchTencentQuoteChunk(c));
    for (const part of parts) {
      if (!part?.ok) {
        delayed = true;
        continue;
      }
      for (const [k, payload] of part.map.entries()) {
        quoteMem.set(k, payload);
        payloadMap.set(k, payload);
      }
    }
  })();
  let timedOut = false;
  try {
    await Promise.race([
      work,
      new Promise((resolve) => {
        setTimeout(() => {
          timedOut = true;
          resolve();
        }, budget);
      }),
    ]);
  } catch {
    delayed = true;
  }
  if (timedOut) {
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
  const out = new Map();
  const list = [...new Set((symbols || []).map((sym) => normalizeSymbol(sym)).filter(Boolean))];
  await Promise.all(
    list.map(async (sym) => {
      const rows = await getSymbolDailyCloseRange(sym, addCalendarDays(asOf, -10), asOf);
      let best = null;
      for (const r of rows || []) {
        const d = String(r.date || "").slice(0, 10);
        if (d <= asOf && (!best || d > best.date)) {
          best = r;
        }
      }
      const c = Number(best?.close);
      if (Number.isFinite(c) && c > 0) {
        out.set(sym, { close: c, date: String(best.date || "").slice(0, 10) });
      }
    }),
  );
  return out;
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

function buildLiveFromHomeFrozen({
  tradingDay,
  liveDate,
  frozenThrough,
  homeAcc,
  trades,
  cashTransfers,
  accounts,
  scope,
  fxUsdMap,
  fxHkdMap,
  fxUsdFrozen,
  fxHkdFrozen,
  lastMarketValueCny,
}) {
  const asOf = tradingDay ? liveDate : frozenThrough || liveDate;
  const cashCny = computeLedgerCashCnyUpToDate(trades, cashTransfers, accounts, scope, fxUsdMap, fxHkdMap, asOf);
  const principalCny = principalCnyUpToDate(cashTransfers, accounts, scope, fxUsdMap, fxHkdMap, asOf);
  const ta =
    Number(homeAcc?.eod_total_assets_cny) ||
    (Number(homeAcc?.eod_market_value_cny) || 0) + (Number(homeAcc?.eod_cash_cny) || 0) ||
    lastMarketValueCny ||
    0;
  const mv = Number(homeAcc?.eod_market_value_cny) || lastMarketValueCny || 0;
  const cashUse = Number(homeAcc?.eod_cash_cny) || cashCny;
  const totalAssetsCny = ta || mv + cashUse;
  return {
    tradingDay: !!tradingDay,
    liveDate: tradingDay ? liveDate : null,
    frozenThrough: frozenThrough || null,
    delayed: false,
    quoteTime: null,
    todayProfitCny: 0,
    liveMarketValueCny: mv,
    lastMarketValueCny,
    cashCny: cashUse,
    totalAssetsCny,
    cashRatio: totalAssetsCny > 0 ? cashUse / totalAssetsCny : 0,
    principalCny: Number(homeAcc?.eod_principal_cny) || principalCny,
    positions: [],
    fxUsdCny: fxUsdFrozen || FX_FALLBACK.USD,
    fxHkdCny: fxHkdFrozen || FX_FALLBACK.HKD,
    clearedScope: true,
  };
}

function liveMetricsCacheKey(userId, scope) {
  return `${String(userId || "").trim()}|${String(scope || "all").trim() || "all"}`;
}

async function computeLiveMetrics(userId, accountScope = "all", opts = {}) {
  const uid = String(userId || "").trim();
  const scope = String(accountScope || "all").trim() || "all";
  const tradingDay = shouldEmitTodayLivePoint();
  const liveDate = liveDateKeyShanghai();
  const homeAcc = opts?.preloaded?.homeAccount || null;
  let frozenThrough = String(homeAcc?.frozen_through || homeAcc?.frozenThrough || "").slice(0, 10) || null;
  if (!frozenThrough) {
    frozenThrough = await getLatestAnalysisSnapshotDate(uid, scope);
  }
  let fxUsdFrozen = Number(homeAcc?.eod_fx_usd_cny) || 0;
  let fxHkdFrozen = Number(homeAcc?.eod_fx_hkd_cny) || 0;
  let lastMarketValueCny = Number(homeAcc?.last_market_value_cny) || Number(homeAcc?.eod_market_value_cny) || 0;
  if (!homeAcc || !(fxUsdFrozen > 0) || !(fxHkdFrozen > 0) || !(lastMarketValueCny > 0)) {
    const baseRows = frozenThrough
      ? await getAnalysisDailySnapshots({ accountId: scope, from: frozenThrough, to: frozenThrough }, uid)
      : [];
    const lastSnap = baseRows[baseRows.length - 1] || null;
    if (!(fxUsdFrozen > 0)) {
      fxUsdFrozen = Number(lastSnap?.fxUsdCny ?? lastSnap?.fx_usd_cny) || FX_FALLBACK.USD;
    }
    if (!(fxHkdFrozen > 0)) {
      fxHkdFrozen = Number(lastSnap?.fxHkdCny ?? lastSnap?.fx_hkd_cny) || FX_FALLBACK.HKD;
    }
    if (!(lastMarketValueCny > 0)) {
      lastMarketValueCny =
        Number(lastSnap?.marketValue ?? lastSnap?.market_value) ||
        Number(lastSnap?.totalAssets ?? lastSnap?.total_assets) ||
        0;
    }
  }
  const fxUsdMap = fxUsdFrozen > 0 ? { [String(frozenThrough || liveDate)]: fxUsdFrozen } : {};
  const fxHkdMap = fxHkdFrozen > 0 ? { [String(frozenThrough || liveDate)]: fxHkdFrozen } : {};

  const pre = opts?.preloaded || {};
  const [trades, cashTransfers, accounts] = await Promise.all([
    pre.trades ? Promise.resolve(pre.trades) : getTrades(uid),
    pre.cashTransfers ? Promise.resolve(pre.cashTransfers) : getCashTransfers(uid),
    pre.accounts ? Promise.resolve(pre.accounts) : getAccounts(uid),
  ]);

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
    const ta =
      Number(homeAcc?.eod_total_assets_cny) ||
      (Number(homeAcc?.eod_market_value_cny) || 0) + (Number(homeAcc?.eod_cash_cny) || 0) ||
      lastMarketValueCny ||
      0;
    const mv = Number(homeAcc?.eod_market_value_cny) || lastMarketValueCny || 0;
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

  const symbols = holdingsSymbolsFromTrades(trades, scope, pre.lastEodRows);
  if (pre.scopeCleared || symbols.length === 0) {
    return buildLiveFromHomeFrozen({
      tradingDay: true,
      liveDate,
      frozenThrough,
      homeAcc,
      trades,
      cashTransfers,
      accounts,
      scope,
      fxUsdMap,
      fxHkdMap,
      fxUsdFrozen,
      fxHkdFrozen,
      lastMarketValueCny,
    });
  }
  const quoteKeys = [
    ...symbols.map((s) => toTencentQuoteKey(s)).filter(Boolean),
    "whUSDCNY",
    "whHKDCNY",
  ];
  const [closeFallback, quoteReq] = await Promise.all([
    batchLastCloseForSymbols(symbols, liveDate),
    fetchTencentQuotePayloadMap(quoteKeys, QUOTE_TOTAL_BUDGET_MS),
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
      const fb = closeFallback.get(normalizeSymbol(sym)) || closeFallback.get(sym);
      if (fb && Number(fb.close) > 0) {
        const md = String(fb.date || "").slice(0, 10);
        q = {
          current: Number(fb.close),
          prevClose: Number(fb.close),
          marketDate: md || undefined,
          quoteDate: md || undefined,
        };
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
    const todayKey = liveDateKeyShanghai();
    const todayP = todayProfitCnyForHolding({
      quote,
      symbol,
      qty,
      prevClose,
      current,
      rate,
      trades: scoped,
      todayKey,
    });
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

async function getComputeLiveMetrics(userId, accountScope = "all", opts = {}) {
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
  const task = computeLiveMetrics(uid, scope, opts)
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
