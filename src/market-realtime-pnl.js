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
const { computeLedgerCashBookUpToDate, principalBookUpToDate, bookCurrencyForScope } = require("./ledger-metrics");
const { applyEodPlusLiveTotals, resolveAccountTodayProfitCny } = require("./metrics/snapshot-plus-live");
const { pickLatestQuoteTime, isExtendedQuoteSession } = require("./quotes/quote-common");
const { previousSessionDate } = require("./metrics/freeze-calendar");
const {
  fetchQuoteMap,
  fetchTencentForexMap,
  fetchTencentQuotePayloadMap,
  toTencentQuoteKey,
} = require("./quotes/realtime-quote");
const { shouldEmitTodayLivePoint, liveDateKeyShanghai } = require("./metrics/trading-calendar");
const {
  holdingsSymbolsForLiveMetrics,
  aggregateFrozenEodBySymbol,
  currentQuantityFromFrozenEod,
  frozenMvNatForSymbol,
  hasOpenPositionQuantity,
  wasClearedOnTradingDay,
} = require("./metrics/holdings-active-symbols");
const { computeTodayProfitTracksForHolding } = require("./position-today-pnl");
const { loadFxRatesOnDate } = require("./metrics/fx-maps");

function resolvePreloadedAccounts(pre) {
  if (Array.isArray(pre?.accounts) && pre.accounts.length > 0) {
    return pre.accounts;
  }
  return null;
}
const QUOTE_TOTAL_BUDGET_MS = Math.max(
  2000,
  Math.min(12_000, Number(process.env.QUOTE_TOTAL_BUDGET_MS || 7000)),
);
const LIVE_METRICS_DEDUP_MS = Math.max(
  1000,
  Math.min(8000, Number(process.env.LIVE_METRICS_DEDUP_MS || 4000)),
);
const liveMetricsInflight = new Map();
const liveMetricsRecent = new Map();

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

/** 个股原币金额 → 当前 scope 计价币（全部账户=人民币，单账户=记账币）。 */
function rateSymbolToBook(symCcy, bookCcy, fxSpot) {
  const sym = String(symCcy || "CNY").toUpperCase();
  const book = String(bookCcy || "CNY").toUpperCase();
  if (sym === book) {
    return 1;
  }
  const symCny = sym === "CNY" ? 1 : sym === "USD" ? fxSpot.USD : sym === "HKD" ? fxSpot.HKD : 1;
  const bookCny = book === "CNY" ? 1 : book === "USD" ? fxSpot.USD : book === "HKD" ? fxSpot.HKD : 1;
  return bookCny > 0 ? symCny / bookCny : symCny;
}

/** 冻结日市值：0 为有效值，勿用 total_assets 或 lastMarketValue 顶替。 */
function frozenMarketValueCnyFromHome(homeAcc, lastMarketValueCny = 0) {
  const eod = homeAcc?.eod_market_value_cny ?? homeAcc?.eodMarketValueCny;
  if (eod != null && Number.isFinite(Number(eod))) {
    return Number(eod);
  }
  const last = Number(lastMarketValueCny);
  return Number.isFinite(last) && last > 0 ? last : 0;
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
  const cashBook = computeLedgerCashBookUpToDate(trades, cashTransfers, accounts, scope, fxUsdMap, fxHkdMap, asOf);
  const principalBook = principalBookUpToDate(cashTransfers, accounts, scope, fxUsdMap, fxHkdMap, asOf);
  const mv = frozenMarketValueCnyFromHome(homeAcc, lastMarketValueCny);
  let cashUse = Number(homeAcc?.eod_cash_cny) || cashBook;
  let totalAssetsCny =
    Number(homeAcc?.eod_total_assets_cny) ||
    (Number.isFinite(mv) ? mv : 0) + (Number.isFinite(cashUse) ? cashUse : 0) ||
    0;
  let liveMv = mv;
  let principalLive = Number(homeAcc?.eod_principal_cny) || principalBook;

  if (tradingDay) {
    const hybrid = applyEodPlusLiveTotals({
      homeAcc,
      frozenThrough,
      liveDate,
      liveMarketValueCny: mv,
      ledgerCashAtLive: cashBook,
      trades,
      cashTransfers,
      accounts,
      scope,
      fxUsdMap,
      fxHkdMap,
    });
    if (hybrid) {
      liveMv = hybrid.liveMarketValueCny;
      cashUse = hybrid.cashCny;
      totalAssetsCny = hybrid.totalAssetsCny;
      const eodPrincipal = Number(homeAcc?.eod_principal_cny) || 0;
      principalLive =
        eodPrincipal > 0
          ? eodPrincipal + hybrid.externalFlowTodayCny
          : principalBookUpToDate(cashTransfers, accounts, scope, fxUsdMap, fxHkdMap, liveDate);
    } else if (frozenThrough) {
      const cashFrozen = computeLedgerCashBookUpToDate(
        trades,
        cashTransfers,
        accounts,
        scope,
        fxUsdMap,
        fxHkdMap,
        frozenThrough,
      );
      const eodCash = Number(homeAcc?.eod_cash_cny) || 0;
      cashUse = eodCash + (cashBook - cashFrozen);
      totalAssetsCny = liveMv + cashUse;
    }
  }

  if (!(totalAssetsCny > 0)) {
    totalAssetsCny = liveMv + cashUse;
  }
  return {
    tradingDay: !!tradingDay,
    liveDate: tradingDay ? liveDate : null,
    frozenThrough: frozenThrough || null,
    delayed: false,
    quoteTime: null,
    todayProfitCny: 0,
    liveMarketValueCny: liveMv,
    lastMarketValueCny: mv,
    cashCny: cashUse,
    totalAssetsCny,
    cashRatio: totalAssetsCny > 0 ? cashUse / totalAssetsCny : 0,
    principalCny: principalLive,
    positions: [],
    fxUsdCny: Number(fxUsdFrozen) || 0,
    fxHkdCny: Number(fxHkdFrozen) || 0,
    clearedScope: true,
  };
}

function liveMetricsCacheKey(userId, scope, opts = {}) {
  const uid = String(userId || "").trim();
  const scopeNorm = String(scope || "all").trim() || "all";
  const variant = opts?.preloaded?.homeAccount ? "pack" : "bare";
  return `${uid}|${scopeNorm}|${variant}`;
}

async function computeLiveMetrics(userId, accountScope = "all", opts = {}) {
  const uid = String(userId || "").trim();
  const scope = String(accountScope || "all").trim() || "all";
  const now = new Date();
  const tradingDay = shouldEmitTodayLivePoint(now);
  const liveDate = liveDateKeyShanghai(now);
  const homeAcc = opts?.preloaded?.homeAccount || null;
  let frozenThrough = String(homeAcc?.frozen_through || homeAcc?.frozenThrough || "").slice(0, 10) || null;
  if (!frozenThrough) {
    frozenThrough = await getLatestAnalysisSnapshotDate(uid, scope);
  }
  const frozenFx = frozenThrough ? await loadFxRatesOnDate(frozenThrough) : { USD: 0, HKD: 0 };
  let fxUsdFrozen = Number(frozenFx.USD) || 0;
  let fxHkdFrozen = Number(frozenFx.HKD) || 0;
  let lastMarketValueCny = Number(homeAcc?.last_market_value_cny) || Number(homeAcc?.eod_market_value_cny) || 0;
  if (!(lastMarketValueCny > 0) && frozenThrough) {
    const baseRows = await getAnalysisDailySnapshots({ accountId: scope, from: frozenThrough, to: frozenThrough }, uid);
    const lastSnap = baseRows[baseRows.length - 1] || null;
    lastMarketValueCny = Number(lastSnap?.marketValue ?? lastSnap?.market_value) || 0;
  }
  const frozenDateKeyStored = String(frozenThrough || liveDate).slice(0, 10);
  const fxUsdMap =
    frozenDateKeyStored && fxUsdFrozen > 0 ? { [frozenDateKeyStored]: fxUsdFrozen } : {};
  const fxHkdMap =
    frozenDateKeyStored && fxHkdFrozen > 0 ? { [frozenDateKeyStored]: fxHkdFrozen } : {};

  const pre = opts?.preloaded || {};
  const preloadedAccounts = resolvePreloadedAccounts(pre);
  const [trades, cashTransfers, accounts] = await Promise.all([
    pre.trades ? Promise.resolve(pre.trades) : getTrades(uid),
    pre.cashTransfers ? Promise.resolve(pre.cashTransfers) : getCashTransfers(uid),
    preloadedAccounts ? Promise.resolve(preloadedAccounts) : getAccounts(uid),
  ]);

  const priceAsOf = tradingDay ? liveDate : frozenThrough || liveDate;
  const frozenDate = String(frozenThrough || "").slice(0, 10);
  const frozenEodRows = Array.isArray(pre.frozenSymbolEodRows) ? pre.frozenSymbolEodRows : [];
  const frozenBySym = aggregateFrozenEodBySymbol(frozenEodRows, scope, frozenDate);
  const todayKey = tradingDay ? liveDate : frozenDate;
  const symbols = holdingsSymbolsForLiveMetrics(
    trades,
    scope,
    pre.lastEodRows,
    frozenBySym,
    todayKey,
    tradingDay,
  );
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
  const prevSessionKey = tradingDay ? previousSessionDate(todayKey) : null;
  const [closeFallback, prevSessionClose, quoteReq, fxReq] = await Promise.all([
    batchLastCloseForSymbols(symbols, priceAsOf),
    prevSessionKey ? batchLastCloseForSymbols(symbols, prevSessionKey) : Promise.resolve(new Map()),
    fetchQuoteMap(symbols, { budgetMs: QUOTE_TOTAL_BUDGET_MS }),
    fetchTencentForexMap(QUOTE_TOTAL_BUDGET_MS),
  ]);
  const fxSpot = { CNY: 1, USD: 0, HKD: 0 };
  if (fxReq?.rates?.USD > 0) {
    fxSpot.USD = fxReq.rates.USD;
  }
  if (fxReq?.rates?.HKD > 0) {
    fxSpot.HKD = fxReq.rates.HKD;
  }
  if (tradingDay && (!(fxSpot.USD > 0) || !(fxSpot.HKD > 0))) {
    const dbLiveFx = await loadFxRatesOnDate(liveDate);
    if (!(fxSpot.USD > 0)) {
      fxSpot.USD = dbLiveFx.USD;
    }
    if (!(fxSpot.HKD > 0)) {
      fxSpot.HKD = dbLiveFx.HKD;
    }
  }
  const bookCcy = bookCurrencyForScope(accounts, scope);
  let quoteDelayed = !!quoteReq.delayed || !!fxReq.delayed;
  const quoteSource = quoteReq.quoteSource || "";
  const quoteError = String(quoteReq.quoteError || quoteReq.error || "").trim() || null;

  const quoteMap = {};
  for (const sym of symbols) {
    const norm = normalizeSymbol(sym);
    let q = quoteReq.map?.get(norm) || quoteReq.map?.get(sym);
    if (!q) {
      const fb = closeFallback.get(norm) || closeFallback.get(sym);
      if (fb && Number(fb.close) > 0) {
        const md = String(fb.date || "").slice(0, 10);
        q = {
          current: Number(fb.close),
          prevClose: Number(fb.close),
          marketDate: md || undefined,
          quoteDate: md || undefined,
          source: "close-fallback",
        };
        if (!quoteReq.ok) {
          quoteDelayed = true;
        }
      }
    }
    if (q) {
      quoteMap[sym] = q;
    }
  }

  const scoped =
    scope === "all" ? trades : trades.filter((t) => String(t.accountId || "default") === scope);

  let liveMarketValue = 0;
  const positions = [];
  for (const symbol of symbols) {
    const qty = currentQuantityFromFrozenEod(
      frozenBySym,
      trades,
      symbol,
      todayKey,
      scope,
      tradingDay,
    );
    const clearedToday =
      tradingDay &&
      wasClearedOnTradingDay(frozenBySym, trades, symbol, todayKey, scope, tradingDay);
    if (!hasOpenPositionQuantity(qty) && !clearedToday) {
      continue;
    }
    let quote = quoteMap[symbol];
    if (!quote) {
      const fb = closeFallback.get(normalizeSymbol(symbol)) || closeFallback.get(symbol);
      if (fb && Number(fb.close) > 0) {
        const md = String(fb.date || priceAsOf).slice(0, 10);
        quote = {
          current: Number(fb.close),
          prevClose: Number(fb.close),
          marketDate: md || undefined,
          quoteDate: md || undefined,
        };
        if (!quoteReq.ok) {
          quoteDelayed = true;
        }
      }
    }
    if (!quote) {
      if (!clearedToday) {
        continue;
      }
      quote = { current: 0, prevClose: 0, marketDate: todayKey, quoteDate: todayKey };
    }
    const current = Number(quote.current) || 0;
    let prevClose = Number(quote.prevClose) || current;
    if (isExtendedQuoteSession(quote)) {
      const psc =
        prevSessionClose.get(normalizeSymbol(symbol)) || prevSessionClose.get(symbol);
      if (psc && Number(psc.close) > 0) {
        prevClose = Number(psc.close);
      }
    }
    const symCcy = getSymbolCurrency(symbol);
    const rateToBook = rateSymbolToBook(symCcy, bookCcy, fxSpot);
    const mvNat = hasOpenPositionQuantity(qty) ? qty * current : 0;
    const mv = mvNat * rateToBook;
    liveMarketValue += mv;
    const frozenDateKey = String(frozenThrough || prevSessionKey || todayKey).slice(0, 10);
    const tracks = tradingDay
      ? computeTodayProfitTracksForHolding({
          quote,
          symbol,
          prevClose,
          current,
          trades: scoped,
          todayKey,
          frozenDate: prevSessionKey || frozenDateKey,
          now: new Date(),
          frozenMvNat: frozenMvNatForSymbol(frozenBySym, symbol),
          endQuantity: qty,
          ccy: symCcy,
          book: bookCcy,
          fxLive: { USD: fxSpot.USD, HKD: fxSpot.HKD },
          fxFrozen: {
            USD: Number(fxUsdFrozen) || Number(fxUsdMap[frozenDateKey]) || 0,
            HKD: Number(fxHkdFrozen) || Number(fxHkdMap[frozenDateKey]) || 0,
          },
          clearedToday,
        })
      : { native: { profit: 0 }, book: { profit: 0 }, cny: { profit: 0 } };
    const todayNat = Number(tracks.native?.profit) || 0;
    const todayBook = Number(tracks.book?.profit) || 0;
    const todayP = Number(tracks.cny?.profit) || 0;
    positions.push({
      symbol,
      quantity: qty,
      current,
      prevClose,
      currency: symCcy,
      session: quote.session || null,
      sessionLabel: quote.sessionLabel || null,
      todayProfitNative: todayNat,
      todayProfitBook: todayBook,
      todayProfitCny: todayP,
      marketValueCny: mv,
    });
  }

  const cashAsOf = tradingDay ? liveDate : frozenThrough || liveDate;
  // fxUsdMap/fxHkdMap 只含冻结日一条，缺 liveDate；交易日按实时汇率补上 liveDate，
  // 否则 cashAsOf=liveDate 时外币现金会用 7.2/0.92 兜底汇率换算，虚增 live 现金/总资产。
  const fxUsdMapLive = tradingDay ? { ...fxUsdMap, [String(liveDate)]: fxSpot.USD } : fxUsdMap;
  const fxHkdMapLive = tradingDay ? { ...fxHkdMap, [String(liveDate)]: fxSpot.HKD } : fxHkdMap;
  const ledgerCashAtLive = computeLedgerCashBookUpToDate(
    trades,
    cashTransfers,
    accounts,
    scope,
    fxUsdMapLive,
    fxHkdMapLive,
    cashAsOf,
  );
  const principalBase = principalBookUpToDate(cashTransfers, accounts, scope, fxUsdMapLive, fxHkdMapLive, cashAsOf);
  let cashCny = ledgerCashAtLive;
  let liveMarketValueCny = liveMarketValue;
  let totalAssetsCny = liveMarketValue + cashCny;
  let cashRatio = totalAssetsCny > 0 ? cashCny / totalAssetsCny : 0;
  let eodTotalAssetsCny = Number(homeAcc?.eod_total_assets_cny) || 0;
  let externalFlowTodayCny = 0;
  let todayProfitCny = 0;
  let principalLive = principalBase;

  if (tradingDay) {
    const hybrid = applyEodPlusLiveTotals({
      homeAcc,
      frozenThrough,
      liveDate,
      liveMarketValueCny: liveMarketValue,
      ledgerCashAtLive,
      trades,
      cashTransfers,
      accounts,
      scope,
      fxUsdMap: fxUsdMapLive,
      fxHkdMap: fxHkdMapLive,
    });
    if (hybrid) {
      liveMarketValueCny = hybrid.liveMarketValueCny;
      cashCny = hybrid.cashCny;
      totalAssetsCny = hybrid.totalAssetsCny;
      cashRatio = hybrid.cashRatio;
      eodTotalAssetsCny = hybrid.eodTotalAssetsCny;
      externalFlowTodayCny = hybrid.externalFlowTodayCny;
    }

    const liveForToday = {
      tradingDay: true,
      eodTotalAssetsCny,
      totalAssetsCny,
      externalFlowTodayCny,
    };
    todayProfitCny = resolveAccountTodayProfitCny(liveForToday, positions, quoteMap);
    const eodPrincipal = Number(homeAcc?.eod_principal_cny) || 0;
    principalLive =
      eodPrincipal > 0
        ? eodPrincipal + externalFlowTodayCny
        : principalBookUpToDate(cashTransfers, accounts, scope, fxUsdMapLive, fxHkdMapLive, liveDate);

    return {
      tradingDay: true,
      liveDate,
      frozenThrough: frozenThrough || null,
      bookCurrency: bookCcy,
      delayed: quoteDelayed,
      quoteSource,
      quoteError,
      quoteTime: pickLatestQuoteTime([
        fxReq?.quoteTime,
        ...Object.values(quoteMap).map((q) => q?.time),
      ]),
      todayProfitCny,
      liveMarketValueCny,
      lastMarketValueCny,
      cashCny,
      totalAssetsCny,
      cashRatio,
      principalCny: principalLive,
      eodTotalAssetsCny,
      externalFlowTodayCny,
      positions,
      fxUsdCny: fxSpot.USD,
      fxHkdCny: fxSpot.HKD,
    };
  }

  const mvFrozen = frozenMarketValueCnyFromHome(homeAcc, lastMarketValueCny);
  liveMarketValueCny = liveMarketValue > 0 ? liveMarketValue : mvFrozen;
  const taFrozen =
    Number(homeAcc?.eod_total_assets_cny) ||
    mvFrozen + (Number(homeAcc?.eod_cash_cny) || 0) ||
    0;
  cashCny = Number(homeAcc?.eod_cash_cny) > 0 ? Number(homeAcc.eod_cash_cny) : cashCny;
  totalAssetsCny = taFrozen > 0 ? taFrozen : liveMarketValueCny + cashCny;
  cashRatio = totalAssetsCny > 0 ? cashCny / totalAssetsCny : 0;
  principalLive = Number(homeAcc?.eod_principal_cny) || principalBase;

  return {
    tradingDay: false,
    liveDate: null,
    frozenThrough: frozenThrough || null,
    bookCurrency: bookCcy,
    delayed: quoteDelayed,
    quoteSource,
    quoteError,
    quoteTime: pickLatestQuoteTime([
      fxReq?.quoteTime,
      ...Object.values(quoteMap).map((q) => q?.time),
    ]),
    todayProfitCny: 0,
    liveMarketValueCny,
    lastMarketValueCny: mvFrozen,
    cashCny,
    totalAssetsCny,
    cashRatio,
    principalCny: principalLive,
    eodTotalAssetsCny: taFrozen,
    externalFlowTodayCny: 0,
    positions,
    fxUsdCny: fxSpot.USD,
    fxHkdCny: fxSpot.HKD,
  };
}

async function getComputeLiveMetrics(userId, accountScope = "all", opts = {}) {
  const uid = String(userId || "").trim();
  const scope = String(accountScope || "all").trim() || "all";
  const key = liveMetricsCacheKey(uid, scope, opts);
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
