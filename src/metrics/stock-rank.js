/**
 * 个股排行：冻结 stage_* + 今日 live；划段/天数/涨跌来自 symbol_daily_pnl（+ live）。
 */
const { resolveDisplayNameFromMap } = require("../symbol-name-resolve");
const {
  getTrades,
  getSymbolDailyPnl,
  getSymbolDailyPnlRowsOnOrBefore,
  getMinSymbolDailyPnlDateForAccount,
  getSymbolEodCarryBeforeDate,
  getSymbolNameMap,
  normalizeSymbol,
} = require("../db");
const { resolveStageRange } = require("./stages");
const { liveDateKeyShanghai } = require("./trading-calendar");
const { resolveFxRatesCny } = require("./fx-maps");
const { isLastNdStage, lastNdAnchorAsOf } = require("./last-nd");
const {
  sortTradeAsc,
  countHeldDaysFromPnl,
  resolveEffInterval,
  resolveHoldingSegmentsFromPnl,
  appendLivePnlRow,
  isRankEligibleFromPnl,
  heldDaysFromSegmentDates,
  buildCloseLookupFromPnl,
  buildCloseLookup,
  computePeriodMetricsFromPnl,
  computeMainRowProfitCny,
  computeMainRowTradeCount,
  computeMainRowPxChange,
  scopeSymbolTrades,
  countTradeRecordsInRange,
  profitNativeToAnalysisCny,
  formatHoldingSegmentsLabel,
  formatHoldingSegmentsLabelPublic,
  groupPnlRowsBySymbol,
  symbolPnlForRankScope,
  inferSymbolCurrency,
  inferMarket,
} = require("./stock-rank-period");
const {
  fmtPlainAmount,
  fmtPlainSignedAmount,
  fmtPercentRatio,
  fmtSignedPercentRatio,
} = require("../account-kpi-surface");
const { liveCnyToBookAmount, isAggregateScope } = require("./account-book-metrics");

/** 排行 profitCny 由 profitNativeToAnalysisCny 算出，恒为人民币；单账户展示需换到记账币。 */
function stockRankRowProfitToBook(profitCny, scope, bookCurrency, fxUsdCny, fxHkdCny) {
  if (isAggregateScope(scope)) {
    return Number(profitCny) || 0;
  }
  return liveCnyToBookAmount(profitCny, bookCurrency, fxUsdCny, fxHkdCny);
}

function fmtStockRankProfitCny(profitCny) {
  const v = Number(profitCny);
  if (!Number.isFinite(v)) {
    return "—";
  }
  const sign = v > 0 ? "+" : v < 0 ? "-" : "";
  return `${sign}¥${fmtPlainAmount(Math.abs(v))}`;
}

function fmtStockRankProfitShare(profitBook, accountProfitBook) {
  const num = Number(profitBook);
  const den = Number(accountProfitBook);
  if (!Number.isFinite(num) || !Number.isFinite(den) || Math.abs(den) < 1e-9) {
    return "—";
  }
  return fmtPercentRatio(num / den);
}

function fmtStockRankHeldDays(heldDays) {
  const d = Math.max(0, Math.floor(Number(heldDays) || 0));
  return `${d}天`;
}

function fmtStockRankTradeCount(tradeCount) {
  const n = Math.max(0, Math.floor(Number(tradeCount) || 0));
  return `${n}笔`;
}

function profitToneFromCny(profitCny) {
  const v = Number(profitCny);
  if (!Number.isFinite(v) || Math.abs(v) < 1e-9) {
    return "";
  }
  return v > 0 ? "up" : "down";
}

/** analysis-bundle：排行行输出为前端可直接展示的字符串 */
function formatStockRankRowsForBundle(rows, accountProfitCny, scopeCtx = {}) {
  const accountProfit = Number(accountProfitCny);
  const scope = scopeCtx.scope ?? "all";
  const bookCurrency = scopeCtx.bookCurrency ?? "CNY";
  const fxUsdCny = Number(scopeCtx.fxUsdCny) || 0;
  const fxHkdCny = Number(scopeCtx.fxHkdCny) || 0;
  return (rows || []).map((r) => {
    const profitCny = Number(r.profitCny) || 0;
    const profitBook = stockRankRowProfitToBook(profitCny, scope, bookCurrency, fxUsdCny, fxHkdCny);
    const pxChange = Number(r.pxChange);
    const heldDays = Number(r.heldDays) || 0;
    const tradeCount = Number(r.tradeCount) || 0;
    return {
      rank: r.rank,
      symbol: r.symbol,
      name: r.name,
      holdIntervalsLabel: r.holdIntervalsLabel,
      profit: fmtPlainSignedAmount(profitBook),
      pxChange: Number.isFinite(pxChange) ? fmtSignedPercentRatio(pxChange) : "—",
      tradeCount: fmtStockRankTradeCount(tradeCount),
      heldDays: fmtStockRankHeldDays(heldDays),
      profitShare: fmtStockRankProfitShare(profitBook, accountProfit),
      profitTone: profitToneFromCny(profitBook),
      pxTone: Number.isFinite(pxChange) ? profitToneFromCny(pxChange) : "",
    };
  });
}

/** custom 区间：维持现网日序列算法。 */
async function buildStockRankPayloadLegacy({
  userId,
  accountScope,
  stage,
  live,
  publicLayout = false,
  accountProfitCny = null,
  scopeCtx = null,
  customRange = null,
  preloadedTrades = null,
  firstTradeDate = null,
}) {
  const scope = String(accountScope || "all").trim() || "all";
  const asOf = String(live.frozenThrough || liveDateKeyShanghai()).slice(0, 10);
  const fxResolved = await resolveFxRatesCny({
    dateKey: live.tradingDay ? live.liveDate || asOf : asOf,
    snapshotUsd: scopeCtx?.fxUsdCny,
    snapshotHkd: scopeCtx?.fxHkdCny,
    liveSpot: { USD: live.fxUsdCny, HKD: live.fxHkdCny },
    preferLiveSpot: !!live.tradingDay,
  });
  const fxUsd = fxResolved.fxUsdCny;
  const fxHkd = fxResolved.fxHkdCny;
  const trades = preloadedTrades ?? (await getTrades(userId));
  const scopeTrades =
    scope === "all" ? trades : trades.filter((t) => String(t.accountId || "default") === scope);
  let firstTrade = asOf;
  if (firstTradeDate) {
    firstTrade = String(firstTradeDate).slice(0, 10);
  } else if (scopeTrades.length > 0) {
    firstTrade = [...scopeTrades].sort(sortTradeAsc)[0].date;
  }
  const { start: a, end: b } = resolveStageRange(stage, asOf, firstTrade, customRange);
  const periodEnd = live.tradingDay && live.liveDate ? live.liveDate : b;
  const pnlFrom = firstTrade && firstTrade < a ? firstTrade : a;
  const accountIdForPnl = scope === "all" ? "all" : scope;
  const allPnlRows = await getSymbolDailyPnl(
    { accountId: accountIdForPnl, from: pnlFrom, to: periodEnd },
    userId,
  );
  const pnlBySym = groupPnlRowsBySymbol(allPnlRows);

  const symSet = new Set(scopeTrades.map((t) => normalizeSymbol(t.symbol)).filter(Boolean));
  const rows = [];

  for (const sym of symSet) {
    const symbolTrades = scopeTrades
      .filter((t) => normalizeSymbol(t.symbol) === sym)
      .sort(sortTradeAsc);
    if (!symbolTrades.length) {
      continue;
    }
    const pnlRows = symbolPnlForRankScope(pnlBySym, sym, scope);
    if (countHeldDaysFromPnl(pnlRows, a, periodEnd) < 1) {
      continue;
    }
    const { effStart, effEnd } = resolveEffInterval(symbolTrades, a, periodEnd);
    if (effStart > effEnd) {
      continue;
    }
    const currency = inferSymbolCurrency(symbolTrades, pnlRows);
    const market = inferMarket(sym);
    const m = computePeriodMetricsFromPnl({
      pnlRows,
      symbolTrades,
      startKey: effStart,
      endKey: effEnd,
    });
    const profitCny = profitNativeToAnalysisCny(m.profitNative, currency, market, fxUsd, fxHkd);
    let holdIntervalsLabel = "";
    if (publicLayout) {
      const closeLookup = buildCloseLookup(pnlRows, null, live.liveDate, live.tradingDay, [], symbolTrades);
      holdIntervalsLabel = formatHoldingSegmentsLabelPublic({
        symbolTrades,
        periodStart: a,
        periodEnd,
        closeLookup,
      });
    } else {
      holdIntervalsLabel = formatHoldingSegmentsLabel({
        symbolTrades,
        periodStart: a,
        periodEnd,
        pnlRows,
        closeLookup: buildCloseLookup(pnlRows, null, live.liveDate, live.tradingDay, [], symbolTrades),
        currency,
        market,
        scope,
        bookCurrency: scopeCtx?.bookCurrency ?? "CNY",
        fxUsd,
        fxHkd,
        frozenThrough: asOf,
        live,
        livePosition: null,
      });
    }
    rows.push({
      symbol: sym,
      holdIntervalsLabel,
      profitCny,
      pxChange: m.pxChange,
      tradeCount: countTradeRecordsInRange(symbolTrades, a, periodEnd),
      heldDays: m.heldDays,
    });
  }

  return finalizeStockRankPayload({
    stage,
    periodStart: a,
    periodEnd,
    accountProfitCny,
    scopeCtx,
    rows,
    userId,
  });
}

async function buildStockRankPayloadV3({
  userId,
  accountScope,
  stage,
  live,
  publicLayout = false,
  accountProfitCny = null,
  scopeCtx = null,
  preloadedTrades = null,
}) {
  const scope = String(accountScope || "all").trim() || "all";
  const stageKey = String(stage || "mtd").trim() || "mtd";
  const asOf = String(live.frozenThrough || liveDateKeyShanghai()).slice(0, 10);
  const frozenThrough = asOf;
  const fxResolved = await resolveFxRatesCny({
    dateKey: live.tradingDay ? live.liveDate || asOf : asOf,
    snapshotUsd: scopeCtx?.fxUsdCny,
    snapshotHkd: scopeCtx?.fxHkdCny,
    liveSpot: { USD: live.fxUsdCny, HKD: live.fxHkdCny },
    preferLiveSpot: !!live.tradingDay,
  });
  const fxUsdEod = fxResolved.fxUsdCny;
  const fxHkdEod = fxResolved.fxHkdCny;
  const bookCurrency = scopeCtx?.bookCurrency ?? "CNY";
  const accountIdForPnl = scope === "all" ? "all" : scope;

  let firstDataDate = asOf;
  if (stageKey === "inception") {
    firstDataDate =
      (await getMinSymbolDailyPnlDateForAccount({ accountId: accountIdForPnl }, userId)) || asOf;
  }

  const { start: periodStart, end: periodEndRaw } = resolveStageRange(stageKey, asOf, firstDataDate, null);
  const periodEnd =
    live.tradingDay && live.liveDate ? String(live.liveDate).slice(0, 10) : String(periodEndRaw).slice(0, 10);

  const liveBySym = new Map((live.positions || []).map((p) => [normalizeSymbol(p.symbol), p]));

  const [allPnlRows, carryRows] = await Promise.all([
    getSymbolDailyPnl({ accountId: accountIdForPnl, from: periodStart, to: periodEnd }, userId),
    getSymbolEodCarryBeforeDate(userId, accountIdForPnl, periodStart),
  ]);
  const pnlBySym = groupPnlRowsBySymbol(allPnlRows);
  const carryBySym = new Map(
    (carryRows || []).map((r) => [normalizeSymbol(r.symbol), Number(r.eodShares) || 0]),
  );

  const symSet = new Set(pnlBySym.keys());
  for (const p of live.positions || []) {
    const s = normalizeSymbol(p.symbol);
    if (s) {
      symSet.add(s);
    }
  }

  const candidates = [];
  for (const sym of symSet) {
    const pnlRows = symbolPnlForRankScope(pnlBySym, sym, scope);
    const livePos = liveBySym.get(sym) || null;
    const pnlWithLive = appendLivePnlRow(pnlRows, livePos, live.liveDate, live.tradingDay, periodEnd);
    const carryEod = carryBySym.get(sym) || 0;
    const segments = resolveHoldingSegmentsFromPnl(pnlWithLive, carryEod, periodStart, periodEnd);
    if (!isRankEligibleFromPnl(pnlWithLive, segments, livePos, periodStart, periodEnd)) {
      continue;
    }
    candidates.push({ sym, pnlRows: pnlWithLive, segments, livePos });
  }

  const candidateSyms = candidates.map(({ sym }) => sym);
  const anchorAsOf = isLastNdStage(stageKey) ? lastNdAnchorAsOf(periodStart) : "";
  const [frozenRowBySym, anchorRowBySym] = await Promise.all([
    candidateSyms.length > 0
      ? getSymbolDailyPnlRowsOnOrBefore(
          { accountId: accountIdForPnl, symbols: candidateSyms, asOf: frozenThrough },
          userId,
        )
      : Promise.resolve(new Map()),
    anchorAsOf && candidateSyms.length > 0
      ? getSymbolDailyPnlRowsOnOrBefore(
          { accountId: accountIdForPnl, symbols: candidateSyms, asOf: anchorAsOf },
          userId,
        )
      : Promise.resolve(new Map()),
  ]);

  const rows = [];

  for (const { sym, pnlRows, segments, livePos } of candidates) {
    const frozenRow = frozenRowBySym.get(sym) || null;
    const anchorRow = anchorRowBySym.get(sym) || null;
    const currency = inferSymbolCurrency([], frozenRow ? [{ currency: frozenRow.currency }] : pnlRows);
    const market = inferMarket(sym);
    const closeLookup = buildCloseLookupFromPnl(pnlRows, livePos, live.liveDate, live.tradingDay);

    const profitCny = computeMainRowProfitCny({
      stageKey,
      stageStart: periodStart,
      frozenRow,
      anchorRow,
      live,
      livePosition: livePos,
      currency,
      market,
      periodEnd,
      frozenThrough,
      fxUsdEod,
      fxHkdEod,
    });
    const heldDays = heldDaysFromSegmentDates(segments);
    const pxChange = computeMainRowPxChange({
      stageKey,
      frozenRow,
      anchorRow,
      segments,
      closeLookup,
    });
    const symbolTrades = scopeSymbolTrades(preloadedTrades, scope, sym);
    const tradeCount = computeMainRowTradeCount({
      stageKey,
      stageStart: periodStart,
      frozenRow,
      live,
      symbolTrades,
      periodStart,
      periodEnd,
      frozenThrough,
    });

    let holdIntervalsLabel = "";
    if (publicLayout) {
      holdIntervalsLabel = formatHoldingSegmentsLabelPublic({
        symbolTrades: [],
        periodStart,
        periodEnd,
        closeLookup,
        segments,
      });
    } else {
      holdIntervalsLabel = formatHoldingSegmentsLabel({
        symbolTrades: [],
        periodStart,
        periodEnd,
        pnlRows,
        closeLookup,
        currency,
        market,
        scope,
        bookCurrency,
        fxUsd: fxUsdEod,
        fxHkd: fxHkdEod,
        frozenThrough,
        live,
        livePosition: livePos,
        segments,
      });
    }

    rows.push({
      symbol: sym,
      holdIntervalsLabel,
      profitCny,
      pxChange,
      tradeCount,
      heldDays,
    });
  }

  return finalizeStockRankPayload({
    stage: stageKey,
    periodStart,
    periodEnd,
    accountProfitCny,
    scopeCtx,
    rows,
    userId,
  });
}

async function finalizeStockRankPayload({
  stage,
  periodStart,
  periodEnd,
  accountProfitCny,
  scopeCtx,
  rows,
  userId,
}) {
  rows.sort((x, y) => y.profitCny - x.profitCny);
  const nameMap = await getSymbolNameMap(rows.map((r) => r.symbol));
  const accountProfit =
    accountProfitCny != null && Number.isFinite(Number(accountProfitCny)) ? Number(accountProfitCny) : null;
  const rawRows = rows.map((r, i) => {
    const displayName = resolveDisplayNameFromMap(r.symbol, nameMap);
    return {
      rank: i + 1,
      symbol: r.symbol,
      name: displayName,
      holdIntervalsLabel: r.holdIntervalsLabel,
      profitCny: r.profitCny,
      pxChange: r.pxChange,
      tradeCount: r.tradeCount,
      heldDays: r.heldDays,
    };
  });
  return {
    stage,
    periodStart,
    periodEnd,
    accountProfitCny: accountProfit,
    rows:
      accountProfit != null
        ? formatStockRankRowsForBundle(rawRows, accountProfit, scopeCtx || {})
        : rawRows,
  };
}

async function buildStockRankPayload(params) {
  const stageKey = String(params.stage || "mtd").trim() || "mtd";
  if (stageKey === "custom") {
    return buildStockRankPayloadLegacy(params);
  }
  return buildStockRankPayloadV3(params);
}

module.exports = {
  buildStockRankPayload,
  formatStockRankRowsForBundle,
  fmtStockRankProfitCny,
  fmtStockRankProfitShare,
};
