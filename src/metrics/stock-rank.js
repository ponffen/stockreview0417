/**
 * 个股排行：冻结 stage_* + 今日 live；划段与天数来自成交。
 */
const { resolveDisplayNameFromMap } = require("../symbol-name-resolve");
const {
  getTrades,
  getSymbolDailyPnl,
  getSymbolDailyPnlRowOnOrBefore,
  getSymbolDailyCloseRange,
  getSymbolNameMap,
  normalizeSymbol,
} = require("../db");
const { resolveStageRange } = require("./stages");
const { liveDateKeyShanghai } = require("./trading-calendar");
const {
  sortTradeAsc,
  countHeldDaysFromPnl,
  resolveEffInterval,
  resolveHoldingSegments,
  isRankEligible,
  heldDaysFromSegments,
  buildCloseLookup,
  computePeriodMetricsFromPnl,
  computeMainRowProfitCny,
  profitNativeToAnalysisCny,
  pxChangeMainRow,
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
  const fxUsdCny = scopeCtx.fxUsdCny ?? 7.2;
  const fxHkdCny = scopeCtx.fxHkdCny ?? 0.92;
  return (rows || []).map((r) => {
    const profitCny = Number(r.profitCny) || 0;
    const profitBook = stockRankRowProfitToBook(profitCny, scope, bookCurrency, fxUsdCny, fxHkdCny);
    const pxChange = Number(r.pxChange);
    const heldDays = Number(r.heldDays) || 0;
    return {
      rank: r.rank,
      symbol: r.symbol,
      name: r.name,
      holdIntervalsLabel: r.holdIntervalsLabel,
      profit: fmtPlainSignedAmount(profitBook),
      pxChange: Number.isFinite(pxChange) ? fmtSignedPercentRatio(pxChange) : "—",
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
}) {
  const scope = String(accountScope || "all").trim() || "all";
  const asOf = String(live.frozenThrough || liveDateKeyShanghai()).slice(0, 10);
  const fxUsd = Number(live.fxUsdCny) || 7.2;
  const fxHkd = Number(live.fxHkdCny) || 0.92;
  const trades = await getTrades(userId);
  const scopeTrades =
    scope === "all" ? trades : trades.filter((t) => String(t.accountId || "default") === scope);
  const firstTrade =
    scopeTrades.length > 0 ? [...scopeTrades].sort(sortTradeAsc)[0].date : asOf;
  const { start: a, end: b } = resolveStageRange(stage, asOf, firstTrade, customRange);
  const periodEnd = live.tradingDay && live.liveDate ? live.liveDate : b;
  const pnlFrom = firstTrade && firstTrade < a ? firstTrade : a;
  const accountIdForPnl = scope === "all" ? "" : scope;
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
}) {
  const scope = String(accountScope || "all").trim() || "all";
  const stageKey = String(stage || "mtd").trim() || "mtd";
  const asOf = String(live.frozenThrough || liveDateKeyShanghai()).slice(0, 10);
  const frozenThrough = asOf;
  const fxUsdEod = Number(scopeCtx?.fxUsdCny) || Number(live.fxUsdCny) || 7.2;
  const fxHkdEod = Number(scopeCtx?.fxHkdCny) || Number(live.fxHkdCny) || 0.92;
  const bookCurrency = scopeCtx?.bookCurrency ?? "CNY";
  const trades = await getTrades(userId);
  const scopeTrades =
    scope === "all" ? trades : trades.filter((t) => String(t.accountId || "default") === scope);
  const firstTrade =
    scopeTrades.length > 0 ? [...scopeTrades].sort(sortTradeAsc)[0].date : asOf;
  const { start: periodStart, end: periodEndRaw } = resolveStageRange(stageKey, asOf, firstTrade, null);
  const periodEnd =
    live.tradingDay && live.liveDate ? String(live.liveDate).slice(0, 10) : String(periodEndRaw).slice(0, 10);

  const accountIdForPnl = scope === "all" ? "all" : scope;
  const liveBySym = new Map((live.positions || []).map((p) => [normalizeSymbol(p.symbol), p]));

  const symSet = new Set(scopeTrades.map((t) => normalizeSymbol(t.symbol)).filter(Boolean));
  const candidates = [];

  for (const sym of symSet) {
    const symbolTrades = scopeTrades
      .filter((t) => normalizeSymbol(t.symbol) === sym)
      .sort(sortTradeAsc);
    if (!symbolTrades.length) {
      continue;
    }
    const segments = resolveHoldingSegments(symbolTrades, periodStart, periodEnd);
    if (!isRankEligible(symbolTrades, segments, periodStart, periodEnd)) {
      continue;
    }
    candidates.push({ sym, symbolTrades, segments });
  }

  const frozenRows = await Promise.all(
    candidates.map(({ sym }) =>
      getSymbolDailyPnlRowOnOrBefore(
        { accountId: accountIdForPnl, symbol: sym, asOf: frozenThrough },
        userId,
      ),
    ),
  );

  const multiSegSyms = candidates.filter((c) => c.segments.length >= 2).map((c) => c.sym);
  let pnlBySym = new Map();
  if (multiSegSyms.length > 0) {
    const allPnlRows = await getSymbolDailyPnl(
      { accountId: accountIdForPnl, from: periodStart, to: periodEnd },
      userId,
    );
    pnlBySym = groupPnlRowsBySymbol(allPnlRows);
  }

  const closeBySym = new Map();
  await Promise.all(
    candidates.map(async ({ sym, segments }) => {
      let closeFrom = periodStart;
      if (segments.length > 0) {
        const segMin = segments.reduce((min, s) => (s.start < min ? s.start : min), segments[0].start);
        closeFrom = segMin < periodStart ? segMin : periodStart;
      }
      const closeRows = await getSymbolDailyCloseRange(sym, closeFrom, periodEnd);
      closeBySym.set(sym, closeRows);
    }),
  );

  const rows = [];

  for (let i = 0; i < candidates.length; i += 1) {
    const { sym, symbolTrades, segments } = candidates[i];
    const frozenRow = frozenRows[i];
    const livePos = liveBySym.get(sym) || null;
    const pnlRows =
      segments.length >= 2 ? symbolPnlForRankScope(pnlBySym, sym, scope) : [];
    const currency = inferSymbolCurrency(
      symbolTrades,
      frozenRow ? [{ currency: frozenRow.currency }] : pnlRows,
    );
    const market = inferMarket(sym);
    const closeRows = closeBySym.get(sym) || [];
    const closeLookup = buildCloseLookup(
      pnlRows,
      livePos,
      live.liveDate,
      live.tradingDay,
      closeRows,
      symbolTrades,
    );

    const profitCny = computeMainRowProfitCny({
      stageKey,
      stageStart: periodStart,
      frozenRow,
      live,
      livePosition: livePos,
      currency,
      market,
      periodEnd,
      frozenThrough,
      fxUsdEod,
      fxHkdEod,
    });
    const heldDays = heldDaysFromSegments(symbolTrades, segments);
    const pxChange = segments.length > 0 ? pxChangeMainRow(symbolTrades, segments, closeLookup) : NaN;

    let holdIntervalsLabel = "";
    if (publicLayout) {
      holdIntervalsLabel = formatHoldingSegmentsLabelPublic({
        symbolTrades,
        periodStart,
        periodEnd,
        closeLookup,
        segments,
      });
    } else {
      holdIntervalsLabel = formatHoldingSegmentsLabel({
        symbolTrades,
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
