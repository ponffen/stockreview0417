/**
 * 个股排行：算法 B — 区间指标来自 symbol_daily_pnl 日序列；划段仍用成交。
 */
const {
  getTrades,
  getSymbolDailyPnl,
  getSymbolNameMap,
  normalizeSymbol,
} = require("../db");
const { resolveStageRange } = require("./stages");
const { liveDateKeyShanghai } = require("./trading-calendar");
const {
  sortTradeAsc,
  countHeldDaysFromPnl,
  resolveEffInterval,
  buildCloseLookup,
  computePeriodMetricsFromPnl,
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

async function buildStockRankPayload({
  userId,
  accountScope,
  stage,
  live,
  publicLayout = false,
  accountProfitCny = null,
  scopeCtx = null,
}) {
  const scope = String(accountScope || "all").trim() || "all";
  const asOf = liveDateKeyShanghai();
  const fxUsd = Number(live.fxUsdCny) || 7.2;
  const fxHkd = Number(live.fxHkdCny) || 0.92;
  const trades = await getTrades(userId);
  const scopeTrades =
    scope === "all" ? trades : trades.filter((t) => String(t.accountId || "default") === scope);
  const firstTrade =
    scopeTrades.length > 0 ? [...scopeTrades].sort(sortTradeAsc)[0].date : asOf;
  const { start: a, end: b } = resolveStageRange(stage, asOf, firstTrade);
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
        symbol: sym,
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
        currency,
        market,
        fxUsd,
        fxHkd,
      });
    }
    const tradeName = String(symbolTrades[0].name || sym).trim();
    rows.push({
      symbol: sym,
      tradeName,
      holdIntervalsLabel,
      profitCny,
      pxChange: m.pxChange,
      heldDays: m.heldDays,
    });
  }

  rows.sort((x, y) => y.profitCny - x.profitCny);
  const nameMap = await getSymbolNameMap(rows.map((r) => r.symbol));
  const accountProfit =
    accountProfitCny != null && Number.isFinite(Number(accountProfitCny)) ? Number(accountProfitCny) : null;
  const rawRows = rows.map((r, i) => {
    const nameCn = String(nameMap[r.symbol] || "").trim();
    const tradeName = String(r.tradeName || "").trim();
    const displayName =
      nameCn ||
      (tradeName && tradeName.toLowerCase() !== r.symbol.toLowerCase() ? tradeName : "") ||
      r.symbol;
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
    periodStart: a,
    periodEnd,
    accountProfitCny: accountProfit,
    rows:
      accountProfit != null
        ? formatStockRankRowsForBundle(rawRows, accountProfit, scopeCtx || {})
        : rawRows,
  };
}

module.exports = {
  buildStockRankPayload,
  formatStockRankRowsForBundle,
  fmtStockRankProfitCny,
  fmtStockRankProfitShare,
};
