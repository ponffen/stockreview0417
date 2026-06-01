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
  addDay,
  countHeldDaysFromPnl,
  resolveEffInterval,
  buildCloseLookup,
  computePeriodMetrics,
  computePeriodMetricsFromPnl,
  profitNativeToAnalysisCny,
  formatHoldingSegmentsLabel,
  formatHoldingSegmentsLabelPublic,
  groupPnlRowsBySymbol,
  symbolPnlForRankScope,
  inferSymbolCurrency,
  inferMarket,
} = require("./stock-rank-period");

async function buildStockRankPayload({ userId, accountScope, stage, live, publicLayout = false }) {
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
  const total = rows.reduce((s, r) => s + r.profitCny, 0);
  return {
    stage,
    periodStart: a,
    periodEnd,
    rows: rows.map((r, i) => {
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
        nameCn,
        holdIntervalsLabel: r.holdIntervalsLabel,
        profitCny: r.profitCny,
        pxChange: r.pxChange,
        heldDays: r.heldDays,
        profitShare: total !== 0 ? r.profitCny / total : 0,
      };
    }),
  };
}

module.exports = { buildStockRankPayload };
