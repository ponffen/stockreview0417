/**
 * 个股排行：与分析 tab 同一 stage 窗口；服务端计算，前端只展示。
 */
const { getTrades, getSymbolDailyPnl, normalizeSymbol } = require("../db");
const { resolveStageRange } = require("./stages");
const {
  sortTradeAsc,
  countHeldDaysInRange,
  resolveEffInterval,
  buildCloseLookup,
  computePeriodMetrics,
  profitNativeToAnalysisCny,
  formatHoldingSegmentsLabel,
  formatHoldingSegmentsLabelPublic,
  groupPnlRowsBySymbol,
  inferSymbolCurrency,
  inferMarket,
} = require("./stock-rank-period");

async function buildStockRankPayload({ userId, accountScope, stage, live, publicLayout = false }) {
  const scope = String(accountScope || "all").trim() || "all";
  const asOf = live.frozenThrough || live.liveDate || "";
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
  const liveBySym = new Map((live.positions || []).map((p) => [normalizeSymbol(p.symbol), p]));

  const symSet = new Set(scopeTrades.map((t) => normalizeSymbol(t.symbol)).filter(Boolean));
  const rows = [];
  for (const sym of symSet) {
    const symbolTrades = scopeTrades
      .filter((t) => normalizeSymbol(t.symbol) === sym)
      .sort(sortTradeAsc);
    if (!symbolTrades.length) {
      continue;
    }
    if (countHeldDaysInRange(symbolTrades, a, periodEnd) < 1) {
      continue;
    }
    const { effStart, effEnd } = resolveEffInterval(symbolTrades, a, periodEnd);
    if (effStart > effEnd) {
      continue;
    }
    const pnlRows = pnlBySym.get(sym) || [];
    const livePos = liveBySym.get(sym) || null;
    const closeLookup = buildCloseLookup(pnlRows, livePos, live.liveDate, live.tradingDay);
    const currency = inferSymbolCurrency(symbolTrades, pnlRows);
    const market = inferMarket(sym);
    const m = computePeriodMetrics({
      symbol: sym,
      symbolTrades,
      startKey: effStart,
      endKey: effEnd,
      closeLookup,
    });
    const profitCny = profitNativeToAnalysisCny(m.profitNative, currency, market, fxUsd, fxHkd);
    const holdIntervalsLabel = publicLayout
      ? formatHoldingSegmentsLabelPublic({
          symbol: sym,
          symbolTrades,
          periodStart: a,
          periodEnd,
          closeLookup,
        })
      : formatHoldingSegmentsLabel({
          symbol: sym,
          symbolTrades,
          periodStart: a,
          periodEnd,
          closeLookup,
          currency,
          market,
          fxUsd,
          fxHkd,
        });
    rows.push({
      symbol: sym,
      name: symbolTrades[0].name || sym,
      holdIntervalsLabel,
      profitCny,
      pxChange: m.pxChange,
      heldDays: m.heldDays,
    });
  }
  rows.sort((x, y) => y.profitCny - x.profitCny);
  const total = rows.reduce((s, r) => s + r.profitCny, 0);
  return {
    stage,
    periodStart: a,
    periodEnd,
    rows: rows.map((r, i) => ({
      rank: i + 1,
      symbol: r.symbol,
      name: r.name,
      holdIntervalsLabel: r.holdIntervalsLabel,
      profitCny: r.profitCny,
      pxChange: r.pxChange,
      heldDays: r.heldDays,
      profitShare: total !== 0 ? r.profitCny / total : 0,
    })),
  };
}

module.exports = { buildStockRankPayload };
