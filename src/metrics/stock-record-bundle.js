/**
 * 个股交易记录页 bundle：headline + charts.points（格式化字符串）。
 */
const {
  getTrades,
  getSettings,
  getAccounts,
  getUserMetricsMeta,
  getSymbolDailyCloseRange,
  getSymbolDailyPnlChartSeries,
  getSymbolNameMap,
  formatSymbolForDisplay,
  normalizeSymbol,
  resolveBookCurrencyForAccountScope,
} = require("../db");
const {
  fmtPlainAmount,
  fmtPlainSignedAmount,
  fmtPercentRatio,
  fmtSignedPercentRatio,
} = require("../account-kpi-surface");
const { getComputeLiveMetrics } = require("../market-realtime-pnl");
const { getSymbolCurrency, lastPositiveCloseOnOrBefore } = require("../return-calcs");
const { liveDateKeyShanghai } = require("./trading-calendar");
const { enumerateFreezeSessionDates } = require("./freeze-calendar");
const {
  sortTradeAsc,
  collectHoldingSegmentsInPeriod,
  symbolPnlForRankScope,
  groupPnlRowsBySymbol,
} = require("./stock-rank-period");
const { finalizeMetricsBundlePayload } = require("./bundle-payload");

function filterTradesForScope(trades, scope, symbol) {
  const sym = normalizeSymbol(symbol);
  const sc = String(scope || "all").trim() || "all";
  return (trades || [])
    .filter((t) => normalizeSymbol(t.symbol) === sym)
    .filter((t) => sc === "all" || String(t.accountId || "default") === sc)
    .sort(sortTradeAsc);
}

function closeLookupFromRows(closeRows) {
  const sorted = (closeRows || [])
    .map((r) => ({ day: String(r.date || "").slice(0, 10), close: Number(r.close) }))
    .filter((r) => r.day && r.close > 0)
    .sort((a, b) => a.day.localeCompare(b.day));
  return {
    sorted,
    closeOn(day) {
      return lastPositiveCloseOnOrBefore(sorted, day);
    },
  };
}

function formatClosePrice(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return v.toFixed(3);
}

function formatShares(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return String(Math.round(v));
}

function buildHoldingIntervalLabel(symbolTrades, endDate) {
  if (!symbolTrades.length) {
    return { interval: "—", hint: "" };
  }
  const start = String(symbolTrades[0].date || "").slice(0, 10);
  const end = String(endDate || symbolTrades[symbolTrades.length - 1].date || "").slice(0, 10);
  const segments = collectHoldingSegmentsInPeriod(symbolTrades, start, end);
  if (!segments.length) {
    return { interval: "—", hint: "" };
  }
  const last = segments[segments.length - 1];
  const openEnded = last.end === end && symbolTrades.some((t) => {
    const dk = String(t.date).slice(0, 10);
    return dk >= last.start && dk <= end;
  });
  const endLabel = openEnded && last.end === end ? "至今" : last.end.replace(/-/g, "/");
  const interval = `${last.start.replace(/-/g, "/")} ~ ${endLabel}`;
  const hint =
    segments.length > 1
      ? segments.map((s) => `${s.start}～${s.end}`).join("；")
      : "当前持仓区间的起止交易日";
  return { interval, hint };
}

function buildChartPoints({
  pnlRows,
  closeLookup,
  symbolTrades,
  live,
  livePosition,
  ccy,
}) {
  if (!closeLookup.sorted.length && !pnlRows.length) {
    return [];
  }
  const firstTrade = symbolTrades[0]?.date ? String(symbolTrades[0].date).slice(0, 10) : "";
  const firstPnl = pnlRows[0]?.date ? String(pnlRows[0].date).slice(0, 10) : "";
  const firstClose = closeLookup.sorted[0]?.day || "";
  const startCandidates = [firstTrade, firstPnl, firstClose].filter(Boolean).sort();
  const startDate = startCandidates[0] || firstTrade || firstClose;
  const frozenThrough = String(live?.frozenThrough || "").slice(0, 10);
  const endDate = live?.tradingDay ? String(live.liveDate || liveDateKeyShanghai()).slice(0, 10) : frozenThrough;
  if (!startDate || !endDate || startDate > endDate) {
    return [];
  }

  const pnlByDate = new Map(
    (pnlRows || []).map((r) => [String(r.date).slice(0, 10), r]),
  );
  const days = enumerateFreezeSessionDates(startDate, endDate);
  const raw = [];

  for (const dk of days) {
    const close = closeLookup.closeOn(dk);
    if (!(close > 0)) {
      continue;
    }
    const pnl = pnlByDate.get(dk);
    const shares = pnl ? Number(pnl.eodShares) || 0 : 0;
    const mvNat =
      pnl?.eodMarketValueNative != null && Number.isFinite(Number(pnl.eodMarketValueNative))
        ? Number(pnl.eodMarketValueNative)
        : shares * close;
    const weight = pnl?.positionWeight != null && Number.isFinite(Number(pnl.positionWeight)) ? Number(pnl.positionWeight) : 0;
    raw.push({ date: dk, close, shares, mvNat, weight });
  }

  if (live?.tradingDay && livePosition) {
    const liveDate = String(live.liveDate || "").slice(0, 10);
    const current = Number(livePosition.current) || 0;
    const qty = Number(livePosition.quantity) || 0;
    const mvNat = qty * current;
    const totalAssetsCny = Number(live.totalAssetsCny) || 0;
    const rate =
      ccy === "USD"
        ? Number(live.fxUsdCny) || 0
        : ccy === "HKD"
          ? Number(live.fxHkdCny) || 0
          : 1;
    const mvCny = ccy === "CNY" ? mvNat : rate > 0 ? mvNat * rate : 0;
    const weight = totalAssetsCny > 0 ? mvCny / totalAssetsCny : 0;
    const row = {
      date: liveDate,
      close: current > 0 ? current : raw.length ? raw[raw.length - 1].close : 0,
      shares: qty,
      mvNat,
      weight,
    };
    const hit = raw.findIndex((p) => p.date === liveDate);
    if (hit >= 0) {
      raw[hit] = row;
    } else if (liveDate >= startDate && liveDate <= endDate) {
      raw.push(row);
      raw.sort((a, b) => a.date.localeCompare(b.date));
    }
  }

  return raw.map((p) => ({
    date: p.date,
    close: formatClosePrice(p.close),
    shares: formatShares(p.shares),
    marketValueNative: fmtPlainAmount(p.mvNat),
    weight: fmtPercentRatio(p.weight),
  }));
}

async function buildStockRecordBundlePayload({ userId, accountScope, symbol, publicLayout = false }) {
  const uid = String(userId || "").trim();
  const sym = normalizeSymbol(symbol);
  const scope = String(accountScope || "all").trim() || "all";
  if (!uid || !sym) {
    throw new Error("missing user or symbol");
  }

  const [settings, trades, accounts, um, live] = await Promise.all([
    getSettings(uid),
    getTrades(uid),
    getAccounts(uid),
    getUserMetricsMeta(uid),
    getComputeLiveMetrics(uid, scope),
  ]);
  const book = resolveBookCurrencyForAccountScope({ ...settings, accounts }, scope);
  const ccy = getSymbolCurrency(sym);
  const symbolTrades = filterTradesForScope(trades, scope, sym);
  if (!symbolTrades.length) {
    throw new Error("no trades for symbol");
  }

  const firstTrade = String(symbolTrades[0].date).slice(0, 10);
  const frozenThrough = String(live.frozenThrough || um?.frozenThrough || "").slice(0, 10);
  const endDate = live.tradingDay ? String(live.liveDate || liveDateKeyShanghai()).slice(0, 10) : frozenThrough;

  const accountIdForPnl = scope === "all" ? "" : scope;
  const [pnlAll, closeRows, nameMap] = await Promise.all([
    getSymbolDailyPnlChartSeries(
      { accountId: accountIdForPnl, symbol: sym, from: firstTrade, to: endDate || "9999-12-31" },
      uid,
    ),
    getSymbolDailyCloseRange(sym, firstTrade, endDate || "9999-12-31"),
    getSymbolNameMap([sym]),
  ]);
  const pnlBySym = groupPnlRowsBySymbol(
    pnlAll.map((r) => ({
      ...r,
      dayClosePrice: r.eodPrice ?? r.dayClosePrice,
      dayPnlNative: 0,
    })),
  );
  const pnlRows = symbolPnlForRankScope(pnlBySym, sym, scope);
  const closeLookup = closeLookupFromRows(closeRows);

  const livePos = (live.positions || []).find((p) => normalizeSymbol(p.symbol) === sym) || null;
  const points = buildChartPoints({
    pnlRows,
    closeLookup,
    symbolTrades,
    live,
    livePosition: livePos,
    ccy,
  });

  const current = Number(livePos?.current) || closeLookup.closeOn(endDate) || 0;
  const prev = Number(livePos?.prevClose) || current;
  const changeAbs = current - prev;
  const changePct = prev > 0 ? changeAbs / prev : 0;
  const displayName = String(nameMap[sym] || symbolTrades[0]?.name || sym).trim() || sym;
  const { interval, hint } = buildHoldingIntervalLabel(symbolTrades, endDate);

  const payload = {
    meta: {
      accountId: scope,
      symbol: sym,
      bookCurrency: book,
      currency: ccy,
      frozenThrough: frozenThrough || null,
      liveDate: live.tradingDay ? live.liveDate : null,
      tradingDay: !!live.tradingDay,
      dataVersion: Number(um?.dataVersion) || 0,
      rebuilding: !!um?.rebuilding,
      quoteTime: live.quoteTime ?? null,
    },
    headline: {
      name: displayName,
      code: formatSymbolForDisplay(sym),
      price: formatClosePrice(current),
      change: fmtPlainSignedAmount(changeAbs),
      changePct: fmtSignedPercentRatio(changePct),
      quoteTime: live.quoteTime || livePos?.quoteTime || "—",
      holdingInterval: interval,
      holdingIntervalHint: publicLayout ? "持仓区间（公开页仅展示起止日期）" : hint,
    },
    charts: {
      points,
      defaults: {
        showClose: true,
        showShares: true,
        showMarketValue: false,
      },
    },
  };

  return finalizeMetricsBundlePayload(payload);
}

module.exports = {
  buildStockRecordBundlePayload,
};
