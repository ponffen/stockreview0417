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
const { getComputeLiveMetrics, fetchTencentQuotePayloadMap, toTencentQuoteKey } = require("../market-realtime-pnl");
const { normalizeQuoteTimeToBeijingBySymbol } = require("../tencent-quote-time");
const { getSymbolCurrency, lastPositiveCloseOnOrBefore } = require("../return-calcs");
const { liveDateKeyShanghai, getTradingDateKeyBy0830 } = require("./trading-calendar");
const { sortTradeAsc, groupPnlRowsBySymbol } = require("./stock-rank-period");
const { finalizeMetricsBundlePayload } = require("./bundle-payload");

const POSITION_EPS = 1e-6;

function formatQuoteTimeDisplay(timeStr) {
  const raw = String(timeStr || "").trim();
  if (!raw || raw === "—" || raw === "--") {
    return "—";
  }
  const compact = raw.replace(/\D/g, "");
  if (compact.length >= 14) {
    return `${compact.slice(4, 6)}-${compact.slice(6, 8)} ${compact.slice(8, 10)}:${compact.slice(10, 12)}:${compact.slice(12, 14)}`;
  }
  const iso = /^(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})(?:\D+(\d{1,2})[:：](\d{1,2})(?:[:：](\d{1,2}))?)?/.exec(raw);
  if (iso) {
    const month = String(Number(iso[2])).padStart(2, "0");
    const day = String(Number(iso[3])).padStart(2, "0");
    const hour = String(Number(iso[4] || 0)).padStart(2, "0");
    const minute = String(Number(iso[5] || 0)).padStart(2, "0");
    const second = String(Number(iso[6] || 0)).padStart(2, "0");
    return `${month}-${day} ${hour}:${minute}:${second}`;
  }
  return raw;
}

function formatTradingIntervalWithSide(rate, side) {
  const normalizedSide = String(side || "").trim().toLowerCase();
  const suffix = normalizedSide === "buy" ? "B" : normalizedSide === "sell" ? "S" : "";
  const safe = Number.isFinite(Number(rate)) ? Number(rate) : 0;
  const num = (safe * 100).toFixed(2);
  const rateText = `${safe > 0 ? "+" : ""}${num}%`;
  return suffix ? `${rateText} ${suffix}` : rateText;
}

/** 现价相对「当日之前」最近一笔成交价涨跌幅（与前端 tooltip 一致）。 */
function computeTradingIntervalFormatted(symbolTrades, currentPrice, sessionDateKey) {
  const price = Number(currentPrice);
  if (!(price > 0) || !Array.isArray(symbolTrades) || !symbolTrades.length) {
    return "—";
  }
  const todayKey = String(sessionDateKey || getTradingDateKeyBy0830()).slice(0, 10);
  let refTrade = null;
  for (let i = symbolTrades.length - 1; i >= 0; i -= 1) {
    const dk = String(symbolTrades[i]?.date || "").slice(0, 10);
    if (dk && dk < todayKey) {
      refTrade = symbolTrades[i];
      break;
    }
  }
  const refPrice = Number(refTrade?.price);
  if (!refTrade || !(refPrice > 0)) {
    return "—";
  }
  return formatTradingIntervalWithSide((price - refPrice) / refPrice, refTrade.side);
}

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

function parseSymbolLiveQuote(sym, raw) {
  if (!raw) {
    return null;
  }
  if (typeof raw === "object" && Number(raw.current) > 0) {
    return {
      current: Number(raw.current),
      prevClose: Number(raw.prevClose) > 0 ? raw.prevClose : raw.current,
      time: raw.time || null,
    };
  }
  if (typeof raw !== "string") {
    return null;
  }
  const parts = raw.split("~");
  const current = Number(String(parts[3] || "").replace(/,/g, ""));
  const prevClose = Number(String(parts[4] || "").replace(/,/g, ""));
  const rawTime = String(parts[30] || parts[31] || "").trim();
  const time = normalizeQuoteTimeToBeijingBySymbol(rawTime, sym);
  if (!Number.isFinite(current) || current <= 0) {
    return null;
  }
  return {
    current,
    prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
    time: time || rawTime || null,
  };
}

async function resolveHeadlineQuote(sym, livePos, live, closeLookup, endDate) {
  let directQuote = null;
  const quoteKey = toTencentQuoteKey(sym);
  if (live.tradingDay && quoteKey) {
    const req = await fetchTencentQuotePayloadMap([quoteKey]);
    directQuote = parseSymbolLiveQuote(sym, req.payloadMap?.get(String(quoteKey).toLowerCase()));
  }
  const frozenClose = closeLookup.closeOn(endDate) || 0;
  const current =
    Number(directQuote?.current) || Number(livePos?.current) || Number(frozenClose) || 0;
  const prev =
    Number(directQuote?.prevClose) ||
    Number(livePos?.prevClose) ||
    (Number(frozenClose) > 0 ? Number(frozenClose) : current);
  const quoteTime = directQuote?.time || live.quoteTime || null;
  return { current, prevClose: prev, quoteTime };
}

function pnlRowsForStockRecordScope(pnlBySym, sym, scope) {
  const list = pnlBySym.get(sym) || [];
  const sc = String(scope || "all").trim() || "all";
  if (sc === "all") {
    return list.filter((r) => String(r.accountId || "") === "all");
  }
  return list.filter((r) => String(r.accountId || "default") === sc);
}

function todayProfitNativeFromLive(livePosition, ccy, live) {
  const todayProfitCny = Number(livePosition?.todayProfitCny) || 0;
  if (ccy === "CNY") {
    return todayProfitCny;
  }
  const fx =
    ccy === "USD"
      ? Number(live?.fxUsdCny) || 0
      : ccy === "HKD"
        ? Number(live?.fxHkdCny) || 0
        : 1;
  return fx > 0 ? todayProfitCny / fx : 0;
}

function frozenInceptionProfitOnOrBefore(pnlRows, asOfDate) {
  const asOf = String(asOfDate || "").slice(0, 10);
  if (!asOf) {
    return 0;
  }
  let best = null;
  for (const row of pnlRows || []) {
    const dk = String(row.date || "").slice(0, 10);
    if (!dk || dk > asOf) {
      continue;
    }
    if (!best || dk > best.date) {
      best = { date: dk, profit: Number(row.stageInceptionProfit) };
    }
  }
  return Number.isFinite(best?.profit) ? best.profit : 0;
}

function buildChartPoints({
  pnlRows,
  closeLookup,
  symbolTrades,
  live,
  livePosition,
  ccy,
}) {
  if (!pnlRows.length) {
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

  const sortedPnl = [...pnlRows]
    .map((row) => ({ ...row, dk: String(row.date || "").slice(0, 10) }))
    .filter((row) => row.dk && row.dk >= startDate && row.dk <= endDate)
    .sort((a, b) => a.dk.localeCompare(b.dk));

  const raw = [];
  for (const pnl of sortedPnl) {
    const dk = pnl.dk;
    const close = closeLookup.closeOn(dk);
    if (!(close > 0)) {
      continue;
    }
    const shares = Number(pnl.eodShares) || 0;
    const mvNat =
      pnl.eodMarketValueNative != null && Number.isFinite(Number(pnl.eodMarketValueNative))
        ? Number(pnl.eodMarketValueNative)
        : shares * close;
    const weight =
      pnl.positionWeight != null && Number.isFinite(Number(pnl.positionWeight))
        ? Number(pnl.positionWeight)
        : 0;
    const totalProfitNat = Number(pnl.stageInceptionProfit);
    raw.push({
      date: dk,
      close,
      shares,
      mvNat,
      weight,
      totalProfitNat: Number.isFinite(totalProfitNat) ? totalProfitNat : 0,
    });
  }

  const liveQty = Number(livePosition?.quantity) || 0;
  if (live?.tradingDay && livePosition && Math.abs(liveQty) > POSITION_EPS) {
    const liveDate = String(live.liveDate || "").slice(0, 10);
    const current = Number(livePosition.current) || 0;
    const mvNat = liveQty * current;
    const totalAssetsCny = Number(live.totalAssetsCny) || 0;
    const rate =
      ccy === "USD"
        ? Number(live.fxUsdCny) || 0
        : ccy === "HKD"
          ? Number(live.fxHkdCny) || 0
          : 1;
    const mvCny = ccy === "CNY" ? mvNat : rate > 0 ? mvNat * rate : 0;
    const weight = totalAssetsCny > 0 ? mvCny / totalAssetsCny : 0;
    const frozenProfit = frozenInceptionProfitOnOrBefore(pnlRows, frozenThrough);
    const totalProfitNat = frozenProfit + todayProfitNativeFromLive(livePosition, ccy, live);
    const row = {
      date: liveDate,
      close: current > 0 ? current : raw.length ? raw[raw.length - 1].close : 0,
      shares: liveQty,
      mvNat,
      weight,
      totalProfitNat,
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
    totalProfit: fmtPlainSignedAmount(p.totalProfitNat),
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

  const accountIdForPnl = scope === "all" ? "all" : scope;
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
  const pnlRows = pnlRowsForStockRecordScope(pnlBySym, sym, scope);
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

  const headlineQuote = await resolveHeadlineQuote(sym, livePos, live, closeLookup, endDate);
  const current = headlineQuote.current;
  const prev = headlineQuote.prevClose;
  const changeAbs = current - prev;
  const changePct = prev > 0 ? changeAbs / prev : 0;
  const displayName = String(nameMap[sym] || symbolTrades[0]?.name || sym).trim() || sym;
  const sessionDateKey = live.tradingDay ? String(live.liveDate || liveDateKeyShanghai()).slice(0, 10) : endDate;
  const tradingInterval = computeTradingIntervalFormatted(symbolTrades, current, sessionDateKey);

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
      quoteTime: headlineQuote.quoteTime ?? null,
    },
    headline: {
      name: displayName,
      code: formatSymbolForDisplay(sym),
      price: formatClosePrice(current),
      change: fmtPlainSignedAmount(changeAbs),
      changePct: fmtSignedPercentRatio(changePct),
      quoteTime: formatQuoteTimeDisplay(headlineQuote.quoteTime),
      tradingInterval,
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
