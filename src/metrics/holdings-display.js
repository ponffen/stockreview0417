/**
 * f 持仓表：symbol_daily_pnl 冻结日行（v3 stage_*）+ computeLiveMetrics（今日/现价）。
 */
const { resolveDisplayNameFromMap, resolveMetaFromMap } = require("../symbol-name-resolve");
const {
  normalizeSymbol,
  getSymbolMetaMap,
  resolveBookCurrencyForAccountScope,
  getSymbolDailyPnl,
  getSymbolDailyPnlRowsOnOrBefore,
} = require("../db");
const {
  fmtPlainAmount,
  fmtPlainSignedAmount,
  fmtPercentRatio,
  fmtSignedPercentRatio,
  cnyScalarToBookAmount,
} = require("../account-kpi-surface");
const { isAggregateScope } = require("./account-book-metrics");
const { chainTwrRate, positionDailyTwrReturn } = require("./snapshot-plus-live");
const { symbolMwrFromValueFlowPoints } = require("../mwr");
const {
  getPositionDayTradeContext,
  getTradingDateKeyBy0830,
} = require("../position-today-pnl");
const { resolveFrozenStageProfits } = require("./profit-tracks");
const { netHoldingsBySymbol, hasOpenPositionQuantity } = require("./holdings-active-symbols");
const { getLatestValuationBySymbolForUser } = require("../dynamics/community-posts-db");
const {
  isFreshStagePeriod,
  monthStartKeyShanghai,
  yearStartKeyShanghai,
} = require("./stages");
const { liveDateKeyShanghai } = require("./trading-calendar");
const { formatValuationPrice } = require("../dynamics/valuation-format");

function profitShareRatio(stockProfitScalar, overviewProfitScalar, accountScope, book, fxUsdCny, fxHkdCny) {
  let stockBook = Number(stockProfitScalar) || 0;
  let overviewBook = Number(overviewProfitScalar) || 0;
  if (isAggregateScope(accountScope)) {
    stockBook = cnyScalarToBookAmount(stockBook, book, fxUsdCny, fxHkdCny);
    overviewBook = cnyScalarToBookAmount(overviewBook, book, fxUsdCny, fxHkdCny);
  }
  if (!Number.isFinite(overviewBook) || Math.abs(overviewBook) < 1e-9) {
    return 0;
  }
  return stockBook / overviewBook;
}

function marketFromMetaTag(marketTag) {
  const tag = String(marketTag || "").toUpperCase();
  if (tag === "CN") {
    return "A股";
  }
  if (tag === "HK") {
    return "港股";
  }
  if (tag === "US") {
    return "美股";
  }
  return "其他";
}

function inferMarket(symbol) {
  const s = String(symbol || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
  if (s.startsWith("sh") || s.startsWith("sz")) {
    return "A股";
  }
  if (s.startsWith("hk") || s.startsWith("rt_hk")) {
    return "港股";
  }
  return "美股";
}

function marketTag(market) {
  if (market === "A股") {
    return "CN";
  }
  if (market === "港股") {
    return "HK";
  }
  if (market === "美股") {
    return "US";
  }
  return "OT";
}

function formatRegretRateWithSide(rate, side) {
  const normalizedSide = String(side || "").trim().toLowerCase();
  const suffix = normalizedSide === "buy" ? "B" : normalizedSide === "sell" ? "S" : "";
  const safe = Number.isFinite(Number(rate)) ? Number(rate) : 0;
  const num = (safe * 100).toFixed(2);
  const rateText = `${safe > 0 ? "+" : ""}${num}%`;
  return suffix ? `${rateText} ${suffix}` : rateText;
}

function formatEstimateChange(price, current) {
  const p = Number(price);
  const c = Number(current);
  if (!Number.isFinite(p) || p <= 0 || !Number.isFinite(c) || c <= 0) {
    return "";
  }
  const rate = p / c - 1;
  return fmtSignedPercentRatio(rate);
}

function lastTradeBySymbol(trades, accountScope) {
  const scope = String(accountScope || "all").trim() || "all";
  const list =
    scope === "all"
      ? trades || []
      : (trades || []).filter((t) => String(t.accountId || "default") === scope);
  const sorted = [...list].sort((a, b) => {
    const ad = new Date(a.date).getTime();
    const bd = new Date(b.date).getTime();
    if (ad !== bd) {
      return ad - bd;
    }
    return Number(a.createdAt || 0) - Number(b.createdAt || 0);
  });
  const map = new Map();
  for (const trade of sorted) {
    const sym = normalizeSymbol(trade.symbol);
    if (!sym) {
      continue;
    }
    const price = Number(trade.price);
    map.set(sym, {
      lastTradePrice: Number.isFinite(price) && price > 0 ? price : 0,
      lastTradeSide: trade.side,
      lastTradeDate: trade.date,
    });
  }
  return map;
}

function symbolPnlToValueFlowPoints(pnlRows) {
  const pts = [];
  for (const r of pnlRows || []) {
    const d = String(r.date || "").slice(0, 10);
    const sh = Number(r.eodShares ?? r.eod_shares) || 0;
    const px = Number(r.dayClosePrice ?? r.day_close_price);
    const flow = Number(r.dayTradeFlowNative ?? r.day_trade_flow_native) || 0;
    if (!d || !(sh > 0) || !(px > 0)) {
      continue;
    }
    pts.push({ date: d, value: sh * px, flow });
  }
  pts.sort((a, b) => a.date.localeCompare(b.date));
  return pts;
}

/** 取单标的在 frozenThrough 当日及之前的最后一条 symbol_daily_pnl（与个股分析一致）。 */
function resolveFrozenProfitsForDisplay(frozenRow, ctx) {
  const native = resolveFrozenStageProfits(frozenRow, ctx, "native");
  const cny = resolveFrozenStageProfits(frozenRow, ctx, "cny");
  return {
    monthFrozenNative: native.monthFrozen,
    yearFrozenNative: native.yearFrozen,
    totalFrozenNative: native.totalFrozen,
    monthFrozenCny: cny.monthFrozen,
    yearFrozenCny: cny.yearFrozen,
    totalFrozenCny: cny.totalFrozen,
  };
}

function frozenRowToRateSnap(frozenRow, frozenThrough, packSnap) {
  if (!frozenRow) {
    return packSnap;
  }
  const rate = Number(frozenRow.stageInceptionRateTwr);
  return {
    ...(packSnap || {}),
    total_rate_twr: Number.isFinite(rate) ? rate : packSnap?.total_rate_twr,
    frozen_through: frozenThrough,
  };
}

function symbolTotalRates({
  snap,
  liveP,
  live,
  trades,
  sym,
  todayProfitNative,
  pnlRows,
  mwrMode,
}) {
  const current = Number(liveP?.current) || 0;
  const prev = Number(liveP?.prevClose) || current;
  const qty = Number(liveP?.quantity) || 0;
  const mvNat = qty * current;
  const todayKey = live.tradingDay ? String(live.liveDate || getTradingDateKeyBy0830()).slice(0, 10) : "";
  const dayCtx = todayKey ? getPositionDayTradeContext(sym, todayKey, trades) : { startQuantity: 0, dayFlowNative: 0 };
  const startMv = (Number(dayCtx.startQuantity) || 0) * prev;
  const flowNat = Number(dayCtx.dayFlowNative) || 0;
  const frozenTotalRateTwr = Number(snap?.total_rate_twr);
  let rateTwr = Number.isFinite(frozenTotalRateTwr) ? frozenTotalRateTwr : 0;
  if (live.tradingDay) {
    const rToday = positionDailyTwrReturn(startMv, mvNat, todayProfitNative, flowNat);
    if (Number.isFinite(frozenTotalRateTwr)) {
      rateTwr = chainTwrRate(frozenTotalRateTwr, rToday);
    } else if (qty > 0 && (Math.abs(flowNat) > 0 || Math.abs(todayProfitNative) > 0)) {
      // 无冻结历史（如今日新开仓）：总收益率 = 当日 TWR，勿默认 0%
      rateTwr = rToday;
    }
  }
  const pts = symbolPnlToValueFlowPoints(pnlRows);
  const frozenThrough = String(snap?.frozen_through || live.frozenThrough || "").slice(0, 10);
  const endDate = live.tradingDay ? todayKey : frozenThrough;
  let rateMwr = null;
  if (pts.length || mvNat > 0) {
    const histPts = live.tradingDay && todayKey ? pts.filter((p) => p.date < todayKey) : pts;
    rateMwr = symbolMwrFromValueFlowPoints(histPts, endDate, mvNat);
  }
  return { rateTwr, rateMwr: mwrMode ? rateMwr : rateTwr, totalRate: mwrMode ? rateMwr : rateTwr };
}

async function buildHoldingsPayload({
  userId,
  accountScope,
  settings,
  live,
  symbolRows,
  accountRow,
  trades,
  overviewStages,
}) {
  const fxU = Number(accountRow?.eod_fx_usd_cny) || live.fxUsdCny || 7.2;
  const fxH = Number(accountRow?.eod_fx_hkd_cny) || live.fxHkdCny || 0.92;
  const book = resolveBookCurrencyForAccountScope(settings, accountScope);
  const mwrMode = String(settings?.algoMode || "twr").toLowerCase() === "mwr";
  const overviewMonthCny = Number(overviewStages?.mtd?.profitCny) || 0;
  const overviewYearCny = Number(overviewStages?.ytd?.profitCny) || 0;
  const overviewTotalCny = Number(overviewStages?.inception?.profitCny) || 0;
  const liveBySym = new Map((live.positions || []).map((p) => [normalizeSymbol(p.symbol), p]));
  const snapBySym = new Map((symbolRows || []).map((r) => [normalizeSymbol(r.symbol), r]));
  const netBySym = netHoldingsBySymbol(trades, accountScope);

  const keys = new Set([...liveBySym.keys(), ...snapBySym.keys()]);
  for (const [sym, q] of netBySym.entries()) {
    if (hasOpenPositionQuantity(q)) {
      keys.add(sym);
    }
  }
  const nameMap = await getSymbolMetaMap([...keys]);
  const tradeBySym = lastTradeBySymbol(trades, accountScope);
  const valuationBySym = userId ? await getLatestValuationBySymbolForUser(userId) : new Map();
  const scopeId = String(accountScope || "all").trim() || "all";
  const frozenThrough = String(
    accountRow?.frozen_through || symbolRows?.[0]?.frozen_through || live.frozenThrough || "",
  ).slice(0, 10);
  const sessionAsOf = liveDateKeyShanghai();
  const freshMonth = isFreshStagePeriod(monthStartKeyShanghai(sessionAsOf), frozenThrough);
  const freshYear = isFreshStagePeriod(yearStartKeyShanghai(sessionAsOf), frozenThrough);
  const firstTrade = String(accountRow?.first_trade_date || accountRow?.firstTradeDate || frozenThrough || "1970-01-01").slice(
    0,
    10,
  );
  const pnlTo = live.tradingDay
    ? String(live.liveDate || frozenThrough).slice(0, 10)
    : frozenThrough;
  const pnlBySym = new Map();
  // MWR 需全历史日序列算个股资金加权收益；TWR 用冻结行 stage_* + 链式 TWR，不拉全表。
  if (mwrMode && userId && pnlTo) {
    const allPnl = await getSymbolDailyPnl(
      { accountId: scopeId === "all" ? "" : scopeId, from: firstTrade, to: pnlTo },
      userId,
    );
    for (const r of allPnl) {
      const sym = normalizeSymbol(r.symbol);
      if (!pnlBySym.has(sym)) {
        pnlBySym.set(sym, []);
      }
      pnlBySym.get(sym).push(r);
    }
  }

  const accountIdForPnl = scopeId === "all" ? "all" : scopeId;
  const activeSyms = [...keys].filter((sym) => {
    const qty = Number(liveBySym.get(sym)?.quantity) || 0;
    if (hasOpenPositionQuantity(qty)) {
      return true;
    }
    const todayP = live.tradingDay ? Number(liveBySym.get(sym)?.todayProfitCny) || 0 : 0;
    return Math.abs(todayP) > 1e-6;
  });
  const frozenRowBySym =
    userId && frozenThrough && activeSyms.length
      ? await getSymbolDailyPnlRowsOnOrBefore(
          { accountId: accountIdForPnl, symbols: activeSyms, asOf: frozenThrough },
          userId,
        )
      : new Map();

  const rowsOut = [];
  for (const sym of keys) {
    const liveP = liveBySym.get(sym);
    const snap = snapBySym.get(sym);
    const frozenRow = frozenRowBySym.get(sym) || null;
    const qty = Number(liveP?.quantity) || 0;
    const sessionLabel = liveP?.sessionLabel ? String(liveP.sessionLabel) : null;
    const todayProfitBookRow = live.tradingDay ? Number(liveP?.todayProfitCny) || 0 : 0;
    if (!hasOpenPositionQuantity(qty) && Math.abs(todayProfitBookRow) <= 1e-6) {
      continue;
    }
    const ccy = String(frozenRow?.currency || snap?.currency || liveP?.currency || "CNY").toUpperCase();
    const meta = resolveMetaFromMap(sym, nameMap);
    const market = marketFromMetaTag(meta.marketTag);
    const isCnyStock = ccy === "CNY" || market === "A股";
    const fx =
      ccy === "USD" && fxU > 0 ? fxU : ccy === "HKD" && fxH > 0 ? fxH : 1;
    const frozenCtx = { freshMonth, freshYear, frozenThrough, sessionAsOf };
    const {
      monthFrozenNative,
      yearFrozenNative,
      totalFrozenNative,
      monthFrozenCny,
      yearFrozenCny,
      totalFrozenCny,
    } = resolveFrozenProfitsForDisplay(frozenRow, frozenCtx);
    const rateSnap = frozenRowToRateSnap(frozenRow, frozenThrough, snap);
    const current = Number(liveP?.current) || 0;
    const prev = Number(liveP?.prevClose) || current;
    const todayProfitBook = live.tradingDay ? Number(liveP?.todayProfitBook ?? liveP?.todayProfitCny) || 0 : 0;
    const todayProfitNative =
      liveP?.todayProfitNative != null && Number.isFinite(Number(liveP.todayProfitNative))
        ? Number(liveP.todayProfitNative)
        : todayProfitBook;
    const todayProfitCny =
      liveP?.todayProfitCny != null && Number.isFinite(Number(liveP.todayProfitCny))
        ? Number(liveP.todayProfitCny)
        : todayProfitBook;
    const monthNative = monthFrozenNative + todayProfitNative;
    const yearNative = yearFrozenNative + todayProfitNative;
    const totalNative = totalFrozenNative + todayProfitNative;
    const monthCny = monthFrozenCny + todayProfitCny;
    const yearCny = yearFrozenCny + todayProfitCny;
    const totalCny = totalFrozenCny + todayProfitCny;
    const dayChg = prev > 0 ? (current - prev) / prev : 0;
    const mvNat = qty * current;
    const mvCny = Number(liveP?.marketValueCny) || mvNat * (isCnyStock ? 1 : fx);
    const costNat =
      qty > 0 && Math.abs(totalNative) > 1e-9
        ? mvNat - totalNative
        : Math.abs(totalFrozenNative) > 0 && Number(rateSnap?.total_rate_twr)
          ? mvNat - totalNative
          : mvNat;
    const sigma = qty > 0 ? costNat / qty : 0;
    const { totalRate, rateTwr, rateMwr } = symbolTotalRates({
      snap: rateSnap,
      liveP,
      live,
      trades,
      sym,
      todayProfitNative,
      pnlRows: pnlBySym.get(sym) || [],
      mwrMode,
    });

    const displayName = resolveDisplayNameFromMap(sym, nameMap);
    const lastTr = tradeBySym.get(sym) || {};
    const lastTradePrice = Number(lastTr.lastTradePrice) || 0;
    const regretRate =
      lastTradePrice > 0 ? (current - lastTradePrice) / lastTradePrice : 0;
    const valuationExtra = valuationBySym.get(sym) || {};
    const lowChg = formatEstimateChange(valuationExtra.lowPrice, current);
    const highChg = formatEstimateChange(valuationExtra.highPrice, current);

    rowsOut.push({
      symbol: sym,
      name: displayName,
      market,
      currency: ccy,
      isCnyStock,
      marketTag: meta.marketTag,
      stockCode: meta.stockCode,
      price: Number.isFinite(current) ? current.toFixed(3) : "—",
      sessionLabel,
      dayChange: fmtSignedPercentRatio(dayChg),
      marketValue: fmtPlainAmount(mvNat),
      marketValueCny: fmtPlainAmount(mvCny),
      quantity: String(Math.round(qty)),
      weight: "—",
      cost: Number.isFinite(sigma) ? sigma.toFixed(3) : "—",
      todayProfit: fmtPlainSignedAmount(todayProfitNative),
      todayProfitCny: fmtPlainSignedAmount(todayProfitCny),
      monthProfit: fmtPlainSignedAmount(monthNative),
      monthProfitCny: fmtPlainSignedAmount(monthCny),
      monthWeight: "—",
      yearProfit: fmtPlainSignedAmount(yearNative),
      yearProfitCny: fmtPlainSignedAmount(yearCny),
      yearWeight: "—",
      totalProfit: fmtPlainSignedAmount(totalNative),
      totalProfitCny: fmtPlainSignedAmount(totalCny),
      totalWeight: "—",
      totalRate: fmtSignedPercentRatio(totalRate),
      regret: formatRegretRateWithSide(regretRate, lastTr.lastTradeSide),
      lastTradeSide: lastTr.lastTradeSide || "",
      lastTradeDate: lastTr.lastTradeDate || "",
      lowEstimate: formatValuationPrice(valuationExtra.lowPrice),
      lowEstimateChange: lowChg,
      highEstimate: formatValuationPrice(valuationExtra.highPrice),
      highEstimateChange: highChg,
    });
  }

  const totalMv = rowsOut.reduce((s, r) => {
    const lp = liveBySym.get(normalizeSymbol(r.symbol));
    return s + (Number(lp?.marketValueCny) || 0);
  }, 0);
  const totalAssets = Number(live.totalAssetsCny) || totalMv + (Number(live.cashCny) || 0);
  for (const row of rowsOut) {
    const sym = normalizeSymbol(row.symbol);
    const liveP = liveBySym.get(sym);
    const frozenRow = frozenRowBySym.get(sym) || null;
    const ccy = String(row.currency || "").toUpperCase();
    const isCn = row.isCnyStock === true;
    const fx =
      ccy === "USD" && fxU > 0 ? fxU : ccy === "HKD" && fxH > 0 ? fxH : 1;
    const todayProfitBook = live.tradingDay ? Number(liveP?.todayProfitBook ?? liveP?.todayProfitCny) || 0 : 0;
    const {
      monthFrozenNative,
      yearFrozenNative,
      totalFrozenNative,
      monthFrozenCny,
      yearFrozenCny,
      totalFrozenCny,
    } = resolveFrozenProfitsForDisplay(frozenRow, { freshMonth, freshYear, frozenThrough, sessionAsOf });
    const todayProfitNative =
      liveP?.todayProfitNative != null && Number.isFinite(Number(liveP.todayProfitNative))
        ? Number(liveP.todayProfitNative)
        : todayProfitBook;
    const todayProfitCny =
      liveP?.todayProfitCny != null && Number.isFinite(Number(liveP.todayProfitCny))
        ? Number(liveP.todayProfitCny)
        : todayProfitBook;
    const monthNative = monthFrozenNative + todayProfitNative;
    const yearNative = yearFrozenNative + todayProfitNative;
    const totalNative = totalFrozenNative + todayProfitNative;
    const monthCny = monthFrozenCny + todayProfitCny;
    const yearCny = yearFrozenCny + todayProfitCny;
    const totalCny = totalFrozenCny + todayProfitCny;
    const mv = Number(liveP?.marketValueCny) || 0;
    const weight = totalAssets > 0 ? mv / totalAssets : 0;
    const monthStock = isAggregateScope(accountScope) ? monthCny : monthNative;
    const yearStock = isAggregateScope(accountScope) ? yearCny : yearNative;
    const totalStock = isAggregateScope(accountScope) ? totalCny : totalNative;
    const monthW = profitShareRatio(monthStock, overviewMonthCny, accountScope, book, fxU, fxH);
    const yearW = profitShareRatio(yearStock, overviewYearCny, accountScope, book, fxU, fxH);
    const totalW = profitShareRatio(totalStock, overviewTotalCny, accountScope, book, fxU, fxH);
    row.weight = totalAssets > 0 ? fmtPercentRatio(weight) : "0.00%";
    row.monthWeight = fmtPercentRatio(monthW);
    row.yearWeight = fmtPercentRatio(yearW);
    row.totalWeight = fmtPercentRatio(totalW);
  }

  rowsOut.sort((a, b) => {
    const pa = liveBySym.get(normalizeSymbol(a.symbol));
    const pb = liveBySym.get(normalizeSymbol(b.symbol));
    return (Number(pb?.marketValueCny) || 0) - (Number(pa?.marketValueCny) || 0);
  });

  return rowsOut;
}

module.exports = { buildHoldingsPayload };
