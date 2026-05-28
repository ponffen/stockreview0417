/**
 * f 持仓表：symbol_home_summary（冻结）+ computeLiveMetrics（今日/现价），方案 A 全 display。
 */
const {
  normalizeSymbol,
  formatSymbolForDisplay,
  getSymbolNameMap,
  resolveBookCurrencyForAccountScope,
} = require("../db");
const { fmtPlainAmount, fmtPlainSignedAmount, fmtPercentRatio, cnyScalarToBookAmount } = require("../account-kpi-surface");
const { chainTwrRate } = require("./snapshot-plus-live");

function profitShareRatio(stockProfitCny, overviewProfitCny, book, fxUsdCny, fxHkdCny) {
  const stockBook = cnyScalarToBookAmount(stockProfitCny, book, fxUsdCny, fxHkdCny);
  const overviewBook = cnyScalarToBookAmount(overviewProfitCny, book, fxUsdCny, fxHkdCny);
  if (!Number.isFinite(overviewBook) || Math.abs(overviewBook) < 1e-9) {
    return 0;
  }
  return stockBook / overviewBook;
}

function inferMarket(symbol) {
  const s = String(symbol || "");
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

async function buildHoldingsPayload({
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
  const overviewMonthCny = Number(overviewStages?.mtd?.profitCny) || 0;
  const overviewYearCny = Number(overviewStages?.ytd?.profitCny) || 0;
  const overviewTotalCny = Number(overviewStages?.inception?.profitCny) || 0;
  const liveBySym = new Map((live.positions || []).map((p) => [normalizeSymbol(p.symbol), p]));
  const snapBySym = new Map((symbolRows || []).map((r) => [normalizeSymbol(r.symbol), r]));

  const keys = new Set([...liveBySym.keys(), ...snapBySym.keys()]);
  const nameMap = await getSymbolNameMap([...keys]);
  const tradeBySym = lastTradeBySymbol(trades, accountScope);
  const rowsOut = [];
  for (const sym of keys) {
    const liveP = liveBySym.get(sym);
    const snap = snapBySym.get(sym);
    const qty = liveP?.quantity || 0;
    if (!(qty > 1e-6)) {
      continue;
    }
    const ccy = String(snap?.currency || liveP?.currency || "CNY").toUpperCase();
    const market = inferMarket(sym);
    const isCnyStock = ccy === "CNY" || market === "A股";
    const fx =
      ccy === "USD" && fxU > 0 ? fxU : ccy === "HKD" && fxH > 0 ? fxH : 1;
    const monthFrozenNative = Number(snap?.month_profit_native) || 0;
    const yearFrozenNative = Number(snap?.ytd_profit_native) || 0;
    const totalFrozenNative = Number(snap?.total_profit_native) || 0;
    const current = Number(liveP?.current) || 0;
    const prev = Number(liveP?.prevClose) || current;
    const todayProfitCny = live.tradingDay ? Number(liveP?.todayProfitCny) || 0 : 0;
    const todayProfitNative = isCnyStock ? todayProfitCny : fx > 0 ? todayProfitCny / fx : 0;
    const monthNative = monthFrozenNative + todayProfitNative;
    const yearNative = yearFrozenNative + todayProfitNative;
    const totalNative = totalFrozenNative + todayProfitNative;
    const monthCny = monthFrozenNative * (isCnyStock ? 1 : fx) + todayProfitCny;
    const yearCny = yearFrozenNative * (isCnyStock ? 1 : fx) + todayProfitCny;
    const totalCny = totalFrozenNative * (isCnyStock ? 1 : fx) + todayProfitCny;
    const dayChg = prev > 0 ? (current - prev) / prev : 0;
    const mvNat = qty * current;
    const mvCny = Number(liveP?.marketValueCny) || mvNat * (isCnyStock ? 1 : fx);
    const costNat =
      Math.abs(totalFrozenNative) > 0 && Number(snap?.total_rate_twr)
        ? mvNat - totalNative
        : mvNat;
    const sigma = qty > 0 ? costNat / qty : 0;
    const frozenTotalRate = Number(snap?.total_rate_twr);
    let totalRate = Number.isFinite(frozenTotalRate)
      ? frozenTotalRate
      : Math.abs(sigma * qty) > 0
        ? totalNative / Math.abs(sigma * qty)
        : 0;
    if (live.tradingDay && Number.isFinite(frozenTotalRate) && Number.isFinite(dayChg)) {
      totalRate = chainTwrRate(frozenTotalRate, dayChg);
    }

    const snapName = String(snap?.name || "").trim();
    const mappedName = String(nameMap[sym] || "").trim();
    const displayName =
      mappedName || (snapName && snapName.toLowerCase() !== sym.toLowerCase() ? snapName : "") || sym;
    const lastTr = tradeBySym.get(sym) || {};
    const lastTradePrice = Number(lastTr.lastTradePrice) || 0;
    const regretRate =
      lastTradePrice > 0 ? (current - lastTradePrice) / lastTradePrice : 0;

    rowsOut.push({
      symbol: sym,
      name: displayName,
      market,
      currency: ccy,
      isCnyStock,
      marketTag: marketTag(market),
      stockCode: formatSymbolForDisplay(sym),
      priceDisplay: Number.isFinite(current) ? current.toFixed(3) : "—",
      dayChangeDisplay: fmtPercentRatio(dayChg),
      marketValueDisplay: fmtPlainAmount(mvNat),
      marketValueDisplayCny: fmtPlainAmount(mvCny),
      quantityDisplay: String(Math.round(qty)),
      weightDisplay: "—",
      costDisplay: Number.isFinite(sigma) ? sigma.toFixed(3) : "—",
      todayProfitDisplay: fmtPlainSignedAmount(todayProfitNative),
      todayProfitDisplayCny: fmtPlainSignedAmount(todayProfitCny),
      monthProfitDisplay: fmtPlainSignedAmount(monthNative),
      monthProfitDisplayCny: fmtPlainSignedAmount(monthCny),
      monthWeightDisplay: "—",
      monthWeightNum: 0,
      yearProfitDisplay: fmtPlainSignedAmount(yearNative),
      yearProfitDisplayCny: fmtPlainSignedAmount(yearCny),
      yearWeightDisplay: "—",
      yearWeightNum: 0,
      totalProfitDisplay: fmtPlainSignedAmount(totalNative),
      totalProfitDisplayCny: fmtPlainSignedAmount(totalCny),
      totalWeightDisplay: "—",
      totalWeightNum: 0,
      totalRateDisplay: fmtPercentRatio(totalRate),
      totalRateNum: totalRate,
      regretDisplay: formatRegretRateWithSide(regretRate, lastTr.lastTradeSide),
      regretRateNum: regretRate,
      lastTradeSide: lastTr.lastTradeSide || "",
      lastTradeDate: lastTr.lastTradeDate || "",
      lastTradeDateMs: Date.parse(lastTr.lastTradeDate || 0) || 0,
      currentPriceNum: current,
      marketValueNum: mvCny,
      costNum: sigma,
      dayChangeRate: dayChg,
      todayProfitNativeNum: todayProfitNative,
      todayProfitCnyNum: todayProfitCny,
      monthProfitNativeNum: monthNative,
      monthProfitCnyNum: monthCny,
      yearProfitNativeNum: yearNative,
      yearProfitCnyNum: yearCny,
      totalProfitNativeNum: totalNative,
      totalProfitCnyNum: totalCny,
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
    const monthCny = Number(row.monthProfitCnyNum) || 0;
    const yearCny = Number(row.yearProfitCnyNum) || 0;
    const totalCny = Number(row.totalProfitCnyNum) || 0;
    const mv = Number(liveP?.marketValueCny) || 0;
    const weight = totalAssets > 0 ? mv / totalAssets : 0;
    const monthW = profitShareRatio(monthCny, overviewMonthCny, book, fxU, fxH);
    const yearW = profitShareRatio(yearCny, overviewYearCny, book, fxU, fxH);
    const totalW = profitShareRatio(totalCny, overviewTotalCny, book, fxU, fxH);
    row.weightDisplay = totalAssets > 0 ? fmtPercentRatio(weight) : "0.00%";
    row.monthWeightDisplay = fmtPercentRatio(monthW);
    row.yearWeightDisplay = fmtPercentRatio(yearW);
    row.totalWeightDisplay = fmtPercentRatio(totalW);
    row.weightNum = weight;
    row.monthWeightNum = monthW;
    row.yearWeightNum = yearW;
    row.totalWeightNum = totalW;
  }

  rowsOut.sort((a, b) => {
    const pa = liveBySym.get(normalizeSymbol(a.symbol));
    const pb = liveBySym.get(normalizeSymbol(b.symbol));
    return (Number(pb?.marketValueCny) || 0) - (Number(pa?.marketValueCny) || 0);
  });

  return rowsOut;
}

module.exports = { buildHoldingsPayload };
