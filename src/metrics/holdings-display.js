/**
 * f 持仓表：symbol_home_summary（冻结）+ computeLiveMetrics（今日/现价），方案 A 全 display。
 */
const { normalizeSymbol, formatSymbolForDisplay, getSymbolNameMap } = require("../db");
const { fmtPlainAmount, fmtPlainSignedAmount, fmtPercentRatio } = require("../account-kpi-surface");

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

function formatRegret(rate, side) {
  const r = Number(rate);
  if (!Number.isFinite(r)) {
    return "—";
  }
  const s = String(side || "").toLowerCase() === "sell" ? "卖" : "买";
  return `${s}后 ${fmtPercentRatio(r)}`;
}

async function buildHoldingsPayload({ accountScope, settings, live, symbolRows, accountRow }) {
  const fxU = Number(accountRow?.eod_fx_usd_cny) || live.fxUsdCny || 7.2;
  const fxH = Number(accountRow?.eod_fx_hkd_cny) || live.fxHkdCny || 0.92;
  const liveBySym = new Map((live.positions || []).map((p) => [normalizeSymbol(p.symbol), p]));
  const snapBySym = new Map((symbolRows || []).map((r) => [normalizeSymbol(r.symbol), r]));

  const keys = new Set([...liveBySym.keys(), ...snapBySym.keys()]);
  const nameMap = await getSymbolNameMap([...keys]);
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
    const todayProfitNative =
      live.tradingDay && Number.isFinite(current) && Number.isFinite(prev) ? qty * (current - prev) : 0;
    let todayProfitCny = todayProfitNative * (isCnyStock ? 1 : fx);
    const liveTodayCny = live.tradingDay ? Number(liveP?.todayProfitCny) : 0;
    if (live.tradingDay && Number.isFinite(liveTodayCny) && Math.abs(liveTodayCny) > 1e-9) {
      todayProfitCny = liveTodayCny;
    }
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
    const totalRate = Math.abs(sigma * qty) > 0 ? totalNative / Math.abs(sigma * qty) : 0;

    const snapName = String(snap?.name || "").trim();
    const mappedName = String(nameMap[sym] || "").trim();
    const displayName =
      mappedName || (snapName && snapName.toLowerCase() !== sym.toLowerCase() ? snapName : "") || sym;

    rowsOut.push({
      symbol: sym,
      name: displayName,
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
      yearProfitDisplay: fmtPlainSignedAmount(yearNative),
      yearProfitDisplayCny: fmtPlainSignedAmount(yearCny),
      yearWeightDisplay: "—",
      totalProfitDisplay: fmtPlainSignedAmount(totalNative),
      totalProfitDisplayCny: fmtPlainSignedAmount(totalCny),
      totalRateDisplay: fmtPercentRatio(totalRate),
      totalRateNum: totalRate,
      regretDisplay: "—",
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
  let monthDen = 0;
  let yearDen = 0;
  for (const sym of keys) {
    const snap = snapBySym.get(sym);
    const liveP = liveBySym.get(sym);
    if (!(liveP?.quantity > 1e-6)) {
      continue;
    }
    const todayCny = live.tradingDay ? Number(liveP?.todayProfitCny) || 0 : 0;
    monthDen += Math.abs((Number(snap?.month_profit_native) || 0) + todayCny);
    yearDen += Math.abs((Number(snap?.ytd_profit_native) || 0) + todayCny);
  }
  for (const row of rowsOut) {
    const sym = normalizeSymbol(row.symbol);
    const snap = snapBySym.get(sym);
    const liveP = liveBySym.get(sym);
    const todayCny = live.tradingDay ? Number(liveP?.todayProfitCny) || 0 : 0;
    const monthCny = (Number(snap?.month_profit_native) || 0) + todayCny;
    const yearCny = (Number(snap?.ytd_profit_native) || 0) + todayCny;
    const mv = Number(liveP?.marketValueCny) || 0;
    row.weightDisplay = totalAssets > 0 ? fmtPercentRatio(mv / totalAssets) : "0.00%";
    row.monthWeightDisplay = monthDen > 0 ? fmtPercentRatio(monthCny / monthDen) : "0.00%";
    row.yearWeightDisplay = yearDen > 0 ? fmtPercentRatio(yearCny / yearDen) : "0.00%";
  }

  rowsOut.sort((a, b) => {
    const pa = liveBySym.get(normalizeSymbol(a.symbol));
    const pb = liveBySym.get(normalizeSymbol(b.symbol));
    return (Number(pb?.marketValueCny) || 0) - (Number(pa?.marketValueCny) || 0);
  });

  return rowsOut;
}

module.exports = { buildHoldingsPayload };
