/**
 * TWR / 组合日序列（账户级）：NAV 用市值；TWR 现金流仅「出入金」；
 * tradeFlow 仅用于首日 BMV 估计，不进入 TWR 分子减项。
 */

const validNumber = (v, fb = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fb;
};

function inferMarket(symbol) {
  const s = String(symbol || "");
  if (s.startsWith("sh") || s.startsWith("sz")) return "A股";
  if (s.startsWith("hk") || s.startsWith("rt_hk")) return "港股";
  if (s.startsWith("gb_") || /^[a-z]/i.test(s)) return "美股";
  return "其他";
}

function getSymbolCurrency(symbol, market = inferMarket(symbol)) {
  if (market === "港股") return "HKD";
  if (market === "美股") return "USD";
  return "CNY";
}

function fxToCnyOnDate(fxUsdMap, fxHkdMap, currency, dateKey, fxFallback = { USD: 7.2, HKD: 0.92 }) {
  if (currency === "CNY") return 1;
  if (currency === "USD") return validNumber(fxUsdMap[dateKey], fxFallback.USD);
  if (currency === "HKD") return validNumber(fxHkdMap[dateKey], fxFallback.HKD);
  return 1;
}

function signedAmount(trade) {
  return trade.side === "buy" ? trade.amount : -trade.amount;
}

function normalizeSymbol(s) {
  return String(s || "")
    .trim()
    .toLowerCase();
}

function filterTradesForAccount(allTrades, accountId) {
  if (accountId === "all") return [...allTrades];
  return allTrades.filter((t) => String(t.accountId) === String(accountId));
}

function filterCashForAccount(allCash, accountId) {
  if (accountId === "all") return [...allCash];
  return allCash.filter((c) => String(c.accountId) === String(accountId));
}

function cashTransferNetCnyForRow(r, accById, fxUsdMap, fxHkdMap) {
  const acc = accById.get(String(r.accountId)) || { currency: "CNY" };
  const ccy = String(acc.currency || "CNY").toUpperCase();
  const sign = r.direction === "out" ? -1 : 1;
  const nat = sign * Math.abs(validNumber(r.amount, 0));
  if (!Number.isFinite(nat) || nat === 0) {
    return 0;
  }
  const d = String(r.date).slice(0, 10);
  return ccy === "CNY" ? nat : nat * fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, d);
}

function cashTransferNetNativeForRow(r, accById) {
  const acc = accById.get(String(r.accountId)) || { currency: "CNY" };
  const sign = r.direction === "out" ? -1 : 1;
  const nat = sign * Math.abs(validNumber(r.amount, 0));
  return Number.isFinite(nat) ? nat : 0;
}

/** 每个业务日：出入金折人民币净额（与 NAV 同为 CNY 口径，供 TWR） */
function buildExternalFlowCnyByDate(allCash, accountId, accounts, allDates, fxUsdMap, fxHkdMap) {
  const accById = new Map(accounts.map((a) => [String(a.id), a]));
  const m = new Map();
  for (const D of allDates) {
    m.set(D, 0);
  }
  const filtered = filterCashForAccount(allCash, accountId);
  for (const r of filtered) {
    const d = String(r.date).slice(0, 10);
    if (!m.has(d)) continue;
    const v = cashTransferNetCnyForRow(r, accById, fxUsdMap, fxHkdMap);
    m.set(d, (m.get(d) || 0) + v);
  }
  return m;
}

function closeOnOrBefore(sortedKline, dateKey) {
  let lo = 0;
  let hi = sortedKline.length - 1;
  let ans = null;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const d = sortedKline[mid].day;
    if (d <= dateKey) {
      ans = sortedKline[mid].close;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans;
}

function sortTradeAsc(a, b) {
  const ad = new Date(a.date).getTime();
  const bd = new Date(b.date).getTime();
  if (ad !== bd) return ad - bd;
  return Number(a.createdAt) - Number(b.createdAt);
}

/** NAV 与 TWR 出入金均按「折人民币」口径；`external_flow_native` 存子账户当日出入金原币净额（审计用）。 */
function buildPortfolioDayPoints(accountTrades, dateKeys, klineBySym, fxUsd, fxHkd, allCash, accountId, accounts) {
  const accById = new Map(accounts.map((a) => [String(a.id), a]));
  const extCnyByDate = buildExternalFlowCnyByDate(allCash, accountId, accounts, dateKeys, fxUsd, fxHkd);
  const extNativeByDate = new Map();
  for (const D of dateKeys) {
    extNativeByDate.set(D, 0);
  }
  if (accountId !== "all") {
    const filtered = filterCashForAccount(allCash, accountId);
    for (const r of filtered) {
      const d = String(r.date).slice(0, 10);
      if (!extNativeByDate.has(d)) continue;
      extNativeByDate.set(d, (extNativeByDate.get(d) || 0) + cashTransferNetNativeForRow(r, accById));
    }
  }

  const symbolSet = [...new Set(accountTrades.map((t) => normalizeSymbol(t.symbol)).filter(Boolean))];
  const closeMemo = new Map();
  const getClose = (sym, dk) => {
    const key = `${sym}|${dk}`;
    if (closeMemo.has(key)) return closeMemo.get(key);
    const kl = klineBySym.get(sym);
    if (!kl || !kl.length) {
      closeMemo.set(key, null);
      return null;
    }
    const v = closeOnOrBefore(kl, dk);
    closeMemo.set(key, v);
    return v;
  };

  const tradesByDate = {};
  for (const tr of accountTrades) {
    if (!tradesByDate[tr.date]) tradesByDate[tr.date] = [];
    tradesByDate[tr.date].push(tr);
  }
  Object.values(tradesByDate).forEach((list) => list.sort(sortTradeAsc));

  const holdings = {};
  const points = [];
  for (const dateKey of dateKeys) {
    const dailyTrades = tradesByDate[dateKey] || [];
    for (const tr of dailyTrades) {
      const sym = normalizeSymbol(tr.symbol);
      if (!sym) continue;
      if (holdings[sym] == null) holdings[sym] = 0;
      holdings[sym] += tr.side === "buy" ? tr.quantity : -tr.quantity;
    }
    let tradeFlow = 0;
    for (const tr of dailyTrades) {
      const m = inferMarket(tr.symbol);
      const ccy = getSymbolCurrency(tr.symbol, m);
      const fx = fxToCnyOnDate(fxUsd, fxHkd, ccy, dateKey);
      tradeFlow += signedAmount(tr) * fx;
    }
    let value = 0;
    for (const sym of symbolSet) {
      const q = holdings[sym] || 0;
      if (q === 0) continue;
      const c = getClose(sym, dateKey);
      if (!(c > 0)) continue;
      const m = inferMarket(sym);
      const ccy = getSymbolCurrency(sym, m);
      const fx = fxToCnyOnDate(fxUsd, fxHkd, ccy, dateKey);
      value += q * c * fx;
    }
    const extFlow = extCnyByDate.get(dateKey) || 0;
    const extNative = accountId === "all" ? 0 : extNativeByDate.get(dateKey) || 0;
    points.push({ date: dateKey, nav: value, extFlow, tradeFlow, externalFlowNative: extNative });
  }
  return points;
}

/** 日 TWR 链式收益 + 累计 TWR */
function computeTwrFromDayPoints(points) {
  const out = [];
  let compound = 1;
  let prevNav = null;
  for (let i = 0; i < points.length; i += 1) {
    const { date, nav, extFlow, tradeFlow } = points[i];
    let daily = 0;
    if (i === 0) {
      const bmv = Math.max(nav - extFlow - tradeFlow, 1e-9);
      const denom = bmv + Math.max(extFlow, 0);
      daily = denom !== 0 ? (nav - bmv - extFlow) / denom : 0;
      prevNav = nav;
    } else {
      const denom = prevNav + Math.max(extFlow, 0);
      daily = denom !== 0 ? (nav - prevNav - extFlow) / denom : 0;
      prevNav = nav;
    }
    compound *= 1 + daily;
    const twCumulative = compound - 1;
    out.push({ date, twRDaily: daily, twRCumulative: twCumulative });
  }
  return out;
}

module.exports = {
  buildPortfolioDayPoints,
  computeTwrFromDayPoints,
  fxToCnyOnDate,
  signedAmount,
  normalizeSymbol,
  inferMarket,
  getSymbolCurrency,
  filterTradesForAccount,
  filterCashForAccount,
  cashTransferNetCnyForRow,
};
