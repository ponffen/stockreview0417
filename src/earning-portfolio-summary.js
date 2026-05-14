/**
 * 服务端组合摘要：供 /api/state 与 /api/portfolio/summary。
 * 用 symbol_daily_close 作现价/昨收；analysis_daily_snapshot 最新行作 USD/HKD→CNY；
 * 与浏览器 computePortfolio 口径尽量一致（TWR 列仍用 profitRate 近似，表格「月/年」等仍主要依赖日快照合并）。
 */
const { toDateKey } = require("../scripts/lib/market-fetch");
const {
  q,
  getTrades,
  getCashTransfers,
  getAccounts,
  getSettings,
  getLatestSymbolDailyClose,
  getSymbolDailyCloseRange,
  normalizeSymbol,
  isUsTickerSymbol,
} = require("./db");

const FX_FALLBACK = { CNY: 1, HKD: 0.92, USD: 7.2 };

function validNumber(...values) {
  for (const v of values) {
    const n = Number(v);
    if (Number.isFinite(n)) {
      return n;
    }
  }
  return 0;
}

function inferMarket(symbol) {
  const s = String(symbol || "").trim().toLowerCase();
  if (!s) {
    return "其他";
  }
  if (s.startsWith("sh") || s.startsWith("sz")) {
    return "A股";
  }
  if (s.startsWith("hk") || s.startsWith("rt_hk")) {
    return "港股";
  }
  if (isUsTickerSymbol(s)) {
    return "美股";
  }
  return "其他";
}

function getSymbolCurrency(symbol, market = inferMarket(symbol)) {
  if (market === "港股") {
    return "HKD";
  }
  if (market === "美股") {
    return "USD";
  }
  return "CNY";
}

function sortTradeAsc(a, b) {
  const da = String(a.date || "").localeCompare(String(b.date || ""));
  if (da !== 0) {
    return da;
  }
  return Number(a.createdAt || 0) - Number(b.createdAt || 0);
}

function signedAmount(trade) {
  return trade.side === "buy" ? trade.amount : -trade.amount;
}

function filterTradesByAccount(trades, accountId) {
  const list = Array.isArray(trades) ? trades : [];
  if (!accountId || accountId === "all") {
    return [...list];
  }
  return list.filter((t) => String(t.accountId || "default") === String(accountId));
}

function filterCashByAccount(rows, accountId) {
  const list = Array.isArray(rows) ? rows : [];
  if (!accountId || accountId === "all") {
    return [...list];
  }
  return list.filter((r) => String(r.accountId) === String(accountId));
}

function cashTransferRowNetCny(r, accounts, fxUsd, fxHkd) {
  const acc = accounts.find((a) => String(a.id) === String(r.accountId));
  const ccy = String((acc && acc.currency) || "CNY").toUpperCase();
  const sign = r.direction === "out" ? -1 : 1;
  const nat = sign * Math.abs(Number(r.amount) || 0);
  if (!Number.isFinite(nat) || nat === 0) {
    return 0;
  }
  if (ccy === "CNY") {
    return nat;
  }
  if (ccy === "USD") {
    return nat * fxUsd;
  }
  if (ccy === "HKD") {
    return nat * fxHkd;
  }
  return nat;
}

function getTradeFxRate(trade, fxUsd, fxHkd) {
  const market = inferMarket(trade.symbol);
  const currency = getSymbolCurrency(trade.symbol, market);
  if (currency === "CNY") {
    return 1;
  }
  if (currency === "USD") {
    return fxUsd;
  }
  if (currency === "HKD") {
    return fxHkd;
  }
  return 1;
}

function getPositionDayTradeContext(symbol, dateKey, tradeList) {
  const symbolTrades = tradeList.filter((trade) => trade.symbol === symbol).sort(sortTradeAsc);
  let startQuantity = 0;
  let endQuantity = 0;
  let dayFlowNative = 0;
  for (const trade of symbolTrades) {
    const deltaQty = trade.side === "buy" ? trade.quantity : -trade.quantity;
    if (trade.date < dateKey) {
      startQuantity += deltaQty;
    }
    if (trade.date <= dateKey) {
      endQuantity += deltaQty;
    }
    if (trade.date === dateKey) {
      dayFlowNative += signedAmount(trade);
    }
  }
  return { startQuantity, endQuantity, dayFlowNative };
}

async function loadLatestFxFromAnalysis(uid, accountId) {
  const acc = String(accountId || "all").trim() || "all";
  const { rows } = await q(
    `SELECT fx_hkd_cny, fx_usd_cny FROM analysis_daily_snapshot
     WHERE user_id = $1 AND account_id = $2
     ORDER BY date DESC NULLS LAST
     LIMIT 1`,
    [uid, acc]
  );
  const r = rows[0] || {};
  const fxUsd = validNumber(r.fx_usd_cny, FX_FALLBACK.USD);
  const fxHkd = validNumber(r.fx_hkd_cny, FX_FALLBACK.HKD);
  return { fxUsd, fxHkd };
}

async function buildKlineForSymbol(symbol) {
  const sym = normalizeSymbol(symbol);
  if (!sym) {
    return [];
  }
  const to = toDateKey(new Date());
  const rows = await getSymbolDailyCloseRange(sym, "2010-01-01", to);
  return Array.isArray(rows)
    ? rows.map((x) => ({ day: String(x.date || "").slice(0, 10), close: Number(x.close) }))
    : [];
}

function getSymbolCloseBeforeDateFromKline(kline, dateKey, fallbackPrice) {
  const list = Array.isArray(kline) ? kline : [];
  for (let i = list.length - 1; i >= 0; i -= 1) {
    const item = list[i];
    if (item.day < dateKey && Number.isFinite(Number(item.close))) {
      return Number(item.close);
    }
  }
  return validNumber(fallbackPrice, 0);
}

function getSymbolCloseOnOrBeforeKeyFromKline(kline, dateKey, fallbackPrice) {
  const list = Array.isArray(kline) ? kline : [];
  for (let i = list.length - 1; i >= 0; i -= 1) {
    const item = list[i];
    if (item.day <= dateKey && Number.isFinite(Number(item.close))) {
      return Number(item.close);
    }
  }
  return validNumber(fallbackPrice, 0);
}

function computePositionProfitInDateRange(position, startKey, endKey, symbolTrades, kline) {
  if (!symbolTrades.length) {
    return 0;
  }
  let startQuantity = 0;
  let endQuantity = 0;
  let stageFlowNative = 0;
  for (const trade of symbolTrades) {
    const delta = trade.side === "buy" ? trade.quantity : -trade.quantity;
    if (trade.date < startKey) {
      startQuantity += delta;
    }
    if (trade.date <= endKey) {
      endQuantity += delta;
    }
    if (trade.date >= startKey && trade.date <= endKey) {
      stageFlowNative += signedAmount(trade);
    }
  }
  const startClose = getSymbolCloseBeforeDateFromKline(kline, startKey, position.prevClose);
  const endClose = getSymbolCloseOnOrBeforeKeyFromKline(
    kline,
    endKey,
    validNumber(position.currentPrice, position.prevClose),
  );
  const startMv = startQuantity * startClose;
  const endMv = endQuantity * endClose;
  return endMv - startMv - stageFlowNative;
}

function getStageStartKey(stageRange, firstDate) {
  const today = new Date();
  const start = new Date(today);
  if (stageRange === "week") {
    start.setDate(today.getDate() - 6);
  } else if (stageRange === "month") {
    start.setDate(1);
  } else if (stageRange === "quarter") {
    start.setDate(today.getDate() - 89);
  } else if (stageRange === "ytd") {
    start.setMonth(0, 1);
  } else if (stageRange === "total" && firstDate) {
    return firstDate;
  }
  return toDateKey(start);
}

function computePositionStageProfit(position, stageRange, tradeList, kline) {
  const firstTradeDate = tradeList.length ? [...tradeList].sort(sortTradeAsc)[0].date : toDateKey(new Date());
  const startKey = getStageStartKey(stageRange, firstTradeDate);
  const endKey = toDateKey(new Date());
  const symbolTrades = tradeList.filter((t) => t.symbol === position.symbol).sort(sortTradeAsc);
  return computePositionProfitInDateRange(position, startKey, endKey, symbolTrades, kline);
}

function overviewBookCurrencyForAccount(accounts, accountId) {
  if (!accountId || accountId === "all") {
    return "CNY";
  }
  const acc = accounts.find((a) => String(a.id) === String(accountId));
  const c = String(acc?.currency || "CNY").toUpperCase();
  if (c === "USD" || c === "HKD" || c === "CNY") {
    return c;
  }
  return "CNY";
}

function applyFxForOverviewRow(row, nativeVal, stockAmountDisplay) {
  const cnyBook = row.currency === "CNY" || row.market === "A股";
  const n = Number.isFinite(Number(nativeVal)) ? Number(nativeVal) : 0;
  if (cnyBook) {
    return n;
  }
  if (stockAmountDisplay === "cny") {
    return n * (validNumber(row.fxRate, 1) || 1);
  }
  return n;
}

function nativeToOverviewBook(row, nativeVal, bookCcy, fxUsd, fxHkd) {
  const ccy = row.currency || "CNY";
  const n = Number.isFinite(Number(nativeVal)) ? Number(nativeVal) : 0;
  if (bookCcy === "CNY") {
    if (ccy === "CNY" || row.market === "A股") {
      return n;
    }
    const fx = ccy === "USD" ? fxUsd : ccy === "HKD" ? fxHkd : 1;
    return n * fx;
  }
  if (bookCcy === ccy) {
    return n;
  }
  const inCny =
    ccy === "CNY" || row.market === "A股" ? n : ccy === "USD" ? n * fxUsd : ccy === "HKD" ? n * fxHkd : n;
  if (bookCcy === "USD") {
    return inCny / fxUsd;
  }
  if (bookCcy === "HKD") {
    return inCny / fxHkd;
  }
  return inCny;
}

function amountBookFromCny(principalCny, bookCcy, fxUsd, fxHkd) {
  if (bookCcy === "CNY") {
    return principalCny;
  }
  if (bookCcy === "USD") {
    return principalCny / fxUsd;
  }
  if (bookCcy === "HKD") {
    return principalCny / fxHkd;
  }
  return principalCny;
}

function emptyPortfolio(bookCcy) {
  return {
    positions: [],
    visiblePositions: [],
    sigmaAmountAll: 0,
    principal: 0,
    overviewBookCurrency: bookCcy || "CNY",
    overviewPrincipal: 0,
    overviewCash: 0,
    totalMarketValue: 0,
    yesterdayMarketValue: 0,
    cash: 0,
    totalAssets: 0,
    todayProfit: 0,
    todayRate: 0,
    totalProfit: 0,
    externalFlowTodayCny: 0,
  };
}

async function buildPortfolioSummaryForAccount(userId, accountId) {
  const uid = String(userId || "").trim();
  const aid = String(accountId || "all").trim() || "all";
  if (!uid) {
    return emptyPortfolio("CNY");
  }
  const [tradesAll, cashAll, accounts, settings] = await Promise.all([
    getTrades(uid),
    getCashTransfers(uid),
    getAccounts(uid),
    getSettings(uid),
  ]);
  const tradeList = filterTradesByAccount(tradesAll, aid);
  const ctf = filterCashByAccount(cashAll, aid);
  const fxScope = aid === "all" ? "all" : aid;
  const { fxUsd, fxHkd } = await loadLatestFxFromAnalysis(uid, fxScope);
  const bookCcy = overviewBookCurrencyForAccount(accounts, aid);
  const stockAmountDisplay = settings.stockAmountDisplay === "cny" ? "cny" : "native";

  const todayKey = toDateKey(new Date());
  const externalFlowTodayCny = ctf.reduce((s, r) => {
    if (String(r.date).slice(0, 10) === todayKey) {
      return s + cashTransferRowNetCny(r, accounts, fxUsd, fxHkd);
    }
    return s;
  }, 0);

  const grouped = new Map();
  const sortedTrades = [...tradeList].sort(sortTradeAsc);
  for (const trade of sortedTrades) {
    if (!grouped.has(trade.symbol)) {
      grouped.set(trade.symbol, {
        symbol: trade.symbol,
        name: trade.name || trade.symbol,
        market: inferMarket(trade.symbol),
        quantity: 0,
        sigmaAmount: 0,
        lastTradePrice: trade.price,
        lastTradeSide: trade.side,
        lastTradeDate: trade.date,
      });
    }
    const item = grouped.get(trade.symbol);
    item.name = trade.name || item.name;
    item.market = inferMarket(trade.symbol);
    item.quantity += trade.side === "buy" ? trade.quantity : -trade.quantity;
    item.sigmaAmount += signedAmount(trade);
    if (trade.price > 0) {
      item.lastTradePrice = trade.price;
    }
    item.lastTradeSide = trade.side;
    item.lastTradeDate = trade.date;
  }

  const symKeys = [...new Set([...grouped.keys()].map((s) => normalizeSymbol(s)).filter(Boolean))];
  const klineBySym = new Map();
  const latestCloseBySym = new Map();
  await Promise.all(
    symKeys.map(async (sym) => {
      const [kline, latest] = await Promise.all([
        buildKlineForSymbol(sym),
        getLatestSymbolDailyClose(sym),
      ]);
      klineBySym.set(sym, kline);
      latestCloseBySym.set(sym, latest);
    }),
  );

  const positions = [...grouped.values()].map((item) => {
    const sym = item.symbol;
    const ns = normalizeSymbol(sym);
    const kline = klineBySym.get(ns) || [];
    const lc = latestCloseBySym.get(ns);
    const market = inferMarket(sym);
    const currency = getSymbolCurrency(sym, market);
    const fxRate = currency === "CNY" ? 1 : currency === "USD" ? fxUsd : fxHkd;
    const closeFromDb = lc && Number.isFinite(Number(lc.close)) && Number(lc.close) > 0 ? Number(lc.close) : 0;
    const currentPrice = validNumber(closeFromDb, item.lastTradePrice);
    const prevRow = kline.length >= 2 ? kline[kline.length - 2] : null;
    const prevClose = validNumber(
      prevRow && Number.isFinite(prevRow.close) ? prevRow.close : 0,
      currentPrice,
      item.lastTradePrice,
    );
    const marketValueNative = item.quantity * currentPrice;
    const yesterdayValueNative = item.quantity * prevClose;
    const sigmaAmountNative = item.sigmaAmount;
    const marketValue = currency === "CNY" ? marketValueNative : marketValueNative * fxRate;
    const yesterdayValue = currency === "CNY" ? yesterdayValueNative : yesterdayValueNative * fxRate;
    const sigmaAmountCny = currency === "CNY" ? sigmaAmountNative : sigmaAmountNative * fxRate;
    const cost = item.quantity !== 0 ? item.sigmaAmount / item.quantity : 0;
    const totalProfitNative = marketValueNative - sigmaAmountNative;
    const profitRate =
      Math.abs(sigmaAmountNative) > 0 ? totalProfitNative / Math.abs(sigmaAmountNative) : 0;
    const closeDate = lc && lc.date ? String(lc.date).slice(0, 10) : "";
    const countTodayPnl = Boolean(closeDate && closeDate === todayKey);
    const dayCtx = getPositionDayTradeContext(sym, todayKey, sortedTrades);
    const todayStartMarketValueNative = dayCtx.startQuantity * prevClose;
    const todayProfitNative = countTodayPnl
      ? dayCtx.endQuantity * currentPrice - todayStartMarketValueNative - dayCtx.dayFlowNative
      : 0;
    const dayChangeRate = prevClose > 0 ? (currentPrice - prevClose) / prevClose : 0;
    const regretRate =
      item.lastTradePrice > 0 ? (currentPrice - item.lastTradePrice) / item.lastTradePrice : 0;
    const monthProfitNative = computePositionStageProfit(
      {
        symbol: sym,
        prevClose,
        currentPrice,
      },
      "month",
      sortedTrades,
      kline,
    );
    const yearProfitNative = computePositionStageProfit(
      {
        symbol: sym,
        prevClose,
        currentPrice,
      },
      "ytd",
      sortedTrades,
      kline,
    );
    return {
      ...item,
      market,
      currency,
      fxRate,
      currentPrice,
      prevClose,
      marketValueNative,
      yesterdayValueNative,
      sigmaAmountNative,
      marketValue,
      yesterdayValue,
      sigmaAmountCny,
      cost,
      totalProfitNative,
      profitRate,
      todayStartMarketValueNative,
      todayProfitNative,
      dayChangeRate,
      regretRate,
      totalRate: profitRate,
      totalProfit: totalProfitNative,
      todayProfit: todayProfitNative,
      monthProfitNative,
      yearProfitNative,
      monthProfit: monthProfitNative,
      yearProfit: yearProfitNative,
    };
  });

  const visiblePositions = positions.filter((it) => it.quantity > 0);
  const monthDen = visiblePositions.reduce(
    (sum, item) => sum + Math.abs(applyFxForOverviewRow(item, item.monthProfitNative, stockAmountDisplay)),
    0,
  );
  const yearDen = visiblePositions.reduce(
    (sum, item) => sum + Math.abs(applyFxForOverviewRow(item, item.yearProfitNative, stockAmountDisplay)),
    0,
  );
  visiblePositions.forEach((item) => {
    const mp = applyFxForOverviewRow(item, item.monthProfitNative, stockAmountDisplay);
    const yp = applyFxForOverviewRow(item, item.yearProfitNative, stockAmountDisplay);
    item.monthWeight = monthDen !== 0 ? mp / monthDen : 0;
    item.yearWeight = yearDen !== 0 ? yp / yearDen : 0;
  });

  const sigmaAmountAll = tradeList.reduce((sum, trade) => sum + signedAmount(trade) * getTradeFxRate(trade, fxUsd, fxHkd), 0);
  const sigmaFundCny = ctf.reduce((sum, r) => sum + cashTransferRowNetCny(r, accounts, fxUsd, fxHkd), 0);
  const principal = Math.max(sigmaAmountAll, sigmaFundCny, 0);
  const totalMarketValueCnyBook = visiblePositions.reduce((sum, item) => sum + item.marketValue, 0);
  const cash = principal - sigmaAmountAll;

  const sumBookByCurrency = (getNative) => {
    const byCcy = Object.create(null);
    for (const item of visiblePositions) {
      const ccy = item.currency || "CNY";
      const v = getNative(item);
      if (!Number.isFinite(v) || v === 0) {
        continue;
      }
      byCcy[ccy] = (byCcy[ccy] || 0) + v;
    }
    let sum = 0;
    for (const ccy of Object.keys(byCcy)) {
      const row = visiblePositions.find((p) => (p.currency || "CNY") === ccy);
      if (row) {
        sum += nativeToOverviewBook(row, byCcy[ccy], bookCcy, fxUsd, fxHkd);
      }
    }
    return sum;
  };

  const totalMarketValue = visiblePositions.reduce(
    (sum, item) => sum + nativeToOverviewBook(item, item.marketValueNative, bookCcy, fxUsd, fxHkd),
    0,
  );
  const todayProfit = sumBookByCurrency((item) => item.todayProfitNative);
  const yesterdayMarketValueForRate = visiblePositions.reduce(
    (sum, item) => sum + nativeToOverviewBook(item, item.todayStartMarketValueNative, bookCcy, fxUsd, fxHkd),
    0,
  );
  const todayRate = yesterdayMarketValueForRate !== 0 ? todayProfit / yesterdayMarketValueForRate : 0;
  const totalProfit = sumBookByCurrency((item) => item.totalProfitNative);
  const overviewPrincipal = amountBookFromCny(principal, bookCcy, fxUsd, fxHkd);
  const overviewCash = amountBookFromCny(cash, bookCcy, fxUsd, fxHkd);
  const totalAssets = totalMarketValue + overviewCash;
  const totalAssetsForWeight = totalMarketValueCnyBook + cash;
  positions.forEach((item) => {
    item.weight = totalAssetsForWeight !== 0 ? item.marketValue / totalAssetsForWeight : 0;
  });
  positions.sort((a, b) => Math.abs(b.marketValue) - Math.abs(a.marketValue));

  return {
    positions,
    visiblePositions,
    sigmaAmountAll,
    principal,
    overviewBookCurrency: bookCcy,
    overviewPrincipal,
    overviewCash,
    totalMarketValue,
    yesterdayMarketValue: yesterdayMarketValueForRate,
    cash,
    totalAssets,
    todayProfit,
    todayRate,
    totalProfit,
    externalFlowTodayCny,
  };
}

async function buildPortfolioSummariesForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { all: emptyPortfolio("CNY") };
  }
  const accounts = await getAccounts(uid);
  const ids = ["all", ...accounts.map((a) => String(a.id || "").trim()).filter(Boolean)];
  const out = {};
  await Promise.all(
    ids.map(async (id) => {
      out[id] = await buildPortfolioSummaryForAccount(uid, id);
    }),
  );
  return out;
}

module.exports = {
  buildPortfolioSummaryForAccount,
  buildPortfolioSummariesForUser,
};
