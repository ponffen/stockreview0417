#!/usr/bin/env node
/**
 * 历史回填：清空后重算 symbol_daily_pnl、analysis_daily_snapshot。
 *
 * 1) symbol_daily_pnl：按账户、标的、自然日，用收盘价（优先 symbol_daily_close，缺省再合并新浪）
 *    计算当日持仓与当日收益 day_pnl_native（原币，不乘汇率）。
 * 2) analysis_daily_snapshot：每个账户、每个自然日先在原币侧按币种（CNY/HKD/USD）汇总各标的日收益，
 *    再对各币种合计 × 当日对人民币汇率，加总得到 profit_cny；再与人民币总市值序列算收益率等。
 * 3) 外汇中间价：新浪日 K（需网络）。
 */
const path = require("node:path");

const {
  fetchKlineDataSina,
  fetchSinaForexDayKSeries,
  toDateKey,
  enumerateDays,
  validNumber,
} = require("./lib/market-fetch");

const {
  getTrades,
  getSettings,
  normalizeSymbol,
  getSymbolDailyCloseRange,
  upsertSymbolDailyPnlBatch,
  upsertAnalysisDailySnapshot,
  deleteAllSymbolDailyPnl,
  deleteAllAnalysisDailySnapshot,
} = require(path.join(__dirname, "..", "src", "db"));

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

function signedAmount(trade) {
  return trade.side === "buy" ? trade.amount : -trade.amount;
}

function sortTradeAsc(a, b) {
  const ad = new Date(a.date).getTime();
  const bd = new Date(b.date).getTime();
  if (ad !== bd) return ad - bd;
  return Number(a.createdAt) - Number(b.createdAt);
}

function fxToCnyOnDate(fxUsdMap, fxHkdMap, currency, dateKey) {
  if (currency === "CNY") return 1;
  if (currency === "USD") return validNumber(fxUsdMap[dateKey], 7.2);
  if (currency === "HKD") return validNumber(fxHkdMap[dateKey], 0.92);
  return 1;
}

/** 汇总层：同一账户、同一自然日、同一币种下累加各标的原币日收益（先不加汇率） */
function addDailyProfitNativeByCurrency(accDateNative, accountId, dateKey, pnlNative, currency) {
  const pk = `${accountId}|${dateKey}`;
  let byCcy = accDateNative.get(pk);
  if (!byCcy) {
    byCcy = {};
    accDateNative.set(pk, byCcy);
  }
  byCcy[currency] = (byCcy[currency] || 0) + pnlNative;
}

/** 将「按币种汇总后的原币日收益」乘以当日汇率，得到当日人民币收益合计 */
function profitCnyFromAccDateNative(byCcy, dateKey, fxUsdMap, fxHkdMap) {
  if (!byCcy) return 0;
  let sum = 0;
  for (const [ccy, amt] of Object.entries(byCcy)) {
    sum += amt * fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, dateKey);
  }
  return sum;
}

/** max day <= dateKey 的收盘价 */
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

/** 严格早于 dateKey 的最后一根收盘价 */
function closeBefore(sortedKline, dateKey) {
  let lo = 0;
  let hi = sortedKline.length - 1;
  let ans = null;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const d = sortedKline[mid].day;
    if (d < dateKey) {
      ans = sortedKline[mid].close;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans;
}

function computeCostSeries(points) {
  if (!points.length) return [];
  const result = [];
  const startClose = points[0].value - points[0].flow;
  let sumFlow = 0;
  points.forEach((point) => {
    sumFlow += point.flow;
    const profit = point.value - startClose - sumFlow;
    const denominator = startClose + sumFlow;
    const rate = denominator !== 0 ? profit / denominator : 0;
    result.push({ date: point.date, rate });
  });
  return result;
}

function computeMoneyWeightedSeries(points) {
  if (!points.length) return [];
  const result = [];
  const startClose = points[0].value - points[0].flow;
  const flows = [];
  points.forEach((point, index) => {
    flows.push(point.flow);
    const totalPeriods = index + 1;
    let weightedFlow = 0;
    let sumFlow = 0;
    flows.forEach((flow, flowIdx) => {
      const weight = (totalPeriods - flowIdx) / totalPeriods;
      weightedFlow += flow * weight;
      sumFlow += flow;
    });
    const profit = point.value - startClose - sumFlow;
    const denominator = startClose + weightedFlow;
    const rate = denominator !== 0 ? profit / denominator : 0;
    result.push({ date: point.date, rate });
  });
  return result;
}

function computeTimeWeightedSeries(points) {
  if (!points.length) return [];
  const result = [];
  let compounded = 1;
  let prevValue = points[0].value - points[0].flow;
  points.forEach((point) => {
    const denominator = prevValue + Math.max(point.flow, 0);
    const dailyRate = denominator !== 0 ? (point.value - prevValue - point.flow) / denominator : 0;
    compounded *= 1 + dailyRate;
    result.push({ date: point.date, rate: compounded - 1 });
    prevValue = point.value;
  });
  return result;
}

function buildPortfolioHistoryCny(accountTrades, dateKeys, klineBySym, fxUsd, fxHkd) {
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
    let flow = 0;
    for (const tr of dailyTrades) {
      const m = inferMarket(tr.symbol);
      const ccy = getSymbolCurrency(tr.symbol, m);
      const fx = fxToCnyOnDate(fxUsd, fxHkd, ccy, dateKey);
      flow += signedAmount(tr) * fx;
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
    points.push({ date: dateKey, value, flow });
  }
  return points;
}

function filterTradesForAccount(allTrades, accountId) {
  if (accountId === "all") return [...allTrades].sort(sortTradeAsc);
  return allTrades.filter((t) => t.accountId === accountId).sort(sortTradeAsc);
}

/** 优先采用 symbol_daily_close，再叠加新浪补缺；输出与 fetchKlineDataSina 相同 { day, close }[] */
function mergeKlinePreferDb(dbRows, sinaRows) {
  const map = new Map();
  for (const r of sinaRows) {
    if (r?.day && Number.isFinite(r.close) && r.close > 0) {
      map.set(r.day, r.close);
    }
  }
  for (const r of dbRows) {
    const d = r?.date != null ? String(r.date).slice(0, 10) : "";
    if (d && Number.isFinite(r.close) && r.close > 0) {
      map.set(d, r.close);
    }
  }
  const days = [...map.keys()].sort((a, b) => a.localeCompare(b));
  return days.map((day) => ({ day, close: map.get(day) }));
}

async function main() {
  const now = Date.now();
  console.log("[backfill] 清空两张日表…");
  deleteAllSymbolDailyPnl();
  deleteAllAnalysisDailySnapshot();

  const allTrades = getTrades().map((t) => ({
    ...t,
    symbol: normalizeSymbol(t.symbol),
  }));
  if (!allTrades.length) {
    console.log("[backfill] 无交易，退出。");
    process.exit(0);
  }

  const settings = getSettings();
  const capitalAmount = validNumber(settings.capitalAmount, 0);

  console.log("[backfill] 拉取外汇日 K…");
  const fxUsdMap = await fetchSinaForexDayKSeries("usdcny", "USDCNY");
  const fxHkdMap = await fetchSinaForexDayKSeries("hkdcny", "HKDCNY");

  let minD = allTrades[0].date;
  let maxD = allTrades[0].date;
  for (const t of allTrades) {
    if (t.date < minD) minD = t.date;
    if (t.date > maxD) maxD = t.date;
  }
  const today = toDateKey(new Date());
  if (maxD < today) maxD = today;

  const symbols = [...new Set(allTrades.map((t) => t.symbol).filter(Boolean))].sort();
  console.log(`[backfill] 合并日 K（优先 symbol_daily_close）+ 新浪，共 ${symbols.length} 标的…`);
  const klineBySym = new Map();
  for (let i = 0; i < symbols.length; i += 1) {
    const sym = symbols[i];
    const dbRows = getSymbolDailyCloseRange(sym, minD, maxD);
    let sinaRows = [];
    try {
      sinaRows = await fetchKlineDataSina(sym, 1023);
    } catch (e) {
      console.warn(`  [warn] 新浪K线失败 ${sym}:`, e.message || e);
    }
    const merged = mergeKlinePreferDb(dbRows, sinaRows);
    klineBySym.set(sym, merged);
    if (dbRows.length) {
      console.log(`  ${sym}: 本地 ${dbRows.length} 条 → 合并后 ${merged.length} 根`);
    } else if ((i + 1) % 10 === 0) {
      console.log(`  … ${i + 1}/${symbols.length}`);
    }
    await new Promise((r) => setTimeout(r, 120));
  }

  const allDates = enumerateDays(minD, maxD);
  const accountIds = ["all", ...new Set(allTrades.map((t) => String(t.accountId || "default")))];
  /** `${accountId}|${date}` -> { CNY?: number, HKD?: number, USD?: number } 原币日收益按币种已加总 */
  const accDateNative = new Map();

  const symbolRowsBuffer = [];
  const flushSym = () => {
    if (!symbolRowsBuffer.length) return;
    upsertSymbolDailyPnlBatch(symbolRowsBuffer.splice(0, symbolRowsBuffer.length));
  };

  for (const accountId of accountIds) {
    const accTrades = filterTradesForAccount(allTrades, accountId);
    if (!accTrades.length) continue;

    for (const sym of symbols) {
      const symTrades = accTrades.filter((t) => t.symbol === sym).sort(sortTradeAsc);
      if (!symTrades.length) continue;
      const kl = klineBySym.get(sym) || [];
      if (!kl.length) continue;

      let pi = 0;
      let qty = 0;
      for (const D of allDates) {
        while (pi < symTrades.length && symTrades[pi].date < D) {
          const u = symTrades[pi];
          qty += u.side === "buy" ? u.quantity : -u.quantity;
          pi += 1;
        }
        const qBod = qty;
        const dayTrades = [];
        while (pi < symTrades.length && symTrades[pi].date === D) {
          dayTrades.push(symTrades[pi]);
          const u = symTrades[pi];
          qty += u.side === "buy" ? u.quantity : -u.quantity;
          pi += 1;
        }
        const qEod = qty;
        if (qBod <= 0 && qEod <= 0) continue;

        const closeD = closeOnOrBefore(kl, D);
        const closePrev = closeBefore(kl, D);
        if (!(closeD > 0)) continue;
        const prevPx = closePrev != null && closePrev > 0 ? closePrev : closeD;

        let dayFlow = 0;
        let dayAmt = 0;
        let dayTurnoverQty = 0;
        for (const u of dayTrades) {
          dayFlow += signedAmount(u);
          dayAmt += validNumber(u.amount, 0);
          dayTurnoverQty += validNumber(u.quantity, 0);
        }

        // 原币日收益：Δ持仓市值 − 当日净流入持仓的现金（signedAmount：买 +amount、卖 −amount）。
        // 须用「− dayFlow」，与开仓日 qEod*closeD−买款 一致；用 + dayFlow 会把累计 profit_cny 与总市值走势算反。
        const pnlNative = qEod * closeD - qBod * prevPx - dayFlow;
        const ccy = getSymbolCurrency(sym);

        symbolRowsBuffer.push({
          accountId,
          symbol: sym,
          date: D,
          eodShares: qEod,
          dayTradeQty: dayTurnoverQty,
          dayTradeAmount: dayAmt,
          dayClosePrice: closeD,
          dayPnlNative: pnlNative,
          currency: ccy,
          createdAt: now,
        });

        // 汇总层：先按账户+日+币种累加原币收益，回填结束后再统一乘当日汇率得人民币
        addDailyProfitNativeByCurrency(accDateNative, accountId, D, pnlNative, ccy);
        if (symbolRowsBuffer.length >= 500) flushSym();
      }
    }
  }
  flushSym();
  console.log(`[backfill] symbol_daily_pnl 写入完成。`);

  /** `${accountId}|${date}` -> 当日 profit_cny（各币种原币合计后再换算） */
  const profitCnyByAccDate = new Map();
  for (const [pk, byCcy] of accDateNative) {
    const sep = pk.indexOf("|");
    const dateKey = pk.slice(sep + 1);
    profitCnyByAccDate.set(pk, profitCnyFromAccDateNative(byCcy, dateKey, fxUsdMap, fxHkdMap));
  }

  for (const accountId of accountIds) {
    const accTrades = filterTradesForAccount(allTrades, accountId);
    if (!accTrades.length) continue;

    const points = buildPortfolioHistoryCny(accTrades, allDates, klineBySym, fxUsdMap, fxHkdMap);
    if (!points.length) continue;

    const costS = computeCostSeries(points);
    const twrS = computeTimeWeightedSeries(points);
    const dietzS = computeMoneyWeightedSeries(points);

    let cumProfit = 0;
    let prevMv = 0;

    for (let i = 0; i < points.length; i += 1) {
      const p = points[i];
      const dk = p.date;
      const profitCny = profitCnyByAccDate.get(`${accountId}|${dk}`) ?? 0;
      const beginNav = i === 0 ? Math.max(validNumber(points[0].value - points[0].flow, 0), 1e-9) : prevMv;
      const rateCostD = beginNav > 0 ? profitCny / beginNav : 0;

      const twrDaily =
        i === 0 ? twrS[0]?.rate ?? 0 : (1 + (twrS[i]?.rate ?? 0)) / (1 + (twrS[i - 1]?.rate ?? 0)) - 1;
      const dietzDaily =
        i === 0
          ? dietzS[0]?.rate ?? 0
          : (1 + (dietzS[i]?.rate ?? 0)) / (1 + (dietzS[i - 1]?.rate ?? 0)) - 1;

      let sigmaCny = 0;
      for (const tr of accTrades) {
        if (tr.date > dk) break;
        const m = inferMarket(tr.symbol);
        const ccy = getSymbolCurrency(tr.symbol, m);
        const fx = fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, tr.date);
        sigmaCny += signedAmount(tr) * fx;
      }
      const principal = Math.max(capitalAmount, sigmaCny, 0);

      cumProfit += profitCny;

      upsertAnalysisDailySnapshot({
        accountId,
        date: dk,
        profitCny: profitCny,
        rateCost: rateCostD,
        rateTwr: twrDaily,
        rateDietz: dietzDaily,
        totalProfit: cumProfit,
        totalRateCost: costS[i]?.rate ?? 0,
        totalRateTwr: twrS[i]?.rate ?? 0,
        totalRateDietz: dietzS[i]?.rate ?? 0,
        principal,
        marketValue: p.value,
        fxHkdCny: fxHkdMap[dk] ?? null,
        fxUsdCny: fxUsdMap[dk] ?? null,
        createdAt: now,
      });

      prevMv = p.value;
    }
    console.log(`[backfill] analysis_daily_snapshot account=${accountId} rows=${points.length}`);
  }

  console.log("[backfill] 完成。");
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
