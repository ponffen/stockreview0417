const {
  getTrades,
  getCashTransfers,
  getAccounts,
  normalizeSymbol,
  getSymbolDailyCloseRange,
  upsertSymbolDailyPnlBatch,
  upsertAnalysisDailySnapshot,
  upsertSymbolDailyCloseBatch,
  listAllUserIds,
  getLatestAnalysisSnapshotDate,
  setSnapshotWatermark,
  ensurePerformanceSchemaV2,
} = require("./db");
const { fetchRemoteDailyClosesForSymbol } = require("./daily-close-backfill");
const { fetchSinaForexDayKSeries, toDateKey, enumerateDays, validNumber } = require("../scripts/lib/market-fetch");
const {
  buildPortfolioDayPoints,
  computeTwrFromDayPoints,
  normalizeTradeCalendarDateKey,
  lastPositiveCloseOnOrBefore,
} = require("./return-calcs");
const { rebuildPerformanceSeriesCache } = require("./performance-cache-service");
const { rebuildHomeSummaryForUser } = require("./home-summary-service");

const FX_FALLBACK = {
  USD: 7.2,
  HKD: 0.92,
};

function sortTradeAsc(a, b) {
  const ad = new Date(a.date).getTime();
  const bd = new Date(b.date).getTime();
  if (ad !== bd) {
    return ad - bd;
  }
  return Number(a.createdAt) - Number(b.createdAt);
}

function addCalendarDays(dateKey, days) {
  const d = new Date(`${String(dateKey || "").slice(0, 10)}T12:00:00+08:00`);
  if (Number.isNaN(d.getTime())) {
    return toDateKey(new Date());
  }
  d.setDate(d.getDate() + Number(days || 0));
  return toDateKey(d);
}

function getShanghaiWallClockParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);
  const get = (type) => parts.find((p) => p.type === type)?.value;
  return {
    y: Number(get("year")),
    m: Number(get("month")),
    d: Number(get("day")),
    h: Number(get("hour")),
    min: Number(get("minute")),
  };
}

function getTradingDateKey(baseDate = new Date()) {
  const { y, m, d, h, min } = getShanghaiWallClockParts(baseDate);
  const current = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  if (h < 8 || (h === 8 && min < 30)) {
    return addCalendarDays(current, -1);
  }
  return current;
}

function resolveFrozenDate(input) {
  if (input) {
    return toDateKey(input);
  }
  const tradingDate = getTradingDateKey(new Date());
  return addCalendarDays(tradingDate, -1);
}

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

function fxToCnyOnDate(fxUsdMap, fxHkdMap, currency, dateKey) {
  if (currency === "CNY") return 1;
  if (currency === "USD") return validNumber(fxUsdMap[dateKey], FX_FALLBACK.USD);
  if (currency === "HKD") return validNumber(fxHkdMap[dateKey], FX_FALLBACK.HKD);
  return 1;
}

function addDailyProfitNativeByCurrency(accDateNative, accountId, dateKey, pnlNative, currency) {
  const pk = `${accountId}|${dateKey}`;
  let byCcy = accDateNative.get(pk);
  if (!byCcy) {
    byCcy = {};
    accDateNative.set(pk, byCcy);
  }
  byCcy[currency] = (byCcy[currency] || 0) + pnlNative;
}

function profitCnyFromAccDateNative(byCcy, dateKey, fxUsdMap, fxHkdMap) {
  if (!byCcy) return 0;
  let sum = 0;
  for (const [ccy, amt] of Object.entries(byCcy)) {
    sum += amt * fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, dateKey);
  }
  return sum;
}

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

function filterTradesForAccount(allTrades, accountId) {
  const norm = (t) => ({ ...t, date: normalizeTradeCalendarDateKey(t.date) });
  if (accountId === "all") return [...allTrades].map(norm).sort(sortTradeAsc);
  return allTrades.filter((t) => t.accountId === accountId).map(norm).sort(sortTradeAsc);
}

function filterCashForAccount(allCash, accountId) {
  if (accountId === "all") {
    return [...allCash];
  }
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

function buildFundCumCnyByDate(cashRows, accountId, accounts, allDates, fxUsdMap, fxHkdMap) {
  const accById = new Map(accounts.map((a) => [String(a.id), a]));
  const filtered = filterCashForAccount(cashRows, accountId);
  const dayDelta = new Map();
  for (const r of filtered) {
    const d = String(r.date).slice(0, 10);
    const net = cashTransferNetCnyForRow(r, accById, fxUsdMap, fxHkdMap);
    dayDelta.set(d, (dayDelta.get(d) || 0) + net);
  }
  let cum = 0;
  const out = new Map();
  for (const D of allDates) {
    cum += dayDelta.get(D) || 0;
    out.set(D, cum);
  }
  return out;
}

async function syncSymbolDailyCloseForWindow(symbols, fromDate, toDate, logger = console) {
  const uniq = [...new Set((symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))];
  let written = 0;
  for (const sym of uniq) {
    try {
      const rows = await fetchRemoteDailyClosesForSymbol(sym, fromDate, toDate);
      if (!rows.length) {
        continue;
      }
      const payload = rows.map((r) => ({
        symbol: sym,
        date: String(r.date || "").slice(0, 10),
        close: Number(r.close),
        source: String(r.source || "sina"),
      }));
      written += await upsertSymbolDailyCloseBatch(payload);
    } catch (error) {
      logger.warn?.(`[freeze] daily-close sync failed symbol=${sym}`, error?.message || error);
    }
  }
  return written;
}

async function buildFxMaps(allDates, logger = console) {
  let fxUsdMap = {};
  let fxHkdMap = {};
  try {
    fxUsdMap = await fetchSinaForexDayKSeries("USDCNY");
  } catch (error) {
    logger.warn?.("[freeze] fetch USDCNY dayk failed, fallback fixed rate", error?.message || error);
  }
  try {
    fxHkdMap = await fetchSinaForexDayKSeries("HKDCNY");
  } catch (error) {
    logger.warn?.("[freeze] fetch HKDCNY dayk failed, fallback fixed rate", error?.message || error);
  }
  allDates.forEach((dateKey) => {
    if (!Number.isFinite(Number(fxUsdMap[dateKey])) || Number(fxUsdMap[dateKey]) <= 0) {
      fxUsdMap[dateKey] = FX_FALLBACK.USD;
    }
    if (!Number.isFinite(Number(fxHkdMap[dateKey])) || Number(fxHkdMap[dateKey]) <= 0) {
      fxHkdMap[dateKey] = FX_FALLBACK.HKD;
    }
  });
  return { fxUsdMap, fxHkdMap };
}

async function freezeUserToDate(userId, frozenDate, options = {}) {
  const logger = options.logger || console;
  const syncDailyClose = options.syncDailyClose === true;
  const uid = String(userId || "").trim();
  if (!uid) {
    return { userId: "", skipped: true, reason: "missing-user" };
  }

  await ensurePerformanceSchemaV2();

  const latestAllSnapshotDate = await getLatestAnalysisSnapshotDate(uid, "all");
  if (!options.force && latestAllSnapshotDate && latestAllSnapshotDate >= frozenDate) {
    return { userId: uid, skipped: true, reason: "already-up-to-date", latestSnapshotDate: latestAllSnapshotDate };
  }

  const allTrades = (await getTrades(uid)).map((t) => ({
    ...t,
    symbol: normalizeSymbol(t.symbol),
  }));
  if (!allTrades.length) {
    return { userId: uid, skipped: true, reason: "no-trades" };
  }
  allTrades.sort(sortTradeAsc);
  const minD = allTrades[0].date;
  if (minD > frozenDate) {
    return { userId: uid, skipped: true, reason: "trade-after-frozen-date" };
  }

  const allCash = await getCashTransfers(uid);
  const accounts = await getAccounts(uid);
  const allDates = enumerateDays(minD, frozenDate);
  const symbols = [...new Set(allTrades.map((t) => t.symbol).filter(Boolean))].sort();

  const syncedDailyCloseRows = syncDailyClose ? await syncSymbolDailyCloseForWindow(symbols, minD, frozenDate, logger) : 0;

  const klineBySym = new Map();
  for (const sym of symbols) {
    const dbRows = await getSymbolDailyCloseRange(sym, minD, frozenDate);
    const list = (Array.isArray(dbRows) ? dbRows : [])
      .map((row) => ({ day: String(row.date || "").slice(0, 10), close: Number(row.close) }))
      .filter((row) => row.day && Number.isFinite(row.close) && row.close > 0)
      .sort((a, b) => a.day.localeCompare(b.day));
    if (list.length) {
      klineBySym.set(sym, list);
    }
  }

  const { fxUsdMap, fxHkdMap } = await buildFxMaps(allDates, logger);
  const accountIds = ["all", ...new Set(allTrades.map((t) => String(t.accountId || "default")))];
  const accDateNative = new Map();

  const symbolRowsBuffer = [];
  let symbolRowsWritten = 0;
  const flushSym = async () => {
    if (!symbolRowsBuffer.length) return;
    symbolRowsWritten += await upsertSymbolDailyPnlBatch(symbolRowsBuffer.splice(0, symbolRowsBuffer.length), uid);
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

        const closeD = lastPositiveCloseOnOrBefore(kl, D);
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
        const pnlNative = qEod * closeD - qBod * prevPx - dayFlow;
        const ccy = getSymbolCurrency(sym);

        symbolRowsBuffer.push({
          accountId,
          symbol: sym,
          date: D,
          eodShares: qEod,
          dayTradeQty: dayTurnoverQty,
          dayTradeAmount: dayAmt,
          dayTradeFlowNative: dayFlow,
          dayClosePrice: closeD,
          dayPnlNative: pnlNative,
          currency: ccy,
          createdAt: Date.now(),
        });
        addDailyProfitNativeByCurrency(accDateNative, accountId, D, pnlNative, ccy);
        if (symbolRowsBuffer.length >= 500) await flushSym();
      }
    }
  }
  await flushSym();

  const profitCnyByAccDate = new Map();
  for (const [pk, byCcy] of accDateNative) {
    const sep = pk.indexOf("|");
    const dateKey = pk.slice(sep + 1);
    profitCnyByAccDate.set(pk, profitCnyFromAccDateNative(byCcy, dateKey, fxUsdMap, fxHkdMap));
  }

  let analysisRowsWritten = 0;
  for (const accountId of accountIds) {
    const accTrades = filterTradesForAccount(allTrades, accountId);
    if (!accTrades.length) continue;

    const fundCumByDate = buildFundCumCnyByDate(allCash, accountId, accounts, allDates, fxUsdMap, fxHkdMap);
    const dayPoints = buildPortfolioDayPoints(accTrades, allDates, klineBySym, fxUsdMap, fxHkdMap, allCash, accountId, accounts);
    if (!dayPoints.length) continue;

    const twrArr = computeTwrFromDayPoints(dayPoints);

    let cumProfit = 0;
    for (let i = 0; i < dayPoints.length; i += 1) {
      const p = dayPoints[i];
      const tw = twrArr[i] || { twRDaily: 0, twRCumulative: 0 };
      const dk = p.date;
      const profitCny = profitCnyByAccDate.get(`${accountId}|${dk}`) ?? 0;

      let sigmaCny = 0;
      for (const tr of accTrades) {
        if (tr.date > dk) break;
        const m = inferMarket(tr.symbol);
        const ccy = getSymbolCurrency(tr.symbol, m);
        const fx = fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, tr.date);
        sigmaCny += signedAmount(tr) * fx;
      }
      const fundCny = fundCumByDate.get(dk) ?? 0;
      const principal = Math.max(sigmaCny, fundCny, 0);
      cumProfit += profitCny;
      await upsertAnalysisDailySnapshot(
        {
          accountId,
          date: dk,
          profitCny,
          twRDaily: tw.twRDaily,
          twRCumulative: tw.twRCumulative,
          externalFlowCny: p.extFlow,
          externalFlowNative: p.externalFlowNative,
          totalProfit: cumProfit,
          principal,
          marketValue: p.nav,
          fxHkdCny: fxHkdMap[dk] ?? null,
          fxUsdCny: fxUsdMap[dk] ?? null,
          createdAt: Date.now(),
        },
        uid
      );
      analysisRowsWritten += 1;
    }
  }

  await rebuildPerformanceSeriesCache({
    userId: uid,
    asOfDate: frozenDate,
    accountIds,
  });

  try {
    const hr = await rebuildHomeSummaryForUser(uid);
    if (hr?.ok) {
      logger.info?.("[freeze] home summary ok", hr.frozenThrough, "symbols=", hr.symbolCount);
    } else if (hr && !hr.skip) {
      logger.warn?.("[freeze] home summary", hr.error || hr.reason || hr);
    }
  } catch (e) {
    logger.warn?.("[freeze] home summary failed", e?.message || e);
  }

  return {
    userId: uid,
    skipped: false,
    frozenDate,
    symbols: symbols.length,
    syncedDailyCloseRows,
    dailyCloseSyncedInFreeze: syncDailyClose,
    symbolRowsWritten,
    analysisRowsWritten,
  };
}

async function runDailyFreeze(options = {}) {
  const logger = options.logger || console;
  const frozenDate = resolveFrozenDate(options.frozenDate);
  const force = options.force === true;
  const syncDailyClose = options.syncDailyClose === true;
  const userIdsInput = Array.isArray(options.userIds) ? options.userIds : [];
  const userIds = userIdsInput.length
    ? [...new Set(userIdsInput.map((u) => String(u || "").trim()).filter(Boolean))]
    : await listAllUserIds();
  const startedAt = Date.now();
  const results = [];
  try {
    for (const uid of userIds) {
      const one = await freezeUserToDate(uid, frozenDate, { logger, force, syncDailyClose });
      results.push(one);
    }
    const successCount = results.filter((r) => !r.skipped).length;
    await setSnapshotWatermark({
      frozenDate,
      status: "success",
      message: `users=${userIds.length}, updated=${successCount}`,
    });
    return {
      ok: true,
      frozenDate,
      startedAt,
      finishedAt: Date.now(),
      elapsedMs: Date.now() - startedAt,
      users: results,
    };
  } catch (error) {
    await setSnapshotWatermark({
      frozenDate,
      status: "failed",
      message: String(error?.message || error || "freeze failed"),
    });
    throw error;
  }
}

module.exports = {
  runDailyFreeze,
  resolveFrozenDate,
};
