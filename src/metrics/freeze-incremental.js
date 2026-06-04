/**
 * 冻结递推：昨日快照 + 今日成交/银证（定时任务 / 链式回填）。
 * MWR 按区间现金流算持有期 XIRR（非年化）；tw_r_cumulative 链式乘日 TWR。
 */
const {
  getTrades,
  getCashTransfers,
  getAccounts,
  normalizeSymbol,
  getSymbolDailyCloseRange,
  upsertSymbolDailyCloseBatch,
  upsertUserMetricsMeta,
  upsertAccountMetricsMeta,
  getLatestAnalysisSnapshotDate,
} = require("../db");
const { ensureMetricsSchemaV3 } = require("./schema-v3");
const { StageAccumulator } = require("./stage-accumulator");
const { windowStartForStage } = require("./stage-accumulator");
const {
  lastPositiveCloseOnOrBefore,
  fxToCnyOnDate,
  inferMarket,
  getSymbolCurrency,
  signedAmount,
} = require("../return-calcs");
const {
  computeLedgerCashCnyUpToDate,
  externalFlowCnyForDate,
  tradeCashFlowInAccountCurrency,
} = require("../ledger-metrics");
const { xirrPeriodFromSnapshotWindow, xirrPeriodFromSymbolValueFlowPoints } = require("../home-summary-maths");
const { mwrForFreezeStorage } = require("../mwr");
const { fetchRemoteDailyClosesForSymbol } = require("../daily-close-backfill");
const { fetchSinaForexDayKSeries, validNumber } = require("../../scripts/lib/market-fetch");
const {
  enumerateFreezeSessionDates,
  sessionDatesAfterLatest,
  previousSessionDate,
  ledgerSessionDateKey,
  forwardFillFxMap,
  capFrozenThroughToSnapshot,
} = require("./freeze-calendar");
const { resolveFrozenDate } = require("../eod-freeze-service");
const {
  upsertAnalysisBatchV3,
  upsertSymbolBatchV3,
} = require("./freeze-v3-upsert");

const FX_FALLBACK = { USD: 7.2, HKD: 0.92 };

function sortTradeAsc(a, b) {
  const ad = new Date(a.date).getTime();
  const bd = new Date(b.date).getTime();
  if (ad !== bd) return ad - bd;
  return Number(a.createdAt) - Number(b.createdAt);
}

function filterTradesForAccount(allTrades, accountId) {
  if (accountId === "all") return [...allTrades].sort(sortTradeAsc);
  return allTrades.filter((t) => t.accountId === accountId).sort(sortTradeAsc);
}

function accountBookCurrency(accountId, accounts) {
  if (String(accountId) === "all") return "CNY";
  const acc = (accounts || []).find((a) => String(a.id) === String(accountId));
  return String(acc?.currency || "CNY").toUpperCase();
}

function cnyToBook(amountCny, book, dateKey, fxUsdMap, fxHkdMap) {
  const v = Number(amountCny) || 0;
  if (book === "CNY") return v;
  const fx = fxToCnyOnDate(fxUsdMap, fxHkdMap, book, dateKey, FX_FALLBACK);
  return fx > 0 ? v / fx : v;
}

function nativeToCny(amountNative, ccy, dateKey, fxUsdMap, fxHkdMap) {
  const v = Number(amountNative) || 0;
  const c = String(ccy || "CNY").toUpperCase();
  if (c === "CNY") return v;
  return v * fxToCnyOnDate(fxUsdMap, fxHkdMap, c, dateKey, FX_FALLBACK);
}

function tradesForFreezeSession(allTrades) {
  return (allTrades || []).map((t) => ({
    ...t,
    date: ledgerSessionDateKey(t.date),
  }));
}

function cashForFreezeSession(allCash) {
  return (allCash || []).map((c) => ({
    ...c,
    date: ledgerSessionDateKey(c.date),
  }));
}

function listAccountIdsForFreeze(allTrades, accounts) {
  const ids = new Set(["all"]);
  for (const a of accounts || []) {
    if (a?.id) ids.add(String(a.id));
  }
  for (const t of allTrades || []) {
    ids.add(String(t.accountId || "default"));
  }
  return [...ids].sort();
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

function holdingsAtDate(accTrades, dk) {
  const h = {};
  for (const t of accTrades) {
    const d = String(t.date).slice(0, 10);
    if (d > dk) continue;
    const sym = normalizeSymbol(t.symbol);
    if (!sym) continue;
    h[sym] = (h[sym] || 0) + (t.side === "buy" ? Number(t.quantity) : -Number(t.quantity));
  }
  return h;
}

function marketValueCny(holdings, klineBySym, dk, fxUsdMap, fxHkdMap) {
  let mv = 0;
  for (const [sym, q] of Object.entries(holdings)) {
    if (Math.abs(q) < 1e-9) continue;
    const kl = klineBySym.get(sym);
    if (!kl?.length) continue;
    const c = lastPositiveCloseOnOrBefore(
      kl.map((x) => ({ day: x.day, close: x.close })),
      dk,
    );
    if (!(c > 0)) continue;
    const ccy = getSymbolCurrency(sym, inferMarket(sym));
    mv += q * c * fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, dk, FX_FALLBACK);
  }
  return mv;
}

function dayCashTransferBook(sessionCash, accounts, accountId, dk, book, fxUsdMap, fxHkdMap) {
  const accById = new Map((accounts || []).map((a) => [String(a.id), a]));
  const scopeAll = String(accountId || "all").trim() === "all";
  let sum = 0;
  for (const r of sessionCash || []) {
    const rowAccId = String(r.accountId || "default");
    if (!scopeAll && rowAccId !== String(accountId)) continue;
    if (String(r.date).slice(0, 10) !== dk) continue;
    const acc = accById.get(rowAccId) || { currency: "CNY" };
    const ccy = String(acc.currency || "CNY").toUpperCase();
    const signIn = String(r.direction || "").toLowerCase() === "out" ? -1 : 1;
    const nat = signIn * Math.abs(Number(r.amount) || 0);
    const cny = ccy === "CNY" ? nat : nat * fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, dk, FX_FALLBACK);
    sum += cnyToBook(cny, book, dk, fxUsdMap, fxHkdMap);
  }
  return sum;
}

function dayTradeCashFlowBook(accTrades, accounts, accountId, dk, fxUsdMap, fxHkdMap) {
  const book = accountBookCurrency(accountId, accounts);
  let sum = 0;
  for (const t of accTrades) {
    if (String(t.date).slice(0, 10) !== dk) continue;
    sum += tradeCashFlowInAccountCurrency(t, book, fxUsdMap, fxHkdMap);
  }
  return sum;
}

function mapAnalysisDbRow(row) {
  if (!row) return null;
  return {
    date: String(row.date).slice(0, 10),
    cash: Number(row.cash),
    totalAssets: Number(row.total_assets),
    marketValue: Number(row.market_value),
    dailyExternalFlow: Number(row.daily_external_flow),
    twRCumulative: Number(row.tw_r_cumulative),
    stageMtdProfit: Number(row.stage_mtd_profit),
    stageMtdRateTwr: Number(row.stage_mtd_rate_twr),
    stageYtdProfit: Number(row.stage_ytd_profit),
    stageYtdRateTwr: Number(row.stage_ytd_rate_twr),
    stageInceptionProfit: Number(row.stage_inception_profit),
    stageInceptionRateTwr: Number(row.stage_inception_rate_twr),
    stageLast7dProfit: Number(row.stage_last_7d_profit),
    stageLast7dRateTwr: Number(row.stage_last_7d_rate_twr),
    stageLast30dProfit: Number(row.stage_last_30d_profit),
    stageLast30dRateTwr: Number(row.stage_last_30d_rate_twr),
    stageLast90dProfit: Number(row.stage_last_90d_profit),
    stageLast90dRateTwr: Number(row.stage_last_90d_rate_twr),
    principal: Number(row.principal),
  };
}

async function loadAnalysisRow(client, uid, accountId, dateKey) {
  const { rows } = await client.query(
    `SELECT date, cash, total_assets, market_value, daily_external_flow, tw_r_cumulative,
            stage_mtd_profit, stage_mtd_rate_twr, stage_ytd_profit, stage_ytd_rate_twr,
            stage_inception_profit, stage_inception_rate_twr,
            stage_last_7d_profit, stage_last_7d_rate_twr,
            stage_last_30d_profit, stage_last_30d_rate_twr,
            stage_last_90d_profit, stage_last_90d_rate_twr, principal
     FROM analysis_daily_snapshot
     WHERE user_id = $1 AND account_id = $2 AND date = $3`,
    [uid, accountId, dateKey],
  );
  return mapAnalysisDbRow(rows[0]);
}

function hydrateStageAcc(stageAcc, yesterday, prevDateKey, dk) {
  if (!yesterday) return;
  stageAcc.mtd.profit = Number(yesterday.stageMtdProfit) || 0;
  stageAcc.mtd.rate = Number(yesterday.stageMtdRateTwr) || 0;
  stageAcc.ytd.profit = Number(yesterday.stageYtdProfit) || 0;
  stageAcc.ytd.rate = Number(yesterday.stageYtdRateTwr) || 0;
  stageAcc.inception.profit = Number(yesterday.stageInceptionProfit) || 0;
  stageAcc.inception.rate = Number(yesterday.stageInceptionRateTwr) || 0;
  stageAcc.last7.profit = Number(yesterday.stageLast7dProfit) || 0;
  stageAcc.last7.rate = Number(yesterday.stageLast7dRateTwr) || 0;
  stageAcc.last30.profit = Number(yesterday.stageLast30dProfit) || 0;
  stageAcc.last30.rate = Number(yesterday.stageLast30dRateTwr) || 0;
  stageAcc.last90.profit = Number(yesterday.stageLast90dProfit) || 0;
  stageAcc.last90.rate = Number(yesterday.stageLast90dRateTwr) || 0;
  stageAcc.curMonth = prevDateKey ? require("./stages").monthStartKeyShanghai(prevDateKey) : "";
  stageAcc.curYear = prevDateKey ? require("./stages").yearStartKeyShanghai(prevDateKey) : "";
}

async function loadProfitByDate(client, uid, accountId, fromD, toD) {
  const { rows } = await client.query(
    `SELECT date::text AS d, daily_profit FROM analysis_daily_snapshot
     WHERE user_id = $1 AND account_id = $2 AND date >= $3 AND date <= $4 ORDER BY date`,
    [uid, accountId, fromD, toD],
  );
  const map = new Map();
  for (const r of rows) {
    map.set(String(r.d).slice(0, 10), Number(r.daily_profit) || 0);
  }
  return map;
}

function computeMwrPatchPeriod(rowsAsc, asOf, firstTrade) {
  const mapped = rowsAsc.map((r) => ({
    date: r.date,
    totalAssets: r.totalAssets,
    externalFlowCny: r.dailyExternalFlow,
  }));
  const wrap = (stage) =>
    mwrForFreezeStorage(
      xirrPeriodFromSnapshotWindow(mapped, windowStartForStage(stage, asOf, firstTrade), asOf),
    );
  return {
    stageMtdRateMwr: wrap("mtd"),
    stageYtdRateMwr: wrap("ytd"),
    stageInceptionRateMwr: wrap("inception"),
    stageLast7dRateMwr: wrap("last_7d"),
    stageLast30dRateMwr: wrap("last_30d"),
    stageLast90dRateMwr: wrap("last_90d"),
  };
}

async function freezeAccountOneDay({
  uid,
  accountId,
  dk,
  sessionTrades,
  sessionCash,
  accounts,
  klineBySym,
  fxUsdMap,
  fxHkdMap,
  client,
  accountMem,
}) {
  const book = accountBookCurrency(accountId, accounts);
  const accTrades = filterTradesForAccount(sessionTrades, accountId);
  if (!accTrades.length) return null;

  const prevD = previousSessionDate(dk);
  let yesterday = accountMem?.lastRow || null;
  if (!yesterday && prevD) {
    yesterday = await loadAnalysisRow(client, uid, accountId, prevD);
  }

  const extCny = externalFlowCnyForDate(sessionCash, accounts, accountId, fxUsdMap, fxHkdMap, dk);
  const ext = cnyToBook(extCny, book, dk, fxUsdMap, fxHkdMap);

  let cashBook;
  if (yesterday) {
    cashBook =
      Number(yesterday.cash) +
      dayTradeCashFlowBook(accTrades, accounts, accountId, dk, fxUsdMap, fxHkdMap) +
      dayCashTransferBook(sessionCash, accounts, accountId, dk, book, fxUsdMap, fxHkdMap);
  } else {
    const cashCny = computeLedgerCashCnyUpToDate(
      sessionTrades,
      sessionCash,
      accounts,
      accountId,
      fxUsdMap,
      fxHkdMap,
      dk,
    );
    cashBook = cnyToBook(cashCny, book, dk, fxUsdMap, fxHkdMap);
  }

  const holdings = holdingsAtDate(accTrades, dk);
  const mvCny = marketValueCny(holdings, klineBySym, dk, fxUsdMap, fxHkdMap);
  const cash = cashBook;
  const mv = cnyToBook(mvCny, book, dk, fxUsdMap, fxHkdMap);
  const ta = cash + mv;

  const prevTa = yesterday ? Number(yesterday.totalAssets) : 0;
  const dailyProfit = yesterday ? ta - prevTa - ext : 0;
  const denom = prevTa + Math.max(ext, 0);
  const dailyRateTwr = denom > 0 ? dailyProfit / denom : 0;
  const prevTwr = yesterday ? Number(yesterday.twRCumulative) : 0;
  const twRCumulative = (1 + prevTwr) * (1 + dailyRateTwr) - 1;

  let principalBook;
  if (yesterday) {
    principalBook = Number(yesterday.principal) + dayCashTransferBook(
      sessionCash,
      accounts,
      accountId,
      dk,
      book,
      fxUsdMap,
      fxHkdMap,
    );
  } else {
    const pCny = require("../ledger-metrics").principalCnyUpToDate(
      sessionCash,
      accounts,
      accountId,
      fxUsdMap,
      fxHkdMap,
      dk,
    );
    principalBook = cnyToBook(pCny, book, dk, fxUsdMap, fxHkdMap);
  }

  const stageAcc = new StageAccumulator();
  if (yesterday && prevD) {
    const fromP = addCalendarDaysForProfit(dk, -89);
    const profitMap = accountMem?.profitByDate || (await loadProfitByDate(client, uid, accountId, fromP, prevD));
    for (const [d, p] of profitMap) {
      stageAcc.profitByDate.set(d, p);
    }
    hydrateStageAcc(stageAcc, yesterday, prevD, dk);
  }
  stageAcc.onDay(dk, dailyProfit, dailyRateTwr);
  const snap = stageAcc.snapshotTwr();

  const rowsAsc = accountMem?.rowsAsc ? [...accountMem.rowsAsc] : [];
  rowsAsc.push({ date: dk, totalAssets: ta, dailyExternalFlow: ext });
  const firstTrade = accTrades[0].date;
  const mwr = computeMwrPatchPeriod(rowsAsc, dk, firstTrade);

  const row = {
    accountId,
    date: dk,
    bookCurrency: book,
    dailyProfit,
    dailyRateTwr,
    dailyExternalFlow: ext,
    dailyCashDelta: yesterday ? cash - Number(yesterday.cash) : 0,
    twRCumulative,
    marketValue: mv,
    totalAssets: ta,
    cash,
    cashRatio: ta > 0 ? cash / ta : 0,
    principal: principalBook,
    fxHkdCny: fxHkdMap[dk] ?? null,
    fxUsdCny: fxUsdMap[dk] ?? null,
    stageMtdProfit: snap.stageMtdProfit,
    stageMtdRateTwr: snap.stageMtdRateTwr,
    stageMtdRateMwr: mwr.stageMtdRateMwr,
    stageYtdProfit: snap.stageYtdProfit,
    stageYtdRateTwr: snap.stageYtdRateTwr,
    stageYtdRateMwr: mwr.stageYtdRateMwr,
    stageInceptionProfit: snap.stageInceptionProfit,
    stageInceptionRateTwr: snap.stageInceptionRateTwr,
    stageInceptionRateMwr: mwr.stageInceptionRateMwr,
    stageLast7dProfit: snap.stageLast7dProfit,
    stageLast7dRateTwr: snap.stageLast7dRateTwr,
    stageLast7dRateMwr: mwr.stageLast7dRateMwr,
    stageLast30dProfit: snap.stageLast30dProfit,
    stageLast30dRateTwr: snap.stageLast30dRateTwr,
    stageLast30dRateMwr: mwr.stageLast30dRateMwr,
    stageLast90dProfit: snap.stageLast90dProfit,
    stageLast90dRateTwr: snap.stageLast90dRateTwr,
    stageLast90dRateMwr: mwr.stageLast90dRateMwr,
  };

  const profitByDate = accountMem?.profitByDate || new Map();
  profitByDate.set(dk, dailyProfit);

  return { row, rowsAsc, lastRow: row, profitByDate };
}

function addCalendarDaysForProfit(dk, n) {
  const { addCalendarDays } = require("./stages");
  return addCalendarDays(dk, n);
}

function symbolsForDay(accTrades, dk, prevQtyBySym) {
  const set = new Set(Object.keys(prevQtyBySym || {}).filter((s) => Math.abs(prevQtyBySym[s]) > 1e-6));
  for (const t of accTrades) {
    if (String(t.date).slice(0, 10) !== dk) continue;
    set.add(normalizeSymbol(t.symbol));
  }
  const holdings = holdingsAtDate(accTrades, dk);
  for (const [sym, q] of Object.entries(holdings)) {
    if (Math.abs(q) > 1e-6) set.add(sym);
  }
  return [...set].filter(Boolean);
}

async function freezeSymbolOneDay({
  uid,
  accountId,
  sym,
  dk,
  accTrades,
  klineBySym,
  taCny,
  book,
  fxUsdMap,
  fxHkdMap,
  symbolMem,
}) {
  const kl = klineBySym.get(sym);
  if (!kl?.length) return null;

  const symTrades = accTrades.filter((t) => normalizeSymbol(t.symbol) === sym).sort(sortTradeAsc);
  if (!symTrades.length && !symbolMem) return null;

  let qty = symbolMem?.eodShares ?? 0;
  if (!symbolMem && symTrades.length) {
    const prevD = previousSessionDate(dk);
    const h = holdingsAtDate(accTrades, prevD || dk);
    qty = h[sym] || 0;
  }

  const dayTrades = symTrades.filter((t) => String(t.date).slice(0, 10) === dk);
  for (const u of dayTrades) {
    qty += u.side === "buy" ? Number(u.quantity) : -Number(u.quantity);
  }

  const hasActivity =
    dayTrades.length > 0 || Math.abs(qty) > 1e-6 || Math.abs(symbolMem?.eodShares || 0) > 1e-6;
  if (!hasActivity) return null;

  const closeD = lastPositiveCloseOnOrBefore(
    kl.map((x) => ({ day: x.day, close: x.close })),
    dk,
  );
  if (!(closeD > 0)) return null;
  const closePrev = closeBefore(kl, dk);
  const prevPx = closePrev != null && closePrev > 0 ? closePrev : closeD;

  let qBod = qty;
  for (const u of dayTrades) {
    qBod -= u.side === "buy" ? Number(u.quantity) : -Number(u.quantity);
  }

  let dayFlow = 0;
  let dayAmt = 0;
  let dayTurn = 0;
  for (const u of dayTrades) {
    dayFlow += signedAmount(u);
    dayAmt += validNumber(u.amount, 0);
    dayTurn += validNumber(u.quantity, 0);
  }

  const pnl = qty * closeD - qBod * prevPx - dayFlow;
  const denom = qBod * prevPx + Math.max(dayFlow, 0);
  const rDay = denom > 0 ? pnl / denom : 0;

  const stageAcc = new StageAccumulator();
  const prevD = previousSessionDate(dk);
  if (symbolMem?.lastRow && prevD) {
    hydrateStageAcc(stageAcc, symbolMem.lastRow, prevD, dk);
    if (symbolMem.profitByDate) {
      for (const [d, p] of symbolMem.profitByDate) {
        stageAcc.profitByDate.set(d, p);
      }
    }
  }
  stageAcc.onDay(dk, pnl, rDay);
  const snap = stageAcc.snapshotTwr();

  const flowPts = symbolMem?.flowPts ? [...symbolMem.flowPts] : [];
  if (Math.abs(qty) > 1e-6 && closeD > 0) {
    const existing = flowPts.find((p) => p.date === dk);
    const pt = { date: dk, value: qty * closeD, flow: dayFlow };
    if (existing) {
      Object.assign(existing, pt);
    } else {
      flowPts.push(pt);
    }
    flowPts.sort((a, b) => a.date.localeCompare(b.date));
  }

  const endVal = qty * closeD;
  const mwrRate = mwrForFreezeStorage(xirrPeriodFromSymbolValueFlowPoints(flowPts, dk, endVal));
  const ccy = getSymbolCurrency(sym);
  const eodMarketValueNative = qty * closeD;
  const mvCny = nativeToCny(eodMarketValueNative, ccy, dk, fxUsdMap, fxHkdMap);
  const positionWeight = taCny > 0 ? mvCny / taCny : 0;

  const row = {
    accountId,
    symbol: sym,
    date: dk,
    bookCurrency: book,
    currency: ccy,
    dailyProfit: pnl,
    dailyTradeQty: dayTurn,
    dailyTradeAmount: dayAmt,
    dailyTradeFlow: dayFlow,
    dailyRateTwr: rDay,
    eodShares: qty,
    eodPrice: closeD,
    eodMarketValueNative,
    positionWeight,
    stageMtdProfit: snap.stageMtdProfit,
    stageMtdRateTwr: snap.stageMtdRateTwr,
    stageMtdRateMwr: mwrRate,
    stageYtdProfit: snap.stageYtdProfit,
    stageYtdRateTwr: snap.stageYtdRateTwr,
    stageYtdRateMwr: mwrRate,
    stageInceptionProfit: snap.stageInceptionProfit,
    stageInceptionRateTwr: snap.stageInceptionRateTwr,
    stageInceptionRateMwr: mwrRate,
    stageLast7dProfit: snap.stageLast7dProfit,
    stageLast7dRateTwr: snap.stageLast7dRateTwr,
    stageLast7dRateMwr: mwrRate,
    stageLast30dProfit: snap.stageLast30dProfit,
    stageLast30dRateTwr: snap.stageLast30dRateTwr,
    stageLast30dRateMwr: mwrRate,
    stageLast90dProfit: snap.stageLast90dProfit,
    stageLast90dRateTwr: snap.stageLast90dRateTwr,
    stageLast90dRateMwr: mwrRate,
  };

  const profitByDate = symbolMem?.profitByDate || new Map();
  profitByDate.set(dk, pnl);

  return {
    row,
    eodShares: qty,
    flowPts,
    lastRow: {
      stageMtdProfit: row.stageMtdProfit,
      stageMtdRateTwr: row.stageMtdRateTwr,
      stageYtdProfit: row.stageYtdProfit,
      stageYtdRateTwr: row.stageYtdRateTwr,
      stageInceptionProfit: row.stageInceptionProfit,
      stageInceptionRateTwr: row.stageInceptionRateTwr,
      stageLast7dProfit: row.stageLast7dProfit,
      stageLast7dRateTwr: row.stageLast7dRateTwr,
      stageLast30dProfit: row.stageLast30dProfit,
      stageLast30dRateTwr: row.stageLast30dRateTwr,
      stageLast90dProfit: row.stageLast90dProfit,
      stageLast90dRateTwr: row.stageLast90dRateTwr,
    },
    profitByDate,
  };
}

async function buildFxMaps(allDates, logger = console) {
  let fxUsdMap = {};
  let fxHkdMap = {};
  try {
    fxUsdMap = await fetchSinaForexDayKSeries("USDCNY");
  } catch (e) {
    logger.warn?.("[freeze-incremental] USDCNY", e?.message || e);
  }
  try {
    fxHkdMap = await fetchSinaForexDayKSeries("HKDCNY");
  } catch (e) {
    logger.warn?.("[freeze-incremental] HKDCNY", e?.message || e);
  }
  forwardFillFxMap(fxUsdMap, allDates, FX_FALLBACK.USD);
  forwardFillFxMap(fxHkdMap, allDates, FX_FALLBACK.HKD);
  return { fxUsdMap, fxHkdMap };
}

async function runFreezeIncrementalForUser(userId, options = {}) {
  const logger = options.logger || console;
  const uid = String(userId || "").trim();
  const frozenDate = options.frozenDate || resolveFrozenDate();
  const frozenDateKey = String(frozenDate).slice(0, 10);
  if (!uid) {
    return { ok: false, reason: "missing-user" };
  }

  await ensureMetricsSchemaV3();

  const allTrades = tradesForFreezeSession(
    (await getTrades(uid)).map((t) => ({ ...t, symbol: normalizeSymbol(t.symbol) })),
  );
  if (!allTrades.length) {
    return { ok: false, reason: "no-trades" };
  }
  allTrades.sort(sortTradeAsc);
  const minD = allTrades[0].date;
  if (minD > frozenDateKey) {
    return { ok: false, reason: "trade-after-frozen" };
  }

  const latest = await getLatestAnalysisSnapshotDate(uid, "all");
  if (!options.force && !options.fullRebuild && latest && latest >= frozenDateKey) {
    return { ok: true, userId: uid, skipped: true, reason: "already-up-to-date", frozenDate: latest };
  }

  let datesToProcess;
  if (options.fullRebuild || options.force) {
    datesToProcess = enumerateFreezeSessionDates(minD, frozenDateKey);
  } else if (latest && latest < frozenDateKey) {
    datesToProcess = sessionDatesAfterLatest(latest, frozenDateKey);
  } else {
    datesToProcess = [frozenDateKey];
  }
  if (!datesToProcess.length) {
    return { ok: true, userId: uid, skipped: true, reason: "no-dates", frozenDate: latest };
  }

  const allCash = cashForFreezeSession(await getCashTransfers(uid));
  const accounts = await getAccounts(uid);
  const accountIds = listAccountIdsForFreeze(allTrades, accounts);
  const symbols = [...new Set(allTrades.map((t) => t.symbol).filter(Boolean))].sort();
  const allDates = enumerateFreezeSessionDates(minD, frozenDateKey);

  if (options.syncDailyClose) {
    const closeFrom = options.fullRebuild ? minD : datesToProcess[0];
    for (const sym of symbols) {
      try {
        const rows = await fetchRemoteDailyClosesForSymbol(sym, closeFrom, frozenDate);
        if (rows.length) {
          await upsertSymbolDailyCloseBatch(
            rows.map((r) => ({ symbol: sym, date: r.date, close: r.close, source: r.source || "sina" })),
          );
        }
      } catch (e) {
        logger.warn?.("[freeze-incremental] close", sym, e?.message || e);
      }
    }
  }

  const klineBySym = new Map();
  for (const sym of symbols) {
    const dbRows = await getSymbolDailyCloseRange(sym, minD, frozenDate);
    const list = (dbRows || [])
      .map((row) => ({ day: String(row.date || "").slice(0, 10), close: Number(row.close) }))
      .filter((row) => row.day && Number.isFinite(row.close) && row.close > 0)
      .sort((a, b) => a.day.localeCompare(b.day));
    if (list.length) klineBySym.set(sym, list);
  }

  const { fxUsdMap, fxHkdMap } = await buildFxMaps(allDates, logger);

  const dbMod = require("../db");
  const pool = await dbMod.initPool();
  const client = await pool.connect();

  const accountMem = new Map();
  const symbolMem = new Map();
  let accountRowsWritten = 0;
  let symbolRowsWritten = 0;

  try {
    await client.query("BEGIN");
    if (options.fullRebuild || options.force) {
      await client.query("DELETE FROM symbol_daily_pnl WHERE user_id = $1", [uid]);
      await client.query("DELETE FROM analysis_daily_snapshot WHERE user_id = $1", [uid]);
    } else {
      await client.query("DELETE FROM analysis_daily_snapshot WHERE user_id = $1 AND date = ANY($2::text[])", [
        uid,
        datesToProcess,
      ]);
      await client.query("DELETE FROM symbol_daily_pnl WHERE user_id = $1 AND date = ANY($2::text[])", [
        uid,
        datesToProcess,
      ]);
    }

    const analysisBuffer = [];
    const symbolBuffer = [];

    for (const dk of datesToProcess) {
      const taCnyByAccount = new Map();

      for (const accountId of accountIds) {
        const result = await freezeAccountOneDay({
          uid,
          accountId,
          dk,
          sessionTrades: allTrades,
          sessionCash: allCash,
          accounts,
          klineBySym,
          fxUsdMap,
          fxHkdMap,
          client,
          accountMem: accountMem.get(accountId),
        });
        if (!result) continue;
        accountMem.set(accountId, {
          rowsAsc: result.rowsAsc,
          lastRow: result.lastRow,
          profitByDate: result.profitByDate,
        });
        analysisBuffer.push(result.row);
        accountRowsWritten += 1;
        const book = accountBookCurrency(accountId, accounts);
        let taCny = Number(result.row.totalAssets);
        if (book === "USD") {
          taCny *= Number(fxUsdMap[dk]) || FX_FALLBACK.USD;
        } else if (book === "HKD") {
          taCny *= Number(fxHkdMap[dk]) || FX_FALLBACK.HKD;
        }
        taCnyByAccount.set(accountId, taCny);
      }

      for (const accountId of accountIds) {
        const accTrades = filterTradesForAccount(allTrades, accountId);
        const book = accountBookCurrency(accountId, accounts);
        const taCny = taCnyByAccount.get(accountId) || 0;
        const prevQty = {};
        for (const [key, mem] of symbolMem) {
          if (!key.startsWith(`${accountId}|`)) continue;
          const sym = key.slice(accountId.length + 1);
          prevQty[sym] = mem.eodShares;
        }
        const syms = symbolsForDay(accTrades, dk, prevQty);
        for (const sym of syms) {
          const memKey = `${accountId}|${sym}`;
          const symResult = await freezeSymbolOneDay({
            uid,
            accountId,
            sym,
            dk,
            accTrades,
            klineBySym,
            taCny,
            book,
            fxUsdMap,
            fxHkdMap,
            symbolMem: symbolMem.get(memKey),
          });
          if (!symResult) continue;
          symbolMem.set(memKey, symResult);
          symbolBuffer.push(symResult.row);
          symbolRowsWritten += 1;
        }
      }

      if (analysisBuffer.length >= 400) {
        await upsertAnalysisBatchV3(client, uid, analysisBuffer.splice(0, analysisBuffer.length));
      }
      if (symbolBuffer.length >= 200) {
        await upsertSymbolBatchV3(client, uid, symbolBuffer.splice(0, symbolBuffer.length));
        symbolBuffer.length = 0;
      }
    }

    if (analysisBuffer.length) {
      await upsertAnalysisBatchV3(client, uid, analysisBuffer);
    }
    if (symbolBuffer.length) {
      await upsertSymbolBatchV3(client, uid, symbolBuffer);
    }

    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }

  const latestSnap = await getLatestAnalysisSnapshotDate(uid, "all");
  const frozenThroughEffective = capFrozenThroughToSnapshot(frozenDateKey, latestSnap) || latestSnap || frozenDateKey;

  await upsertUserMetricsMeta(uid, {
    frozenThrough: frozenThroughEffective,
    isCleared: false,
    clearedAt: null,
  });
  for (const accId of accountIds.filter((a) => a !== "all")) {
    const accLatest = await getLatestAnalysisSnapshotDate(uid, accId);
    const accFt = capFrozenThroughToSnapshot(frozenDateKey, accLatest) || accLatest || frozenThroughEffective;
    await upsertAccountMetricsMeta(uid, accId, { frozenThrough: accFt });
  }

  return {
    ok: true,
    userId: uid,
    frozenDate: frozenThroughEffective,
    datesToProcess,
    timing: { accountRows: accountRowsWritten, symbolRows: symbolRowsWritten },
  };
}

module.exports = {
  runFreezeIncrementalForUser,
};
