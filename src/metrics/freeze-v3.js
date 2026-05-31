/**
 * Metrics v3 冻结/回填：账户表全历史；个股表按有持仓（含空头）或当日有成交逐日一行（含 eod=0 清仓日）。
 */
const {
  getTrades,
  getCashTransfers,
  getAccounts,
  normalizeSymbol,
  getSymbolDailyCloseRange,
  deleteAllSymbolDailyPnl,
  deleteAllAnalysisDailySnapshot,
  upsertSymbolDailyCloseBatch,
  setSnapshotWatermark,
  upsertUserMetricsMeta,
  upsertAccountMetricsMeta,
  getLatestAnalysisSnapshotDate,
} = require("../db");
const { ensureMetricsSchemaV3, METRICS_SOURCE_VERSION } = require("./schema-v3");
const { StageAccumulator } = require("./stage-accumulator");
const { windowStartForStage } = require("./stage-accumulator");
const { addCalendarDays, monthStartKeyShanghai, yearStartKeyShanghai } = require("./stages");
const {
  buildPortfolioDayPoints,
  computeTwrFromDayPoints,
  normalizeTradeCalendarDateKey,
  lastPositiveCloseOnOrBefore,
  fxToCnyOnDate,
  inferMarket,
  getSymbolCurrency,
  signedAmount,
} = require("../return-calcs");
const {
  computeLedgerCashCnyUpToDate,
  principalCnyUpToDate,
  externalFlowCnyForDate,
} = require("../ledger-metrics");
const { xirrFromSnapshotWindow, xirrFromSymbolValueFlowPoints } = require("../home-summary-maths");
const { fetchRemoteDailyClosesForSymbol } = require("../daily-close-backfill");
const { fetchSinaForexDayKSeries, enumerateDays, validNumber } = require("../../scripts/lib/market-fetch");
const { resolveFrozenDate } = require("../eod-freeze-service");

const FX_FALLBACK = { USD: 7.2, HKD: 0.92 };

function sortTradeAsc(a, b) {
  const ad = new Date(a.date).getTime();
  const bd = new Date(b.date).getTime();
  if (ad !== bd) return ad - bd;
  return Number(a.createdAt) - Number(b.createdAt);
}

function filterTradesForAccount(allTrades, accountId) {
  const norm = (t) => ({ ...t, date: normalizeTradeCalendarDateKey(t.date) });
  if (accountId === "all") return [...allTrades].map(norm).sort(sortTradeAsc);
  return allTrades.filter((t) => t.accountId === accountId).map(norm).sort(sortTradeAsc);
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

async function buildFxMaps(allDates, logger = console) {
  let fxUsdMap = {};
  let fxHkdMap = {};
  try {
    fxUsdMap = await fetchSinaForexDayKSeries("USDCNY");
  } catch (e) {
    logger.warn?.("[freeze-v3] USDCNY", e?.message || e);
  }
  try {
    fxHkdMap = await fetchSinaForexDayKSeries("HKDCNY");
  } catch (e) {
    logger.warn?.("[freeze-v3] HKDCNY", e?.message || e);
  }
  for (const dateKey of allDates) {
    if (!Number.isFinite(Number(fxUsdMap[dateKey])) || Number(fxUsdMap[dateKey]) <= 0) {
      fxUsdMap[dateKey] = FX_FALLBACK.USD;
    }
    if (!Number.isFinite(Number(fxHkdMap[dateKey])) || Number(fxHkdMap[dateKey]) <= 0) {
      fxHkdMap[dateKey] = FX_FALLBACK.HKD;
    }
  }
  return { fxUsdMap, fxHkdMap };
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

function computeMwrPatch(rowsAsc, asOf, firstTrade) {
  const mapped = rowsAsc.map((r) => ({
    date: r.date,
    totalAssets: r.totalAssets,
    externalFlowCny: r.dailyExternalFlow,
  }));
  return {
    stageMtdRateMwr: xirrFromSnapshotWindow(mapped, windowStartForStage("mtd", asOf, firstTrade), asOf),
    stageYtdRateMwr: xirrFromSnapshotWindow(mapped, windowStartForStage("ytd", asOf, firstTrade), asOf),
    stageInceptionRateMwr: xirrFromSnapshotWindow(
      mapped,
      windowStartForStage("inception", asOf, firstTrade),
      asOf,
    ),
    stageLast7dRateMwr: xirrFromSnapshotWindow(mapped, windowStartForStage("last_7d", asOf, firstTrade), asOf),
    stageLast30dRateMwr: xirrFromSnapshotWindow(mapped, windowStartForStage("last_30d", asOf, firstTrade), asOf),
    stageLast90dRateMwr: xirrFromSnapshotWindow(mapped, windowStartForStage("last_90d", asOf, firstTrade), asOf),
  };
}

function buildURankSymbols(trades, accountId, frozenDate) {
  const D = String(frozenDate).slice(0, 10);
  const d90 = addCalendarDays(D, -89);
  const m0 = monthStartKeyShanghai(D);
  const y0 = yearStartKeyShanghai(D);
  const set = new Set();
  const accTrades = filterTradesForAccount(trades, accountId);
  for (const t of accTrades) {
    const d = String(t.date).slice(0, 10);
    const sym = normalizeSymbol(t.symbol);
    if (!sym || d > D) continue;
    set.add(sym);
    if (d >= d90 || d >= m0 || d >= y0) {
      set.add(sym);
    }
  }
  return set;
}

async function upsertAnalysisBatchV3(client, uid, rows) {
  const now = Date.now();
  for (const r of rows) {
    await client.query(
      `INSERT INTO analysis_daily_snapshot (
         user_id, account_id, date, book_currency, source_version,
         daily_profit, daily_rate_twr, daily_external_flow, daily_cash_delta, tw_r_cumulative,
         market_value, total_assets, cash, cash_ratio, principal,
         fx_hkd_cny, fx_usd_cny,
         stage_mtd_profit, stage_mtd_rate_twr, stage_mtd_rate_mwr,
         stage_ytd_profit, stage_ytd_rate_twr, stage_ytd_rate_mwr,
         stage_inception_profit, stage_inception_rate_twr, stage_inception_rate_mwr,
         stage_last_7d_profit, stage_last_7d_rate_twr, stage_last_7d_rate_mwr,
         stage_last_30d_profit, stage_last_30d_rate_twr, stage_last_30d_rate_mwr,
         stage_last_90d_profit, stage_last_90d_rate_twr, stage_last_90d_rate_mwr,
         profit_cny, tw_r_daily, external_flow_cny, cash_cny,
         created_at, updated_at
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
         $18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,
         $36,$37,$38,$39,$40,$41
       )
       ON CONFLICT (user_id, account_id, date) DO UPDATE SET
         book_currency=EXCLUDED.book_currency, source_version=EXCLUDED.source_version,
         daily_profit=EXCLUDED.daily_profit, daily_rate_twr=EXCLUDED.daily_rate_twr,
         daily_external_flow=EXCLUDED.daily_external_flow, daily_cash_delta=EXCLUDED.daily_cash_delta,
         tw_r_cumulative=EXCLUDED.tw_r_cumulative,
         market_value=EXCLUDED.market_value, total_assets=EXCLUDED.total_assets,
         cash=EXCLUDED.cash, cash_ratio=EXCLUDED.cash_ratio, principal=EXCLUDED.principal,
         fx_hkd_cny=EXCLUDED.fx_hkd_cny, fx_usd_cny=EXCLUDED.fx_usd_cny,
         stage_mtd_profit=EXCLUDED.stage_mtd_profit, stage_mtd_rate_twr=EXCLUDED.stage_mtd_rate_twr, stage_mtd_rate_mwr=EXCLUDED.stage_mtd_rate_mwr,
         stage_ytd_profit=EXCLUDED.stage_ytd_profit, stage_ytd_rate_twr=EXCLUDED.stage_ytd_rate_twr, stage_ytd_rate_mwr=EXCLUDED.stage_ytd_rate_mwr,
         stage_inception_profit=EXCLUDED.stage_inception_profit, stage_inception_rate_twr=EXCLUDED.stage_inception_rate_twr, stage_inception_rate_mwr=EXCLUDED.stage_inception_rate_mwr,
         stage_last_7d_profit=EXCLUDED.stage_last_7d_profit, stage_last_7d_rate_twr=EXCLUDED.stage_last_7d_rate_twr, stage_last_7d_rate_mwr=EXCLUDED.stage_last_7d_rate_mwr,
         stage_last_30d_profit=EXCLUDED.stage_last_30d_profit, stage_last_30d_rate_twr=EXCLUDED.stage_last_30d_rate_twr, stage_last_30d_rate_mwr=EXCLUDED.stage_last_30d_rate_mwr,
         stage_last_90d_profit=EXCLUDED.stage_last_90d_profit, stage_last_90d_rate_twr=EXCLUDED.stage_last_90d_rate_twr, stage_last_90d_rate_mwr=EXCLUDED.stage_last_90d_rate_mwr,
         profit_cny=EXCLUDED.profit_cny, tw_r_daily=EXCLUDED.tw_r_daily,
         external_flow_cny=EXCLUDED.external_flow_cny, cash_cny=EXCLUDED.cash_cny,
         updated_at=EXCLUDED.updated_at`,
      [
        uid,
        r.accountId,
        r.date,
        r.bookCurrency,
        METRICS_SOURCE_VERSION,
        r.dailyProfit,
        r.dailyRateTwr,
        r.dailyExternalFlow,
        r.dailyCashDelta,
        r.twRCumulative,
        r.marketValue,
        r.totalAssets,
        r.cash,
        r.cashRatio,
        r.principal,
        r.fxHkdCny,
        r.fxUsdCny,
        r.stageMtdProfit,
        r.stageMtdRateTwr,
        r.stageMtdRateMwr,
        r.stageYtdProfit,
        r.stageYtdRateTwr,
        r.stageYtdRateMwr,
        r.stageInceptionProfit,
        r.stageInceptionRateTwr,
        r.stageInceptionRateMwr,
        r.stageLast7dProfit,
        r.stageLast7dRateTwr,
        r.stageLast7dRateMwr,
        r.stageLast30dProfit,
        r.stageLast30dRateTwr,
        r.stageLast30dRateMwr,
        r.stageLast90dProfit,
        r.stageLast90dRateTwr,
        r.stageLast90dRateMwr,
        r.dailyProfit,
        r.dailyRateTwr,
        r.dailyExternalFlow,
        r.cash,
        now,
        now,
      ],
    );
  }
}

async function upsertSymbolBatchV3(client, uid, rows) {
  const now = Date.now();
  for (const r of rows) {
    await client.query(
      `INSERT INTO symbol_daily_pnl (
         user_id, account_id, symbol, date, book_currency, source_version,
         daily_profit, daily_trade_qty, daily_trade_amount, daily_trade_flow, daily_rate_twr,
         eod_shares, eod_price,
         stage_mtd_profit, stage_mtd_rate_twr, stage_mtd_rate_mwr,
         stage_ytd_profit, stage_ytd_rate_twr, stage_ytd_rate_mwr,
         stage_inception_profit, stage_inception_rate_twr, stage_inception_rate_mwr,
         stage_last_7d_profit, stage_last_7d_rate_twr, stage_last_7d_rate_mwr,
         stage_last_30d_profit, stage_last_30d_rate_twr, stage_last_30d_rate_mwr,
         stage_last_90d_profit, stage_last_90d_rate_twr, stage_last_90d_rate_mwr,
         day_trade_qty, day_trade_amount, day_trade_flow_native, day_close_price, day_pnl_native, currency,
         created_at, updated_at
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
         $14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,
         $32,$33,$34,$35,$36,$37,$38,$39
       )
       ON CONFLICT (user_id, account_id, symbol, date) DO UPDATE SET
         book_currency=EXCLUDED.book_currency, source_version=EXCLUDED.source_version,
         daily_profit=EXCLUDED.daily_profit, daily_trade_qty=EXCLUDED.daily_trade_qty,
         daily_trade_amount=EXCLUDED.daily_trade_amount, daily_trade_flow=EXCLUDED.daily_trade_flow,
         daily_rate_twr=EXCLUDED.daily_rate_twr, eod_price=EXCLUDED.eod_price,
         stage_mtd_profit=EXCLUDED.stage_mtd_profit, stage_mtd_rate_twr=EXCLUDED.stage_mtd_rate_twr, stage_mtd_rate_mwr=EXCLUDED.stage_mtd_rate_mwr,
         stage_ytd_profit=EXCLUDED.stage_ytd_profit, stage_ytd_rate_twr=EXCLUDED.stage_ytd_rate_twr, stage_ytd_rate_mwr=EXCLUDED.stage_ytd_rate_mwr,
         stage_inception_profit=EXCLUDED.stage_inception_profit, stage_inception_rate_twr=EXCLUDED.stage_inception_rate_twr, stage_inception_rate_mwr=EXCLUDED.stage_inception_rate_mwr,
         stage_last_7d_profit=EXCLUDED.stage_last_7d_profit, stage_last_7d_rate_twr=EXCLUDED.stage_last_7d_rate_twr, stage_last_7d_rate_mwr=EXCLUDED.stage_last_7d_rate_mwr,
         stage_last_30d_profit=EXCLUDED.stage_last_30d_profit, stage_last_30d_rate_twr=EXCLUDED.stage_last_30d_rate_twr, stage_last_30d_rate_mwr=EXCLUDED.stage_last_30d_rate_mwr,
         stage_last_90d_profit=EXCLUDED.stage_last_90d_profit, stage_last_90d_rate_twr=EXCLUDED.stage_last_90d_rate_twr, stage_last_90d_rate_mwr=EXCLUDED.stage_last_90d_rate_mwr,
         day_pnl_native=EXCLUDED.daily_profit, day_trade_flow_native=EXCLUDED.daily_trade_flow,
         updated_at=EXCLUDED.updated_at`,
      [
        uid,
        r.accountId,
        r.symbol,
        r.date,
        r.bookCurrency,
        METRICS_SOURCE_VERSION,
        r.dailyProfit,
        r.dailyTradeQty,
        r.dailyTradeAmount,
        r.dailyTradeFlow,
        r.dailyRateTwr,
        r.eodShares,
        r.eodPrice,
        r.stageMtdProfit,
        r.stageMtdRateTwr,
        r.stageMtdRateMwr,
        r.stageYtdProfit,
        r.stageYtdRateTwr,
        r.stageYtdRateMwr,
        r.stageInceptionProfit,
        r.stageInceptionRateTwr,
        r.stageInceptionRateMwr,
        r.stageLast7dProfit,
        r.stageLast7dRateTwr,
        r.stageLast7dRateMwr,
        r.stageLast30dProfit,
        r.stageLast30dRateTwr,
        r.stageLast30dRateMwr,
        r.stageLast90dProfit,
        r.stageLast90dRateTwr,
        r.stageLast90dRateMwr,
        r.dailyTradeQty,
        r.dailyTradeAmount,
        r.dailyTradeFlow,
        r.eodPrice,
        r.dailyProfit,
        r.currency,
        now,
        now,
      ],
    );
  }
}

async function freezeAccountHistory({
  uid,
  accountId,
  frozenDate,
  allTrades,
  allCash,
  accounts,
  klineBySym,
  fxUsdMap,
  fxHkdMap,
  client,
}) {
  const book = accountBookCurrency(accountId, accounts);
  const accTrades = filterTradesForAccount(allTrades, accountId);
  if (!accTrades.length) return 0;

  const accMinD = String(accTrades[0].date).slice(0, 10);
  const fd = String(frozenDate).slice(0, 10);
  if (accMinD > fd) return 0;
  const accountDates = enumerateDays(accMinD, fd);

  const dayPoints = buildPortfolioDayPoints(
    accTrades,
    accountDates,
    klineBySym,
    fxUsdMap,
    fxHkdMap,
    allCash,
    accountId,
    accounts,
  );
  if (!dayPoints.length) return 0;

  const twrInputs = [];
  for (const p of dayPoints) {
    const dk = p.date;
    const cashCny = computeLedgerCashCnyUpToDate(allTrades, allCash, accounts, accountId, fxUsdMap, fxHkdMap, dk);
    const mvCny = Number(p.nav) || 0;
    twrInputs.push({ date: dk, nav: mvCny + cashCny, extFlow: p.extFlow });
  }
  const twrArr = computeTwrFromDayPoints(twrInputs);

  const stageAcc = new StageAccumulator();
  const rowsAsc = [];
  let prevTa = 0;
  let prevCash = 0;
  const buffer = [];
  const firstTrade = accTrades[0].date;

  for (let i = 0; i < dayPoints.length; i += 1) {
    const p = dayPoints[i];
    const dk = p.date;
    const tw = twrArr[i] || { twRDaily: 0, twRCumulative: 0 };
    const cashCny = computeLedgerCashCnyUpToDate(allTrades, allCash, accounts, accountId, fxUsdMap, fxHkdMap, dk);
    const mvCny = Number(p.nav) || 0;
    const taCny = mvCny + cashCny;
    const cash = cnyToBook(cashCny, book, dk, fxUsdMap, fxHkdMap);
    const mv = cnyToBook(mvCny, book, dk, fxUsdMap, fxHkdMap);
    const ta = cnyToBook(taCny, book, dk, fxUsdMap, fxHkdMap);
    const extCny = externalFlowCnyForDate(allCash, accounts, accountId, fxUsdMap, fxHkdMap, dk);
    const ext = cnyToBook(extCny, book, dk, fxUsdMap, fxHkdMap);
    const principal = cnyToBook(principalCnyUpToDate(allCash, accounts, accountId, fxUsdMap, fxHkdMap, dk), book, dk, fxUsdMap, fxHkdMap);
    const cashDelta = i === 0 ? cash : cash - prevCash;
    const dailyProfit = i === 0 ? 0 : ta - prevTa - ext;
    const denom = prevTa + Math.max(ext, 0);
    const dailyRateTwr = denom > 0 ? dailyProfit / denom : Number(tw.twRDaily) || 0;

    stageAcc.onDay(dk, dailyProfit, dailyRateTwr);
    const snap = stageAcc.snapshotTwr();
    rowsAsc.push({ date: dk, totalAssets: ta, dailyExternalFlow: ext });
    const mwr = computeMwrPatch(rowsAsc, dk, firstTrade);

    buffer.push({
      accountId,
      date: dk,
      bookCurrency: book,
      dailyProfit,
      dailyRateTwr,
      dailyExternalFlow: ext,
      dailyCashDelta: cashDelta,
      twRCumulative: Number(tw.twRCumulative) || snap.stageInceptionRateTwr,
      marketValue: mv,
      totalAssets: ta,
      cash,
      cashRatio: ta > 0 ? cash / ta : 0,
      principal,
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
    });

    prevTa = ta;
    prevCash = cash;
    if (buffer.length >= 400) {
      await upsertAnalysisBatchV3(client, uid, buffer.splice(0, buffer.length));
    }
  }
  if (buffer.length) {
    await upsertAnalysisBatchV3(client, uid, buffer);
  }
  return dayPoints.length;
}

/** 逐日回放：有持仓（含空头负股数）或当日有成交则写入；含日终 eod=0 的清仓日。 */
function replaySymbolDailyRows(sym, accountId, accTrades, allDates, kline, frozenDate) {
  const kl = kline || [];
  const symTrades = accTrades.filter((t) => t.symbol === sym).sort(sortTradeAsc);
  if (!symTrades.length || !kl.length) {
    return null;
  }

  const D = String(frozenDate).slice(0, 10);
  const dates = allDates.filter((d) => d <= D);
  const stageAcc = new StageAccumulator();
  const flowPts = [];
  const dailyOut = [];
  let pi = 0;
  let qty = 0;

  for (const day of dates) {
    while (pi < symTrades.length && symTrades[pi].date < day) {
      const u = symTrades[pi];
      qty += u.side === "buy" ? u.quantity : -u.quantity;
      pi += 1;
    }
    const qBod = qty;
    const dayTrades = [];
    while (pi < symTrades.length && symTrades[pi].date === day) {
      dayTrades.push(symTrades[pi]);
      const u = symTrades[pi];
      qty += u.side === "buy" ? u.quantity : -u.quantity;
      pi += 1;
    }
    const qEod = qty;
    const hasActivity =
      dayTrades.length > 0 || Math.abs(qBod) > 1e-6 || Math.abs(qEod) > 1e-6;
    if (!hasActivity) {
      continue;
    }

    const closeD = lastPositiveCloseOnOrBefore(
      kl.map((x) => ({ day: x.day, close: x.close })),
      day,
    );
    const closePrev = closeBefore(kl, day);
    if (!(closeD > 0)) {
      continue;
    }
    const prevPx = closePrev != null && closePrev > 0 ? closePrev : closeD;

    let dayFlow = 0;
    let dayAmt = 0;
    let dayTurn = 0;
    for (const u of dayTrades) {
      dayFlow += signedAmount(u);
      dayAmt += validNumber(u.amount, 0);
      dayTurn += validNumber(u.quantity, 0);
    }
    const pnl = qEod * closeD - qBod * prevPx - dayFlow;
    const denom = qBod * prevPx + Math.max(dayFlow, 0);
    const rDay = denom > 0 ? pnl / denom : 0;
    stageAcc.onDay(day, pnl, rDay);
    if (Math.abs(qEod) > 1e-6 && closeD > 0) {
      flowPts.push({ date: day, value: qEod * closeD, flow: dayFlow });
    }
    const snap = stageAcc.snapshotTwr();
    const endVal = qEod * closeD;
    const mwrRate = xirrFromSymbolValueFlowPoints(flowPts, day, endVal);
    dailyOut.push({
      date: day,
      dailyProfit: pnl,
      dailyTradeQty: dayTurn,
      dailyTradeAmount: dayAmt,
      dailyTradeFlow: dayFlow,
      dailyRateTwr: rDay,
      eodShares: qEod,
      eodPrice: closeD,
      ...snap,
      stageMtdRateMwr: mwrRate,
      stageYtdRateMwr: mwrRate,
      stageInceptionRateMwr: mwrRate,
      stageLast7dRateMwr: mwrRate,
      stageLast30dRateMwr: mwrRate,
      stageLast90dRateMwr: mwrRate,
    });
  }

  return dailyOut.length ? dailyOut : null;
}

function replaySymbolToDate(sym, accountId, accTrades, allDates, kline, frozenDate) {
  const rows = replaySymbolDailyRows(sym, accountId, accTrades, allDates, kline, frozenDate);
  if (!rows?.length) {
    return null;
  }
  const D = String(frozenDate).slice(0, 10);
  const last = rows.find((r) => r.date === D) || rows[rows.length - 1];
  return { ...last };
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

async function freezeSymbolsForUser(ctx) {
  const {
    uid,
    frozenDate,
    allTrades,
    accounts,
    allDates,
    klineBySym,
    client,
    logger,
    syncMissingCloses,
  } = ctx;
  const accountIds = listAccountIdsForFreeze(allTrades, accounts);
  const unionSyms = new Set();
  for (const accountId of accountIds) {
    for (const sym of buildURankSymbols(allTrades, accountId, frozenDate)) {
      unionSyms.add(sym);
    }
  }
  if (syncMissingCloses) {
    const minD = allDates[0];
    for (const sym of unionSyms) {
      if (klineBySym.has(sym)) continue;
      try {
        const rows = await fetchRemoteDailyClosesForSymbol(sym, minD, frozenDate);
        if (rows.length) {
          await upsertSymbolDailyCloseBatch(
            rows.map((r) => ({ symbol: sym, date: r.date, close: r.close, source: r.source || "sina" })),
          );
          const list = rows
            .map((r) => ({ day: String(r.date).slice(0, 10), close: Number(r.close) }))
            .filter((r) => r.day && r.close > 0)
            .sort((a, b) => a.day.localeCompare(b.day));
          if (list.length) klineBySym.set(sym, list);
        }
      } catch (e) {
        logger.warn?.("[freeze-v3] symbol close sync", sym, e?.message || e);
      }
    }
  }

  const symBuffer = [];
  let symbolRowsWritten = 0;
  for (const accountId of accountIds) {
    const book = accountBookCurrency(accountId, accounts);
    const accTrades = filterTradesForAccount(allTrades, accountId);
    const symSet = new Set(
      accTrades.map((t) => normalizeSymbol(t.symbol)).filter(Boolean),
    );
    for (const sym of symSet) {
      const kl = klineBySym.get(sym);
      if (!kl) {
        logger.warn?.("[freeze-v3] missing kline", accountId, sym);
        continue;
      }
      const dailyRows = replaySymbolDailyRows(sym, accountId, accTrades, allDates, kl, frozenDate);
      if (!dailyRows?.length) {
        continue;
      }
      const ccy = getSymbolCurrency(sym);
      for (const replay of dailyRows) {
        symBuffer.push({
          accountId,
          symbol: sym,
          date: replay.date,
          bookCurrency: ccy || book,
          currency: ccy,
          dailyProfit: replay.dailyProfit,
          dailyTradeQty: replay.dailyTradeQty,
          dailyTradeAmount: replay.dailyTradeAmount,
          dailyTradeFlow: replay.dailyTradeFlow,
          dailyRateTwr: replay.dailyRateTwr,
          eodShares: replay.eodShares,
          eodPrice: replay.eodPrice,
          stageMtdProfit: replay.stageMtdProfit,
          stageMtdRateTwr: replay.stageMtdRateTwr,
          stageMtdRateMwr: replay.stageMtdRateMwr,
          stageYtdProfit: replay.stageYtdProfit,
          stageYtdRateTwr: replay.stageYtdRateTwr,
          stageYtdRateMwr: replay.stageYtdRateMwr,
          stageInceptionProfit: replay.stageInceptionProfit,
          stageInceptionRateTwr: replay.stageInceptionRateTwr,
          stageInceptionRateMwr: replay.stageInceptionRateMwr,
          stageLast7dProfit: replay.stageLast7dProfit,
          stageLast7dRateTwr: replay.stageLast7dRateTwr,
          stageLast7dRateMwr: replay.stageLast7dRateMwr,
          stageLast30dProfit: replay.stageLast30dProfit,
          stageLast30dRateTwr: replay.stageLast30dRateTwr,
          stageLast30dRateMwr: replay.stageLast30dRateMwr,
          stageLast90dProfit: replay.stageLast90dProfit,
          stageLast90dRateTwr: replay.stageLast90dRateTwr,
          stageLast90dRateMwr: replay.stageLast90dRateMwr,
        });
        if (symBuffer.length >= 200) {
          const chunk = symBuffer.splice(0, symBuffer.length);
          await upsertSymbolBatchV3(client, uid, chunk);
          symbolRowsWritten += chunk.length;
        }
      }
    }
  }
  if (symBuffer.length) {
    await upsertSymbolBatchV3(client, uid, symBuffer);
    symbolRowsWritten += symBuffer.length;
  }
  return { symbolRowsWritten, accountIds: accountIds.length };
}

async function runFreezeV3ForUser(userId, options = {}) {
  const logger = options.logger || console;
  const uid = String(userId || "").trim();
  const frozenDate = options.frozenDate || resolveFrozenDate();
  if (!uid) {
    return { ok: false, reason: "missing-user" };
  }

  await ensureMetricsSchemaV3();

  const allTrades = (await getTrades(uid)).map((t) => ({ ...t, symbol: normalizeSymbol(t.symbol) }));
  if (!allTrades.length) {
    return { ok: false, reason: "no-trades" };
  }
  allTrades.sort(sortTradeAsc);
  const minD = allTrades[0].date;
  if (minD > frozenDate) {
    return { ok: false, reason: "trade-after-frozen" };
  }

  const latest = await getLatestAnalysisSnapshotDate(uid, "all");
  if (!options.force && !options.symbolsOnly && latest && latest >= String(frozenDate).slice(0, 10)) {
    return { ok: true, userId: uid, skipped: true, reason: "already-up-to-date", frozenDate: latest };
  }

  const allCash = await getCashTransfers(uid);
  const accounts = await getAccounts(uid);
  const allDates = enumerateDays(minD, frozenDate);
  const symbols = [...new Set(allTrades.map((t) => t.symbol).filter(Boolean))].sort();
  const accountIds = listAccountIdsForFreeze(allTrades, accounts);

  if (options.syncDailyClose && !options.symbolsOnly) {
    for (const sym of symbols) {
      try {
        const rows = await fetchRemoteDailyClosesForSymbol(sym, minD, frozenDate);
        if (rows.length) {
          await upsertSymbolDailyCloseBatch(
            rows.map((r) => ({ symbol: sym, date: r.date, close: r.close, source: r.source || "sina" })),
          );
        }
      } catch (e) {
        logger.warn?.("[freeze-v3] close", sym, e?.message || e);
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

  const timing = { accountMs: 0, symbolMs: 0, accountRows: 0, symbolRows: 0 };

  try {
    await client.query("BEGIN");
    if (options.symbolsOnly) {
      await client.query("DELETE FROM symbol_daily_pnl WHERE user_id = $1", [uid]);
    } else {
      await client.query("DELETE FROM symbol_daily_pnl WHERE user_id = $1", [uid]);
      await client.query("DELETE FROM analysis_daily_snapshot WHERE user_id = $1", [uid]);
    }

    if (!options.symbolsOnly) {
      const t0 = Date.now();
      for (const accountId of accountIds) {
        const n = await freezeAccountHistory({
          uid,
          accountId,
          frozenDate,
          allTrades,
          allCash,
          accounts,
          klineBySym,
          fxUsdMap,
          fxHkdMap,
          client,
        });
        timing.accountRows += n;
      }
      timing.accountMs = Date.now() - t0;
    }

    const t1 = Date.now();
    const symResult = await freezeSymbolsForUser({
      uid,
      frozenDate,
      allTrades,
      accounts,
      allDates,
      klineBySym,
      client,
      logger,
      syncMissingCloses: true,
    });
    timing.symbolRows = symResult.symbolRowsWritten;
    timing.symbolMs = Date.now() - t1;

    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }

  const frozenDateKey = String(frozenDate).slice(0, 10);
  await setSnapshotWatermark(uid, frozenDateKey);
  await upsertUserMetricsMeta(uid, { frozenThrough: frozenDateKey, isCleared: false, clearedAt: null });
  for (const accId of accountIds.filter((a) => a !== "all")) {
    await upsertAccountMetricsMeta(uid, accId, { frozenThrough: frozenDateKey });
  }

  return {
    ok: true,
    userId: uid,
    frozenDate: frozenDateKey,
    timing,
  };
}

async function runSymbolsOnlyV3ForUser(userId, options = {}) {
  return runFreezeV3ForUser(userId, { ...options, symbolsOnly: true, force: true });
}

module.exports = {
  runFreezeV3ForUser,
  runSymbolsOnlyV3ForUser,
  buildURankSymbols,
  freezeSymbolsForUser,
  replaySymbolDailyRows,
};
