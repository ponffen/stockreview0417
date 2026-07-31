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
const {
  StageAccumulator,
  TrackStageGroup,
  windowStartForStage,
  hydrateStageAccFromRow,
  hydrateStageAccFromRowTrack,
  advanceStageAccSessionGap,
} = require("./stage-accumulator");
const {
  computeSymbolDailyProfitTracks,
  applyStageSnapshotsToRow,
} = require("./profit-tracks");
const {
  StageTradeCounter,
  countTradeRecordsOnDate,
  isTradeRecord,
  hydrateStageTradeAccFromRow,
  advanceStageTradeAccSessionGap,
  resolveTradeSnapForFreezeDay,
} = require("./stage-trade-counter");
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
const { validNumber } = require("../../scripts/lib/market-fetch");
const {
  enumerateFreezeSessionDates,
  sessionDatesAfterLatest,
  previousSessionDate,
  ledgerSessionDateKey,
  capFrozenThroughToSnapshot,
} = require("./freeze-calendar");
const { resolveFrozenDate } = require("../eod-freeze-service");
const { buildFxMaps } = require("./fx-maps");
const {
  upsertAnalysisBatchV3,
  upsertSymbolBatchV3,
} = require("./freeze-v3-upsert");

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
  const fx = fxToCnyOnDate(fxUsdMap, fxHkdMap, book, dateKey);
  return fx > 0 ? v / fx : v;
}

function nativeToCny(amountNative, ccy, dateKey, fxUsdMap, fxHkdMap) {
  const v = Number(amountNative) || 0;
  const c = String(ccy || "CNY").toUpperCase();
  if (c === "CNY") return v;
  return v * fxToCnyOnDate(fxUsdMap, fxHkdMap, c, dateKey);
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
    mv += q * c * fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, dk);
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
    const cny = ccy === "CNY" ? nat : nat * fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, dk);
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
    totalAssetsCny: Number(row.total_assets_cny ?? row.total_assets),
    marketValue: Number(row.market_value),
    dailyExternalFlow: Number(row.daily_external_flow),
    dailyProfitCny: Number(row.daily_profit_cny ?? row.daily_profit),
    dailyRateTwrCny: Number(row.daily_rate_twr_cny ?? row.daily_rate_twr),
    twRCumulative: Number(row.tw_r_cumulative),
    stageMtdProfit: Number(row.stage_mtd_profit),
    stageMtdRateTwr: Number(row.stage_mtd_rate_twr),
    stageMtdProfitCny: Number(row.stage_mtd_profit_cny ?? row.stage_mtd_profit),
    stageMtdRateTwrCny: Number(row.stage_mtd_rate_twr_cny ?? row.stage_mtd_rate_twr),
    stageYtdProfit: Number(row.stage_ytd_profit),
    stageYtdRateTwr: Number(row.stage_ytd_rate_twr),
    stageYtdProfitCny: Number(row.stage_ytd_profit_cny ?? row.stage_ytd_profit),
    stageYtdRateTwrCny: Number(row.stage_ytd_rate_twr_cny ?? row.stage_ytd_rate_twr),
    stageInceptionProfit: Number(row.stage_inception_profit),
    stageInceptionRateTwr: Number(row.stage_inception_rate_twr),
    stageInceptionProfitCny: Number(row.stage_inception_profit_cny ?? row.stage_inception_profit),
    stageInceptionRateTwrCny: Number(row.stage_inception_rate_twr_cny ?? row.stage_inception_rate_twr),
    stageLast7dProfit: Number(row.stage_last_7d_profit),
    stageLast7dRateTwr: Number(row.stage_last_7d_rate_twr),
    stageLast7dProfitCny: Number(row.stage_last_7d_profit_cny ?? row.stage_last_7d_profit),
    stageLast7dRateTwrCny: Number(row.stage_last_7d_rate_twr_cny ?? row.stage_last_7d_rate_twr),
    stageLast30dProfit: Number(row.stage_last_30d_profit),
    stageLast30dRateTwr: Number(row.stage_last_30d_rate_twr),
    stageLast30dProfitCny: Number(row.stage_last_30d_profit_cny ?? row.stage_last_30d_profit),
    stageLast30dRateTwrCny: Number(row.stage_last_30d_rate_twr_cny ?? row.stage_last_30d_rate_twr),
    stageLast90dProfit: Number(row.stage_last_90d_profit),
    stageLast90dRateTwr: Number(row.stage_last_90d_rate_twr),
    stageLast90dProfitCny: Number(row.stage_last_90d_profit_cny ?? row.stage_last_90d_profit),
    stageLast90dRateTwrCny: Number(row.stage_last_90d_rate_twr_cny ?? row.stage_last_90d_rate_twr),
    principal: Number(row.principal),
    dailyTradeCount: Number(row.daily_trade_count) || 0,
    stageMtdTradeCount: Number(row.stage_mtd_trade_count) || 0,
    stageYtdTradeCount: Number(row.stage_ytd_trade_count) || 0,
    stageInceptionTradeCount: Number(row.stage_inception_trade_count) || 0,
    stageLast7dTradeCount: Number(row.stage_last_7d_trade_count) || 0,
    stageLast30dTradeCount: Number(row.stage_last_30d_trade_count) || 0,
    stageLast90dTradeCount: Number(row.stage_last_90d_trade_count) || 0,
  };
}

async function loadAnalysisRow(client, uid, accountId, dateKey) {
  const { rows } = await client.query(
    `SELECT date, cash, total_assets, total_assets_cny, market_value, daily_external_flow,
            daily_profit_cny, daily_rate_twr_cny, tw_r_cumulative,
            stage_mtd_profit, stage_mtd_rate_twr, stage_mtd_profit_cny, stage_mtd_rate_twr_cny,
            stage_ytd_profit, stage_ytd_rate_twr, stage_ytd_profit_cny, stage_ytd_rate_twr_cny,
            stage_inception_profit, stage_inception_rate_twr, stage_inception_profit_cny, stage_inception_rate_twr_cny,
            stage_last_7d_profit, stage_last_7d_rate_twr, stage_last_7d_profit_cny, stage_last_7d_rate_twr_cny,
            stage_last_30d_profit, stage_last_30d_rate_twr, stage_last_30d_profit_cny, stage_last_30d_rate_twr_cny,
            stage_last_90d_profit, stage_last_90d_rate_twr, stage_last_90d_profit_cny, stage_last_90d_rate_twr_cny,
            principal,
            daily_trade_count, stage_mtd_trade_count, stage_ytd_trade_count,
            stage_inception_trade_count, stage_last_7d_trade_count,
            stage_last_30d_trade_count, stage_last_90d_trade_count
     FROM analysis_daily_snapshot
     WHERE user_id = $1 AND account_id = $2 AND date = $3`,
    [uid, accountId, dateKey],
  );
  return mapAnalysisDbRow(rows[0]);
}

function mapSymbolStageFields(row) {
  const out = {
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
    stageMtdProfitBook: Number(row.stage_mtd_profit_book ?? row.stage_mtd_profit),
    stageMtdRateTwrBook: Number(row.stage_mtd_rate_twr_book ?? row.stage_mtd_rate_twr),
    stageYtdProfitBook: Number(row.stage_ytd_profit_book ?? row.stage_ytd_profit),
    stageYtdRateTwrBook: Number(row.stage_ytd_rate_twr_book ?? row.stage_ytd_rate_twr),
    stageInceptionProfitBook: Number(row.stage_inception_profit_book ?? row.stage_inception_profit),
    stageInceptionRateTwrBook: Number(row.stage_inception_rate_twr_book ?? row.stage_inception_rate_twr),
    stageLast7dProfitBook: Number(row.stage_last_7d_profit_book ?? row.stage_last_7d_profit),
    stageLast7dRateTwrBook: Number(row.stage_last_7d_rate_twr_book ?? row.stage_last_7d_rate_twr),
    stageLast30dProfitBook: Number(row.stage_last_30d_profit_book ?? row.stage_last_30d_profit),
    stageLast30dRateTwrBook: Number(row.stage_last_30d_rate_twr_book ?? row.stage_last_30d_rate_twr),
    stageLast90dProfitBook: Number(row.stage_last_90d_profit_book ?? row.stage_last_90d_profit),
    stageLast90dRateTwrBook: Number(row.stage_last_90d_rate_twr_book ?? row.stage_last_90d_rate_twr),
    stageMtdProfitCny: Number(row.stage_mtd_profit_cny ?? row.stage_mtd_profit),
    stageMtdRateTwrCny: Number(row.stage_mtd_rate_twr_cny ?? row.stage_mtd_rate_twr),
    stageYtdProfitCny: Number(row.stage_ytd_profit_cny ?? row.stage_ytd_profit),
    stageYtdRateTwrCny: Number(row.stage_ytd_rate_twr_cny ?? row.stage_ytd_rate_twr),
    stageInceptionProfitCny: Number(row.stage_inception_profit_cny ?? row.stage_inception_profit),
    stageInceptionRateTwrCny: Number(row.stage_inception_rate_twr_cny ?? row.stage_inception_rate_twr),
    stageLast7dProfitCny: Number(row.stage_last_7d_profit_cny ?? row.stage_last_7d_profit),
    stageLast7dRateTwrCny: Number(row.stage_last_7d_rate_twr_cny ?? row.stage_last_7d_rate_twr),
    stageLast30dProfitCny: Number(row.stage_last_30d_profit_cny ?? row.stage_last_30d_profit),
    stageLast30dRateTwrCny: Number(row.stage_last_30d_rate_twr_cny ?? row.stage_last_30d_rate_twr),
    stageLast90dProfitCny: Number(row.stage_last_90d_profit_cny ?? row.stage_last_90d_profit),
    stageLast90dRateTwrCny: Number(row.stage_last_90d_rate_twr_cny ?? row.stage_last_90d_rate_twr),
  };
  return out;
}

function mapSymbolDbRow(row) {
  if (!row) return null;
  return {
    date: String(row.date || row.d || "").slice(0, 10),
    ...mapSymbolStageFields(row),
    dailyTradeCount: Number(row.daily_trade_count) || 0,
    stageMtdTradeCount: Number(row.stage_mtd_trade_count) || 0,
    stageYtdTradeCount: Number(row.stage_ytd_trade_count) || 0,
    stageInceptionTradeCount: Number(row.stage_inception_trade_count) || 0,
    stageLast7dTradeCount: Number(row.stage_last_7d_trade_count) || 0,
    stageLast30dTradeCount: Number(row.stage_last_30d_trade_count) || 0,
    stageLast90dTradeCount: Number(row.stage_last_90d_trade_count) || 0,
  };
}

const SYMBOL_STAGE_SELECT = `
  stage_mtd_profit, stage_mtd_rate_twr, stage_ytd_profit, stage_ytd_rate_twr,
  stage_inception_profit, stage_inception_rate_twr,
  stage_last_7d_profit, stage_last_7d_rate_twr,
  stage_last_30d_profit, stage_last_30d_rate_twr,
  stage_last_90d_profit, stage_last_90d_rate_twr,
  stage_mtd_profit_book, stage_mtd_rate_twr_book, stage_ytd_profit_book, stage_ytd_rate_twr_book,
  stage_inception_profit_book, stage_inception_rate_twr_book,
  stage_last_7d_profit_book, stage_last_7d_rate_twr_book,
  stage_last_30d_profit_book, stage_last_30d_rate_twr_book,
  stage_last_90d_profit_book, stage_last_90d_rate_twr_book,
  stage_mtd_profit_cny, stage_mtd_rate_twr_cny, stage_ytd_profit_cny, stage_ytd_rate_twr_cny,
  stage_inception_profit_cny, stage_inception_rate_twr_cny,
  stage_last_7d_profit_cny, stage_last_7d_rate_twr_cny,
  stage_last_30d_profit_cny, stage_last_30d_rate_twr_cny,
  stage_last_90d_profit_cny, stage_last_90d_rate_twr_cny`;

async function loadSymbolRow(client, uid, accountId, sym, dateKey) {
  const { rows } = await client.query(
    `SELECT date::text AS date, ${SYMBOL_STAGE_SELECT},
            daily_trade_count, stage_mtd_trade_count, stage_ytd_trade_count,
            stage_inception_trade_count, stage_last_7d_trade_count,
            stage_last_30d_trade_count, stage_last_90d_trade_count
     FROM symbol_daily_pnl
     WHERE user_id = $1 AND account_id = $2 AND symbol = $3 AND date = $4`,
    [uid, accountId, normalizeSymbol(sym), dateKey],
  );
  return mapSymbolDbRow(rows[0]);
}

async function loadLatestSymbolRowBefore(client, uid, accountId, sym, beforeDateKey) {
  const { rows } = await client.query(
    `SELECT date::text AS date, ${SYMBOL_STAGE_SELECT},
            daily_trade_count, stage_mtd_trade_count, stage_ytd_trade_count,
            stage_inception_trade_count, stage_last_7d_trade_count,
            stage_last_30d_trade_count, stage_last_90d_trade_count
     FROM symbol_daily_pnl
     WHERE user_id = $1 AND account_id = $2 AND symbol = $3 AND date <= $4
     ORDER BY date DESC
     LIMIT 1`,
    [uid, accountId, normalizeSymbol(sym), beforeDateKey],
  );
  return mapSymbolDbRow(rows[0]);
}

async function loadSymbolDailyProfitsByTrack(client, uid, accountId, sym, fromD, toD) {
  const { rows } = await client.query(
    `SELECT date::text AS d, daily_profit, daily_profit_book, daily_profit_cny
     FROM symbol_daily_pnl
     WHERE user_id = $1 AND account_id = $2 AND symbol = $3 AND date >= $4 AND date <= $5
     ORDER BY date`,
    [uid, accountId, normalizeSymbol(sym), fromD, toD],
  );
  const native = new Map();
  const book = new Map();
  const cny = new Map();
  for (const r of rows) {
    const d = String(r.d).slice(0, 10);
    native.set(d, Number(r.daily_profit) || 0);
    book.set(d, Number(r.daily_profit_book ?? r.daily_profit) || 0);
    cny.set(d, Number(r.daily_profit_cny ?? r.daily_profit) || 0);
  }
  return { native, book, cny };
}

async function loadSymbolProfitByDate(client, uid, accountId, sym, fromD, toD) {
  const { rows } = await client.query(
    `SELECT date::text AS d, daily_profit FROM symbol_daily_pnl
     WHERE user_id = $1 AND account_id = $2 AND symbol = $3 AND date >= $4 AND date <= $5
     ORDER BY date`,
    [uid, accountId, normalizeSymbol(sym), fromD, toD],
  );
  const map = new Map();
  for (const r of rows) {
    map.set(String(r.d).slice(0, 10), Number(r.daily_profit) || 0);
  }
  return map;
}

async function loadSymbolFlowPts(client, uid, accountId, sym, fromD, toD) {
  const { rows } = await client.query(
    `SELECT date::text AS d, eod_shares, eod_price, day_trade_flow_native
     FROM symbol_daily_pnl
     WHERE user_id = $1 AND account_id = $2 AND symbol = $3 AND date >= $4 AND date <= $5
     ORDER BY date`,
    [uid, accountId, normalizeSymbol(sym), fromD, toD],
  );
  const pts = [];
  for (const r of rows) {
    const d = String(r.d).slice(0, 10);
    const sh = Number(r.eod_shares) || 0;
    const px = Number(r.eod_price);
    if (!d || !(sh > 0) || !(px > 0)) continue;
    pts.push({
      date: d,
      value: sh * px,
      flow: Number(r.day_trade_flow_native) || 0,
    });
  }
  return pts;
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

async function loadProfitCnyByDate(client, uid, accountId, fromD, toD) {
  const { rows } = await client.query(
    `SELECT date::text AS d, daily_profit_cny FROM analysis_daily_snapshot
     WHERE user_id = $1 AND account_id = $2 AND date >= $3 AND date <= $4 ORDER BY date`,
    [uid, accountId, fromD, toD],
  );
  const map = new Map();
  for (const r of rows) {
    map.set(String(r.d).slice(0, 10), Number(r.daily_profit_cny) || 0);
  }
  return map;
}

async function loadTradeCountByDate(client, uid, accountId, fromD, toD) {
  const { rows } = await client.query(
    `SELECT date::text AS d, daily_trade_count FROM analysis_daily_snapshot
     WHERE user_id = $1 AND account_id = $2 AND date >= $3 AND date <= $4 ORDER BY date`,
    [uid, accountId, fromD, toD],
  );
  const map = new Map();
  for (const r of rows) {
    map.set(String(r.d).slice(0, 10), Number(r.daily_trade_count) || 0);
  }
  return map;
}

async function loadSymbolTradeCountByDate(client, uid, accountId, sym, fromD, toD) {
  const { rows } = await client.query(
    `SELECT date::text AS d, daily_trade_count FROM symbol_daily_pnl
     WHERE user_id = $1 AND account_id = $2 AND symbol = $3 AND date >= $4 AND date <= $5
     ORDER BY date`,
    [uid, accountId, normalizeSymbol(sym), fromD, toD],
  );
  const map = new Map();
  for (const r of rows) {
    map.set(String(r.d).slice(0, 10), Number(r.daily_trade_count) || 0);
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
  const cashCny =
    book === "CNY"
      ? cash
      : computeLedgerCashCnyUpToDate(
          sessionTrades,
          sessionCash,
          accounts,
          accountId,
          fxUsdMap,
          fxHkdMap,
          dk,
        );
  const taCny = cashCny + mvCny;

  const prevTa = yesterday ? Number(yesterday.totalAssets) : 0;
  const dailyProfit = yesterday ? ta - prevTa - ext : 0;
  const denom = prevTa + Math.max(ext, 0);
  const dailyRateTwr = denom > 0 ? dailyProfit / denom : 0;
  const prevTaCny = yesterday ? Number(yesterday.totalAssetsCny) : 0;
  const dailyProfitCny = yesterday ? taCny - prevTaCny - extCny : 0;
  const denomCny = prevTaCny + Math.max(extCny, 0);
  const dailyRateTwrCny = denomCny > 0 ? dailyProfitCny / denomCny : 0;
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
  const stageAccCny = new StageAccumulator();
  const stageTradeAcc = new StageTradeCounter();
  const dailyTradeCount = countTradeRecordsOnDate(accTrades, dk);
  if (yesterday && prevD) {
    const fromP = addCalendarDaysForProfit(dk, -89);
    const profitMap = accountMem?.profitByDate || (await loadProfitByDate(client, uid, accountId, fromP, prevD));
    for (const [d, p] of profitMap) {
      stageAcc.profitByDate.set(d, p);
    }
    const profitCnyMap =
      accountMem?.profitCnyByDate || (await loadProfitCnyByDate(client, uid, accountId, fromP, prevD));
    for (const [d, p] of profitCnyMap) {
      stageAccCny.profitByDate.set(d, p);
    }
    const tradeCountMap =
      accountMem?.tradeCountByDate || (await loadTradeCountByDate(client, uid, accountId, fromP, prevD));
    for (const [d, c] of tradeCountMap) {
      stageTradeAcc.countByDate.set(d, c);
    }
    hydrateStageAccFromRow(stageAcc, yesterday, dk);
    hydrateStageAccFromRowTrack(stageAccCny, yesterday, dk, "cny");
    hydrateStageTradeAccFromRow(stageTradeAcc, yesterday, dk);
    if (yesterday.date) {
      advanceStageAccSessionGap(stageAcc, yesterday.date, dk);
      advanceStageAccSessionGap(stageAccCny, yesterday.date, dk);
      advanceStageTradeAccSessionGap(stageTradeAcc, yesterday.date, dk);
    }
  }
  stageAcc.onDay(dk, dailyProfit, dailyRateTwr);
  stageAccCny.onDay(dk, dailyProfitCny, dailyRateTwrCny);
  stageTradeAcc.onDay(dk, dailyTradeCount);
  const snap = stageAcc.snapshotTwr();
  const snapCny = stageAccCny.snapshotTwr();
  const tradeSnap = resolveTradeSnapForFreezeDay(dailyTradeCount, stageTradeAcc.snapshot(), yesterday);

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
    dailyProfitCny,
    dailyRateTwrCny,
    totalAssetsCny: taCny,
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
    stageMtdProfitCny: snapCny.stageMtdProfit,
    stageMtdRateTwrCny: snapCny.stageMtdRateTwr,
    stageYtdProfitCny: snapCny.stageYtdProfit,
    stageYtdRateTwrCny: snapCny.stageYtdRateTwr,
    stageInceptionProfitCny: snapCny.stageInceptionProfit,
    stageInceptionRateTwrCny: snapCny.stageInceptionRateTwr,
    stageLast7dProfitCny: snapCny.stageLast7dProfit,
    stageLast7dRateTwrCny: snapCny.stageLast7dRateTwr,
    stageLast30dProfitCny: snapCny.stageLast30dProfit,
    stageLast30dRateTwrCny: snapCny.stageLast30dRateTwr,
    stageLast90dProfitCny: snapCny.stageLast90dProfit,
    stageLast90dRateTwrCny: snapCny.stageLast90dRateTwr,
    dailyTradeCount,
    ...tradeSnap,
  };

  const profitByDate = accountMem?.profitByDate || new Map();
  profitByDate.set(dk, dailyProfit);
  const profitCnyByDate = accountMem?.profitCnyByDate || new Map();
  profitCnyByDate.set(dk, dailyProfitCny);
  const tradeCountByDate = accountMem?.tradeCountByDate || new Map();
  tradeCountByDate.set(dk, dailyTradeCount);

  return { row, rowsAsc, lastRow: row, profitByDate, profitCnyByDate, tradeCountByDate };
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
  client,
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

  const prevD = previousSessionDate(dk);
  const ccy = getSymbolCurrency(sym);
  const tracks = computeSymbolDailyProfitTracks({
    qty,
    qBod,
    closeD,
    prevPx,
    dayFlow,
    ccy,
    book,
    dk,
    prevD: prevD || dk,
    fxUsdMap,
    fxHkdMap,
  });
  const pnl = tracks.native.profit;
  const rDay = tracks.native.rateTwr;

  let yesterday = symbolMem?.lastRow || null;
  let profitByDate = symbolMem?.profitByDate || null;
  let flowPts = symbolMem?.flowPts ? [...symbolMem.flowPts] : [];

  if (!yesterday && prevD && client && uid) {
    yesterday = await loadSymbolRow(client, uid, accountId, sym, prevD);
    if (!yesterday) {
      yesterday = await loadLatestSymbolRowBefore(client, uid, accountId, sym, prevD);
    }
    if (yesterday) {
      if (!profitByDate) {
        const fromP = addCalendarDaysForProfit(dk, -89);
        profitByDate = await loadSymbolProfitByDate(client, uid, accountId, sym, fromP, prevD);
      }
      if (!flowPts.length) {
        const fromFlow = addCalendarDaysForProfit(dk, -89);
        flowPts = await loadSymbolFlowPts(client, uid, accountId, sym, fromFlow, prevD);
      }
    }
  }

  const stageGroup = new TrackStageGroup();
  const stageTradeAcc = new StageTradeCounter();
  const dailyTradeCount = dayTrades.filter((t) => isTradeRecord(t)).length;
  if (yesterday) {
    let profitMaps = symbolMem?.profitMaps || null;
    if (!profitMaps && prevD && client && uid) {
      const fromP = addCalendarDaysForProfit(dk, -89);
      profitMaps = await loadSymbolDailyProfitsByTrack(client, uid, accountId, sym, fromP, prevD);
    }
    if (profitMaps) {
      for (const [d, p] of profitMaps.native) {
        stageGroup.native.profitByDate.set(d, p);
      }
      for (const [d, p] of profitMaps.book) {
        stageGroup.book.profitByDate.set(d, p);
      }
      for (const [d, p] of profitMaps.cny) {
        stageGroup.cny.profitByDate.set(d, p);
      }
    }
    let tradeCountMap = symbolMem?.tradeCountByDate || null;
    if (!tradeCountMap && prevD && client && uid) {
      const fromP = addCalendarDaysForProfit(dk, -89);
      tradeCountMap = await loadSymbolTradeCountByDate(client, uid, accountId, sym, fromP, prevD);
    }
    for (const [d, c] of tradeCountMap || []) {
      stageTradeAcc.countByDate.set(d, c);
    }
    stageGroup.hydrateFromRow(yesterday, dk);
    hydrateStageTradeAccFromRow(stageTradeAcc, yesterday, dk);
    const rowDate = yesterday.date || prevD;
    if (rowDate) {
      stageGroup.advanceGap(rowDate, dk);
      advanceStageTradeAccSessionGap(stageTradeAcc, rowDate, dk);
    }
  }
  stageGroup.onDay(dk, tracks);
  stageTradeAcc.onDay(dk, dailyTradeCount);
  const snaps = stageGroup.snapshotTwr();
  const tradeSnap = resolveTradeSnapForFreezeDay(dailyTradeCount, stageTradeAcc.snapshot(), yesterday);
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
  const eodMarketValueNative = qty * closeD;
  const mvCnySym = nativeToCny(eodMarketValueNative, ccy, dk, fxUsdMap, fxHkdMap);
  const positionWeight = taCny > 0 ? mvCnySym / taCny : 0;

  const row = {
    accountId,
    symbol: sym,
    date: dk,
    bookCurrency: book,
    currency: ccy,
    dailyProfit: tracks.native.profit,
    dailyProfitBook: tracks.book.profit,
    dailyProfitCny: tracks.cny.profit,
    dailyTradeQty: dayTurn,
    dailyTradeAmount: dayAmt,
    dailyTradeFlow: dayFlow,
    dailyRateTwr: tracks.native.rateTwr,
    dailyRateTwrBook: tracks.book.rateTwr,
    dailyRateTwrCny: tracks.cny.rateTwr,
    eodShares: qty,
    eodPrice: closeD,
    eodMarketValueNative,
    positionWeight,
    stageMtdRateMwr: mwrRate,
    stageYtdRateMwr: mwrRate,
    stageInceptionRateMwr: mwrRate,
    stageLast7dRateMwr: mwrRate,
    stageLast30dRateMwr: mwrRate,
    stageLast90dRateMwr: mwrRate,
    dailyTradeCount,
    ...tradeSnap,
  };
  applyStageSnapshotsToRow(row, snaps);

  const profitByDateOut = profitByDate || new Map();
  profitByDateOut.set(dk, pnl);
  const profitMapsOut = {
    native: profitByDateOut,
    book: symbolMem?.profitMaps?.book || new Map(),
    cny: symbolMem?.profitMaps?.cny || new Map(),
  };
  profitMapsOut.book.set(dk, tracks.book.profit);
  profitMapsOut.cny.set(dk, tracks.cny.profit);
  const tradeCountByDateOut = symbolMem?.tradeCountByDate || new Map();
  tradeCountByDateOut.set(dk, dailyTradeCount);

  return {
    row,
    eodShares: qty,
    flowPts,
    profitMaps: profitMapsOut,
    lastRow: {
      date: dk,
      ...mapSymbolStageFields({
        stage_mtd_profit: row.stageMtdProfit,
        stage_mtd_rate_twr: row.stageMtdRateTwr,
        stage_ytd_profit: row.stageYtdProfit,
        stage_ytd_rate_twr: row.stageYtdRateTwr,
        stage_inception_profit: row.stageInceptionProfit,
        stage_inception_rate_twr: row.stageInceptionRateTwr,
        stage_last_7d_profit: row.stageLast7dProfit,
        stage_last_7d_rate_twr: row.stageLast7dRateTwr,
        stage_last_30d_profit: row.stageLast30dProfit,
        stage_last_30d_rate_twr: row.stageLast30dRateTwr,
        stage_last_90d_profit: row.stageLast90dProfit,
        stage_last_90d_rate_twr: row.stageLast90dRateTwr,
        stage_mtd_profit_book: row.stageMtdProfitBook,
        stage_mtd_rate_twr_book: row.stageMtdRateTwrBook,
        stage_ytd_profit_book: row.stageYtdProfitBook,
        stage_ytd_rate_twr_book: row.stageYtdRateTwrBook,
        stage_inception_profit_book: row.stageInceptionProfitBook,
        stage_inception_rate_twr_book: row.stageInceptionRateTwrBook,
        stage_last_7d_profit_book: row.stageLast7dProfitBook,
        stage_last_7d_rate_twr_book: row.stageLast7dRateTwrBook,
        stage_last_30d_profit_book: row.stageLast30dProfitBook,
        stage_last_30d_rate_twr_book: row.stageLast30dRateTwrBook,
        stage_last_90d_profit_book: row.stageLast90dProfitBook,
        stage_last_90d_rate_twr_book: row.stageLast90dRateTwrBook,
        stage_mtd_profit_cny: row.stageMtdProfitCny,
        stage_mtd_rate_twr_cny: row.stageMtdRateTwrCny,
        stage_ytd_profit_cny: row.stageYtdProfitCny,
        stage_ytd_rate_twr_cny: row.stageYtdRateTwrCny,
        stage_inception_profit_cny: row.stageInceptionProfitCny,
        stage_inception_rate_twr_cny: row.stageInceptionRateTwrCny,
        stage_last_7d_profit_cny: row.stageLast7dProfitCny,
        stage_last_7d_rate_twr_cny: row.stageLast7dRateTwrCny,
        stage_last_30d_profit_cny: row.stageLast30dProfitCny,
        stage_last_30d_rate_twr_cny: row.stageLast30dRateTwrCny,
        stage_last_90d_profit_cny: row.stageLast90dProfitCny,
        stage_last_90d_rate_twr_cny: row.stageLast90dRateTwrCny,
      }),
      dailyTradeCount: row.dailyTradeCount,
      stageMtdTradeCount: row.stageMtdTradeCount,
      stageYtdTradeCount: row.stageYtdTradeCount,
      stageInceptionTradeCount: row.stageInceptionTradeCount,
      stageLast7dTradeCount: row.stageLast7dTradeCount,
      stageLast30dTradeCount: row.stageLast30dTradeCount,
      stageLast90dTradeCount: row.stageLast90dTradeCount,
    },
    profitByDate: profitByDateOut,
    tradeCountByDate: tradeCountByDateOut,
  };
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
    await upsertUserMetricsMeta(uid, { rebuilding: false, rebuildFrom: null });
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
    await upsertUserMetricsMeta(uid, { rebuilding: false, rebuildFrom: null });
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
          profitCnyByDate: result.profitCnyByDate,
          tradeCountByDate: result.tradeCountByDate,
        });
        analysisBuffer.push(result.row);
        accountRowsWritten += 1;
        const book = accountBookCurrency(accountId, accounts);
        let taCny = Number(result.row.totalAssets);
        if (book === "USD") {
          taCny *= Number(fxUsdMap[dk]) || 0;
        } else if (book === "HKD") {
          taCny *= Number(fxHkdMap[dk]) || 0;
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
            client,
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
    rebuilding: false,
    rebuildFrom: null,
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
