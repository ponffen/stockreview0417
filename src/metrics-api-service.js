/**
 * Metrics API a–h：读路径编排（L1 冻结 + L2 实时）。
 */
const {
  getSettings,
  getTrades,
  getCashTransfers,
  getAccounts,
  getLatestAnalysisSnapshotDate,
  getAnalysisDailySnapshots,
  getHomeSummaryForUser,
  getSymbolDailyCloseRange,
  resolveBookCurrencyForAccountScope,
  getUserMetricsMeta,
  getAccountMetricsMetaForUser,
  getLastEodSharesForUser,
  getSymbolDailyEodRowsAtDate,
  fetchHomeBundleFrozenPack,
  normalizeSymbol,
} = require("./db");
const {
  fmtMoney,
  fmtPlainAmount,
  fmtPlainSignedAmount,
  fmtPercentRatio,
  fmtSignedPercentRatio,
} = require("./account-kpi-surface");
const {
  buildProfitSeries,
  metricsForWindow,
  lastAnalysisDailyRowOnOrBefore,
  xirrTodayOnly,
  xirrStageToLive,
} = require("./home-summary-maths");
const { getComputeLiveMetrics } = require("./market-realtime-pnl");
const {
  ALL_STAGES,
  parseStagesParam,
  resolveStageRange,
  homeUiStageToApi,
  isFreshStagePeriod,
  stageUsesFrozenCumulativeFields,
} = require("./metrics/stages");
const { shouldEmitTodayLivePoint, liveDateKeyShanghai } = require("./metrics/trading-calendar");
const { isWeekendDateKey } = require("./metrics/freeze-calendar");
const { buildHoldingsPayload } = require("./metrics/holdings-display");
const {
  chainTwrRate,
  accountDailyTwrReturn,
  todayProfitCnyFromTotals,
} = require("./metrics/snapshot-plus-live");
const { holdingsSymbolsFromTrades, hasOpenPositionQuantity } = require("./metrics/holdings-active-symbols");
const { buildStockRankPayload } = require("./metrics/stock-rank");
const { buildBenchmarkSeriesPayload } = require("./metrics/benchmark-series");
const { buildStockRecordBundlePayload } = require("./metrics/stock-record-bundle");
const { finalizeMetricsBundlePayload } = require("./metrics/bundle-payload");
const {
  isAggregateScope,
  formatSignedProfitForScope,
  formatPlainAssetForScope,
  formatMoneyAssetForScope,
  resolveAccountAssetScalars,
  liveProfitScalarToBook,
} = require("./metrics/account-book-metrics");

const METRICS_RULE_VERSION = 4;
const BENCHMARK_SYMBOLS = new Set(["sh000001", "sz399001", "rt_hkHSI", "gb_inx"]);
const STANDARD_RETURN_STAGES = new Set(["today", "mtd", "ytd", "inception"]);
const METRICS_DB_BATCH_SIZE = Math.max(1, Math.min(3, Number(process.env.METRICS_DB_BATCH_SIZE || 3)));

function useMetricsDbBatching() {
  return process.env.METRICS_DB_BATCH === "1" || process.env.VERCEL === "1";
}

async function runMetricsDbBatch(tasks) {
  const list = tasks.map((fn) => (typeof fn === "function" ? fn : () => fn));
  if (!useMetricsDbBatching()) {
    return Promise.all(list.map((fn) => fn()));
  }
  const out = [];
  for (let i = 0; i < list.length; i += METRICS_DB_BATCH_SIZE) {
    const chunk = list.slice(i, i + METRICS_DB_BATCH_SIZE);
    const part = await Promise.all(chunk.map((fn) => fn()));
    out.push(...part);
  }
  return out;
}

function isScopeMetricsCleared(scope, um, accountMetaList) {
  if (um?.isCleared === true) {
    return true;
  }
  const sc = String(scope || "all").trim() || "all";
  if (sc === "all") {
    return false;
  }
  const meta = (accountMetaList || []).find((m) => String(m.accountId) === sc);
  return meta?.isCleared === true;
}

function accountsFromHomeBundlePack(pack) {
  if (Array.isArray(pack?.accounts) && pack.accounts.length > 0) {
    return pack.accounts;
  }
  if (Array.isArray(pack?.settings?.accounts) && pack.settings.accounts.length > 0) {
    return pack.settings.accounts;
  }
  return [];
}

function filterActiveSymbolHomeRows(symbolRows, trades, scope, lastEodRows) {
  const active = new Set(holdingsSymbolsFromTrades(trades, scope, lastEodRows));
  if (!active.size) {
    return [];
  }
  return (symbolRows || []).filter((r) => active.has(normalizeSymbol(r.symbol)));
}



function metaEnvelope(userId, scope, settings, live, extra = {}) {
  const algo = String(settings?.algoMode || "twr").toLowerCase() === "mwr" ? "mwr" : "twr";
  const book = resolveBookCurrencyForAccountScope(settings, scope);
  return {
    accountScope: scope,
    bookCurrency: book,
    algoMode: algo,
    ruleVersion: METRICS_RULE_VERSION,
    frozenThrough: live?.frozenThrough || extra.frozenThrough || null,
    liveDate: live?.tradingDay ? live.liveDate : null,
    tradingDay: !!live?.tradingDay,
    delayed: !!live?.delayed,
    quoteTime: live?.quoteTime ?? null,
    dataVersion: extra.dataVersion ?? 0,
    rebuilding: !!extra.rebuilding,
    ...extra,
  };
}

function buildBundleMeta(ctx, extra = {}) {
  const { scope, settings, live, um } = ctx;
  const base = metaEnvelope(ctx.userId, scope, settings, live, {
    dataVersion: Number(um?.dataVersion) || 0,
    rebuilding: !!um?.rebuilding,
    ...extra,
  });
  return {
    accountId: scope,
    frozenThrough: base.frozenThrough,
    liveDate: base.liveDate,
    tradingDay: base.tradingDay,
    quoteTime: base.quoteTime,
    bookCurrency: base.bookCurrency,
    algoMode: base.algoMode,
    dataVersion: base.dataVersion,
    rebuilding: base.rebuilding,
    delayed: base.delayed,
    ruleVersion: base.ruleVersion,
    ...(extra.stage != null ? { stage: extra.stage } : {}),
    ...(extra.from != null ? { from: extra.from } : {}),
    ...(extra.to != null ? { to: extra.to } : {}),
  };
}

function fxFromCtx(ctx) {
  const { home, live } = ctx;
  const acc = home?.account;
  return {
    fxU: Number(acc?.eod_fx_usd_cny) || live.fxUsdCny || 0,
    fxH: Number(acc?.eod_fx_hkd_cny) || live.fxHkdCny || 0,
    book: resolveBookCurrencyForAccountScope(ctx.settings, ctx.scope),
  };
}

function customStageRangeFromOpts(opts = {}) {
  const from = String(opts.customFrom || "").slice(0, 10);
  const to = String(opts.customTo || "").slice(0, 10);
  return from && to ? { from, to } : null;
}

function tradesForMetricsScope(trades, scope) {
  const sc = String(scope || "all").trim() || "all";
  if (sc === "all" || !Array.isArray(trades)) {
    return trades;
  }
  return trades.filter((t) => String(t.accountId || "default") === sc);
}

function firstTradeDateFromCtx(ctx, fallback) {
  const asOf = String(fallback || ctx.live?.frozenThrough || liveDateKeyShanghai()).slice(0, 10);
  const fromAccount = String(
    ctx.home?.account?.first_trade_date || ctx.home?.account?.firstTradeDate || "",
  ).slice(0, 10);
  if (fromAccount) {
    return fromAccount;
  }
  return firstTradeDateFromTrades(tradesForMetricsScope(ctx.trades, ctx.scope), asOf);
}

async function computeReturnStages(ctx, stagesRaw, customRange = null) {
  const { userId, scope, settings, live, home } = ctx;
  const asOf = live.frozenThrough || live.liveDate || liveDateKeyShanghai();
  const frozen = frozenMetricsFromHomeAccount(home.account);
  const raw = String(stagesRaw ?? "").trim();
  const want = raw === "custom" ? ["custom"] : parseStagesParam(stagesRaw);
  const mwrMode = String(settings?.algoMode || "twr").toLowerCase() === "mwr";
  const stagesNeedingRows = stageKeysNeedingSnapshotRows(want, live, mwrMode);
  const firstTrade = firstTradeDateFromCtx(ctx, asOf);
  let rowsAsc = [];
  if (stagesNeedingRows.length) {
    const rangeAsOf = liveDateKeyShanghai();
    let minStart = rangeAsOf;
    for (const key of stagesNeedingRows) {
      const { start } = resolveStageRange(key, rangeAsOf, firstTrade, customRange);
      if (start < minStart) {
        minStart = start;
      }
    }
    rowsAsc = await loadSnapshotRowsAsc(userId, scope, minStart, asOf);
  }
  const { fxU, fxH, book } = fxFromCtx(ctx);
  const scopeCtx = { scope, bookCurrency: book, fxUsdCny: fxU, fxHkdCny: fxH };
  const stages = {};
  for (const key of want) {
    stages[key] = stageProfitFromFrozenAndLive(
      key,
      frozen,
      live,
      firstTrade,
      rowsAsc,
      asOf,
      scopeCtx,
      customRange,
    );
  }
  return { stages, mwrMode, rowsAsc, firstTrade };
}

function formatReturnStagesApi(stages, ctx) {
  const { fxU, fxH, book } = fxFromCtx(ctx);
  const mwrMode = String(ctx.settings?.algoMode || "twr").toLowerCase() === "mwr";
  const out = {};
  for (const [key, row] of Object.entries(stages || {})) {
    const rate = mwrMode ? row.rateMwr : row.rateTwr;
    out[key] = {
      profit: formatSignedProfitForScope(row.profitCny, ctx.scope, book, fxU, fxH),
      rate: fmtSignedPercentRatio(rate),
    };
  }
  return out;
}

function buildAssetsApi(ctx) {
  const { live, scope } = ctx;
  const { fxU, fxH, book } = fxFromCtx(ctx);
  const scalars = resolveAccountAssetScalars({
    ...ctx,
    bookCurrency: book,
    fxUsdCny: fxU,
    fxHkdCny: fxH,
  });
  const { totalAssets: ta, marketValue: mv, cash, principal, cashRatioPct } = scalars;
  const ratioStr =
    ta > 0 ? fmtPercentRatio(cash / ta) : fmtPercentRatio((Number(cashRatioPct) || 0) / 100);
  const stockRatioStr = ta > 0 ? fmtPercentRatio(mv / ta) : fmtPercentRatio(0);
  return {
    totalAssets: formatPlainAssetForScope(ta, scope, book, fxU, fxH),
    marketValue: formatPlainAssetForScope(mv, scope, book, fxU, fxH),
    cash: formatPlainAssetForScope(cash, scope, book, fxU, fxH),
    cashRatio: ratioStr,
    stockRatio: stockRatioStr,
    principal: formatPlainAssetForScope(principal, scope, book, fxU, fxH),
  };
}

/** 分析「收益走势」：按所选 stage 取快照行 stage_*_profit（累计），非 daily_profit。 */
function stageProfitCnyFromSnapshotRow(row, stageKey) {
  const st = String(stageKey || "mtd").trim() || "mtd";
  if (st === "today") {
    return Number(row.dailyProfit ?? row.profitCny ?? 0);
  }
  if (st === "mtd") {
    return Number(row.stageMtdProfit ?? 0);
  }
  if (st === "ytd") {
    return Number(row.stageYtdProfit ?? 0);
  }
  if (st === "inception") {
    return Number(row.stageInceptionProfit ?? 0);
  }
  if (st === "last_7d") {
    return Number(row.stageLast7dProfit ?? 0);
  }
  if (st === "last_30d") {
    return Number(row.stageLast30dProfit ?? 0);
  }
  if (st === "last_90d") {
    return Number(row.stageLast90dProfit ?? 0);
  }
  return Number(row.dailyProfit ?? row.profitCny ?? 0);
}

/** 分析「收益率走势」：按所选 stage 取快照行 stage_*_rate_twr（累计），非 TWR 重算。 */
function stageRateTwrFromSnapshotRow(row, stageKey) {
  const st = String(stageKey || "mtd").trim() || "mtd";
  if (st === "today") {
    return Number(row.dailyRateTwr ?? row.twRDaily ?? 0);
  }
  if (st === "mtd") {
    return Number(row.stageMtdRateTwr ?? 0);
  }
  if (st === "ytd") {
    return Number(row.stageYtdRateTwr ?? 0);
  }
  if (st === "inception") {
    return Number(row.stageInceptionRateTwr ?? 0);
  }
  if (st === "last_7d") {
    return Number(row.stageLast7dRateTwr ?? 0);
  }
  if (st === "last_30d") {
    return Number(row.stageLast30dRateTwr ?? 0);
  }
  if (st === "last_90d") {
    return Number(row.stageLast90dRateTwr ?? 0);
  }
  return Number(row.dailyRateTwr ?? row.twRDaily ?? 0);
}

function stageRateMwrFromSnapshotRow(row, stageKey) {
  const st = String(stageKey || "mtd").trim() || "mtd";
  if (st === "today") {
    return Number(row.dailyRateMwr ?? row.dailyRateTwr ?? row.twRDaily ?? 0);
  }
  if (st === "mtd") {
    return Number(row.stageMtdRateMwr ?? 0);
  }
  if (st === "ytd") {
    return Number(row.stageYtdRateMwr ?? 0);
  }
  if (st === "inception") {
    return Number(row.stageInceptionRateMwr ?? 0);
  }
  if (st === "last_7d") {
    return Number(row.stageLast7dRateMwr ?? 0);
  }
  if (st === "last_30d") {
    return Number(row.stageLast30dRateMwr ?? 0);
  }
  if (st === "last_90d") {
    return Number(row.stageLast90dRateMwr ?? 0);
  }
  return Number(row.dailyRateMwr ?? row.dailyRateTwr ?? row.twRDaily ?? 0);
}

async function loadSnapshotRowsAsc(userId, scope, from, to) {
  const rows = await getAnalysisDailySnapshots({ accountId: scope, from, to }, userId);
  return rows
    .map((r) => ({
      date: String(r.date || "").slice(0, 10),
      profitCny: Number(r.dailyProfit ?? r.profitCny ?? r.profit_cny ?? 0),
      dailyProfit: Number(r.dailyProfit ?? r.profitCny ?? 0),
      dailyRateTwr: Number(r.dailyRateTwr ?? r.twRDaily ?? 0),
      dailyRateMwr: Number(r.dailyRateMwr ?? r.dailyRateTwr ?? r.twRDaily ?? 0),
      twRDaily: Number(r.twRDaily ?? r.dailyRateTwr ?? 0),
      stageMtdProfit: Number(r.stageMtdProfit ?? 0),
      stageMtdRateTwr: Number(r.stageMtdRateTwr ?? 0),
      stageMtdRateMwr: Number(r.stageMtdRateMwr ?? 0),
      stageYtdProfit: Number(r.stageYtdProfit ?? 0),
      stageYtdRateTwr: Number(r.stageYtdRateTwr ?? 0),
      stageYtdRateMwr: Number(r.stageYtdRateMwr ?? 0),
      stageInceptionProfit: Number(r.stageInceptionProfit ?? 0),
      stageInceptionRateTwr: Number(r.stageInceptionRateTwr ?? 0),
      stageInceptionRateMwr: Number(r.stageInceptionRateMwr ?? 0),
      stageLast7dProfit: Number(r.stageLast7dProfit ?? 0),
      stageLast7dRateTwr: Number(r.stageLast7dRateTwr ?? 0),
      stageLast7dRateMwr: Number(r.stageLast7dRateMwr ?? 0),
      stageLast30dProfit: Number(r.stageLast30dProfit ?? 0),
      stageLast30dRateTwr: Number(r.stageLast30dRateTwr ?? 0),
      stageLast30dRateMwr: Number(r.stageLast30dRateMwr ?? 0),
      stageLast90dProfit: Number(r.stageLast90dProfit ?? 0),
      stageLast90dRateTwr: Number(r.stageLast90dRateTwr ?? 0),
      stageLast90dRateMwr: Number(r.stageLast90dRateMwr ?? 0),
      totalAssets: Number(r.totalAssets ?? r.total_assets ?? 0) || Number(r.marketValue ?? r.market_value ?? 0),
      marketValue: Number(r.marketValue ?? r.market_value ?? 0),
      cash: Number(r.cash ?? 0),
      cashRatio: Number(r.cashRatio ?? r.cash_ratio ?? 0),
      principal: Number(r.principal ?? 0),
      externalFlowCny: Number(r.dailyExternalFlow ?? r.externalFlowCny ?? r.external_flow_cny ?? 0),
      twRCumulative: Number(r.twRCumulative ?? r.tw_r_cumulative ?? 0),
    }))
    .filter((r) => r.date && !isWeekendDateKey(r.date))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function stageProfitFromFrozenAndLive(
  stageKey,
  frozenMetrics,
  live,
  firstTradeDate,
  rowsAsc,
  asOf,
  scopeCtx = null,
  customRange = null,
) {
  const tradingSession = liveDateKeyShanghai();
  const frozenDate = String(live.frozenThrough || asOf).slice(0, 10);
  const liveDate = live.tradingDay
    ? String(live.liveDate || tradingSession).slice(0, 10)
    : frozenDate;
  const rangeAsOf = tradingSession;
  const { start } = resolveStageRange(stageKey, rangeAsOf, firstTradeDate, customRange);
  const freshPeriod =
    stageUsesFrozenCumulativeFields(stageKey) && isFreshStagePeriod(start, frozenDate);
  const todayP = Number(live.todayProfitCny);
  const todayPFinite = Number.isFinite(todayP) ? todayP : todayProfitCnyFromTotals(live);
  const scope = scopeCtx?.scope ?? "all";
  const book = scopeCtx?.bookCurrency ?? "CNY";
  const fxU = scopeCtx?.fxUsdCny ?? 0;
  const fxH = scopeCtx?.fxHkdCny ?? 0;
  const todayProfit = liveProfitScalarToBook(todayPFinite, scope, book, fxU, fxH);
  let frozenProfit = 0;
  let rateTwr = 0;
  let rateMwr = 0;
  if (!freshPeriod) {
    if (stageKey === "mtd") {
      frozenProfit = frozenMetrics.monthProfitCny;
      rateTwr = frozenMetrics.monthRateTwr;
      rateMwr = frozenMetrics.monthRateMwr;
    } else if (stageKey === "ytd") {
      frozenProfit = frozenMetrics.ytdProfitCny;
      rateTwr = frozenMetrics.ytdRateTwr;
      rateMwr = frozenMetrics.ytdRateMwr;
    } else if (stageKey === "inception") {
      frozenProfit = frozenMetrics.totalProfitCny;
      rateTwr = frozenMetrics.totalRateTwr;
      rateMwr = frozenMetrics.totalRateMwr;
    } else if (stageKey !== "today") {
      const { end } = resolveStageRange(stageKey, rangeAsOf, firstTradeDate, customRange);
      const m = metricsForWindow(rowsAsc, start, end);
      frozenProfit = m.profitCny;
      rateTwr = m.rateTwr;
      rateMwr = m.rateMwr;
    }
  } else if (stageKey !== "today" && !stageUsesFrozenCumulativeFields(stageKey)) {
    const { end } = resolveStageRange(stageKey, rangeAsOf, firstTradeDate, customRange);
    const m = metricsForWindow(rowsAsc, start, end);
    frozenProfit = m.profitCny;
    rateTwr = m.rateTwr;
    rateMwr = m.rateMwr;
  }
  const frozenTa =
    Number(live.eodTotalAssetsCny) || Number(frozenMetrics.eodTotalAssetsCny) || 0;
  const flowToday = liveProfitScalarToBook(
    Number(live.externalFlowTodayCny) || 0,
    scope,
    book,
    fxU,
    fxH,
  );
  const liveTaRaw = Number(live.totalAssetsCny) || frozenTa;
  const liveTa = isAggregateScope(scope)
    ? liveTaRaw
    : liveProfitScalarToBook(liveTaRaw, scope, book, fxU, fxH) || frozenTa;
  let profitCny = frozenProfit;
  const baseMvToday = Number(live.lastMarketValueCny) || frozenTa;
  const baseMvTodayBook = isAggregateScope(scope)
    ? baseMvToday
    : liveProfitScalarToBook(baseMvToday, scope, book, fxU, fxH) || frozenTa;
  if (stageKey === "today") {
    profitCny = todayProfit;
    rateTwr =
      live.tradingDay && baseMvTodayBook > 0 && Math.abs(todayProfit) > 1e-9
        ? todayProfit / baseMvTodayBook
        : 0;
    rateMwr = rateTwr;
  } else if (freshPeriod) {
    if (live.tradingDay) {
      profitCny = todayProfit;
      rateTwr =
        baseMvTodayBook > 0 && Math.abs(todayProfit) > 1e-9 ? todayProfit / baseMvTodayBook : 0;
      rateMwr = rateTwr;
      const rowsForMwr = appendLiveSnapshotRow(rowsAsc, live, liveDate, scopeCtx);
      if (rowsForMwr.length) {
        rateMwr = xirrStageToLive(rowsForMwr, start, liveDate, liveTa);
      }
    } else {
      profitCny = 0;
      rateTwr = 0;
      rateMwr = 0;
    }
  } else if (live.tradingDay) {
    profitCny = frozenProfit + todayProfit;
    const rToday =
      Math.abs(todayProfit) > 1e-9 ? accountDailyTwrReturn(frozenTa, liveTa, flowToday) : 0;
    rateTwr = chainTwrRate(rateTwr, rToday);
    const rowsForMwr = appendLiveSnapshotRow(rowsAsc, live, liveDate, scopeCtx);
    rateMwr = xirrStageToLive(rowsForMwr, start, liveDate, liveTa);
  } else if (STANDARD_RETURN_STAGES.has(stageKey) && stageUsesFrozenCumulativeFields(stageKey)) {
    const taEnd = live.tradingDay
      ? liveTa
      : Number(frozenMetrics.eodTotalAssetsCny) || liveTa;
    rateMwr = rowsAsc.length ? xirrStageToLive(rowsAsc, start, rangeAsOf, taEnd) : rateMwr;
  }
  return { profitCny, rateTwr, rateMwr };
}

/** 非标准 stage，或 MWR/交易日需 analysis 日快照 */
function stageKeysNeedingSnapshotRows(want, live, mwrMode) {
  const set = new Set(want);
  const out = want.filter((k) => !STANDARD_RETURN_STAGES.has(k));
  if (mwrMode || live.tradingDay) {
    for (const k of ["mtd", "ytd", "inception"]) {
      if (set.has(k) && !out.includes(k)) {
        out.push(k);
      }
    }
  }
  return out;
}

function appendLiveSnapshotRow(rowsAsc, live, liveDate, scopeCtx = null) {
  if (!liveDate) {
    return rowsAsc;
  }
  const ld = String(liveDate).slice(0, 10);
  const scope = scopeCtx?.scope ?? "all";
  const book = scopeCtx?.bookCurrency ?? "CNY";
  const fxU = scopeCtx?.fxUsdCny ?? 0;
  const fxH = scopeCtx?.fxHkdCny ?? 0;
  const out = rowsAsc.filter((r) => r.date < ld);
  out.push({
    date: ld,
    totalAssets: liveProfitScalarToBook(live.totalAssetsCny, scope, book, fxU, fxH),
    marketValue: liveProfitScalarToBook(live.liveMarketValueCny, scope, book, fxU, fxH),
    externalFlowCny: liveProfitScalarToBook(live.externalFlowTodayCny, scope, book, fxU, fxH),
    profitCny: liveProfitScalarToBook(live.todayProfitCny, scope, book, fxU, fxH),
  });
  return out;
}

function frozenMetricsFromHomeAccount(acc) {
  if (!acc) {
    return {
      monthProfitCny: 0,
      monthRateTwr: 0,
      monthRateMwr: 0,
      ytdProfitCny: 0,
      ytdRateTwr: 0,
      ytdRateMwr: 0,
      totalProfitCny: 0,
      totalRateTwr: 0,
      totalRateMwr: 0,
      lastMarketValueCny: 0,
      eodTotalAssetsCny: 0,
      eodMarketValueCny: 0,
      eodCashCny: 0,
    };
  }
  return {
    monthProfitCny: Number(acc.month_profit_cny) || 0,
    monthRateTwr: Number(acc.month_rate_twr) || 0,
    monthRateMwr: Number(acc.month_rate_mwr) || 0,
    ytdProfitCny: Number(acc.ytd_profit_cny) || 0,
    ytdRateTwr: Number(acc.ytd_rate_twr) || 0,
    ytdRateMwr: Number(acc.ytd_rate_mwr) || 0,
    totalProfitCny: Number(acc.total_profit_cny) || 0,
    totalRateTwr: Number(acc.total_rate_twr) || 0,
    totalRateMwr: Number(acc.total_rate_mwr) || 0,
    lastMarketValueCny: Number(acc.last_market_value_cny) || Number(acc.eod_market_value_cny) || 0,
    eodTotalAssetsCny: Number(acc.eod_total_assets_cny) || 0,
    eodMarketValueCny: Number(acc.eod_market_value_cny) || 0,
    eodCashCny: Number(acc.eod_cash_cny) || 0,
  };
}


const homeBundleCache = new Map();
const analysisBundleCache = new Map();
const HOME_BUNDLE_CACHE_MS = Math.max(
  0,
  Math.min(30_000, Number(process.env.HOME_BUNDLE_CACHE_MS || 12_000)),
);
const ANALYSIS_BUNDLE_CACHE_MS = Math.max(
  0,
  Math.min(30_000, Number(process.env.ANALYSIS_BUNDLE_CACHE_MS || 12_000)),
);
const ANALYSIS_BUNDLE_DEADLINE_MS = Math.max(
  8_000,
  Math.min(55_000, Number(process.env.ANALYSIS_BUNDLE_DEADLINE_MS || 48_000)),
);
const HOME_BUNDLE_DEADLINE_MS = Math.max(
  8_000,
  Math.min(55_000, Number(process.env.HOME_BUNDLE_DEADLINE_MS || 48_000)),
);
const LIVE_METRICS_MAX_MS = Math.max(
  3_000,
  Math.min(25_000, Number(process.env.LIVE_METRICS_MAX_MS || 18_000)),
);


function homeBundleUsesFrozenOnly() {
  return String(process.env.HOME_BUNDLE_FROZEN_ONLY || "").trim() === "1";
}

function filterSymbolHomeRowsByEod(symbolRows, scope, lastEodRows) {
  if (!lastEodRows?.length) {
    return symbolRows || [];
  }
  const wanted = String(scope || "all").trim() || "all";
  const active = new Set();
  for (const row of lastEodRows || []) {
    const acc = String(row.accountId || row.account_id || "default");
    if (wanted !== "all" && acc !== wanted) {
      continue;
    }
    const sym = normalizeSymbol(row.symbol);
    if (sym && hasOpenPositionQuantity(Number(row.eodShares ?? row.eod_shares) || 0)) {
      active.add(sym);
    }
  }
  if (!active.size) {
    return [];
  }
  return (symbolRows || []).filter((r) => active.has(normalizeSymbol(r.symbol)));
}

function liveFromFrozenPack(homeAcc, lastEodRows, scope) {
  const base = frozenOnlyLiveFromHomeAccount(homeAcc);
  const positions = [];
  const wanted = String(scope || "all").trim() || "all";
  for (const row of lastEodRows || []) {
    const acc = String(row.accountId || "default");
    if (wanted !== "all" && acc !== wanted) {
      continue;
    }
    const qty = Number(row.eodShares ?? row.eod_shares) || 0;
    if (!hasOpenPositionQuantity(qty)) {
      continue;
    }
    positions.push({
      symbol: normalizeSymbol(row.symbol),
      quantity: qty,
      current: 0,
      prevClose: 0,
      todayProfitCny: 0,
      marketValueCny: 0,
    });
  }
  return { ...base, positions, snapshotOnly: true };
}


function frozenOnlyLiveFromHomeAccount(homeAcc) {
  const ft = String(homeAcc?.frozen_through || homeAcc?.frozenThrough || "").slice(0, 10) || null;
  const ta = Number(homeAcc?.eod_total_assets_cny) || 0;
  const mv = Number(homeAcc?.eod_market_value_cny) || 0;
  const cash = Number(homeAcc?.eod_cash_cny) || 0;
  const total = ta || mv + cash;
  return {
    tradingDay: false,
    liveDate: null,
    frozenThrough: ft,
    delayed: true,
    quoteTime: null,
    todayProfitCny: 0,
    liveMarketValueCny: mv,
    lastMarketValueCny: Number(homeAcc?.last_market_value_cny) || mv,
    cashCny: cash,
    totalAssetsCny: total,
    cashRatio: total > 0 ? cash / total : (Number(homeAcc?.eod_cash_ratio) || 0) / 100,
    principalCny: Number(homeAcc?.eod_principal_cny) || 0,
    positions: [],
    fxUsdCny: Number(homeAcc?.eod_fx_usd_cny) || 7.2,
    fxHkdCny: Number(homeAcc?.eod_fx_hkd_cny) || 0.92,
    degradedFrozen: true,
  };
}

async function raceWithTimeout(promise, ms, onTimeout) {
  let timer;
  const timeout = new Promise((resolve) => {
    timer = setTimeout(() => resolve(onTimeout()), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}



async function loadMetricsScopeContext(userId, accountScope, diag = null) {
  const scope = String(accountScope || "all").trim() || "all";
  const phases = diag || null;
  const mark = async (name, fn) => {
    const t0 = Date.now();
    const v = await fn();
    if (phases) phases[name] = Date.now() - t0;
    return v;
  };
  const [settings, home, um] = await runMetricsDbBatch([
    () => mark("db.settings", () => getSettings(userId)),
    () => mark("db.home", () => getHomeSummaryForUser(userId, scope)),
    () => mark("db.userMeta", () => getUserMetricsMeta(userId, { light: true })),
  ]);
  const [trades, cashTransfers, accounts] = await runMetricsDbBatch([
    () => mark("db.trades", () => getTrades(userId)),
    () => mark("db.cashTransfers", () => getCashTransfers(userId)),
    () => mark("db.accounts", () => getAccounts(userId)),
  ]);
  const accountMetaList = await mark("db.accountMeta", () => getAccountMetricsMetaForUser(userId));
  const scopeCleared = isScopeMetricsCleared(scope, um, accountMetaList);
  let lastEodRows = [];
  let frozenSymbolEodRows = [];
  const frozenThroughKey = String(home.account?.frozen_through || um?.frozenThrough || "").slice(0, 10);
  if (!scopeCleared) {
    lastEodRows = await mark("db.lastEodShares", () => getLastEodSharesForUser(userId));
    if (frozenThroughKey) {
      frozenSymbolEodRows = await mark("db.frozenSymbolEod", () =>
        getSymbolDailyEodRowsAtDate(userId, scope, frozenThroughKey),
      );
    }
  }
  home.symbols = filterActiveSymbolHomeRows(home.symbols, trades, scope, lastEodRows);
  let liveFallback = false;
  const live = await mark("live", () => {
    if (scopeCleared) {
      liveFallback = true;
      return frozenOnlyLiveFromHomeAccount(home.account);
    }
    return raceWithTimeout(
      getComputeLiveMetrics(userId, scope, {
        preloaded: {
          trades,
          cashTransfers,
          accounts: accounts.length > 0 ? accounts : undefined,
          homeAccount: home.account,
          scopeCleared,
          lastEodRows,
          frozenSymbolEodRows,
        },
      }),
      LIVE_METRICS_MAX_MS,
      () => {
        liveFallback = true;
        return frozenOnlyLiveFromHomeAccount(home.account);
      },
    );
  });
  if (phases && liveFallback) {
    phases.liveFallback = true;
  }
  if (phases) phases.total = Object.values(phases).reduce((s, n) => s + (Number(n) || 0), 0);
  return { userId, scope, settings, live, um, home, accountMetaList, scopeCleared, trades, cashTransfers };
}


async function loadMetricsScopeContextDbOnly(userId, accountScope, diag = null) {
  const scope = String(accountScope || "all").trim() || "all";
  const phases = diag || null;
  const mark = async (name, fn) => {
    const t0 = Date.now();
    const v = await fn();
    if (phases) phases[name] = Date.now() - t0;
    return v;
  };
  const [settings, home, um] = await runMetricsDbBatch([
    () => mark("db.settings", () => getSettings(userId)),
    () => mark("db.home", () => getHomeSummaryForUser(userId, scope)),
    () => mark("db.userMeta", () => getUserMetricsMeta(userId, { light: true })),
  ]);
  const [trades, cashTransfers, accounts] = await runMetricsDbBatch([
    () => mark("db.trades", () => getTrades(userId)),
    () => mark("db.cashTransfers", () => getCashTransfers(userId)),
    () => mark("db.accounts", () => getAccounts(userId)),
  ]);
  const accountMetaList = await mark("db.accountMeta", () => getAccountMetricsMetaForUser(userId));
  const scopeCleared = isScopeMetricsCleared(scope, um, accountMetaList);
  let lastEodRows = [];
  if (!scopeCleared) {
    lastEodRows = await mark("db.lastEodShares", () => getLastEodSharesForUser(userId));
  }
  home.symbols = filterActiveSymbolHomeRows(home.symbols, trades, scope, lastEodRows);
  const live = frozenOnlyLiveFromHomeAccount(home.account);
  if (phases) {
    phases.live = 0;
    phases.liveSkipped = true;
    phases.total = Object.values(phases).reduce((s, n) => s + (Number(n) || 0), 0);
  }
  return { userId, scope, settings, live, um, home, accountMetaList, scopeCleared, trades, cashTransfers };
}


async function loadMetricsScopeContextSnapshotOnly(userId, accountScope, diag = null) {
  const scope = String(accountScope || "all").trim() || "all";
  const phases = diag || null;
  const mark = async (name, fn) => {
    const t0 = Date.now();
    const v = await fn();
    if (phases) phases[name] = Date.now() - t0;
    return v;
  };

  const pack = await mark("db.pack", () => fetchHomeBundleFrozenPack(userId, scope));
  if (pack) {
    const scopeCleared = isScopeMetricsCleared(scope, pack.um, pack.accountMetaList);
    const symbols = filterSymbolHomeRowsByEod(pack.home.symbols, scope, pack.lastEodRows);
    pack.home.symbols = symbols;
    const live = liveFromFrozenPack(pack.home.account, pack.lastEodRows, scope);
    if (phases) {
      phases.live = 0;
      phases.liveSkipped = true;
      phases.snapshotOnly = true;
      phases.singleConnection = true;
      phases.total = Object.values(phases).reduce((s, n) => s + (Number(n) || 0), 0);
    }
    return {
      userId,
      scope,
      settings: pack.settings,
      live,
      um: pack.um,
      home: pack.home,
      accountMetaList: pack.accountMetaList,
      scopeCleared,
      snapshotOnly: true,
      trades: pack.trades || [],
      cashTransfers: pack.cashTransfers || [],
    };
  }

  return loadMetricsScopeContextDbOnly(userId, accountScope, diag);
}

async function loadMetricsScopeContextLiveFromPack(userId, accountScope, diag = null) {
  const scope = String(accountScope || "all").trim() || "all";
  const phases = diag || null;
  const mark = async (name, fn) => {
    const t0 = Date.now();
    const v = await fn();
    if (phases) phases[name] = Date.now() - t0;
    return v;
  };

  const pack = await mark("db.pack", () => fetchHomeBundleFrozenPack(userId, scope));
  if (!pack) {
    return loadMetricsScopeContext(userId, accountScope, diag);
  }

  const { settings, home, um, accountMetaList, lastEodRows, frozenSymbolEodRows, trades, cashTransfers, accounts } =
    {
      settings: pack.settings,
      home: pack.home,
      um: pack.um,
      accountMetaList: pack.accountMetaList,
      lastEodRows: pack.lastEodRows,
      frozenSymbolEodRows: pack.frozenSymbolEodRows || [],
      trades: pack.trades || [],
      cashTransfers: pack.cashTransfers || [],
      accounts: accountsFromHomeBundlePack(pack),
    };
  const scopeCleared = isScopeMetricsCleared(scope, um, accountMetaList);
  home.symbols = filterActiveSymbolHomeRows(home.symbols, trades, scope, lastEodRows);

  let liveFallback = false;
  const live = await mark("live", () => {
    if (scopeCleared) {
      liveFallback = true;
      return frozenOnlyLiveFromHomeAccount(home.account);
    }
    return raceWithTimeout(
      getComputeLiveMetrics(userId, scope, {
        preloaded: {
          trades,
          cashTransfers,
          accounts,
          homeAccount: home.account,
          scopeCleared,
          lastEodRows,
          frozenSymbolEodRows,
        },
      }),
      LIVE_METRICS_MAX_MS,
      () => {
        liveFallback = true;
        return frozenOnlyLiveFromHomeAccount(home.account);
      },
    );
  });
  if (phases) {
    if (liveFallback) phases.liveFallback = true;
    phases.snapshotOnly = false;
    phases.singleConnection = true;
    phases.total = Object.values(phases).reduce((s, n) => s + (Number(n) || 0), 0);
  }
  return {
    userId,
    scope,
    settings,
    live,
    um,
    home,
    accountMetaList,
    scopeCleared,
    snapshotOnly: false,
    trades,
    cashTransfers,
  };
}

async function loadMetricsScopeContextForHome(userId, accountScope, diag = null) {
  if (homeBundleUsesFrozenOnly()) {
    return loadMetricsScopeContextSnapshotOnly(userId, accountScope, diag);
  }
  return loadMetricsScopeContextLiveFromPack(userId, accountScope, diag);
}

async function probeMetricsHomeBundleDb(userId, accountScope) {
  const phases = {};
  const t0 = Date.now();
  const ctx = await loadMetricsScopeContextSnapshotOnly(userId, accountScope, phases);
  phases.wallMs = Date.now() - t0;
  return {
    phases,
    scopeCleared: !!ctx.scopeCleared,
    symbolRows: ctx.home?.symbols?.length ?? 0,
    hasHomeAccount: !!ctx.home?.account,
    frozenThrough: ctx.live?.frozenThrough || null,
  };
}

async function buildMetricsHoldingsFromContext(ctx, overviewStagesInternal) {
  const { userId, scope, settings, live, home } = ctx;
  const trades = ctx.trades || (await getTrades(userId));
  const rows = await buildHoldingsPayload({
    userId,
    accountScope: scope,
    settings,
    live,
    symbolRows: home.symbols,
    accountRow: home.account,
    trades,
    overviewStages: overviewStagesInternal || {},
  });
  return { rows };
}


async function assembleHomeBundleFromContext(ctx, stagesRaw, diag, extraDiag = {}) {
  const { stages } = await computeReturnStages(ctx, stagesRaw);
  const overviewInternal = {};
  for (const [k, v] of Object.entries(stages)) {
    overviewInternal[k] = { profitCny: v.profitCny };
  }
  const value = {
    meta: buildBundleMeta(ctx),
    returns: { stages: formatReturnStagesApi(stages, ctx) },
    assets: buildAssetsApi(ctx),
    holdings: await buildMetricsHoldingsFromContext(ctx, overviewInternal),
  };
  if (diag) {
    value._diag = {
      phases: diag,
      scopeCleared: !!ctx.scopeCleared,
      symbolRows: ctx.home?.symbols?.length ?? 0,
      snapshotOnly: ctx.snapshotOnly === true,
      liveQuotes: ctx.snapshotOnly !== true,
      ...extraDiag,
    };
  }
  return finalizeMetricsBundlePayload(value);
}

async function getMetricsHomeBundle(userId, accountScope, stagesRaw, opts = {}) {
  const scope = String(accountScope || "all").trim() || "all";
  const stagesKey = String(stagesRaw || "").trim();
  const wantDiag = opts.diag === true || opts.diagOnly === true;
  const cacheKey = `${String(userId || "").trim()}|${scope}|${stagesKey}`;
  const now = Date.now();
  if (opts.diagOnly) {
    return { _diag: await probeMetricsHomeBundleDb(userId, scope) };
  }
  if (!wantDiag && HOME_BUNDLE_CACHE_MS > 0) {
    const hit = homeBundleCache.get(cacheKey);
    if (hit && now - hit.at < HOME_BUNDLE_CACHE_MS) {
      return hit.value;
    }
  }
  const diag = wantDiag ? {} : null;
  const buildFull = async () => {
    const ctx = await loadMetricsScopeContextForHome(userId, accountScope, diag);
    return assembleHomeBundleFromContext(ctx, stagesRaw, diag);
  };
  let value;
  try {
    value = await raceWithTimeout(buildFull(), HOME_BUNDLE_DEADLINE_MS, null);
    if (!value) {
      throw new Error("HOME_BUNDLE_DEADLINE");
    }
  } catch (err) {
    if (String(err?.message || err) !== "HOME_BUNDLE_DEADLINE") {
      throw err;
    }
    const ctx = await loadMetricsScopeContextSnapshotOnly(userId, accountScope, diag);
    value = await assembleHomeBundleFromContext(ctx, stagesRaw, diag, {
      degraded: true,
      reason: "HOME_BUNDLE_DEADLINE",
      deadlineMs: HOME_BUNDLE_DEADLINE_MS,
    });
  }
  if (!wantDiag && HOME_BUNDLE_CACHE_MS > 0) {
    homeBundleCache.set(cacheKey, { at: now, value });
  }
  return value;
}


function firstTradeDateFromTrades(trades, fallback) {
  const asOf = String(fallback || "").slice(0, 10);
  if (!Array.isArray(trades) || !trades.length) {
    return asOf;
  }
  return [...trades].sort((a, b) => String(a.date).localeCompare(String(b.date)))[0].date;
}

async function buildSeriesDailyProfitFromContext(ctx, stage, trades, rowsAsc, customRange = null) {
  const { userId, scope, settings, live, um, home } = ctx;
  const { fxU, fxH, book } = fxFromCtx(ctx);
  const asOf = live.frozenThrough || liveDateKeyShanghai();
  const sessionAsOf = liveDateKeyShanghai();
  const firstTrade = firstTradeDateFromCtx({ ...ctx, trades: trades ?? ctx.trades }, asOf);
  const st = String(stage || "mtd").trim() || "mtd";
  const { start, end } = resolveStageRange(st, sessionAsOf, firstTrade, customRange);
  const rows = rowsAsc || (await loadSnapshotRowsAsc(userId, scope, start, end));
  const filtered = rows.filter((r) => r.date >= start && r.date <= end);
  let points;
  if (st === "custom") {
    let cum = 0;
    points = filtered.map((r) => {
      cum += Number(r.dailyProfit) || 0;
      return {
        date: r.date,
        profit: formatSignedProfitForScope(cum, scope, book, fxU, fxH),
      };
    });
  } else {
    points = filtered.map((r) => ({
      date: r.date,
      profit: formatSignedProfitForScope(stageProfitCnyFromSnapshotRow(r, st), scope, book, fxU, fxH),
    }));
  }
  const scopeCtx = { scope, bookCurrency: book, fxUsdCny: fxU, fxHkdCny: fxH };
  if (live.tradingDay) {
    const liveDate = String(live.liveDate || "").slice(0, 10);
    if (liveDate >= start && liveDate <= end) {
      const frozen = frozenMetricsFromHomeAccount(home?.account);
      const { profitCny } = stageProfitFromFrozenAndLive(
        st,
        frozen,
        live,
        firstTrade,
        rows,
        asOf,
        scopeCtx,
        customRange,
      );
      const row = {
        date: liveDate,
        profit: formatSignedProfitForScope(profitCny, scope, book, fxU, fxH),
      };
      const hit = points.findIndex((p) => p.date === liveDate);
      if (hit >= 0) {
        points[hit] = row;
      } else {
        points.push(row);
        points.sort((a, b) => String(a.date).localeCompare(String(b.date)));
      }
    }
  }
  return { meta: metaEnvelope(userId, scope, settings, live, um), stage: st, points };
}

async function buildSeriesDailyTwrFromContext(ctx, stage, trades, rowsAscPreload, customRange = null) {
  const { userId, scope, settings, live, um, home } = ctx;
  const st = String(stage || "mtd").trim() || "mtd";
  const mwrMode = String(settings?.algoMode || "twr").toLowerCase() === "mwr";
  const asOf = live.frozenThrough || liveDateKeyShanghai();
  const firstTrade = firstTradeDateFromCtx({ ...ctx, trades: trades ?? ctx.trades }, asOf);
  const sessionAsOf = liveDateKeyShanghai();
  const { start, end } = resolveStageRange(st, sessionAsOf, firstTrade, customRange);
  const rows = rowsAscPreload || (await loadSnapshotRowsAsc(userId, scope, start, end));
  const filtered = rows.filter((r) => r.date >= start && r.date <= end);
  let points;
  if (st === "custom") {
    let rate = 0;
    points = filtered.map((r) => {
      const d = Number(r.dailyRateTwr) || 0;
      rate = chainTwrRate(rate, d);
      return { date: r.date, rate: fmtSignedPercentRatio(rate) };
    });
  } else {
    const rateFromRow = (r) =>
      mwrMode ? stageRateMwrFromSnapshotRow(r, st) : stageRateTwrFromSnapshotRow(r, st);
    points = filtered.map((r) => ({
      date: r.date,
      rate: fmtSignedPercentRatio(rateFromRow(r)),
    }));
  }
  const { fxU, fxH, book } = fxFromCtx(ctx);
  const scopeCtx = { scope, bookCurrency: book, fxUsdCny: fxU, fxHkdCny: fxH };
  if (live.tradingDay) {
    const liveDate = String(live.liveDate || "").slice(0, 10);
    if (liveDate >= start && liveDate <= end) {
      const frozen = frozenMetricsFromHomeAccount(home?.account);
      const { rateTwr, rateMwr } = stageProfitFromFrozenAndLive(
        st,
        frozen,
        live,
        firstTrade,
        rows,
        asOf,
        scopeCtx,
        customRange,
      );
      const rateVal = mwrMode ? rateMwr : rateTwr;
      const row = {
        date: liveDate,
        rate: fmtSignedPercentRatio(rateVal),
      };
      const hit = points.findIndex((p) => p.date === liveDate);
      if (hit >= 0) {
        points[hit] = row;
      } else {
        points.push(row);
        points.sort((a, b) => String(a.date).localeCompare(String(b.date)));
      }
    }
  }
  return { meta: metaEnvelope(userId, scope, settings, live, um), stage: st, points };
}

const ASSET_METRICS = new Set(["total_assets", "market_value", "cash", "cash_ratio", "principal"]);

async function buildSeriesDailyAssetFromContext(ctx, stage, trades, rowsAsc, metric) {
  const m = String(metric || "total_assets").trim();
  if (!ASSET_METRICS.has(m)) {
    throw new Error("invalid metric");
  }
  const { userId, scope, settings, live, um, home } = ctx;
  const book = resolveBookCurrencyForAccountScope(settings, scope);
  const fxU = Number(home.account?.eod_fx_usd_cny) || 0;
  const fxH = Number(home.account?.eod_fx_hkd_cny) || 0;
  const asOf = live.frozenThrough || liveDateKeyShanghai();
  const firstTrade = firstTradeDateFromCtx({ ...ctx, trades: trades ?? ctx.trades }, asOf);
  const st = String(stage || "mtd").trim() || "mtd";
  const { start, end } = resolveStageRange(st, asOf, firstTrade);
  const rows = rowsAsc || (await loadSnapshotRowsAsc(userId, scope, start, end));
  const points = rows
    .filter((r) => r.date >= start && r.date <= end)
    .map((r) => {
      let valueCny = 0;
      if (m === "total_assets") {
        valueCny = r.totalAssets;
      } else if (m === "market_value") {
        valueCny = r.marketValue;
      } else if (m === "cash") {
        valueCny = r.cash;
      } else if (m === "cash_ratio") {
        valueCny = r.cashRatio;
      } else {
        valueCny = r.principal;
      }
      const display =
        m === "cash_ratio"
          ? fmtPercentRatio(valueCny)
          : formatMoneyAssetForScope(valueCny, scope, book, fxU, fxH);
      return { date: r.date, value: valueCny, valueDisplay: display };
    });
  return { meta: metaEnvelope(userId, scope, settings, live, um), stage: st, metric: m, points };
}

async function buildStockRankFromContext(ctx, stage, rankOpts = {}) {
  const { userId, scope, settings, live, um } = ctx;
  const st = String(stage || "mtd").trim() || "mtd";
  const { fxU, fxH, book } = fxFromCtx(ctx);
  const payload = await buildStockRankPayload({
    userId,
    accountScope: scope,
    stage: st,
    live,
    publicLayout: rankOpts.publicLayout === true,
    accountProfitCny: rankOpts.accountProfitCny,
    scopeCtx: { scope, bookCurrency: book, fxUsdCny: fxU, fxHkdCny: fxH },
    customRange: rankOpts.customRange || null,
    preloadedTrades: ctx.trades,
    firstTradeDate: ctx.home?.account?.first_trade_date || ctx.home?.account?.firstTradeDate,
    customRange: rankOpts.customRange || null,
  });
  return { meta: metaEnvelope(userId, scope, settings, live, um), ...payload };
}

function mergeSeriesLivePoint(points, liveRow, valueKey) {
  if (!liveRow?.date || !valueKey) {
    return points;
  }
  const out = [...(points || [])];
  const hit = out.findIndex((p) => p.date === liveRow.date);
  const row = { date: liveRow.date, [valueKey]: liveRow[valueKey] };
  if (hit >= 0) {
    out[hit] = row;
  } else {
    out.push(row);
    out.sort((a, b) => String(a.date).localeCompare(String(b.date)));
  }
  return out;
}

/** 分析页 series：六条曲线同级，点为已格式化字符串 */
async function buildAnalysisSeriesBundle(ctx, stage, trades, rowsAscPreload, customRange = null) {
  const { live, scope } = ctx;
  const { fxU, fxH, book } = fxFromCtx(ctx);
  const st = String(stage || "mtd").trim() || "mtd";
  const asOf = live.frozenThrough || liveDateKeyShanghai();
  const firstTrade = firstTradeDateFromCtx({ ...ctx, trades: trades ?? ctx.trades }, asOf);
  const { start, end } = resolveStageRange(st, asOf, firstTrade, customRange);
  const rows = rowsAscPreload || [];

  const profitRes = await buildSeriesDailyProfitFromContext(ctx, st, trades, rows, customRange);
  const twrRes = await buildSeriesDailyTwrFromContext(ctx, st, trades, rows, customRange);

  const totalAssets = [];
  const marketValue = [];
  const cash = [];
  const cashRatio = [];
  for (const r of rows) {
    if (r.date < start || r.date > end) {
      continue;
    }
    const taCny = Number(r.totalAssets) || 0;
    const mvCny = Number(r.marketValue) || 0;
    const cashCny = Number(r.cash) || 0;
    const ratioRaw = Number(r.cashRatio) || 0;
    totalAssets.push({
      date: r.date,
      totalAssets: formatPlainAssetForScope(taCny, scope, book, fxU, fxH),
    });
    marketValue.push({
      date: r.date,
      marketValue: formatPlainAssetForScope(mvCny, scope, book, fxU, fxH),
    });
    cash.push({
      date: r.date,
      cash: formatPlainAssetForScope(cashCny, scope, book, fxU, fxH),
    });
    cashRatio.push({
      date: r.date,
      cashRatio: fmtPercentRatio(ratioRaw),
    });
  }

  const liveAssets = todayPointForAssets(live, scope, book, fxU, fxH);
  if (liveAssets && liveAssets.date >= start && liveAssets.date <= end) {
    return {
      stageProfit: profitRes.points,
      stageRate: twrRes.points,
      totalAssets: mergeSeriesLivePoint(totalAssets, liveAssets, "totalAssets"),
      marketValue: mergeSeriesLivePoint(marketValue, liveAssets, "marketValue"),
      cash: mergeSeriesLivePoint(cash, liveAssets, "cash"),
      cashRatio: mergeSeriesLivePoint(cashRatio, liveAssets, "cashRatio"),
    };
  }

  return {
    stageProfit: profitRes.points,
    stageRate: twrRes.points,
    totalAssets,
    marketValue,
    cash,
    cashRatio,
  };
}

async function assembleAnalysisBundleFromContext(ctx, stage, benchmarkSymbol, diag, extraDiag = {}, bundleOpts = {}) {
  const { userId, scope } = ctx;
  const st = String(stage || "mtd").trim() || "mtd";
  const sym = String(benchmarkSymbol || "").trim();
  const customRange = customStageRangeFromOpts(bundleOpts);
  const asOf = ctx.live.frozenThrough || liveDateKeyShanghai();
  const firstTrade = firstTradeDateFromCtx(ctx, asOf);
  const { start, end } = resolveStageRange(st, asOf, firstTrade, customRange);
  const { stages, rowsAsc: stageRowsAsc } = await computeReturnStages(ctx, st, customRange);
  const rowsAsc = stageRowsAsc.length
    ? stageRowsAsc.filter((r) => r.date >= start && r.date <= end)
    : await loadSnapshotRowsAsc(userId, scope, start, end);
  const stageRow = stages[st] || { profitCny: 0, rateTwr: 0, rateMwr: 0 };
  const mwrMode = String(ctx.settings?.algoMode || "twr").toLowerCase() === "mwr";
  const rateVal = mwrMode ? stageRow.rateMwr : stageRow.rateTwr;
  const { fxU, fxH, book } = fxFromCtx(ctx);
  const rankOpts = {
    publicLayout: bundleOpts.publicRankLayout === true,
    accountProfitCny: Number(stageRow.profitCny) || 0,
    customRange,
  };

  const [series, stockRank] = await Promise.all([
    buildAnalysisSeriesBundle(ctx, st, ctx.trades, rowsAsc, customRange),
    buildStockRankFromContext(ctx, st, rankOpts),
  ]);

  let benchmark = null;
  if (sym && BENCHMARK_SYMBOLS.has(sym)) {
    benchmark = await getBenchmarkSeries(userId, sym, st);
  }

  const value = {
    meta: buildBundleMeta(ctx, { stage: st, from: st === "custom" ? start : null, to: st === "custom" ? end : null }),
    returns: {
      profit: formatSignedProfitForScope(stageRow.profitCny, scope, book, fxU, fxH),
      rate: fmtSignedPercentRatio(rateVal),
    },
    assets: buildAssetsApi(ctx),
    series,
    stockRank,
    benchmark,
  };
  if (diag) {
    value._diag = {
      phases: diag,
      scopeCleared: !!ctx.scopeCleared,
      snapshotOnly: ctx.snapshotOnly === true,
      ...extraDiag,
    };
  }
  return finalizeMetricsBundlePayload(value);
}

async function getMetricsAnalysisBundle(userId, accountScope, stage, benchmarkSymbol, opts = {}) {
  const scope = String(accountScope || "all").trim() || "all";
  const st = String(stage || "mtd").trim() || "mtd";
  const sym = String(benchmarkSymbol || "").trim();
  const customRange = customStageRangeFromOpts(opts);
  const customKey =
    st === "custom" && customRange ? `|${customRange.from}|${customRange.to}` : "";
  const pubRank = opts.publicRankLayout === true ? "pub" : "priv";
  const cacheKey = `${String(userId || "").trim()}|${scope}|${st}|${sym}|${pubRank}|sr2${customKey}`;
  const now = Date.now();
  if (!opts.diag && ANALYSIS_BUNDLE_CACHE_MS > 0) {
    const hit = analysisBundleCache.get(cacheKey);
    if (hit && now - hit.at < ANALYSIS_BUNDLE_CACHE_MS) {
      return hit.value;
    }
  }
  const diag = opts.diag ? {} : null;
  const bundleOpts = {
    publicRankLayout: opts.publicRankLayout === true,
    customFrom: opts.customFrom,
    customTo: opts.customTo,
  };
  const buildFull = async () => {
    const ctx = await loadMetricsScopeContextForHome(userId, scope, diag);
    return assembleAnalysisBundleFromContext(ctx, st, sym, diag, {}, bundleOpts);
  };
  let value;
  try {
    value = await raceWithTimeout(buildFull(), ANALYSIS_BUNDLE_DEADLINE_MS, null);
    if (!value) {
      throw new Error("ANALYSIS_BUNDLE_DEADLINE");
    }
  } catch (err) {
    if (String(err?.message || err) !== "ANALYSIS_BUNDLE_DEADLINE") {
      throw err;
    }
    const ctx = await loadMetricsScopeContextSnapshotOnly(userId, scope, diag);
    value = await assembleAnalysisBundleFromContext(
      ctx,
      st,
      sym,
      diag,
      {
        degraded: true,
        reason: "ANALYSIS_BUNDLE_DEADLINE",
        deadlineMs: ANALYSIS_BUNDLE_DEADLINE_MS,
      },
      bundleOpts,
    );
  }
  if (!opts.diag && ANALYSIS_BUNDLE_CACHE_MS > 0) {
    analysisBundleCache.set(cacheKey, { at: now, value });
  }
  return value;
}

async function getBenchmarkSeries(userId, symbol, stage) {
  const sym = String(symbol || "").trim();
  if (!BENCHMARK_SYMBOLS.has(sym)) {
    throw new Error("invalid benchmark symbol");
  }
  const scope = "all";
  const settings = await getSettings(userId);
  const live = await getComputeLiveMetrics(userId, scope);
  const um = await getUserMetricsMeta(userId);
  const payload = await buildBenchmarkSeriesPayload({ userId, symbol: sym, stage, live });
  return { meta: metaEnvelope(userId, scope, settings, live, um), ...payload };
}

/** 今日点：供分析图拼接（a/b 终值） */
function todayPointForReturns(live, scope, book, fxUsdCny, fxHkdCny) {
  if (!live.tradingDay) {
    return null;
  }
  const baseMvRaw = Number(live.lastMarketValueCny) || 0;
  const baseMv = isAggregateScope(scope)
    ? baseMvRaw
    : liveProfitScalarToBook(baseMvRaw, scope, book, fxUsdCny, fxHkdCny);
  const profitCny = Number(live.todayProfitCny) || 0;
  const profitBook = liveProfitScalarToBook(profitCny, scope, book, fxUsdCny, fxHkdCny);
  const rateTwr = baseMv > 0 ? profitBook / baseMv : 0;
  return {
    date: live.liveDate,
    profitCny: profitBook,
    profit: formatSignedProfitForScope(profitBook, scope, book, fxUsdCny, fxHkdCny),
    rate: fmtSignedPercentRatio(rateTwr),
  };
}

async function getMetricsStockRecordBundle(userId, accountScope, symbol, opts = {}) {
  const scope = String(accountScope || "all").trim() || "all";
  const sym = normalizeSymbol(symbol);
  if (!sym) {
    throw new Error("missing symbol");
  }
  return buildStockRecordBundlePayload({
    userId,
    accountScope: scope,
    symbol: sym,
    publicLayout: opts.publicLayout === true,
    chartRange: opts.chartRange,
    range: opts.chartRange ?? opts.range,
    pointsLimit: opts.pointsLimit,
    pointsOffset: opts.pointsOffset,
  });
}

function todayPointForAssets(live, scope, book, fxU, fxH) {
  if (!live.tradingDay) {
    return null;
  }
  const ta = liveProfitScalarToBook(live.totalAssetsCny, scope, book, fxU, fxH);
  const mv = liveProfitScalarToBook(live.liveMarketValueCny, scope, book, fxU, fxH);
  const cash = liveProfitScalarToBook(live.cashCny, scope, book, fxU, fxH);
  const ratio = ta > 0 ? cash / ta : Number(live.cashRatio) || 0;
  return {
    date: live.liveDate,
    totalAssets: formatPlainAssetForScope(ta, scope, book, fxU, fxH),
    marketValue: formatPlainAssetForScope(mv, scope, book, fxU, fxH),
    cash: formatPlainAssetForScope(cash, scope, book, fxU, fxH),
    cashRatio: fmtPercentRatio(ratio),
  };
}

const { redactPublicHomeBundle } = require("./metrics/public-home-bundle-redact");
const { redactPublicAnalysisBundle } = require("./metrics/public-analysis-bundle-redact");
const { redactPublicStockRecordBundle } = require("./metrics/public-stock-record-bundle-redact");

async function getMetricsPublicHomeBundle(userId, accountScope, stagesRaw, opts = {}) {
  const full = await getMetricsHomeBundle(userId, accountScope, stagesRaw, opts);
  return redactPublicHomeBundle(full);
}

async function getMetricsPublicAnalysisBundle(userId, accountScope, stage, benchmarkSymbol, opts = {}) {
  const full = await getMetricsAnalysisBundle(userId, accountScope, stage, benchmarkSymbol, {
    ...opts,
    publicRankLayout: true,
  });
  return redactPublicAnalysisBundle(full);
}

async function getMetricsPublicStockRecordBundle(userId, accountScope, symbol, opts = {}) {
  const full = await getMetricsStockRecordBundle(userId, accountScope, symbol, opts);
  return redactPublicStockRecordBundle(full);
}

module.exports = {
  METRICS_RULE_VERSION,
  BENCHMARK_SYMBOLS,
  homeUiStageToApi,
  getMetricsHomeBundle,
  getMetricsPublicHomeBundle,
  getMetricsPublicAnalysisBundle,
  getMetricsPublicStockRecordBundle,
  getMetricsAnalysisBundle,
  getMetricsStockRecordBundle,
  probeMetricsHomeBundleDb,
  getBenchmarkSeries,
  todayPointForReturns,
  todayPointForAssets,
};
