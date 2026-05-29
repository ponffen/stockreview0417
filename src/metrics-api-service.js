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
  getPerformancePresetSnapshot,
  getSymbolDailyCloseRange,
  resolveBookCurrencyForAccountScope,
  getUserMetricsMeta,
  getAccountMetricsMetaForUser,
  getLastEodSharesForUser,
  fetchHomeBundleFrozenPack,
  normalizeSymbol,
} = require("./db");
const {
  fmtMoney,
  fmtPlainAmount,
  fmtPlainSignedAmount,
  fmtPercentRatio,
  fmtSignedPercentRatio,
  cnyScalarToBookAmount,
} = require("./account-kpi-surface");
const {
  buildProfitSeries,
  rebaseRateSeriesByFirstDay,
  computeTimeWeightedSeries,
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
} = require("./metrics/stages");
const { shouldEmitTodayLivePoint, liveDateKeyShanghai } = require("./metrics/trading-calendar");
const { buildHoldingsPayload } = require("./metrics/holdings-display");
const {
  chainTwrRate,
  accountDailyTwrReturn,
  todayProfitCnyFromTotals,
} = require("./metrics/snapshot-plus-live");
const { holdingsSymbolsFromTrades } = require("./metrics/holdings-active-symbols");
const { buildStockRankPayload } = require("./metrics/stock-rank");
const { buildBenchmarkSeriesPayload } = require("./metrics/benchmark-series");

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

async function loadSnapshotRowsAsc(userId, scope, from, to) {
  const rows = await getAnalysisDailySnapshots({ accountId: scope, from, to }, userId);
  return rows
    .map((r) => ({
      date: String(r.date || "").slice(0, 10),
      profitCny: Number(r.profitCny ?? r.profit_cny ?? 0),
      totalAssets: Number(r.totalAssets ?? r.total_assets ?? 0) || Number(r.marketValue ?? r.market_value ?? 0),
      marketValue: Number(r.marketValue ?? r.market_value ?? 0),
      cash: Number(r.cash ?? 0),
      cashRatio: Number(r.cashRatio ?? r.cash_ratio ?? 0),
      principal: Number(r.principal ?? 0),
      externalFlowCny: Number(r.externalFlowCny ?? r.external_flow_cny ?? 0),
      twRCumulative: Number(r.twRCumulative ?? r.tw_r_cumulative ?? 0),
    }))
    .filter((r) => r.date)
    .sort((a, b) => a.date.localeCompare(b.date));
}

function stageProfitFromFrozenAndLive(stageKey, frozenMetrics, live, firstTradeDate, rowsAsc, asOf) {
  const liveDate = live.tradingDay
    ? String(live.liveDate || "").slice(0, 10)
    : String(live.frozenThrough || asOf).slice(0, 10);
  const rangeAsOf = liveDate || String(asOf).slice(0, 10);
  const { start } = resolveStageRange(stageKey, rangeAsOf, firstTradeDate);
  const todayP = Number(live.todayProfitCny);
  const todayPFinite = Number.isFinite(todayP) ? todayP : todayProfitCnyFromTotals(live);
  let frozenProfit = 0;
  let rateTwr = 0;
  let rateMwr = 0;
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
    const { end } = resolveStageRange(stageKey, rangeAsOf, firstTradeDate);
    const m = metricsForWindow(rowsAsc, start, end);
    frozenProfit = m.profitCny;
    rateTwr = m.rateTwr;
    rateMwr = m.rateMwr;
  }
  const frozenTa =
    Number(live.eodTotalAssetsCny) || Number(frozenMetrics.eodTotalAssetsCny) || 0;
  const flowToday = Number(live.externalFlowTodayCny) || 0;
  const liveTa = Number(live.totalAssetsCny) || frozenTa;
  const frozenDate = String(live.frozenThrough || asOf).slice(0, 10);

  let profitCny = frozenProfit;
  const todayProfit = todayPFinite;
  const baseMvToday = Number(live.lastMarketValueCny) || frozenTa;
  if (stageKey === "today") {
    profitCny = todayProfit;
    rateTwr =
      live.tradingDay && baseMvToday > 0 && Math.abs(todayProfit) > 1e-9 ? todayProfit / baseMvToday : 0;
    rateMwr = rateTwr;
  } else if (live.tradingDay) {
    profitCny = frozenProfit + todayProfit;
    const rToday =
      Math.abs(todayProfit) > 1e-9 ? accountDailyTwrReturn(frozenTa, liveTa, flowToday) : 0;
    rateTwr = chainTwrRate(rateTwr, rToday);
    const rowsForMwr = appendLiveSnapshotRow(rowsAsc, live, liveDate);
    rateMwr = xirrStageToLive(rowsForMwr, start, liveDate, liveTa);
  } else if (STANDARD_RETURN_STAGES.has(stageKey) && ["mtd", "ytd", "inception"].includes(stageKey)) {
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

function appendLiveSnapshotRow(rowsAsc, live, liveDate) {
  if (!liveDate) {
    return rowsAsc;
  }
  const ld = String(liveDate).slice(0, 10);
  const out = rowsAsc.filter((r) => r.date < ld);
  out.push({
    date: ld,
    totalAssets: Number(live.totalAssetsCny) || 0,
    marketValue: Number(live.liveMarketValueCny) || 0,
    externalFlowCny: Number(live.externalFlowTodayCny) || 0,
    profitCny: Number(live.todayProfitCny) || 0,
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
const HOME_BUNDLE_CACHE_MS = Math.max(
  0,
  Math.min(30_000, Number(process.env.HOME_BUNDLE_CACHE_MS || 12_000)),
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
    if (sym && (Number(row.eodShares ?? row.eod_shares) || 0) > 1e-6) {
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
    if (qty <= 1e-6) {
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
  if (!scopeCleared) {
    lastEodRows = await mark("db.lastEodShares", () => getLastEodSharesForUser(userId));
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
  return { userId, scope, settings, live, um, home, accountMetaList, scopeCleared };
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
  return { userId, scope, settings, live, um, home, accountMetaList, scopeCleared };
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

  const { settings, home, um, accountMetaList, lastEodRows, trades, cashTransfers, accounts } = {
    settings: pack.settings,
    home: pack.home,
    um: pack.um,
    accountMetaList: pack.accountMetaList,
    lastEodRows: pack.lastEodRows,
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

async function buildMetricsReturnsFromContext(ctx, stagesRaw) {
  const { userId, scope, settings, live, um, home } = ctx;
  const asOf = live.frozenThrough || live.liveDate || liveDateKeyShanghai();
  const frozen = frozenMetricsFromHomeAccount(home.account);
  const want = parseStagesParam(stagesRaw);
  const mwrMode = String(settings?.algoMode || "twr").toLowerCase() === "mwr";
  const stagesNeedingRows = stageKeysNeedingSnapshotRows(want, live, mwrMode);
  let firstTrade =
    String(home.account?.first_trade_date || home.account?.firstTradeDate || "").slice(0, 10) || asOf;
  let rowsAsc = [];
  if (stagesNeedingRows.length) {
    const trades = await getTrades(userId);
    if (trades.length > 0) {
      firstTrade = [...trades].sort((a, b) => String(a.date).localeCompare(String(b.date)))[0].date;
    }
    const rangeAsOf = live.tradingDay
      ? String(live.liveDate || "").slice(0, 10)
      : String(live.frozenThrough || asOf).slice(0, 10);
    let minStart = rangeAsOf;
    for (const key of stagesNeedingRows) {
      const { start } = resolveStageRange(key, rangeAsOf, firstTrade);
      if (start < minStart) {
        minStart = start;
      }
    }
    rowsAsc = await loadSnapshotRowsAsc(userId, scope, minStart, asOf);
  }
  const stages = {};
  for (const key of want) {
    const { profitCny, rateTwr, rateMwr } = stageProfitFromFrozenAndLive(
      key,
      frozen,
      live,
      firstTrade,
      rowsAsc,
      asOf,
    );
    const rate = mwrMode ? rateMwr : rateTwr;
    stages[key] = {
      profitCny,
      rateTwr,
      rateMwr,
      rate,
      profitDisplay: fmtPlainSignedAmount(profitCny),
      rateTwrDisplay: fmtSignedPercentRatio(rateTwr),
      rateMwrDisplay: fmtSignedPercentRatio(rateMwr),
      rateDisplay: fmtSignedPercentRatio(rate),
    };
  }
  return {
    meta: metaEnvelope(userId, scope, settings, live, um),
    stages,
  };
}

function buildMetricsAssetsFromContext(ctx) {
  const { userId, scope, settings, live, um, home } = ctx;
  const book = resolveBookCurrencyForAccountScope(settings, scope);
  const acc = home.account;
  const fxU = Number(acc?.eod_fx_usd_cny) || live.fxUsdCny || 0;
  const fxH = Number(acc?.eod_fx_hkd_cny) || live.fxHkdCny || 0;
  let taCny = Number(acc?.eod_total_assets_cny) || 0;
  let mvCny = Number(acc?.eod_market_value_cny) || 0;
  let cashCny = Number(acc?.eod_cash_cny) || 0;
  let principalCny = Number(acc?.eod_principal_cny) || 0;
  let ratio = Number(acc?.eod_cash_ratio) || 0;
  if (live.tradingDay) {
    taCny = Number(live.totalAssetsCny) || taCny;
    mvCny = Number(live.liveMarketValueCny) || mvCny;
    cashCny = Number(live.cashCny) || cashCny;
    principalCny = Number(live.principalCny) || principalCny;
    ratio = taCny > 0 ? (cashCny / taCny) * 100 : ratio;
  }
  const ta = cnyScalarToBookAmount(taCny, book, fxU, fxH);
  const mv = cnyScalarToBookAmount(mvCny, book, fxU, fxH);
  const cash = cnyScalarToBookAmount(cashCny, book, fxU, fxH);
  const principal = cnyScalarToBookAmount(principalCny, book, fxU, fxH);
  const ratioStr =
    Number.isFinite(taCny) && taCny > 0 ? fmtPercentRatio(cashCny / taCny) : fmtPercentRatio(ratio / 100);
  const stockRatioStr =
    Number.isFinite(taCny) && taCny > 0 ? fmtPercentRatio(mvCny / taCny) : fmtPercentRatio(0);
  return {
    meta: metaEnvelope(userId, scope, settings, live, um),
    totalAssetsCny: taCny,
    marketValueCny: mvCny,
    cashCny,
    cashRatio: taCny > 0 ? cashCny / taCny : 0,
    stockRatio: taCny > 0 ? mvCny / taCny : 0,
    principalCny,
    totalAssetsDisplay: fmtPlainAmount(ta),
    marketValueDisplay: fmtPlainAmount(mv),
    cashDisplay: fmtPlainAmount(cash),
    stockRatioDisplay: stockRatioStr,
    cashRatioDisplay: ratioStr,
    principalDisplay: fmtPlainAmount(principal),
  };
}

async function buildMetricsHoldingsFromContext(ctx, overviewStages) {
  const { userId, scope, settings, live, um, home } = ctx;
  const trades = await getTrades(userId);
  const rows = await buildHoldingsPayload({
    userId,
    accountScope: scope,
    settings,
    live,
    symbolRows: home.symbols,
    accountRow: home.account,
    trades,
    overviewStages: overviewStages || {},
  });
  return { meta: metaEnvelope(userId, scope, settings, live, um), rows };
}


async function getMetricsReturns(userId, accountScope, stagesRaw) {
  const ctx = await loadMetricsScopeContext(userId, accountScope);
  return buildMetricsReturnsFromContext(ctx, stagesRaw);
}

async function getMetricsAssets(userId, accountScope) {
  const ctx = await loadMetricsScopeContext(userId, accountScope);
  return buildMetricsAssetsFromContext(ctx);
}

async function assembleHomeBundleFromContext(ctx, stagesRaw, diag, extraDiag = {}) {
  const returns = await buildMetricsReturnsFromContext(ctx, stagesRaw);
  const assets = buildMetricsAssetsFromContext(ctx);
  const holdings = await buildMetricsHoldingsFromContext(ctx, returns.stages);
  const value = { returns, assets, holdings };
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
  return value;
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


async function getSeriesDailyProfit(userId, accountScope, stage) {
  const scope = String(accountScope || "all").trim() || "all";
  const settings = await getSettings(userId);
  const live = await getComputeLiveMetrics(userId, scope);
  const um = await getUserMetricsMeta(userId);
  const asOf = live.frozenThrough || liveDateKeyShanghai();
  const trades = await getTrades(userId);
  const firstTrade =
    trades.length > 0
      ? [...trades].sort((a, b) => String(a.date).localeCompare(String(b.date)))[0].date
      : asOf;
  const { start, end } = resolveStageRange(stage, asOf, firstTrade);
  const rows = await loadSnapshotRowsAsc(userId, scope, start, end);
  const points = rows.map((r) => ({
    date: r.date,
    profitCny: r.profitCny,
    profitDisplay: fmtMoney(r.profitCny, "CNY"),
  }));
  const todayPt = todayPointForReturns(live, settings?.profitAlgoMode);
  if (todayPt && todayPt.date >= start && todayPt.date <= end) {
    const hit = points.findIndex((p) => p.date === todayPt.date);
    const row = {
      date: todayPt.date,
      profitCny: todayPt.profitCny,
      profitDisplay: todayPt.profitDisplay,
    };
    if (hit >= 0) {
      points[hit] = row;
    } else {
      points.push(row);
      points.sort((a, b) => String(a.date).localeCompare(String(b.date)));
    }
  }
  return { meta: metaEnvelope(userId, scope, settings, live, um), stage, points };
}

async function getSeriesDailyTwr(userId, accountScope, stage) {
  const scope = String(accountScope || "all").trim() || "all";
  const settings = await getSettings(userId);
  const live = await getComputeLiveMetrics(userId, scope);
  const um = await getUserMetricsMeta(userId);
  const asOf = live.frozenThrough || liveDateKeyShanghai();
  const snap = await getPerformancePresetSnapshot(userId, scope, stage, asOf);
  if (snap?.twr?.seriesJson) {
    try {
      const parsed = JSON.parse(snap.twr.seriesJson);
      const dates = parsed.dates || [];
      const rates = parsed.twrRebased || [];
      const points = dates.map((date, i) => ({
        date,
        rate: Number(rates[i]) || 0,
        rateDisplay: fmtPercentRatio(Number(rates[i]) || 0),
      }));
      return { meta: metaEnvelope(userId, scope, settings, live, um), stage, points };
    } catch {
      /* fall through */
    }
  }
  const trades = await getTrades(userId);
  const firstTrade =
    trades.length > 0
      ? [...trades].sort((a, b) => String(a.date).localeCompare(String(b.date)))[0].date
      : asOf;
  const { start, end } = resolveStageRange(stage, asOf, firstTrade);
  const rows = await loadSnapshotRowsAsc(userId, scope, start, end);
  const pts = rows.map((r) => ({
    date: r.date,
    value: r.totalAssets,
    flow: r.externalFlowCny,
  }));
  const tw = rebaseRateSeriesByFirstDay(computeTimeWeightedSeries(pts));
  const points = tw.map((p) => ({
    date: p.date,
    rate: p.rate,
    rateDisplay: fmtPercentRatio(p.rate),
  }));
  return { meta: metaEnvelope(userId, scope, settings, live, um), stage, points };
}

const ASSET_METRICS = new Set(["total_assets", "market_value", "cash", "cash_ratio", "principal"]);

async function getSeriesDailyAsset(userId, accountScope, stage, metric) {
  const m = String(metric || "total_assets").trim();
  if (!ASSET_METRICS.has(m)) {
    throw new Error("invalid metric");
  }
  const scope = String(accountScope || "all").trim() || "all";
  const settings = await getSettings(userId);
  const live = await getComputeLiveMetrics(userId, scope);
  const um = await getUserMetricsMeta(userId);
  const book = resolveBookCurrencyForAccountScope(settings, scope);
  const asOf = live.frozenThrough || liveDateKeyShanghai();
  const trades = await getTrades(userId);
  const firstTrade =
    trades.length > 0
      ? [...trades].sort((a, b) => String(a.date).localeCompare(String(b.date)))[0].date
      : asOf;
  const { start, end } = resolveStageRange(stage, asOf, firstTrade);
  const rows = await loadSnapshotRowsAsc(userId, scope, start, end);
  const home = await getHomeSummaryForUser(userId, scope);
  const fxU = Number(home.account?.eod_fx_usd_cny) || 0;
  const fxH = Number(home.account?.eod_fx_hkd_cny) || 0;
  const points = rows.map((r) => {
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
        ? fmtPercentRatio(valueCny / 100)
        : fmtMoney(cnyScalarToBookAmount(valueCny, book, fxU, fxH), book);
    return { date: r.date, value: valueCny, valueDisplay: display };
  });
  return { meta: metaEnvelope(userId, scope, settings, live, um), stage, metric: m, points };
}

async function getHoldings(userId, accountScope) {
  const ctx = await loadMetricsScopeContext(userId, accountScope);
  const returns = await buildMetricsReturnsFromContext(ctx, "today,mtd,ytd,inception");
  return buildMetricsHoldingsFromContext(ctx, returns.stages);
}

async function getStockRank(userId, accountScope, stage) {
  const scope = String(accountScope || "all").trim() || "all";
  const settings = await getSettings(userId);
  const live = await getComputeLiveMetrics(userId, scope);
  const um = await getUserMetricsMeta(userId);
  const payload = await buildStockRankPayload({ userId, accountScope: scope, stage, live });
  return { meta: metaEnvelope(userId, scope, settings, live, um), ...payload };
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
function todayPointForReturns(live, algoMode) {
  if (!live.tradingDay) {
    return null;
  }
  const mwr = String(algoMode || "twr").toLowerCase() === "mwr";
  const baseMv = Number(live.lastMarketValueCny) || 0;
  const rateTwr = baseMv > 0 ? live.todayProfitCny / baseMv : 0;
  return {
    date: live.liveDate,
    profitCny: live.todayProfitCny,
    profitDisplay: fmtMoney(live.todayProfitCny, "CNY"),
    rate: mwr ? rateTwr : rateTwr,
    rateDisplay: fmtSignedPercentRatio(rateTwr),
  };
}

function todayPointForAssets(live, book, fxU, fxH) {
  if (!live.tradingDay) {
    return null;
  }
  return {
    date: live.liveDate,
    totalAssetsDisplay: fmtMoney(cnyScalarToBookAmount(live.totalAssetsCny, book, fxU, fxH), book),
    marketValueDisplay: fmtMoney(cnyScalarToBookAmount(live.liveMarketValueCny, book, fxU, fxH), book),
    cashDisplay: fmtMoney(cnyScalarToBookAmount(live.cashCny, book, fxU, fxH), book),
    cashRatioDisplay: fmtPercentRatio(live.cashRatio),
    principalDisplay: fmtMoney(cnyScalarToBookAmount(live.principalCny, book, fxU, fxH), book),
  };
}

module.exports = {
  METRICS_RULE_VERSION,
  BENCHMARK_SYMBOLS,
  homeUiStageToApi,
  getMetricsReturns,
  getMetricsAssets,
  getMetricsHomeBundle,
  probeMetricsHomeBundleDb,
  getSeriesDailyProfit,
  getSeriesDailyTwr,
  getSeriesDailyAsset,
  getHoldings,
  getStockRank,
  getBenchmarkSeries,
  todayPointForReturns,
  todayPointForAssets,
};
