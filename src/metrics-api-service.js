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
} = require("./db");
const { fmtMoney, fmtPercentRatio, cnyScalarToBookAmount } = require("./account-kpi-surface");
const {
  buildProfitSeries,
  rebaseRateSeriesByFirstDay,
  computeTimeWeightedSeries,
  metricsForWindow,
  lastAnalysisDailyRowOnOrBefore,
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
const { buildStockRankPayload } = require("./metrics/stock-rank");
const { buildBenchmarkSeriesPayload } = require("./metrics/benchmark-series");

const METRICS_RULE_VERSION = 3;
const BENCHMARK_SYMBOLS = new Set(["sh000001", "sz399001", "rt_hkHSI", "gb_inx"]);
const STANDARD_RETURN_STAGES = new Set(["today", "mtd", "ytd", "inception"]);

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
  const { start, end } = resolveStageRange(stageKey, asOf, firstTradeDate);
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
    const m = metricsForWindow(rowsAsc, start, end);
    frozenProfit = m.profitCny;
    rateTwr = m.rateTwr;
    rateMwr = m.rateMwr;
  }
  let profitCny = frozenProfit;
  if (stageKey === "today") {
    profitCny = live.tradingDay ? live.todayProfitCny : 0;
    const baseMv = Number(frozenMetrics.lastMarketValueCny) || 0;
    rateTwr = baseMv > 0 && live.tradingDay ? live.todayProfitCny / baseMv : 0;
    rateMwr = rateTwr;
  } else if (live.tradingDay && stageKey !== "today") {
    profitCny = frozenProfit + live.todayProfitCny;
    const winEnd = live.liveDate || asOf;
    const mLive = metricsForWindow(
      appendLiveSnapshotRow(rowsAsc, live, winEnd),
      start,
      winEnd,
    );
    rateTwr = mLive.rateTwr;
    rateMwr = mLive.rateMwr;
  }
  return { profitCny, rateTwr, rateMwr };
}

function appendLiveSnapshotRow(rowsAsc, live, liveDate) {
  if (!live.tradingDay || !liveDate) {
    return rowsAsc;
  }
  const out = rowsAsc.filter((r) => r.date < liveDate);
  out.push({
    date: liveDate,
    totalAssets: live.totalAssetsCny,
    marketValue: live.liveMarketValueCny,
    externalFlowCny: 0,
    profitCny: live.todayProfitCny,
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
  };
}


const homeBundleCache = new Map();
const HOME_BUNDLE_CACHE_MS = Math.max(
  0,
  Math.min(30_000, Number(process.env.HOME_BUNDLE_CACHE_MS || 12_000)),
);

async function loadMetricsScopeContext(userId, accountScope) {
  const scope = String(accountScope || "all").trim() || "all";
  const [settings, home, um, trades, cashTransfers, accounts] = await Promise.all([
    getSettings(userId),
    getHomeSummaryForUser(userId, scope),
    getUserMetricsMeta(userId),
    getTrades(userId),
    getCashTransfers(userId),
    getAccounts(userId),
  ]);
  const live = await getComputeLiveMetrics(userId, scope, {
    preloaded: { trades, cashTransfers, accounts, homeAccount: home.account },
  });
  return { userId, scope, settings, live, um, home };
}

async function buildMetricsReturnsFromContext(ctx, stagesRaw) {
  const { userId, scope, settings, live, um, home } = ctx;
  const asOf = live.frozenThrough || live.liveDate || liveDateKeyShanghai();
  const frozen = frozenMetricsFromHomeAccount(home.account);
  const want = parseStagesParam(stagesRaw);
  const customStages = want.filter((k) => !STANDARD_RETURN_STAGES.has(k));
  let firstTrade =
    String(home.account?.first_trade_date || home.account?.firstTradeDate || "").slice(0, 10) || asOf;
  let rowsAsc = [];
  if (customStages.length) {
    const trades = await getTrades(userId);
    if (trades.length > 0) {
      firstTrade = [...trades].sort((a, b) => String(a.date).localeCompare(String(b.date)))[0].date;
    }
    let minStart = asOf;
    for (const key of customStages) {
      const { start } = resolveStageRange(key, asOf, firstTrade);
      if (start < minStart) {
        minStart = start;
      }
    }
    rowsAsc = await loadSnapshotRowsAsc(userId, scope, minStart, asOf);
  }
  const stages = {};
  const mwrMode = String(settings?.algoMode || "twr").toLowerCase() === "mwr";
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
      profitDisplay: fmtMoney(profitCny, "CNY"),
      rateTwrDisplay: fmtPercentRatio(rateTwr),
      rateMwrDisplay: fmtPercentRatio(rateMwr),
      rateDisplay: fmtPercentRatio(rate),
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
    taCny = live.totalAssetsCny;
    mvCny = live.liveMarketValueCny;
    cashCny = live.cashCny;
    principalCny = live.principalCny;
    ratio = live.cashRatio * 100;
  }
  const ta = cnyScalarToBookAmount(taCny, book, fxU, fxH);
  const mv = cnyScalarToBookAmount(mvCny, book, fxU, fxH);
  const cash = cnyScalarToBookAmount(cashCny, book, fxU, fxH);
  const principal = cnyScalarToBookAmount(principalCny, book, fxU, fxH);
  const ratioStr =
    Number.isFinite(taCny) && taCny > 0 ? fmtPercentRatio(cashCny / taCny) : fmtPercentRatio(ratio / 100);
  return {
    meta: metaEnvelope(userId, scope, settings, live, um),
    totalAssetsCny: taCny,
    marketValueCny: mvCny,
    cashCny,
    cashRatio: taCny > 0 ? cashCny / taCny : 0,
    principalCny,
    totalAssetsDisplay: fmtMoney(ta, book),
    marketValueDisplay: fmtMoney(mv, book),
    cashDisplay: fmtMoney(cash, book),
    cashRatioDisplay: ratioStr,
    principalDisplay: fmtMoney(principal, book),
  };
}

async function buildMetricsHoldingsFromContext(ctx) {
  const { userId, scope, settings, live, um, home } = ctx;
  const rows = await buildHoldingsPayload({
    userId,
    accountScope: scope,
    settings,
    live,
    symbolRows: home.symbols,
    accountRow: home.account,
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

async function getMetricsHomeBundle(userId, accountScope, stagesRaw) {
  const scope = String(accountScope || "all").trim() || "all";
  const stagesKey = String(stagesRaw || "").trim();
  const cacheKey = `${String(userId || "").trim()}|${scope}|${stagesKey}`;
  const now = Date.now();
  if (HOME_BUNDLE_CACHE_MS > 0) {
    const hit = homeBundleCache.get(cacheKey);
    if (hit && now - hit.at < HOME_BUNDLE_CACHE_MS) {
      return hit.value;
    }
  }
  const ctx = await loadMetricsScopeContext(userId, accountScope);
  const returns = await buildMetricsReturnsFromContext(ctx, stagesRaw);
  const assets = buildMetricsAssetsFromContext(ctx);
  const holdings = await buildMetricsHoldingsFromContext(ctx);
  const value = { returns, assets, holdings };
  if (HOME_BUNDLE_CACHE_MS > 0) {
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
  return buildMetricsHoldingsFromContext(ctx);
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
    rateDisplay: fmtPercentRatio(rateTwr),
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
  getSeriesDailyProfit,
  getSeriesDailyTwr,
  getSeriesDailyAsset,
  getHoldings,
  getStockRank,
  getBenchmarkSeries,
  todayPointForReturns,
  todayPointForAssets,
};
