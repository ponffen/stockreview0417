/**
 * 首页汇总（截止到 frozen_through 日终）与 app.js 分析口径一致的纯函数。
 */

function monthStartKeyShanghai(asOf) {
  const d = new Date(`${String(asOf).slice(0, 10)}T12:00:00+08:00`);
  if (Number.isNaN(d.getTime())) {
    return String(asOf || "").slice(0, 10);
  }
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  return `${y}-${m}-01`;
}

function yearStartKeyShanghai(asOf) {
  const d = new Date(`${String(asOf).slice(0, 10)}T12:00:00+08:00`);
  if (Number.isNaN(d.getTime())) {
    return `${String(asOf || "").slice(0, 4)}-01-01`;
  }
  return `${d.getFullYear()}-01-01`;
}

function toDateKeyLocal(d) {
  const dt = d instanceof Date ? d : new Date(d);
  if (Number.isNaN(dt.getTime())) {
    return "1970-01-01";
  }
  const y = dt.getFullYear();
  const m = String(dt.getMonth() + 1).padStart(2, "0");
  const day = String(dt.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function normalizeProfitAlgoMode(mode) {
  const m = String(mode || "twr").toLowerCase();
  if (m === "money" || m === "mwr") return "mwr";
  return "twr";
}

const {
  mwrFromCashflows,
  buildAccountMwrCashflows,
  buildAccountMwrCashflowsStageToLive,
  buildAccountMwrCashflowsToday,
  buildSymbolMwrCashflows,
  accountMwrFromSnapshotWindow,
  accountMwrStageToLive,
  accountMwrTodayOnly,
  symbolMwrFromValueFlowPoints,
} = require("./mwr");

const xirrPeriodFromSnapshotWindow = accountMwrFromSnapshotWindow;
const xirrPeriodStageToLive = accountMwrStageToLive;
const xirrPeriodFromSymbolValueFlowPoints = symbolMwrFromValueFlowPoints;
const xirrPeriodReturn = mwrFromCashflows;
const xirrTodayOnly = accountMwrTodayOnly;
const xirrStageToLive = accountMwrStageToLive;
const xirrFromSnapshotWindow = accountMwrFromSnapshotWindow;
const xirrFromSymbolValueFlowPoints = symbolMwrFromValueFlowPoints;

function computeMoneyWeightedSeries(points) {
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

function computeModeSeries(historyPoints, mode) {
  if (!historyPoints.length) {
    return [{ date: toDateKeyLocal(new Date()), rate: 0 }];
  }
  const m = normalizeProfitAlgoMode(mode);
  if (m === "twr") {
    return computeTimeWeightedSeries(historyPoints);
  }
  return computeMoneyWeightedSeries(historyPoints);
}

function rebaseRateSeriesByFirstDay(series) {
  if (!Array.isArray(series) || !series.length) {
    return [{ date: toDateKeyLocal(new Date()), rate: 0 }];
  }
  const baseRate = Number(series[0]?.rate) || 0;
  const denom = 1 + baseRate;
  return series.map((item, index) => {
    const raw = Number(item?.rate) || 0;
    if (!Number.isFinite(denom) || Math.abs(denom) < 1e-9) {
      return { date: item.date, rate: index === 0 ? 0 : raw - baseRate };
    }
    return {
      date: item.date,
      rate: index === 0 ? 0 : (1 + raw) / denom - 1,
    };
  });
}

function rebaseValueSeriesByFirstDay(series, valueKey = "value") {
  if (!Array.isArray(series) || !series.length) {
    return [{ date: toDateKeyLocal(new Date()), [valueKey]: 0 }];
  }
  const first = Number(series[0]?.[valueKey]) || 0;
  return series.map((item) => ({
    ...item,
    [valueKey]: (Number(item?.[valueKey]) || 0) - first,
  }));
}

function buildProfitSeries(points) {
  if (!points.length) {
    return [{ date: toDateKeyLocal(new Date()), value: 0 }];
  }
  const startClose = points[0].value - points[0].flow;
  let sumFlow = 0;
  const raw = points.map((point) => {
    sumFlow += point.flow;
    return {
      date: point.date,
      value: point.value - startClose - sumFlow,
    };
  });
  return rebaseValueSeriesByFirstDay(raw, "value");
}

function metricsForWindow(allRowsThroughF, windowStart, windowEnd) {
  const rowsAsc = allRowsThroughF
    .map((r) => ({
      date: String(r.date).slice(0, 10),
      totalAssets:
        Number(r.totalAssets ?? r.total_assets ?? 0) ||
        Number(r.marketValue ?? r.market_value ?? 0) ||
        0,
      externalFlowCny: Number(r.externalFlowCny ?? r.external_flow_cny ?? 0) || 0,
    }))
    .filter((r) => r.date && r.date <= windowEnd)
    .sort((a, b) => a.date.localeCompare(b.date));
  const w = rowsAsc.filter((r) => r.date >= windowStart && r.date <= windowEnd);
  if (!w.length) {
    return { profitCny: 0, rateTwr: 0, rateMwr: 0 };
  }
  const pts = w.map((r) => ({
    date: r.date,
    value: r.totalAssets,
    flow: r.externalFlowCny,
  }));
  const profitSeries = buildProfitSeries(pts);
  const rateTwr = rebaseRateSeriesByFirstDay(computeModeSeries(pts, "twr")).at(-1)?.rate ?? 0;
  const rateMwr = accountMwrFromSnapshotWindow(rowsAsc, windowStart, windowEnd);
  return {
    profitCny: profitSeries.at(-1)?.value ?? 0,
    rateTwr,
    rateMwr,
  };
}

/** 取冻结日及之前最近一条 analysis 日快照（rows 可为 getAnalysisDailySnapshots 的 camelCase 行，已按 date 升序更佳）。 */
function lastAnalysisDailyRowOnOrBefore(rows, frozenThrough) {
  const F = String(frozenThrough || "").slice(0, 10);
  let best = null;
  let bestD = "";
  for (const r of rows || []) {
    const d = String(r.date || "").slice(0, 10);
    if (!d || d > F) {
      continue;
    }
    if (!best || d >= bestD) {
      best = r;
      bestD = d;
    }
  }
  return best;
}

/**
 * @param {Array<{date:string,marketValue:number,externalFlowCny?:number,external_flow_cny?:number}>} rowsAllAll
 * @param {string} frozenThrough YYYY-MM-DD
 * @param {string} firstTradeDate 总收益起点：首笔交易日
 */
function computeAccountHomeSummaryFromSnapshots(rowsAllAll, frozenThrough, firstTradeDate, asOfToday) {
  const F = String(frozenThrough).slice(0, 10);
  // Use the caller-supplied today date so MTD/YTD boundaries reflect the current
  // month/year rather than the frozen date (important for cleared accounts).
  const today = asOfToday ? String(asOfToday).slice(0, 10) : F;
  const ms = monthStartKeyShanghai(today);
  const ys = yearStartKeyShanghai(today);
  const ft = String(firstTradeDate || F).slice(0, 10);
  const rows = rowsAllAll
    .map((r) => ({
      date: String(r.date || "").slice(0, 10),
      marketValue: Number(r.marketValue ?? r.market_value ?? 0),
      totalAssets:
        Number(r.totalAssets ?? r.total_assets ?? 0) ||
        Number(r.marketValue ?? r.market_value ?? 0) ||
        0,
      externalFlowCny: Number(r.externalFlowCny ?? r.external_flow_cny ?? 0) || 0,
    }))
    .filter((r) => r.date && r.date <= F)
    .sort((a, b) => a.date.localeCompare(b.date));
  const lastMv = rows.length ? Number(rows[rows.length - 1].totalAssets) || 0 : 0;
  const month = metricsForWindow(rows, ms, F);
  const ytd = metricsForWindow(rows, ys, F);
  const total = metricsForWindow(rows, ft, F);
  return {
    frozenThrough: F,
    firstTradeDate: ft,
    lastMarketValueCny: lastMv,
    monthProfitCny: month.profitCny,
    monthRateTwr: month.rateTwr,
    monthRateMwr: month.rateMwr,
    ytdProfitCny: ytd.profitCny,
    ytdRateTwr: ytd.rateTwr,
    ytdRateMwr: ytd.rateMwr,
    totalProfitCny: total.profitCny,
    totalRateTwr: total.rateTwr,
    totalRateMwr: total.rateMwr,
  };
}

function symbolRatesFromPnlPoints(ptsSorted) {
  if (!ptsSorted || ptsSorted.length < 2) {
    return { rateTwr: 0, rateMwr: 0 };
  }
  const last = ptsSorted[ptsSorted.length - 1];
  const endVal = Number(last.value) || 0;
  const endDate = String(last.date).slice(0, 10);
  return {
    rateTwr: rebaseRateSeriesByFirstDay(computeModeSeries(ptsSorted, "twr")).at(-1)?.rate ?? 0,
    rateMwr: symbolMwrFromValueFlowPoints(ptsSorted.slice(0, -1), endDate, endVal),
  };
}

module.exports = {
  monthStartKeyShanghai,
  yearStartKeyShanghai,
  lastAnalysisDailyRowOnOrBefore,
  computeAccountHomeSummaryFromSnapshots,
  symbolRatesFromPnlPoints,
  buildProfitSeries,
  rebaseRateSeriesByFirstDay,
  computeTimeWeightedSeries,
  metricsForWindow,
  mwrFromCashflows,
  buildAccountMwrCashflows,
  buildAccountMwrCashflowsStageToLive,
  buildAccountMwrCashflowsToday,
  buildSymbolMwrCashflows,
  accountMwrFromSnapshotWindow,
  accountMwrStageToLive,
  accountMwrTodayOnly,
  symbolMwrFromValueFlowPoints,
  xirrPeriodFromSnapshotWindow,
  xirrPeriodStageToLive,
  xirrPeriodFromSymbolValueFlowPoints,
  xirrPeriodReturn,
  xirrTodayOnly,
  xirrStageToLive,
  xirrFromSnapshotWindow,
  xirrFromSymbolValueFlowPoints,
  computeModeSeries,
};
