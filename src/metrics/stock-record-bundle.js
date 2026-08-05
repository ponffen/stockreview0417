/**
 * 个股分析页 bundle：headline + charts.points（格式化字符串，按区间全量返回）。
 */
const { resolveDisplayNameFromMap, stockCodeForDisplay } = require("../symbol-name-resolve");
const {
  getTrades,
  getSettings,
  getAccounts,
  getUserMetricsMeta,
  getSymbolDailyCloseRange,
  getSymbolDailyPnlChartSeriesDateRange,
  getSymbolDailyPnlRowOnOrBefore,
  getSymbolMetaMap,
  normalizeSymbol,
  resolveBookCurrencyForAccountScope,
  addCalendarDays,
  upsertSymbolDailyCloseBatch,
} = require("../db");
const {
  fmtPlainAmount,
  fmtPlainSignedAmount,
  fmtPercentRatio,
  fmtSignedPercentRatio,
} = require("../account-kpi-surface");
const { getLiveMetricsWithFrozenPack } = require("./live-metrics-context");
const { getSymbolCurrency, lastPositiveCloseOnOrBefore } = require("../return-calcs");
const { liveDateKeyShanghai } = require("./trading-calendar");
const { resolveStageRange } = require("./stages");
const {
  stockRecordRangeChipToStage,
  stageProfitFromSymbolPnlRow,
  firstTradeDateFromTrades,
} = require("./stage-chart-common");
const { isLastNdStage, lastNdProfit } = require("./last-nd");
const { sortTradeAsc } = require("./stock-rank-period");
const { finalizeMetricsBundlePayload } = require("./bundle-payload");
const { enumerateFreezeSessionDates } = require("./freeze-calendar");
const { fetchRemoteDailyClosesForSymbol } = require("../daily-close-backfill");
const CHART_LEAD_PADDING_CLOSE_SOURCE = "chart-lead-padding";
const {
  resolveChartLeadStart,
  leadSessionDates,
  firstSeriesDate,
  padStockRecordChartPointsLead,
} = require("./chart-stage-lead-padding");
const { hasOpenPositionQuantity } = require("./holdings-active-symbols");
const {
  getPositionDayTradeContext,
  computeTodayProfitNative,
} = require("../position-today-pnl");

const POSITION_EPS = 1e-6;

const VALID_CHART_RANGES = new Set(["7", "30", "90", "mtd", "ytd", "all"]);

function pnlRowOnOrBeforeFromSeries(rows, asOf) {
  const target = String(asOf || "").slice(0, 10);
  if (!target) {
    return null;
  }
  let best = null;
  for (const row of rows || []) {
    const d = String(row.date || "").slice(0, 10);
    if (d && d <= target && (!best || d > String(best.date || "").slice(0, 10))) {
      best = row;
    }
  }
  return best;
}

function earliestDateKey(...dates) {
  const keys = dates.map((d) => String(d || "").slice(0, 10)).filter(Boolean).sort();
  return keys[0] || null;
}

/** 请求外补收盘价：只写库，不阻塞当前 bundle 响应。 */
function scheduleRemoteDailyCloseBackfill(sym, from, to, source = "sina") {
  const fromKey = String(from || "").slice(0, 10);
  const toKey = String(to || "").slice(0, 10);
  if (!sym || !fromKey || !toKey || fromKey > toKey) {
    return;
  }
  void (async () => {
    try {
      const remoteRows = await fetchRemoteDailyClosesForSymbol(sym, fromKey, toKey);
      if (!remoteRows.length) {
        return;
      }
      const toPersist = [];
      for (const row of remoteRows) {
        const d = String(row.date || "").slice(0, 10);
        const c = Number(row.close);
        if (d && c > 0) {
          toPersist.push({ symbol: sym, date: d, close: c, source: row.source || source });
        }
      }
      if (toPersist.length) {
        await upsertSymbolDailyCloseBatch(toPersist);
      }
    } catch {
      /* background only */
    }
  })();
}

function parseChartRangePreset(opts = {}) {
  const raw = String(opts.chartRange ?? opts.range ?? "").trim().toLowerCase();
  if (VALID_CHART_RANGES.has(raw)) {
    return raw;
  }
  return "30";
}

function parseStockRecordChartRequest(opts = {}) {
  return {
    range: parseChartRangePreset(opts),
  };
}

function chartRangeMetaFromPoints(preset, stageKey, points, endDate, fromDate) {
  const list = Array.isArray(points) ? points : [];
  const dates = list.map((p) => String(p.date || "").slice(0, 10)).filter(Boolean).sort();
  return {
    preset,
    stage: stageKey,
    fromDate: fromDate || dates[0] || null,
    returned: list.length,
    oldestDate: dates[0] || null,
    newestDate: dates.length ? dates[dates.length - 1] : null,
  };
}

function formatQuoteTimeDisplay(timeStr) {
  const raw = String(timeStr || "").trim();
  if (!raw || raw === "—" || raw === "--") {
    return "—";
  }
  const compact = raw.replace(/\D/g, "");
  if (compact.length >= 14) {
    return `${compact.slice(4, 6)}-${compact.slice(6, 8)} ${compact.slice(8, 10)}:${compact.slice(10, 12)}:${compact.slice(12, 14)}`;
  }
  const iso = /^(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})(?:\D+(\d{1,2})[:：](\d{1,2})(?:[:：](\d{1,2}))?)?/.exec(raw);
  if (iso) {
    const month = String(Number(iso[2])).padStart(2, "0");
    const day = String(Number(iso[3])).padStart(2, "0");
    const hour = String(Number(iso[4] || 0)).padStart(2, "0");
    const minute = String(Number(iso[5] || 0)).padStart(2, "0");
    const second = String(Number(iso[6] || 0)).padStart(2, "0");
    return `${month}-${day} ${hour}:${minute}:${second}`;
  }
  return raw;
}

function formatTradingIntervalWithSide(rate, side) {
  const normalizedSide = String(side || "").trim().toLowerCase();
  const suffix = normalizedSide === "buy" ? "B" : normalizedSide === "sell" ? "S" : "";
  const safe = Number.isFinite(Number(rate)) ? Number(rate) : 0;
  const num = (safe * 100).toFixed(2);
  const rateText = `${safe > 0 ? "+" : ""}${num}%`;
  return suffix ? `${rateText} ${suffix}` : rateText;
}

/** 现价相对最近一笔成交价涨跌幅（与首页持仓表 regret 一致）。 */
function computeTradingIntervalFormatted(symbolTrades, currentPrice) {
  const price = Number(currentPrice);
  if (!(price > 0) || !Array.isArray(symbolTrades) || !symbolTrades.length) {
    return "—";
  }
  const sorted = [...symbolTrades].sort(sortTradeAsc);
  const refTrade = sorted[sorted.length - 1];
  const refPrice = Number(refTrade?.price);
  if (!refTrade || !(refPrice > 0)) {
    return "—";
  }
  return formatTradingIntervalWithSide((price - refPrice) / refPrice, refTrade.side);
}

function filterTradesForScope(trades, scope, symbol) {
  const sym = normalizeSymbol(symbol);
  const sc = String(scope || "all").trim() || "all";
  return (trades || [])
    .filter((t) => normalizeSymbol(t.symbol) === sym)
    .filter((t) => sc === "all" || String(t.accountId || "default") === sc)
    .sort(sortTradeAsc);
}

/** 图表 tooltip 成交标注：按 chart 区间裁剪，公开页不含 quantity。 */
function buildChartTradesByDate(symbolTrades, rangeFrom, rangeTo, publicLayout) {
  const from = String(rangeFrom || "").slice(0, 10);
  const to = String(rangeTo || "").slice(0, 10);
  const out = {};
  for (const t of symbolTrades || []) {
    const dk = String(t.date || t.trade_date || "").slice(0, 10);
    if (!dk) {
      continue;
    }
    if (from && dk < from) {
      continue;
    }
    if (to && dk > to) {
      continue;
    }
    if (!out[dk]) {
      out[dk] = [];
    }
    const side = String(t.side || "").trim() || "buy";
    const price = Number(t.price) || 0;
    if (publicLayout) {
      out[dk].push({
        side,
        price,
        amountShareRatio:
          t.amountShareRatio != null && Number.isFinite(Number(t.amountShareRatio))
            ? Number(t.amountShareRatio)
            : null,
      });
    } else {
      out[dk].push({
        side,
        price,
        quantity: Number(t.quantity) || 0,
      });
    }
  }
  for (const dk of Object.keys(out)) {
    out[dk].sort((a, b) => String(b.side).localeCompare(String(a.side)));
  }
  return out;
}

function closeLookupFromRows(closeRows) {
  const sorted = (closeRows || [])
    .map((r) => ({ day: String(r.date || "").slice(0, 10), close: Number(r.close) }))
    .filter((r) => r.day && r.close > 0)
    .sort((a, b) => a.day.localeCompare(b.day));
  return {
    sorted,
    closeOn(day) {
      return lastPositiveCloseOnOrBefore(sorted, day);
    },
  };
}

function formatClosePrice(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return v.toFixed(3);
}

function formatShares(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return String(Math.round(v));
}

function parseSymbolLiveQuote(sym, raw) {
  if (!raw) {
    return null;
  }
  if (typeof raw === "object" && Number(raw.current) > 0) {
    return {
      current: Number(raw.current),
      prevClose: Number(raw.prevClose) > 0 ? raw.prevClose : raw.current,
      time: raw.time || null,
      sessionLabel: raw.sessionLabel || null,
    };
  }
  return null;
}

function resolveHeadlineQuote(sym, livePos, live, closeLookup, endDate) {
  const frozenClose = closeLookup.closeOn(endDate) || 0;
  const current =
    Number(livePos?.current) || Number(frozenClose) || 0;
  const prev =
    Number(livePos?.prevClose) ||
    (Number(frozenClose) > 0 ? Number(frozenClose) : current);
  const quoteTime = livePos ? live.quoteTime || null : live.quoteTime || null;
  return {
    current,
    prevClose: prev,
    quoteTime,
    sessionLabel: livePos?.sessionLabel || null,
  };
}

function netQtyFromTrades(symbolTrades) {
  let net = 0;
  for (const tr of symbolTrades || []) {
    net += tr.side === "buy" ? Number(tr.quantity) || 0 : -(Number(tr.quantity) || 0);
  }
  return net;
}

function lastTradeDateFromTrades(symbolTrades) {
  const sorted = [...(symbolTrades || [])].sort(sortTradeAsc);
  return sorted.length ? String(sorted[sorted.length - 1].date || "").slice(0, 10) : "";
}

/**
 * 已清仓且冻结已追上最后成交：才走清仓图表补点逻辑。
 * rebuilding / frozen 未追上 / 仍有持仓 → false，保持原逻辑。
 */
function isSymbolClearedStable({
  symbolTrades,
  livePosition,
  um,
  frozenThrough,
  anchorPnlRow,
}) {
  if (um?.rebuilding) {
    return false;
  }
  const frozen = String(frozenThrough || "").slice(0, 10);
  if (!frozen || !anchorPnlRow) {
    return false;
  }
  const lastTrade = lastTradeDateFromTrades(symbolTrades);
  if (!lastTrade || lastTrade > frozen) {
    return false;
  }
  if (hasOpenPositionQuantity(netQtyFromTrades(symbolTrades))) {
    return false;
  }
  const liveQty = Number(livePosition?.quantity) || 0;
  if (hasOpenPositionQuantity(liveQty)) {
    return false;
  }
  const eodShares = Number(anchorPnlRow.eodShares ?? anchorPnlRow.eod_shares) || 0;
  return !hasOpenPositionQuantity(eodShares);
}

function closeForChartDay(pnl, closeLookup, dk) {
  const fromLookup = closeLookup.closeOn(dk);
  const pnlDay = String(pnl?.date || pnl?.dk || "").slice(0, 10);
  const fromPnl =
    Number(pnl?.eodPrice ?? pnl?.eod_price) ||
    Number(pnl?.dayClosePrice ?? pnl?.day_close_price) ||
    0;
  if (fromLookup > 0 && pnlDay === dk) {
    return fromLookup;
  }
  if (fromPnl > 0) {
    return fromPnl;
  }
  return fromLookup;
}

function chartPointFromPnlRow(pnl, closeLookup, profitOf) {
  const dk = String(pnl.date || pnl.dk || "").slice(0, 10);
  const close = closeForChartDay(pnl, closeLookup, dk);
  if (!(close > 0)) {
    return null;
  }
  const shares = Number(pnl.eodShares ?? pnl.eod_shares) || 0;
  const mvNat =
    pnl.eodMarketValueNative != null && Number.isFinite(Number(pnl.eodMarketValueNative))
      ? Number(pnl.eodMarketValueNative)
      : shares * close;
  const weight =
    pnl.positionWeight != null && Number.isFinite(Number(pnl.positionWeight))
      ? Number(pnl.positionWeight)
      : 0;
  const profitNat = profitOf(pnl);
  return {
    date: dk,
    close,
    shares,
    mvNat,
    weight,
    profitNat: Number.isFinite(profitNat) ? profitNat : 0,
  };
}

/** 清仓段补点：ytd/mtd 跨自然年/月时收益归零；成立以来沿用平仓日锚点。 */
function clearedSegmentProfitForDay(anchorPnlRow, dayKey, stageKey, profitOf) {
  const dk = String(dayKey || "").slice(0, 10);
  const st = String(stageKey || "").trim() || "last_30d";
  if (!anchorPnlRow || !dk) {
    return 0;
  }
  const anchorDate = pnlRowDateKey(anchorPnlRow);
  if (st === "ytd" && dk.slice(0, 4) !== anchorDate.slice(0, 4)) {
    return 0;
  }
  if (st === "mtd" && dk.slice(0, 7) !== anchorDate.slice(0, 7)) {
    return 0;
  }
  const anchorProfit = profitOf(anchorPnlRow);
  return Number.isFinite(anchorProfit) ? anchorProfit : 0;
}

/** 清仓标的：区间内每个交易日一个点；缺冻结行则股数/市值/占比为 0，收益拉最近 EOD 直线。 */
function buildClearedStableChartPoints({
  sessionDates,
  pnlRows,
  closeLookup,
  profitOf,
  anchorPnlRow,
  stageKey,
}) {
  const pnlByDate = new Map();
  for (const row of pnlRows || []) {
    const dk = String(row.date || row.dk || "").slice(0, 10);
    if (dk) {
      pnlByDate.set(dk, row);
    }
  }
  const raw = [];
  for (const dk of sessionDates || []) {
    const pnl = pnlByDate.get(dk);
    if (pnl) {
      const pt = chartPointFromPnlRow(pnl, closeLookup, profitOf);
      if (pt) {
        raw.push(pt);
      }
      continue;
    }
    const close = closeLookup.closeOn(dk);
    if (!(close > 0)) {
      continue;
    }
    const profitNat = clearedSegmentProfitForDay(anchorPnlRow, dk, stageKey, profitOf);
    raw.push({
      date: dk,
      close,
      shares: 0,
      mvNat: 0,
      weight: 0,
      profitNat,
    });
  }
  return raw.map((p) => ({
    date: p.date,
    close: formatClosePrice(p.close),
    shares: formatShares(p.shares),
    marketValueNative: fmtPlainAmount(p.mvNat),
    weight: fmtPercentRatio(p.weight),
    profit: fmtPlainSignedAmount(p.profitNat),
  }));
}

function pnlEodShares(row) {
  return Number(row?.eodShares ?? row?.eod_shares) || 0;
}

function pnlRowDateKey(row) {
  return String(row?.date || row?.dk || "").slice(0, 10);
}

/**
 * 自 symbol_daily_pnl 识别「平仓后、再建仓前」的清仓段（冻结已覆盖平仓日）。
 */
function detectClearedSegments(pnlRowsAsc, frozenThrough, um) {
  if (um?.rebuilding) {
    return [];
  }
  const frozen = String(frozenThrough || "").slice(0, 10);
  if (!frozen) {
    return [];
  }
  const sorted = [...(pnlRowsAsc || [])]
    .map((row) => ({ ...row, dk: pnlRowDateKey(row) }))
    .filter((row) => row.dk)
    .sort((a, b) => a.dk.localeCompare(b.dk));

  const segments = [];
  for (let i = 1; i < sorted.length; i += 1) {
    const prevSh = pnlEodShares(sorted[i - 1]);
    const curSh = pnlEodShares(sorted[i]);
    if (!hasOpenPositionQuantity(prevSh) || hasOpenPositionQuantity(curSh)) {
      continue;
    }
    const exitDate = sorted[i].dk;
    if (exitDate > frozen) {
      continue;
    }
    const anchorPnlRow = sorted[i];
    let j = i + 1;
    while (j < sorted.length && !hasOpenPositionQuantity(pnlEodShares(sorted[j]))) {
      j += 1;
    }
    const reentryDate = j < sorted.length ? sorted[j].dk : null;
    const fillFrom = addCalendarDays(exitDate, 1);
    const fillTo = reentryDate ? addCalendarDays(reentryDate, -1) : frozen;
    if (fillFrom && fillTo && fillFrom <= fillTo) {
      segments.push({ from: fillFrom, to: fillTo, exitDate, reentryDate, anchorPnlRow });
    }
    if (j < sorted.length) {
      i = j - 1;
    }
  }
  return segments;
}

function clearedSegmentsInChartRange(segments, chartFrom, endDate) {
  const from = String(chartFrom || "").slice(0, 10);
  const end = String(endDate || "").slice(0, 10);
  if (!from || !end) {
    return [];
  }
  return (segments || []).filter((seg) => seg.from <= end && seg.to >= from);
}

function mapRawChartPoints(raw) {
  return (raw || []).map((p) => ({
    date: p.date,
    close: formatClosePrice(p.close),
    shares: formatShares(p.shares),
    marketValueNative: fmtPlainAmount(p.mvNat),
    weight: fmtPercentRatio(p.weight),
    profit: fmtPlainSignedAmount(p.profitNat),
  }));
}

function todayProfitNativeFromLive(livePosition, ccy, live) {
  if (livePosition?.todayProfitNative != null && Number.isFinite(Number(livePosition.todayProfitNative))) {
    return Number(livePosition.todayProfitNative);
  }
  const todayProfitBook = Number(livePosition?.todayProfitCny) || 0;
  if (ccy === "CNY") {
    return todayProfitBook;
  }
  const fx =
    ccy === "USD"
      ? Number(live?.fxUsdCny) || 0
      : ccy === "HKD"
        ? Number(live?.fxHkdCny) || 0
        : 1;
  return fx > 0 ? todayProfitBook / fx : 0;
}

function frozenMvNatFromPnlRow(row) {
  if (!row) {
    return 0;
  }
  const mv = Number(row.eodMarketValueNative ?? row.eod_market_value_native);
  if (Number.isFinite(mv) && mv > 0) {
    return mv;
  }
  const sh = Number(row.eodShares ?? row.eod_shares) || 0;
  const px = Number(row.eodPrice ?? row.eod_price) || 0;
  return sh > 0 && px > 0 ? sh * px : 0;
}

/** 交易日最后一个点：按实时状态必画（含当日盘中清仓、不在 live.positions 内）。 */
async function applyLiveChartPoint(raw, {
  sym,
  symbolTrades,
  live,
  livePosition,
  ccy,
  endDate,
  includeLive,
  frozenStageProfit,
  frozenMvNat,
  closeLookup,
}) {
  const list = Array.isArray(raw) ? raw : [];
  const end = String(endDate || "").slice(0, 10);
  if (!includeLive || !live?.tradingDay || !end) {
    return list;
  }
  const liveDate = String(live.liveDate || "").slice(0, 10);
  if (!liveDate || liveDate > end) {
    return list;
  }

  const dayCtx = getPositionDayTradeContext(sym, liveDate, symbolTrades);
  const liveQty =
    livePosition != null && Number.isFinite(Number(livePosition.quantity))
      ? Number(livePosition.quantity)
      : dayCtx.endQuantity;

  let current = Number(livePosition?.current) || 0;
  let prevClose = Number(livePosition?.prevClose) || 0;
  let quote = null;
  if (!(current > 0) && closeLookup) {
    const fallbackClose = closeLookup.closeOn(liveDate);
    if (fallbackClose > 0) {
      current = fallbackClose;
      prevClose = fallbackClose;
    }
  }
  if (current > 0 && livePosition) {
    quote = {
      current,
      prevClose,
      marketDate: liveDate,
      quoteDate: liveDate,
      sessionLabel: livePosition.sessionLabel || null,
    };
  } else if (current > 0) {
    quote = { marketDate: liveDate, quoteDate: liveDate };
  }
  if (!(current > 0) && closeLookup) {
    const fallbackClose = closeLookup.closeOn(liveDate);
    if (fallbackClose > 0) {
      current = fallbackClose;
      prevClose = fallbackClose;
    }
  }

  const mvNat = liveQty * (current > 0 ? current : 0);
  const totalAssetsCny = Number(live.totalAssetsCny) || 0;
  const rate =
    ccy === "USD"
      ? Number(live.fxUsdCny) || 0
      : ccy === "HKD"
        ? Number(live.fxHkdCny) || 0
        : 1;
  const mvCny =
    Number(livePosition?.marketValueCny) ||
    (ccy === "CNY" ? mvNat : rate > 0 ? mvNat * rate : 0);
  const weight = totalAssetsCny > 0 ? mvCny / totalAssetsCny : 0;
  const frozenProfit = Number.isFinite(Number(frozenStageProfit)) ? Number(frozenStageProfit) : 0;
  let todayNat = 0;
  if (
    livePosition &&
    (livePosition.todayProfitNative != null || livePosition.todayProfitCny != null)
  ) {
    todayNat = todayProfitNativeFromLive(livePosition, ccy, live);
  } else {
    todayNat = computeTodayProfitNative({
      quote: quote || { marketDate: liveDate, quoteDate: liveDate },
      symbol: sym,
      prevClose: prevClose > 0 ? prevClose : current,
      current: current > 0 ? current : prevClose,
      trades: symbolTrades,
      todayKey: liveDate,
      frozenMvNat: Number(frozenMvNat) || 0,
      endQuantity: liveQty,
    });
  }
  const profitNat = frozenProfit + todayNat;
  const lastClose = list.length ? Number(list[list.length - 1].close) : 0;
  const row = {
    date: liveDate,
    close: current > 0 ? current : lastClose > 0 ? lastClose : 0,
    shares: liveQty,
    mvNat,
    weight,
    profitNat,
  };
  const hit = list.findIndex((p) => p.date === liveDate);
  if (hit >= 0) {
    const next = list.slice();
    next[hit] = row;
    return next;
  }
  return [...list, row].sort((a, b) => a.date.localeCompare(b.date));
}

/** 在已有点基础上，为历史清仓段补工作日点（股数/市值/占比 0，收益拉平仓日 EOD 直线）。 */
function appendClearedSegmentChartPoints(raw, {
  clearedSegments,
  chartFrom,
  endDate,
  closeLookup,
  profitOf,
  stageKey,
}) {
  const out = Array.isArray(raw) ? [...raw] : [];
  const dates = new Set(out.map((p) => p.date));
  const chartStart = String(chartFrom || "").slice(0, 10);
  const chartEnd = String(endDate || "").slice(0, 10);

  for (const seg of clearedSegments || []) {
    const segFrom = seg.from > chartStart ? seg.from : chartStart;
    const segTo = seg.to < chartEnd ? seg.to : chartEnd;
    if (!segFrom || !segTo || segFrom > segTo) {
      continue;
    }
    const sessionDates = enumerateFreezeSessionDates(segFrom, segTo);
    for (const dk of sessionDates) {
      if (dates.has(dk)) {
        continue;
      }
      const close = closeLookup.closeOn(dk);
      if (!(close > 0)) {
        continue;
      }
      const profitNat = clearedSegmentProfitForDay(seg.anchorPnlRow, dk, stageKey, profitOf);
      out.push({
        date: dk,
        close,
        shares: 0,
        mvNat: 0,
        weight: 0,
        profitNat,
      });
      dates.add(dk);
    }
  }
  out.sort((a, b) => a.date.localeCompare(b.date));
  return out;
}

async function buildChartPointsForPage({
  sym,
  symbolTrades,
  pnlRows,
  closeLookup,
  live,
  livePosition,
  ccy,
  endDate,
  includeLive,
  profitOf,
  frozenStageProfit,
  frozenMvNat,
  clearedSegments,
  chartFrom,
  stageKey,
}) {
  if (!pnlRows.length && !includeLive && !(clearedSegments || []).length) {
    return [];
  }
  const end = String(endDate || "").slice(0, 10);
  if (!end) {
    return [];
  }

  const sortedPnl = [...pnlRows]
    .map((row) => ({ ...row, dk: String(row.date || "").slice(0, 10) }))
    .filter((row) => row.dk && row.dk <= end)
    .sort((a, b) => a.dk.localeCompare(b.dk));

  let raw = [];
  for (const pnl of sortedPnl) {
    const pt = chartPointFromPnlRow(pnl, closeLookup, profitOf);
    if (pt) {
      raw.push(pt);
    }
  }

  raw = appendClearedSegmentChartPoints(raw, {
    clearedSegments,
    chartFrom,
    endDate: end,
    closeLookup,
    profitOf,
    stageKey,
  });

  raw = await applyLiveChartPoint(raw, {
    sym,
    symbolTrades,
    live,
    livePosition,
    ccy,
    endDate: end,
    includeLive,
    frozenStageProfit,
    frozenMvNat,
    closeLookup,
  });

  return mapRawChartPoints(raw);
}

function pageCloseDateRange(pnlRowsAsc, endDate, bufferDays = 7) {
  const dates = (pnlRowsAsc || [])
    .map((row) => String(row.date || "").slice(0, 10))
    .filter(Boolean)
    .sort();
  if (!dates.length) {
    const end = String(endDate || "").slice(0, 10);
    return end ? { from: addCalendarDays(end, -bufferDays), to: end } : null;
  }
  return {
    from: addCalendarDays(dates[0], -bufferDays),
    to: dates[dates.length - 1],
  };
}

async function buildStockRecordBundlePayload({
  userId,
  accountScope,
  symbol,
  publicLayout = false,
  live: liveIn,
  ...chartOpts
}) {
  const uid = String(userId || "").trim();
  const sym = normalizeSymbol(symbol);
  const scope = String(accountScope || "all").trim() || "all";
  const { range } = parseStockRecordChartRequest(chartOpts);
  if (!uid || !sym) {
    throw new Error("missing user or symbol");
  }

  const [settings, trades, accounts, um, live] = await Promise.all([
    getSettings(uid),
    getTrades(uid),
    getAccounts(uid),
    getUserMetricsMeta(uid, { light: true }),
    liveIn ? Promise.resolve(liveIn) : getLiveMetricsWithFrozenPack(uid, scope),
  ]);
  const book = resolveBookCurrencyForAccountScope({ ...settings, accounts }, scope);
  const ccy = getSymbolCurrency(sym);
  const symbolTrades = filterTradesForScope(trades, scope, sym);
  if (!symbolTrades.length) {
    const frozenThrough = String(live.frozenThrough || um?.frozenThrough || "").slice(0, 10);
    const endDate = live.tradingDay
      ? String(live.liveDate || liveDateKeyShanghai()).slice(0, 10)
      : frozenThrough;
    const { range } = parseStockRecordChartRequest(chartOpts);
    const rangePreset = range;
    const stageKey = stockRecordRangeChipToStage(rangePreset);
    const [headlineCloseRows, metaMap] = await Promise.all([
      getSymbolDailyCloseRange(
        sym,
        addCalendarDays(endDate || frozenThrough || "1970-01-01", -90),
        endDate || "9999-12-31",
      ),
      getSymbolMetaMap([sym]),
    ]);
    const headlineCloseLookup = closeLookupFromRows(headlineCloseRows);
    const livePos = (live.positions || []).find((p) => normalizeSymbol(p.symbol) === sym) || null;
    const headlineQuote = resolveHeadlineQuote(sym, livePos, live, headlineCloseLookup, endDate);
    const current = headlineQuote.current;
    const prev = headlineQuote.prevClose;
    const changeAbs = current - prev;
    const changePct = prev > 0 ? changeAbs / prev : 0;
    const displayName = resolveDisplayNameFromMap(sym, metaMap);
    const tradingInterval = computeTradingIntervalFormatted(symbolTrades, current);
    const payload = {
      meta: {
        accountId: scope,
        symbol: sym,
        bookCurrency: book,
        currency: ccy,
        frozenThrough: frozenThrough || null,
        liveDate: live.tradingDay ? live.liveDate : null,
        tradingDay: !!live.tradingDay,
        dataVersion: Number(um?.dataVersion) || 0,
        rebuilding: !!um?.rebuilding,
        quoteTime: headlineQuote.quoteTime ?? null,
        stage: stageKey,
        positionStatus: "none",
        clearedSegments: [],
        noTrades: true,
      },
      headline: {
        name: displayName,
        code: stockCodeForDisplay(sym),
        price: formatClosePrice(current),
        change: fmtPlainSignedAmount(changeAbs),
        changePct: fmtSignedPercentRatio(changePct),
        quoteTime: formatQuoteTimeDisplay(headlineQuote.quoteTime),
        sessionLabel: headlineQuote.sessionLabel || null,
        tradingInterval,
      },
      charts: {
        points: [],
        tradesByDate: {},
        noTrades: true,
        range: chartRangeMetaFromPoints(rangePreset, stageKey, [], endDate, null),
        defaults: {
          showClose: true,
          showShares: true,
          showMarketValue: false,
        },
      },
    };
    return finalizeMetricsBundlePayload(payload);
  }

  const frozenThrough = String(live.frozenThrough || um?.frozenThrough || "").slice(0, 10);
  const endDate = live.tradingDay
    ? String(live.liveDate || liveDateKeyShanghai()).slice(0, 10)
    : frozenThrough;
  const accountIdForPnl = scope === "all" ? "all" : scope;
  const pnlQueryBase = { accountId: accountIdForPnl, symbol: sym };

  const rangePreset = range;
  const stageKey = stockRecordRangeChipToStage(rangePreset);
  const firstTrade = firstTradeDateFromTrades(symbolTrades, endDate);
  const sessionAsOf = endDate || liveDateKeyShanghai();
  const { start: from } = resolveStageRange(stageKey, sessionAsOf, firstTrade);
  const chartFrom = String(from || firstTrade || endDate || "").slice(0, 10);
  const chartLeadStart = resolveChartLeadStart(stageKey, sessionAsOf, firstTrade, null);
  const lastNd = isLastNdStage(stageKey);
  const pnlSeriesFrom = earliestDateKey(
    from || endDate,
    firstTrade,
    lastNd ? addCalendarDays(chartFrom, -1) : null,
  );

  const pnlRowsForSegments = await getSymbolDailyPnlChartSeriesDateRange(
    { ...pnlQueryBase, from: pnlSeriesFrom || firstTrade || "1970-01-01", to: endDate || "9999-12-31" },
    uid,
  );
  const pnlRows = pnlRowsForSegments.filter(
    (r) => String(r.date || "").slice(0, 10) >= chartFrom,
  );
  const clearedSegmentsAll = detectClearedSegments(pnlRowsForSegments, frozenThrough, um);
  const clearedSegments = clearedSegmentsInChartRange(clearedSegmentsAll, chartFrom, endDate);

  const livePos = (live.positions || []).find((p) => normalizeSymbol(p.symbol) === sym) || null;
  const includeLive = true;
  const anchorAsOf = frozenThrough || endDate;

  const [headlineCloseRows, metaMap] = await Promise.all([
    getSymbolDailyCloseRange(sym, addCalendarDays(endDate || frozenThrough || "1970-01-01", -90), endDate || "9999-12-31"),
    getSymbolMetaMap([sym]),
  ]);

  const anchorPnlRow = pnlRowOnOrBeforeFromSeries(pnlRowsForSegments, anchorAsOf);
  const frozenProfitRow =
    includeLive && live.tradingDay
      ? pnlRowOnOrBeforeFromSeries(pnlRowsForSegments, frozenThrough || endDate)
      : null;

  const clearedStable = isSymbolClearedStable({
    symbolTrades,
    livePosition: livePos,
    um,
    frozenThrough,
    anchorPnlRow,
  });

  let pageCloseFrom = clearedStable
    ? addCalendarDays(chartFrom, -7)
    : pageCloseDateRange(pnlRows, endDate)?.from;
  if (chartLeadStart) {
    const leadCandidate = addCalendarDays(chartLeadStart, -7);
    if (!pageCloseFrom || leadCandidate < pageCloseFrom) {
      pageCloseFrom = leadCandidate;
    }
  }
  if (clearedSegments.length) {
    const segMin = clearedSegments.reduce((min, seg) => (seg.from < min ? seg.from : min), clearedSegments[0].from);
    const segCandidate = addCalendarDays(segMin, -7);
    if (!pageCloseFrom || segCandidate < pageCloseFrom) {
      pageCloseFrom = segCandidate;
    }
  }
  const pageCloseTo = endDate || frozenThrough || chartFrom;
  let pageCloseRows =
    pageCloseFrom && pageCloseTo
      ? await getSymbolDailyCloseRange(sym, pageCloseFrom, pageCloseTo)
      : [];

  if (pageCloseFrom && pageCloseTo) {
    scheduleRemoteDailyCloseBackfill(sym, pageCloseFrom, pageCloseTo);
  }

  const pageCloseLookup = closeLookupFromRows(pageCloseRows);
  const headlineCloseLookup = closeLookupFromRows(headlineCloseRows);

  // 近 N 日（自然日窗口）：持仓收益走势统一由个股「成立以来累计」相对锚点 rebase 得出，
  // 锚点 = 窗口前一交易日（chartFrom−1）的累计收益；不足 N 天 → 0。
  let symbolAnchorInception = 0;
  if (lastNd) {
    const lastNdAsOf = addCalendarDays(chartFrom, -1);
    let anchorRow = pnlRowOnOrBeforeFromSeries(pnlRowsForSegments, lastNdAsOf);
    if (!anchorRow) {
      anchorRow = await getSymbolDailyPnlRowOnOrBefore(
        { accountId: accountIdForPnl, symbol: sym, asOf: lastNdAsOf },
        uid,
      );
    }
    symbolAnchorInception = Number(anchorRow?.stageInceptionProfit) || 0;
  }
  const profitOf = (row) =>
    lastNd
      ? lastNdProfit(Number(row?.stageInceptionProfit) || 0, symbolAnchorInception)
      : stageProfitFromSymbolPnlRow(row, stageKey);

  const frozenStageProfit = frozenProfitRow ? profitOf(frozenProfitRow) : 0;

  let points;
  const visualChartFrom =
    chartLeadStart && chartLeadStart < chartFrom ? chartLeadStart : chartFrom;
  if (clearedStable) {
    const sessionDates = enumerateFreezeSessionDates(visualChartFrom, endDate);
    points = buildClearedStableChartPoints({
      sessionDates,
      pnlRows,
      closeLookup: pageCloseLookup,
      profitOf,
      anchorPnlRow,
      stageKey,
    });
  } else {
    points = await buildChartPointsForPage({
      sym,
      symbolTrades,
      pnlRows,
      closeLookup: pageCloseLookup,
      live,
      livePosition: livePos,
      ccy,
      endDate,
      includeLive,
      profitOf,
      frozenStageProfit,
      frozenMvNat: frozenMvNatFromPnlRow(frozenProfitRow),
      clearedSegments,
      chartFrom,
      stageKey,
    });
  }

  if (chartLeadStart && stageKey !== "today") {
    const firstDataDate = firstSeriesDate(points);
    const leadTo = firstDataDate ? addCalendarDays(firstDataDate, -1) : chartLeadStart;
    const closeMap = new Map();
    if (leadTo && chartLeadStart <= leadTo) {
      const localRows = await getSymbolDailyCloseRange(sym, chartLeadStart, leadTo);
      for (const row of localRows) {
        const d = String(row.date || "").slice(0, 10);
        const c = Number(row.close);
        if (d && c > 0) {
          closeMap.set(d, c);
        }
      }
      const leadDates = leadSessionDates(chartLeadStart, firstDataDate);
      const needsRemote = leadDates.some((d) => !closeMap.has(d));
      if (needsRemote && leadTo) {
        scheduleRemoteDailyCloseBackfill(sym, chartLeadStart, leadTo, CHART_LEAD_PADDING_CLOSE_SOURCE);
      }
    }
    points = padStockRecordChartPointsLead(points, chartLeadStart, closeMap);
  }

  const headlineQuote = resolveHeadlineQuote(sym, livePos, live, headlineCloseLookup, endDate);
  const current = headlineQuote.current;
  const prev = headlineQuote.prevClose;
  const changeAbs = current - prev;
  const changePct = prev > 0 ? changeAbs / prev : 0;
  const displayName = resolveDisplayNameFromMap(sym, metaMap);
  const tradingInterval = computeTradingIntervalFormatted(symbolTrades, current);
  const tradesByDate = buildChartTradesByDate(
    symbolTrades,
    visualChartFrom,
    endDate,
    publicLayout === true,
  );

  const payload = {
    meta: {
      accountId: scope,
      symbol: sym,
      bookCurrency: book,
      currency: ccy,
      frozenThrough: frozenThrough || null,
      liveDate: live.tradingDay ? live.liveDate : null,
      tradingDay: !!live.tradingDay,
      dataVersion: Number(um?.dataVersion) || 0,
      rebuilding: !!um?.rebuilding,
      quoteTime: headlineQuote.quoteTime ?? null,
      stage: stageKey,
      positionStatus: clearedStable ? "cleared" : "open",
      clearedSegments: clearedSegments.map((seg) => ({
        from: seg.from,
        to: seg.to,
        exitDate: seg.exitDate,
        reentryDate: seg.reentryDate || null,
      })),
    },
    headline: {
      name: displayName,
      code: stockCodeForDisplay(sym),
      price: formatClosePrice(current),
      change: fmtPlainSignedAmount(changeAbs),
      changePct: fmtSignedPercentRatio(changePct),
      quoteTime: formatQuoteTimeDisplay(headlineQuote.quoteTime),
      tradingInterval,
    },
    charts: {
      points,
      tradesByDate,
      range: chartRangeMetaFromPoints(rangePreset, stageKey, points, endDate, from),
      defaults: {
        showClose: true,
        showShares: true,
        showMarketValue: false,
      },
    },
  };

  return finalizeMetricsBundlePayload(payload);
}

module.exports = {
  buildStockRecordBundlePayload,
  buildChartTradesByDate,
  parseStockRecordChartRequest,
  stockRecordRangeChipToStage,
};
