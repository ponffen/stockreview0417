/**
 * 个股排行周期指标：冻结 stage + 今日 live；划段/天数/涨跌来自 symbol_daily_pnl（+ live）。
 */
const { normalizeSymbol } = require("../db");
const { fmtPlainSignedAmount } = require("../account-kpi-surface");
const { liveCnyToBookAmount, isAggregateScope } = require("./account-book-metrics");
const { isFreshStagePeriod, stageUsesFrozenCumulativeFields } = require("./stages");
const {
  stageTradeCountFromRow,
  countTradeRecordsOnDate,
  countTradeRecordsInRange,
  isTradeRecord,
} = require("./stage-trade-counter");

function stageProfitFromFrozenRow(row, stageKey) {
  const st = String(stageKey || "last_30d").trim() || "last_30d";
  if (!row) {
    return 0;
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
  return Number(row.stageLast30dProfit ?? row.stageInceptionProfit ?? 0);
}

function validNumber(...values) {
  for (const v of values) {
    const n = Number(v);
    if (Number.isFinite(n)) {
      return n;
    }
  }
  return 0;
}

function sortTradeAsc(a, b) {
  return String(a.date).localeCompare(String(b.date)) || Number(a.createdAt || 0) - Number(b.createdAt || 0);
}

function signedAmount(trade) {
  return trade.side === "buy" ? Number(trade.amount) || 0 : -(Number(trade.amount) || 0);
}

function addDay(dateKey, delta = 1) {
  const d = new Date(`${String(dateKey).slice(0, 10)}T12:00:00+08:00`);
  d.setDate(d.getDate() + delta);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function inferMarket(symbol) {
  const s = String(symbol || "").toLowerCase();
  if (/^(sh|sz)\d{6}$/.test(s) || /^\d{6}$/.test(s)) {
    return "A股";
  }
  if (/^hk\d{5}$/.test(s)) {
    return "港股";
  }
  return "美股";
}

function profitNativeToAnalysisCny(profitNative, currency, market, fxUsd, fxHkd) {
  const n = Number.isFinite(Number(profitNative)) ? Number(profitNative) : 0;
  const ccy = String(currency || "CNY").toUpperCase();
  if (ccy === "CNY" || market === "A股") {
    return n;
  }
  if (ccy === "USD") {
    return n * (validNumber(fxUsd, 7.2) || 7.2);
  }
  if (ccy === "HKD") {
    return n * (validNumber(fxHkd, 0.92) || 0.92);
  }
  return n;
}

function formatPercentRatio(value) {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  const num = (safe * 100).toFixed(2);
  return `${safe > 0 ? "+" : ""}${num}%`;
}

function lastTradePriceOnOrBefore(symbolTrades, dateKey, { strictBefore = false } = {}) {
  const dk = String(dateKey).slice(0, 10);
  let best = null;
  let bestDate = "";
  for (const t of [...(symbolTrades || [])].sort(sortTradeAsc)) {
    const td = String(t.date).slice(0, 10);
    if (strictBefore ? td >= dk : td > dk) {
      continue;
    }
    const px = validNumber(t.price, 0);
    if (px > 1e-9 && td >= bestDate) {
      bestDate = td;
      best = px;
    }
  }
  return best;
}

/** pnl 日收盘优先；symbol_daily_close 补缺；live/成交价兜底（避免清仓标的期末价变 0）。 */
function buildCloseLookup(pnlRows, livePos, liveDate, tradingDay, snapshotCloses = [], symbolTrades = []) {
  const sorted = [...(pnlRows || [])].sort((a, b) => String(a.date).localeCompare(String(b.date)));
  const byDate = new Map();
  for (const r of snapshotCloses || []) {
    const px = Number(r.close ?? r.dayClosePrice ?? r.day_close_price);
    const d = String(r.date || r.day || "").slice(0, 10);
    if (d && Number.isFinite(px) && px > 0 && !byDate.has(d)) {
      byDate.set(d, px);
    }
  }
  for (const r of sorted) {
    const px = Number(r.dayClosePrice ?? r.day_close_price);
    if (Number.isFinite(px) && px > 0) {
      byDate.set(String(r.date).slice(0, 10), px);
    }
  }
  const liveDateKey = String(liveDate || "").slice(0, 10);
  const fallbackPrev = validNumber(livePos?.prevClose, livePos?.current, 0);
  const fallbackCurrent = validNumber(livePos?.current, livePos?.prevClose, 0);

  function pickFromMap(dk, strictBefore) {
    let best = null;
    let bestDate = "";
    for (const [d, px] of byDate.entries()) {
      if (strictBefore ? d < dk : d <= dk) {
        if (d >= bestDate) {
          bestDate = d;
          best = px;
        }
      }
    }
    return best;
  }

  function closeOnOrBefore(dateKey) {
    const dk = String(dateKey).slice(0, 10);
    const fromMap = pickFromMap(dk, false);
    if (fromMap != null) {
      return fromMap;
    }
    if (tradingDay && liveDateKey && dk >= liveDateKey && (fallbackCurrent > 0 || fallbackPrev > 0)) {
      return fallbackCurrent || fallbackPrev;
    }
    const fromTrade = lastTradePriceOnOrBefore(symbolTrades, dk);
    if (fromTrade != null) {
      return fromTrade;
    }
    if (fallbackCurrent > 0) {
      return fallbackCurrent;
    }
    if (fallbackPrev > 0) {
      return fallbackPrev;
    }
    return 0;
  }

  function closeBefore(dateKey) {
    const dk = String(dateKey).slice(0, 10);
    const fromMap = pickFromMap(dk, true);
    if (fromMap != null) {
      return fromMap;
    }
    const fromTrade = lastTradePriceOnOrBefore(symbolTrades, dk, { strictBefore: true });
    if (fromTrade != null) {
      return fromTrade;
    }
    if (fallbackPrev > 0) {
      return fallbackPrev;
    }
    return 0;
  }

  return { closeOnOrBefore, closeBefore };
}

function countHeldDaysInRange(symbolTrades, startKey, endKey) {
  let qty = 0;
  for (const t of symbolTrades) {
    if (String(t.date).slice(0, 10) < startKey) {
      qty += t.side === "buy" ? Number(t.quantity) : -Number(t.quantity);
    }
  }
  const startDate = new Date(`${startKey}T12:00:00+08:00`);
  const endDate = new Date(`${endKey}T12:00:00+08:00`);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime()) || startDate > endDate) {
    return 0;
  }
  let held = 0;
  const cursor = new Date(startDate);
  while (cursor <= endDate) {
    const dk = `${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, "0")}-${String(cursor.getDate()).padStart(2, "0")}`;
    for (const t of symbolTrades) {
      if (String(t.date).slice(0, 10) === dk) {
        qty += t.side === "buy" ? Number(t.quantity) : -Number(t.quantity);
      }
    }
    if (qty > 1e-6) {
      held += 1;
    }
    cursor.setDate(cursor.getDate() + 1);
  }
  return held;
}

function collectHoldingSegmentsInPeriod(symbolTrades, periodStart, periodEnd) {
  let qty = 0;
  for (const t of symbolTrades) {
    if (String(t.date).slice(0, 10) < periodStart) {
      qty += t.side === "buy" ? Number(t.quantity) : -Number(t.quantity);
    }
  }
  const startDate = new Date(`${periodStart}T12:00:00+08:00`);
  const endDate = new Date(`${periodEnd}T12:00:00+08:00`);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime()) || startDate > endDate) {
    return [];
  }
  const segments = [];
  let runStart = null;
  const cursor = new Date(startDate);
  while (cursor <= endDate) {
    const dk = `${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, "0")}-${String(cursor.getDate()).padStart(2, "0")}`;
    for (const t of symbolTrades) {
      if (String(t.date).slice(0, 10) === dk) {
        qty += t.side === "buy" ? Number(t.quantity) : -Number(t.quantity);
      }
    }
    if (qty > 1e-6) {
      if (runStart === null) {
        runStart = dk;
      }
    } else if (runStart !== null) {
      const endSeg = addDay(dk, -1);
      if (endSeg >= runStart) {
        segments.push({ start: runStart, end: endSeg });
      }
      runStart = null;
    }
    cursor.setDate(cursor.getDate() + 1);
  }
  if (runStart !== null) {
    segments.push({ start: runStart, end: periodEnd });
  }
  return segments;
}

function symbolEodQtyOnOrBefore(symbolTrades, dateKey) {
  let qty = 0;
  for (const t of [...symbolTrades].sort(sortTradeAsc)) {
    if (String(t.date).slice(0, 10) <= dateKey) {
      qty += t.side === "buy" ? Number(t.quantity) : -Number(t.quantity);
    }
  }
  return qty;
}

function resolveEffInterval(symbolTrades, periodStart, periodEnd) {
  const sorted = [...symbolTrades].sort(sortTradeAsc);
  const A = sorted[0].date;
  const B = sorted[sorted.length - 1].date;
  let effStart = A < periodStart ? periodStart : A;
  let effEnd = B < periodEnd ? B : periodEnd;
  if (Math.abs(symbolEodQtyOnOrBefore(symbolTrades, periodEnd)) > 1e-6) {
    effEnd = periodEnd;
  }
  return { effStart, effEnd };
}

function sumPnlInRange(pnlRows, startKey, endKey) {
  let sum = 0;
  for (const r of pnlRows || []) {
    const d = String(r.date).slice(0, 10);
    if (d >= startKey && d <= endKey) {
      sum += Number(r.dayPnlNative) || 0;
    }
  }
  return sum;
}

function countHeldDaysFromPnl(pnlRows, startKey, endKey) {
  let n = 0;
  for (const r of pnlRows || []) {
    const d = String(r.date).slice(0, 10);
    if (d >= startKey && d <= endKey) {
      n += 1;
    }
  }
  return n;
}

function pxChangeFromPnl(pnlRows, startKey, endKey, symbolTrades) {
  const inRange = (pnlRows || [])
    .filter((r) => {
      const d = String(r.date).slice(0, 10);
      return d >= startKey && d <= endKey;
    })
    .sort((a, b) => String(a.date).localeCompare(String(b.date)));
  if (!inRange.length) {
    return 0;
  }
  const endClose = Number(inRange[inRange.length - 1].dayClosePrice) || 0;
  if (!(endClose > 0)) {
    return 0;
  }
  let entryPx = 0;
  for (const trade of [...(symbolTrades || [])].sort(sortTradeAsc)) {
    const dk = String(trade.date).slice(0, 10);
    if (dk < startKey) {
      continue;
    }
    if (dk > endKey) {
      break;
    }
    if (trade.side === "buy" && validNumber(trade.price, 0) > 0) {
      entryPx = Number(trade.price);
      break;
    }
  }
  const startPx =
    entryPx > 1e-9 ? entryPx : Number(inRange[0].dayClosePrice) || 0;
  return startPx > 1e-9 ? endClose / startPx - 1 : 0;
}

const POSITION_EPS = 1e-6;

function symbolPnlForRankScope(pnlBySym, sym, accountScope) {
  const list = pnlBySym.get(sym) || [];
  const scope = String(accountScope || "all").trim() || "all";
  if (scope === "all") {
    return list.filter((r) => String(r.accountId || "") === "all");
  }
  return list.filter((r) => String(r.accountId || "default") === scope);
}

function countInclusiveCalendarDays(startKey, endKey) {
  const startDate = new Date(`${String(startKey).slice(0, 10)}T12:00:00+08:00`);
  const endDate = new Date(`${String(endKey).slice(0, 10)}T12:00:00+08:00`);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime()) || startDate > endDate) {
    return 0;
  }
  return Math.floor((endDate.getTime() - startDate.getTime()) / 86400000) + 1;
}

function appendLivePnlRow(pnlRows, livePos, liveDate, tradingDay, periodEnd) {
  if (!tradingDay || !livePos) {
    return pnlRows || [];
  }
  const ld = String(liveDate || "").slice(0, 10);
  const pe = String(periodEnd || "").slice(0, 10);
  if (!ld || ld !== pe) {
    return pnlRows || [];
  }
  const rows = [...(pnlRows || [])];
  if (rows.some((r) => String(r.date).slice(0, 10) === ld)) {
    return rows;
  }
  const px = validNumber(livePos.current, livePos.prevClose, 0);
  rows.push({
    date: ld,
    eodShares: Number(livePos.quantity) || 0,
    dayClosePrice: px > 0 ? px : null,
    dayPnlNative: 0,
    currency: livePos.currency,
  });
  rows.sort((a, b) => String(a.date).localeCompare(String(b.date)));
  return rows;
}

/** eod_shares 日序列划段：清仓终点=当日；末段仍持仓终点=periodEnd。 */
function resolveHoldingSegmentsFromPnl(pnlRows, carryEod, periodStart, periodEnd) {
  const ps = String(periodStart || "").slice(0, 10);
  const pe = String(periodEnd || "").slice(0, 10);
  if (!ps || !pe || ps > pe) {
    return [];
  }
  const inPeriod = [...(pnlRows || [])]
    .filter((r) => {
      const d = String(r.date).slice(0, 10);
      return d >= ps && d <= pe;
    })
    .sort((a, b) => String(a.date).localeCompare(String(b.date)));

  const segments = [];
  let prevEod = Number(carryEod) || 0;
  let runStart = null;
  if (prevEod > POSITION_EPS) {
    runStart = ps;
  }

  for (const row of inPeriod) {
    const dk = String(row.date).slice(0, 10);
    const eod = Number(row.eodShares) || 0;
    if (prevEod <= POSITION_EPS && eod > POSITION_EPS) {
      runStart = dk;
    } else if (prevEod > POSITION_EPS && eod <= POSITION_EPS) {
      if (runStart !== null) {
        segments.push({ start: runStart, end: dk });
      }
      runStart = null;
    }
    prevEod = eod;
  }

  if (runStart !== null) {
    segments.push({ start: runStart, end: pe });
  }
  return segments;
}

function isRankEligibleFromPnl(pnlRows, segments, livePos, periodStart, periodEnd) {
  if (segments.length > 0) {
    return true;
  }
  if (livePos && Math.abs(Number(livePos.quantity) || 0) > POSITION_EPS) {
    return true;
  }
  const ps = String(periodStart || "").slice(0, 10);
  const pe = String(periodEnd || "").slice(0, 10);
  return (pnlRows || []).some((r) => {
    const d = String(r.date).slice(0, 10);
    return d >= ps && d <= pe;
  });
}

function heldDaysFromSegmentDates(segments) {
  let total = 0;
  for (const seg of segments || []) {
    total += countInclusiveCalendarDays(seg.start, seg.end);
  }
  return total;
}

function pxChangeFromCloseDates(closeLookup, startKey, endKey) {
  const startClose = closeLookup.closeOnOrBefore(startKey);
  const endClose = closeLookup.closeOnOrBefore(endKey);
  if (!(startClose > 0) || !(endClose > 0)) {
    return NaN;
  }
  return endClose / startClose - 1;
}

function pxChangeMainRowFromSegments(segments, closeLookup) {
  if (!segments.length) {
    return NaN;
  }
  const first = segments[0];
  const last = segments[segments.length - 1];
  return pxChangeFromCloseDates(closeLookup, first.start, last.end);
}

function buildCloseLookupFromPnl(pnlRows, livePos, liveDate, tradingDay) {
  return buildCloseLookup(pnlRows, livePos, liveDate, tradingDay, [], []);
}

/** 排行 B：区间指标仅来自 symbol_daily_pnl 日序列。 */
function computePeriodMetricsFromPnl({ pnlRows, symbolTrades, startKey, endKey }) {
  if (!pnlRows?.length) {
    return { profitNative: 0, pxChange: 0, heldDays: 0 };
  }
  return {
    profitNative: sumPnlInRange(pnlRows, startKey, endKey),
    pxChange: pxChangeFromPnl(pnlRows, startKey, endKey, symbolTrades),
    heldDays: countHeldDaysFromPnl(pnlRows, startKey, endKey),
  };
}

function computePositionProfitInDateRange(symbol, symbolTrades, startKey, endKey, closeLookup) {
  if (!symbolTrades.length) {
    return 0;
  }
  let startQuantity = 0;
  let endQuantity = 0;
  let stageFlowNative = 0;
  for (const trade of symbolTrades) {
    const dk = String(trade.date).slice(0, 10);
    const delta = trade.side === "buy" ? Number(trade.quantity) : -Number(trade.quantity);
    if (dk < startKey) {
      startQuantity += delta;
    }
    if (dk <= endKey) {
      endQuantity += delta;
    }
    if (dk >= startKey && dk <= endKey) {
      stageFlowNative += signedAmount(trade);
    }
  }
  const startClose = closeLookup.closeBefore(startKey);
  const endClose = closeLookup.closeOnOrBefore(endKey);
  const startMv = startQuantity * startClose;
  const endMv = endQuantity * endClose;
  return endMv - startMv - stageFlowNative;
}

function isCnyStock(currency, market) {
  const ccy = String(currency || "CNY").toUpperCase();
  return ccy === "CNY" || market === "A股";
}

function symbolFxEod(currency, fxUsdEod, fxHkdEod) {
  const ccy = String(currency || "CNY").toUpperCase();
  if (ccy === "USD") {
    const fx = Number(fxUsdEod) || 0;
    return fx > 0 ? fx : 7.2;
  }
  if (ccy === "HKD") {
    const fx = Number(fxHkdEod) || 0;
    return fx > 0 ? fx : 0.92;
  }
  return 1;
}

/** 与持仓表 holdings-display 一致：冻结本币×EOD 汇率 + todayProfitCny。 */
function nativeFrozenPlusTodayToCny(frozenNative, todayProfitCny, currency, market, fxUsdEod, fxHkdEod) {
  const frozen = Number(frozenNative) || 0;
  const today = Number(todayProfitCny) || 0;
  if (isCnyStock(currency, market)) {
    return frozen + today;
  }
  return frozen * symbolFxEod(currency, fxUsdEod, fxHkdEod) + today;
}

function frozenStageProfitNative(frozenRow, stageKey, stageStart, frozenThrough) {
  const st = String(stageKey || "mtd").trim() || "mtd";
  if (st === "today") {
    return 0;
  }
  if (stageUsesFrozenCumulativeFields(st) && isFreshStagePeriod(stageStart, frozenThrough)) {
    return 0;
  }
  return stageProfitFromFrozenRow(frozenRow, st);
}

function profitCnyToBookScalar(profitCny, scope, bookCurrency, fxUsdEod, fxHkdEod) {
  if (isAggregateScope(scope)) {
    return Number(profitCny) || 0;
  }
  return liveCnyToBookAmount(profitCny, bookCurrency, fxUsdEod, fxHkdEod);
}

function formatRankProfitCnyDisplay(profitCny, scope, bookCurrency, fxUsdEod, fxHkdEod) {
  return fmtPlainSignedAmount(profitCnyToBookScalar(profitCny, scope, bookCurrency, fxUsdEod, fxHkdEod));
}

function tradesInPeriod(symbolTrades, periodStart, periodEnd) {
  return (symbolTrades || []).filter((t) => {
    const d = String(t.date).slice(0, 10);
    return d >= periodStart && d <= periodEnd;
  });
}

/** 逐日划段：清仓终点取卖光日，末段仍持仓时终点取 periodEnd。 */
function resolveHoldingSegments(symbolTrades, periodStart, periodEnd) {
  const ps = String(periodStart || "").slice(0, 10);
  const pe = String(periodEnd || "").slice(0, 10);
  if (!ps || !pe || ps > pe) {
    return [];
  }
  return collectHoldingSegmentsInPeriod(symbolTrades, ps, pe);
}

function isRankEligible(symbolTrades, segments, periodStart, periodEnd) {
  if (segments.length > 0) {
    return true;
  }
  return tradesInPeriod(symbolTrades, periodStart, periodEnd).length > 0;
}

function heldDaysFromSegments(symbolTrades, segments) {
  let total = 0;
  for (const seg of segments || []) {
    total += countHeldDaysInRange(symbolTrades, seg.start, seg.end);
  }
  return total;
}

function sumPnlInSegment(pnlRows, segStart, segEnd, frozenThrough) {
  const ft = String(frozenThrough || "").slice(0, 10);
  let sum = 0;
  for (const r of pnlRows || []) {
    const d = String(r.date).slice(0, 10);
    if (d < segStart || d > segEnd) {
      continue;
    }
    if (ft && d > ft) {
      continue;
    }
    sum += Number(r.dayPnlNative) || 0;
  }
  return sum;
}

function shouldAddTodayLiveForMainRow({ stageKey, live, periodEnd, livePosition }) {
  if (!live?.tradingDay || !livePosition) {
    return false;
  }
  const liveDate = String(live.liveDate || "").slice(0, 10);
  const pe = String(periodEnd || "").slice(0, 10);
  if (stageKey !== "today" && liveDate !== pe) {
    return false;
  }
  return Math.abs(Number(livePosition.quantity) || 0) > 1e-6;
}

function scopeSymbolTrades(trades, scope, symbol) {
  const sym = normalizeSymbol(symbol);
  const sc = String(scope || "all").trim() || "all";
  return (trades || []).filter((t) => {
    if (!isTradeRecord(t)) {
      return false;
    }
    if (normalizeSymbol(t.symbol) !== sym) {
      return false;
    }
    if (sc === "all") {
      return true;
    }
    return String(t.accountId || "default") === sc;
  });
}

function computeMainRowTradeCount({
  stageKey,
  stageStart,
  frozenRow,
  live,
  symbolTrades,
  periodStart,
  periodEnd,
  frozenThrough,
}) {
  const st = String(stageKey || "mtd").trim() || "mtd";
  const ps = String(periodStart || stageStart || "").slice(0, 10);
  const pe = String(periodEnd || "").slice(0, 10);
  const ft = String(frozenThrough || "").slice(0, 10);
  const tradeList = symbolTrades || [];

  if (st === "custom") {
    return countTradeRecordsInRange(tradeList, ps, pe);
  }

  if (st === "today") {
    const dk = live?.tradingDay ? String(live.liveDate || "").slice(0, 10) : ft;
    if (live?.tradingDay && dk) {
      return countTradeRecordsOnDate(tradeList, dk);
    }
    return Number(frozenRow?.dailyTradeCount) || 0;
  }

  const ss = String(stageStart || ps).slice(0, 10);
  let count = stageTradeCountFromRow(frozenRow, st);
  const frozenEnd = ft || pe;
  // 冻结库 stage_*_trade_count 历史缺数时，回退按成交记录统计（至 frozenThrough）。
  if (!count && tradeList.length && frozenEnd) {
    count = countTradeRecordsInRange(tradeList, ss, frozenEnd);
  }

  if (live?.tradingDay) {
    const liveDate = String(live.liveDate || "").slice(0, 10);
    if (liveDate > ft && liveDate >= ss && liveDate <= pe) {
      count += countTradeRecordsOnDate(tradeList, liveDate);
    }
  }
  return count;
}

function computeMainRowProfitCny({
  stageKey,
  stageStart,
  frozenRow,
  live,
  livePosition,
  currency,
  market,
  periodEnd,
  frozenThrough,
  fxUsdEod,
  fxHkdEod,
}) {
  const frozenNative = frozenStageProfitNative(frozenRow, stageKey, stageStart, frozenThrough);
  const todayCny = shouldAddTodayLiveForMainRow({ stageKey, live, periodEnd, livePosition })
    ? Number(livePosition?.todayProfitCny) || 0
    : 0;
  return nativeFrozenPlusTodayToCny(frozenNative, todayCny, currency, market, fxUsdEod, fxHkdEod);
}

function segmentNeedsTodayLive(seg, segments, periodEnd, live, livePosition) {
  if (!live?.tradingDay || !livePosition) {
    return false;
  }
  const liveDate = String(live.liveDate || "").slice(0, 10);
  const pe = String(periodEnd || "").slice(0, 10);
  if (seg.end !== liveDate || liveDate !== pe) {
    return false;
  }
  const last = segments[segments.length - 1];
  if (!last || last.end !== seg.end) {
    return false;
  }
  return Math.abs(Number(livePosition.quantity) || 0) > 1e-6;
}

function segmentProfitCny(
  seg,
  segments,
  pnlRows,
  frozenThrough,
  live,
  livePosition,
  currency,
  market,
  periodEnd,
  fxUsdEod,
  fxHkdEod,
) {
  const frozenPart = sumPnlInSegment(pnlRows, seg.start, seg.end, frozenThrough);
  const todayCny = segmentNeedsTodayLive(seg, segments, periodEnd, live, livePosition)
    ? Number(livePosition?.todayProfitCny) || 0
    : 0;
  return nativeFrozenPlusTodayToCny(frozenPart, todayCny, currency, market, fxUsdEod, fxHkdEod);
}

function pxChangeForInterval(symbolTrades, startKey, endKey, closeLookup) {
  const endClose = closeLookup.closeOnOrBefore(endKey);
  if (!(endClose > 0)) {
    return NaN;
  }
  let entryPx = 0;
  for (const trade of [...(symbolTrades || [])].sort(sortTradeAsc)) {
    const dk = String(trade.date).slice(0, 10);
    if (dk < startKey) {
      continue;
    }
    if (dk > endKey) {
      break;
    }
    if (trade.side === "buy" && validNumber(trade.price, 0) > 0) {
      entryPx = Number(trade.price);
      break;
    }
  }
  const startPx = entryPx > 1e-9 ? entryPx : closeLookup.closeBefore(startKey);
  return startPx > 1e-9 ? endClose / startPx - 1 : NaN;
}

function pxChangeMainRow(symbolTrades, segments, closeLookup) {
  if (!segments.length) {
    return NaN;
  }
  const first = segments[0];
  const last = segments[segments.length - 1];
  return pxChangeForInterval(symbolTrades, first.start, last.end, closeLookup);
}

function computePeriodMetrics({
  symbol,
  symbolTrades,
  startKey,
  endKey,
  closeLookup,
}) {
  if (!symbolTrades.length) {
    return { profitNative: 0, pxChange: 0, heldDays: 0 };
  }
  const profitNative = computePositionProfitInDateRange(symbol, symbolTrades, startKey, endKey, closeLookup);
  const endClose = closeLookup.closeOnOrBefore(endKey);
  let entryPx = 0;
  for (const trade of [...symbolTrades].sort(sortTradeAsc)) {
    const dk = String(trade.date).slice(0, 10);
    if (dk < startKey) {
      continue;
    }
    if (dk > endKey) {
      break;
    }
    if (trade.side === "buy" && validNumber(trade.price, 0) > 0) {
      entryPx = Number(trade.price);
      break;
    }
  }
  const startPxForStockMove = entryPx > 1e-9 ? entryPx : closeLookup.closeBefore(startKey);
  const pxChange = startPxForStockMove > 1e-9 ? endClose / startPxForStockMove - 1 : 0;
  const heldDays = countHeldDaysInRange(symbolTrades, startKey, endKey);
  return { profitNative, pxChange, heldDays };
}

function formatHoldingSegmentsLabel({
  symbolTrades,
  periodStart,
  periodEnd,
  pnlRows,
  closeLookup,
  currency,
  market,
  scope,
  bookCurrency,
  fxUsd,
  fxHkd,
  frozenThrough,
  live,
  livePosition,
  segments: segmentsIn,
}) {
  const usePnlPath = !(symbolTrades || []).length;
  const segments = segmentsIn || (usePnlPath ? [] : resolveHoldingSegments(symbolTrades, periodStart, periodEnd));
  if (!segments.length) {
    return "";
  }
  if (segments.length === 1) {
    const s = segments[0];
    return `${s.start}～${s.end}`;
  }
  return segments
    .map((s) => {
      const heldDays = usePnlPath
        ? countInclusiveCalendarDays(s.start, s.end)
        : countHeldDaysInRange(symbolTrades, s.start, s.end);
      const pxChange = usePnlPath
        ? pxChangeFromCloseDates(closeLookup, s.start, s.end)
        : pxChangeForInterval(symbolTrades, s.start, s.end, closeLookup);
      const profitCny = segmentProfitCny(
        s,
        segments,
        pnlRows,
        frozenThrough,
        live,
        livePosition,
        currency,
        market,
        periodEnd,
        fxUsd,
        fxHkd,
      );
      const pctStr = formatPercentRatio(pxChange);
      const profitStr = formatRankProfitCnyDisplay(profitCny, scope, bookCurrency, fxUsd, fxHkd);
      return `${s.start}～${s.end}（${heldDays}天，${pctStr}，${profitStr}）`;
    })
    .join("\n");
}

function formatHoldingSegmentsLabelPublic({
  symbolTrades,
  periodStart,
  periodEnd,
  closeLookup,
  segments: segmentsIn,
}) {
  const usePnlPath = !(symbolTrades || []).length;
  const segments = segmentsIn || (usePnlPath ? [] : resolveHoldingSegments(symbolTrades, periodStart, periodEnd));
  if (!segments.length) {
    return "";
  }
  if (segments.length === 1) {
    const s = segments[0];
    return `${s.start}～${s.end}`;
  }
  return segments
    .map((s) => {
      const heldDays = usePnlPath
        ? countInclusiveCalendarDays(s.start, s.end)
        : countHeldDaysInRange(symbolTrades, s.start, s.end);
      const pxChange = usePnlPath
        ? pxChangeFromCloseDates(closeLookup, s.start, s.end)
        : pxChangeForInterval(symbolTrades, s.start, s.end, closeLookup);
      const pctStr = formatPercentRatio(pxChange);
      return `${s.start}～${s.end}（${heldDays}天，股价${pctStr}）`;
    })
    .join("\n");
}

function groupPnlRowsBySymbol(pnlRows) {
  const map = new Map();
  for (const r of pnlRows || []) {
    const sym = normalizeSymbol(r.symbol);
    if (!sym) {
      continue;
    }
    if (!map.has(sym)) {
      map.set(sym, []);
    }
    map.get(sym).push(r);
  }
  for (const list of map.values()) {
    list.sort((a, b) => String(a.date).localeCompare(String(b.date)));
  }
  return map;
}

function inferSymbolCurrency(symbolTrades, pnlRows) {
  const fromPnl = (pnlRows || []).find((r) => r.currency);
  if (fromPnl?.currency) {
    return String(fromPnl.currency).toUpperCase();
  }
  return "CNY";
}

module.exports = {
  sortTradeAsc,
  addDay,
  countHeldDaysInRange,
  countHeldDaysFromPnl,
  countInclusiveCalendarDays,
  collectHoldingSegmentsInPeriod,
  symbolEodQtyOnOrBefore,
  resolveEffInterval,
  resolveHoldingSegments,
  resolveHoldingSegmentsFromPnl,
  appendLivePnlRow,
  isRankEligible,
  isRankEligibleFromPnl,
  heldDaysFromSegments,
  heldDaysFromSegmentDates,
  buildCloseLookup,
  buildCloseLookupFromPnl,
  computePeriodMetrics,
  computePeriodMetricsFromPnl,
  computeMainRowProfitCny,
  computeMainRowTradeCount,
  scopeSymbolTrades,
  nativeFrozenPlusTodayToCny,
  profitNativeToAnalysisCny,
  profitCnyToBookScalar,
  pxChangeMainRow,
  pxChangeMainRowFromSegments,
  pxChangeForInterval,
  pxChangeFromCloseDates,
  formatHoldingSegmentsLabel,
  formatHoldingSegmentsLabelPublic,
  groupPnlRowsBySymbol,
  symbolPnlForRankScope,
  inferSymbolCurrency,
  inferMarket,
};
