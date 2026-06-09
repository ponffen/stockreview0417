/**
 * 个股排行周期指标：冻结 stage + 今日 live；划段与天数来自成交。
 */
const { normalizeSymbol } = require("../db");
const { fmtPlainSignedAmount } = require("../account-kpi-surface");
const { liveCnyToBookAmount, isAggregateScope } = require("./account-book-metrics");
const { isFreshStagePeriod, stageUsesFrozenCumulativeFields } = require("./stages");

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

function aggregatePnlRowsByDate(rows) {
  const byDate = new Map();
  for (const r of rows || []) {
    const d = String(r.date).slice(0, 10);
    if (!d) {
      continue;
    }
    const cur = byDate.get(d) || {
      date: d,
      symbol: r.symbol,
      accountId: "all",
      eodShares: 0,
      dayPnlNative: 0,
      dayClosePrice: null,
      currency: r.currency,
    };
    cur.eodShares += Number(r.eodShares) || 0;
    cur.dayPnlNative += Number(r.dayPnlNative) || 0;
    const px = Number(r.dayClosePrice);
    if (Number.isFinite(px) && px > 0) {
      cur.dayClosePrice = px;
    }
    if (r.currency) {
      cur.currency = r.currency;
    }
    byDate.set(d, cur);
  }
  return [...byDate.values()].sort((a, b) => String(a.date).localeCompare(String(b.date)));
}

function symbolPnlForRankScope(pnlBySym, sym, accountScope) {
  const list = pnlBySym.get(sym) || [];
  const scope = String(accountScope || "all").trim() || "all";
  if (scope === "all") {
    const onlyAll = list.filter((r) => String(r.accountId || "") === "all");
    if (onlyAll.length) {
      return onlyAll;
    }
    return aggregatePnlRowsByDate(list);
  }
  return list.filter((r) => String(r.accountId || "default") === scope);
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

function todayProfitNativeFromLive(livePosition, ccy, live) {
  const todayProfitCny = Number(livePosition?.todayProfitCny) || 0;
  const currency = String(ccy || "CNY").toUpperCase();
  if (currency === "CNY") {
    return todayProfitCny;
  }
  const fx =
    currency === "USD"
      ? Number(live?.fxUsdCny) || 0
      : currency === "HKD"
        ? Number(live?.fxHkdCny) || 0
        : 1;
  return fx > 0 ? todayProfitCny / fx : 0;
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

function profitNativeToBookScalar(profitNative, currency, market, scope, bookCurrency, fxUsd, fxHkd) {
  const profitCny = profitNativeToAnalysisCny(profitNative, currency, market, fxUsd, fxHkd);
  if (isAggregateScope(scope)) {
    return profitCny;
  }
  return liveCnyToBookAmount(profitCny, bookCurrency, fxUsd, fxHkd);
}

function formatRankProfitDisplay(profitNative, currency, market, scope, bookCurrency, fxUsd, fxHkd) {
  const book = profitNativeToBookScalar(profitNative, currency, market, scope, bookCurrency, fxUsd, fxHkd);
  return fmtPlainSignedAmount(book);
}

function tradesInPeriod(symbolTrades, periodStart, periodEnd) {
  return (symbolTrades || []).filter((t) => {
    const d = String(t.date).slice(0, 10);
    return d >= periodStart && d <= periodEnd;
  });
}

function hasRebuyAfterClearInPeriod(symbolTrades, periodStart, periodEnd) {
  let qty = 0;
  for (const t of [...(symbolTrades || [])].sort(sortTradeAsc)) {
    const dk = String(t.date).slice(0, 10);
    if (dk < periodStart) {
      qty += t.side === "buy" ? Number(t.quantity) : -Number(t.quantity);
    }
  }
  let clearedOnce = false;
  for (const t of [...(symbolTrades || [])].sort(sortTradeAsc)) {
    const dk = String(t.date).slice(0, 10);
    if (dk < periodStart || dk > periodEnd) {
      continue;
    }
    const before = qty;
    qty += t.side === "buy" ? Number(t.quantity) : -Number(t.quantity);
    if (before > 1e-6 && qty <= 1e-6) {
      clearedOnce = true;
    }
    if (clearedOnce && before <= 1e-6 && qty > 1e-6) {
      return true;
    }
  }
  return false;
}

function firstBuyDateInPeriod(symbolTrades, periodStart, periodEnd) {
  for (const t of [...(symbolTrades || [])].sort(sortTradeAsc)) {
    const dk = String(t.date).slice(0, 10);
    if (dk < periodStart || dk > periodEnd) {
      continue;
    }
    if (t.side === "buy" && validNumber(t.quantity, 0) > 0) {
      return dk;
    }
  }
  return null;
}

/** 单段快捷或逐日划段；多段卖光再买必须完整划段。 */
function resolveHoldingSegments(symbolTrades, periodStart, periodEnd) {
  const ps = String(periodStart || "").slice(0, 10);
  const pe = String(periodEnd || "").slice(0, 10);
  if (!ps || !pe || ps > pe) {
    return [];
  }
  if (!hasRebuyAfterClearInPeriod(symbolTrades, ps, pe)) {
    let qty = 0;
    for (const t of [...(symbolTrades || [])].sort(sortTradeAsc)) {
      const dk = String(t.date).slice(0, 10);
      if (dk < ps) {
        qty += t.side === "buy" ? Number(t.quantity) : -Number(t.quantity);
      }
    }
    if (qty > 1e-6) {
      return [{ start: ps, end: pe }];
    }
    const firstBuy = firstBuyDateInPeriod(symbolTrades, ps, pe);
    if (firstBuy) {
      return [{ start: firstBuy, end: pe }];
    }
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

function computeMainRowProfitNative({
  stageKey,
  stageStart,
  frozenRow,
  live,
  livePosition,
  currency,
  periodEnd,
  frozenThrough,
}) {
  const frozenProfit = frozenStageProfitNative(frozenRow, stageKey, stageStart, frozenThrough);
  let todayPart = 0;
  if (shouldAddTodayLiveForMainRow({ stageKey, live, periodEnd, livePosition })) {
    todayPart = todayProfitNativeFromLive(livePosition, currency, live);
  }
  return frozenProfit + todayPart;
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

function segmentProfitNative(seg, segments, pnlRows, frozenThrough, live, livePosition, currency, periodEnd) {
  let profit = sumPnlInSegment(pnlRows, seg.start, seg.end, frozenThrough);
  if (segmentNeedsTodayLive(seg, segments, periodEnd, live, livePosition)) {
    profit += todayProfitNativeFromLive(livePosition, currency, live);
  }
  return profit;
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
  const segments = segmentsIn || resolveHoldingSegments(symbolTrades, periodStart, periodEnd);
  if (!segments.length) {
    return "";
  }
  if (segments.length === 1) {
    const s = segments[0];
    return `${s.start}～${s.end}`;
  }
  return segments
    .map((s) => {
      const heldDays = countHeldDaysInRange(symbolTrades, s.start, s.end);
      const pxChange = pxChangeForInterval(symbolTrades, s.start, s.end, closeLookup);
      const profitNative = segmentProfitNative(
        s,
        segments,
        pnlRows,
        frozenThrough,
        live,
        livePosition,
        currency,
        periodEnd,
      );
      const pctStr = formatPercentRatio(pxChange);
      const profitStr = formatRankProfitDisplay(
        profitNative,
        currency,
        market,
        scope,
        bookCurrency,
        fxUsd,
        fxHkd,
      );
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
  const segments = segmentsIn || resolveHoldingSegments(symbolTrades, periodStart, periodEnd);
  if (!segments.length) {
    return "";
  }
  if (segments.length === 1) {
    const s = segments[0];
    return `${s.start}～${s.end}`;
  }
  return segments
    .map((s) => {
      const heldDays = countHeldDaysInRange(symbolTrades, s.start, s.end);
      const pxChange = pxChangeForInterval(symbolTrades, s.start, s.end, closeLookup);
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
  collectHoldingSegmentsInPeriod,
  symbolEodQtyOnOrBefore,
  resolveEffInterval,
  resolveHoldingSegments,
  isRankEligible,
  heldDaysFromSegments,
  buildCloseLookup,
  computePeriodMetrics,
  computePeriodMetricsFromPnl,
  computeMainRowProfitNative,
  profitNativeToAnalysisCny,
  profitNativeToBookScalar,
  pxChangeMainRow,
  pxChangeForInterval,
  formatHoldingSegmentsLabel,
  formatHoldingSegmentsLabelPublic,
  groupPnlRowsBySymbol,
  symbolPnlForRankScope,
  inferSymbolCurrency,
  inferMarket,
};
