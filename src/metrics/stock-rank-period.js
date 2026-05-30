/**
 * 个股排行周期指标：与前端 renderAnalysisStockRank / computePositionPeriodMetrics 同口径。
 */
const { normalizeSymbol } = require("../db");

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
  if (symbolEodQtyOnOrBefore(symbolTrades, periodEnd) > 1e-6) {
    effEnd = periodEnd;
  }
  return { effStart, effEnd };
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
  symbol,
  symbolTrades,
  periodStart,
  periodEnd,
  closeLookup,
  currency,
  market,
  fxUsd,
  fxHkd,
}) {
  const segments = collectHoldingSegmentsInPeriod(symbolTrades, periodStart, periodEnd);
  if (!segments.length) {
    return "";
  }
  if (segments.length === 1) {
    const s = segments[0];
    return `${s.start}～${s.end}`;
  }
  return segments
    .map((s) => {
      const m = computePeriodMetrics({
        symbol,
        symbolTrades,
        startKey: s.start,
        endKey: s.end,
        closeLookup,
      });
      const profitCny = profitNativeToAnalysisCny(m.profitNative, currency, market, fxUsd, fxHkd);
      const pctStr = formatPercentRatio(m.pxChange);
      const profitAbs = Math.abs(profitCny).toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
      const profitStr = `${profitCny >= 0 ? "+" : "-"}¥${profitAbs}`;
      return `${s.start}～${s.end}（${m.heldDays}天，${pctStr}，${profitStr}）`;
    })
    .join("\n");
}

function formatHoldingSegmentsLabelPublic({
  symbol,
  symbolTrades,
  periodStart,
  periodEnd,
  closeLookup,
}) {
  const segments = collectHoldingSegmentsInPeriod(symbolTrades, periodStart, periodEnd);
  if (!segments.length) {
    return "";
  }
  if (segments.length === 1) {
    const s = segments[0];
    return `${s.start}～${s.end}`;
  }
  return segments
    .map((s) => {
      const m = computePeriodMetrics({
        symbol,
        symbolTrades,
        startKey: s.start,
        endKey: s.end,
        closeLookup,
      });
      const pctStr = formatPercentRatio(m.pxChange);
      return `${s.start}～${s.end}（${m.heldDays}天，股价${pctStr}）`;
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
  collectHoldingSegmentsInPeriod,
  symbolEodQtyOnOrBefore,
  resolveEffInterval,
  buildCloseLookup,
  computePeriodMetrics,
  profitNativeToAnalysisCny,
  formatHoldingSegmentsLabel,
  formatHoldingSegmentsLabelPublic,
  groupPnlRowsBySymbol,
  inferSymbolCurrency,
  inferMarket,
};
