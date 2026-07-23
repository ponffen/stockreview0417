/**
 * 持仓「今日收益」口径：08:30 北京交易日期门控 + 含当日买卖现金流。
 * 与 app.js computePortfolio 中 todayProfitNative 一致。
 */
const { normalizeSymbol, addCalendarDays } = require("./db");

const POSITION_EPS = 1e-6;

function hasPositionQuantity(qty) {
  return Math.abs(Number(qty) || 0) > POSITION_EPS;
}

function parseQuoteTimeToDateKey(timeStr) {
  if (!timeStr || typeof timeStr !== "string") {
    return null;
  }
  const t = String(timeStr).trim();
  if (!t || t === "--") {
    return null;
  }
  // A 股等：20260527161433
  const compact = /^(\d{4})(\d{2})(\d{2})/.exec(t.replace(/\s/g, ""));
  if (compact) {
    return `${compact[1]}-${compact[2]}-${compact[3]}`;
  }
  // 港美股等：2026-05-27 10:59:40、2026/05/27
  const iso = /^(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})/.exec(t);
  if (iso) {
    return `${iso[1]}-${String(Number(iso[2])).padStart(2, "0")}-${String(Number(iso[3])).padStart(2, "0")}`;
  }
  return null;
}

function getTradingDateKeyBy0830(baseDate = new Date()) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(baseDate);
  const get = (type) => parts.find((p) => p.type === type)?.value;
  const y = Number(get("year"));
  const m = Number(get("month"));
  const d = Number(get("day"));
  const h = Number(get("hour"));
  const min = Number(get("minute"));
  const current = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  if (h < 8 || (h === 8 && min < 30)) {
    return addCalendarDays(current, -1);
  }
  return current;
}

function shouldCountTodayPositionPnlFromQuote(quote, now = new Date()) {
  const tradingKey = getTradingDateKeyBy0830(now);
  const quoteKey =
    (quote && quote.marketDate) ||
    (quote && quote.quoteDate) ||
    (quote && parseQuoteTimeToDateKey(quote.rawTime)) ||
    (quote && parseQuoteTimeToDateKey(quote.time)) ||
    null;
  return !!quoteKey && quoteKey === tradingKey;
}

function tradeSignedAmount(trade) {
  const sign = trade.side === "buy" ? 1 : -1;
  return sign * Math.abs(Number(trade.amount) || 0);
}

/** 自然日 dateKey 上的期初/期末数量与当日买卖现金流（原币） */
function getPositionDayTradeContext(symbol, dateKey, trades) {
  const sym = normalizeSymbol(symbol);
  const dk = String(dateKey || "").slice(0, 10);
  const list = (trades || [])
    .filter((t) => normalizeSymbol(t.symbol) === sym)
    .sort((a, b) => {
      const ad = new Date(a.date).getTime() - new Date(b.date).getTime();
      if (ad !== 0) {
        return ad;
      }
      return Number(a.createdAt || 0) - Number(b.createdAt || 0);
    });
  let startQuantity = 0;
  let endQuantity = 0;
  let dayFlowNative = 0;
  for (const trade of list) {
    const d = String(trade.date || "").slice(0, 10);
    const deltaQty = trade.side === "buy" ? Number(trade.quantity || 0) : -Number(trade.quantity || 0);
    if (d < dk) {
      startQuantity += deltaQty;
    }
    if (d <= dk) {
      endQuantity += deltaQty;
    }
    if (d === dk) {
      dayFlowNative += tradeSignedAmount(trade);
    }
  }
  return { startQuantity, endQuantity, dayFlowNative };
}

function isExtendedQuoteSession(quote) {
  const s = String(quote?.session || "").toLowerCase();
  return s === "pre" || s === "post" || s === "overnight";
}

function resolveTodayStartMvNat({ frozenMvNat, startQuantity, prevClose, quote }) {
  const startQty = Number(startQuantity) || 0;
  const prev = Number(prevClose) || 0;
  const fromQuote = startQty > 0 && prev > 0 ? startQty * prev : 0;
  const frozen = Number(frozenMvNat) || 0;
  const liveCurrent = Number(quote?.current) || 0;
  // 盘前/盘后/夜盘：今日收益基准优先对齐最近日冻结市值，其次昨收×期初数量
  if (isExtendedQuoteSession(quote)) {
    if (frozen > 0) {
      return frozen;
    }
    if (fromQuote > 0) {
      return fromQuote;
    }
    return 0;
  }
  if (fromQuote > 0 && liveCurrent > 0) {
    return fromQuote;
  }
  if (frozen > 0) {
    return frozen;
  }
  return fromQuote;
}

function computeTodayProfitNative({
  quote,
  symbol,
  prevClose,
  current,
  trades,
  todayKey,
  now,
  frozenMvNat,
  endQuantity,
  clearedToday = false,
}) {
  const dayCtx = getPositionDayTradeContext(symbol, todayKey, trades);
  const endQty =
    endQuantity != null && Number.isFinite(Number(endQuantity))
      ? Number(endQuantity)
      : dayCtx.endQuantity;
  const todayStartMvNat = resolveTodayStartMvNat({
    frozenMvNat,
    startQuantity: dayCtx.startQuantity,
    prevClose,
    quote,
  });

  const isClearedToday =
    clearedToday ||
    (hasPositionQuantity(dayCtx.startQuantity) && !hasPositionQuantity(endQty));
  if (isClearedToday && !hasPositionQuantity(endQty)) {
    return 0 - todayStartMvNat - dayCtx.dayFlowNative;
  }

  if (!shouldCountTodayPositionPnlFromQuote(quote, now)) {
    return 0;
  }
  return endQty * current - todayStartMvNat - dayCtx.dayFlowNative;
}

function todayProfitCnyForHolding({
  quote,
  symbol,
  prevClose,
  current,
  rate,
  trades,
  todayKey,
  now,
  frozenMvNat,
  endQuantity,
}) {
  const nat = computeTodayProfitNative({
    quote,
    symbol,
    prevClose,
    current,
    trades,
    todayKey,
    now,
    frozenMvNat,
    endQuantity,
  });
  return nat * (Number(rate) || 1);
}

module.exports = {
  parseQuoteTimeToDateKey,
  getTradingDateKeyBy0830,
  shouldCountTodayPositionPnlFromQuote,
  tradeSignedAmount,
  getPositionDayTradeContext,
  isExtendedQuoteSession,
  resolveTodayStartMvNat,
  computeTodayProfitNative,
  todayProfitCnyForHolding,
};
