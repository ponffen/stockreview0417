/**
 * 持仓「今日收益」口径：08:30 北京交易日期门控 + 含当日买卖现金流。
 * 与 app.js computePortfolio 中 todayProfitNative 一致。
 */
const { normalizeSymbol, addCalendarDays } = require("./db");

function parseQuoteTimeToDateKey(timeStr) {
  if (!timeStr || typeof timeStr !== "string") {
    return null;
  }
  const compact = /^(\d{4})(\d{2})(\d{2})/.exec(String(timeStr).trim().replace(/\s/g, ""));
  return compact ? `${compact[1]}-${compact[2]}-${compact[3]}` : null;
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

function computeTodayProfitNative({ quote, symbol, prevClose, current, trades, todayKey, now }) {
  if (!shouldCountTodayPositionPnlFromQuote(quote, now)) {
    return 0;
  }
  const dayCtx = getPositionDayTradeContext(symbol, todayKey, trades);
  const todayStartMvNat = dayCtx.startQuantity * prevClose;
  return dayCtx.endQuantity * current - todayStartMvNat - dayCtx.dayFlowNative;
}

function todayProfitCnyForHolding({ quote, symbol, prevClose, current, rate, trades, todayKey, now }) {
  const nat = computeTodayProfitNative({
    quote,
    symbol,
    prevClose,
    current,
    trades,
    todayKey,
    now,
  });
  return nat * (Number(rate) || 1);
}

module.exports = {
  parseQuoteTimeToDateKey,
  getTradingDateKeyBy0830,
  shouldCountTodayPositionPnlFromQuote,
  tradeSignedAmount,
  getPositionDayTradeContext,
  computeTodayProfitNative,
  todayProfitCnyForHolding,
};
