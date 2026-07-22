/**
 * h 基准曲线：symbol_daily_close + 交易日实时价拼接。
 */
const { getSymbolDailyCloseRange, normalizeSymbol } = require("../db");
const { fmtPercentRatio } = require("../account-kpi-surface");
const { resolveStageRange } = require("./stages");
const { fetchQuoteMap } = require("../quotes/realtime-quote");
const { liveDateKeyShanghai, shouldEmitTodayLivePoint } = require("./trading-calendar");

function parseTencentPriceField(v) {
  const n = Number(String(v || "").replace(/,/g, ""));
  return Number.isFinite(n) ? n : NaN;
}

function parseQuoteFromRaw(sym, raw) {
  if (!raw) {
    return null;
  }
  if (typeof raw === "object" && raw.current > 0) {
    return raw;
  }
  if (typeof raw !== "string") {
    return null;
  }
  const parts = raw.split("~");
  const current = parseTencentPriceField(parts[3]);
  if (!Number.isFinite(current) || current <= 0) {
    return null;
  }
  return { current, prevClose: parseTencentPriceField(parts[4]) || current };
}

async function buildBenchmarkSeriesPayload({ symbol, stage, live }) {
  const sym = normalizeSymbol(symbol);
  const asOf = live.frozenThrough || liveDateKeyShanghai();
  const { start, end } = resolveStageRange(stage, asOf, "2000-01-01");
  const closes = await getSymbolDailyCloseRange(sym, start, end);
  const byDate = new Map(closes.map((r) => [String(r.date).slice(0, 10), Number(r.close)]));
  const dates = [...byDate.keys()].filter((d) => d >= start && d <= end).sort();
  if (!dates.length) {
    return { symbol: sym, stage, points: [], today: null };
  }
  const base = byDate.get(dates[0]) || 1;
  const points = dates.map((date) => {
    const px = byDate.get(date) || base;
    const rate = base > 0 ? (px - base) / base : 0;
    return { date, rate, rateDisplay: fmtPercentRatio(rate) };
  });
  let today = null;
  if (shouldEmitTodayLivePoint() && live.tradingDay) {
    const req = await fetchQuoteMap([sym]);
    const q = req.map?.get(sym);
    const lastClose = byDate.get(dates[dates.length - 1]) || base;
    const px = q?.current > 0 ? q.current : lastClose;
    const rate = base > 0 ? (px - base) / base : 0;
    today = {
      date: live.liveDate,
      rate,
      rateDisplay: fmtPercentRatio(rate),
    };
  }
  return { symbol: sym, stage, points, today };
}

module.exports = { buildBenchmarkSeriesPayload };
