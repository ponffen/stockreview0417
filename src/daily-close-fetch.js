/**
 * 日 K 拉取：新浪 DailyK_Batch（主）+ 腾讯 qt 实时价（定时增量兜底）。
 * source: sina_dailyk | tencent_qt
 */
const { fetchSinaDailyKBatchFromUpstream, toSinaDailyKBatchSymbol } = require("./sina-kline-upstream");
const { fetchTencentQuoteMap } = require("./quotes/tencent-quote");
const { toTencentQuoteKey } = require("./tencent-quote-meta");
const { normalizeSymbol, addCalendarDays } = require("./db");
const { parseQuoteTimeToDateKey } = require("./position-today-pnl");

const SOURCE_SINA = "sina_dailyk";
const SOURCE_TENCENT = "tencent_qt";
const BATCH_CHUNK_SIZE = 25;
const LEN_INCREMENTAL = 2;
const LEN_BACKFILL = 5000;

function normDateKey(raw) {
  const s = String(raw || "").slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

function num(v) {
  return Number(String(v ?? "").replace(/,/g, ""));
}

function inferCloseMarket(symbol) {
  const s = String(symbol || "").trim().toLowerCase();
  if (/^(sh|sz)\d{6}$/.test(s)) {
    return "cn";
  }
  if (/^hk\d{5}$/.test(s) || /^rt_hk/.test(s)) {
    return "hk";
  }
  if (/^fx_/.test(s) || /^wh(usdcny|hkdcny)$/i.test(s)) {
    return "fx";
  }
  return "us";
}

function mapSinaBars(items, symbol) {
  return (items || [])
    .map((item) => {
      const day = normDateKey(item?.day ?? item?.date ?? item?.d);
      const close = num(item?.close ?? item?.c);
      if (!day || !(close > 0)) {
        return null;
      }
      return { symbol: normalizeSymbol(symbol), date: day, close, source: SOURCE_SINA };
    })
    .filter(Boolean);
}

function pickBarOnDate(bars, dateKey) {
  const dk = normDateKey(dateKey);
  if (!dk) {
    return null;
  }
  return (bars || []).find((b) => b.date === dk) || null;
}

function parseQuoteTimeHour(timeStr) {
  const t = String(timeStr || "").trim();
  if (!t || t === "--") {
    return null;
  }
  const compact = t.replace(/\D/g, "");
  if (compact.length >= 10) {
    const hour = Number(compact.slice(8, 10));
    return Number.isFinite(hour) ? hour : null;
  }
  const iso = /^(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})(?:\D+(\d{1,2}))?/.exec(t);
  if (iso) {
    const hour = Number(iso[4]);
    return Number.isFinite(hour) ? hour : null;
  }
  return null;
}

/**
 * 腾讯兜底日期校验：
 * - 默认 quoteDate === frozenDate
 * - 美股：另允许 quoteDate === frozenDate+1 且行情北京时间 < 08:00（刚收盘那根）
 */
function tencentQuoteMatchesFrozenDate(quote, frozenDate, symbol) {
  const fd = normDateKey(frozenDate);
  if (!quote || !fd) {
    return false;
  }
  const timeStr = String(quote.rawTime || quote.time || "").trim();
  const quoteDate = normDateKey(quote.quoteDate || quote.marketDate || parseQuoteTimeToDateKey(timeStr));
  if (!quoteDate) {
    return false;
  }
  if (quoteDate === fd) {
    return true;
  }
  if (inferCloseMarket(symbol) === "us") {
    const nextDay = normDateKey(addCalendarDays(fd, 1));
    if (quoteDate === nextDay) {
      const hour = parseQuoteTimeHour(timeStr);
      return hour != null && hour < 8;
    }
  }
  return false;
}

async function fetchSinaDailyKBatchMulti(symbols, options = {}) {
  const list = [...new Set((symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))];
  const requestToSymbol = new Map();
  for (const sym of list) {
    const req = toSinaDailyKBatchSymbol(sym);
    if (req) {
      requestToSymbol.set(req, sym);
    }
  }
  const requestSymbols = [...requestToSymbol.keys()];
  const out = new Map();
  for (const sym of list) {
    out.set(sym, []);
  }
  for (let i = 0; i < requestSymbols.length; i += BATCH_CHUNK_SIZE) {
    const chunk = requestSymbols.slice(i, i + BATCH_CHUNK_SIZE);
    const res = await fetchSinaDailyKBatchFromUpstream(chunk, {
      len: options.len != null ? options.len : LEN_INCREMENTAL,
      asc: 0,
      start: options.start,
      end: options.end,
    });
    if (!res.ok) {
      continue;
    }
    for (const reqSym of chunk) {
      const sym = requestToSymbol.get(reqSym);
      if (!sym) {
        continue;
      }
      const bars = mapSinaBars(res.data[reqSym] || [], sym);
      out.set(sym, bars);
    }
  }
  return out;
}

/**
 * 定时增量：冻结日单行；新浪优先，失败则腾讯 qt current。
 * @returns {Promise<{ written: number, result: object }>}
 */
async function fetchIncrementalCloseForSymbol(symbol, frozenDate, prefetchedBars) {
  const sym = normalizeSymbol(symbol);
  const fd = normDateKey(frozenDate);
  if (!sym || !fd) {
    return { written: 0, result: { symbol: sym, ok: false, reason: "invalid-args" } };
  }

  const bars = prefetchedBars || (await fetchSinaDailyKBatchMulti([sym], { len: LEN_INCREMENTAL })).get(sym) || [];
  const sinaBar = pickBarOnDate(bars, fd);
  if (sinaBar) {
    return {
      written: 1,
      row: { symbol: sym, date: fd, close: sinaBar.close, source: SOURCE_SINA },
      result: { symbol: sym, ok: true, source: SOURCE_SINA, close: sinaBar.close },
    };
  }

  const tencentRes = await fetchTencentQuoteMap([sym]);
  const quote = tencentRes.map?.get(sym);
  const current = Number(quote?.current);
  if (!quote || !(current > 0)) {
    return {
      written: 0,
      result: {
        symbol: sym,
        ok: false,
        reason: "no-sina-bar-and-no-tencent-quote",
        sinaDays: bars.map((b) => b.date),
      },
    };
  }
  if (!tencentQuoteMatchesFrozenDate(quote, fd, sym)) {
    const timeStr = String(quote.rawTime || quote.time || "");
    return {
      written: 0,
      result: {
        symbol: sym,
        ok: false,
        reason: "tencent-date-mismatch",
        quoteDate: normDateKey(quote.quoteDate || parseQuoteTimeToDateKey(timeStr)),
        frozenDate: fd,
        quoteTime: timeStr,
        sinaDays: bars.map((b) => b.date),
      },
    };
  }

  return {
    written: 1,
    row: { symbol: sym, date: fd, close: current, source: SOURCE_TENCENT },
    result: {
      symbol: sym,
      ok: true,
      source: SOURCE_TENCENT,
      close: current,
      quoteTime: String(quote.time || ""),
    },
  };
}

/**
 * 多天回填：DailyK_Batch len=5000 + start/end，按 bar.day 逐日入库。
 */
async function fetchBackfillClosesForSymbol(symbol, fromDate, toDate) {
  const sym = normalizeSymbol(symbol);
  const from = normDateKey(fromDate);
  const to = normDateKey(toDate);
  if (!sym || !from || !to || from > to) {
    return [];
  }
  const bars =
    (await fetchSinaDailyKBatchMulti([sym], { len: LEN_BACKFILL, start: from, end: to })).get(sym) || [];
  return bars.filter((b) => b.date >= from && b.date <= to);
}

module.exports = {
  SOURCE_SINA,
  SOURCE_TENCENT,
  LEN_INCREMENTAL,
  LEN_BACKFILL,
  inferCloseMarket,
  pickBarOnDate,
  tencentQuoteMatchesFrozenDate,
  fetchSinaDailyKBatchMulti,
  fetchIncrementalCloseForSymbol,
  fetchBackfillClosesForSymbol,
  toTencentQuoteKey,
};
