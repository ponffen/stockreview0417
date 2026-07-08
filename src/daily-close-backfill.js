const {
  fetchSinaKlineJsonFromUpstream,
  fetchSinaDailyKBatchFromUpstream,
  toSinaDailyKBatchSymbol,
} = require("./sina-kline-upstream");

const SINA_CN_KLINE_ENDPOINT =
  "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData";
const SINA_US_DAILY_ENDPOINT =
  "https://stock.finance.sina.com.cn/usstock/api/json.php/US_MinKService.getDailyK";
const GTIMG_HK_KLINE_ENDPOINT = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get";

const SINA_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  Referer: "https://finance.sina.com.cn/",
};

function addCalendarDays(dateKey, days) {
  const d = new Date(`${String(dateKey || "").slice(0, 10)}T12:00:00+08:00`);
  if (Number.isNaN(d.getTime())) {
    return String(dateKey || "").slice(0, 10);
  }
  d.setDate(d.getDate() + Number(days || 0));
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function num(v) {
  return Number(String(v ?? "").replace(/,/g, ""));
}

function normalizeDayKey(raw) {
  const text = String(raw ?? "").trim();
  if (!text) {
    return "";
  }
  if (text.includes(" ")) {
    return text.replace(/\//g, "-").slice(0, 10);
  }
  return text.slice(0, 10).replace(/\//g, "-");
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
  if (/^(sh000001|sz399001)$/.test(s)) {
    return "cn";
  }
  return "us";
}

function usTickerFromSymbol(symbol) {
  const s = String(symbol || "").trim();
  if (/^gb_/i.test(s)) {
    return s.slice(3).replace(/\.(oq|n)$/i, "").toUpperCase();
  }
  if (/^us_/i.test(s)) {
    return s.slice(3).replace(/\.(oq|n)$/i, "").toUpperCase();
  }
  return s.replace(/\.(oq|n)$/i, "").toUpperCase();
}

function hkCodeFromSymbol(symbol) {
  const s = String(symbol || "").trim().toLowerCase();
  if (/^hk\d{5}$/.test(s)) {
    return s;
  }
  if (/^rt_hk/.test(s)) {
    const digits = s.replace(/^rt_hk_?/i, "").replace(/\D/g, "").padStart(5, "0");
    return `hk${digits}`;
  }
  return s;
}

function mapSinaDailyBarRows(items, symbol, source = "sina") {
  const normalizedSymbol = String(symbol || "").trim();
  return (items || [])
    .map((item) => {
      const day = normalizeDayKey(item?.day ?? item?.d ?? item?.date);
      const close = num(item?.close ?? item?.c);
      return { symbol: normalizedSymbol, date: day, close, source };
    })
    .filter((r) => r.symbol && r.date && Number.isFinite(r.close) && r.close > 0);
}

function filterRowsToRange(rows, from, to) {
  return (rows || []).filter((r) => r.date >= from && r.date <= to);
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: SINA_HEADERS,
    signal: AbortSignal.timeout(35_000),
  });
  if (!response.ok) {
    throw new Error(`http ${response.status}`);
  }
  return response.json();
}

/** A 股/沪深 ETF：新浪 CN_MarketData.getKLineData（最近约 1023 个交易日）。 */
async function fetchSinaCnDailyCloses(symbol) {
  const requestSymbol = String(symbol || "").trim().toLowerCase();
  if (!/^(sh|sz)\d{6}$/.test(requestSymbol)) {
    return [];
  }
  const url = `${SINA_CN_KLINE_ENDPOINT}?symbol=${encodeURIComponent(requestSymbol)}&scale=240&ma=no&datalen=1023`;
  const payload = await fetchJson(url);
  if (!Array.isArray(payload)) {
    return [];
  }
  return mapSinaDailyBarRows(payload, symbol, "sina");
}

/** 美股：新浪 US_MinKService.getDailyK（全历史）。 */
async function fetchSinaUsDailyCloses(symbol) {
  const ticker = usTickerFromSymbol(symbol);
  if (!ticker) {
    return [];
  }
  const url = `${SINA_US_DAILY_ENDPOINT}?symbol=${encodeURIComponent(ticker)}`;
  const payload = await fetchJson(url);
  if (!Array.isArray(payload)) {
    return [];
  }
  return mapSinaDailyBarRows(payload, symbol, "sina");
}

/**
 * 港股：新浪 HK 历史接口已失效，使用腾讯 fqkline 补齐（港股行情常与新浪页共用）。
 * 仍会与 DailyK_Batch 近端数据合并。
 */
async function fetchGtimgHkDailyCloses(symbol, fromDate, toDate) {
  const hkCode = hkCodeFromSymbol(symbol);
  if (!/^hk\d{5}$/.test(hkCode)) {
    return [];
  }
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  const merged = new Map();
  const chunks = [];
  let cursor = from;
  while (cursor <= to) {
    const chunkEndRaw = addCalendarDays(cursor, 900);
    const chunkEnd = chunkEndRaw > to ? to : chunkEndRaw;
    chunks.push({ from: cursor, to: chunkEnd });
    if (chunkEnd >= to) {
      break;
    }
    cursor = addCalendarDays(chunkEnd, 1);
  }
  for (const chunk of chunks) {
    const url = `${GTIMG_HK_KLINE_ENDPOINT}?param=${encodeURIComponent(`${hkCode},day,${chunk.from},${chunk.to},1000,qfq`)}`;
    const payload = await fetchJson(url);
    const dayRows = payload?.data?.[hkCode]?.day;
    if (!Array.isArray(dayRows)) {
      continue;
    }
    for (const row of dayRows) {
      if (!Array.isArray(row) || row.length < 3) {
        continue;
      }
      const date = normalizeDayKey(row[0]);
      const close = num(row[2]);
      if (!date || !(close > 0) || date < from || date > to) {
        continue;
      }
      if (!merged.has(date)) {
        merged.set(date, {
          symbol: String(symbol || "").trim(),
          date,
          close,
          source: "gtimg_hk",
        });
      }
    }
  }
  return [...merged.values()].sort((a, b) => a.date.localeCompare(b.date));
}

async function fetchSinaDailyKBatchCloses(symbol, fromDate, toDate) {
  const requestSymbol = toSinaDailyKBatchSymbol(symbol);
  if (!requestSymbol) {
    return [];
  }
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  const res = await fetchSinaDailyKBatchFromUpstream([requestSymbol], {
    len: 5000,
    asc: 0,
    start: from,
    end: to,
  });
  if (!res.ok) {
    return [];
  }
  return mapSinaDailyBarRows(res.data[requestSymbol] || [], symbol, "sina");
}

/** 合并去重：同一日保留先出现的（长历史源优先于短窗口源）。 */
function mergeDailyRows(primary, secondary) {
  const m = new Map();
  [...primary, ...secondary].forEach((r) => {
    if (!r?.date || !Number.isFinite(r.close)) {
      return;
    }
    if (!m.has(r.date)) {
      m.set(r.date, r);
    }
  });
  return [...m.values()].sort((a, b) => a.date.localeCompare(b.date));
}

/**
 * 为单一标的拉取 [from,to] 内日线收盘。
 * - A 股：新浪 CN_MarketData.getKLineData
 * - 美股：新浪 US_MinKService.getDailyK
 * - 港股：gtimg 全历史 + 新浪 DailyK_Batch 近端
 * - 其它：新浪 DailyK_Batch
 */
async function fetchRemoteDailyClosesForSymbol(normalized, fromDate, toDate) {
  const symbol = String(normalized || "").trim();
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  if (!symbol || !from || !to || from > to) {
    return [];
  }

  const market = inferCloseMarket(symbol);
  let rows = [];
  if (market === "cn") {
    rows = await fetchSinaCnDailyCloses(symbol);
  } else if (market === "us") {
    rows = await fetchSinaUsDailyCloses(symbol);
  } else if (market === "hk") {
    const [gtimgRows, batchRows] = await Promise.all([
      fetchGtimgHkDailyCloses(symbol, from, to),
      fetchSinaDailyKBatchCloses(symbol, from, to),
    ]);
    rows = mergeDailyRows(gtimgRows, batchRows);
  } else {
    rows = await fetchSinaDailyKBatchCloses(symbol, from, to);
    if (!rows.length) {
      const res = await fetchSinaKlineJsonFromUpstream({
        symbol,
        start: from,
        end: to,
        len: "5000",
        asc: "0",
      });
      if (res.ok) {
        rows = mapSinaDailyBarRows(res.data == null ? [] : res.data, symbol, "sina");
      }
    }
  }
  return filterRowsToRange(rows, from, to);
}

/** 以远端行情为基准，统计本地 symbol_daily_close 缺口。 */
function diffMissingCloseDates(localRows, remoteRows, fromDate, toDate) {
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  const localDates = new Set((localRows || []).map((r) => String(r.date || "").slice(0, 10)));
  return (remoteRows || [])
    .map((r) => String(r.date || "").slice(0, 10))
    .filter((d) => d >= from && d <= to && !localDates.has(d))
    .sort();
}

function summarizeGapRanges(missingDates) {
  const dates = [...(missingDates || [])].sort();
  if (!dates.length) {
    return [];
  }
  const ranges = [];
  let start = dates[0];
  let prev = dates[0];
  let count = 1;
  for (let i = 1; i < dates.length; i += 1) {
    const cur = dates[i];
    const gapDays = Math.round((new Date(cur) - new Date(prev)) / 86400000);
    if (gapDays > 4) {
      ranges.push({ from: start, to: prev, count });
      start = cur;
      count = 1;
    } else {
      count += 1;
    }
    prev = cur;
  }
  ranges.push({ from: start, to: prev, count });
  return ranges;
}

module.exports = {
  fetchRemoteDailyClosesForSymbol,
  fetchSinaCnDailyCloses,
  fetchSinaUsDailyCloses,
  fetchGtimgHkDailyCloses,
  mergeDailyRows,
  diffMissingCloseDates,
  summarizeGapRanges,
  inferCloseMarket,
};
