const {
  fetchSinaDailyKBatchFromUpstream,
  toSinaDailyKBatchSymbol,
} = require("./sina-kline-upstream");
const { SOURCE_SINA } = require("./daily-close-fetch");

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

function mapSinaDailyBarRows(items, symbol, source = SOURCE_SINA) {
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
  return mapSinaDailyBarRows(res.data[requestSymbol] || [], symbol, SOURCE_SINA);
}

/**
 * 为单一标的拉取 [from,to] 内日线收盘（仅新浪 DailyK_Batch，供审计/缺口比对）。
 */
async function fetchRemoteDailyClosesForSymbol(normalized, fromDate, toDate) {
  const symbol = String(normalized || "").trim();
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  if (!symbol || !from || !to || from > to) {
    return [];
  }
  return filterRowsToRange(await fetchSinaDailyKBatchCloses(symbol, from, to), from, to);
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
  fetchSinaDailyKBatchCloses,
  diffMissingCloseDates,
  summarizeGapRanges,
  inferCloseMarket,
  addCalendarDays,
};
