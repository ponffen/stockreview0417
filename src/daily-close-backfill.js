const { fetchSinaKlineJsonFromUpstream } = require("./sina-kline-upstream");

const CHUNK_CALENDAR_DAYS = 3200;

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

function mapSinaPayloadToRows(payload, symbol) {
  if (payload == null) {
    return [];
  }
  const arr = Array.isArray(payload) ? payload : [];
  const normalizedSymbol = String(symbol || "").trim();
  const num = (v) => Number(String(v ?? "").replace(/,/g, ""));
  return arr
    .map((item) => {
      const raw = String(item?.day ?? "").trim();
      const day = raw.includes(" ") ? raw.replace(/\//g, "-").slice(0, 10) : raw.slice(0, 10).replace(/\//g, "-");
      const close = num(item?.close);
      return { symbol: normalizedSymbol, date: day, close, source: "sina" };
    })
    .filter((r) => r.symbol && r.date && Number.isFinite(r.close) && r.close > 0);
}

/** 合并去重：同一日保留先出现的（优先东财长区间） */
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
 * 为单一标的拉取 [from,to] 内日线收盘：统一使用新浪 DailyK_Batch。
 */
async function fetchRemoteDailyClosesForSymbol(normalized, fromDate, toDate) {
  const symbol = String(normalized || "").trim();
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  if (!symbol || !from || !to || from > to) {
    return [];
  }
  const mergedByDate = new Map();
  let cursor = from;
  while (cursor <= to) {
    const chunkToRaw = addCalendarDays(cursor, CHUNK_CALENDAR_DAYS);
    const chunkTo = chunkToRaw > to ? to : chunkToRaw;
    const res = await fetchSinaKlineJsonFromUpstream({
      symbol,
      start: cursor,
      end: chunkTo,
      len: "5000",
      asc: "0",
    });
    if (res.ok) {
      const raw = res.data == null ? [] : res.data;
      for (const row of mapSinaPayloadToRows(raw, symbol)) {
        if (row.date >= from && row.date <= to) {
          mergedByDate.set(row.date, row);
        }
      }
    }
    if (chunkTo >= to) {
      break;
    }
    cursor = addCalendarDays(chunkTo, 1);
  }
  return [...mergedByDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

module.exports = {
  fetchRemoteDailyClosesForSymbol,
  mergeDailyRows,
};
