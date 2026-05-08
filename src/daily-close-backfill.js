const { fetchSinaKlineJsonFromUpstream } = require("./sina-kline-upstream");

function mapSinaPayloadToRows(payload) {
  if (payload == null) {
    return [];
  }
  const arr = Array.isArray(payload) ? payload : [];
  const num = (v) => Number(String(v ?? "").replace(/,/g, ""));
  return arr
    .map((item) => {
      const raw = String(item?.day ?? "").trim();
      const day = raw.includes(" ") ? raw.replace(/\//g, "-").slice(0, 10) : raw.slice(0, 10).replace(/\//g, "-");
      const close = num(item?.close);
      return { date: day, close, source: "sina" };
    })
    .filter((r) => r.date && Number.isFinite(r.close) && r.close > 0);
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
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  if (!from || !to) {
    return [];
  }
  const res = await fetchSinaKlineJsonFromUpstream({
    symbol: normalized,
    start: from,
    end: to,
    len: "5000",
    asc: "0",
  });
  if (!res.ok) {
    return [];
  }
  const raw = res.data == null ? [] : res.data;
  return mapSinaPayloadToRows(raw).filter((r) => r.date >= from && r.date <= to);
}

module.exports = {
  fetchRemoteDailyClosesForSymbol,
  mergeDailyRows,
};
