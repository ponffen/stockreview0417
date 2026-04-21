const { fetchSinaKlineJsonFromUpstream } = require("./sina-kline-upstream");
const { fetchEastmoneyDailyCloses } = require("./eastmoney-daily-close");
const { fetchTencentDailyCloses } = require("./tencent-daily-close");
const { fetchFmpDailyCloses } = require("./fmp-daily-close");

/** 与 app toSinaKlineSymbol 一致 */
function toSinaKlineSymbol(normalized) {
  const n = String(normalized || "").toLowerCase().trim();
  if (!n) {
    return "";
  }
  if (/^sh\d{6}$/.test(n) || /^sz\d{6}$/.test(n)) {
    return n;
  }
  if (/^hk\d{5}$/.test(n)) {
    return `rt_hk_${n.slice(2)}`;
  }
  if (/^rt_hk/i.test(n)) {
    const digits = n.replace(/^rt_hk_?/i, "").replace(/\D/g, "").padStart(5, "0");
    return `rt_hk_${digits}`;
  }
  if (/^gb_/i.test(n)) {
    return `gb_${n.slice(3).toUpperCase()}`;
  }
  if (/^[a-z][a-z0-9._-]*$/i.test(n)) {
    return `gb_${n.toUpperCase()}`;
  }
  return "";
}

/** 裸 gb_ / 拉丁 ticker / usXXX：腾讯 fqkline 对美股经常只有 1 根，勿用来「补缺」 */
function shouldSkipTencentForNormalized(normalized) {
  const n = String(normalized || "").trim().toLowerCase();
  if (!n) {
    return true;
  }
  if (/^sh\d{6}$/.test(n) || /^sz\d{6}$/.test(n) || /^hk\d{5}$/.test(n) || /^rt_hk/i.test(n)) {
    return false;
  }
  if (/^gb_/i.test(n)) {
    return true;
  }
  if (/^us[a-z]/i.test(n)) {
    return true;
  }
  if (/^[a-z][a-z0-9._-]*$/i.test(n)) {
    return true;
  }
  return false;
}

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
 * 为单一标的拉取 [from,to] 内日线收盘：东财长区间 + 新浪 1023 根合并去重（同一日东财优先）。
 */
async function fetchRemoteDailyClosesForSymbol(normalized, fromDate, toDate) {
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  const em = await fetchEastmoneyDailyCloses(normalized, from, to).catch(() => []);
  let sinaRows = [];
  const sinaSym = toSinaKlineSymbol(normalized);
  if (sinaSym) {
    const res = await fetchSinaKlineJsonFromUpstream({
      symbol: sinaSym,
      scale: "240",
      ma: "no",
      datalen: "1023",
    });
    if (res.ok) {
      const raw = res.data == null ? [] : res.data;
      sinaRows = mapSinaPayloadToRows(raw).filter((r) => r.date >= from && r.date <= to);
    }
  }
  let fmpRows = await fetchFmpDailyCloses(normalized, from, to).catch(() => []);
  if (!fmpRows.length) {
    fmpRows = [];
  }
  let tx = [];
  if (!shouldSkipTencentForNormalized(normalized)) {
    tx = await fetchTencentDailyCloses(normalized, from, to).catch(() => []);
  }
  /** 同一日：东财 > 新浪 > FMP（可选）> 腾讯（仅补缺，且跳过无效的美股腾讯） */
  return mergeDailyRows(mergeDailyRows(mergeDailyRows(em, sinaRows), fmpRows), tx);
}

module.exports = {
  fetchRemoteDailyClosesForSymbol,
  mergeDailyRows,
  toSinaKlineSymbol,
};
