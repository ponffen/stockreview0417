const SINA_CHROME_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const SINA_REFERER = "https://quotes.sina.cn/";
const SINA_DAILY_BATCH_ENDPOINT =
  "https://quotes.sina.cn/hq/api/openapi.php/MarketCenterService.getDailyK_Batch";

function toSinaDailyKBatchSymbol(rawSymbol) {
  const src = String(rawSymbol || "").trim().replace(/\s+/g, "");
  const n = src.toLowerCase();
  if (!n) {
    return "";
  }
  if (/^cn_(sh|sz)\d{6}$/i.test(src)) {
    return n;
  }
  if (/^hk_hk\d{5}$/i.test(src)) {
    return `hk_hk${src.replace(/^hk_hk/i, "")}`;
  }
  if (/^us_[A-Z0-9._-]+$/i.test(src)) {
    const base = src.replace(/^us_/i, "").replace(/\.(OQ|N)$/i, "").toUpperCase();
    return base ? `us_${base}` : "";
  }
  if (/^fx_(USDCNY|HKDCNY)$/i.test(src)) {
    return `fx_${src.replace(/^fx_/i, "").toUpperCase()}`;
  }
  if (/^wh(USDCNY|HKDCNY)$/i.test(src)) {
    return `fx_${src.replace(/^wh/i, "").toUpperCase()}`;
  }
  if (/^sh\d{6}$/.test(n) || /^sz\d{6}$/.test(n)) {
    return `cn_${n}`;
  }
  if (/^hk\d{5}$/.test(n)) {
    return `hk_${n}`;
  }
  if (/^rt_hk/i.test(n)) {
    const digits = n.replace(/^rt_hk_?/i, "").replace(/\D/g, "").padStart(5, "0");
    return `hk_hk${digits}`;
  }
  if (/^gb_/i.test(n)) {
    const ticker = n.slice(3).replace(/\.(oq|n)$/i, "").toUpperCase();
    return ticker ? `us_${ticker}` : "";
  }
  if (/^us[A-Z0-9._-]+$/i.test(src)) {
    const ticker = src.replace(/^us/i, "").replace(/\.(OQ|N)$/i, "").toUpperCase();
    return ticker ? `us_${ticker}` : "";
  }
  if (/^fx_usdcny$/i.test(src)) {
    return "fx_USDCNY";
  }
  if (/^fx_hkdcny$/i.test(src)) {
    return "fx_HKDCNY";
  }
  if (/^[a-z][a-z0-9._-]*$/i.test(src)) {
    return `us_${src.replace(/\.(OQ|N)$/i, "").toUpperCase()}`;
  }
  return "";
}

function normalizeSinaDailyBar(item) {
  const dayRaw = String(item?.day ?? item?.date ?? item?.d ?? "").trim();
  const day = dayRaw.slice(0, 10).replace(/\//g, "-");
  const num = (v) => Number(String(v ?? "").replace(/,/g, ""));
  const close = num(item?.close ?? item?.c);
  if (!day || !Number.isFinite(close) || close <= 0) {
    return null;
  }
  const open = num(item?.open ?? item?.o);
  const high = num(item?.high ?? item?.h);
  const low = num(item?.low ?? item?.l);
  const volume = num(item?.volume ?? item?.v ?? item?.amount ?? item?.a);
  return {
    day,
    open: Number.isFinite(open) ? open : close,
    high: Number.isFinite(high) ? high : close,
    low: Number.isFinite(low) ? low : close,
    close,
    volume: Number.isFinite(volume) ? volume : 0,
  };
}

function parseSinaDailyBatchRows(payload, requestSymbol) {
  const root = payload?.result?.data || payload?.data || {};
  const candidates = [
    requestSymbol,
    requestSymbol.toLowerCase(),
    requestSymbol.toUpperCase(),
    requestSymbol.replace(/^fx_/i, "fx_"),
  ];
  let source = [];
  for (const key of candidates) {
    if (Array.isArray(root?.[key])) {
      source = root[key];
      break;
    }
  }
  if (!source.length && root && typeof root === "object") {
    const vals = Object.values(root);
    if (vals.length === 1 && Array.isArray(vals[0])) {
      source = vals[0];
    }
  }
  const out = [];
  for (const item of source) {
    const row = normalizeSinaDailyBar(item);
    if (row) {
      out.push(row);
    }
  }
  out.sort((a, b) => a.day.localeCompare(b.day));
  return out;
}

function normalizeLen(input) {
  const n = Number(input);
  if (!Number.isFinite(n)) {
    return 1023;
  }
  return Math.min(5000, Math.max(2, Math.floor(n)));
}

/**
 * 从新浪开放接口拉取日 K 批量数据。
 * @param {string[]} symbols
 * @param {{ len?: number|string, asc?: number|string, start?: string, end?: string }} options
 */
async function fetchSinaDailyKBatchFromUpstream(symbols, options = {}) {
  const normalized = [...new Set((symbols || []).map(toSinaDailyKBatchSymbol).filter(Boolean))];
  if (!normalized.length) {
    return { ok: false, data: {}, error: "invalid symbol(s)" };
  }
  const params = new URLSearchParams({
    symbols: normalized.join(","),
    len: String(normalizeLen(options.len)),
    asc: String(options.asc != null ? options.asc : 0),
  });
  const start = String(options.start || "").slice(0, 10);
  const end = String(options.end || "").slice(0, 10);
  if (/^\d{4}-\d{2}-\d{2}$/.test(start)) {
    params.set("start", start);
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(end)) {
    params.set("end", end);
  }
  const url = `${SINA_DAILY_BATCH_ENDPOINT}?${params.toString()}`;
  let response;
  try {
    response = await fetch(url, {
      headers: { "User-Agent": SINA_CHROME_UA, Referer: SINA_REFERER },
      signal: AbortSignal.timeout(30_000),
    });
  } catch (e) {
    return { ok: false, data: {}, error: e?.message || "fetch failed" };
  }
  if (!response.ok) {
    return { ok: false, data: {}, error: `sina ${response.status}` };
  }
  let payload;
  try {
    payload = await response.json();
  } catch {
    return { ok: false, data: {}, error: "invalid json" };
  }
  const out = {};
  for (const sym of normalized) {
    out[sym] = parseSinaDailyBatchRows(payload, sym);
  }
  return { ok: true, data: out, error: "" };
}

/**
 * 兼容旧调用：单 symbol 返回数组（day/open/high/low/close/volume）。
 */
async function fetchSinaKlineJsonFromUpstream(paramsObj) {
  const rawSymbol = paramsObj?.symbol != null ? String(paramsObj.symbol) : "";
  const requestSymbol = toSinaDailyKBatchSymbol(rawSymbol);
  if (!requestSymbol) {
    return { ok: false, data: null, error: "invalid symbol" };
  }
  const len = paramsObj?.len != null ? paramsObj.len : paramsObj?.datalen;
  const res = await fetchSinaDailyKBatchFromUpstream([requestSymbol], {
    len: len != null ? len : 1023,
    asc: paramsObj?.asc != null ? paramsObj.asc : 0,
    start: paramsObj?.start,
    end: paramsObj?.end,
  });
  if (!res.ok) {
    return { ok: false, data: null, error: res.error || "sina failed" };
  }
  return { ok: true, data: res.data[requestSymbol] || [], error: "" };
}

module.exports = {
  fetchSinaKlineJsonFromUpstream,
  fetchSinaDailyKBatchFromUpstream,
  toSinaDailyKBatchSymbol,
  SINA_CHROME_UA,
  SINA_REFERER,
  SINA_DAILY_BATCH_ENDPOINT,
};
