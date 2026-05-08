/**
 * 新浪 DailyK_Batch：统一日 K 与外汇日线。
 */
const SINA_DAILY_BATCH_ENDPOINT =
  "https://quotes.sina.cn/hq/api/openapi.php/MarketCenterService.getDailyK_Batch";
const SINA_KLINE_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  Referer: "https://quotes.sina.cn/",
};

function toSinaDailyBatchSymbolFromNormalized(symbol) {
  const src = String(symbol || "").trim().replace(/\s+/g, "");
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
    return `us_${src.replace(/^us_/i, "").replace(/\.(OQ|N)$/i, "").toUpperCase()}`;
  }
  if (/^fx_(USDCNY|HKDCNY)$/i.test(src)) {
    return `fx_${src.replace(/^fx_/i, "").toUpperCase()}`;
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
    return `us_${n.slice(3).replace(/\.(oq|n)$/i, "").toUpperCase()}`;
  }
  if (/^us[A-Z0-9._-]+$/i.test(src)) {
    return `us_${src.replace(/^us/i, "").replace(/\.(OQ|N)$/i, "").toUpperCase()}`;
  }
  if (/^fx_usdcny$/i.test(src) || /^whusdcny$/i.test(src) || /^usdcny$/i.test(src)) {
    return "fx_USDCNY";
  }
  if (/^fx_hkdcny$/i.test(src) || /^whhkdcny$/i.test(src) || /^hkdcny$/i.test(src)) {
    return "fx_HKDCNY";
  }
  if (/^[a-z][a-z0-9._-]*$/i.test(src)) {
    return `us_${src.replace(/\.(OQ|N)$/i, "").toUpperCase()}`;
  }
  return "";
}

function mapSinaDailyRows(source) {
  if (!Array.isArray(source)) {
    return [];
  }
  const num = (v) => Number(String(v ?? "").replace(/,/g, ""));
  const out = [];
  for (const item of source) {
    const day = String(item?.day ?? item?.date ?? item?.d ?? "")
      .trim()
      .slice(0, 10)
      .replace(/\//g, "-");
    const close = num(item?.close ?? item?.c);
    if (!day || !Number.isFinite(close) || close <= 0) {
      continue;
    }
    out.push({
      day,
      open: num(item?.open ?? item?.o),
      high: num(item?.high ?? item?.h),
      low: num(item?.low ?? item?.l),
      close,
      volume: num(item?.volume ?? item?.v ?? item?.amount ?? item?.a),
    });
  }
  out.sort((a, b) => a.day.localeCompare(b.day));
  return out;
}

async function fetchSinaDailyBatchRows(symbol, options = {}) {
  const requestSymbol = toSinaDailyBatchSymbolFromNormalized(symbol);
  if (!requestSymbol) {
    return [];
  }
  const len = Math.min(5000, Math.max(2, Number(options.len) || 2048));
  const params = new URLSearchParams({
    symbols: requestSymbol,
    len: String(len),
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
  const response = await fetch(url, {
    cache: "no-store",
    headers: SINA_KLINE_HEADERS,
    signal: AbortSignal.timeout(35_000),
  });
  if (!response.ok) {
    throw new Error(`新浪DailyK失败 ${symbol}: ${response.status}`);
  }
  const payload = await response.json();
  const root = payload?.result?.data || payload?.data || {};
  const arr = root?.[requestSymbol] || root?.[requestSymbol.toLowerCase()] || root?.[requestSymbol.toUpperCase()] || [];
  return mapSinaDailyRows(arr);
}

async function fetchKlineDataSina(symbol, datalen = 2048) {
  return fetchSinaDailyBatchRows(symbol, { len: datalen, asc: 0 });
}

async function fetchSinaForexDayKSeries(currencyOrPair) {
  const raw = String(currencyOrPair || "").trim().toUpperCase();
  const symbol =
    raw === "USD" || raw === "USDCNY" || raw === "FX_USDCNY" ? "fx_USDCNY" : raw === "HKD" || raw === "HKDCNY" || raw === "FX_HKDCNY" ? "fx_HKDCNY" : "";
  if (!symbol) {
    return {};
  }
  const rows = await fetchSinaDailyBatchRows(symbol, { len: 5000, asc: 0 });
  const out = {};
  for (const row of rows) {
    out[row.day] = row.close;
  }
  return out;
}

function toDateKey(date = new Date()) {
  const d = date instanceof Date ? date : new Date(date);
  const base = Number.isNaN(d.getTime()) ? new Date() : d;
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(base);
}

function enumerateDays(fromStr, toStr) {
  const out = [];
  const a = new Date(`${fromStr}T12:00:00+08:00`);
  const b = new Date(`${toStr}T12:00:00+08:00`);
  const cur = new Date(a);
  while (cur <= b) {
    out.push(toDateKey(cur));
    cur.setDate(cur.getDate() + 1);
  }
  return out;
}

function validNumber(...values) {
  for (const v of values) {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

module.exports = {
  fetchKlineDataSina,
  fetchSinaForexDayKSeries,
  toSinaKlineSymbolFromNormalized: toSinaDailyBatchSymbolFromNormalized,
  toDateKey,
  enumerateDays,
  validNumber,
};
