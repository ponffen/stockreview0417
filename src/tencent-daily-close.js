/**
 * 腾讯 fqkline 日 K（服务端拉取），在东财/新浪无数据时兜底（尤其美股代码以小写存库时）。
 */
function extractTencentFqklineRows(payload, requestSymbol) {
  const data = payload?.data || {};
  const root = data[requestSymbol] || data[Object.keys(data)[0]] || {};
  const qfq = root.qfq || {};
  if (Array.isArray(qfq.day)) {
    return qfq.day;
  }
  if (Array.isArray(qfq.week)) {
    return qfq.week;
  }
  if (Array.isArray(qfq.month)) {
    return qfq.month;
  }
  /** 美股常走 root.day 裸数组，非 qfq.day */
  if (Array.isArray(root.day)) {
    return root.day;
  }
  return (
    root.qfqday ||
    root.qfqweek ||
    root.week ||
    root.qfqmonth ||
    root.month ||
    root.qfqmin ||
    root.min ||
    []
  );
}

/** 与 app toTencentQuoteSymbol 一致 */
function toTencentKlineSymbolNormalized(normalized) {
  const raw = String(normalized || "").trim().toLowerCase().replace(/\s+/g, "");
  const orig = String(normalized || "").trim().replace(/\s+/g, "");
  if (/^sh\d{6}$/.test(raw) || /^sz\d{6}$/.test(raw) || /^hk\d{5}$/.test(raw)) {
    return raw;
  }
  if (/^us[A-Z0-9._-]+$/i.test(orig)) {
    const base = orig.replace(/^us/i, "").replace(/\.(OQ|N)$/i, "");
    return `us${base.toUpperCase()}`;
  }
  if (/^gb_/i.test(raw)) {
    return `us${raw.slice(3).toUpperCase()}`;
  }
  if (/^rt_hk/i.test(raw)) {
    const code = raw.replace(/^rt_hk/i, "").padStart(5, "0");
    return `hk${code}`;
  }
  if (/^[a-z][a-z0-9._-]*$/i.test(raw)) {
    return `us${raw.toUpperCase()}`;
  }
  return "";
}

/**
 * 返回 { date, close, source: 'tencent' }[] 升序
 */
async function fetchTencentDailyCloses(normalized, startDateKey, endDateKey) {
  const requestSymbol = toTencentKlineSymbolNormalized(normalized);
  if (!requestSymbol) {
    return [];
  }
  const startStr = String(startDateKey || "").slice(0, 10);
  const endStr = String(endDateKey || "").slice(0, 10);
  if (!startStr || !endStr) {
    return [];
  }
  const count = 2000;
  const param = `${requestSymbol},day,${startStr},${endStr},${count},qfq`;
  const url = `https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=${encodeURIComponent(param)}`;
  let response;
  try {
    response = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; stockreview-backfill/1.0)" },
      signal: AbortSignal.timeout(35_000),
    });
  } catch {
    return [];
  }
  if (!response.ok) {
    return [];
  }
  let payload;
  try {
    payload = await response.json();
  } catch {
    return [];
  }
  const source = extractTencentFqklineRows(payload, requestSymbol);
  if (!Array.isArray(source) || !source.length) {
    return [];
  }
  const out = [];
  for (const item of source) {
    const day = String(item?.[0] || "").slice(0, 10).replace(/\//g, "-");
    const close = Number(item?.[2]);
    if (!day || !Number.isFinite(close) || close <= 0) {
      continue;
    }
    if (day < startStr || day > endStr) {
      continue;
    }
    out.push({ date: day, close, source: "tencent" });
  }
  out.sort((a, b) => a.date.localeCompare(b.date));
  return out;
}

module.exports = {
  fetchTencentDailyCloses,
  toTencentKlineSymbolNormalized,
};
