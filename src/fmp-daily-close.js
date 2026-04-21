/**
 * Financial Modeling Prep 日收盘价（可选）。
 * 需环境变量 FMP_API_KEY（免费注册 https://site.financialmodelingprep.com/ ）。
 * 仅用于「类美股」代码：gb_xxx 或裸拉丁字母 ticker，不用于 A 股 / 港股代码格式。
 */

function normalizedToFmpSymbol(normalized) {
  const raw = String(normalized || "").trim();
  const n = raw.toLowerCase();
  if (!n) {
    return "";
  }
  if (/^sh\d{6}$/.test(n) || /^sz\d{6}$/.test(n) || /^hk\d{5}$/.test(n) || /^rt_hk/i.test(n)) {
    return "";
  }
  if (/^gb_/i.test(n)) {
    return raw.slice(3).toUpperCase();
  }
  if (/^us[A-Z0-9._-]+$/i.test(raw)) {
    return raw.replace(/^us/i, "").replace(/\.(OQ|N)$/i, "").toUpperCase();
  }
  if (/^[a-z][a-z0-9._-]*$/i.test(n)) {
    return raw.toUpperCase();
  }
  return "";
}

/**
 * 返回 { date, close, source: 'fmp' }[] 升序
 */
async function fetchFmpDailyCloses(normalized, begDateKey, endDateKey) {
  const key = String(process.env.FMP_API_KEY || "").trim();
  const sym = normalizedToFmpSymbol(normalized);
  if (!key || !sym) {
    return [];
  }
  const from = String(begDateKey || "").slice(0, 10);
  const to = String(endDateKey || "").slice(0, 10);
  if (!from || !to) {
    return [];
  }
  const url = `https://financialmodelingprep.com/api/v3/historical-price-full/${encodeURIComponent(sym)}?from=${from}&to=${to}&apikey=${encodeURIComponent(key)}`;
  let response;
  try {
    response = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; stockreview-backfill/1.0)" },
      signal: AbortSignal.timeout(45_000),
    });
  } catch {
    return [];
  }
  if (!response.ok) {
    return [];
  }
  let j;
  try {
    j = await response.json();
  } catch {
    return [];
  }
  const hist = j?.historical;
  if (!Array.isArray(hist) || !hist.length) {
    return [];
  }
  const out = [];
  for (const row of hist) {
    const day = String(row?.date || "").slice(0, 10);
    const close = Number(row?.close);
    if (!day || !Number.isFinite(close) || close <= 0) {
      continue;
    }
    if (day < from || day > to) {
      continue;
    }
    out.push({ date: day, close, source: "fmp" });
  }
  out.sort((a, b) => a.date.localeCompare(b.date));
  return out;
}

module.exports = {
  fetchFmpDailyCloses,
  normalizedToFmpSymbol,
};
