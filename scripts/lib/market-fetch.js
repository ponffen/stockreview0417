/**
 * 新浪 CN_MarketData.getKLineData 日 K（与 app.js fetchKlineDataSina 规则一致）+ 新浪外汇 JSONP 解析
 */
const SINA_KLINE_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  Referer: "https://finance.sina.com.cn/",
};

/** normalized symbol -> 新浪 symbol */
function toSinaKlineSymbolFromNormalized(symbol) {
  const n = String(symbol || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
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
  return n;
}

async function fetchKlineDataSina(symbol, datalen = 1023) {
  const requestSymbol = toSinaKlineSymbolFromNormalized(symbol);
  if (!requestSymbol) {
    return [];
  }
  const len = Math.min(1023, Math.max(2, Number(datalen) || 1023));
  const params = new URLSearchParams({
    symbol: requestSymbol,
    scale: "240",
    ma: "no",
    datalen: String(len),
  });
  const url = `https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?${params}`;
  const response = await fetch(url, {
    cache: "no-store",
    headers: SINA_KLINE_HEADERS,
  });
  if (!response.ok) {
    throw new Error(`新浪K线失败 ${symbol}: ${response.status}`);
  }
  let payload;
  try {
    payload = await response.json();
  } catch {
    return [];
  }
  if (payload == null || !Array.isArray(payload)) {
    return [];
  }
  return payload
    .map((item) => {
      const raw = String(item?.day ?? "").trim();
      const day = raw.includes(" ")
        ? raw.replace(/\//g, "-")
        : raw.slice(0, 10).replace(/\//g, "-");
      const close = Number(String(item?.close ?? "").replace(/,/g, ""));
      return { day, close };
    })
    .filter((item) => item.day && Number.isFinite(item.close) && item.close > 0)
    .sort((a, b) => a.day.localeCompare(b.day));
}

function parseTencentPriceField(segment) {
  const t = String(segment ?? "").trim().replace(/,/g, "");
  const n = Number(t);
  return Number.isFinite(n) ? n : NaN;
}

function parseSinaForexDayKJsonp(text, varName) {
  const out = {};
  const cleaned = String(text).replace(/^\/\*[\s\S]*?\*\/\s*/m, "");
  const re = new RegExp(`var\\s+${varName}\\s*=\\s*\\("([^"]*)"\\s*\\)\\s*;?`);
  const m = cleaned.match(re);
  if (!m || !m[1]) {
    return out;
  }
  const body = m[1];
  body.split("|").forEach((rec) => {
    const parts = rec.split(",");
    if (parts.length < 5) return;
    const day = String(parts[0] || "")
      .trim()
      .slice(0, 10);
    const close = parseTencentPriceField(parts[4]);
    if (day && Number.isFinite(close) && close > 0) {
      out[day] = close;
    }
  });
  return out;
}

const SINA_FX_DAYK_URL = {
  usdcny:
    "http://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var%20USDCNY=/NewForexService.getDayKLine?symbol=fx_susdcny",
  hkdcny:
    "http://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var%20HKDCNY=/NewForexService.getDayKLine?symbol=fx_shkdcny",
};

async function fetchSinaForexDayKSeries(pair, varName) {
  const url = SINA_FX_DAYK_URL[pair];
  if (!url) return {};
  const response = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0 (compatible; stockreview-backfill/1.0)" },
  });
  if (!response.ok) {
    throw new Error(`sina fx ${pair} ${response.status}`);
  }
  const text = await response.text();
  return parseSinaForexDayKJsonp(text, varName);
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
  toSinaKlineSymbolFromNormalized,
  toDateKey,
  enumerateDays,
  validNumber,
};
