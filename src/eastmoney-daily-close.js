/**
 * 东方财富 push2his 日 K：用于长区间回填（新浪 datalen 上限 1023）。
 */
const dns = require("node:dns");
try {
  dns.setDefaultResultOrder("ipv4first");
} catch {
  /* ignore */
}

async function fetchEastmoneyKlineJson(urlStr) {
  const r = await fetch(urlStr, {
    headers: {
      Referer: "https://quote.eastmoney.com/",
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    },
    signal: AbortSignal.timeout(35_000),
  });
  if (!r.ok) {
    throw new Error(`eastmoney ${r.status}`);
  }
  return r.json();
}

/** normalizeSymbol 结果 → eastmoney secid，不支持则 null */
function normalizedToEastmoneySecid(normalized) {
  const n = String(normalized || "").toLowerCase().trim();
  if (/^sh(\d{6})$/.test(n)) {
    return `1.${n.slice(2)}`;
  }
  if (/^sz(\d{6})$/.test(n)) {
    return `0.${n.slice(2)}`;
  }
  if (/^hk(\d{5})$/.test(n)) {
    return `116.${n.slice(2)}`;
  }
  if (/^rt_hk/i.test(n)) {
    const digits = n.replace(/^rt_hk_?/i, "").replace(/\D/g, "").padStart(5, "0");
    return `116.${digits}`;
  }
  if (/^gb_/i.test(n)) {
    return `106.${n.slice(3).toUpperCase()}`;
  }
  if (/^[a-z][a-z0-9._-]*$/i.test(n)) {
    return `106.${n.toUpperCase()}`;
  }
  return null;
}

function yyyymmdd(dateStr) {
  const s = String(dateStr || "").replace(/\D/g, "");
  if (s.length >= 8) {
    return s.slice(0, 8);
  }
  return "";
}

function klinesToRows(lines) {
  if (!Array.isArray(lines) || !lines.length) {
    return [];
  }
  const out = [];
  for (const line of lines) {
    const parts = String(line).split(",");
    const dayRaw = parts[0];
    const close = Number(parts[2]);
    if (!dayRaw || !Number.isFinite(close) || close <= 0) {
      continue;
    }
    const day = String(dayRaw).slice(0, 10).replace(/\//g, "-");
    out.push({ date: day, close, source: "eastmoney" });
  }
  out.sort((a, b) => a.date.localeCompare(b.date));
  return out;
}

async function fetchEastmoneyDailyClosesOneRange(secid, begYmd, endYmd) {
  const url =
    `https://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61` +
    `&ut=fa5fd1943c7b386f172d6893dbfba10b&klt=101&fqt=1&secid=${encodeURIComponent(secid)}&beg=${begYmd}&end=${endYmd}`;
  let lastErr;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const j = await fetchEastmoneyKlineJson(url);
      const lines = j?.data?.klines;
      return klinesToRows(lines);
    } catch (e) {
      lastErr = e;
      await new Promise((r) => setTimeout(r, 350 * attempt));
    }
  }
  if (lastErr) {
    throw lastErr;
  }
  return [];
}

/**
 * 返回 { date: 'YYYY-MM-DD', close }[] 升序
 */
async function fetchEastmoneyDailyCloses(normalized, begDateKey, endDateKey) {
  const secid = normalizedToEastmoneySecid(normalized);
  if (!secid) {
    return [];
  }
  const beg = yyyymmdd(begDateKey);
  const end = yyyymmdd(endDateKey);
  if (!beg || !end) {
    return [];
  }
  try {
    return await fetchEastmoneyDailyClosesOneRange(secid, beg, end);
  } catch {
    return [];
  }
}

module.exports = {
  fetchEastmoneyDailyCloses,
  normalizedToEastmoneySecid,
};
