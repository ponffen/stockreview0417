/**
 * 腾讯 qt.gtimg.cn 实时行情（备灾 + 外汇）。
 */
const { normalizeSymbol } = require("../db");
const { toTencentQuoteKey } = require("../tencent-quote-meta");
const { normalizeQuoteTimeToBeijingBySymbol } = require("../tencent-quote-time");
const { parseQuoteTimeToDateKey } = require("../position-today-pnl");
const {
  parsePriceField,
  buildQuoteRecord,
  pickLatestQuoteTime,
} = require("./quote-common");

const QUOTE_CHUNK_SIZE = 55;
const QUOTE_FETCH_TIMEOUT_MS = 5_000;
const QUOTE_TOTAL_BUDGET_MS = Math.max(
  2000,
  Math.min(12_000, Number(process.env.QUOTE_TOTAL_BUDGET_MS || 7000)),
);
const QUOTE_CHUNK_CONCURRENCY = Math.max(1, Math.min(3, Number(process.env.QUOTE_CHUNK_CONCURRENCY || 2)));
const QUOTE_PROXY_BASE =
  String(process.env.ALIYUN_QUOTE_PROXY_BASE_URL || "").trim().replace(/\/+$/, "") ||
  "https://market-et-proxy-chbtzurmsn.cn-hangzhou.fcapp.run";

const quoteMem = new Map();
const FOREX_KEYS = ["whUSDCNY", "whHKDCNY"];

/** 腾讯 qt 对美股代码大小写敏感（须 usGOOG，不能 usgoog）；A/HK 用小写。 */
function canonicalTencentRequestKey(key) {
  const s = String(key || "").trim();
  if (!s) {
    return "";
  }
  const lower = s.toLowerCase();
  if (/^(sh|sz)\d{6}$/.test(lower) || /^hk\d{5}$/.test(lower)) {
    return lower;
  }
  if (/^wh[a-z0-9]+$/.test(lower)) {
    return `wh${lower.slice(2).toUpperCase()}`;
  }
  if (/^us[a-z0-9._-]+$/.test(lower)) {
    return `us${lower.slice(2).toUpperCase()}`;
  }
  return lower;
}

function tencentKeyLookup(key) {
  return String(key || "").trim().toLowerCase();
}

function parseTencentQuoteRecord(symbol, rawText) {
  if (!rawText || typeof rawText !== "string") {
    return null;
  }
  const parts = rawText.split("~");
  if (parts.length < 6) {
    return null;
  }
  const current = parsePriceField(parts[3]);
  const prevClose = parsePriceField(parts[4]);
  const rawTime = String(parts[30] || parts[31] || "--").trim();
  const time = normalizeQuoteTimeToBeijingBySymbol(rawTime, symbol);
  return buildQuoteRecord({
    symbol,
    name: String(parts[1] || "").trim() || symbol,
    current,
    prevClose,
    time,
    rawTime,
    source: "tencent",
  });
}

function parseTencentQuoteTextToMap(text) {
  const map = new Map();
  for (const chunk of String(text || "").split(";")) {
    const m = /v_(\w+)="([^"]*)"/.exec(chunk);
    if (!m) {
      continue;
    }
    const key = String(m[1]).toLowerCase();
    const rec = parseTencentQuoteRecord(key, m[2]);
    if (rec) {
      map.set(key, rec);
    }
  }
  return map;
}

function parseTencentQuoteTextToRawMap(text) {
  const out = new Map();
  const re = /v_([A-Za-z0-9._]+)="([^"]*)"/g;
  let m;
  while ((m = re.exec(String(text || ""))) !== null) {
    out.set(String(m[1] || "").toLowerCase(), String(m[2] || ""));
  }
  return out;
}

async function mapPool(items, limit, fn) {
  const n = items.length;
  if (!n) {
    return [];
  }
  const out = new Array(n);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, n) }, async () => {
    while (next < n) {
      const i = next++;
      out[i] = await fn(items[i], i);
    }
  });
  await Promise.all(workers);
  return out;
}

async function fetchTencentQuoteChunk(keys) {
  const url = `${QUOTE_PROXY_BASE}/api/quote/tencent?q=${encodeURIComponent(keys.join(","))}`;
  const r = await fetch(url, {
    signal: AbortSignal.timeout(QUOTE_FETCH_TIMEOUT_MS),
  });
  if (!r.ok) {
    return { ok: false, map: new Map(), rawMap: new Map() };
  }
  const text = await r.text();
  return { ok: true, map: parseTencentQuoteTextToMap(text), rawMap: parseTencentQuoteTextToRawMap(text) };
}

async function fetchTencentQuotePayloadMap(reqKeys, budgetMs = QUOTE_TOTAL_BUDGET_MS) {
  const keys = [
    ...new Set(
      (reqKeys || [])
        .map((s) => canonicalTencentRequestKey(String(s || "").trim()))
        .filter(Boolean),
    ),
  ];
  if (!keys.length) {
    return { ok: false, payloadMap: new Map(), delayed: true, source: "", error: "empty keys" };
  }
  const budget = Math.max(1000, Number(budgetMs) || QUOTE_TOTAL_BUDGET_MS);
  const payloadMap = new Map();
  let delayed = false;
  const chunks = [];
  for (let i = 0; i < keys.length; i += QUOTE_CHUNK_SIZE) {
    chunks.push(keys.slice(i, i + QUOTE_CHUNK_SIZE));
  }
  const work = (async () => {
    const parts = await mapPool(chunks, QUOTE_CHUNK_CONCURRENCY, (c) => fetchTencentQuoteChunk(c));
    for (const part of parts) {
      if (!part?.ok) {
        delayed = true;
        continue;
      }
      for (const [k, payload] of part.rawMap.entries()) {
        quoteMem.set(k, payload);
        payloadMap.set(k, payload);
      }
    }
  })();
  let timedOut = false;
  try {
    await Promise.race([
      work,
      new Promise((resolve) => {
        setTimeout(() => {
          timedOut = true;
          resolve();
        }, budget);
      }),
    ]);
  } catch {
    delayed = true;
  }
  if (timedOut) {
    delayed = true;
  }
  for (const key of keys) {
    const k = tencentKeyLookup(key);
    if (!payloadMap.has(k) && quoteMem.has(k)) {
      payloadMap.set(k, quoteMem.get(k));
      delayed = true;
    }
  }
  return { ok: payloadMap.size > 0, payloadMap, delayed, source: delayed ? "memory-cache" : "tencent" };
}

/**
 * @param {string[]} symbols 库内 symbol
 * @returns {Promise<{ ok: boolean, map: Map<string, QuoteRecord>, delayed: boolean, source: string }>}
 */
async function fetchTencentQuoteMap(symbols, budgetMs = QUOTE_TOTAL_BUDGET_MS) {
  const symList = [...new Set((symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))];
  const keyToSym = new Map();
  const requestKeys = [];
  for (const sym of symList) {
    const key = toTencentQuoteKey(sym);
    if (!key) {
      continue;
    }
    const lk = tencentKeyLookup(key);
    if (!keyToSym.has(lk)) {
      keyToSym.set(lk, sym);
      requestKeys.push(canonicalTencentRequestKey(key));
    }
  }
  const keys = requestKeys;
  if (!keys.length) {
    return { ok: false, map: new Map(), delayed: false, source: "" };
  }
  const budget = Math.max(1000, Number(budgetMs) || QUOTE_TOTAL_BUDGET_MS);
  const out = new Map();
  let delayed = false;
  const chunks = [];
  for (let i = 0; i < keys.length; i += QUOTE_CHUNK_SIZE) {
    chunks.push(keys.slice(i, i + QUOTE_CHUNK_SIZE));
  }
  const work = (async () => {
    const parts = await mapPool(chunks, QUOTE_CHUNK_CONCURRENCY, (c) => fetchTencentQuoteChunk(c));
    for (const part of parts) {
      if (!part?.ok) {
        delayed = true;
        continue;
      }
      for (const [k, rec] of part.map.entries()) {
        const sym = keyToSym.get(k);
        if (!sym || !rec) {
          continue;
        }
        quoteMem.set(k, rec);
        out.set(normalizeSymbol(sym), { ...rec, symbol: normalizeSymbol(sym), source: "tencent", delayed });
      }
    }
  })();
  let timedOut = false;
  try {
    await Promise.race([
      work,
      new Promise((resolve) => {
        setTimeout(() => {
          timedOut = true;
          resolve();
        }, budget);
      }),
    ]);
  } catch {
    delayed = true;
  }
  if (timedOut) {
    delayed = true;
  }
  for (const [k, sym] of keyToSym.entries()) {
    if (out.has(normalizeSymbol(sym))) {
      continue;
    }
    const cached = quoteMem.get(k);
    if (cached && typeof cached === "object" && cached.current > 0) {
      out.set(normalizeSymbol(sym), { ...cached, symbol: normalizeSymbol(sym), delayed: true });
      delayed = true;
    }
  }
  return { ok: out.size > 0, map: out, delayed, source: delayed ? "tencent-cache" : "tencent" };
}

function parseTencentForexFromPayload(raw) {
  if (!raw) {
    return null;
  }
  if (typeof raw === "object" && Number(raw.current) > 0) {
    return raw;
  }
  if (typeof raw !== "string") {
    return null;
  }
  const parts = raw.split("~");
  const current = parsePriceField(parts[3]);
  if (!Number.isFinite(current) || current <= 0) {
    return null;
  }
  const prevClose = parsePriceField(parts[4]);
  const time = String(parts[parts.length - 1] || parts[10] || "").trim() || "--";
  return {
    current,
    prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
    time,
  };
}

async function fetchTencentForexMap(budgetMs = QUOTE_TOTAL_BUDGET_MS) {
  const req = await fetchTencentQuotePayloadMap(FOREX_KEYS, budgetMs);
  const rates = {};
  const usd = parseTencentForexFromPayload(req.payloadMap?.get("whusdcny"));
  const hkd = parseTencentForexFromPayload(req.payloadMap?.get("whhkdcny"));
  if (usd?.current > 0) {
    rates.USD = usd.current;
  }
  if (hkd?.current > 0) {
    rates.HKD = hkd.current;
  }
  return {
    ok: Object.keys(rates).length > 0,
    rates,
    quoteTime: pickLatestQuoteTime([usd?.time, hkd?.time]),
    delayed: !!req.delayed,
    source: req.source || "tencent",
  };
}

module.exports = {
  toTencentQuoteKey,
  canonicalTencentRequestKey,
  tencentKeyLookup,
  parseTencentQuoteRecord,
  parseTencentQuoteTextToMap,
  parseTencentQuoteTextToRawMap,
  parseTencentForexFromPayload,
  fetchTencentQuotePayloadMap,
  fetchTencentQuoteMap,
  fetchTencentForexMap,
  FOREX_KEYS,
};
