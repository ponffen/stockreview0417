const path = require("node:path");
const fs = require("node:fs");
const express = require("express");
const compression = require("compression");
const cors = require("cors");
const iconv = require("iconv-lite");

const {
  fetchSinaKlineJsonFromUpstream,
  fetchSinaDailyKBatchFromUpstream,
  toSinaDailyKBatchSymbol,
} = require("./src/sina-kline-upstream");
const { fetchRemoteDailyClosesForSymbol } = require("./src/daily-close-backfill");

const MARKET_KLINE_DEFAULT_LEN = 120;
const MARKET_CACHE_TTL_MS = 30 * 60 * 1000;
const ANALYSIS_DAILY_CACHE_TTL_MS = 20 * 1000;
const DAILY_CLOSE_FOR_TRADES_CACHE_TTL_MS = 20 * 1000;
const REALTIME_PATCH_CACHE_TTL_MS = 10 * 1000;
const sinaKlineMemoryCache = new Map();
const tencentQuoteMemoryCache = new Map();
const analysisDailyMemoryCache = new Map();
const dailyCloseForTradesMemoryCache = new Map();
const realtimePatchMemoryCache = new Map();

function cacheSet(map, key, value) {
  map.set(String(key), { value, updatedAt: Date.now() });
}

function cacheGetWithTtl(map, key, ttlMs) {
  const hit = map.get(String(key));
  if (!hit) {
    return null;
  }
  if (Date.now() - Number(hit.updatedAt || 0) > ttlMs) {
    map.delete(String(key));
    return null;
  }
  return hit.value;
}

function cacheGet(map, key) {
  return cacheGetWithTtl(map, key, MARKET_CACHE_TTL_MS);
}

function clearUserScopedCache(map, userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  const prefix = `u:${uid}:`;
  for (const key of map.keys()) {
    if (String(key).startsWith(prefix)) {
      map.delete(key);
    }
  }
}

const {
  notifyLedgerMutation,
  hintDatesFromTradeMutation,
  hintDatesFromCashMutation,
  hintDatesFromImportRows,
} = require("./src/metrics-invalidate");

function invalidateDailyCloseAndAnalysisCache(userId, opts = {}) {
  clearUserScopedCache(analysisDailyMemoryCache, userId);
  clearUserScopedCache(dailyCloseForTradesMemoryCache, userId);
  clearUserScopedCache(realtimePatchMemoryCache, userId);
  notifyLedgerMutation(userId, opts);
}

function sanitizeSymbolList(input) {
  const raw = String(input || "").trim();
  if (!raw) {
    return [];
  }
  return [
    ...new Set(
      raw
        .split(",")
        .map((s) => normalizeSymbol(String(s || "")))
        .filter(Boolean)
    ),
  ];
}

function normalizeLenParam(input, fallback = MARKET_KLINE_DEFAULT_LEN) {
  const n = Number(input);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return Math.min(5000, Math.max(2, Math.floor(n)));
}

function dateKeyDaysFromToday(deltaDays) {
  const d = new Date();
  d.setDate(d.getDate() + Number(deltaDays || 0));
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function mapSinaDailySymbolToDbSymbol(symbol) {
  const canonical = toSinaDailyKBatchSymbol(symbol);
  if (!canonical) {
    return "";
  }
  if (/^cn_sh\d{6}$/i.test(canonical)) {
    return `sh${canonical.slice(-6)}`;
  }
  if (/^cn_sz\d{6}$/i.test(canonical)) {
    return `sz${canonical.slice(-6)}`;
  }
  if (/^hk_hk\d{5}$/i.test(canonical)) {
    return `hk${canonical.slice(-5)}`;
  }
  if (/^us_[A-Z0-9._-]+$/i.test(canonical)) {
    return canonical.slice(3).toLowerCase();
  }
  return "";
}

function mapDailyCloseRowsToSinaBars(rows) {
  return (rows || [])
    .map((r) => {
      const day = String(r?.date || "").slice(0, 10);
      const close = Number(r?.close);
      if (!day || !Number.isFinite(close) || close <= 0) {
        return null;
      }
      return {
        day,
        open: close,
        high: close,
        low: close,
        close,
        volume: 0,
      };
    })
    .filter(Boolean);
}

function clipRowsByRangeAndLen(rows, { len, start, end }) {
  let out = Array.isArray(rows) ? [...rows] : [];
  if (start) {
    out = out.filter((r) => String(r?.day || "").slice(0, 10) >= start);
  }
  if (end) {
    out = out.filter((r) => String(r?.day || "").slice(0, 10) <= end);
  }
  out.sort((a, b) => String(a.day).localeCompare(String(b.day)));
  if (out.length > len) {
    return out.slice(out.length - len);
  }
  return out;
}

async function loadSinaFallbackRows(requestSymbol, { len, start, end }) {
  const dbSymbol = mapSinaDailySymbolToDbSymbol(requestSymbol);
  if (dbSymbol) {
    const to = end || dateKeyDaysFromToday(0);
    const from = start || dateKeyDaysFromToday(-Math.max(30, len * 4));
    try {
      const dbRows = await getSymbolDailyCloseRange(dbSymbol, from, to);
      if (Array.isArray(dbRows) && dbRows.length) {
        return {
          rows: clipRowsByRangeAndLen(mapDailyCloseRowsToSinaBars(dbRows), { len, start, end }),
          source: "db-cache",
        };
      }
    } catch {
      // ignore DB fallback failure and continue to memory cache
    }
  }
  const memoryRows = cacheGet(sinaKlineMemoryCache, requestSymbol);
  if (Array.isArray(memoryRows) && memoryRows.length) {
    return {
      rows: clipRowsByRangeAndLen(memoryRows, { len, start, end }),
      source: "memory-cache",
    };
  }
  return { rows: [], source: "" };
}

function setDelayedHeaders(res, source) {
  res.setHeader("X-Market-Data-Delayed", "1");
  res.setHeader("X-Market-Data-Source", String(source || "cache"));
}

function parseTencentQuoteTextToMap(text) {
  const out = new Map();
  const re = /v_([A-Za-z0-9._]+)="([^"]*)"/g;
  let m;
  while ((m = re.exec(String(text || ""))) !== null) {
    out.set(String(m[1] || "").toLowerCase(), String(m[2] || ""));
  }
  return out;
}

function buildTencentQuoteTextFromMap(reqKeys, payloadMap) {
  const lines = [];
  for (const key of reqKeys) {
    const payload = payloadMap.get(String(key || "").toLowerCase());
    if (payload == null) {
      continue;
    }
    lines.push(`v_${key}="${payload}";`);
  }
  return lines.join("\n");
}

function toTencentQuoteSymbol(rawSymbol) {
  if (!rawSymbol) {
    return "";
  }
  const src = String(rawSymbol).trim();
  const raw = src.toLowerCase().replace(/\s+/g, "");
  const orig = src.replace(/\s+/g, "");
  if (/^sh\d{6}$/.test(raw) || /^sz\d{6}$/.test(raw) || /^hk\d{5}$/.test(raw)) {
    return raw;
  }
  if (/^us_[a-z0-9._-]+$/i.test(src)) {
    const base = src.replace(/^us_/i, "").replace(/\.(OQ|N)$/i, "");
    return `us${base.toUpperCase()}`;
  }
  if (/^us[A-Z0-9._-]+$/i.test(orig)) {
    const base = orig.replace(/^us/i, "").replace(/\.(OQ|N)$/i, "");
    return `us${base.toUpperCase()}`;
  }
  if (/^gb_/i.test(raw)) {
    return `us${raw.slice(3).toUpperCase()}`;
  }
  if (/^rt_hk/i.test(raw)) {
    const code = raw.replace(/^rt_hk_?/i, "").replace(/\D/g, "").padStart(5, "0");
    return `hk${code}`;
  }
  if (/^[a-z][a-z0-9._-]*$/i.test(raw)) {
    return `us${raw.toUpperCase()}`;
  }
  return "";
}

function parseTencentPriceField(segment) {
  if (segment == null) {
    return NaN;
  }
  const t = String(segment).trim().replace(/,/g, "");
  const n = Number(t);
  return Number.isFinite(n) ? n : NaN;
}

function parseQuoteTimeToDateKey(timeStr) {
  if (!timeStr || typeof timeStr !== "string") {
    return null;
  }
  const t = timeStr.trim();
  if (!t || t === "--") {
    return null;
  }
  const compact = /^(\d{4})(\d{2})(\d{2})/.exec(t.replace(/\s/g, ""));
  if (compact && compact[0].length >= 8) {
    return `${compact[1]}-${compact[2]}-${compact[3]}`;
  }
  const iso = /^(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})/.exec(t);
  if (iso) {
    return `${iso[1]}-${String(Number(iso[2])).padStart(2, "0")}-${String(Number(iso[3])).padStart(2, "0")}`;
  }
  return null;
}

function getShanghaiWallClockParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);
  const get = (type) => parts.find((p) => p.type === type)?.value;
  return {
    y: Number(get("year")),
    m: Number(get("month")),
    d: Number(get("day")),
    h: Number(get("hour")),
    min: Number(get("minute")),
  };
}

function getTradingDateKeyBy0830(baseDate = new Date()) {
  const { y, m, d, h, min } = getShanghaiWallClockParts(baseDate);
  const current = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  if (h < 8 || (h === 8 && min < 30)) {
    return addCalendarDays(current, -1);
  }
  return current;
}

function shouldCountTodayPositionPnlFromQuote(quote, now = new Date()) {
  const tradingKey = getTradingDateKeyBy0830(now);
  const quoteKey =
    (quote && quote.marketDate) ||
    (quote && quote.quoteDate) ||
    (quote && parseQuoteTimeToDateKey(quote.rawTime)) ||
    (quote && parseQuoteTimeToDateKey(quote.time)) ||
    null;
  return !!quoteKey && quoteKey === tradingKey;
}

function quoteTimeSortKey(timeStr) {
  if (!timeStr || typeof timeStr !== "string") {
    return 0;
  }
  const raw = String(timeStr).trim();
  if (!raw || raw === "--") {
    return 0;
  }
  const digits = raw.replace(/\D/g, "");
  if (digits.length >= 14) {
    return Number(digits.slice(0, 14)) || 0;
  }
  if (digits.length >= 8) {
    return Number(`${digits.slice(0, 8)}000000`) || 0;
  }
  return 0;
}

function pickLatestQuoteTime(times) {
  const list = Array.isArray(times) ? times : [];
  let best = "";
  let bestKey = 0;
  for (const item of list) {
    const time = String(item || "").trim();
    const key = quoteTimeSortKey(time);
    if (key > bestKey) {
      best = time;
      bestKey = key;
    }
  }
  return best || "--";
}

function parseTencentQuoteRecord(symbol, rawText) {
  if (!rawText || typeof rawText !== "string") {
    return null;
  }
  const parts = rawText.split("~");
  if (parts.length < 6) {
    return null;
  }
  const name = String(parts[1] || "").trim() || symbol;
  const current = parseTencentPriceField(parts[3]);
  const prevClose = parseTencentPriceField(parts[4]);
  const rawTime = String(parts[30] || parts[31] || "--").trim();
  const time = rawTime;
  const marketDate = parseQuoteTimeToDateKey(rawTime) || parseQuoteTimeToDateKey(time);
  if (!Number.isFinite(current) || current <= 0) {
    return null;
  }
  return {
    name,
    current,
    prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
    time: time || "--",
    rawTime: rawTime || "--",
    marketDate,
    quoteDate: marketDate,
  };
}

function parseTencentForexQuotePayload(rawText) {
  if (!rawText || typeof rawText !== "string") {
    return null;
  }
  const parts = rawText.split("~");
  if (parts.length < 4) {
    return null;
  }
  const current = parseTencentPriceField(parts[3]);
  const prevClose = parseTencentPriceField(parts[4]);
  const time = String(parts[parts.length - 1] || parts[10] || "").trim() || "--";
  if (!Number.isFinite(current) || current <= 0) {
    return null;
  }
  return {
    current,
    prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
    time,
  };
}

async function fetchSinaDailyBatchWithFallback(inputSymbols, options = {}) {
  const start = options.start != null ? String(options.start) : "";
  const end = options.end != null ? String(options.end) : "";
  const asc = options.asc != null ? String(options.asc) : "0";
  const len = normalizeLenParam(options.len, MARKET_KLINE_DEFAULT_LEN);
  const symbols = [...new Set((inputSymbols || []).map((s) => toSinaDailyKBatchSymbol(s)).filter(Boolean))];
  if (!symbols.length) {
    return { ok: false, data: {}, delayed: false, source: "", error: "invalid symbol mapping" };
  }
  let payload = {};
  let upstreamOk = false;
  try {
    const result = await fetchSinaDailyKBatchFromUpstream(symbols, { len, asc, start, end });
    if (result.ok) {
      upstreamOk = true;
      payload = result.data || {};
      for (const sym of symbols) {
        const rows = Array.isArray(payload?.[sym]) ? payload[sym] : [];
        if (rows.length) {
          cacheSet(sinaKlineMemoryCache, sym, rows);
        }
      }
    }
  } catch {
    // ignore and continue to cache fallback
  }

  const delayedSources = new Set();
  for (const sym of symbols) {
    const rows = Array.isArray(payload?.[sym]) ? payload[sym] : [];
    if (rows.length) {
      continue;
    }
    const fallback = await loadSinaFallbackRows(sym, { len, start, end });
    if (fallback.rows.length) {
      payload[sym] = fallback.rows;
      delayedSources.add(fallback.source || "cache");
    }
  }

  if (!Object.keys(payload).length) {
    return { ok: false, data: {}, delayed: false, source: "", error: "sina kline failed and no cache" };
  }
  const delayed = delayedSources.size > 0 || !upstreamOk;
  return {
    ok: true,
    data: payload,
    delayed,
    source: delayed ? [...delayedSources].join(",") || "cache" : "",
    error: "",
  };
}

async function fetchTencentQuotePayloadMap(reqKeys) {
  const keys = [...new Set((reqKeys || []).map((s) => String(s || "").trim()).filter(Boolean))];
  if (!keys.length) {
    return { ok: false, payloadMap: new Map(), delayed: false, source: "", error: "empty q keys", missingKeys: [] };
  }
  let payloadMap = new Map();
  let usedCache = false;
  let upstreamError = "";
  try {
    const url = `https://qt.gtimg.cn/q=${encodeURIComponent(keys.join(","))}&_=${Date.now()}`;
    const r = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; stockreview/1.0)" },
      signal: AbortSignal.timeout(20_000),
    });
    if (r.ok) {
      const buf = Buffer.from(await r.arrayBuffer());
      const text = iconv.decode(buf, "gbk");
      const liveMap = parseTencentQuoteTextToMap(text);
      for (const [k, payload] of liveMap.entries()) {
        cacheSet(tencentQuoteMemoryCache, k, payload);
      }
      for (const key of keys) {
        const k = String(key).toLowerCase();
        if (liveMap.has(k)) {
          payloadMap.set(k, liveMap.get(k));
          continue;
        }
        const cached = cacheGet(tencentQuoteMemoryCache, k);
        if (cached != null) {
          payloadMap.set(k, cached);
          usedCache = true;
        }
      }
    } else {
      upstreamError = `tencent ${r.status}`;
      for (const key of keys) {
        const cached = cacheGet(tencentQuoteMemoryCache, String(key).toLowerCase());
        if (cached != null) {
          payloadMap.set(String(key).toLowerCase(), cached);
          usedCache = true;
        }
      }
    }
  } catch (error) {
    upstreamError = error?.message || "tencent quote failed";
    for (const key of keys) {
      const cached = cacheGet(tencentQuoteMemoryCache, String(key).toLowerCase());
      if (cached != null) {
        payloadMap.set(String(key).toLowerCase(), cached);
        usedCache = true;
      }
    }
  }
  if (!payloadMap.size) {
    return {
      ok: false,
      payloadMap,
      delayed: false,
      source: "",
      error: upstreamError || "tencent quote failed and no cache",
      missingKeys: keys,
    };
  }
  const missingKeys = keys.filter((key) => !payloadMap.has(String(key).toLowerCase()));
  return {
    ok: true,
    payloadMap,
    delayed: usedCache,
    source: usedCache ? "memory-cache" : "",
    error: "",
    missingKeys,
  };
}

const PROBE_DEFAULT_TIMEOUT_MS = 12_000;
const PROBE_MAX_TIMEOUT_MS = 30_000;
const PROBE_DEFAULT_ROUNDS = 1;
const PROBE_MAX_ROUNDS = 5;
const DEFAULT_ALIYUN_PROBE_URL =
  "https://market-oxy-http-market-proxy-pbftovdfne.cn-hangzhou.fcapp.run/api/probe/upstream";

function parsePositiveInt(input, fallback, min, max) {
  const n = Number(input);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  const v = Math.floor(n);
  return Math.min(max, Math.max(min, v));
}

function summarizeSinaProbeBody(text) {
  try {
    const payload = JSON.parse(String(text || ""));
    const root = payload?.result?.data || payload?.data || {};
    const symbols = Object.keys(root || {});
    let totalBars = 0;
    for (const key of symbols) {
      const rows = Array.isArray(root?.[key]) ? root[key] : [];
      totalBars += rows.length;
    }
    return {
      parseOk: true,
      symbols,
      totalBars,
    };
  } catch (error) {
    return {
      parseOk: false,
      parseError: error?.message || "invalid json",
    };
  }
}

function summarizeTencentProbeBody(text) {
  const map = parseTencentQuoteTextToMap(text);
  return {
    parseOk: map.size > 0,
    records: map.size,
    keys: [...map.keys()].slice(0, 12),
  };
}

async function runUpstreamHttpProbe({ name, url, timeoutMs, parser }) {
  const startedAt = Date.now();
  const result = {
    name,
    url,
    timeoutMs,
    startedAt: new Date(startedAt).toISOString(),
    elapsedMs: 0,
    ok: false,
    status: 0,
    statusText: "",
    error: "",
    response: {
      contentType: "",
      contentLength: 0,
      preview: "",
      summary: {},
    },
  };
  try {
    const response = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; stockreview-probe/1.0)" },
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await response.text();
    result.ok = response.ok;
    result.status = Number(response.status || 0);
    result.statusText = String(response.statusText || "");
    result.response.contentType = String(response.headers.get("content-type") || "");
    result.response.contentLength = Buffer.byteLength(text, "utf8");
    result.response.preview = String(text || "").slice(0, 220);
    result.response.summary = typeof parser === "function" ? parser(text) : {};
  } catch (error) {
    result.error = error?.message || String(error);
  } finally {
    result.elapsedMs = Date.now() - startedAt;
  }
  return result;
}

async function runUpstreamProbeRound({ timeoutMs, sinaUrl, tencentUrl }) {
  const [sina, tencent] = await Promise.all([
    runUpstreamHttpProbe({
      name: "sina",
      url: sinaUrl,
      timeoutMs,
      parser: summarizeSinaProbeBody,
    }),
    runUpstreamHttpProbe({
      name: "tencent",
      url: tencentUrl,
      timeoutMs,
      parser: summarizeTencentProbeBody,
    }),
  ]);
  return { sina, tencent };
}

/**
 * 新浪 DailyK_Batch 代理：优先上游实时，失败时回退 DB/内存缓存并显式标记延迟数据。
 */
async function handleSinaKlineProxy(req, res) {
  const symbol = req.query.symbol != null ? String(req.query.symbol) : "";
  const symbolsRaw = req.query.symbols != null ? String(req.query.symbols) : "";
  const datalen = req.query.datalen != null ? String(req.query.datalen) : String(MARKET_KLINE_DEFAULT_LEN);
  const lenRaw = req.query.len != null ? String(req.query.len) : datalen;
  const asc = req.query.asc != null ? String(req.query.asc) : "0";
  const start = req.query.start != null ? String(req.query.start) : "";
  const end = req.query.end != null ? String(req.query.end) : "";
  const inputSymbols = (symbolsRaw ? symbolsRaw.split(",") : [symbol])
    .map((s) => String(s || "").trim())
    .filter(Boolean);
  if (!inputSymbols.length || inputSymbols.some((sym) => sym.length > 64 || !/^[a-zA-Z0-9._-]+$/.test(sym))) {
    res.status(400).json({ ok: false, error: "invalid symbol" });
    return;
  }
  if (!/^\d+$/.test(lenRaw) || !/^[01]$/.test(asc)) {
    res.status(400).json({ ok: false, error: "invalid len or asc" });
    return;
  }
  if ((start && !/^\d{4}-\d{2}-\d{2}$/.test(start)) || (end && !/^\d{4}-\d{2}-\d{2}$/.test(end))) {
    res.status(400).json({ ok: false, error: "invalid start or end" });
    return;
  }
  const symbols = [...new Set(inputSymbols.map((s) => toSinaDailyKBatchSymbol(s)).filter(Boolean))];
  if (!symbols.length) {
    res.status(400).json({ ok: false, error: "invalid symbol mapping" });
    return;
  }
  const isBatch = symbolsRaw || symbols.length > 1;

  const result = await fetchSinaDailyBatchWithFallback(symbols, {
    len: lenRaw,
    asc,
    start,
    end,
  });
  if (!result.ok) {
    res.status(502).json({ ok: false, error: result.error || "sina kline failed" });
    return;
  }
  if (result.delayed) {
    setDelayedHeaders(res, result.source);
  }
  res.setHeader("Cache-Control", "no-store");
  if (isBatch) {
    res.json(result.data || {});
  } else {
    res.json((result.data || {})[symbols[0]] || []);
  }
}

function ensureDataDir() {
  if (process.env.VERCEL) return; // Serverless 环境文件系统只读，直接跳过
  const dataDir = path.join(__dirname, "data");
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
}

ensureDataDir();

/**
 * 浏览器静态根目录。本地优先仓库根（与 server.js 同级的 index.html）；Vercel 上由 `npm run build`
 * 把资源复制到 api/public/ 并由 includeFiles 打进函数包，避免 sendFile ENOENT。
 */
function resolveWebStaticRoot() {
  const cands = [__dirname, path.join(__dirname, ".."), path.join(__dirname, "api", "public")];
  for (const dir of cands) {
    if (fs.existsSync(path.join(dir, "app.js")) && fs.existsSync(path.join(dir, "index.html"))) {
      return dir;
    }
  }
  return __dirname;
}

const WEB_ROOT = resolveWebStaticRoot();
const STATIC_ASSET_LONG_CACHE =
  process.env.VERCEL === "1" ||
  String(process.env.VERCEL || "").toLowerCase() === "true" ||
  process.env.NODE_ENV === "production";

const {
  DEFAULT_SETTINGS,
  normalizeSymbol,
  normalizeTrade,
  getTrades,
  getTradesForSymbol,
  getTradesPageForSymbol,
  getTradesPage,
  upsertTrade,
  importTrades,
  getTradeByIdForUser,
  deleteTradeById,
  getCashTransfers,
  getCashTransfersPage,
  upsertCashTransfer,
  importCashTransfers,
  getCashTransferByIdForUser,
  deleteCashTransferById,
  getAccounts,
  getSettings,
  setSettings,
  insertCronJobRun,
  listCronJobRuns,
  getSymbolDailyPnl,
  upsertSymbolDailyPnlBatch,
  getAnalysisDailySnapshots,
  upsertSymbolDailyCloseBatch,
  getSymbolDailyCloseRange,
  getLatestAnalysisSnapshotDate,
  createSymbolNameMapTableNow,
  getTradeWindowForDailyClose,
  getSnapshotWatermark,
  setSnapshotWatermark,
  verifyUserLogin,
  createRegisteredUser,
  getAuthSessionUserPayload,
  getUserValidUntil,
  updateUserPassword,
  verifyUserPasswordById,
  getUserPhone,
  isValidPhone,
  isValidPasswordDigits,
  getUserCommunityRow,
  updateUserCommunityProfile,
  setCommunityFollow,
  removeCommunityFollow,
  isCommunityFollowing,
  pingDatabase,
} = require("./src/db");
const { isSubscriptionExpired } = require("./src/user-subscription");
const { runDailyFreeze, resolveFrozenDate, freezeUserToDate } = require("./src/eod-freeze-service");

const {
  maskPhone,
  displayNameForUser,
  getLeaderboard,
  getPublicProfileDetail,
  getPublicTradesPage,
  getFollowingCards,
  getFeedTrades,
  enrichLeaderboardPayloadWithSymbolNames,
  enrichLeaderboardPayloadWithViewer,
  enrichTopPositionsOnCards,
} = require("./src/community-service");
const {
  ensureSymbolNameMapOnNewTrade,
  enrichTradesWithSymbolNames,
} = require("./src/symbol-name-resolve");
const { readUserIdFromRequest, setSessionCookie, clearSessionCookie } = require("./src/auth-session");
const { parseSinaSuggestText, suggestLineToItem, publicSearchResults } = require("./src/sina-suggest");
const { runDailyCloseSync } = require("./src/daily-close-sync-service");
const { todayProfitCnyForHolding } = require("./src/position-today-pnl");
const { liveDateKeyShanghai } = require("./src/metrics/trading-calendar");
const {
  getMetricsHomeBundle,
  getMetricsPublicHomeBundle,
  getMetricsPublicAnalysisBundle,
  getMetricsAnalysisBundle,
  getMetricsStockRecordBundle,
  getMetricsPublicStockRecordBundle,
} = require("./src/metrics-api-service");

function sendMetricsJson(res, data) {
  res.setHeader("Cache-Control", "no-store");
  res.json({ ok: true, data });
}

async function assertPublicMetricsTarget(viewerUserId, targetId) {
  const tid = String(targetId || "").trim();
  const vid = String(viewerUserId || "").trim();
  if (!tid) {
    return { ok: false, status: 400, error: "invalid target" };
  }
  if (vid === tid) {
    return { ok: true, userId: tid };
  }
  const row = await getUserCommunityRow(tid);
  if (!row) {
    return { ok: false, status: 404, error: "用户不存在" };
  }
  if (!Number(row.community_public)) {
    return { ok: false, status: 403, error: "未公开持仓" };
  }
  return { ok: true, userId: tid };
}

const app = express();
const PORT = Number(process.env.PORT || 3030);

// 诊断中间件：在最前面打印进入与响应耗时。build=v4 版本戳（含 url 兜底还原）。
app.use((req, res, next) => {
  const started = Date.now();
  // 兜底：Vercel 的 legacy routes 会把 /api/xxx 的 URL 改写成 /api 甚至 /
  // 导致 Express 落到 SPA 兜底。如果 req.url 看上去被剥短了，优先用 Vercel 透传的 header 还原。
  try {
    const original =
      req.headers["x-forwarded-uri"] ||
      req.headers["x-original-url"] ||
      req.headers["x-matched-path"];
    const reqPathOnly = String(req.url || "").split("?")[0] || "/";
    const looksStripped =
      reqPathOnly === "/" || reqPathOnly === "/api" || reqPathOnly === "/api/" || reqPathOnly === "/api/index";
    const origPathOnly = typeof original === "string" ? String(original.split("?")[0] || "/") || "/" : "";
    if (
      looksStripped &&
      typeof original === "string" &&
      original.startsWith("/api/") &&
      origPathOnly !== "/api" &&
      origPathOnly !== "/api/"
    ) {
      console.log("[req.url-restore] from=%s to=%s", req.url, original);
      req.url = original;
    }
  } catch (_) {}
  console.log(
    "[req.in] build=v4 method=%s url=%s x-matched-path=%s x-forwarded-uri=%s",
    req.method,
    req.url,
    req.headers["x-matched-path"] || "-",
    req.headers["x-forwarded-uri"] || req.headers["x-original-url"] || "-"
  );
  res.on("finish", () => {
    console.log(
      "[req.out] build=v4 method=%s url=%s status=%d ms=%d",
      req.method,
      req.url,
      res.statusCode,
      Date.now() - started
    );
  });
  res.on("close", () => {
    if (!res.writableEnded) {
      console.log(
        "[req.closed-early] build=v4 method=%s url=%s ms=%d",
        req.method,
        req.url,
        Date.now() - started
      );
    }
  });
  next();
});

app.use(compression());
app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: "5mb" }));

// 强力禁止 Vercel CDN/浏览器缓存任何 API 响应，防止串号和数据泄露
app.use("/api", (req, res, next) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  next();
});

async function requireAuth(req, res, next) {
  try {
    const userId = readUserIdFromRequest(req);
    if (!userId) {
      res.status(401).json({ ok: false, error: "请先登录" });
      return;
    }
    const validUntil = await getUserValidUntil(userId);
    if (isSubscriptionExpired(validUntil)) {
      res.status(403).json({ ok: false, error: "免费使用已结束", code: "subscription_expired" });
      return;
    }
    req.userId = userId;
    next();
  } catch (error) {
    next(error);
  }
}

function parseBooleanInput(input, fallback = false) {
  if (input == null) {
    return fallback;
  }
  const v = String(input).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(v)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(v)) {
    return false;
  }
  return fallback;
}

function extractBearerToken(req) {
  const auth = String(req.headers?.authorization || "").trim();
  if (!auth.toLowerCase().startsWith("bearer ")) {
    return "";
  }
  return auth.slice(7).trim();
}

function isCronAuthorized(req) {
  const cronHeader = req.headers?.["x-vercel-cron"];
  if (cronHeader != null) {
    return true;
  }
  const configuredSecret = String(process.env.CRON_SECRET || process.env.VERCEL_CRON_SECRET || "").trim();
  if (!configuredSecret) {
    return false;
  }
  const bearer = extractBearerToken(req);
  const queryToken = String(req.query?.token || req.body?.token || "").trim();
  return bearer === configuredSecret || queryToken === configuredSecret;
}

async function collectLiveSymbolsForUser(userId, accountId = "all") {
  const allTrades = await getTrades(userId);
  const wantedAccount = String(accountId || "all").trim() || "all";
  const trades =
    wantedAccount === "all" ? allTrades : allTrades.filter((trade) => String(trade.accountId || "default") === wantedAccount);
  const holdings = new Map();
  for (const trade of trades.sort((a, b) => {
    const ad = new Date(a.date).getTime();
    const bd = new Date(b.date).getTime();
    if (ad !== bd) {
      return ad - bd;
    }
    return Number(a.createdAt || 0) - Number(b.createdAt || 0);
  })) {
    const symbol = normalizeSymbol(trade.symbol);
    if (!symbol) {
      continue;
    }
    const current = holdings.get(symbol) || 0;
    holdings.set(symbol, current + (trade.side === "buy" ? Number(trade.quantity || 0) : -Number(trade.quantity || 0)));
  }
  return [...holdings.entries()]
    .filter(([, qty]) => Math.abs(Number(qty) || 0) > 1e-6)
    .map(([symbol]) => symbol);
}

const {
  handleWellKnownProtectedResource,
  handleWellKnownAuthServer,
  handleOAuthAuthorize,
  handleOAuthToken,
} = require("./src/mcp/oauth-handlers");
const { handleMcpRequest } = require("./src/mcp/protocol");
const { handleConnectionStatus, handleConnectionRevoke } = require("./src/mcp/connection-api");

app.get("/.well-known/oauth-protected-resource", handleWellKnownProtectedResource);
app.get("/.well-known/oauth-authorization-server", handleWellKnownAuthServer);
app.get("/oauth/authorize", (req, res) => void handleOAuthAuthorize(req, res));
app.post("/oauth/token", (req, res) => void handleOAuthToken(req, res));
app.get("/mcp", (req, res) => void handleMcpRequest(req, res));
app.post("/mcp", (req, res) => void handleMcpRequest(req, res));
app.get("/api/mcp/connection-status", (req, res) => void handleConnectionStatus(req, res));
app.delete("/api/mcp/connection", (req, res) => void handleConnectionRevoke(req, res));

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, node: process.version });
});

/**
 * 上游可达性探针：
 * - 默认重定向至阿里云 FC 探针（避免 Vercel 下载附件或超时，浏览器可直接展示 JSON）。
 * - 若需强制探测当前 Vercel 运行时，请加 query: target=vercel。
 * 浏览器可直接访问：
 * /api/probe/upstream
 * /api/probe/upstream?rounds=3&timeoutMs=15000&len=120&target=vercel
 */
app.get("/api/probe/upstream", async (req, res) => {
  const target = String(req.query.target || "aliyun").trim().toLowerCase();
  if (target !== "vercel") {
    const upstreamBase = String(process.env.ALIYUN_PROBE_UPSTREAM_URL || DEFAULT_ALIYUN_PROBE_URL).trim();
    const qs = new URLSearchParams();
    Object.entries(req.query || {}).forEach(([k, v]) => {
      if (k === "target" || v == null) {
        return;
      }
      if (Array.isArray(v)) {
        v.forEach((item) => qs.append(k, String(item)));
      } else {
        qs.set(k, String(v));
      }
    });
    const redirectUrl = qs.toString() ? `${upstreamBase}?${qs.toString()}` : upstreamBase;
    res.setHeader("Cache-Control", "no-store");
    res.redirect(307, redirectUrl);
    return;
  }

  const rounds = parsePositiveInt(req.query.rounds, PROBE_DEFAULT_ROUNDS, 1, PROBE_MAX_ROUNDS);
  const timeoutMs = parsePositiveInt(
    req.query.timeoutMs,
    PROBE_DEFAULT_TIMEOUT_MS,
    1_000,
    PROBE_MAX_TIMEOUT_MS
  );
  const len = parsePositiveInt(req.query.len, MARKET_KLINE_DEFAULT_LEN, 2, 5000);
  const sinaSymbolsRaw =
    req.query.sinaSymbols != null ? String(req.query.sinaSymbols) : "us_TSM,cn_sh601899,fx_USDCNY";
  const tencentQRaw =
    req.query.tencentQ != null ? String(req.query.tencentQ) : "usTSM,sh601899,whUSDCNY";
  const sinaSymbols = [...new Set(sinaSymbolsRaw.split(",").map((s) => String(s || "").trim()).filter(Boolean))];
  const tencentQ = [...new Set(tencentQRaw.split(",").map((s) => String(s || "").trim()).filter(Boolean))];
  if (!sinaSymbols.length || !tencentQ.length) {
    res.status(400).json({ ok: false, error: "sinaSymbols/tencentQ required" });
    return;
  }

  const sinaUrl =
    "https://quotes.sina.cn/hq/api/openapi.php/MarketCenterService.getDailyK_Batch?" +
    new URLSearchParams({
      symbols: sinaSymbols.join(","),
      len: String(len),
      asc: "0",
    }).toString();
  const tencentUrl = `https://qt.gtimg.cn/q=${encodeURIComponent(tencentQ.join(","))}`;

  const probeStartedAt = Date.now();
  const roundsOut = [];
  for (let i = 0; i < rounds; i += 1) {
    const roundStartedAt = Date.now();
    const upstream = await runUpstreamProbeRound({ timeoutMs, sinaUrl, tencentUrl });
    roundsOut.push({
      round: i + 1,
      startedAt: new Date(roundStartedAt).toISOString(),
      elapsedMs: Date.now() - roundStartedAt,
      upstream,
    });
  }

  const all = roundsOut.flatMap((r) => [r.upstream.sina, r.upstream.tencent]);
  const okCount = all.filter((x) => x.ok).length;
  const timeoutCount = all.filter((x) => /timed out/i.test(String(x.error || ""))).length;
  const errorCount = all.length - okCount;
  const avgElapsedMs =
    all.length > 0
      ? Math.round(all.reduce((sum, x) => sum + (Number(x.elapsedMs) || 0), 0) / all.length)
      : 0;

  res.setHeader("Cache-Control", "no-store");
  res.json({
    ok: true,
    message: "Runs from current server runtime, not browser network.",
    runtime: {
      node: process.version,
      env: process.env.VERCEL ? "vercel" : "local",
      vercelRegion: process.env.VERCEL_REGION || null,
      vercelUrl: process.env.VERCEL_URL || null,
      requestHost: req.headers.host || null,
      xVercelId: req.headers["x-vercel-id"] || null,
      ts: Date.now(),
    },
    config: {
      rounds,
      timeoutMs,
      len,
      sinaSymbols,
      tencentQ,
    },
    summary: {
      totalChecks: all.length,
      okCount,
      errorCount,
      timeoutCount,
      avgElapsedMs,
      totalElapsedMs: Date.now() - probeStartedAt,
    },
    rounds: roundsOut,
  });
});

// 部署版本自证端点：直接硬编码当前构建号，便于在浏览器判断 Vercel 是否跑的是最新代码
app.get("/api/diag/v3", (_req, res) => {
  res.json({
    ok: true,
    build: "v3",
    commit: "1f6d9ce-plus-diag",
    node: process.version,
    env: process.env.VERCEL ? "vercel" : "local",
    ts: Date.now(),
  });
});

/** 数据库连通性探活（会走 initPool + 一次只读 SQL；勿对外暴露敏感信息） */
app.get("/api/health/db", async (_req, res) => {
  try {
    const row = await pingDatabase();
    res.setHeader("Cache-Control", "no-store");
    res.json({
      ok: true,
      database: row.db != null ? String(row.db) : null,
      schema: row.schema != null ? String(row.schema) : null,
      serverTime: row.server_time != null ? String(row.server_time) : null,
    });
  } catch (error) {
    res.setHeader("Cache-Control", "no-store");
    res.status(503).json({
      ok: false,
      error: error?.message || "database unreachable",
    });
  }
});

app.get("/api/auth/me", async (req, res) => {
  try {
    console.log("[auth.me] start");
    const userId = readUserIdFromRequest(req);
    if (!userId) {
      res.status(401).json({ ok: false, error: "未登录" });
      return;
    }
    const user = await getAuthSessionUserPayload(userId);
    console.log("[auth.me] ok userId=", userId);
    res.json({
      ok: true,
      user,
    });
  } catch (error) {
    console.error("[auth.me] error:", error?.message || error);
    res.status(500).json({
      ok: false,
      error: error?.message || "auth/me failed",
      code: error?.code || null,
    });
  }
});

app.patch("/api/me/community-profile", requireAuth, async (req, res) => {
  try {
    const body = req.body || {};
    const u = await updateUserCommunityProfile(req.userId, {
      nickname: body.nickname,
      communityPublic: body.communityPublic,
    });
    res.json({
      ok: true,
      profile: {
        nickname: u.nickname != null && String(u.nickname).trim() ? String(u.nickname).trim() : null,
        communityPublic: !!Number(u.community_public),
        displayName: displayNameForUser(u),
      },
    });
  } catch (error) {
    const msg = error?.message || "更新失败";
    if (msg.includes("nickname taken")) {
      res.status(409).json({ ok: false, error: "昵称已被占用" });
      return;
    }
    if (msg.includes("too long")) {
      res.status(400).json({ ok: false, error: "昵称最长 20 个字符" });
      return;
    }
    res.status(400).json({ ok: false, error: msg });
  }
});

app.get("/api/community/leaderboard", requireAuth, async (req, res) => {
  try {
    const data = await getLeaderboard();
    await enrichLeaderboardPayloadWithViewer(data, req.userId);
    await enrichLeaderboardPayloadWithSymbolNames(data);
    res.json({ ok: true, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "leaderboard failed" });
  }
});

app.get("/api/community/following", requireAuth, async (req, res) => {
  try {
    const cards = await getFollowingCards(req.userId);
    await enrichTopPositionsOnCards(cards);
    res.json({ ok: true, data: cards });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "following failed" });
  }
});

app.get("/api/community/feed", requireAuth, async (req, res) => {
  try {
    const rows = await getFeedTrades(req.userId);
    res.json({ ok: true, data: rows });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "feed failed" });
  }
});

app.get("/api/community/users/:targetId/profile", requireAuth, async (req, res) => {
  try {
    const targetId = String(req.params.targetId || "").trim();
    const detail = await getPublicProfileDetail(req.userId, targetId);
    if (detail.error === "unauthorized") {
      res.status(401).json({ ok: false, error: "未登录" });
      return;
    }
    if (detail.error === "hidden") {
      res.status(404).json({ ok: false, error: "用户未公开或不可见" });
      return;
    }
    res.set("Cache-Control", "no-store");
    res.json({ ok: true, data: detail });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "profile failed" });
  }
});

app.get("/api/public/:targetId/trades", requireAuth, async (req, res) => {
  try {
    const targetId = String(req.params.targetId || "").trim();
    const symbol = req.query.symbol != null ? String(req.query.symbol).trim() : "";
    const accountId = req.query.account_id != null ? String(req.query.account_id).trim() : "";
    const data = await getPublicTradesPage(req.userId, targetId, {
      symbol: symbol || undefined,
      accountId: accountId || undefined,
      limit: req.query.limit,
      offset: req.query.offset,
    });
    if (data.error === "unauthorized") {
      res.status(401).json({ ok: false, error: "未登录" });
      return;
    }
    if (data.error === "hidden") {
      res.status(404).json({ ok: false, error: "用户未公开或不可见" });
      return;
    }
    res.set("Cache-Control", "no-store");
    res.json({ ok: true, data: data.data, pagination: data.pagination });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "public trades failed" });
  }
});

app.post("/api/community/follow/:targetId", requireAuth, async (req, res) => {
  const targetId = String(req.params.targetId || "").trim();
  if (!targetId) {
    res.status(400).json({ ok: false, error: "invalid target" });
    return;
  }
  await setCommunityFollow(req.userId, targetId);
  res.json({ ok: true, following: await isCommunityFollowing(req.userId, targetId) });
});

app.delete("/api/community/follow/:targetId", requireAuth, async (req, res) => {
  const targetId = String(req.params.targetId || "").trim();
  await removeCommunityFollow(req.userId, targetId);
  res.json({ ok: true });
});

const { REGISTER_INVITE_CODE } = require("./src/register-invite");

app.post("/api/auth/register", async (req, res) => {
  try {
    const phone = req.body?.phone != null ? String(req.body.phone).trim() : "";
    const password = req.body?.password != null ? String(req.body.password) : "";
    const inviteCode =
      req.body?.inviteCode != null ? String(req.body.inviteCode).trim() : "";
    if (!isValidPhone(phone)) {
      res.status(400).json({ ok: false, error: "请输入 11 位手机号" });
      return;
    }
    if (!isValidPasswordDigits(password)) {
      res.status(400).json({ ok: false, error: "密码为不少于 6 位的数字" });
      return;
    }
    if (inviteCode !== REGISTER_INVITE_CODE) {
      res.status(400).json({ ok: false, error: "邀请码错误" });
      return;
    }
    const u = await createRegisteredUser(phone, password);
    setSessionCookie(res, u.id);
    const user = await getAuthSessionUserPayload(u.id);
    res.json({ ok: true, user });
  } catch (error) {
    const msg = error?.message || "注册失败";
    if (msg.includes("already registered")) {
      res.status(409).json({ ok: false, error: "该手机号已注册" });
      return;
    }
    res.status(400).json({ ok: false, error: msg });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    console.log("[auth.login] start");
    const phone = req.body?.phone != null ? String(req.body.phone).trim() : "";
    const password = req.body?.password != null ? String(req.body.password) : "";
    const u = await verifyUserLogin(phone, password);
    if (!u) {
      res.status(401).json({ ok: false, error: "手机号或密码错误" });
      return;
    }
    setSessionCookie(res, u.id);
    console.log("[auth.login] ok userId=", u.id);
    const user = await getAuthSessionUserPayload(u.id);
    res.json({ ok: true, user });
  } catch (error) {
    console.error("[auth.login] error:", error?.message || error);
    res.status(500).json({
      ok: false,
      error: error?.message || "auth/login failed",
      code: error?.code || null,
    });
  }
});

app.post("/api/auth/logout", (_req, res) => {
  clearSessionCookie(res);
  res.json({ ok: true });
});

app.post("/api/auth/password", requireAuth, async (req, res) => {
  try {
    const oldPw = req.body?.oldPassword != null ? String(req.body.oldPassword) : "";
    const newPw = req.body?.newPassword != null ? String(req.body.newPassword) : "";
    if (!isValidPasswordDigits(newPw)) {
      res.status(400).json({ ok: false, error: "新密码为不少于 6 位的数字" });
      return;
    }
    if (!(await verifyUserPasswordById(req.userId, oldPw))) {
      res.status(400).json({ ok: false, error: "原密码错误" });
      return;
    }
    await updateUserPassword(req.userId, newPw);
    res.json({ ok: true });
  } catch (error) {
    res.status(400).json({ ok: false, error: error?.message || "修改失败" });
  }
});

/** 可选子路径部署，如 BASE_PATH=myapp → /myapp/api/sina-kline */
const PUBLIC_BASE_PATH = String(process.env.BASE_PATH || "")
  .trim()
  .replace(/^\/+|\/+$/g, "");
const escapePathRegex = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** 新浪 K 线代理：先注册字面路径（避免个别环境 RegExp 未命中仍落 404），再补正则变体与可选子路径 */
app.get("/api/sina-kline", handleSinaKlineProxy);
app.get("/api/sina_kline", handleSinaKlineProxy);
app.get("/api/sinakline", handleSinaKlineProxy);
app.get("/api/sina-kline/", handleSinaKlineProxy);
app.get("/api/sina_kline/", handleSinaKlineProxy);
app.get(/^\/api\/sina([-_])?kline\/?$/i, handleSinaKlineProxy);
if (PUBLIC_BASE_PATH) {
  const pb = escapePathRegex(PUBLIC_BASE_PATH);
  app.get(`/${PUBLIC_BASE_PATH}/api/sina-kline`, handleSinaKlineProxy);
  app.get(`/${PUBLIC_BASE_PATH}/api/sina_kline`, handleSinaKlineProxy);
  app.get(
    new RegExp(`^/${pb}/api/sina([-_])?kline/?$`, "i"),
    handleSinaKlineProxy
  );
}

app.post("/api/admin/create-symbol-name-map", requireAuth, async (_req, res) => {
  try {
    const created = await createSymbolNameMapTableNow();
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, created });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "create symbol_name_map failed" });
  }
});

function metricsAccountIdFromQuery(query) {
  return String(query?.account_id || query?.accountScope || "all").trim() || "all";
}

function stockRecordChartQueryFromQuery(query) {
  const range = String(query?.range || query?.chartRange || "30").trim().toLowerCase();
  if (["7", "30", "90", "mtd", "ytd", "all"].includes(range)) {
    return { chartRange: range };
  }
  return { chartRange: "30" };
}

app.get("/api/metrics/home-bundle", requireAuth, async (req, res) => {
  try {
    const accountScope = metricsAccountIdFromQuery(req.query);
    sendMetricsJson(res, await getMetricsHomeBundle(req.userId, accountScope, req.query.stages));
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "home-bundle failed" });
  }
});

app.get("/api/metrics/analysis-bundle", requireAuth, async (req, res) => {
  try {
    const accountScope = metricsAccountIdFromQuery(req.query);
    const stage = String(req.query.stage || "mtd").trim() || "mtd";
    const symbol = String(req.query.symbol || "").trim();
    const customFrom = String(req.query.from || "").slice(0, 10);
    const customTo = String(req.query.to || "").slice(0, 10);
    sendMetricsJson(
      res,
      await getMetricsAnalysisBundle(req.userId, accountScope, stage, symbol, {
        customFrom: customFrom || undefined,
        customTo: customTo || undefined,
      }),
    );
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "analysis-bundle failed" });
  }
});

app.get("/api/metrics/stock-record-bundle", requireAuth, async (req, res) => {
  try {
    const accountScope = metricsAccountIdFromQuery(req.query);
    const symbol = String(req.query.symbol || "").trim();
    if (!symbol) {
      res.status(400).json({ ok: false, error: "missing symbol" });
      return;
    }
    sendMetricsJson(
      res,
      await getMetricsStockRecordBundle(req.userId, accountScope, symbol, {
        ...stockRecordChartQueryFromQuery(req.query),
      }),
    );
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "stock-record-bundle failed" });
  }
});

async function handlePublicAnalysisBundle(req, res) {
  try {
    const gate = await assertPublicMetricsTarget(req.userId, req.params.targetId);
    if (!gate.ok) {
      res.status(gate.status).json({ ok: false, error: gate.error });
      return;
    }
    const accountScope = metricsAccountIdFromQuery(req.query);
    const stage = String(req.query.stage || "mtd").trim() || "mtd";
    const symbol = String(req.query.symbol || "").trim();
    const customFrom = String(req.query.from || "").slice(0, 10);
    const customTo = String(req.query.to || "").slice(0, 10);
    sendMetricsJson(
      res,
      await getMetricsPublicAnalysisBundle(gate.userId, accountScope, stage, symbol, {
        customFrom: customFrom || undefined,
        customTo: customTo || undefined,
      }),
    );
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "public analysis-bundle failed" });
  }
}

app.get("/api/public/:targetId/analysis-bundle", requireAuth, handlePublicAnalysisBundle);
app.get("/api/public/:targetId/metrics/analysis-bundle", requireAuth, handlePublicAnalysisBundle);

app.get("/api/public/:targetId/metrics/stock-record-bundle", requireAuth, async (req, res) => {
  try {
    const gate = await assertPublicMetricsTarget(req.userId, req.params.targetId);
    if (!gate.ok) {
      res.status(gate.status).json({ ok: false, error: gate.error });
      return;
    }
    const accountScope = metricsAccountIdFromQuery(req.query);
    const symbol = String(req.query.symbol || "").trim();
    if (!symbol) {
      res.status(400).json({ ok: false, error: "missing symbol" });
      return;
    }
    sendMetricsJson(
      res,
      await getMetricsPublicStockRecordBundle(gate.userId, accountScope, symbol, {
        ...stockRecordChartQueryFromQuery(req.query),
      }),
    );
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "public stock-record-bundle failed" });
  }
});

async function handlePublicHomeBundle(req, res) {
  try {
    const gate = await assertPublicMetricsTarget(req.userId, req.params.targetId);
    if (!gate.ok) {
      res.status(gate.status).json({ ok: false, error: gate.error });
      return;
    }
    const accountScope = metricsAccountIdFromQuery(req.query);
    sendMetricsJson(res, await getMetricsPublicHomeBundle(gate.userId, accountScope, req.query.stages));
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "public home-bundle failed" });
  }
}

app.get("/api/public/:targetId/home-bundle", requireAuth, handlePublicHomeBundle);
app.get("/api/public/:targetId/metrics/home-bundle", requireAuth, handlePublicHomeBundle);

app.get("/api/ops/cron-runs", requireAuth, async (req, res) => {
  try {
    if (!(await isMetricsOpsAdmin(req.userId))) {
      res.status(403).json({ ok: false, error: "forbidden" });
      return;
    }
    const data = await listCronJobRuns(req.query.limit);
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "cron runs failed" });
  }
});

function parseLedgerListQuery(req) {
  const limitRaw = req.query?.limit;
  if (limitRaw == null || String(limitRaw).trim() === "") {
    return null;
  }
  const limit = Math.min(100, Math.max(1, parseInt(String(limitRaw), 10) || 10));
  const offset = Math.max(0, parseInt(String(req.query?.offset ?? "0"), 10) || 0);
  const accountIdRaw = req.query?.accountId != null ? String(req.query.accountId).trim() : "all";
  const accountId = accountIdRaw && accountIdRaw !== "all" ? accountIdRaw : null;
  return { limit, offset, accountId };
}

/** 个股记录：默认每页 10 条（新→旧）。 */
function parseSymbolTradesListQuery(req) {
  const limit = Math.min(100, Math.max(1, parseInt(String(req.query?.limit ?? "10"), 10) || 10));
  const offset = Math.max(0, parseInt(String(req.query?.offset ?? "0"), 10) || 0);
  const accountIdRaw = req.query?.accountId != null ? String(req.query.accountId).trim() : "all";
  const accountId = accountIdRaw && accountIdRaw !== "all" ? accountIdRaw : null;
  return { limit, offset, accountId };
}

app.get("/api/trades", requireAuth, async (req, res) => {
  const symbolRaw = req.query?.symbol != null ? String(req.query.symbol).trim() : "";
  if (symbolRaw) {
    const symbol = normalizeSymbol(symbolRaw);
    if (!symbol) {
      res.status(400).json({ ok: false, error: "invalid symbol" });
      return;
    }
    const pageOpts = parseSymbolTradesListQuery(req);
    const { data, pagination } = await getTradesPageForSymbol(req.userId, symbol, pageOpts);
    await enrichTradesWithSymbolNames(data);
    res.json({ ok: true, data, pagination });
    return;
  }
  const pageOpts = parseLedgerListQuery(req);
  if (pageOpts) {
    const { data, pagination } = await getTradesPage(req.userId, pageOpts);
    await enrichTradesWithSymbolNames(data);
    res.json({ ok: true, data, pagination });
    return;
  }
  const data = await getTrades(req.userId);
  await enrichTradesWithSymbolNames(data);
  res.json({ ok: true, data });
});

app.post("/api/trades", requireAuth, async (req, res) => {
  try {
    const trade = normalizeTrade(req.body || {});
    if (!trade.symbol) {
      res.status(400).json({ ok: false, error: "symbol is required" });
      return;
    }
    const prior = trade.id ? await getTradeByIdForUser(trade.id, req.userId) : null;
    const saved = await upsertTrade(trade, req.userId);
    if (!prior) {
      await ensureSymbolNameMapOnNewTrade(saved.symbol, saved.name);
    }
    invalidateDailyCloseAndAnalysisCache(req.userId, {
      hintDates: hintDatesFromTradeMutation(prior, saved),
    });
    const [enriched] = await enrichTradesWithSymbolNames([saved]);
    res.json({ ok: true, data: enriched });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "save trade failed" });
  }
});

app.delete("/api/trades/:id", requireAuth, async (req, res) => {
  const del = await deleteTradeById(req.params.id, req.userId);
  if (del.deleted) {
    invalidateDailyCloseAndAnalysisCache(req.userId, {
      hintDates: del.date ? [del.date] : [],
    });
  }
  res.json({ ok: true, deleted: del.deleted });
});

app.get("/api/cash-transfers", requireAuth, async (req, res) => {
  const pageOpts = parseLedgerListQuery(req);
  if (pageOpts) {
    const { data, pagination } = await getCashTransfersPage(req.userId, pageOpts);
    res.json({ ok: true, data, pagination });
    return;
  }
  res.json({ ok: true, data: await getCashTransfers(req.userId) });
});

app.post("/api/cash-transfers", requireAuth, async (req, res) => {
  try {
    const body = req.body || {};
    const prior = body.id ? await getCashTransferByIdForUser(body.id, req.userId) : null;
    const row = await upsertCashTransfer(body, req.userId);
    invalidateDailyCloseAndAnalysisCache(req.userId, {
      hintDates: hintDatesFromCashMutation(prior, row),
    });
    res.json({ ok: true, data: row });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "save cash transfer failed" });
  }
});

app.delete("/api/cash-transfers/:id", requireAuth, async (req, res) => {
  const del = await deleteCashTransferById(req.params.id, req.userId);
  if (del.deleted) {
    invalidateDailyCloseAndAnalysisCache(req.userId, {
      hintDates: del.date ? [del.date] : [],
    });
  }
  res.json({ ok: true, deleted: del.deleted });
});

app.post("/api/cash-transfers/import", requireAuth, async (req, res) => {
  try {
    const payload = req.body || {};
    const mode = payload.mode === "replace" ? "replace" : "append";
    const rows = Array.isArray(payload.cashTransfers) ? payload.cashTransfers : [];
    const data = await importCashTransfers(rows, mode, req.userId);
    invalidateDailyCloseAndAnalysisCache(req.userId, {
      fullRebuild: mode === "replace",
      hintDates: hintDatesFromImportRows(rows, "date"),
    });
    res.json({ ok: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "import failed" });
  }
});

app.post("/api/trades/import", requireAuth, async (req, res) => {
  try {
    const payload = req.body || {};
    const mode = payload.mode === "replace" ? "replace" : "append";
    const trades = Array.isArray(payload.trades) ? payload.trades : [];
    const normalized = trades.map((item) => normalizeTrade(item));
    const data = await importTrades(normalized, mode, req.userId);
    invalidateDailyCloseAndAnalysisCache(req.userId, {
      fullRebuild: mode === "replace",
      hintDates: hintDatesFromImportRows(normalized, "date"),
    });
    res.json({ ok: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "import failed" });
  }
});

app.get("/api/settings", requireAuth, async (req, res) => {
  res.json({ ok: true, data: await getSettings(req.userId) });
});

app.patch("/api/settings", requireAuth, async (req, res) => {
  try {
    const patch = req.body && typeof req.body === "object" ? req.body : {};
    const sanitized = {};
    for (const key of Object.keys(DEFAULT_SETTINGS)) {
      if (Object.hasOwn(patch, key)) {
        sanitized[key] = patch[key];
      }
    }
    const data = await setSettings(sanitized, req.userId);
    res.json({ ok: true, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "update settings failed" });
  }
});

app.all("/api/cron/freeze-eod", async (req, res) => {
  const sessionUser = readUserIdFromRequest(req);
  if (!sessionUser && !isCronAuthorized(req)) {
    res.status(401).json({ ok: false, error: "unauthorized cron request" });
    return;
  }
  const startedAt = Date.now();
  try {
    await ensureAppDerivedTables();
    const forcedDate = req.query?.frozenDate || req.body?.frozenDate;
    const force = parseBooleanInput(req.query?.force ?? req.body?.force, false);
    const syncDailyClose = parseBooleanInput(req.query?.syncDailyClose ?? req.body?.syncDailyClose, false);
    const rebuildFromDate = req.query?.rebuildFromDate || req.body?.rebuildFromDate || null;
    const fullRebuild = parseBooleanInput(req.query?.fullRebuild ?? req.body?.fullRebuild, false);
    const userIds = Array.isArray(req.body?.userIds) ? req.body.userIds : [];
    const fromCron = req.headers["x-vercel-cron"] != null;
    const result = await runDailyFreeze({
      frozenDate: forcedDate,
      force,
      syncDailyClose,
      userIds,
      fromCron,
      rebuildFromDate,
      fullRebuild,
      logger: console,
    });
    analysisDailyMemoryCache.clear();
    dailyCloseForTradesMemoryCache.clear();
    realtimePatchMemoryCache.clear();
    await insertCronJobRun({
      jobName: "freeze-eod",
      startedAt,
      finishedAt: Date.now(),
      ok: true,
      metaJson: JSON.stringify({ frozenDate: result?.frozenDate, users: result?.users?.length }),
    });
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, data: result });
  } catch (error) {
    await insertCronJobRun({
      jobName: "freeze-eod",
      startedAt,
      finishedAt: Date.now(),
      ok: false,
      errorMessage: error?.message || String(error),
    }).catch(() => {});
    res.status(500).json({ ok: false, error: error?.message || "freeze failed" });
  }
});

app.all("/api/cron/sync-daily-close", async (req, res) => {
  const sessionUser = readUserIdFromRequest(req);
  if (!sessionUser && !isCronAuthorized(req)) {
    res.status(401).json({ ok: false, error: "unauthorized cron request" });
    return;
  }
  const startedAt = Date.now();
  try {
    await ensureAppDerivedTables();
    const asOfDate = req.query?.asOfDate || req.body?.asOfDate;
    const bodySymbols = Array.isArray(req.body?.symbols) ? req.body.symbols : [];
    const querySymbols = sanitizeSymbolList(req.query?.symbols);
    const symbols = [...new Set([...bodySymbols, ...querySymbols].map((s) => normalizeSymbol(String(s || ""))).filter(Boolean))];
    const result = await runDailyCloseSync({
      asOfDate,
      symbols,
      logger: console,
    });
    dailyCloseForTradesMemoryCache.clear();
    realtimePatchMemoryCache.clear();
    await insertCronJobRun({
      jobName: "sync-daily-close",
      startedAt,
      finishedAt: Date.now(),
      ok: true,
      metaJson: JSON.stringify({ rowsWritten: result?.rowsWritten, failedSymbols: result?.failedSymbols }),
    });
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, data: result });
  } catch (error) {
    await insertCronJobRun({
      jobName: "sync-daily-close",
      startedAt,
      finishedAt: Date.now(),
      ok: false,
      errorMessage: error?.message || String(error),
    }).catch(() => {});
    res.status(500).json({ ok: false, error: error?.message || "sync daily close failed" });
  }
});

app.get("/api/snapshot/watermark", async (_req, res) => {
  try {
    const data = await getSnapshotWatermark();
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "watermark failed" });
  }
});

app.get("/api/snapshot/account-daily", requireAuth, async (req, res) => {
  try {
    const accountId = req.query.accountId != null ? String(req.query.accountId).trim() : "";
    const from = req.query.from != null && String(req.query.from).trim() ? String(req.query.from).trim() : "1970-01-01";
    const to = req.query.to != null && String(req.query.to).trim() ? String(req.query.to).trim() : "9999-12-31";
    const cacheKey = `u:${req.userId}:snapshot-account:account=${accountId || "*"}:from=${from}:to=${to}`;
    const cached = cacheGetWithTtl(analysisDailyMemoryCache, cacheKey, ANALYSIS_DAILY_CACHE_TTL_MS);
    if (cached) {
      res.setHeader("Cache-Control", "no-store");
      res.json({ ok: true, data: cached, cached: true });
      return;
    }
    const data = await getAnalysisDailySnapshots({ accountId, from, to }, req.userId);
    cacheSet(analysisDailyMemoryCache, cacheKey, data);
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "snapshot account failed" });
  }
});

app.get("/api/snapshot/symbol-daily", requireAuth, async (req, res) => {
  try {
    const accountId = req.query.accountId != null ? String(req.query.accountId).trim() : "";
    const from = req.query.from != null && String(req.query.from).trim() ? String(req.query.from).trim() : "1970-01-01";
    const to = req.query.to != null && String(req.query.to).trim() ? String(req.query.to).trim() : "9999-12-31";
    const symbols = sanitizeSymbolList(req.query.symbols);
    let rows = [];
    if (!symbols.length) {
      rows = await getSymbolDailyPnl({ accountId, from, to, symbol: req.query.symbol }, req.userId);
    } else {
      const chunks = await Promise.all(
        symbols.map((symbol) => getSymbolDailyPnl({ accountId, from, to, symbol }, req.userId))
      );
      rows = chunks.flat();
      rows.sort((a, b) => {
        if (a.date !== b.date) {
          return String(a.date).localeCompare(String(b.date));
        }
        return String(a.symbol).localeCompare(String(b.symbol));
      });
    }
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, data: rows });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "snapshot symbol daily failed" });
  }
});

app.get("/api/snapshot/symbol-close", requireAuth, async (req, res) => {
  try {
    const w = await getTradeWindowForDailyClose(req.userId);
    if (!w.symbols.length) {
      res.json({ ok: true, data: {}, from: null, to: null, symbols: [] });
      return;
    }
    const requested = sanitizeSymbolList(req.query.symbols);
    const wantedSet = requested.length ? new Set(requested) : null;
    const symbols = wantedSet ? w.symbols.filter((sym) => wantedSet.has(sym)) : w.symbols;
    if (!symbols.length) {
      res.json({ ok: true, data: {}, from: null, to: null, symbols: [] });
      return;
    }
    const days = parsePositiveInt(req.query.days, 240, 30, 4000);
    const to = w.to || dateKeyDaysFromToday(0);
    const floorFrom = dateKeyDaysFromToday(-days);
    const from = w.from && w.from > floorFrom ? w.from : floorFrom;
    const cacheKey = `u:${req.userId}:snapshot-close:from=${from}:to=${to}:symbols=${symbols.join(",")}`;
    const cached = cacheGetWithTtl(dailyCloseForTradesMemoryCache, cacheKey, DAILY_CLOSE_FOR_TRADES_CACHE_TTL_MS);
    if (cached) {
      res.setHeader("Cache-Control", "no-store");
      res.json({ ok: true, ...cached, cached: true });
      return;
    }
    const data = {};
    const rowsBySymbol = await Promise.all(symbols.map(async (sym) => [sym, await getSymbolDailyCloseRange(sym, from, to)]));
    for (const [sym, rows] of rowsBySymbol) {
      data[sym] = rows;
    }
    const payload = { data, from, to, symbols };
    cacheSet(dailyCloseForTradesMemoryCache, cacheKey, payload);
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, ...payload });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "snapshot symbol close failed" });
  }
});

app.get("/api/realtime/fx", requireAuth, async (_req, res) => {
  try {
    const tRes = await fetchTencentQuotePayloadMap(["whUSDCNY", "whHKDCNY"]);
    if (!tRes.ok) {
      res.status(502).json({ ok: false, error: tRes.error || "realtime fx failed" });
      return;
    }
    if (tRes.delayed) {
      setDelayedHeaders(res, tRes.source || "cache");
    }
    const usd = parseTencentForexQuotePayload(tRes.payloadMap.get("whusdcny"));
    const hkd = parseTencentForexQuotePayload(tRes.payloadMap.get("whhkdcny"));
    const fxSpot = {};
    if (usd && Number.isFinite(usd.current) && usd.current > 0) {
      fxSpot.USD = usd.current;
    }
    if (hkd && Number.isFinite(hkd.current) && hkd.current > 0) {
      fxSpot.HKD = hkd.current;
    }
    const quoteTime = pickLatestQuoteTime([usd?.time, hkd?.time]);
    res.setHeader("Cache-Control", "no-store");
    res.json({
      ok: true,
      fxSpot,
      quoteTime,
      delayed: !!tRes.delayed,
      delaySource: tRes.source || "",
    });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "realtime fx failed" });
  }
});

app.post("/api/realtime/patch", requireAuth, async (req, res) => {
  try {
    const accountId = req.body?.accountId != null ? String(req.body.accountId).trim() : "all";
    const cacheKey = `u:${req.userId}:realtime-patch:account=${accountId || "all"}`;
    const cached = cacheGetWithTtl(realtimePatchMemoryCache, cacheKey, REALTIME_PATCH_CACHE_TTL_MS);
    if (cached) {
      res.setHeader("Cache-Control", "no-store");
      res.json({ ok: true, ...cached, cached: true });
      return;
    }

    const latestDate = await getLatestAnalysisSnapshotDate(req.userId, accountId || "all");
    const baseRows = latestDate
      ? await getAnalysisDailySnapshots({ accountId, from: latestDate, to: latestDate }, req.userId)
      : [];
    const baseSnapshot = baseRows[baseRows.length - 1] || null;

    const symbols = await collectLiveSymbolsForUser(req.userId, accountId || "all");
    const quoteReq = await fetchTencentQuotePayloadMap(symbols.map((s) => toTencentQuoteSymbol(s)).filter(Boolean));
    const fxReq = await fetchTencentQuotePayloadMap(["whUSDCNY", "whHKDCNY"]);

    const quoteMap = {};
    if (quoteReq.ok) {
      const keyToSymbol = new Map();
      symbols.forEach((sym) => {
        const key = toTencentQuoteSymbol(sym);
        if (key) {
          keyToSymbol.set(String(key).toLowerCase(), sym);
        }
      });
      for (const [k, payload] of quoteReq.payloadMap.entries()) {
        const sym = keyToSymbol.get(String(k).toLowerCase());
        if (!sym) continue;
        const parsed = parseTencentQuoteRecord(sym, payload);
        if (parsed) {
          quoteMap[sym] = parsed;
        }
      }
    }

    const fxSpot = {};
    const usd = fxReq.ok ? parseTencentForexQuotePayload(fxReq.payloadMap.get("whusdcny")) : null;
    const hkd = fxReq.ok ? parseTencentForexQuotePayload(fxReq.payloadMap.get("whhkdcny")) : null;
    if (usd?.current > 0) fxSpot.USD = usd.current;
    if (hkd?.current > 0) fxSpot.HKD = hkd.current;
    const fxRate = (currency) => {
      if (currency === "CNY") return 1;
      if (currency === "USD") return Number(fxSpot.USD) > 0 ? Number(fxSpot.USD) : 7.2;
      if (currency === "HKD") return Number(fxSpot.HKD) > 0 ? Number(fxSpot.HKD) : 0.92;
      return 1;
    };

    const trades = await getTrades(req.userId);
    const accountTrades =
      accountId === "all"
        ? trades
        : trades.filter((trade) => String(trade.accountId || "default") === String(accountId));
    accountTrades.sort((a, b) => {
      const ad = new Date(a.date).getTime();
      const bd = new Date(b.date).getTime();
      if (ad !== bd) return ad - bd;
      return Number(a.createdAt || 0) - Number(b.createdAt || 0);
    });
    const holdings = new Map();
    accountTrades.forEach((trade) => {
      const symbol = normalizeSymbol(trade.symbol);
      if (!symbol) return;
      const entry = holdings.get(symbol) || { quantity: 0 };
      entry.quantity += trade.side === "buy" ? Number(trade.quantity || 0) : -Number(trade.quantity || 0);
      holdings.set(symbol, entry);
    });

    let liveMarketValue = 0;
    let todayProfitCny = 0;
    const todayKey = liveDateKeyShanghai();
    for (const [symbol, item] of holdings.entries()) {
      if (!(Math.abs(Number(item.quantity) || 0) > 1e-6)) {
        continue;
      }
      const quote = quoteMap[symbol];
      if (!quote) {
        continue;
      }
      const current = Number(quote.current) || 0;
      const prevClose = Number(quote.prevClose) || current;
      const currency = symbol.startsWith("hk") || symbol.startsWith("rt_hk") ? "HKD" : symbol.startsWith("sh") || symbol.startsWith("sz") ? "CNY" : "USD";
      const rate = fxRate(currency);
      liveMarketValue += item.quantity * current * rate;
      todayProfitCny += todayProfitCnyForHolding({
        quote,
        symbol,
        prevClose,
        current,
        rate,
        trades: accountTrades,
        todayKey,
      });
    }
    const liveTotalProfit = baseSnapshot
      ? baseSnapshot.date === todayKey
        ? Number(baseSnapshot.totalProfit || 0) - Number(baseSnapshot.profitCny || 0) + todayProfitCny
        : Number(baseSnapshot.totalProfit || 0) + todayProfitCny
      : todayProfitCny;

    const payload = {
      accountId,
      frozenDate: latestDate || null,
      liveDate: todayKey,
      baseSnapshot,
      todayProfitCny,
      liveMarketValue,
      liveTotalProfit,
      quoteTime: pickLatestQuoteTime([usd?.time, hkd?.time, ...Object.values(quoteMap).map((item) => item?.time)]),
      delayed: !!quoteReq.delayed || !!fxReq.delayed,
    };
    cacheSet(realtimePatchMemoryCache, cacheKey, payload);
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, ...payload });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || "realtime patch failed" });
  }
});

app.get("/api/fx/sina-dayk", async (req, res) => {
  const pair = String(req.query.pair || "").toLowerCase().trim();
  const symbol = pair === "usdcny" ? "fx_USDCNY" : pair === "hkdcny" ? "fx_HKDCNY" : "";
  if (!symbol) {
    res.status(400).json({ ok: false, error: "pair must be usdcny or hkdcny" });
    return;
  }
  try {
    const result = await fetchSinaKlineJsonFromUpstream({
      symbol,
      len: req.query.len != null ? String(req.query.len) : "5000",
      asc: req.query.asc != null ? String(req.query.asc) : "0",
      start: req.query.start != null ? String(req.query.start) : "",
      end: req.query.end != null ? String(req.query.end) : "",
    });
    if (!result.ok) {
      res.status(502).json({ ok: false, error: result.error || "sina dayk failed" });
      return;
    }
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, data: result.data || [] });
  } catch (error) {
    res.status(502).json({ ok: false, error: error.message || "sina dayk failed" });
  }
});

/**
 * 腾讯 qt.gtimg.cn 实时行情：服务端拉取并 gbk→utf8，避免浏览器 JSONP + window 变量在部分环境下取不到 usGOOG 等，导致回退成成交价（如 326）。
 */
app.get("/api/quote/tencent", async (req, res) => {
  const q = req.query.q != null ? String(req.query.q) : "";
  if (!q || q.length > 2048 || !/^[a-zA-Z0-9._,\-]+$/.test(q)) {
    res.status(400).json({ ok: false, error: "invalid q" });
    return;
  }
  const reqKeys = [...new Set(q.split(",").map((s) => String(s || "").trim()).filter(Boolean))];
  if (!reqKeys.length) {
    res.status(400).json({ ok: false, error: "invalid q keys" });
    return;
  }
  const result = await fetchTencentQuotePayloadMap(reqKeys);
  if (!result.ok) {
    res.status(502).json({ ok: false, error: result.error || "tencent quote failed" });
    return;
  }
  const text = buildTencentQuoteTextFromMap(reqKeys, result.payloadMap);
  if (!text) {
    res.status(502).json({ ok: false, error: "no quote payload available" });
    return;
  }
  if (result.delayed) {
    setDelayedHeaders(res, result.source);
  }
  res.setHeader("Cache-Control", "no-store");
  res.type("text/plain; charset=utf-8");
  res.send(text);
});

/**
 * 美股：取「严格早于 before（YYYY-MM-DD）」的最后一个交易日收盘（新浪 DailyK_Batch，us_TICKER）。
 */
app.get("/api/us-historical-close", async (req, res) => {
  const raw = req.query.symbol != null ? String(req.query.symbol) : "";
  const before = req.query.before != null ? String(req.query.before) : "";
  if (!raw.trim() || !/^\d{4}-\d{2}-\d{2}$/.test(before)) {
    res.status(400).json({ ok: false, error: "symbol and before=YYYY-MM-DD required" });
    return;
  }
  let lowered = raw.trim().toLowerCase().replace(/\s+/g, "");
  if (lowered.startsWith("gb_")) {
    lowered = lowered.slice(3);
  }
  const stripped = lowered.replace(/^us/i, "").replace(/\.(OQ|N)$/i, "");
  const ticker = /^[a-z0-9._-]+$/i.test(stripped) ? stripped.toUpperCase() : "";
  if (!ticker || ticker.length > 32) {
    res.status(400).json({ ok: false, error: "invalid symbol" });
    return;
  }
  const sinaSymbol = `us_${ticker}`;
  try {
    const sinaRes = await fetchSinaKlineJsonFromUpstream({
      symbol: sinaSymbol,
      len: "5000",
      asc: "0",
    });
    if (!sinaRes.ok) {
      res.status(502).json({ ok: false, error: sinaRes.error || "sina failed" });
      return;
    }
    const arr = sinaRes.data;
    if (!Array.isArray(arr) || !arr.length) {
      res.json({ ok: false, error: "no series" });
      return;
    }
    let bestDay = "";
    let bestClose = NaN;
    for (const row of arr) {
      const dayKey = String(row?.day ?? "")
        .trim()
        .slice(0, 10)
        .replace(/\//g, "-");
      const close = Number(String(row?.close ?? "").replace(/,/g, ""));
      if (!dayKey || dayKey >= before) {
        continue;
      }
      if (!Number.isFinite(close) || close <= 0) {
        continue;
      }
      if (!bestDay || dayKey > bestDay) {
        bestDay = dayKey;
        bestClose = close;
      }
    }
    if (!bestDay || !Number.isFinite(bestClose)) {
      res.json({ ok: false, error: "no bar before date" });
      return;
    }
    res.setHeader("Cache-Control", "public, max-age=3600");
    res.json({ ok: true, day: bestDay, close: bestClose });
  } catch (error) {
    res.status(502).json({ ok: false, error: error.message || "sina failed" });
  }
});

/** 本地缓存的日收盘价（股票、日期、收盘），计算时优先进页前先 GET for-trades 灌入前端 */
app.get("/api/daily-close", async (req, res) => {
  try {
    const raw = req.query.symbol != null ? String(req.query.symbol) : "";
    const symbol = normalizeSymbol(raw);
    if (!symbol) {
      res.status(400).json({ ok: false, error: "symbol required" });
      return;
    }
    const from = req.query.from != null ? String(req.query.from).trim() : "1970-01-01";
    const to = req.query.to != null ? String(req.query.to).trim() : "9999-12-31";
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, data: await getSymbolDailyCloseRange(symbol, from, to) });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "daily-close failed" });
  }
});

app.get("/api/daily-close/for-trades", requireAuth, async (req, res) => {
  try {
    const w = await getTradeWindowForDailyClose(req.userId);
    if (!w.symbols.length) {
      res.json({ ok: true, data: {}, from: null, to: null, symbols: [] });
      return;
    }
    const requested = sanitizeSymbolList(req.query.symbols);
    const wantedSet = requested.length ? new Set(requested) : null;
    const symbols = wantedSet ? w.symbols.filter((sym) => wantedSet.has(sym)) : w.symbols;
    if (!symbols.length) {
      res.json({ ok: true, data: {}, from: null, to: null, symbols: [] });
      return;
    }
    const days = parsePositiveInt(req.query.days, 240, 30, 1460);
    const to = w.to || dateKeyDaysFromToday(0);
    const floorFrom = dateKeyDaysFromToday(-days);
    const from = w.from && w.from > floorFrom ? w.from : floorFrom;
    const cacheKey = `u:${req.userId}:daily-close:from=${from}:to=${to}:symbols=${symbols.join(",")}`;
    const cached = cacheGetWithTtl(dailyCloseForTradesMemoryCache, cacheKey, DAILY_CLOSE_FOR_TRADES_CACHE_TTL_MS);
    if (cached) {
      res.setHeader("Cache-Control", "no-store");
      res.json({ ok: true, ...cached, cached: true });
      return;
    }
    const data = {};
    const rowsBySymbol = await Promise.all(
      symbols.map(async (sym) => [sym, await getSymbolDailyCloseRange(sym, from, to)])
    );
    for (const [sym, rows] of rowsBySymbol) {
      data[sym] = rows;
    }
    const payload = { data, from, to, symbols };
    cacheSet(dailyCloseForTradesMemoryCache, cacheKey, payload);
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, ...payload });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "daily-close failed" });
  }
});

/** 从东财+新浪拉区间写入 symbol_daily_close（先跑回填再打开页面） */
app.post("/api/daily-close/backfill", requireAuth, async (req, res) => {
  try {
    const w = await getTradeWindowForDailyClose(req.userId);
    if (!w.symbols.length) {
      res.json({ ok: true, message: "no trades", counts: {} });
      return;
    }
    let symbols = w.symbols;
    if (Array.isArray(req.body?.symbols) && req.body.symbols.length) {
      const want = new Set(req.body.symbols.map((s) => normalizeSymbol(String(s))).filter(Boolean));
      symbols = w.symbols.filter((s) => want.has(s));
    }
    const counts = {};
    for (const sym of symbols) {
      const rows = await fetchRemoteDailyClosesForSymbol(sym, w.from, w.to);
      await upsertSymbolDailyCloseBatch(rows);
      counts[sym] = rows.length;
      await new Promise((r) => setTimeout(r, 200));
    }
    clearUserScopedCache(dailyCloseForTradesMemoryCache, req.userId);
    res.json({ ok: true, from: w.from, to: w.to, counts });
  } catch (error) {
    res.status(502).json({ ok: false, error: error.message || "backfill failed" });
  }
});

/** 股票搜索联想（新增交易搜股） */
app.get("/api/search", async (req, res) => {
  const query = req.query.query != null ? String(req.query.query).trim() : "";
  if (!query || query.length > 64) {
    res.status(400).json({ ok: false, error: "invalid query" });
    return;
  }
  const url = `https://suggest3.sinajs.cn/suggest/?key=${encodeURIComponent(
    query
  )}&type=111,41,31,101&name=suggest&num=50`;
  try {
    const r = await fetch(url, {
      headers: { "user-agent": "stockreview/1" },
      signal: AbortSignal.timeout(6500),
    });
    if (!r.ok) {
      res.status(502).json({ ok: false, error: "search upstream http error" });
      return;
    }
    const buf = Buffer.from(await r.arrayBuffer());
    let text = iconv.decode(buf, "gbk");
    if (!/var suggest\s*=/.test(text) && /var suggest/.test(buf.toString("utf8"))) {
      text = buf.toString("utf8");
    }
    const lines = parseSinaSuggestText(text);
    const results = [];
    for (const line of lines) {
      const item = suggestLineToItem(line, normalizeSymbol);
      if (item) {
        results.push(item);
      }
    }
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, results: publicSearchResults(results) });
  } catch (error) {
    res.status(502).json({ ok: false, error: error?.message || "search failed" });
  }
});

app.use("/api", (_req, res) => {
  res.status(404).json({ ok: false, error: "API route not found" });
});

// 生产环境对 app.js / styles.css 使用较短强缓存 + SWR，减轻重复访问体积；HTML 仍禁用强缓存以免旧壳引用错位。
// 本地开发（非 VERCEL、NODE_ENV 非 production）维持 no-store，避免改代码后仍见旧脚本。
app.use(
  express.static(WEB_ROOT, {
    setHeaders(res, filePath) {
      const base = path.basename(String(filePath || ""));
      if (STATIC_ASSET_LONG_CACHE && /^(app\.js|styles\.css)$/i.test(base)) {
        res.setHeader("Cache-Control", "public, max-age=1800, stale-while-revalidate=86400");
        return;
      }
      if (/\.(html|js|css|json|ico|svg)$/i.test(filePath)) {
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
        res.setHeader("Pragma", "no-cache");
      }
    },
  })
);

app.use((req, res) => {
  if (req.url.startsWith("/api/")) {
    res.status(404).json({ ok: false, error: "API Not Found", url: req.url });
    return;
  }
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
  const htmlPath = path.join(WEB_ROOT, "index.html");
  if (fs.existsSync(htmlPath)) {
    // Avoid res.sendFile in serverless environments as it can cause 300s hangs
    // due to unclosed streams in serverless-http.
    const html = fs.readFileSync(htmlPath, "utf-8");
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(html);
  } else {
    res.status(404).send("Not Found");
  }
});

if (require.main === module) {
  app.listen(PORT, () => {
    // eslint-disable-next-line no-console
    console.log(`Server listening on http://0.0.0.0:${PORT}`);
  });
}

module.exports = app;
