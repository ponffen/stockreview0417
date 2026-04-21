const path = require("node:path");
const fs = require("node:fs");
const express = require("express");
const cors = require("cors");
const iconv = require("iconv-lite");

const { fetchSinaKlineJsonFromUpstream } = require("./src/sina-kline-upstream");
const { fetchRemoteDailyClosesForSymbol } = require("./src/daily-close-backfill");

/**
 * 新浪 CN_MarketData.getKLineData 代理。勿依赖单一字符串路径，避免部署/转发后落到 app.use('/api') 的 404。
 */
async function handleSinaKlineProxy(req, res) {
  const symbol = req.query.symbol != null ? String(req.query.symbol) : "";
  const scale = req.query.scale != null ? String(req.query.scale) : "240";
  const datalen = req.query.datalen != null ? String(req.query.datalen) : "1023";
  const ma = req.query.ma != null ? String(req.query.ma) : "no";
  const sym = symbol.trim();
  if (!sym || sym.length > 64 || !/^[a-zA-Z0-9._-]+$/.test(sym)) {
    res.status(400).json({ ok: false, error: "invalid symbol" });
    return;
  }
  if (!/^\d+$/.test(scale) || !/^\d+$/.test(datalen)) {
    res.status(400).json({ ok: false, error: "invalid scale or datalen" });
    return;
  }
  const result = await fetchSinaKlineJsonFromUpstream({ symbol: sym, scale, ma, datalen });
  if (!result.ok) {
    res.status(502).json({ ok: false, error: result.error || "sina kline failed" });
    return;
  }
  res.setHeader("Cache-Control", "no-store");
  /** 新浪对无效/暂无数据常返回 JSON null；统一成 [] 便于前端解析 */
  const body = result.data == null ? [] : result.data;
  res.json(body);
}

function ensureDataDir() {
  const dataDir = path.join(__dirname, "data");
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
}

ensureDataDir();

const {
  DEFAULT_SETTINGS,
  normalizeSymbol,
  normalizeTrade,
  getTrades,
  upsertTrade,
  importTrades,
  deleteTradeById,
  getAccounts,
  getDailyReturns,
  upsertDailyReturn,
  importDailyReturns,
  deleteDailyReturn,
  getSettings,
  setSettings,
  getState,
  getSymbolDailyPnl,
  upsertSymbolDailyPnlBatch,
  getAnalysisDailySnapshots,
  upsertAnalysisDailySnapshot,
  upsertSymbolDailyCloseBatch,
  getSymbolDailyCloseRange,
  getTradeWindowForDailyClose,
} = require("./src/db");

const app = express();
const PORT = Number(process.env.PORT || 3030);

app.use(cors());
app.use(express.json({ limit: "5mb" }));

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, node: process.version });
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

function eastmoneySuggestQueryInput(normalized) {
  const n = String(normalized || "").toLowerCase();
  if (n.startsWith("sz") || n.startsWith("sh")) {
    return n.slice(2);
  }
  if (n.startsWith("hk")) {
    return n.slice(2).padStart(5, "0");
  }
  return "";
}

function pickEastmoneySuggestRow(normalized, json) {
  const rows = json?.QuotationCodeTable?.Data;
  if (!Array.isArray(rows) || !rows.length) {
    return null;
  }
  const n = String(normalized).toLowerCase();
  if (n.startsWith("hk")) {
    const hk5 = n.slice(2).padStart(5, "0");
    return (
      rows.find(
        (r) =>
          String(r.Code).padStart(5, "0") === hk5 &&
          (r.Classify === "HK" || String(r.QuoteID || "").startsWith("116"))
      ) ||
      rows.find((r) => String(r.Code).padStart(5, "0") === hk5) ||
      null
    );
  }
  if (n.startsWith("sz") || n.startsWith("sh")) {
    const c6 = n.slice(2);
    return rows.find((r) => String(r.Code) === c6) || rows[0];
  }
  return rows[0];
}

app.get("/api/stock/name", async (req, res) => {
  try {
    const raw = req.query.symbol != null ? String(req.query.symbol) : "";
    const normalized = normalizeSymbol(raw);
    if (!normalized) {
      res.status(400).json({ ok: false, error: "symbol required" });
      return;
    }
    const input = eastmoneySuggestQueryInput(normalized);
    if (!input) {
      res.json({ ok: true, name: "", symbol: normalized });
      return;
    }
    const url = `https://searchadapter.eastmoney.com/api/suggest/get?input=${encodeURIComponent(input)}&type=14`;
    const response = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; stockreview/1.0)",
        Referer: "https://www.eastmoney.com/",
      },
    });
    if (!response.ok) {
      res.json({ ok: true, name: "", symbol: normalized });
      return;
    }
    const json = await response.json();
    const row = pickEastmoneySuggestRow(normalized, json);
    const name = String(row?.Name || "").trim();
    res.json({ ok: true, name, symbol: normalized });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "name lookup failed" });
  }
});

app.get("/api/state", (_req, res) => {
  res.json({ ok: true, data: getState() });
});

app.get("/api/trades", (_req, res) => {
  res.json({ ok: true, data: getTrades() });
});

app.post("/api/trades", (req, res) => {
  try {
    const trade = normalizeTrade(req.body || {});
    if (!trade.symbol) {
      res.status(400).json({ ok: false, error: "symbol is required" });
      return;
    }
    const saved = upsertTrade(trade);
    res.json({ ok: true, data: saved });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "save trade failed" });
  }
});

app.delete("/api/trades/:id", (req, res) => {
  const ok = deleteTradeById(req.params.id);
  res.json({ ok: true, deleted: ok });
});

app.post("/api/trades/import", (req, res) => {
  try {
    const payload = req.body || {};
    const mode = payload.mode === "replace" ? "replace" : "append";
    const trades = Array.isArray(payload.trades) ? payload.trades : [];
    const normalized = trades.map((item) => normalizeTrade(item));
    const data = importTrades(normalized, mode);
    res.json({ ok: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "import failed" });
  }
});

app.get("/api/settings", (_req, res) => {
  res.json({ ok: true, data: getSettings() });
});

app.patch("/api/settings", (req, res) => {
  try {
    const patch = req.body && typeof req.body === "object" ? req.body : {};
    const sanitized = {};
    for (const key of Object.keys(DEFAULT_SETTINGS)) {
      if (Object.hasOwn(patch, key)) {
        sanitized[key] = patch[key];
      }
    }
    const data = setSettings(sanitized);
    res.json({ ok: true, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "update settings failed" });
  }
});

app.get("/api/accounts", (_req, res) => {
  res.json({ ok: true, data: getAccounts() });
});

app.get("/api/symbol-daily", (req, res) => {
  try {
    const data = getSymbolDailyPnl({
      accountId: req.query.accountId,
      from: req.query.from,
      to: req.query.to,
    });
    res.json({ ok: true, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "symbol daily failed" });
  }
});

app.post("/api/symbol-daily/batch", (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    upsertSymbolDailyPnlBatch(rows);
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "symbol daily batch failed" });
  }
});

app.get("/api/analysis-daily", (req, res) => {
  try {
    const data = getAnalysisDailySnapshots({
      accountId: req.query.accountId,
      from: req.query.from,
      to: req.query.to,
    });
    res.json({ ok: true, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "analysis daily failed" });
  }
});

app.post("/api/analysis-daily", (req, res) => {
  try {
    const row = upsertAnalysisDailySnapshot(req.body || {});
    res.json({ ok: true, data: row });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "analysis daily upsert failed" });
  }
});

app.get("/api/daily-returns", (req, res) => {
  try {
    const { accountId, from, to } = req.query || {};
    const data = getDailyReturns({
      accountId: accountId != null ? String(accountId) : "",
      from: from != null ? String(from) : "",
      to: to != null ? String(to) : "",
    });
    res.json({ ok: true, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "list daily returns failed" });
  }
});

app.post("/api/daily-returns", (req, res) => {
  try {
    const row = upsertDailyReturn(req.body || {});
    res.json({ ok: true, data: row });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "save daily return failed" });
  }
});

app.post("/api/daily-returns/import", (req, res) => {
  try {
    const payload = req.body || {};
    const mode = payload.mode === "replace" ? "replace" : "append";
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    const data = importDailyReturns(rows, mode);
    res.json({ ok: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "import daily returns failed" });
  }
});

app.delete("/api/daily-returns", (req, res) => {
  try {
    const accountId = req.query.accountId != null ? String(req.query.accountId) : "";
    const date = req.query.date != null ? String(req.query.date) : "";
    if (!accountId || !date) {
      res.status(400).json({ ok: false, error: "accountId and date are required" });
      return;
    }
    const deleted = deleteDailyReturn(accountId, date);
    res.json({ ok: true, deleted });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "delete daily return failed" });
  }
});

/** 新浪外汇日 K JSONP 原文（日期,开,高,低,收,），供前端解析收盘价 */
const SINA_FX_DAYK_URL = {
  usdcny:
    "http://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var%20USDCNY=/NewForexService.getDayKLine?symbol=fx_susdcny",
  hkdcny:
    "http://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var%20HKDCNY=/NewForexService.getDayKLine?symbol=fx_shkdcny",
};

app.get("/api/fx/sina-dayk", async (req, res) => {
  const pair = String(req.query.pair || "").toLowerCase();
  const url = SINA_FX_DAYK_URL[pair];
  if (!url) {
    res.status(400).json({ ok: false, error: "pair must be usdcny or hkdcny" });
    return;
  }
  try {
    const r = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; stockreview/1.0)" },
    });
    if (!r.ok) {
      res.status(502).json({ ok: false, error: `sina ${r.status}` });
      return;
    }
    const text = await r.text();
    res.setHeader("Cache-Control", "no-store");
    res.type("text/plain; charset=utf-8");
    res.send(text);
  } catch (error) {
    res.status(502).json({ ok: false, error: error.message || "sina dayk failed" });
  }
});

/** 外汇实时：waihui123（USD 基准下 CNY/HKD 为交叉盘中间价，由前端换算为 1 USD、1 HKD 兑 CNY） */
app.get("/api/fx/waihui123", async (_req, res) => {
  try {
    const url = "https://www.waihui123.com/reteapi?action=get&code=USD,CNY,HKD";
    const r = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; stockreview/1.0)" },
    });
    if (!r.ok) {
      res.status(502).json({ ok: false, error: `waihui123 ${r.status}` });
      return;
    }
    const json = await r.json();
    res.setHeader("Cache-Control", "no-store");
    res.json(json);
  } catch (error) {
    res.status(502).json({ ok: false, error: error.message || "waihui123 fx failed" });
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
  try {
    const url = `https://qt.gtimg.cn/q=${encodeURIComponent(q)}&_=${Date.now()}`;
    const r = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; stockreview/1.0)" },
    });
    if (!r.ok) {
      res.status(502).json({ ok: false, error: `tencent ${r.status}` });
      return;
    }
    const buf = Buffer.from(await r.arrayBuffer());
    const text = iconv.decode(buf, "gbk");
    res.setHeader("Cache-Control", "no-store");
    res.type("text/plain; charset=utf-8");
    res.send(text);
  } catch (error) {
    res.status(502).json({ ok: false, error: error.message || "tencent quote failed" });
  }
});

/**
 * 美股：取「严格早于 before（YYYY-MM-DD）」的最后一个交易日的收盘（新浪 CN_MarketData.getKLineData，gb_TICKER 日 K）。
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
  const sinaSymbol = `gb_${ticker}`;
  try {
    const sinaRes = await fetchSinaKlineJsonFromUpstream({
      symbol: sinaSymbol,
      scale: "240",
      ma: "no",
      datalen: "1023",
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
app.get("/api/daily-close", (req, res) => {
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
    res.json({ ok: true, data: getSymbolDailyCloseRange(symbol, from, to) });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "daily-close failed" });
  }
});

app.get("/api/daily-close/for-trades", (_req, res) => {
  try {
    const w = getTradeWindowForDailyClose();
    if (!w.symbols.length) {
      res.json({ ok: true, data: {}, from: null, to: null, symbols: [] });
      return;
    }
    const data = {};
    for (const sym of w.symbols) {
      data[sym] = getSymbolDailyCloseRange(sym, w.from, w.to);
    }
    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, data, from: w.from, to: w.to, symbols: w.symbols });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "daily-close failed" });
  }
});

/** 从东财+新浪拉区间写入 symbol_daily_close（先跑回填再打开页面） */
app.post("/api/daily-close/backfill", async (req, res) => {
  try {
    const w = getTradeWindowForDailyClose();
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
      upsertSymbolDailyCloseBatch(rows);
      counts[sym] = rows.length;
      await new Promise((r) => setTimeout(r, 200));
    }
    res.json({ ok: true, from: w.from, to: w.to, counts });
  } catch (error) {
    res.status(502).json({ ok: false, error: error.message || "backfill failed" });
  }
});

app.use("/api", (_req, res) => {
  res.status(404).json({ ok: false, error: "API route not found" });
});

// 避免浏览器强缓存 HTML/JS/CSS，否则改代码后仍常见「刷新仍是旧页面」
app.use(
  express.static(path.join(__dirname), {
    setHeaders(res, filePath) {
      if (/\.(html|js|css|json|ico|svg)$/i.test(filePath)) {
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
        res.setHeader("Pragma", "no-cache");
      }
    },
  })
);

app.use((_req, res) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
  res.sendFile(path.join(__dirname, "index.html"));
});
app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`Server listening on http://0.0.0.0:${PORT}`);
});
