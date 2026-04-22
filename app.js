const STORAGE_KEY = "earning-clone-state-v2";
const API_BASE = "/api";

/** 与 index.html meta[name=stockreview-api-base] 一致；子路径部署时避免仍请求 /api/... 导致 404 */
function getApiBaseForFetch() {
  try {
    if (typeof document !== "undefined") {
      const el = document.querySelector('meta[name="stockreview-api-base"]');
      const c = el && String(el.getAttribute("content") || "").trim();
      if (c.startsWith("/")) {
        const t = c.replace(/\/+$/, "");
        return t || "/api";
      }
    }
  } catch {
    // ignore
  }
  return API_BASE;
}
const QUOTE_REFRESH_MS = 60_000;
const KLINE_DATALEN = 1023;
const CHART_FALLBACK_DAYS = 90;
const STATE_SYNC_KEYS = [
  "route",
  "useDemoData",
  "algoMode",
  "benchmark",
  "stageRange",
  "rangeDays",
  "analysisRangeMode",
  "customRangeStart",
  "customRangeEnd",
  "capitalTrendMode",
  "capitalAmount",
  "accounts",
  "selectedAccountId",
  "tradeFilterAccountId",
  "stockSortKey",
  "stockSortOrder",
  "stockAmountDisplay",
];
const DEFAULT_BENCHMARK_PRICE = {
  sh000001: 0,
  sz399001: 0,
  rt_hkHSI: 0,
  gb_inx: 0,
};
const FX_RATE_FALLBACK = {
  CNY: 1,
  HKD: 0.92,
  USD: 7.2,
};
/** 实时外汇主源：waihui123（失败则腾讯 qt 外汇） */
const WAIHUI123_FX_API = "https://www.waihui123.com/reteapi?action=get&code=USD,CNY,HKD";
/** 腾讯财经外汇（实时兜底）：qt.gtimg.cn */
const TENCENT_FOREX_SPOT_CODES = ["fx_susdcny", "fx_shkdcn"];
const TENCENT_FOREX_CODE_TO_CCY = { fx_susdcny: "USD", fx_shkdcn: "HKD" };
/** 新浪外汇日 K：一次返回全历史，param 见 server.js /api/fx/sina-dayk */
const SINA_FX_DAYK_DIRECT = {
  USD: "http://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var%20USDCNY=/NewForexService.getDayKLine?symbol=fx_susdcny",
  HKD: "http://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var%20HKDCNY=/NewForexService.getDayKLine?symbol=fx_shkdcny",
};
const SINA_FX_DAYK_VAR = { USD: "USDCNY", HKD: "HKDCNY" };
const SINA_FX_DAYK_PAIR = { USD: "usdcny", HKD: "hkdcny" };
const FX_HISTORY_DAYS = 420;
const FX_API_BASE = "https://api.frankfurter.app";
const FX_TIMEFRAME_DAYS = 120;
const DEFAULT_ACCOUNT = { id: "default", name: "默认账户", currency: "CNY", createdAt: 0 };
const MARKET_SORT_WEIGHT = { A股: 1, 港股: 2, 美股: 3, 其他: 9 };
const CHART_EDGE_SCROLL_PX = 22;

const demoTrades = [
  {
    id: crypto.randomUUID(),
    type: "trade",
    symbol: "sz300750",
    name: "宁德时代",
    side: "buy",
    price: 443.27,
    quantity: 100,
    amount: 44327,
    date: "2026-04-17",
    note: "",
    createdAt: Date.now() - 2,
  },
  {
    id: crypto.randomUUID(),
    type: "trade",
    symbol: "sh601899",
    name: "紫金矿业",
    side: "buy",
    price: 34.68,
    quantity: 300,
    amount: 10404,
    date: "2026-04-17",
    note: "",
    createdAt: Date.now() - 1,
  },
];

const state = {
  route: "earning",
  previousRoute: "earning",
  useDemoData: true,
  algoMode: "cost",
  benchmark: "none",
  stageRange: "month",
  rangeDays: 30,
  analysisRangeMode: "preset",
  customRangeStart: "",
  customRangeEnd: "",
  capitalTrendMode: "both",
  capitalAmount: 0,
  accounts: [DEFAULT_ACCOUNT],
  selectedAccountId: "all",
  tradeFilterAccountId: "all",
  stockSortKey: "default",
  stockSortOrder: "default",
  stockAmountDisplay: "native",
  analysisPanOffset: 0,
  dailyReturns: [],
  trades: [],
  quoteMap: {},
  klineMap: {},
  nameMap: {},
  quoteTime: "--",
  marketLoading: false,
  editingTradeId: null,
  activeRecordId: null,
  activeRecordSymbol: null,
  stockRecordWindow: 30,
  stockRecordOffset: 0,
  chartCrosshairMap: {},
  lastPinchDistanceMap: {},
  fxRatesToCnyByDate: {},
  /** 腾讯 qt 外汇实时：USD / HKD → 兑 CNY 中间价 */
  fxSpot: {},
  fxLoaded: false,
  fxLoading: false,
};
let apiReady = false;

const routeButtons = [...document.querySelectorAll(".bottom-tab-btn")];
const routePanes = [...document.querySelectorAll(".route-pane")];
const overviewGrid = document.getElementById("overviewGrid");
const quoteTime = document.getElementById("quoteTime");
const todayProfitMain = document.getElementById("todayProfitMain");
const monthProfitMain = document.getElementById("monthProfitMain");
const stageRangeSelect = document.getElementById("stageRangeSelect");
const accountFilterSelect = document.getElementById("accountFilterSelect");
const analysisAccountSelect = document.getElementById("analysisAccountSelect");
const tradeAccountFilterSelect = document.getElementById("tradeAccountFilterSelect");
const stockTableBody = document.getElementById("stockTableBody");
const stockCurrencyToggle = document.getElementById("stockCurrencyToggle");
const stockSortButtons = [...document.querySelectorAll(".th-sort-btn")];
const accountForm = document.getElementById("accountForm");
const accountTableBody = document.getElementById("accountTableBody");
const analysisRateSummary = document.getElementById("analysisRateSummary");
const analysisProfitSummary = document.getElementById("analysisProfitSummary");
const analysisRateChart = document.getElementById("analysisRateChart");
const analysisProfitChart = document.getElementById("analysisProfitChart");
const analysisAssetChart = document.getElementById("analysisAssetChart");
const analysisRateTooltip = document.getElementById("analysisRateTooltip");
const analysisProfitTooltip = document.getElementById("analysisProfitTooltip");
const analysisAssetTooltip = document.getElementById("analysisAssetTooltip");
const demoToggleBtn = document.getElementById("demoToggleBtn");
const quickTradeBtn = document.getElementById("quickTradeBtn");
const recordTradeBtn = document.getElementById("recordTradeBtn");
const tradeAddBtn = document.getElementById("tradeAddBtn");
const setCapitalBtn = document.getElementById("setCapitalBtn");
const algoModeSelectMine = document.getElementById("algoModeSelectMine");
const mineAlgoSummary = document.getElementById("mineAlgoSummary");
const benchmarkSelect = document.getElementById("benchmark");
const rangeChips = [...document.querySelectorAll(".range-chip")];
const customRangeRow = document.getElementById("customRangeRow");
const customRangeStartInput = document.getElementById("customRangeStart");
const customRangeEndInput = document.getElementById("customRangeEnd");
const applyCustomRangeBtn = document.getElementById("applyCustomRangeBtn");
const assetCurveModeSelect = document.getElementById("assetCurveMode");
const tradeTableBody = document.getElementById("tradeTableBody");
const tradeDialog = document.getElementById("tradeDialog");
const tradeForm = document.getElementById("tradeForm");
const closeTradeDialogBtn = document.getElementById("closeTradeDialogBtn");
const tradeTypeInput = document.getElementById("tradeType");
const tradePriceInput = document.getElementById("tradePrice");
const tradeQuantityInput = document.getElementById("tradeQuantity");
const tradeSideInput = document.getElementById("tradeSide");
const tradeAmountInput = document.getElementById("tradeAmount");
const tradeDialogTitle = document.getElementById("tradeDialogTitle");
const tradeSubmitBtn = document.getElementById("tradeSubmitBtn");
const capitalDialog = document.getElementById("capitalDialog");
const capitalForm = document.getElementById("capitalForm");
const closeCapitalDialogBtn = document.getElementById("closeCapitalDialogBtn");
const capitalAmountInput = document.getElementById("capitalAmount");
const closeStockRecordDialogBtn = document.getElementById("closeStockRecordDialogBtn");
const stockRecordTitle = document.getElementById("stockRecordTitle");
const stockRecordTime = document.getElementById("stockRecordTime");
const stockRecordPrice = document.getElementById("stockRecordPrice");
const stockRecordChange = document.getElementById("stockRecordChange");
const stockRecordChart = document.getElementById("stockRecordChart");
const stockRecordMarket = document.getElementById("stockRecordMarket");
const stockRecordRegret = document.getElementById("stockRecordRegret");
const stockRecordListBody = document.getElementById("stockRecordListBody");
const recordActionPopover = document.getElementById("recordActionPopover");
const tradeSymbolInput = document.getElementById("tradeSymbol");
const tradeNameInput = document.getElementById("tradeName");
const tradeDateInput = document.getElementById("tradeDate");
const tradeNoteInput = document.getElementById("tradeNote");
const tradeAccountInput = document.getElementById("tradeAccount");
const stockRecordTooltip = document.getElementById("stockRecordTooltip");

const chartRuntimeMap = new Map();

initialize();

async function initialize() {
  await hydrateState();
  bindEvents();
  renderAll();
  void initializeFxRates();
  await refreshMarketData();
  void fetchQuoteNames(state.trades.map((trade) => trade.symbol)).then(() => {
    renderOverviewAndStockTable();
    renderTradeTable();
    if (state.route === "stock-record" && state.activeRecordSymbol) {
      void renderStockRecordPage(state.activeRecordSymbol);
    }
  });
  window.setInterval(refreshMarketData, QUOTE_REFRESH_MS);
  window.dumpMonthlyReturnAudit = dumpMonthlyReturnAudit;
  window.buildMonthlyReturnAuditRows = buildMonthlyReturnAuditRows;
}

async function initializeFxRates() {
  if (state.fxLoaded || state.fxLoading) {
    return;
  }
  state.fxLoading = true;
  try {
    const bounds = resolveFxRangeBounds();
    state.fxRangeStart = bounds.start;
    state.fxRangeEnd = bounds.end;
    const [usdSeries, hkdSeries] = await Promise.all([
      fetchFxSeriesForCurrency("USD", bounds.start, bounds.end),
      fetchFxSeriesForCurrency("HKD", bounds.start, bounds.end),
    ]);
    const map = {};
    Object.entries(usdSeries).forEach(([date, rate]) => {
      map[date] = map[date] || {};
      map[date].USD = rate;
    });
    Object.entries(hkdSeries).forEach(([date, rate]) => {
      map[date] = map[date] || {};
      map[date].HKD = rate;
    });
    state.fxRatesToCnyByDate = map;
    state.fxLoaded = true;
    renderAll();
  } catch (error) {
    console.error("加载历史汇率失败（新浪日 K / Frankfurter），已回退固定汇率", error);
  } finally {
    state.fxLoading = false;
  }
}

function resolveFxRangeBounds() {
  const today = new Date();
  const defaultStart = new Date(today);
  defaultStart.setDate(defaultStart.getDate() - FX_HISTORY_DAYS);
  let minDate = toDateKey(defaultStart);
  for (const trade of state.trades) {
    if (trade?.date && trade.date < minDate) {
      minDate = trade.date;
    }
  }
  return { start: minDate, end: toDateKey(today) };
}

async function fetchFxSeriesForCurrency(currency, startDate, endDate) {
  const result = {};
  if (currency === "CNY") {
    result[startDate] = 1;
    result[endDate] = 1;
    return result;
  }
  try {
    const full = await fetchSinaForexDayKSeries(currency);
    if (full && Object.keys(full).length) {
      Object.entries(full).forEach(([date, rate]) => {
        if (date >= startDate && date <= endDate) {
          result[date] = rate;
        }
      });
      if (Object.keys(result).length) {
        return result;
      }
    }
  } catch {
    // fall through
  }
  let cursor = new Date(startDate);
  const end = new Date(endDate);
  while (cursor <= end) {
    const chunkStart = toDateKey(cursor);
    const chunkEndDate = new Date(cursor);
    chunkEndDate.setDate(chunkEndDate.getDate() + (FX_TIMEFRAME_DAYS - 1));
    if (chunkEndDate > end) {
      chunkEndDate.setTime(end.getTime());
    }
    const chunkEnd = toDateKey(chunkEndDate);
    try {
      const chunkRates = await fetchFxHistorySeriesFrankfurter(currency, chunkStart, chunkEnd);
      Object.assign(result, chunkRates);
    } catch {
      // ignore chunk failure
    }
    cursor.setDate(cursor.getDate() + FX_TIMEFRAME_DAYS);
  }
  return result;
}

async function fetchFxHistorySeriesFrankfurter(currency, startDate, endDate) {
  const url = `${FX_API_BASE}/${startDate}..${endDate}?from=${encodeURIComponent(currency)}&to=CNY`;
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`FX请求失败: ${currency} ${startDate}..${endDate}`);
  }
  const payload = await response.json();
  const rates = payload?.rates || {};
  const result = {};
  Object.entries(rates).forEach(([date, value]) => {
    const parsed = Number(value?.CNY);
    if (Number.isFinite(parsed) && parsed > 0) {
      result[date] = parsed;
    }
  });
  return result;
}

/**
 * 解析新浪外汇日 K JSONP：`var USDCNY=("日期,开,高,低,收,|...");`
 * 以收盘价（第 5 列）为当日 1 单位外币兑 CNY（USDCNY / HKDCNY 与新浪品种一致）。
 */
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
    if (parts.length < 5) {
      return;
    }
    const day = String(parts[0] || "").trim().slice(0, 10);
    const close = parseTencentPriceField(parts[4]);
    if (day && Number.isFinite(close) && close > 0) {
      out[day] = close;
    }
  });
  return out;
}

async function fetchSinaForexDayKSeries(currency) {
  if (currency !== "USD" && currency !== "HKD") {
    return {};
  }
  const pair = SINA_FX_DAYK_PAIR[currency];
  const varName = SINA_FX_DAYK_VAR[currency];
  const url = apiReady
    ? `${API_BASE}/fx/sina-dayk?pair=${encodeURIComponent(pair)}`
    : SINA_FX_DAYK_DIRECT[currency];
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`sina dayk ${response.status}`);
  }
  const text = await response.text();
  const trimmed = text.trim();
  if (trimmed.startsWith("{")) {
    const j = JSON.parse(trimmed);
    if (j && j.ok === false) {
      throw new Error(String(j.error || "sina proxy error"));
    }
  }
  return parseSinaForexDayKJsonp(text, varName);
}

function normalizeAccounts(rawAccounts) {
  const seen = new Set();
  const base = [];
  const input = Array.isArray(rawAccounts) ? rawAccounts : [];
  for (const raw of input) {
    const id = String(raw?.id || "").trim();
    if (!id || seen.has(id)) {
      continue;
    }
    const name = String(raw?.name || "").trim() || "未命名账户";
    const currency = ["CNY", "USD", "HKD"].includes(String(raw?.currency || "CNY").toUpperCase())
      ? String(raw.currency).toUpperCase()
      : "CNY";
    const createdAt = Number(raw?.createdAt || Date.now());
    base.push({ id, name, currency, createdAt });
    seen.add(id);
  }
  if (!seen.has(DEFAULT_ACCOUNT.id)) {
    base.unshift({ ...DEFAULT_ACCOUNT });
  } else {
    const idx = base.findIndex((item) => item.id === DEFAULT_ACCOUNT.id);
    base[idx] = {
      ...base[idx],
      name: "默认账户",
      currency: base[idx].currency || "CNY",
      createdAt: base[idx].createdAt || 0,
    };
  }
  base.sort((a, b) => {
    if (a.id === DEFAULT_ACCOUNT.id) return -1;
    if (b.id === DEFAULT_ACCOUNT.id) return 1;
    return Number(a.createdAt) - Number(b.createdAt);
  });
  return base;
}

function normalizeDailyReturnRow(raw) {
  const r = raw && typeof raw === "object" ? raw : {};
  const totalRaw = r.totalAsset != null ? r.totalAsset : r.total_asset;
  let totalAsset = null;
  if (totalRaw != null && totalRaw !== "") {
    const n = Number(totalRaw);
    totalAsset = Number.isFinite(n) ? n : null;
  }
  return {
    accountId: String(r.accountId || r.account_id || "default").trim() || "default",
    date: String(r.date || r.day || "").slice(0, 10),
    profit: Number(r.profit ?? r.pnl ?? 0) || 0,
    returnRate: Number((r.returnRate != null ? r.returnRate : r.return_rate) ?? 0) || 0,
    totalAsset,
    createdAt: Number(r.createdAt || r.created_at) || Date.now(),
  };
}

function accountOptionLabel(account) {
  if (!account || account.id === "all") {
    return "全部账户";
  }
  const name = account.name || "未命名账户";
  const currency = getCurrencyLabel(account.currency || "CNY");
  return `${name} (${currency})`;
}

function resolveValidAccountFilter(accountId) {
  if (accountId === "all") {
    return "all";
  }
  return state.accounts.some((account) => account.id === accountId) ? accountId : "all";
}

/** 记一笔默认账户：与首页/分析当前筛选一致（非「全部账户」时用当前选中账户） */
function resolveTradeFormDefaultAccountId() {
  const hasDefault = state.accounts.some((item) => item.id === DEFAULT_ACCOUNT.id);
  const fallback = hasDefault ? DEFAULT_ACCOUNT.id : state.accounts[0]?.id || DEFAULT_ACCOUNT.id;
  const sel = state.selectedAccountId;
  if (sel && sel !== "all" && state.accounts.some((a) => a.id === sel)) {
    return sel;
  }
  return fallback;
}

function getPortfolioScope() {
  const activeAccountId = resolveValidAccountFilter(state.selectedAccountId);
  const trades = getFilteredTrades(activeAccountId);
  return { accountId: activeAccountId, trades };
}

function resolveStockSortKeyValue(row, key) {
  if (key === "currentPrice") {
    return row.currentPrice;
  }
  if (key === "marketValue") {
    return row.marketValue;
  }
  if (key === "weight") {
    return row.weight;
  }
  if (key === "cost") {
    return row.cost;
  }
  if (key === "monthProfit") {
    return applyFxForOverview(row, row.monthProfitNative ?? row.monthProfit);
  }
  if (key === "monthWeight") {
    return row.monthWeight;
  }
  if (key === "yearProfit") {
    return applyFxForOverview(row, row.yearProfitNative ?? row.yearProfit);
  }
  if (key === "yearWeight") {
    return row.yearWeight;
  }
  if (key === "totalProfit") {
    return applyFxForOverview(row, row.totalProfitNative ?? row.totalProfit);
  }
  if (key === "totalRate") {
    return row.totalRate;
  }
  if (key === "todayProfit") {
    return applyFxForOverview(row, row.todayProfitNative ?? row.todayProfit);
  }
  if (key === "regretRate") {
    return row.regretRate;
  }
  if (key === "lastTradeDate") {
    return Date.parse(row.lastTradeDate || 0);
  }
  return 0;
}

function sortPositions(list) {
  const rows = [...list];
  if (!rows.length) {
    return rows;
  }
  if (state.stockSortOrder === "default" || state.stockSortKey === "default") {
    rows.sort((a, b) => {
      const marketCmp = (MARKET_SORT_WEIGHT[a.market] || 99) - (MARKET_SORT_WEIGHT[b.market] || 99);
      if (marketCmp !== 0) {
        return marketCmp;
      }
      return Date.parse(b.lastTradeDate || 0) - Date.parse(a.lastTradeDate || 0);
    });
    return rows;
  }
  const key = state.stockSortKey;
  const direction = state.stockSortOrder === "asc" ? 1 : -1;
  rows.sort((a, b) => {
    const av = resolveStockSortKeyValue(a, key);
    const bv = resolveStockSortKeyValue(b, key);
    return (av - bv) * direction;
  });
  return rows;
}

function getAccountById(accountId) {
  return state.accounts.find((item) => item.id === accountId) || DEFAULT_ACCOUNT;
}

function getFilteredTrades(accountId = "all") {
  if (accountId === "all") {
    return [...state.trades];
  }
  return state.trades.filter((trade) => trade.accountId === accountId);
}

function hasCnNameLabel(text) {
  return /[\u4e00-\u9fff]/.test(String(text || ""));
}

/** A股/港股：行情里常把代码当名称返回，不能当「已有中文名」；美股等保留英文简称。 */
function quoteNameForDisplay(symbol, rawName) {
  const s = String(rawName || "").trim();
  if (!s) {
    return "";
  }
  const m = inferMarket(normalizeSymbol(symbol));
  if (m === "A股" || m === "港股") {
    return hasCnNameLabel(s) ? s : "";
  }
  return s;
}

function getDisplayName(symbol, fallbackName = "") {
  const normalized = normalizeSymbol(symbol || "");
  const alias = normalized.replace(/^gb_/i, "");
  const fromMap = (state.nameMap[normalized] || state.nameMap[alias] || "").trim();
  const quoteName = quoteNameForDisplay(symbol, getQuoteBySymbol(symbol)?.name);
  const m = inferMarket(normalized);
  if (m === "A股" || m === "港股") {
    return (hasCnNameLabel(fromMap) ? fromMap : "") || quoteName || fallbackName || alias.toUpperCase();
  }
  return fromMap || quoteName || fallbackName || alias.toUpperCase();
}

function getQuoteBySymbol(symbol) {
  const normalized = normalizeSymbol(symbol || "");
  if (!normalized) {
    return {};
  }
  const alias = normalized.replace(/^gb_/i, "");
  return state.quoteMap[normalized] || state.quoteMap[alias] || {};
}

function getKlineBySymbol(symbol) {
  const normalized = normalizeSymbol(symbol || "");
  if (!normalized) {
    return [];
  }
  const alias = normalized.replace(/^gb_/i, "");
  return state.klineMap[normalized] || state.klineMap[alias] || [];
}

/** 从 SQLite 日收盘价表灌入 state.klineMap，优先于实时拉日 K */
async function hydrateKlineFromLocalDb() {
  if (!apiReady) {
    return;
  }
  try {
    const r = await fetch(`${API_BASE}/daily-close/for-trades`, { cache: "no-store" });
    if (!r.ok) {
      return;
    }
    const j = await r.json();
    if (!j?.ok || !j.data || typeof j.data !== "object") {
      return;
    }
    const toKlineRows = (arr) =>
      (Array.isArray(arr) ? arr : [])
        .map((row) => ({
          day: String(row.date || "").slice(0, 10),
          open: Number(row.close),
          high: Number(row.close),
          low: Number(row.close),
          close: Number(row.close),
          volume: 0,
        }))
        .filter((x) => x.day && Number.isFinite(x.close))
        .sort((a, b) => a.day.localeCompare(b.day));
    Object.entries(j.data).forEach(([sym, rows]) => {
      const list = toKlineRows(rows);
      if (!list.length) {
        return;
      }
      const normalized = normalizeSymbol(sym);
      const alias = normalized.replace(/^gb_/i, "");
      state.klineMap[normalized] = list;
      state.klineMap[alias] = list;
    });
  } catch {
    // ignore
  }
}

function getCurrencyLabel(currency) {
  if (currency === "USD") return "美元";
  if (currency === "HKD") return "港币";
  return "人民币";
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

function addCalendarDaysShanghai(y, m, d, deltaDays) {
  const t = new Date(`${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}T12:00:00+08:00`);
  t.setTime(t.getTime() + deltaDays * 86400000);
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(t);
}

/** 交易日：以北京时间 08:30 为界，区间 [D 08:30, D+1 08:30) 记为 D 日。 */
function getTradingDateKey(baseDate = new Date()) {
  const { y, m, d, h, min } = getShanghaiWallClockParts(baseDate);
  if (h < 8 || (h === 8 && min < 30)) {
    return addCalendarDaysShanghai(y, m, d, -1);
  }
  return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

async function fetchQuoteNames(symbols) {
  const targets = [...new Set(symbols.filter(Boolean).map((symbol) => normalizeSymbol(symbol)))].filter((symbol) => {
    const alias = symbol.replace(/^gb_/i, "");
    const existing = (state.nameMap[symbol] || state.nameMap[alias] || "").trim();
    const m = inferMarket(symbol);
    if (m === "A股" || m === "港股") {
      return !hasCnNameLabel(existing);
    }
    return !existing;
  });
  if (!targets.length) {
    return;
  }
  const requestToSource = new Map();
  targets.forEach((symbol) => {
    const requestSymbol = toQuoteRequestSymbol(symbol);
    if (!requestToSource.has(requestSymbol)) {
      requestToSource.set(requestSymbol, symbol);
    }
  });
  try {
    const quotes = await fetchRealtimeQuotes([...requestToSource.keys()]);
    Object.entries(quotes || {}).forEach(([requestSymbol, quote]) => {
      const sourceSymbol = normalizeSymbol(requestToSource.get(requestSymbol) || requestSymbol.replace(/^gb_/i, ""));
      const name = String(quote?.name || "").trim();
      const display = quoteNameForDisplay(sourceSymbol, name);
      if (display) {
        state.nameMap[sourceSymbol] = display;
        state.nameMap[sourceSymbol.replace(/^gb_/i, "")] = display;
      }
    });
  } catch {
    // ignore quote-name failures, keep existing display names
  }
}

/** A股/港股：新浪腾讯未返回简称时，由服务端走东方财富 f14 兜底（无需用户改代码）。 */
async function enrichNamesFromEastmoney(symbols) {
  if (!apiReady) {
    return;
  }
  const uniq = [...new Set(symbols.map((s) => normalizeSymbol(s)).filter(Boolean))];
  const need = uniq.filter((sym) => {
    const m = inferMarket(sym);
    if (m !== "A股" && m !== "港股") {
      return false;
    }
    const alias = sym.replace(/^gb_/i, "");
    const fromMap = (state.nameMap[sym] || state.nameMap[alias] || "").trim();
    const fromQuote = String(getQuoteBySymbol(sym)?.name || "").trim();
    if (hasCnNameLabel(fromMap) || hasCnNameLabel(fromQuote)) {
      return false;
    }
    return true;
  });
  if (!need.length) {
    return;
  }
  await Promise.all(
    need.map(async (sym) => {
      try {
        const response = await fetch(`${API_BASE}/stock/name?symbol=${encodeURIComponent(sym)}`, { cache: "no-store" });
        const result = await response.json();
        const name = String(result?.name || "").trim();
        if (!name) {
          return;
        }
        const normalized = normalizeSymbol(sym);
        const alias = normalized.replace(/^gb_/i, "");
        state.nameMap[normalized] = name;
        state.nameMap[alias] = name;
        const q = getQuoteBySymbol(normalized);
        if (q && typeof q === "object") {
          const merged = { ...q, name };
          state.quoteMap[normalized] = merged;
          state.quoteMap[alias] = merged;
        }
      } catch {
        // ignore single-symbol failures
      }
    })
  );
}

/**
 * 腾讯 qt.gtimg.cn / fqkline：沪深 sh/sz、港股 hk、美股 **usTICKER**（大写、无 .OQ 后缀）。
 * 实测 usFUTU.OQ 会返回 v_pv_none_match；usFUTU、usGOOG、usTSM 可正常取价。
 */
function toTencentQuoteSymbol(symbol) {
  if (!symbol) {
    return "";
  }
  const raw = String(symbol).trim().toLowerCase().replace(/\s+/g, "");
  const orig = String(symbol).trim().replace(/\s+/g, "");

  if (/^sh\d{6}$/.test(raw) || /^sz\d{6}$/.test(raw) || /^hk\d{5}$/.test(raw)) {
    return raw;
  }
  if (/^us[A-Z0-9._-]+$/i.test(orig)) {
    const base = orig
      .replace(/^us/i, "")
      .replace(/\.(OQ|N)$/i, "");
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
  return raw;
}

const SINA_KLINE_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  Referer: "https://finance.sina.com.cn/",
};

/** 本机 / 局域网打开页面时也走同源代理：浏览器直连新浪会因 CORS 失败；勿仅依赖 /api/health（apiReady）。 */
function isLikelyLanOrLocalHost() {
  if (typeof window === "undefined" || !window.location) {
    return false;
  }
  const { protocol, hostname } = window.location;
  if (protocol === "file:" || (protocol !== "http:" && protocol !== "https:")) {
    return false;
  }
  if (hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]") {
    return true;
  }
  const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(hostname);
  if (!m) {
    return false;
  }
  const a = Number(m[1]);
  const b = Number(m[2]);
  if (a === 10) {
    return true;
  }
  if (a === 172 && b >= 16 && b <= 31) {
    return true;
  }
  if (a === 192 && b === 168) {
    return true;
  }
  return false;
}

function shouldUseSinaKlineProxy() {
  return apiReady || isLikelyLanOrLocalHost();
}

/**
 * 新浪 getKLineData：A股 sh/sz；港股 rt_hk_00700；美股 gb_AAPL（Ticker 大写）。
 */
function toSinaKlineSymbol(symbol) {
  if (!symbol) {
    return "";
  }
  const n = normalizeSymbol(toQuoteRequestSymbol(symbol));
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

/** 腾讯行情 `~` 分段里的金额，可能含千分位逗号 */
function parseTencentPriceField(segment) {
  if (segment == null) {
    return NaN;
  }
  const t = String(segment).trim().replace(/,/g, "");
  const n = Number(t);
  return Number.isFinite(n) ? n : NaN;
}

/**
 * 腾讯 qt.gtimg.cn 实时：`~` 分段。文档：1 名称、2 代码、3 当前价、4 昨收、30 时间（均为 1-based 序号，对应 parts[1]…parts[4]）。
 */
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
  const time = String(parts[30] || parts[31] || "--").trim();
  const quoteDate = parseQuoteTimeToDateKey(time);
  if (!Number.isFinite(current) || current <= 0) {
    return null;
  }
  return {
    name,
    current,
    prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
    time: time || "--",
    quoteDate,
  };
}

/** 腾讯 qt 外汇：`fx_susdcny` / `fx_shkdcn`，~ 分段 3 当前价、4 昨收 */
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
    time: time || "--",
  };
}

function readTencentQuoteWindowPayload(sourceSymbol) {
  const keys = [
    `v_${sourceSymbol}`,
    `v_${sourceSymbol.replace(/\./g, "_")}`,
    `v_${sourceSymbol.replace(/\./g, "")}`,
  ];
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(window, key)) {
      const payload = window[key];
      if (payload != null) {
        return { key, payload };
      }
    }
  }
  for (const key of keys) {
    const payload = window[key];
    if (payload != null) {
      return { key, payload };
    }
  }
  return { key: keys[0], payload: undefined };
}

function toQuoteRequestSymbol(symbol) {
  if (!symbol) {
    return symbol;
  }
  if (/^gb_/i.test(symbol) || /^rt_hk/i.test(symbol) || /^sh\d{6}$/i.test(symbol) || /^sz\d{6}$/i.test(symbol) || /^hk\d{5}$/i.test(symbol)) {
    return symbol;
  }
  if (/^[a-z][a-z0-9._-]*$/i.test(symbol)) {
    return `gb_${symbol.toLowerCase()}`;
  }
  return symbol;
}

function cycleSortOrder(current) {
  if (current === "default") return "desc";
  if (current === "desc") return "asc";
  return "default";
}

async function fetchStaticSiteState() {
  try {
    const response = await fetch("/site-state.json", { cache: "no-store" });
    if (!response.ok) {
      return null;
    }
    const data = await response.json();
    if (data && typeof data === "object" && Array.isArray(data.trades)) {
      return data;
    }
    return null;
  } catch (error) {
    return null;
  }
}

async function hydrateState() {
  let parsed = null;
  let remoteParsed = null;
  let localParsed = null;
  let staticParsed = null;
  apiReady = await checkApiHealth();
  if (apiReady) {
    remoteParsed = await fetchRemoteState();
  }
  // Only use the baked-in snapshot on static hosts (GitHub Pages). When the API is up,
  // trust /api/state even if the DB is empty — otherwise site-state.json would mask a real empty database.
  if (!apiReady) {
    staticParsed = await fetchStaticSiteState();
  }
  const raw = localStorage.getItem(STORAGE_KEY);
  if (raw) {
    try {
      localParsed = JSON.parse(raw);
    } catch (error) {
      console.error("读取本地数据失败，已使用默认配置", error);
    }
  }
  if (remoteParsed && Array.isArray(remoteParsed.trades) && remoteParsed.trades.length) {
    parsed = remoteParsed;
  } else if (localParsed) {
    parsed = localParsed;
    // Auto-migrate local state to DB-backed API when backend is available.
    if (apiReady) {
      const localTrades = Array.isArray(localParsed.trades) ? localParsed.trades : [];
      if (localTrades.length) {
        void importTradesToApi(localTrades, "replace");
      }
      void pushSettingsToApi(localParsed);
      const localDaily = Array.isArray(localParsed.dailyReturns) ? localParsed.dailyReturns : [];
      if (localDaily.length) {
        void importDailyReturnsToApi(localDaily, "replace");
      }
    }
  } else if (staticParsed && Array.isArray(staticParsed.trades) && staticParsed.trades.length) {
    parsed = staticParsed;
  } else if (remoteParsed) {
    parsed = remoteParsed;
  }
  if (parsed && typeof parsed === "object") {
    state.route = parsed.route ?? state.route;
    if (state.route === "records") {
      state.route = "trade";
    }
    if (state.route === "introduction" || state.route === "account") {
      state.route = "mine";
    }
    state.useDemoData = parsed.useDemoData ?? state.useDemoData;
    state.algoMode = parsed.algoMode ?? state.algoMode;
    state.benchmark = parsed.benchmark ?? state.benchmark;
    state.stageRange = parsed.stageRange ?? state.stageRange;
    state.rangeDays = parsed.rangeDays ?? state.rangeDays;
    state.analysisRangeMode = parsed.analysisRangeMode ?? state.analysisRangeMode;
    state.customRangeStart = parsed.customRangeStart ?? state.customRangeStart;
    state.customRangeEnd = parsed.customRangeEnd ?? state.customRangeEnd;
    state.capitalTrendMode = parsed.capitalTrendMode ?? state.capitalTrendMode;
    state.capitalAmount = Number(parsed.capitalAmount ?? 0);
    state.accounts = normalizeAccounts(parsed.accounts);
    state.selectedAccountId = parsed.selectedAccountId ?? state.selectedAccountId;
    state.tradeFilterAccountId = parsed.tradeFilterAccountId ?? state.tradeFilterAccountId;
    state.stockSortKey = parsed.stockSortKey ?? state.stockSortKey;
    state.stockSortOrder = parsed.stockSortOrder ?? state.stockSortOrder;
    state.stockAmountDisplay =
      parsed.stockAmountDisplay === "cny" || parsed.stockAmountDisplay === "native"
        ? parsed.stockAmountDisplay
        : "native";
    state.trades = Array.isArray(parsed.trades) ? parsed.trades.map(normalizeTrade) : [];
    state.dailyReturns = Array.isArray(parsed.dailyReturns)
      ? parsed.dailyReturns.map(normalizeDailyReturnRow)
      : [];
  }
  if (!["month", "ytd", "total"].includes(state.stageRange)) {
    state.stageRange = "month";
  }
  if (!["preset", "custom", "all"].includes(state.analysisRangeMode)) {
    state.analysisRangeMode = "preset";
  }
  if (!["both", "principal", "market"].includes(state.capitalTrendMode)) {
    state.capitalTrendMode = "both";
  }
  if (state.stockAmountDisplay !== "cny" && state.stockAmountDisplay !== "native") {
    state.stockAmountDisplay = "native";
  }
  if (state.useDemoData && state.trades.length === 0) {
    state.trades = demoTrades.map((item) => ({ ...item }));
  }
  if (state.trades.length === 0) {
    state.useDemoData = true;
    state.trades = demoTrades.map((item) => ({ ...item }));
  }
  if (![7, 30, 90, 365].includes(Number(state.rangeDays))) {
    state.rangeDays = 30;
  }
  state.trades = state.trades.map((trade) => {
    if (!state.accounts.some((account) => account.id === trade.accountId)) {
      return { ...trade, accountId: DEFAULT_ACCOUNT.id };
    }
    return trade;
  });
  state.selectedAccountId = resolveValidAccountFilter(state.selectedAccountId);
  state.tradeFilterAccountId = resolveValidAccountFilter(state.tradeFilterAccountId);
}

function persistState() {
  const payload = {
    route: state.route,
    useDemoData: state.useDemoData,
    algoMode: state.algoMode,
    benchmark: state.benchmark,
    stageRange: state.stageRange,
    rangeDays: state.rangeDays,
    analysisRangeMode: state.analysisRangeMode,
    customRangeStart: state.customRangeStart,
    customRangeEnd: state.customRangeEnd,
    capitalTrendMode: state.capitalTrendMode,
    capitalAmount: state.capitalAmount,
    accounts: state.accounts,
    selectedAccountId: state.selectedAccountId,
    tradeFilterAccountId: state.tradeFilterAccountId,
    stockSortKey: state.stockSortKey,
    stockSortOrder: state.stockSortOrder,
    stockAmountDisplay: state.stockAmountDisplay,
    trades: state.trades,
    dailyReturns: state.dailyReturns,
  };
  localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
  if (apiReady) {
    void pushSettingsToApi(payload);
    void pushDailyReturnsToApi(payload.dailyReturns);
  }
}

async function checkApiHealth() {
  try {
    const response = await fetch(`${API_BASE}/health`, { cache: "no-store" });
    return response.ok;
  } catch (error) {
    return false;
  }
}

async function fetchRemoteState() {
  try {
    const response = await fetch(`${API_BASE}/state`, { cache: "no-store" });
    if (!response.ok) {
      return null;
    }
    const result = await response.json();
    if (!result?.ok || !result.data) {
      return null;
    }
    return result.data;
  } catch (error) {
    return null;
  }
}

async function pushSettingsToApi(payload) {
  try {
    const body = STATE_SYNC_KEYS.reduce((acc, key) => {
      acc[key] = payload[key];
      return acc;
    }, {});
    await fetch(`${API_BASE}/settings`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (error) {
    // keep localStorage as fallback when backend is unavailable
  }
}

async function pushDailyReturnsToApi(rows) {
  if (!apiReady || !Array.isArray(rows)) {
    return;
  }
  try {
    await fetch(`${API_BASE}/daily-returns/import`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "replace", rows }),
    });
  } catch (error) {
    // localStorage remains source of truth when API is down
  }
}

async function importDailyReturnsToApi(rows, mode = "replace") {
  if (!apiReady || !Array.isArray(rows) || !rows.length) {
    return;
  }
  try {
    const response = await fetch(`${API_BASE}/daily-returns/import`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: mode === "replace" ? "replace" : "append", rows }),
    });
    if (!response.ok) {
      return;
    }
    const result = await response.json();
    if (result?.ok && Array.isArray(result.data)) {
      state.dailyReturns = result.data.map(normalizeDailyReturnRow);
    }
  } catch (error) {
    console.error("同步每日收益到数据库失败", error);
  }
}

async function saveTradeToApi(trade) {
  if (!apiReady) {
    return trade;
  }
  const response = await fetch(`${API_BASE}/trades`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(trade),
  });
  if (!response.ok) {
    throw new Error("保存交易失败");
  }
  const result = await response.json();
  return result?.data ? normalizeTrade(result.data) : trade;
}

async function importTradesToApi(trades, mode = "append") {
  if (!apiReady) {
    return Array.isArray(trades) ? trades.map(normalizeTrade) : [];
  }
  const response = await fetch(`${API_BASE}/trades/import`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      mode: mode === "replace" ? "replace" : "append",
      trades,
    }),
  });
  if (!response.ok) {
    throw new Error("批量导入失败");
  }
  const result = await response.json();
  return Array.isArray(result?.data) ? result.data.map(normalizeTrade) : [];
}

async function deleteTradeFromApi(tradeId) {
  if (!apiReady) {
    return true;
  }
  const response = await fetch(`${API_BASE}/trades/${encodeURIComponent(String(tradeId || ""))}`, {
    method: "DELETE",
  });
  return response.ok;
}

function bindEvents() {
  stockCurrencyToggle?.addEventListener("click", () => {
    state.stockAmountDisplay = state.stockAmountDisplay === "cny" ? "native" : "cny";
    persistState();
    renderOverviewAndStockTable();
    renderControls();
  });

  routeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (state.route !== "stock-record") {
        state.previousRoute = state.route;
      }
      state.route = button.dataset.route;
      persistState();
      renderAll();
    });
  });

  if (demoToggleBtn) {
    demoToggleBtn.addEventListener("click", () => {
      state.useDemoData = !state.useDemoData;
      if (state.useDemoData) {
        state.trades = demoTrades.map((item) => ({ ...item }));
        if (apiReady) {
          void importTradesToApi(state.trades, "replace").catch(() => {});
        }
      }
      persistState();
      renderAll();
      refreshMarketData();
    });
  }

  algoModeSelectMine?.addEventListener("change", () => {
    state.algoMode = algoModeSelectMine.value;
    persistState();
    renderOverviewAndStockTable();
    void renderAnalysis();
    renderMineSection();
  });

  document.querySelectorAll("[data-mine-open]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const target = btn.getAttribute("data-mine-open");
      state.route = target === "accounts" ? "mine-accounts" : "mine-algo";
      persistState();
      renderAll();
    });
  });
  document.querySelectorAll("[data-mine-back]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.route = "mine";
      persistState();
      renderAll();
    });
  });

  benchmarkSelect.addEventListener("change", () => {
    state.benchmark = benchmarkSelect.value;
    persistState();
    void renderAnalysis();
    refreshMarketData();
  });

  stageRangeSelect?.addEventListener("change", () => {
    state.stageRange = stageRangeSelect.value;
    persistState();
    renderOverviewAndStockTable();
  });

  accountFilterSelect?.addEventListener("change", () => {
    state.selectedAccountId = resolveValidAccountFilter(accountFilterSelect.value);
    persistState();
    renderAll();
    refreshMarketData();
  });
  analysisAccountSelect?.addEventListener("change", () => {
    state.selectedAccountId = resolveValidAccountFilter(analysisAccountSelect.value);
    persistState();
    renderAll();
    refreshMarketData();
  });
  tradeAccountFilterSelect?.addEventListener("change", () => {
    state.tradeFilterAccountId = resolveValidAccountFilter(tradeAccountFilterSelect.value);
    persistState();
    renderTradeTable();
    renderControls();
  });
  stockSortButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const key = button.dataset.sortKey || "default";
      if (state.stockSortKey !== key) {
        state.stockSortKey = key;
        state.stockSortOrder = "desc";
      } else {
        state.stockSortOrder = cycleSortOrder(state.stockSortOrder);
        if (state.stockSortOrder === "default") {
          state.stockSortKey = "default";
        }
      }
      persistState();
      renderOverviewAndStockTable();
      renderControls();
    });
  });

  rangeChips.forEach((chip) => {
    chip.addEventListener("click", () => {
      const value = chip.dataset.range;
      if (value === "custom") {
        state.analysisRangeMode = "custom";
      } else if (value === "all") {
        state.analysisRangeMode = "all";
        state.analysisPanOffset = 0;
      } else {
        state.analysisRangeMode = "preset";
        state.rangeDays = Number(value);
        state.analysisPanOffset = 0;
      }
      persistState();
      void renderAnalysis();
      renderControls();
    });
  });

  const syncCustomRangeInputsToState = () => {
    if (customRangeStartInput) {
      state.customRangeStart = customRangeStartInput.value || "";
    }
    if (customRangeEndInput) {
      state.customRangeEnd = customRangeEndInput.value || "";
    }
  };
  customRangeStartInput?.addEventListener("change", () => {
    syncCustomRangeInputsToState();
    persistState();
  });
  customRangeStartInput?.addEventListener("input", syncCustomRangeInputsToState);
  customRangeEndInput?.addEventListener("change", () => {
    syncCustomRangeInputsToState();
    persistState();
  });
  customRangeEndInput?.addEventListener("input", syncCustomRangeInputsToState);

  applyCustomRangeBtn?.addEventListener("click", () => {
    let start = customRangeStartInput?.value || "";
    let end = customRangeEndInput?.value || "";
    if (!start && !end) {
      return;
    }
    if (!start) {
      start = getDefaultAnalysisStartDate();
    }
    if (!end) {
      end = toDateKey(new Date());
    }
    if (start > end) {
      [start, end] = [end, start];
    }
    state.customRangeStart = start;
    state.customRangeEnd = end;
    state.analysisRangeMode = "custom";
    state.analysisPanOffset = 0;
    persistState();
    renderControls();
    void renderAnalysis();
  });

  assetCurveModeSelect?.addEventListener("change", () => {
    state.capitalTrendMode = assetCurveModeSelect.value || "both";
    persistState();
    void renderAnalysis();
  });

  [quickTradeBtn, recordTradeBtn, tradeAddBtn].filter(Boolean).forEach((button) => {
    button.addEventListener("click", openNewTradeDialog);
  });
  accountForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    const formData = new FormData(accountForm);
    const name = String(formData.get("name") || "").trim();
    const currency = String(formData.get("currency") || "CNY").toUpperCase();
    if (!name) {
      return;
    }
    const account = {
      id: `acc_${Date.now()}_${Math.round(Math.random() * 1000)}`,
      name,
      currency: ["CNY", "USD", "HKD"].includes(currency) ? currency : "CNY",
      createdAt: Date.now(),
    };
    state.accounts = normalizeAccounts([...state.accounts, account]);
    accountForm.reset();
    persistState();
    renderControls();
    renderAccountSection();
  });

  closeTradeDialogBtn.addEventListener("click", () => {
    clearEditState();
    tradeDialog.close();
  });
  tradeTypeInput.addEventListener("change", applyTradeTypePreset);

  tradeForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const formData = new FormData(tradeForm);
    const type = String(formData.get("type"));
    const symbol = normalizeSymbol(String(formData.get("symbol") || ""));
    const side = normalizedSide(type, String(formData.get("side")));
    const price = Number(formData.get("price") || 0);
    const quantity = Number(formData.get("quantity") || 0);
    const explicitAmount = Number(formData.get("amount"));
    const defaultAmount = Math.abs(price * quantity);
    const amount =
      Number.isFinite(explicitAmount) && explicitAmount !== 0
        ? Math.abs(explicitAmount)
        : Math.abs(defaultAmount);

    if (!symbol) {
      return;
    }

    const trade = normalizeTrade({
      id: state.editingTradeId || crypto.randomUUID(),
      accountId: String(formData.get("accountId") || DEFAULT_ACCOUNT.id),
      type,
      symbol,
      name: String(formData.get("name") || symbol).trim(),
      side,
      price,
      quantity,
      amount,
      date: String(formData.get("date") || toDateKey(new Date())),
      note: String(formData.get("note") || "").trim(),
      createdAt: Date.now(),
    });

    state.useDemoData = false;
    let savedTrade = trade;
    try {
      savedTrade = await saveTradeToApi(trade);
    } catch (error) {
      console.error("保存交易到数据库失败，已回退本地保存", error);
    }
    if (state.editingTradeId) {
      state.trades = state.trades.filter((item) => item.id !== state.editingTradeId);
    }
    state.trades.push(savedTrade);
    state.trades.sort(sortTradeAsc);
    persistState();
    clearEditState();
    tradeDialog.close();
    renderAll();
    refreshMarketData();
  });

  if (setCapitalBtn) {
    setCapitalBtn.addEventListener("click", () => {
      capitalAmountInput.value = state.capitalAmount ? String(state.capitalAmount) : "";
      capitalDialog.showModal();
    });
  }
  closeCapitalDialogBtn?.addEventListener("click", () => capitalDialog.close());
  if (capitalForm) {
    capitalForm.addEventListener("submit", (event) => {
      event.preventDefault();
      const formData = new FormData(capitalForm);
      state.capitalAmount = Math.max(0, Number(formData.get("capitalAmount") || 0));
      persistState();
      capitalDialog.close();
      renderOverviewAndStockTable();
    });
  }

  tradeTableBody?.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-record-id]");
    if (!row) {
      return;
    }
    const id = row.dataset.recordId;
    if (!id) {
      return;
    }
    showRecordActionPopover(row, id);
  });

  document.addEventListener("click", (event) => {
    if (!recordActionPopover) {
      return;
    }
    if (recordActionPopover.contains(event.target)) {
      return;
    }
    if (event.target.closest("tr[data-record-id]")) {
      return;
    }
    hideRecordActionPopover();
  });

  recordActionPopover?.addEventListener("click", (event) => {
    const actionBtn = event.target.closest("button[data-action]");
    if (!actionBtn) {
      return;
    }
    const action = actionBtn.dataset.action;
    const tradeId = recordActionPopover.dataset.tradeId;
    hideRecordActionPopover();
    if (!tradeId) {
      return;
    }
    if (action === "edit") {
      openEditTradeDialog(tradeId);
      return;
    }
    if (action === "delete") {
      void removeTradeById(tradeId);
    }
  });

  stockTableBody?.addEventListener("click", (event) => {
    const link = event.target.closest("[data-stock-record]");
    if (!link) {
      return;
    }
    const symbol = link.dataset.stockRecord;
    openStockRecordDialog(symbol);
  });

  closeStockRecordDialogBtn?.addEventListener("click", () => {
    state.route = state.previousRoute || "earning";
    persistState();
    renderRoute();
  });
}

function applyTradeTypePreset() {
  const type = tradeTypeInput.value;
  if (type === "dividend") {
    tradeSideInput.value = "sell";
    tradePriceInput.value = "0";
    tradeQuantityInput.value = "0";
    tradeAmountInput.placeholder = "填写分红金额";
  } else if (type === "bonus" || type === "split") {
    tradeSideInput.value = "buy";
    tradePriceInput.value = "0";
    tradeAmountInput.value = "0";
    tradeAmountInput.placeholder = "默认为0";
  } else if (type === "merge") {
    tradeSideInput.value = "sell";
    tradePriceInput.value = "0";
    tradeAmountInput.value = "0";
    tradeAmountInput.placeholder = "默认为0";
  } else {
    tradeAmountInput.placeholder = "不填则默认价格*数量";
  }
}

function openNewTradeDialog() {
  clearEditState();
  tradeForm.reset();
  tradeTypeInput.value = "trade";
  applyTradeTypePreset();
  if (tradeAccountInput) {
    tradeAccountInput.value = resolveTradeFormDefaultAccountId();
  }
  tradeDateInput.value = toDateKey(new Date());
  tradeDialog.showModal();
}


function renderAll() {
  renderControls();
  renderRoute();
  renderOverviewAndStockTable();
  renderTradeTable();
  void renderAnalysis();
  if (state.route === "stock-record" && state.activeRecordSymbol) {
    void renderStockRecordPage(state.activeRecordSymbol);
  }
}

function renderMineSection() {
  if (mineAlgoSummary) {
    const labels = { cost: "成本算法", money: "资金加权", time: "时间加权" };
    mineAlgoSummary.textContent = labels[state.algoMode] || labels.cost;
  }
  if (algoModeSelectMine) {
    algoModeSelectMine.value = state.algoMode;
  }
}

function renderControls() {
  renderMineSection();
  benchmarkSelect.value = state.benchmark;
  syncAccountSelectOptions();
  if (stageRangeSelect) {
    stageRangeSelect.value = state.stageRange;
  }
  rangeChips.forEach((chip) => {
    const value = chip.dataset.range;
    const active =
      value === "custom"
        ? state.analysisRangeMode === "custom"
        : value === "all"
          ? state.analysisRangeMode === "all"
          : state.analysisRangeMode === "preset" && Number(value) === state.rangeDays;
    chip.classList.toggle("active", active);
  });
  if (customRangeRow) {
    customRangeRow.classList.toggle("hidden", state.analysisRangeMode !== "custom");
  }
  if (customRangeStartInput) {
    customRangeStartInput.value = state.customRangeStart || "";
  }
  if (customRangeEndInput) {
    customRangeEndInput.value = state.customRangeEnd || "";
  }
  if (assetCurveModeSelect) {
    assetCurveModeSelect.value = state.capitalTrendMode;
  }
  stockSortButtons.forEach((button) => {
    const key = button.dataset.sortKey || "";
    button.classList.remove("asc", "desc", "active");
    if (state.stockSortOrder !== "default" && key === state.stockSortKey) {
      button.classList.add("active", state.stockSortOrder);
    }
  });
  if (stockCurrencyToggle) {
    const cnyOn = state.stockAmountDisplay === "cny";
    stockCurrencyToggle.classList.toggle("active", cnyOn);
    stockCurrencyToggle.title = cnyOn ? "当前为人民币展示（点击切回港币/美元）" : "当前为原币种（点击切换人民币 ¥）";
    stockCurrencyToggle.setAttribute("aria-pressed", cnyOn ? "true" : "false");
  }
  renderAccountSection();
}

function syncAccountSelectOptions() {
  const options = [
    { id: "all", name: "全部账户" },
    ...state.accounts.map((account) => ({ id: account.id, name: accountOptionLabel(account) })),
  ];
  const setSelect = (select, currentValue, includeAll = true) => {
    if (!select) {
      return;
    }
    const list = includeAll
      ? options
      : state.accounts.map((account) => ({ id: account.id, name: accountOptionLabel(account) }));
    select.innerHTML = list
      .map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)}</option>`)
      .join("");
    if (list.some((item) => item.id === currentValue)) {
      select.value = currentValue;
    } else if (list.length) {
      select.value = list[0].id;
    }
  };
  setSelect(accountFilterSelect, state.selectedAccountId, true);
  setSelect(analysisAccountSelect, state.selectedAccountId, true);
  setSelect(tradeAccountFilterSelect, state.tradeFilterAccountId, true);
  setSelect(tradeAccountInput, resolveTradeFormDefaultAccountId(), false);
}

function renderAccountSection() {
  if (!accountTableBody) {
    return;
  }
  const tradeCountByAccount = state.trades.reduce((acc, trade) => {
    const key = trade.accountId || DEFAULT_ACCOUNT.id;
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  accountTableBody.innerHTML = state.accounts
    .map((account) => {
      const count = tradeCountByAccount[account.id] || 0;
      return `
        <tr>
          <td>${escapeHtml(account.name)}</td>
          <td>${getCurrencyLabel(account.currency)}</td>
          <td>${count}</td>
        </tr>
      `;
    })
    .join("");
}

function isMineRoute(route) {
  return route === "mine" || route === "mine-accounts" || route === "mine-algo";
}

function renderRoute() {
  const validRoutes = new Set([
    "earning",
    "analysis",
    "trade",
    "mine",
    "mine-accounts",
    "mine-algo",
    "stock-record",
  ]);
  if (!validRoutes.has(state.route)) {
    state.route = "earning";
  }
  routeButtons.forEach((button) => {
    const r = button.dataset.route;
    const active = r === state.route || (r === "mine" && isMineRoute(state.route));
    button.classList.toggle("active", active);
  });
  routePanes.forEach((pane) => {
    const id = String(pane.id || "").replace(/^route-/, "");
    pane.classList.toggle("active", id === state.route);
  });
  const bottomTabs = document.querySelector(".bottom-tabs");
  if (bottomTabs) {
    bottomTabs.style.display = state.route === "stock-record" ? "none" : "grid";
  }
  if (state.route === "stock-record" && state.activeRecordSymbol) {
    void renderStockRecordPage(state.activeRecordSymbol);
  }
}

function renderOverviewAndStockTable() {
  const scope = getPortfolioScope(state.selectedAccountId);
  const portfolio = computePortfolio(scope.trades);
  const vis = portfolio.visiblePositions;
  const bookCcy = portfolio.overviewBookCurrency || "CNY";
  const toOb = (p, v) => nativeToOverviewBook(p, v, bookCcy);
  let stageProfitOv = 0;
  if (state.stageRange === "month") {
    stageProfitOv = vis.reduce((s, p) => s + toOb(p, p.monthProfitNative), 0);
  } else if (state.stageRange === "ytd") {
    stageProfitOv = vis.reduce((s, p) => s + toOb(p, p.yearProfitNative), 0);
  } else {
    stageProfitOv = vis.reduce((s, p) => s + toOb(p, p.totalProfitNative), 0);
  }
  const stageRateOv =
    portfolio.overviewPrincipal > 0 ? stageProfitOv / portfolio.overviewPrincipal : 0;
  const cards = [
    { label: "总市值", value: formatOverviewPlainMoney(portfolio.totalMarketValue, bookCcy) },
    { label: "本金", value: formatOverviewPlainMoney(portfolio.overviewPrincipal, bookCcy) },
    { label: "总资产", value: formatOverviewPlainMoney(portfolio.totalAssets, bookCcy) },
    { label: "现金", value: formatOverviewPlainMoney(portfolio.overviewCash, bookCcy) },
  ];
  todayProfitMain.innerHTML = metricValueWithRate(portfolio.todayProfit, portfolio.todayRate);
  todayProfitMain.className = `profit-main ${portfolio.todayProfit >= 0 ? "up" : "down"}`;
  monthProfitMain.innerHTML = metricValueWithRate(stageProfitOv, stageRateOv);
  monthProfitMain.className = `profit-main ${stageProfitOv >= 0 ? "up" : "down"}`;

  overviewGrid.innerHTML = cards
    .map(
      (item) => `
      <article class="kpi-item">
        <p class="kpi-label">${item.label}</p>
        <p class="kpi-value">${item.value}</p>
      </article>
    `
    )
    .join("");

  const rows = sortPositions(portfolio.visiblePositions);
  if (!rows.length) {
    stockTableBody.innerHTML = `
      <tr>
        <td colspan="14"><p class="empty">暂无持仓，点击“记一笔”开始记录。</p></td>
      </tr>
    `;
    return;
  }

  stockTableBody.innerHTML = rows
    .map((row) => {
      const stockCode = row.symbol.replace(/^(sh|sz|hk|gb_)/i, "").toUpperCase();
      const tag = row.market === "A股" ? "CN" : row.market === "港股" ? "HK" : row.market === "美股" ? "US" : "OT";
      const dayClass = applyFxForOverview(row, row.todayProfitNative) >= 0 ? "up" : "down";
      const changeClass = row.dayChangeRate >= 0 ? "up" : "down";
      const totalClass = applyFxForOverview(row, row.totalProfitNative) >= 0 ? "up" : "down";
      return `
        <tr>
          <td class="stock-name">
            <strong>${escapeHtml(getDisplayName(row.symbol, row.name))}</strong>
            <span><i class="market-tag">${tag}</i> ${stockCode}</span>
          </td>
          <td class="${dayClass}">${formatStockTableMoney(row, row.todayProfitNative, 2)}</td>
          <td>
            <div class="cell-main">${formatNumber(row.currentPrice, 3)}</div>
            <div class="cell-sub ${changeClass}">${formatPercent(row.dayChangeRate)}</div>
          </td>
          <td>
            <div class="cell-main">${formatStockTableMarketValue(row)}</div>
            <div class="cell-sub">${formatNumber(row.quantity, 0)}</div>
          </td>
          <td>${formatPercent(row.weight)}</td>
          <td>${formatNumber(row.cost, 3)}</td>
          <td class="${applyFxForOverview(row, row.monthProfitNative) >= 0 ? "up" : "down"}">${formatStockTableMoney(
            row,
            row.monthProfitNative,
            2
          )}</td>
          <td>${formatPercent(row.monthWeight)}</td>
          <td class="${applyFxForOverview(row, row.yearProfitNative) >= 0 ? "up" : "down"}">${formatStockTableMoney(
            row,
            row.yearProfitNative,
            2
          )}</td>
          <td>${formatPercent(row.yearWeight)}</td>
          <td class="${totalClass}">${formatStockTableMoney(row, row.totalProfitNative, 2)}</td>
          <td class="${totalClass}">${formatPercent(row.totalRate)}</td>
          <td class="${row.regretRate >= 0 ? "up" : "down"}">${formatPercent(row.regretRate)}</td>
          <td><a href="javascript:void(0)" class="record-link" data-stock-record="${row.symbol}">记录</a></td>
        </tr>
      `;
    })
    .join("");
}

function getStageStartKey(stageRange, firstDate) {
  const today = new Date();
  const start = new Date(today);
  if (stageRange === "week") {
    start.setDate(today.getDate() - 6);
  } else if (stageRange === "month") {
    start.setDate(1);
  } else if (stageRange === "quarter") {
    start.setDate(today.getDate() - 89);
  } else if (stageRange === "ytd") {
    start.setMonth(0, 1);
  } else if (stageRange === "total" && firstDate) {
    return firstDate;
  }
  return toDateKey(start);
}

function getDefaultAnalysisStartDate() {
  const dt = new Date();
  dt.setDate(dt.getDate() - Math.max(state.rangeDays - 1, 0));
  return toDateKey(dt);
}

/** 总览区展示币种：跟随当前筛选股票账户的默认币种；「全部账户」时统一按人民币。 */
function getOverviewBookCurrency() {
  if (state.selectedAccountId === "all") {
    return "CNY";
  }
  const acc = getAccountById(state.selectedAccountId);
  const c = String(acc.currency || "CNY").toUpperCase();
  if (c === "USD" || c === "HKD" || c === "CNY") {
    return c;
  }
  return "CNY";
}

function amountCnyFromPositionNative(row, nativeVal) {
  const n = Number.isFinite(Number(nativeVal)) ? Number(nativeVal) : 0;
  if (row.currency === "CNY" || row.market === "A股") {
    return n;
  }
  return n * (validNumber(row.fxRate, 1) || 1);
}

function amountBookFromCny(amountCny, bookCcy) {
  const x = Number.isFinite(Number(amountCny)) ? Number(amountCny) : 0;
  const c = String(bookCcy || "CNY").toUpperCase();
  if (c === "CNY") {
    return x;
  }
  const usd = validNumber(getFxRateToCny("USD"), FX_RATE_FALLBACK.USD);
  const hkd = validNumber(getFxRateToCny("HKD"), FX_RATE_FALLBACK.HKD);
  if (c === "USD" && usd > 0) {
    return x / usd;
  }
  if (c === "HKD" && hkd > 0) {
    return x / hkd;
  }
  return x;
}

function nativeToOverviewBook(row, nativeVal, bookCcy) {
  return amountBookFromCny(amountCnyFromPositionNative(row, nativeVal), bookCcy);
}

function formatOverviewPlainMoney(value, bookCcy) {
  const t = formatPlainMoney(value);
  const c = String(bookCcy || "CNY").toUpperCase();
  if (c === "USD") {
    return `$${t}`;
  }
  if (c === "HKD") {
    return `HK$${t}`;
  }
  return t;
}

/**
 * 从腾讯实时行情时间串解析日历日期（YYYY-MM-DD）。常见格式 YYYYMMDDHHMMSS。
 * 无法解析时返回 null。
 */
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

/**
 * 「交易日期」：北京时间当日 08:30 至次日 08:30 算同一交易日（与列表/日界一致）。
 */
function getBeijingTradingDateKey(now = new Date()) {
  const CUTOFF = 8 * 60 + 30;
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = fmt.formatToParts(now);
  const m = {};
  for (const p of parts) {
    if (p.type !== "literal") {
      m[p.type] = p.value;
    }
  }
  const y = Number(m.year);
  const mo = Number(m.month);
  const d = Number(m.day);
  const mins = Number(m.hour || 0) * 60 + Number(m.minute || 0);
  if (mins >= CUTOFF) {
    return `${y}-${String(mo).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  }
  const yest = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const p2 = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(yest);
  const m2 = {};
  for (const p of p2) {
    if (p.type !== "literal") {
      m2[p.type] = p.value;
    }
  }
  return `${Number(m2.year)}-${String(Number(m2.month)).padStart(2, "0")}-${String(Number(m2.day)).padStart(2, "0")}`;
}

/**
 * 今日持仓价差收益：接口行情日期与当前「交易日期」一致时才计算；
 * 接口日期早于交易日期（或未解析到日期）则为 0。
 */
function shouldCountTodayPositionPnlFromQuote(quote, now = new Date()) {
  const tradingKey = getBeijingTradingDateKey(now);
  const quoteKey =
    (quote && quote.quoteDate) ||
    (quote && parseQuoteTimeToDateKey(quote.time)) ||
    null;
  if (!quoteKey) {
    return false;
  }
  return quoteKey === tradingKey;
}

/** 总览与个股人民币列：非人民币标的按当前汇率折算；原币种展示时不乘汇率。（仅个股表，受列表上的 ¥ 切换控制） */
function applyFxForOverview(row, nativeVal) {
  const cnyBook = row.currency === "CNY" || row.market === "A股";
  const n = Number.isFinite(Number(nativeVal)) ? Number(nativeVal) : 0;
  if (cnyBook) {
    return n;
  }
  if (state.stockAmountDisplay === "cny") {
    return n * (validNumber(row.fxRate, 1) || 1);
  }
  return n;
}

function computePositionStageProfit(position, stageRange, trades) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const firstTradeDate = tradeList.length
    ? [...tradeList].sort(sortTradeAsc)[0].date
    : toDateKey(new Date());
  const startKey = getStageStartKey(stageRange, firstTradeDate);
  const symbolTrades = tradeList
    .filter((trade) => trade.symbol === position.symbol)
    .sort(sortTradeAsc);
  if (!symbolTrades.length) {
    return 0;
  }

  let startQuantity = 0;
  let stageFlowNative = 0;
  symbolTrades.forEach((trade) => {
    const deltaQty = trade.side === "buy" ? trade.quantity : -trade.quantity;
    if (trade.date < startKey) {
      startQuantity += deltaQty;
    } else {
      stageFlowNative += signedAmount(trade);
    }
  });

  const startClose = getSymbolCloseBeforeDate(position.symbol, startKey, position.prevClose);
  const startMarketValueNative = startQuantity * startClose;
  const marketValueNative =
    position.marketValueNative ?? position.quantity * validNumber(position.currentPrice, 0);
  return marketValueNative - startMarketValueNative - stageFlowNative;
}

function getSymbolCloseBeforeDate(symbol, dateKey, fallbackPrice) {
  const kline = getKlineBySymbol(symbol);
  for (let i = kline.length - 1; i >= 0; i -= 1) {
    const item = kline[i];
    if (item.day < dateKey && Number.isFinite(Number(item.close))) {
      return Number(item.close);
    }
  }
  const quote = getQuoteBySymbol(symbol);
  return validNumber(
    fallbackPrice,
    quote?.prevClose,
    quote?.current,
    0
  );
}

function renderTradeTable() {
  if (!tradeTableBody) {
    return;
  }
  const trades = getFilteredTrades(state.tradeFilterAccountId);
  if (!trades.length) {
    tradeTableBody.innerHTML = `
      <tr>
        <td colspan="6"><p class="empty">暂无交易记录，点击上方“记一笔”新增。</p></td>
      </tr>
    `;
    return;
  }
  const sorted = [...trades].sort(sortTradeDesc);
  tradeTableBody.innerHTML = sorted
    .map((trade) => {
      return `
        <tr class="trade-row" data-record-id="${trade.id}">
          <td>${trade.date.replace(/-/g, "/")}</td>
          <td>${escapeHtml(getDisplayName(trade.symbol, trade.name))}</td>
          <td class="type-cell">${typeLabel(trade.type)}</td>
          <td class="num">${formatNumber(trade.price, 2)}</td>
          <td class="num">${formatNumber(trade.quantity, 0)}</td>
          <td class="num ${trade.side === "buy" ? "down" : "up"}">${
            trade.side === "buy" ? "-" : "+"
          }${formatNumber(trade.amount, 2)}</td>
        </tr>
      `;
    })
    .join("");
}

function showRecordActionPopover(row, tradeId) {
  if (!recordActionPopover) {
    return;
  }
  const rect = row.getBoundingClientRect();
  recordActionPopover.dataset.tradeId = tradeId;
  recordActionPopover.style.top = `${window.scrollY + rect.bottom - 2}px`;
  recordActionPopover.style.left = `${window.scrollX + rect.right - 118}px`;
  recordActionPopover.classList.add("show");
}

function hideRecordActionPopover() {
  if (!recordActionPopover) {
    return;
  }
  recordActionPopover.classList.remove("show");
  recordActionPopover.dataset.tradeId = "";
}

function openEditTradeDialog(tradeId) {
  const trade = state.trades.find((item) => item.id === tradeId);
  if (!trade) {
    return;
  }
  state.editingTradeId = tradeId;
  if (tradeDialogTitle) {
    tradeDialogTitle.textContent = "修改交易";
  }
  if (tradeSubmitBtn) {
    tradeSubmitBtn.textContent = "保存修改";
  }
  tradeTypeInput.value = trade.type;
  tradeSymbolInput.value = trade.symbol;
  tradeNameInput.value = trade.name;
  tradeSideInput.value = trade.side;
  tradePriceInput.value = trade.price;
  tradeQuantityInput.value = trade.quantity;
  tradeAmountInput.value = trade.amount;
  tradeDateInput.value = trade.date;
  tradeNoteInput.value = trade.note || "";
  if (tradeAccountInput) {
    tradeAccountInput.value = trade.accountId || DEFAULT_ACCOUNT.id;
  }
  applyTradeTypePreset();
  tradeDialog.showModal();
}

function clearEditState() {
  state.editingTradeId = null;
  if (tradeDialogTitle) {
    tradeDialogTitle.textContent = "新建交易";
  }
  if (tradeSubmitBtn) {
    tradeSubmitBtn.textContent = "保存交易";
  }
}

async function removeTradeById(tradeId) {
  try {
    await deleteTradeFromApi(tradeId);
  } catch (error) {
    console.error("删除数据库交易失败，继续执行本地删除", error);
  }
  state.trades = state.trades.filter((item) => item.id !== tradeId);
  if (state.trades.length === 0) {
    state.useDemoData = true;
    state.trades = demoTrades.map((item) => ({ ...item }));
    if (apiReady) {
      try {
        await importTradesToApi(state.trades, "replace");
      } catch (error) {
        console.error("同步演示交易到数据库失败", error);
      }
    }
  }
  persistState();
  renderAll();
  refreshMarketData();
}

/** 与 analysis_daily_snapshot.profit_cny 口径一致：今日各标的当日盈亏按即期汇率折算人民币 */
function todayProfitCnyForAnalysisSnapshot(portfolio) {
  return portfolio.visiblePositions.reduce((s, p) => {
    const n = Number(p.todayProfitNative) || 0;
    if (p.currency === "CNY" || p.market === "A股") {
      return s + n;
    }
    return s + n * (validNumber(p.fxRate, 1) || 1);
  }, 0);
}

/**
 * 分析 Tab 最后一行对齐首页总览：总市值、本金、当日 profit_cny（与总览「今日」同口径）。
 * total_profit 仍按库里「日收益累加」延伸：昨日累计 + 今日 profit_cny，避免与历史点混用「持仓成本法 totalProfit」导致曲线断层。
 */
function mergeAnalysisSliceWithLive(sliceRows, portfolio, todayKey, liveModeRate) {
  const mv = portfolio.totalMarketValue;
  const todayP = todayProfitCnyForAnalysisSnapshot(portfolio);
  const next = sliceRows.map((r) => ({ ...r }));
  const hit = next.findIndex((r) => r.date === todayKey);
  const cumFromPrev = (idx) => {
    if (idx <= 0) {
      return 0;
    }
    return Number(next[idx - 1].totalProfit) || 0;
  };
  if (hit >= 0) {
    next[hit] = {
      ...next[hit],
      marketValue: mv,
      principal: portfolio.principal,
      totalProfit: cumFromPrev(hit) + todayP,
      profitCny: todayP,
      totalRateCost: state.algoMode === "cost" ? liveModeRate : next[hit].totalRateCost,
      totalRateTwr: state.algoMode === "time" ? liveModeRate : next[hit].totalRateTwr,
      totalRateDietz: state.algoMode === "money" ? liveModeRate : next[hit].totalRateDietz,
    };
    return next;
  }
  const last = next[next.length - 1];
  if (last && last.date < todayKey) {
    const lastCum = Number(last.totalProfit) || 0;
    next.push({
      ...last,
      date: todayKey,
      profitCny: todayP,
      marketValue: mv,
      principal: portfolio.principal,
      totalProfit: lastCum + todayP,
      totalRateCost: state.algoMode === "cost" ? liveModeRate : last.totalRateCost,
      totalRateTwr: state.algoMode === "time" ? liveModeRate : last.totalRateTwr,
      totalRateDietz: state.algoMode === "money" ? liveModeRate : last.totalRateDietz,
      fxHkdCny: last.fxHkdCny,
      fxUsdCny: last.fxUsdCny,
    });
  }
  return next;
}

function renderAnalysisFromHistory() {
  const scope = getPortfolioScope();
  const portfolio = computePortfolio(scope.trades);
  const history = buildPortfolioHistory(portfolio.positions, scope.trades);
  const selected = resolveAnalysisRange(history);
  const mySeries = computeModeSeries(selected, state.algoMode);
  const benchSeries = buildBenchmarkSeries(selected);
  const profitSeries = buildProfitSeries(selected);
  const assetSeries = buildAssetSeries(selected, portfolio.principal);

  const ratePayload = drawLineChart(mySeries, benchSeries);
  const profitPayload = drawDualLineChart(
    analysisProfitChart,
    profitSeries.map((item) => ({ date: item.date, value: item.value })),
    null,
    "#f45a68",
    null,
    {
      keyA: "profit",
      labelA: "收益",
      yAxisMode: "left",
      leftLabel: "收益",
      valueFormatter: (value) => formatNumber(value, 2),
      axisFormatter: (value) => formatNumber(value, 2),
    }
  );
  const assetPayload = drawAssetChart(assetSeries);

  const refreshAnalysisView = () => {
    renderControls();
    void renderAnalysis();
  };

  const rateHasBenchmark = state.benchmark !== "none";
  bindInteractiveChart(analysisRateChart, analysisRateTooltip, () => ratePayload, {
    mode: "analysis",
    onRefresh: refreshAnalysisView,
    valueFormatter: (_value, key) => {
      if (key === "benchmark" && !rateHasBenchmark) {
        return "--";
      }
      return `${formatNumber(_value, 2)}%`;
    },
  });
  bindInteractiveChart(analysisProfitChart, analysisProfitTooltip, () => profitPayload, {
    mode: "analysis",
    onRefresh: refreshAnalysisView,
    valueFormatter: (value) => formatNumber(value, 2),
  });
  bindInteractiveChart(analysisAssetChart, analysisAssetTooltip, () => assetPayload, {
    mode: "analysis",
    onRefresh: refreshAnalysisView,
    valueFormatter: (value) => formatNumber(value, 2),
  });

  const lastMy = mySeries.at(-1)?.rate ?? 0;
  const lastBench = benchSeries.at(-1)?.rate ?? 0;
  const lastProfit = profitSeries.at(-1)?.value ?? 0;
  const excess = lastMy - lastBench;
  if (analysisRateSummary) {
    analysisRateSummary.textContent =
      state.benchmark === "none"
        ? `我的收益率 ${formatPercent(lastMy)}`
        : `我的 ${formatPercent(lastMy)} / 基准 ${formatPercent(lastBench)} / 对比 ${formatPercent(excess)}`;
  }
  if (analysisProfitSummary) {
    analysisProfitSummary.textContent = `累计收益 ${formatSignedMoney(lastProfit, 2)}`;
  }
}

async function renderAnalysis() {
  const scope = getPortfolioScope();
  const portfolio = computePortfolio(scope.trades);
  const todayKey = toDateKey(new Date());
  const historyFull = buildPortfolioHistory(portfolio.positions, scope.trades);
  const liveModeRate = computeModeSeries(historyFull, state.algoMode).at(-1)?.rate ?? 0;

  let dbRows = [];
  if (apiReady) {
    try {
      const aid = state.selectedAccountId === "all" ? "all" : state.selectedAccountId;
      const res = await fetch(
        `${API_BASE}/analysis-daily?accountId=${encodeURIComponent(aid)}&from=1970-01-01&to=2099-12-31`,
        { cache: "no-store" }
      );
      const j = await res.json();
      if (j?.ok && Array.isArray(j.data) && j.data.length) {
        dbRows = j.data;
      }
    } catch (error) {
      console.warn("加载 analysis_daily 失败，回退本地计算", error);
    }
  }

  if (!dbRows.length) {
    renderAnalysisFromHistory();
    return;
  }

  const sorted = [...dbRows].sort((a, b) => a.date.localeCompare(b.date));
  const pseudoHistory = sorted.map((row) => ({
    date: row.date,
    value: row.marketValue,
    flow: row.profitCny,
  }));
  const selectedPh = resolveAnalysisRange(pseudoHistory);
  const dateSet = new Set(selectedPh.map((p) => p.date));
  let sliceRows = sorted.filter((row) => dateSet.has(row.date));
  sliceRows = mergeAnalysisSliceWithLive(sliceRows, portfolio, todayKey, liveModeRate);

  const mySeries = sliceRows.map((row, idx, arr) => {
    const isLast = idx === arr.length - 1 && row.date === todayKey;
    let r = row.totalRateCost;
    if (state.algoMode === "time") {
      r = row.totalRateTwr;
    }
    if (state.algoMode === "money") {
      r = row.totalRateDietz;
    }
    if (isLast) {
      r = liveModeRate;
    }
    return { date: row.date, rate: r };
  });
  const benchSeries = buildBenchmarkSeries(selectedPh);
  const profitSeries = sliceRows.map((row) => ({ date: row.date, value: row.totalProfit }));
  const assetSeries = sliceRows.map((row) => ({
    date: row.date,
    principal: row.principal,
    market: row.marketValue,
  }));

  const ratePayload = drawLineChart(mySeries, benchSeries);
  const profitPayload = drawDualLineChart(
    analysisProfitChart,
    profitSeries.map((item) => ({ date: item.date, value: item.value })),
    null,
    "#f45a68",
    null,
    {
      keyA: "profit",
      labelA: "收益",
      yAxisMode: "left",
      leftLabel: "收益",
      valueFormatter: (value) => formatNumber(value, 2),
      axisFormatter: (value) => formatNumber(value, 2),
    }
  );
  const assetPayload = drawAssetChart(assetSeries);

  const refreshAnalysisView = () => {
    renderControls();
    void renderAnalysis();
  };

  const rateHasBenchmark = state.benchmark !== "none";
  bindInteractiveChart(analysisRateChart, analysisRateTooltip, () => ratePayload, {
    mode: "analysis",
    onRefresh: refreshAnalysisView,
    valueFormatter: (_value, key) => {
      if (key === "benchmark" && !rateHasBenchmark) {
        return "--";
      }
      return `${formatNumber(_value, 2)}%`;
    },
  });
  bindInteractiveChart(analysisProfitChart, analysisProfitTooltip, () => profitPayload, {
    mode: "analysis",
    onRefresh: refreshAnalysisView,
    valueFormatter: (value) => formatNumber(value, 2),
  });
  bindInteractiveChart(analysisAssetChart, analysisAssetTooltip, () => assetPayload, {
    mode: "analysis",
    onRefresh: refreshAnalysisView,
    valueFormatter: (value) => formatNumber(value, 2),
  });

  const lastMy = mySeries.at(-1)?.rate ?? 0;
  const lastBench = benchSeries.at(-1)?.rate ?? 0;
  const lastProfit = profitSeries.at(-1)?.value ?? 0;
  const excess = lastMy - lastBench;
  if (analysisRateSummary) {
    analysisRateSummary.textContent =
      state.benchmark === "none"
        ? `我的收益率 ${formatPercent(lastMy)}`
        : `我的 ${formatPercent(lastMy)} / 基准 ${formatPercent(lastBench)} / 对比 ${formatPercent(excess)}`;
  }
  if (analysisProfitSummary) {
    analysisProfitSummary.textContent = `累计收益 ${formatSignedMoney(lastProfit, 2)}`;
  }
}

function resolveAnalysisRange(history) {
  if (!history.length) {
    return [{ date: toDateKey(new Date()), value: 0, flow: 0 }];
  }
  if (state.analysisRangeMode === "all") {
    return history.slice();
  }
  if (state.analysisRangeMode === "custom") {
    let start = state.customRangeStart || history[0].date;
    let end = state.customRangeEnd || history[history.length - 1].date;
    if (start > end) {
      [start, end] = [end, start];
    }
    const picked = history.filter((point) => point.date >= start && point.date <= end);
    if (picked.length) {
      return picked;
    }
  }
  const windowSize = Math.min(Math.max(state.rangeDays, 2), history.length);
  const maxOffset = Math.max(0, history.length - windowSize);
  const offset = Math.max(0, Math.min(maxOffset, Number(state.analysisPanOffset || 0)));
  state.analysisPanOffset = offset;
  const end = history.length - offset;
  const start = Math.max(0, end - windowSize);
  return history.slice(start, end);
}

function buildProfitSeries(points) {
  if (!points.length) {
    return [{ date: toDateKey(new Date()), value: 0 }];
  }
  const startClose = points[0].value - points[0].flow;
  let sumFlow = 0;
  return points.map((point) => {
    sumFlow += point.flow;
    return {
      date: point.date,
      value: point.value - startClose - sumFlow,
    };
  });
}

function buildAssetSeries(points, principalFallback) {
  if (!points.length) {
    return [{ date: toDateKey(new Date()), principal: principalFallback || 0, market: 0 }];
  }
  let sigmaFlow = 0;
  return points.map((point) => {
    sigmaFlow += point.flow;
    const principal = Math.max(principalFallback, sigmaFlow, 0);
    return {
      date: point.date,
      principal,
      market: point.value,
    };
  });
}

async function openStockRecordDialog(symbol) {
  state.activeRecordSymbol = symbol;
  state.previousRoute = state.route;
  state.route = "stock-record";
  state.stockRecordWindow = 30;
  state.stockRecordOffset = 0;
  renderAll();
  window.scrollTo(0, 0);
  persistState();

  await ensureSymbolData(symbol);
  await renderStockRecordPage(symbol);
  // wait for layout settle on mobile after route switch
  window.setTimeout(() => void renderStockRecordPage(symbol), 40);
}

async function renderStockRecordPage(symbol) {
  const scope = getPortfolioScope();
  const position = computePortfolio(scope.trades).positions.find((item) => item.symbol === symbol);
  if (!position) {
    return;
  }
  const symbolTrades = scope.trades
    .filter((item) => item.symbol === symbol)
    .sort(sortTradeDesc);
  const quote = getQuoteBySymbol(symbol);
  const current = validNumber(quote.current, position.currentPrice);
  const prev = validNumber(quote.prevClose, position.prevClose, current);
  const change = prev > 0 ? (current - prev) / prev : 0;

  stockRecordTitle.textContent = `${getDisplayName(symbol, position.name)}(${symbol.toUpperCase()})`;
  stockRecordTime.textContent = quote.time || state.quoteTime || "--";
  stockRecordPrice.textContent = formatNumber(current, 3);
  stockRecordPrice.className = `stock-record-price ${change >= 0 ? "up" : "down"}`;
  stockRecordChange.textContent = `${formatSignedMoney(current - prev, 2)} ${formatPercent(change)}`;
  stockRecordChange.className = `stock-record-change ${change >= 0 ? "up" : "down"}`;
  stockRecordMarket.textContent = marketLabel(position.market);
  stockRecordRegret.textContent = `后悔率 ${formatPercent(position.regretRate)}`;
  stockRecordRegret.className = `${position.regretRate >= 0 ? "up" : "down"}`;

  stockRecordListBody.innerHTML = symbolTrades
    .map(
      (trade) => `
      <tr>
        <td>${trade.date.replace(/-/g, "/")}</td>
        <td>${trade.side === "buy" ? "买入" : "卖出"}</td>
        <td>${formatNumber(trade.price, 2)}</td>
        <td>${formatNumber(trade.quantity, 0)}</td>
        <td class="${trade.side === "buy" ? "down" : "up"}">${trade.side === "buy" ? "-" : "+"}${formatNumber(
          trade.amount,
          2
        )}</td>
      </tr>
    `
    )
    .join("");

  let pnlByDate = {};
  if (apiReady) {
    try {
      const aid = state.selectedAccountId === "all" ? "all" : state.selectedAccountId;
      const res = await fetch(
        `${API_BASE}/symbol-daily?accountId=${encodeURIComponent(aid)}&symbol=${encodeURIComponent(
          normalizeSymbol(symbol)
        )}&from=2000-01-01&to=2099-12-31`,
        { cache: "no-store" }
      );
      const j = await res.json();
      if (j?.ok && Array.isArray(j.data)) {
        for (const row of j.data) {
          if (row.symbol === normalizeSymbol(symbol)) {
            pnlByDate[row.date] = Number(row.dayPnlNative) || 0;
          }
        }
      }
    } catch (error) {
      console.warn("symbol-daily fetch failed", error);
    }
  }

  drawStockRecordChart(symbol, symbolTrades, pnlByDate);
}

async function ensureSymbolData(symbol) {
  try {
    const quoteMap = await fetchRealtimeQuotes([symbol]);
    const normalizedSymbol = normalizeSymbol(symbol);
    const alias = normalizedSymbol.replace(/^gb_/i, "");
    if (quoteMap[symbol]) {
      state.quoteMap[normalizedSymbol] = quoteMap[symbol];
      state.quoteMap[alias] = quoteMap[symbol];
      state.quoteTime = quoteMap[symbol].time || state.quoteTime;
        const nm = String(quoteMap[symbol]?.name || "").trim();
      const display = quoteNameForDisplay(normalizedSymbol, nm);
      if (display) {
        state.nameMap[normalizedSymbol] = display;
        state.nameMap[alias] = display;
      }
    }
  } catch (error) {
    console.error("加载个股实时行情失败", error);
  }
  if (!getQuoteBySymbol(symbol)?.current || !Number.isFinite(getQuoteBySymbol(symbol)?.current)) {
    const latest = await fetchLatestQuoteFromDailyKlineFallback(symbol);
    if (latest) {
      const normalizedSymbol = normalizeSymbol(symbol);
      const alias = normalizedSymbol.replace(/^gb_/i, "");
      state.quoteMap[normalizedSymbol] = latest;
      state.quoteMap[alias] = latest;
      state.quoteTime = latest.time || state.quoteTime;
    }
  }

  if (!supportsKline(symbol)) {
    return;
  }
  try {
    if (!getKlineBySymbol(symbol).length) {
      const list = await fetchKlineData(symbol);
      if (list.length) {
        const normalizedSymbol = normalizeSymbol(symbol);
        const alias = normalizedSymbol.replace(/^gb_/i, "");
        state.klineMap[normalizedSymbol] = list;
        state.klineMap[alias] = list;
      } else {
        const fallback = buildFallbackKlineFromTrades(symbol);
        const normalizedSymbol = normalizeSymbol(symbol);
        const alias = normalizedSymbol.replace(/^gb_/i, "");
        state.klineMap[normalizedSymbol] = fallback;
        state.klineMap[alias] = fallback;
      }
    }
    await supplementKlineForMonthBoundary(symbol);
  } catch (error) {
    console.error("加载个股K线失败", error);
    if (!getKlineBySymbol(symbol).length) {
      const fallback = buildFallbackKlineFromTrades(symbol);
      const normalizedSymbol = normalizeSymbol(symbol);
      const alias = normalizedSymbol.replace(/^gb_/i, "");
      state.klineMap[normalizedSymbol] = fallback;
      state.klineMap[alias] = fallback;
    }
  }
}

function ensureSymbolPrefixForQuote(symbol) {
  const normalized = normalizeSymbol(symbol || "");
  if (/^sh600750$/i.test(normalized)) {
    return "sz300750";
  }
  return normalized;
}

function buildFallbackKlineFromTrades(symbol) {
  const scope = getPortfolioScope();
  const symbolTrades = scope.trades
    .filter((item) => item.symbol === symbol)
    .sort(sortTradeAsc);
  if (!symbolTrades.length) {
    return [];
  }
  const start = new Date(symbolTrades[0].date);
  const end = new Date();
  const closeSeed = validNumber(symbolTrades[symbolTrades.length - 1].price, 1);
  const rows = [];
  let cursor = new Date(start);
  let prev = closeSeed;
  while (cursor <= end && rows.length < CHART_FALLBACK_DAYS) {
    const day = toDateKey(cursor);
    const trade = symbolTrades.find((item) => item.date === day);
    const close = validNumber(trade?.price, prev);
    rows.push({
      day,
      open: close,
      high: close,
      low: close,
      close,
      volume: 0,
    });
    prev = close;
    cursor.setDate(cursor.getDate() + 1);
  }
  return rows;
}

function marketLabel(market) {
  if (market === "A股") {
    return "CN 沪深默认";
  }
  if (market === "港股") {
    return "HK 港股默认";
  }
  if (market === "美股") {
    return "US 美股默认";
  }
  return "OT 其他市场";
}

function drawStockRecordChart(symbol, symbolTrades, pnlByDate = {}) {
  const canvas = stockRecordChart;
  if (!canvas) {
    return;
  }
  const kline = getKlineBySymbol(symbol);
  const sortedTrades = [...symbolTrades].sort(sortTradeAsc);
  const source =
    kline.length > 1
      ? kline.map((item) => ({ date: item.day, price: Number(item.close) }))
      : sortedTrades.map((item) => ({ date: item.date, price: validNumber(item.price, 0) }));
  if (!source.length) {
    const ctx = canvas.getContext("2d");
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    return;
  }
  const totalCount = source.length;
  const windowSize = Math.max(12, Math.min(totalCount, Number(state.stockRecordWindow || 60)));
  const maxOffset = Math.max(0, totalCount - windowSize);
  const offset = Math.max(0, Math.min(maxOffset, Number(state.stockRecordOffset || 0)));
  state.stockRecordOffset = offset;
  const end = totalCount - offset;
  const start = Math.max(0, end - windowSize);
  const visible = source.slice(start, end);
  const qtyByDate = {};
  let qty = 0;
  sortedTrades.forEach((trade) => {
    qty += trade.side === "buy" ? trade.quantity : -trade.quantity;
    qtyByDate[trade.date] = qty;
  });
  let rollingQty = 0;
  const useDbPnl = Object.keys(pnlByDate).length > 0;
  const values = visible.map((item) => {
    if (qtyByDate[item.date] != null) {
      rollingQty = qtyByDate[item.date];
    }
    const pnlVal = useDbPnl ? validNumber(pnlByDate[item.date], 0) : rollingQty;
    return { date: item.date, price: validNumber(item.price, 0), qty: rollingQty, pnl: pnlVal };
  });
  const rightLabel = useDbPnl ? "日收益(原币)" : "持仓股数";
  const payload = buildChartPayload(
    [
      {
        key: "price",
        label: "股价",
        color: "#4091e0",
        axis: "left",
        values: values.map((item) => ({ date: item.date, value: item.price })),
      },
      {
        key: "qty",
        label: rightLabel,
        color: "#ff4d4f",
        axis: "right",
        values: values.map((item) => ({ date: item.date, value: useDbPnl ? item.pnl : item.qty })),
      },
    ],
    {
      labels: { price: "股价", qty: rightLabel },
      yAxisMode: "left-right",
      xMin: 52,
      xMax: canvas.width - 28,
      yMin: 20,
      yMax: canvas.height - 36,
    }
  );
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  drawChartGrid(ctx, canvas.width, canvas.height, payload);
  payload.seriesList.forEach((series) => {
    drawSeries(ctx, series.values, payload.mapX, payload.mapY, series.color || "#2f80f6");
  });
  const pointByDate = Object.fromEntries(values.map((item, idx) => [item.date, idx]));
  sortedTrades.forEach((trade) => {
    const idx = pointByDate[trade.date];
    if (idx == null) {
      return;
    }
    const point = payload.seriesMap.price.values[idx];
    if (!point) {
      return;
    }
    ctx.fillStyle = trade.side === "buy" ? "#3b7bf6" : "#ffffff";
    ctx.strokeStyle = "#3b7bf6";
    ctx.beginPath();
    ctx.arc(point.x, point.y, 5, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
  });
  drawAxisLabels(ctx, payload, {
    leftLabel: "股价",
    rightLabel: useDbPnl ? rightLabel : "股数",
    xLabel: "日期",
    valueFormatter: (value, axis, key) => {
      if (key === "qty" || axis === "right") {
        return useDbPnl ? formatNumber(value, 2) : formatNumber(value, 0);
      }
      return formatNumber(value, 2);
    },
  });
  drawCrosshairOverlay(ctx, payload, canvas.id, (value, key, axis) => {
    if (key === "qty" || axis === "right") {
      return useDbPnl ? formatNumber(value, 2) : formatNumber(value, 0);
    }
    return formatNumber(value, 2);
  });
  bindInteractiveChart(canvas, stockRecordTooltip, () => payload, {
    mode: "stock",
    onRefresh: () => drawStockRecordChart(symbol, symbolTrades, pnlByDate),
    valueFormatter: (value, key, axis) => {
      if (key === "qty" || axis === "right") {
        return useDbPnl ? formatNumber(value, 2) : formatNumber(value, 0);
      }
      return formatNumber(value, 2);
    },
  });
}

function computePortfolio(trades = state.trades) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const grouped = new Map();
  const sortedTrades = [...tradeList].sort(sortTradeAsc);

  for (const trade of sortedTrades) {
    if (!grouped.has(trade.symbol)) {
      grouped.set(trade.symbol, {
        symbol: trade.symbol,
        name: trade.name || trade.symbol,
        market: inferMarket(trade.symbol),
        quantity: 0,
        sigmaAmount: 0,
        lastTradePrice: trade.price,
        lastTradeSide: trade.side,
        lastTradeDate: trade.date,
      });
    }
    const item = grouped.get(trade.symbol);
    item.name = trade.name || item.name;
    item.market = inferMarket(trade.symbol);
    item.quantity += trade.side === "buy" ? trade.quantity : -trade.quantity;
    item.sigmaAmount += signedAmount(trade);
    if (trade.price > 0) {
      item.lastTradePrice = trade.price;
    }
    item.lastTradeSide = trade.side;
    item.lastTradeDate = trade.date;
  }

  const positions = [...grouped.values()].map((item) => {
    const quote = getQuoteBySymbol(item.symbol);
    const market = inferMarket(item.symbol);
    const currency = getSymbolCurrency(item.symbol, market);
    const fxRate = getFxRateToCny(currency);
    const currentPrice = validNumber(quote.current, item.lastTradePrice);
    const prevClose = validNumber(quote.prevClose, currentPrice);
    const marketValueNative = item.quantity * currentPrice;
    const yesterdayValueNative = item.quantity * prevClose;
    const sigmaAmountNative = item.sigmaAmount;
    const marketValue = currency === "CNY" ? marketValueNative : marketValueNative * fxRate;
    const yesterdayValue = currency === "CNY" ? yesterdayValueNative : yesterdayValueNative * fxRate;
    const sigmaAmountCny = currency === "CNY" ? sigmaAmountNative : sigmaAmountNative * fxRate;
    const cost = item.quantity !== 0 ? item.sigmaAmount / item.quantity : 0;
    const totalProfitNative = marketValueNative - sigmaAmountNative;
    const profitRate =
      Math.abs(sigmaAmountNative) > 0 ? totalProfitNative / Math.abs(sigmaAmountNative) : 0;
    const countTodayPnl = shouldCountTodayPositionPnlFromQuote(quote);
    const todayProfitNative = countTodayPnl
      ? item.quantity * (currentPrice - prevClose)
      : 0;
    const dayChangeRate = prevClose > 0 ? (currentPrice - prevClose) / prevClose : 0;
    const regretRate =
      item.lastTradePrice > 0 ? (currentPrice - item.lastTradePrice) / item.lastTradePrice : 0;
    return {
      ...item,
      market,
      currency,
      fxRate,
      currentPrice,
      prevClose,
      marketValueNative,
      yesterdayValueNative,
      sigmaAmountNative,
      marketValue,
      yesterdayValue,
      sigmaAmountCny,
      cost,
      totalProfitNative,
      profitRate,
      todayProfitNative,
      dayChangeRate,
      regretRate,
      totalRate: profitRate,
      totalProfit: totalProfitNative,
      todayProfit: todayProfitNative,
    };
  });

  positions.forEach((item) => {
    item.monthProfitNative = computePositionStageProfit(item, "month", tradeList);
    item.yearProfitNative = computePositionStageProfit(item, "ytd", tradeList);
    item.monthProfit = item.monthProfitNative;
    item.yearProfit = item.yearProfitNative;
  });
  const visiblePositions = positions.filter((item) => item.quantity > 0);
  const monthDen = visiblePositions.reduce(
    (sum, item) => sum + Math.abs(applyFxForOverview(item, item.monthProfitNative)),
    0
  );
  const yearDen = visiblePositions.reduce(
    (sum, item) => sum + Math.abs(applyFxForOverview(item, item.yearProfitNative)),
    0
  );
  visiblePositions.forEach((item) => {
    const mp = applyFxForOverview(item, item.monthProfitNative);
    const yp = applyFxForOverview(item, item.yearProfitNative);
    item.monthWeight = monthDen !== 0 ? mp / monthDen : 0;
    item.yearWeight = yearDen !== 0 ? yp / yearDen : 0;
  });

  const sigmaAmountAll = tradeList.reduce(
    (sum, trade) => sum + signedAmount(trade) * getTradeFxRate(trade),
    0
  );
  const principal = Math.max(state.capitalAmount, sigmaAmountAll, 0);
  const totalMarketValueCnyBook = visiblePositions.reduce((sum, item) => sum + item.marketValue, 0);
  const cash = principal - sigmaAmountAll;

  const overviewBookCurrency = getOverviewBookCurrency();
  const toBook = (row, nativeVal) => nativeToOverviewBook(row, nativeVal, overviewBookCurrency);

  /** 同一币种下先汇总原币金额，再按该币种汇率换算（与回填 profit_cny 口径一致） */
  const sumBookByCurrency = (getNative) => {
    const byCcy = Object.create(null);
    for (const item of visiblePositions) {
      const ccy = item.currency || "CNY";
      const v = getNative(item);
      if (!Number.isFinite(v) || v === 0) continue;
      byCcy[ccy] = (byCcy[ccy] || 0) + v;
    }
    let sum = 0;
    for (const ccy of Object.keys(byCcy)) {
      const row = visiblePositions.find((p) => (p.currency || "CNY") === ccy);
      if (row) sum += toBook(row, byCcy[ccy]);
    }
    return sum;
  };

  const totalMarketValue = visiblePositions.reduce((sum, item) => sum + toBook(item, item.marketValueNative), 0);
  const todayProfit = sumBookByCurrency((item) => item.todayProfitNative);
  const yesterdayMarketValueForRate = visiblePositions.reduce(
    (sum, item) => sum + toBook(item, item.quantity * item.prevClose),
    0
  );
  const todayRate = yesterdayMarketValueForRate !== 0 ? todayProfit / yesterdayMarketValueForRate : 0;
  const totalProfit = sumBookByCurrency((item) => item.totalProfitNative);
  const overviewPrincipal = amountBookFromCny(principal, overviewBookCurrency);
  const overviewCash = amountBookFromCny(cash, overviewBookCurrency);
  const totalAssets = totalMarketValue + overviewCash;

  const totalAssetsForWeight = totalMarketValueCnyBook + cash;
  positions.forEach((item) => {
    item.weight = totalAssetsForWeight !== 0 ? item.marketValue / totalAssetsForWeight : 0;
  });
  positions.sort((a, b) => Math.abs(b.marketValue) - Math.abs(a.marketValue));

  return {
    positions,
    visiblePositions,
    sigmaAmountAll,
    principal,
    overviewBookCurrency,
    overviewPrincipal,
    overviewCash,
    totalMarketValue,
    yesterdayMarketValue: yesterdayMarketValueForRate,
    cash,
    totalAssets,
    todayProfit,
    todayRate,
    totalProfit,
  };
}

/**
 * 月收益核对清单：字段与 computePositionStageProfit 一致。
 * - 上月底收盘价：K 线中最后一根 day < 本月起点日 的 close（无则退回 prevClose）
 * - 上月末股数：所有「成交日 < 本月起点日」买卖累加后的持仓股数
 * - 本月交易金额：本月内成交金额的绝对值之和（笔笔金额加总，不分买卖）
 * - 本月净出入金_公式用：本月内 signedAmount(buy+ / sell-) 之和，与月收益公式一致
 */
function buildMonthlyReturnAuditRows(trades) {
  const list = trades != null ? trades : getPortfolioScope().trades;
  const pf = computePortfolio(list);
  const firstTradeDate = list.length ? [...list].sort(sortTradeAsc)[0].date : toDateKey(new Date());
  const monthStartKey = getStageStartKey("month", firstTradeDate);
  const rows = [];
  for (const p of pf.visiblePositions) {
    const symbolTrades = list.filter((t) => t.symbol === p.symbol).sort(sortTradeAsc);
    let startQuantity = 0;
    let stageFlowNative = 0;
    let monthGrossAmount = 0;
    for (const trade of symbolTrades) {
      const deltaQty = trade.side === "buy" ? trade.quantity : -trade.quantity;
      if (trade.date < monthStartKey) {
        startQuantity += deltaQty;
      } else {
        stageFlowNative += signedAmount(trade);
        monthGrossAmount += Math.abs(Number(trade.amount) || 0);
      }
    }
    const prevMonthEndClose = getSymbolCloseBeforeDate(p.symbol, monthStartKey, p.prevClose);
    rows.push({
      股票代码: p.symbol,
      本月起点日: monthStartKey,
      上月底收盘价: prevMonthEndClose,
      当前股价: validNumber(p.currentPrice, 0),
      上月末股数: startQuantity,
      当前股数: p.quantity,
      本月交易金额: monthGrossAmount,
      本月净出入金_公式用: stageFlowNative,
      月收益_native: p.monthProfitNative,
    });
  }
  return rows;
}

function dumpMonthlyReturnAudit() {
  const rows = buildMonthlyReturnAuditRows();
  console.info(
    "[月收益核对] 本月起点日=当月第 1 个自然日；「上月底收盘价」取自 K 线 last(bar.day < 起点日)。数据对应当前账户筛选。"
  );
  console.table(rows);
  if (rows.length) {
    const cols = Object.keys(rows[0]);
    const tsv = [cols.join("\t"), ...rows.map((r) => cols.map((c) => r[c]).join("\t"))].join("\n");
    console.info("TSV（可粘贴 Excel）：\n" + tsv);
    return { rows, tsv };
  }
  return { rows, tsv: "" };
}

function buildPortfolioHistory(positions, trades = state.trades) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const end = new Date();
  const endMid = new Date(toDateKey(end) + "T12:00:00");
  let startMid;
  if (tradeList.length) {
    const firstD = [...tradeList].sort(sortTradeAsc)[0].date;
    const parsed = new Date(String(firstD).slice(0, 10) + "T12:00:00");
    startMid = Number.isNaN(parsed.getTime()) ? new Date(endMid) : parsed;
  } else {
    startMid = new Date(endMid);
    startMid.setDate(startMid.getDate() - 370);
  }
  const maxSpanDays = 4000;
  if ((endMid - startMid) / 86400000 > maxSpanDays) {
    startMid = new Date(endMid.getTime() - maxSpanDays * 86400000);
  }
  if (startMid > endMid) {
    startMid = new Date(endMid);
  }

  const dateKeys = [];
  const cursor = new Date(startMid);
  while (cursor <= endMid) {
    dateKeys.push(toDateKey(cursor));
    cursor.setDate(cursor.getDate() + 1);
  }

  const symbolSet = new Set(positions.map((item) => item.symbol));
  const klineMap = {};
  const lastPriceMap = {};
  const fxRateMap = {};

  symbolSet.forEach((symbol) => {
    const list = getKlineBySymbol(symbol);
    klineMap[symbol] = Object.fromEntries(list.map((item) => [item.day, Number(item.close)]));
    const fallbackTrade = tradeList.find((item) => item.symbol === symbol);
    const quote = getQuoteBySymbol(symbol);
    lastPriceMap[symbol] = validNumber(quote.prevClose, fallbackTrade?.price, 0);
    fxRateMap[symbol] = getFxRateForSymbol(symbol, inferMarket(symbol));
  });

  const tradesByDate = {};
  for (const trade of tradeList) {
    if (!tradesByDate[trade.date]) {
      tradesByDate[trade.date] = [];
    }
    tradesByDate[trade.date].push(trade);
  }
  Object.values(tradesByDate).forEach((list) => list.sort((a, b) => a.createdAt - b.createdAt));

  const holdings = {};
  const points = [];
  const todayKey = toDateKey(new Date());

  for (const dateKey of dateKeys) {
    const dailyTrades = tradesByDate[dateKey] || [];
    for (const trade of dailyTrades) {
      if (holdings[trade.symbol] == null) {
        holdings[trade.symbol] = 0;
      }
      holdings[trade.symbol] += trade.side === "buy" ? trade.quantity : -trade.quantity;
    }

    let value = 0;
    let flow = 0;
    for (const trade of dailyTrades) {
      flow += signedAmount(trade) * getTradeFxRate(trade);
    }
    for (const symbol of symbolSet) {
      const dayClose = klineMap[symbol][dateKey];
      if (Number.isFinite(dayClose) && dayClose > 0) {
        lastPriceMap[symbol] = dayClose;
      } else {
        const quote = getQuoteBySymbol(symbol);
        if (dateKey === todayKey && validNumber(quote.current, 0) > 0) {
          lastPriceMap[symbol] = Number(quote.current);
        }
      }
      value += (holdings[symbol] || 0) * (lastPriceMap[symbol] || 0) * (fxRateMap[symbol] || 1);
    }
    points.push({ date: dateKey, value, flow });
  }
  return points;
}

function computeModeSeries(historyPoints, mode) {
  if (!historyPoints.length) {
    return [{ date: toDateKey(new Date()), rate: 0 }];
  }
  if (mode === "time") {
    return computeTimeWeightedSeries(historyPoints);
  }
  if (mode === "money") {
    return computeMoneyWeightedSeries(historyPoints);
  }
  return computeCostSeries(historyPoints);
}

function computeCostSeries(points) {
  const result = [];
  const startClose = points[0].value - points[0].flow;
  let sumFlow = 0;
  points.forEach((point) => {
    sumFlow += point.flow;
    const profit = point.value - startClose - sumFlow;
    const denominator = startClose + sumFlow;
    const rate = denominator !== 0 ? profit / denominator : 0;
    result.push({ date: point.date, rate });
  });
  return result;
}

function computeMoneyWeightedSeries(points) {
  const result = [];
  const startClose = points[0].value - points[0].flow;
  const flows = [];
  points.forEach((point, index) => {
    flows.push(point.flow);
    const totalPeriods = index + 1;
    let weightedFlow = 0;
    let sumFlow = 0;
    flows.forEach((flow, flowIdx) => {
      const weight = (totalPeriods - flowIdx) / totalPeriods;
      weightedFlow += flow * weight;
      sumFlow += flow;
    });
    const profit = point.value - startClose - sumFlow;
    const denominator = startClose + weightedFlow;
    const rate = denominator !== 0 ? profit / denominator : 0;
    result.push({ date: point.date, rate });
  });
  return result;
}

function computeTimeWeightedSeries(points) {
  const result = [];
  let compounded = 1;
  let prevValue = points[0].value - points[0].flow;
  points.forEach((point) => {
    const denominator = prevValue + Math.max(point.flow, 0);
    const dailyRate = denominator !== 0 ? (point.value - prevValue - point.flow) / denominator : 0;
    compounded *= 1 + dailyRate;
    result.push({ date: point.date, rate: compounded - 1 });
    prevValue = point.value;
  });
  return result;
}

function buildBenchmarkSeries(selectedPoints) {
  if (state.benchmark === "none") {
    return selectedPoints.map((point) => ({ date: point.date, rate: 0 }));
  }

  const symbol = state.benchmark;
  const kline = getKlineBySymbol(symbol);
  if (kline.length) {
    const byDate = Object.fromEntries(kline.map((item) => [item.day, Number(item.close)]));
    let lastPrice = validNumber(kline[0]?.close, DEFAULT_BENCHMARK_PRICE[symbol], 1);
    let base = 0;
    return selectedPoints.map((point, idx) => {
      if (Number.isFinite(byDate[point.date])) {
        lastPrice = Number(byDate[point.date]);
      } else {
        const quote = getQuoteBySymbol(symbol);
        if (idx === selectedPoints.length - 1 && validNumber(quote.current, 0) > 0) {
          lastPrice = Number(quote.current);
        }
      }
      if (idx === 0) {
        base = lastPrice || 1;
      }
      const rate = base ? (lastPrice - base) / base : 0;
      return { date: point.date, rate };
    });
  }

  const quote = getQuoteBySymbol(symbol);
  const fallbackRate =
    quote && validNumber(quote.prevClose, 0) > 0
      ? (validNumber(quote.current, quote.prevClose) - quote.prevClose) / quote.prevClose
      : 0;
  const len = selectedPoints.length;
  return selectedPoints.map((point, index) => ({
    date: point.date,
    rate: len > 1 ? (fallbackRate * index) / (len - 1) : fallbackRate,
  }));
}

function drawLineChart(mySeries, benchmarkSeries) {
  return drawDualLineChart(
    analysisRateChart,
    mySeries.map((item) => ({ date: item.date, value: item.rate * 100 })),
    state.benchmark === "none" ? null : benchmarkSeries.map((item) => ({ date: item.date, value: item.rate * 100 })),
    "#f24957",
    "#2f80f6",
    {
      keyA: "mine",
      keyB: "benchmark",
      labelA: "收益率",
      labelB: "基准",
      yAxisMode: state.benchmark === "none" ? "left" : "left-right",
      leftLabel: "收益率(%)",
      rightLabel: state.benchmark === "none" ? "" : "基准(%)",
      valueFormatter: (value) => `${formatNumber(value, 2)}%`,
      axisFormatter: (value) => `${formatNumber(value, 2)}%`,
    }
  );
}

function drawDualLineChart(canvas, seriesA, seriesB, colorA, colorB, options = {}) {
  if (!canvas) {
    return;
  }
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  const payload = buildChartPayload(
    [
      {
        key: options.keyA || "seriesA",
        label: options.labelA || "曲线A",
        color: colorA,
        axis: "left",
        values: seriesA,
      },
      ...(seriesB && seriesB.length
        ? [
            {
              key: options.keyB || "seriesB",
              label: options.labelB || "曲线B",
              color: colorB || "#2f80f6",
              axis: options.yAxisMode === "left-right" ? "right" : "left",
              values: seriesB,
            },
          ]
        : []),
    ],
    {
      xMin: 52,
      xMax: width - 28,
      yMin: 20,
      yMax: height - 36,
      yAxisMode: options.yAxisMode || (seriesB && seriesB.length ? "left-right" : "left"),
      axisByKey: options.axisByKey || {},
    }
  );
  drawChartGrid(ctx, width, height, payload);
  payload.seriesList.forEach((series) => {
    drawSeries(ctx, series.values, payload.mapX, payload.mapY, series.color || "#2f80f6");
  });
  drawAxisLabels(ctx, payload, {
    leftLabel: options.leftLabel || "",
    rightLabel: options.rightLabel || "",
    xLabel: options.xLabel || "日期",
    valueFormatter: options.axisFormatter,
  });
  drawCrosshairOverlay(ctx, payload, canvas.id, options.valueFormatter || options.axisFormatter);
  return payload;
}

function drawSingleLineChart(canvas, series, color) {
  return drawDualLineChart(canvas, series, null, color, null);
}

function drawAssetChart(assetSeries) {
  const principalSeries = assetSeries.map((item) => ({ date: item.date, value: item.principal }));
  const marketSeries = assetSeries.map((item) => ({ date: item.date, value: item.market }));
  if (state.capitalTrendMode === "principal") {
    return drawDualLineChart(analysisAssetChart, principalSeries, null, "#5f6c82", null, {
      keyA: "principal",
      labelA: "本金",
      yAxisMode: "left",
      leftLabel: "本金",
      valueFormatter: (value) => formatNumber(value, 2),
      axisFormatter: (value) => formatNumber(value, 2),
    });
  }
  if (state.capitalTrendMode === "market") {
    return drawDualLineChart(analysisAssetChart, marketSeries, null, "#4f83f1", null, {
      keyA: "market",
      labelA: "总市值",
      yAxisMode: "left",
      leftLabel: "总市值",
      valueFormatter: (value) => formatNumber(value, 2),
      axisFormatter: (value) => formatNumber(value, 2),
    });
  }
  return drawDualLineChart(analysisAssetChart, principalSeries, marketSeries, "#5f6c82", "#4f83f1", {
    keyA: "principal",
    keyB: "market",
    labelA: "本金",
    labelB: "总市值",
    yAxisMode: "left-right",
    leftLabel: "本金",
    rightLabel: "总市值",
    valueFormatter: (value) => formatNumber(value, 2),
    axisFormatter: (value) => formatNumber(value, 2),
  });
}

function drawChartGrid(ctx, width, height, payload = null) {
  const xMin = payload?.xMin ?? 20;
  const xMax = payload?.xMax ?? width - 20;
  const yMin = payload?.yMin ?? 20;
  const yMax = payload?.yMax ?? height - 20;
  ctx.strokeStyle = "#e6ebf2";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const y = yMin + ((yMax - yMin) / 4) * i;
    ctx.beginPath();
    ctx.moveTo(xMin, y);
    ctx.lineTo(xMax, y);
    ctx.stroke();
  }
  for (let i = 0; i <= 4; i += 1) {
    const x = xMin + ((xMax - xMin) / 4) * i;
    ctx.beginPath();
    ctx.moveTo(x, yMin);
    ctx.lineTo(x, yMax);
    ctx.stroke();
  }
}

function drawSeries(ctx, series, mapX, mapY, color) {
  if (!series || !series.length) {
    return;
  }
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  series.forEach((point, index) => {
    const x = Number.isFinite(point.x) ? point.x : mapX(index);
    const y = Number.isFinite(point.y) ? point.y : mapY(point.value);
    if (index === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();
}

function buildChartPayload(seriesList, options = {}) {
  const labels = options.labels || {};
  const axisByKey = options.axisByKey || {};
  const xMin = options.xMin ?? 20;
  const xMax = options.xMax ?? 680;
  const yMin = options.yMin ?? 20;
  const yMax = options.yMax ?? 300;
  const yAxisMode = options.yAxisMode || "left";
  const maxCount = Math.max(
    ...seriesList.map((item) => Math.max((item.values || []).length, 0)),
    2
  );
  const mapX = (idx) => xMin + (idx / Math.max(maxCount - 1, 1)) * (xMax - xMin);
  const leftValues = [];
  const rightValues = [];
  const withAxis = seriesList.map((item, idx) => {
    const axis = axisByKey[item.key] || item.axis || (yAxisMode === "left-right" && idx > 0 ? "right" : "left");
    const values = Array.isArray(item.values) ? item.values : [];
    values.forEach((point) => {
      const num = Number(point.value);
      if (!Number.isFinite(num)) {
        return;
      }
      if (axis === "right") {
        rightValues.push(num);
      } else {
        leftValues.push(num);
      }
    });
    return { ...item, axis, values };
  });
  const resolveRange = (values) => {
    const min = Math.min(...values, 0);
    const max = Math.max(...values, 0);
    return { min, max, range: Math.max(max - min, 0.001) };
  };
  const leftRange = resolveRange(leftValues.length ? leftValues : [0]);
  const rightRange =
    yAxisMode === "left-right"
      ? resolveRange(rightValues.length ? rightValues : leftValues.length ? leftValues : [0])
      : leftRange;
  const mapYByAxis = (value, axis) => {
    const target = axis === "right" ? rightRange : leftRange;
    return yMin + ((target.max - Number(value)) / target.range) * (yMax - yMin);
  };
  const indexed = withAxis.map((item) => ({
    ...item,
    values: item.values.map((point, index) => ({
      ...point,
      idx: index,
      axis: item.axis,
      x: mapX(index),
      y: mapYByAxis(point.value, item.axis),
    })),
  }));
  const seriesMap = indexed.reduce((acc, item) => {
    acc[item.key] = item;
    return acc;
  }, {});
  return {
    seriesList: indexed,
    seriesMap,
    labels,
    xMin,
    xMax,
    yMin,
    yMax,
    yAxisMode,
    leftRange,
    rightRange,
    mapX,
    mapY(value, axis = "left") {
      return mapYByAxis(value, axis);
    },
    pickNearestByX(x) {
      const firstSeries = indexed[0]?.values || [];
      if (!firstSeries.length) {
        return { index: 0, x: xMin, points: [] };
      }
      let nearest = 0;
      let bestGap = Number.POSITIVE_INFINITY;
      firstSeries.forEach((point, idx) => {
        const gap = Math.abs(point.x - x);
        if (gap < bestGap) {
          bestGap = gap;
          nearest = idx;
        }
      });
      const points = indexed.map((series) => series.values[Math.min(nearest, series.values.length - 1)]).filter(Boolean);
      return { index: nearest, x: firstSeries[nearest]?.x ?? xMin, points };
    },
  };
}

function drawAxisLabels(ctx, payload, options = {}) {
  const valueFormatter = options.valueFormatter || ((value) => formatNumber(value, 2));
  const firstSeries = payload.seriesList[0]?.values || [];
  const xDates = firstSeries.map((item) => item.date).filter(Boolean);
  const leftMax = valueFormatter(payload.leftRange.max, "left");
  const leftMin = valueFormatter(payload.leftRange.min, "left");
  const rightMax = valueFormatter(payload.rightRange.max, "right");
  const rightMin = valueFormatter(payload.rightRange.min, "right");
  ctx.save();
  ctx.fillStyle = "#8f99a9";
  ctx.font = "11px sans-serif";
  ctx.textBaseline = "middle";
  ctx.textAlign = "right";
  ctx.fillText(leftMax, payload.xMin - 6, payload.yMin + 2);
  ctx.fillText(leftMin, payload.xMin - 6, payload.yMax - 2);
  if (payload.yAxisMode === "left-right") {
    ctx.textAlign = "left";
    ctx.fillText(rightMax, payload.xMax + 6, payload.yMin + 2);
    ctx.fillText(rightMin, payload.xMax + 6, payload.yMax - 2);
  }
  if (options.leftLabel) {
    ctx.textAlign = "left";
    ctx.fillText(options.leftLabel, payload.xMin, payload.yMin - 10);
  }
  if (options.rightLabel && payload.yAxisMode === "left-right") {
    ctx.textAlign = "right";
    ctx.fillText(options.rightLabel, payload.xMax, payload.yMin - 10);
  }
  const startDate = xDates[0] || "--";
  const midDate = xDates[Math.floor((xDates.length - 1) / 2)] || startDate;
  const endDate = xDates[xDates.length - 1] || startDate;
  ctx.textBaseline = "top";
  ctx.textAlign = "left";
  ctx.fillText(startDate, payload.xMin, payload.yMax + 8);
  ctx.textAlign = "center";
  ctx.fillText(midDate, (payload.xMin + payload.xMax) / 2, payload.yMax + 8);
  ctx.textAlign = "right";
  ctx.fillText(endDate, payload.xMax, payload.yMax + 8);
  if (options.xLabel) {
    ctx.textAlign = "right";
    ctx.fillText(options.xLabel, payload.xMax, payload.yMax + 22);
  }
  ctx.restore();
}

function drawCrosshairOverlay(ctx, payload, canvasId, valueFormatter) {
  const cross = state.chartCrosshairMap[canvasId];
  if (!cross || !cross.points?.length) {
    return;
  }
  const formatter = valueFormatter || ((value) => formatNumber(value, 2));
  const yPrimary = Number.isFinite(cross.pointerY) ? cross.pointerY : cross.points[0]?.y ?? cross.y;
  ctx.save();
  ctx.strokeStyle = "rgba(80, 92, 112, 0.6)";
  ctx.lineWidth = 1;
  ctx.setLineDash([4, 4]);
  ctx.beginPath();
  ctx.moveTo(cross.x, payload.yMin);
  ctx.lineTo(cross.x, payload.yMax);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(payload.xMin, yPrimary);
  ctx.lineTo(payload.xMax, yPrimary);
  ctx.stroke();
  ctx.setLineDash([]);
  ctx.fillStyle = "#4d5769";
  ctx.font = "11px sans-serif";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  const dateText = cross.date || "--";
  const dateWidth = Math.max(42, ctx.measureText(dateText).width + 10);
  const dateX = Math.max(payload.xMin + dateWidth / 2, Math.min(payload.xMax - dateWidth / 2, cross.x));
  const dateY = payload.yMax + 16;
  ctx.fillRect(dateX - dateWidth / 2, dateY - 8, dateWidth, 16);
  ctx.fillStyle = "#fff";
  ctx.fillText(dateText, dateX, dateY);
  cross.points.forEach((point, index) => {
    const y = point.y;
    const side = point.axis === "right" ? "right" : "left";
    const text = formatter(point.value, point.key, point.axis);
    const w = Math.max(40, ctx.measureText(text).width + 10);
    const x = side === "right" ? payload.xMax + 6 + w / 2 : payload.xMin - 6 - w / 2;
    ctx.fillStyle = "#4d5769";
    ctx.fillRect(x - w / 2, y - 8, w, 16);
    ctx.fillStyle = "#fff";
    ctx.textAlign = "center";
    ctx.fillText(text, x, y);
    if (index === 0) {
      ctx.strokeStyle = "rgba(80, 92, 112, 0.8)";
      ctx.beginPath();
      ctx.arc(cross.x, y, 2.5, 0, Math.PI * 2);
      ctx.stroke();
    }
  });
  ctx.restore();
}

function positionChartTooltip(tooltip, canvas, pickedX) {
  const canvasWidth = canvas.clientWidth || canvas.width || 0;
  const pointerPx = (pickedX / canvas.width) * canvasWidth;
  const showOnRight = pointerPx < canvasWidth / 2;
  const sidePadding = 8;
  const tooltipWidth = 136;
  tooltip.style.left = showOnRight
    ? `${Math.max(sidePadding, canvasWidth - tooltipWidth - sidePadding)}px`
    : `${sidePadding}px`;
  tooltip.style.top = "8px";
}

function drawStockRecordBase(ctx, width, height, points, qtySeries, sortedTrades) {
  const closes = points.map((item) => Number(item.close));
  const maxClose = Math.max(...closes);
  const minClose = Math.min(...closes);
  const maxQty = Math.max(1, ...qtySeries.map((v) => Math.abs(v)));
  const mapX = (idx) => 52 + (idx / Math.max(points.length - 1, 1)) * (width - 84);
  const mapYPrice = (value) =>
    20 + ((maxClose - value) / Math.max(maxClose - minClose, 0.0001)) * (height - 56);
  const mapYQty = (value) => height - 36 - (value / maxQty) * (height - 56);
  ctx.fillStyle = "rgba(64, 145, 224, 0.16)";
  ctx.beginPath();
  points.forEach((item, index) => {
    const x = mapX(index);
    const y = mapYPrice(Number(item.close));
    if (index === 0) {
      ctx.moveTo(x, height - 36);
      ctx.lineTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.lineTo(mapX(points.length - 1), height - 36);
  ctx.closePath();
  ctx.fill();
  ctx.strokeStyle = "#4091e0";
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((item, index) => {
    const x = mapX(index);
    const y = mapYPrice(Number(item.close));
    if (index === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();
  ctx.strokeStyle = "#ff4d4f";
  ctx.lineWidth = 2;
  ctx.beginPath();
  qtySeries.forEach((value, index) => {
    const x = mapX(index);
    const y = mapYQty(value);
    if (index === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();
  const pointByDate = Object.fromEntries(points.map((item, idx) => [item.day, idx]));
  sortedTrades.forEach((trade) => {
    const idx = pointByDate[trade.date];
    if (idx == null) {
      return;
    }
    const x = mapX(idx);
    const y = mapYPrice(validNumber(trade.price, points[idx].close));
    ctx.fillStyle = trade.side === "buy" ? "#3b7bf6" : "#ffffff";
    ctx.strokeStyle = "#3b7bf6";
    ctx.beginPath();
    ctx.arc(x, y, 5, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
  });
}

function bindInteractiveChart(canvas, tooltip, payloadBuilder, options = {}) {
  if (!canvas || !tooltip) {
    return;
  }
  const existing = chartRuntimeMap.get(canvas.id);
  if (existing) {
    existing.payloadBuilder = payloadBuilder;
    existing.options = { ...existing.options, ...options };
    return existing;
  }
  let pressing = false;
  let pressTimer = null;
  let activePointerId = null;
  let crossVisible = !!state.chartCrosshairMap[canvas.id];
  let startX = 0;
  let moved = false;
  let panStarted = false;
  const pointers = new Map();
  const runtime = {
    payloadBuilder,
    options,
    hideCrosshair() {
      crossVisible = false;
      tooltip.classList.remove("show");
      delete state.chartCrosshairMap[canvas.id];
      runtime.options.onRefresh?.();
    },
  };
  chartRuntimeMap.set(canvas.id, runtime);

  const clearPressTimer = () => {
    if (pressTimer) {
      window.clearTimeout(pressTimer);
      pressTimer = null;
    }
  };

  const updateCrosshair = (clientX, clientY = null) => {
    const rect = canvas.getBoundingClientRect();
    const x = ((clientX - rect.left) / rect.width) * canvas.width;
    const payload = runtime.payloadBuilder?.();
    if (!payload) {
      return;
    }
    const picked = payload.pickNearestByX(Math.max(payload.xMin, Math.min(payload.xMax, x)));
    const first = picked.points[0];
    if (!first) {
      return;
    }
    crossVisible = true;
    const pointerY = (() => {
      if (!Number.isFinite(clientY)) {
        return first.y;
      }
      const rectY = ((clientY - rect.top) / rect.height) * canvas.height;
      return Math.max(payload.yMin, Math.min(payload.yMax, rectY));
    })();
    state.chartCrosshairMap[canvas.id] = {
      x: picked.x,
      y: first.y,
      pointerY,
      date: first.date,
      points: picked.points.map((point, idx) => ({
        label: payload.labels[payload.seriesList[idx]?.key] || payload.seriesList[idx]?.label || `曲线${idx + 1}`,
        value: point.value,
        key: payload.seriesList[idx]?.key,
        axis: point.axis || payload.seriesList[idx]?.axis || "left",
        y: point.y,
      })),
    };
    runtime.options.onRefresh?.();
    const formatter = runtime.options.valueFormatter || ((value) => formatNumber(value, 2));
    const rows = state.chartCrosshairMap[canvas.id].points
      .map((item) => `<div>${escapeHtml(item.label)}：${formatter(item.value, item.key, item.axis)}</div>`)
      .join("");
    tooltip.innerHTML = `<div>${escapeHtml(first.date)}</div>${rows}`;
    positionChartTooltip(tooltip, canvas, picked.x);
    tooltip.classList.add("show");
  };

  const handlePan = (deltaPx, payload) => {
    const step = Math.round(deltaPx / CHART_EDGE_SCROLL_PX);
    if (step === 0) {
      return;
    }
    if (runtime.options.mode === "stock") {
      const total = payload?.seriesList?.[0]?.values?.length || 0;
      const windowSize = Math.max(12, Number(state.stockRecordWindow || 30));
      const maxOffset = Math.max(0, total - windowSize);
      state.stockRecordOffset = Math.max(0, Math.min(maxOffset, Number(state.stockRecordOffset || 0) - step));
    } else if (state.analysisRangeMode === "preset") {
      const total = payload?.seriesList?.[0]?.values?.length || 0;
      const maxOffset = Math.max(0, total - Math.max(2, Number(state.rangeDays || 30)));
      state.analysisPanOffset = Math.max(0, Math.min(maxOffset, Number(state.analysisPanOffset || 0) - step));
    }
    runtime.options.onRefresh?.();
  };

  canvas.addEventListener("pointerdown", (event) => {
    event.preventDefault();
    canvas.setPointerCapture(event.pointerId);
    pointers.set(event.pointerId, event);
    activePointerId = event.pointerId;
    pressing = true;
    moved = false;
    panStarted = false;
    startX = event.clientX;
    clearPressTimer();
    pressTimer = window.setTimeout(() => {
      if (!pressing || moved) {
        return;
      }
      updateCrosshair(event.clientX, event.clientY);
    }, 220);
  });

  canvas.addEventListener("pointermove", (event) => {
    event.preventDefault();
    const payload = runtime.payloadBuilder?.();
    if (pointers.has(event.pointerId)) {
      pointers.set(event.pointerId, event);
    }
    if (pointers.size >= 2) {
      clearPressTimer();
      const pair = [...pointers.values()];
      const distance = Math.abs(pair[0].clientX - pair[1].clientX);
      const prevDistance = state.lastPinchDistanceMap[canvas.id];
      if (Number.isFinite(prevDistance) && Math.abs(distance - prevDistance) > 4) {
        const scale = distance / Math.max(prevDistance, 1);
        if (runtime.options.mode === "stock") {
          const total = payload?.seriesList?.[0]?.values?.length || 0;
          updateStockRecordWindowByScale(scale, total);
        } else {
          updateAnalysisWindowByScale(scale);
          renderControls();
        }
        runtime.options.onRefresh?.();
      }
      state.lastPinchDistanceMap[canvas.id] = distance;
      return;
    }
    if (event.pointerType === "mouse" && !pressing) {
      updateCrosshair(event.clientX, event.clientY);
      return;
    }
    if (crossVisible) {
      updateCrosshair(event.clientX, event.clientY);
      return;
    }
    if (pressing && activePointerId === event.pointerId) {
      const deltaFromStart = Math.abs(event.clientX - startX);
      if (deltaFromStart > 4) {
        moved = true;
      }
      if (moved) {
        clearPressTimer();
        panStarted = true;
        handlePan(event.movementX, payload);
      }
    }
  });

  const clearPointer = (event) => {
    event.preventDefault();
    if (canvas.hasPointerCapture(event.pointerId)) {
      canvas.releasePointerCapture(event.pointerId);
    }
    pointers.delete(event.pointerId);
    if (pointers.size < 2) {
      delete state.lastPinchDistanceMap[canvas.id];
    }
    if (panStarted && runtime.options.mode === "analysis") {
      renderControls();
    }
    pressing = false;
    moved = false;
    panStarted = false;
    clearPressTimer();
    if (event.pointerType !== "mouse" && !crossVisible) {
      tooltip.classList.remove("show");
    }
  };
  canvas.addEventListener("pointerup", clearPointer);
  canvas.addEventListener("pointercancel", clearPointer);
  canvas.addEventListener("pointerleave", (event) => {
    clearPointer(event);
    if (event.pointerType === "mouse") {
      runtime.hideCrosshair();
    }
  });
  return runtime;
}

function updateAnalysisWindowByScale(scale) {
  if (!Number.isFinite(scale) || scale === 1) {
    return;
  }
  if (state.analysisRangeMode !== "preset") {
    return;
  }
  const delta = scale > 1 ? -6 : 6;
  state.rangeDays = Math.max(7, Math.min(365, state.rangeDays + delta));
}

function updateStockRecordWindowByScale(scale, totalPoints) {
  if (!Number.isFinite(scale) || scale === 1) {
    return;
  }
  const delta = scale > 1 ? -6 : 6;
  const maxWindow = Math.max(12, Math.min(240, totalPoints || 240));
  state.stockRecordWindow = Math.max(12, Math.min(maxWindow, Number(state.stockRecordWindow || 30) + delta));
  const maxOffset = Math.max(0, Math.max(0, totalPoints || 0) - state.stockRecordWindow);
  state.stockRecordOffset = Math.max(0, Math.min(maxOffset, Number(state.stockRecordOffset || 0)));
}

async function refreshMarketData() {
  if (state.marketLoading) {
    return;
  }
  state.marketLoading = true;

  try {
    await hydrateKlineFromLocalDb();

    const fxSpot = await fetchRealtimeForexSpot().catch(() => ({}));
    if (fxSpot && typeof fxSpot === "object") {
      Object.assign(state.fxSpot, fxSpot);
    }

    const symbols = collectSymbolsForMarket();
    if (!symbols.length) {
      state.marketLoading = false;
      return;
    }
    await fetchQuoteNames(symbols);

    let quoteMap = {};
    try {
      quoteMap = await fetchRealtimeQuotes(symbols);
    } catch (error) {
      quoteMap = {};
    }
    if (Object.keys(quoteMap).length) {
      Object.entries(quoteMap).forEach(([symbol, quote]) => {
        const normalized = normalizeSymbol(symbol);
        const alias = normalized.replace(/^gb_/i, "");
        state.quoteMap[normalized] = quote;
        state.quoteMap[alias] = quote;
        const nm = String(quote?.name || "").trim();
        const display = quoteNameForDisplay(normalized, nm);
        if (display) {
          state.nameMap[normalized] = display;
          state.nameMap[alias] = display;
        }
      });
      const times = Object.values(quoteMap)
        .map((item) => item.time)
        .filter(Boolean);
      state.quoteTime = times[0] || state.quoteTime;
    }

    const klineSymbols = symbols.filter(supportsKline);
    const klineSettled = await Promise.allSettled(
      klineSymbols.map(async (symbol) => {
        const needDaily = !getKlineBySymbol(symbol).length;
        if (needDaily) {
          const list = await fetchKlineData(symbol);
          if (list.length) {
            const normalized = normalizeSymbol(symbol);
            const alias = normalized.replace(/^gb_/i, "");
            state.klineMap[normalized] = list;
            state.klineMap[alias] = list;
          }
        }
        await supplementKlineForMonthBoundary(symbol);
        // Fallback "realtime": use minute-kline last point when realtime endpoint is blocked.
        if (!Number.isFinite(getQuoteBySymbol(symbol)?.current)) {
          const latest = await fetchLatestQuoteFromDailyKlineFallback(symbol);
          if (latest) {
            const normalized = normalizeSymbol(symbol);
            const alias = normalized.replace(/^gb_/i, "");
            state.quoteMap[normalized] = latest;
            state.quoteMap[alias] = latest;
          }
        }
      })
    );
    klineSettled.forEach((result, i) => {
      if (result.status === "rejected") {
        console.warn(`K线拉取失败 ${klineSymbols[i]}`, result.reason);
      }
    });

    await enrichNamesFromEastmoney(symbols);
  } catch (error) {
    console.error("行情拉取失败，保留本地数据展示", error);
  } finally {
    state.marketLoading = false;
    renderAll();
  }
}

/**
 * 实时行情失败时的兜底：用日 K 最后两根 K 线算现价与昨收。
 * 勿用分钟线相邻两根代替昨收，否则涨跌幅会变成「几分钟内波动」，出现约 0.08% 这类与当日真实涨跌严重不符的数。
 */
async function fetchLatestQuoteFromDailyKlineFallback(symbol) {
  try {
    const list = await fetchKlineData(symbol);
    if (!Array.isArray(list) || list.length < 2) {
      return null;
    }
    const last = list[list.length - 1];
    const prevDay = list[list.length - 2];
    const current = Number(last.close);
    const prevClose = Number(prevDay.close);
    if (!Number.isFinite(current) || current <= 0) {
      return null;
    }
    return {
      name: symbol,
      current,
      prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
      time: String(last.day || "--"),
    };
  } catch (error) {
    return null;
  }
}

async function fetchRealtimeQuotes(symbols) {
  const uniqSymbols = [...new Set(symbols.filter(Boolean))];
  const tRes = await fetchRealtimeQuotesTencent(uniqSymbols).catch(() => null);
  const fromTencent = tRes?.parsed ?? {};
  const merged = {};

  uniqSymbols.forEach((sym) => {
    const q = fromTencent[sym];
    if (!q) {
      return;
    }
    merged[sym] = { ...q };
  });
  return merged;
}

async function fetchKlineData(symbol) {
  return fetchKlineDataSina(symbol);
}

async function fetchMinuteKData(symbol, scale = 5, datalen = 2) {
  return fetchKlineDataSina(symbol, scale, datalen);
}

/** 新浪 CN_MarketData.getKLineData：日 K 为 scale=240；分钟线为 1–60。 */
function mapSinaKlineRows(source) {
  if (!Array.isArray(source)) {
    return [];
  }
  const num = (v) => Number(String(v ?? "").replace(/,/g, ""));
  return source
    .map((item) => {
      const raw = String(item?.day ?? "").trim();
      const day = raw.includes(" ")
        ? raw.replace(/\//g, "-")
        : raw.slice(0, 10).replace(/\//g, "-");
      return {
        day,
        open: num(item?.open),
        high: num(item?.high),
        low: num(item?.low),
        close: num(item?.close),
        volume: num(item?.volume),
      };
    })
    .filter((item) => item.day && Number.isFinite(item.close));
}

async function fetchKlineDataSina(symbol, scale = 240, datalen = KLINE_DATALEN) {
  const requestSymbol = toSinaKlineSymbol(symbol);
  if (!requestSymbol) {
    return [];
  }
  const scaleNum = Number(scale);
  const isDaily = !Number.isFinite(scaleNum) || scaleNum >= 240;
  const sinaScale = isDaily ? 240 : Math.max(1, Math.min(60, scaleNum));
  const len = isDaily
    ? Math.min(1023, Math.max(2, Number(datalen) || KLINE_DATALEN))
    : Math.min(2000, Math.max(2, Number(datalen) || 2));
  const params = new URLSearchParams({
    symbol: requestSymbol,
    scale: String(sinaScale),
    ma: "no",
    datalen: String(len),
  });
  const qs = params.toString();
  const useProxy = shouldUseSinaKlineProxy();
  const apiB = getApiBaseForFetch();
  const directHttps = `https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?${qs}`;
  let url = useProxy ? `${apiB}/sina-kline?${qs}` : directHttps;
  let response = await fetch(url, {
    cache: "no-store",
    headers: useProxy ? undefined : SINA_KLINE_HEADERS,
  });
  if (!response.ok && useProxy) {
    response = await fetch(`${apiB}/sina_kline?${qs}`, { cache: "no-store" });
  }
  if (!response.ok) {
    throw new Error(`新浪K线失败: ${response.status}`);
  }
  let payload;
  try {
    payload = await response.json();
  } catch {
    return [];
  }
  if (payload == null) {
    return [];
  }
  return mapSinaKlineRows(Array.isArray(payload) ? payload : []);
}

/** 合并日 K；同一 key（day）后者覆盖前者。 */
function mergeKlineByDay(a, b) {
  const m = new Map();
  [...(a || []), ...(b || [])].forEach((row) => {
    if (!row?.day || !Number.isFinite(Number(row.close))) {
      return;
    }
    m.set(row.day, row);
  });
  return [...m.values()].sort((x, y) => x.day.localeCompare(y.day));
}

/**
 * 补足「月初前」日 K：主请求若过少则合并一次新浪 1023 日 K；美股再尝试服务端按日期补上一交易日收盘。
 */
async function supplementKlineForMonthBoundary(symbol) {
  const normalized = normalizeSymbol(symbol);
  const alias = normalized.replace(/^gb_/i, "");
  let list = [...(getKlineBySymbol(symbol) || [])];
  const firstDate = state.trades.length
    ? [...state.trades].sort(sortTradeAsc)[0].date
    : toDateKey(new Date());
  const monthStartKey = getStageStartKey("month", firstDate);
  const isUs = inferMarket(symbol) === "美股";
  if (!isUs && list.some((item) => item.day && item.day < monthStartKey)) {
    return list;
  }

  let merged = list;
  try {
    const extra = await fetchKlineDataSina(symbol, 240, 1023);
    if (extra.length) {
      merged = mergeKlineByDay(merged, extra);
    }
  } catch {
    // ignore
  }
  if (!isUs && merged.some((item) => item.day && item.day < monthStartKey)) {
    state.klineMap[normalized] = merged;
    state.klineMap[alias] = merged;
    return merged;
  }

  if (isUs && apiReady) {
    try {
      const r = await fetch(
        `${API_BASE}/us-historical-close?symbol=${encodeURIComponent(normalized)}&before=${encodeURIComponent(monthStartKey)}`,
        { cache: "no-store" }
      );
      if (r.ok) {
        const y = await r.json();
        if (y && y.ok && y.day && Number.isFinite(y.close)) {
          const day = String(y.day).slice(0, 10).replace(/\//g, "-");
          merged = mergeKlineByDay(merged, [
            {
              day,
              open: y.close,
              high: y.close,
              low: y.close,
              close: y.close,
              volume: 0,
            },
          ]);
        }
      }
    } catch {
      // ignore
    }
  }
  state.klineMap[normalized] = merged;
  state.klineMap[alias] = merged;
  return merged;
}

async function fetchRealtimeQuotesTencent(symbols) {
  const uniqSymbols = [...new Set(symbols)];
  if (!uniqSymbols.length) {
    return {
      parsed: {},
    };
  }
  const sourceToTarget = new Map();
  uniqSymbols.forEach((symbol) => {
    sourceToTarget.set(toTencentQuoteSymbol(symbol), symbol);
  });
  const keysJoined = [...sourceToTarget.keys()].join(",");
  const url = `https://qt.gtimg.cn/q=${keysJoined}&_=${Date.now()}`;
  const parsed = {};

  const fillFromQuoteText = (text) => {
    if (!text || typeof text !== "string") {
      return;
    }
    const re = /v_([A-Za-z0-9._]+)="([^"]*)"/g;
    let m;
    while ((m = re.exec(text)) !== null) {
      const sourceKey = m[1];
      const payload = m[2];
      const target = sourceToTarget.get(sourceKey);
      if (!target) {
        continue;
      }
      const record = parseTencentQuoteRecord(target, payload);
      if (record) {
        parsed[target] = record;
      }
    }
  };

  if (apiReady) {
    try {
      const r = await fetch(`${API_BASE}/quote/tencent?q=${encodeURIComponent(keysJoined)}`, {
        cache: "no-store",
      });
      if (r.ok) {
        fillFromQuoteText(await r.text());
      }
    } catch {
      // ignore; fall back to JSONP
    }
  }

  const needJsonp = !apiReady || uniqSymbols.some((sym) => !parsed[sym]);
  if (needJsonp) {
    await loadScript(url, "gbk");
    sourceToTarget.forEach((target, sourceSymbol) => {
      if (parsed[target]) {
        return;
      }
      const { key, payload } = readTencentQuoteWindowPayload(sourceSymbol);
      const record = parseTencentQuoteRecord(target, payload);
      if (record) {
        parsed[target] = record;
      }
      try {
        if (key) {
          delete window[key];
        }
      } catch {
        // ignore cleanup failures on non-configurable globals
      }
    });
  }

  return {
    parsed,
  };
}

function loadScript(src, charset = "utf-8") {
  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = src;
    script.charset = charset;
    script.async = true;
    const timer = window.setTimeout(() => {
      script.remove();
      reject(new Error(`加载超时: ${src}`));
    }, 12_000);

    script.onload = () => {
      window.clearTimeout(timer);
      script.remove();
      resolve();
    };
    script.onerror = () => {
      window.clearTimeout(timer);
      script.remove();
      reject(new Error(`加载失败: ${src}`));
    };
    document.head.appendChild(script);
  });
}

/**
 * waihui123 JSON：meta.base_currency=USD 时 data.CNY 为 1 USD 兑 CNY，data.HKD 为 1 USD 兑 HKD；
 * 1 HKD 兑 CNY = CNY/HKD。
 */
function parseWaihui123FxResponse(json) {
  const out = {};
  if (!json || Number(json.code) !== 200 || !json.data || typeof json.data !== "object") {
    return out;
  }
  const d = json.data;
  const cnyPerUsd = Number(d.CNY);
  const hkdPerUsd = Number(d.HKD);
  if (Number.isFinite(cnyPerUsd) && cnyPerUsd > 0) {
    out.USD = cnyPerUsd;
  }
  if (
    Number.isFinite(cnyPerUsd) &&
    cnyPerUsd > 0 &&
    Number.isFinite(hkdPerUsd) &&
    hkdPerUsd > 0
  ) {
    out.HKD = cnyPerUsd / hkdPerUsd;
  }
  return out;
}

async function fetchRealtimeForexWaihui123() {
  const url = apiReady ? `${API_BASE}/fx/waihui123` : WAIHUI123_FX_API;
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) {
    throw new Error(`waihui ${r.status}`);
  }
  const json = await r.json();
  if (json && json.ok === false) {
    throw new Error(String(json.error || "waihui proxy error"));
  }
  return parseWaihui123FxResponse(json);
}

/** 实时外汇：主用 waihui123，缺 USD/HKD 任一则腾讯 qt 外汇补全 */
async function fetchRealtimeForexSpot() {
  let w = {};
  try {
    w = await fetchRealtimeForexWaihui123();
  } catch {
    w = {};
  }
  if (w.USD && w.HKD) {
    return w;
  }
  const t = await fetchRealtimeForexTencent().catch(() => ({}));
  return {
    USD: w.USD || t.USD,
    HKD: w.HKD || t.HKD,
  };
}

/** 腾讯 qt 外汇实时（兜底）：USDCNY / HKDCNY 当前价 */
async function fetchRealtimeForexTencent() {
  const out = {};
  const q = TENCENT_FOREX_SPOT_CODES.join(",");
  const url = `https://qt.gtimg.cn/q=${q}&_=${Date.now()}`;

  const fillFromText = (text) => {
    if (!text || typeof text !== "string") {
      return;
    }
    const re = /v_([A-Za-z0-9._]+)="([^"]*)"/g;
    let m;
    while ((m = re.exec(text)) !== null) {
      const sourceKey = m[1];
      const payload = m[2];
      const ccy = TENCENT_FOREX_CODE_TO_CCY[sourceKey];
      if (!ccy) {
        continue;
      }
      const rec = parseTencentForexQuotePayload(payload);
      if (rec && Number.isFinite(rec.current) && rec.current > 0) {
        out[ccy] = rec.current;
      }
    }
  };

  if (apiReady) {
    try {
      const r = await fetch(`${API_BASE}/quote/tencent?q=${encodeURIComponent(q)}`, {
        cache: "no-store",
      });
      if (r.ok) {
        fillFromText(await r.text());
      }
    } catch {
      // ignore
    }
  }

  const needJsonp = !apiReady || TENCENT_FOREX_SPOT_CODES.some((code) => {
    const ccy = TENCENT_FOREX_CODE_TO_CCY[code];
    return !out[ccy];
  });
  if (needJsonp) {
    try {
      await loadScript(url, "gbk");
      TENCENT_FOREX_SPOT_CODES.forEach((code) => {
        const ccy = TENCENT_FOREX_CODE_TO_CCY[code];
        if (out[ccy]) {
          return;
        }
        const { key, payload } = readTencentQuoteWindowPayload(code);
        const rec = parseTencentForexQuotePayload(payload);
        if (rec && Number.isFinite(rec.current) && rec.current > 0) {
          out[ccy] = rec.current;
        }
        try {
          if (key) {
            delete window[key];
          }
        } catch {
          // ignore
        }
      });
    } catch {
      // ignore
    }
  }

  return out;
}

function collectSymbolsForMarket() {
  const fromTrades = state.trades.map((item) => ensureSymbolPrefixForQuote(item.symbol));
  if (state.benchmark !== "none") {
    fromTrades.push(state.benchmark);
  }
  if (!fromTrades.length) {
    fromTrades.push("sz300750", "sh601899", "sh000001", "sz399001");
  }
  return [...new Set(fromTrades)];
}

function supportsKline(symbol) {
  return /^(sh|sz)\d{6}$/i.test(symbol) || /^hk\d{5}$/i.test(symbol) || /^gb_[a-z0-9._-]+$/i.test(symbol) || /^[a-z][a-z0-9._-]*$/i.test(symbol);
}

function normalizeTrade(input) {
  const trade = { ...input };
  trade.symbol = normalizeSymbol(trade.symbol || "");
  trade.type = trade.type || "trade";
  trade.side = normalizedSide(trade.type, trade.side || "buy");
  trade.price = Number(trade.price || 0);
  trade.quantity = Number(trade.quantity || 0);
  if (trade.type === "dividend") {
    trade.price = 0;
    trade.quantity = 0;
  }
  if (trade.type === "bonus" || trade.type === "split" || trade.type === "merge") {
    trade.price = 0;
  }
  const defaultAmount = Math.abs(trade.price * trade.quantity);
  trade.amount = Math.abs(Number.isFinite(Number(trade.amount)) ? Number(trade.amount) : defaultAmount);
  trade.date = trade.date || toDateKey(new Date());
  trade.note = trade.note || "";
  trade.name = trade.name || trade.symbol;
  trade.createdAt = Number(trade.createdAt || Date.now());
  return trade;
}

function normalizedSide(type, side) {
  if (type === "dividend" || type === "merge") {
    return "sell";
  }
  if (type === "bonus" || type === "split") {
    return "buy";
  }
  return side === "sell" ? "sell" : "buy";
}

function signedAmount(trade) {
  return trade.side === "buy" ? trade.amount : -trade.amount;
}

function normalizeSymbol(rawSymbol) {
  const value = String(rawSymbol || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
  if (!value) {
    return "";
  }
  if (value.startsWith("sh") || value.startsWith("sz") || value.startsWith("hk")) {
    return value;
  }
  if (value.startsWith("rt_hk") || value.startsWith("gb_")) {
    return value;
  }
  if (/^\d{6}$/.test(value)) {
    if (["3"].includes(value[0])) {
      return `sz${value}`;
    }
    if (["5", "6", "9"].includes(value[0])) {
      return `sh${value}`;
    }
    return `sz${value}`;
  }
  if (/^\d{5}$/.test(value)) {
    return `hk${value}`;
  }
  if (/^\d{1,4}$/.test(value)) {
    return `hk${value.padStart(5, "0")}`;
  }
  return value;
}

function inferMarket(symbol) {
  if (symbol.startsWith("sh") || symbol.startsWith("sz")) {
    return "A股";
  }
  if (symbol.startsWith("hk") || symbol.startsWith("rt_hk")) {
    return "港股";
  }
  if (symbol.startsWith("gb_") || /^[a-z]/i.test(symbol)) {
    return "美股";
  }
  return "其他";
}

function getSymbolCurrency(symbol, market = inferMarket(symbol)) {
  if (market === "港股") {
    return "HKD";
  }
  if (market === "美股") {
    return "USD";
  }
  return "CNY";
}

function getFxRateToCny(currency) {
  if (currency === "CNY") {
    return 1;
  }
  const spot = state.fxSpot?.[currency];
  if (Number.isFinite(spot) && spot > 0) {
    return spot;
  }
  return FX_RATE_FALLBACK[currency] || 1;
}

function getFxRateForDate(currency, dateKey) {
  if (currency === "CNY") {
    return 1;
  }
  const mapByDate = state.fxRatesToCnyByDate || {};
  const keys = Object.keys(mapByDate).sort();
  if (!keys.length) {
    return getFxRateToCny(currency);
  }
  if (!dateKey) {
    const latest = mapByDate[keys[keys.length - 1]];
    return Number(latest?.[currency]) || getFxRateToCny(currency);
  }
  for (let i = keys.length - 1; i >= 0; i -= 1) {
    if (keys[i] <= dateKey) {
      const hit = Number(mapByDate[keys[i]]?.[currency]);
      if (Number.isFinite(hit) && hit > 0) {
        return hit;
      }
    }
  }
  for (const key of keys) {
    const hit = Number(mapByDate[key]?.[currency]);
    if (Number.isFinite(hit) && hit > 0) {
      return hit;
    }
  }
  return getFxRateToCny(currency);
}

function getFxRateForSymbol(symbol, market = inferMarket(symbol)) {
  return getFxRateForDate(getSymbolCurrency(symbol, market), toDateKey(new Date()));
}

function getTradeFxRate(trade) {
  const market = inferMarket(trade.symbol);
  const currency = getSymbolCurrency(trade.symbol, market);
  return getFxRateForDate(currency, trade.date);
}

function getTradeFxRateForDate(trade, dateKey) {
  const market = inferMarket(trade.symbol);
  const currency = getSymbolCurrency(trade.symbol, market);
  return getFxRateForDate(currency, dateKey || trade.date);
}

function signedAmountCny(trade) {
  return signedAmount(trade) * getTradeFxRate(trade);
}

function typeLabel(type) {
  if (type === "dividend") return "分红";
  if (type === "bonus") return "送股";
  if (type === "split") return "拆股";
  if (type === "merge") return "合股";
  return "买入卖出";
}

/** 日历日期一律按北京时间（Asia/Shanghai）的「年月日」，与交易日 08:30 划分一致。 */
function toDateKey(date) {
  const d = date instanceof Date ? date : new Date(date);
  const base = Number.isNaN(d.getTime()) ? new Date() : d;
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(base);
}

function sortTradeAsc(a, b) {
  const ad = new Date(a.date).getTime();
  const bd = new Date(b.date).getTime();
  if (ad !== bd) {
    return ad - bd;
  }
  return Number(a.createdAt) - Number(b.createdAt);
}

function sortTradeDesc(a, b) {
  return -sortTradeAsc(a, b);
}

function validNumber(...values) {
  for (const value of values) {
    if (Number.isFinite(Number(value))) {
      return Number(value);
    }
  }
  return 0;
}

/**
 * 个股金额列：valueNative 为标的原币种；人民币展示时再乘当前汇率；A 股不加 ¥ 前缀。
 */
function formatStockTableMoney(row, valueNative, fraction = 2) {
  const isCnyBook = row.market === "A股" || row.currency === "CNY";
  const display = applyFxForOverview(row, valueNative);
  const body = formatSignedMoney(display, fraction);
  if (state.stockAmountDisplay === "cny") {
    if (isCnyBook) {
      return body;
    }
    return `¥ ${body}`;
  }
  if (isCnyBook) {
    return body;
  }
  const native = Number.isFinite(Number(valueNative)) ? Number(valueNative) : 0;
  return formatSignedMoney(native, fraction);
}

function formatStockTableMarketValue(row) {
  const isCnyBook = row.market === "A股" || row.currency === "CNY";
  const mvNative = Number.isFinite(Number(row.marketValueNative)) ? Number(row.marketValueNative) : 0;
  const display = applyFxForOverview(row, mvNative);
  const text = display.toFixed(2);
  if (state.stockAmountDisplay === "cny") {
    if (isCnyBook) {
      return text;
    }
    return `¥ ${text}`;
  }
  if (isCnyBook) {
    return text;
  }
  return mvNative.toFixed(2);
}

function formatCurrency(value) {
  const sign = value < 0 ? "-" : "";
  const abs = Math.abs(value);
  return `${sign}¥${abs.toLocaleString("zh-CN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function formatPlainMoney(value) {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  return safe.toFixed(2);
}

function formatSignedMoney(value, fraction = 2) {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  const abs = Math.abs(safe).toFixed(fraction);
  const sign = safe > 0 ? "+" : safe < 0 ? "-" : "";
  return `${sign}${abs}`;
}

function formatNumber(value, fraction = 2) {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  return safe.toLocaleString("zh-CN", {
    minimumFractionDigits: fraction,
    maximumFractionDigits: fraction,
  });
}

function formatPercent(value) {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  const num = (safe * 100).toFixed(2);
  return `${safe >= 0 ? "+" : ""}${num}%`;
}

function metricValueWithRate(amount, rate) {
  const amountText = formatSignedMoney(amount, 2);
  const rateText = formatPercent(rate);
  return `${amountText}<span class="profit-rate-inline">${rateText}</span>`;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
