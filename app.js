const STORAGE_KEY = "earning-clone-state-v2";
const API_BASE = "/api";
const QUOTE_REFRESH_MS = 60_000;
const KLINE_DATALEN = 420;
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
  analysisPanOffset: 0,
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
const currencyTip = document.getElementById("currencyTip");
const stockTableBody = document.getElementById("stockTableBody");
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
const algoModeSelect = document.getElementById("algoMode");
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
  void fetchQuoteNames(state.trades.map((trade) => trade.symbol)).then(() => {
    renderOverviewAndStockTable();
    renderTradeTable();
    if (state.route === "stock-record" && state.activeRecordSymbol) {
      renderStockRecordPage(state.activeRecordSymbol);
    }
  });
  void initializeFxRates();
  refreshMarketData();
  window.setInterval(refreshMarketData, QUOTE_REFRESH_MS);
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
    console.error("加载历史汇率失败，已回退固定汇率", error);
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
    const chunkRates = await fetchFxHistorySeries(currency, chunkStart, chunkEnd);
    Object.assign(result, chunkRates);
    cursor.setDate(cursor.getDate() + FX_TIMEFRAME_DAYS);
  }
  return result;
}

async function fetchFxHistorySeries(currency, startDate, endDate) {
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
    return row.monthProfit;
  }
  if (key === "monthWeight") {
    return row.monthWeight;
  }
  if (key === "yearProfit") {
    return row.yearProfit;
  }
  if (key === "yearWeight") {
    return row.yearWeight;
  }
  if (key === "totalProfit") {
    return row.totalProfit;
  }
  if (key === "totalRate") {
    return row.totalRate;
  }
  if (key === "todayProfit") {
    return row.todayProfit;
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

function getDisplayName(symbol, fallbackName = "") {
  const normalized = normalizeSymbol(symbol || "");
  const alias = normalized.replace(/^gb_/i, "");
  return state.nameMap[normalized] || state.nameMap[alias] || fallbackName || alias.toUpperCase();
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

function getCurrencyLabel(currency) {
  if (currency === "USD") return "美元";
  if (currency === "HKD") return "港币";
  return "人民币";
}

function getTradingDateKey(baseDate = new Date()) {
  const dt = new Date(baseDate);
  const hour = dt.getHours();
  const minute = dt.getMinutes();
  if (hour < 8 || (hour === 8 && minute < 30)) {
    dt.setDate(dt.getDate() - 1);
  }
  return toDateKey(dt);
}

async function fetchQuoteNames(symbols) {
  const targets = [...new Set(symbols.filter(Boolean).map((symbol) => normalizeSymbol(symbol)))].filter((symbol) => {
    const alias = symbol.replace(/^gb_/i, "");
    return !state.nameMap[symbol] && !state.nameMap[alias];
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
      if (name) {
        state.nameMap[sourceSymbol] = name;
        state.nameMap[sourceSymbol.replace(/^gb_/i, "")] = name;
      }
    });
  } catch {
    // ignore quote-name failures, keep existing display names
  }
}

function toTencentQuoteSymbol(symbol) {
  if (!symbol) {
    return "";
  }
  const raw = String(symbol).toLowerCase();
  if (/^sh\d{6}$/.test(raw) || /^sz\d{6}$/.test(raw) || /^hk\d{5}$/.test(raw) || /^us[A-Z0-9._-]+$/i.test(raw)) {
    return raw;
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

function toTencentKlineSymbol(symbol) {
  return toTencentQuoteSymbol(symbol);
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
  const current = Number(parts[3]);
  const prevClose = Number(parts[4]);
  const time = String(parts[30] || parts[31] || "--").trim();
  if (!Number.isFinite(current) || current <= 0) {
    return null;
  }
  return {
    name,
    current,
    prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
    time: time || "--",
  };
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

async function hydrateState() {
  let parsed = null;
  let remoteParsed = null;
  let localParsed = null;
  apiReady = await checkApiHealth();
  if (apiReady) {
    remoteParsed = await fetchRemoteState();
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
    }
  } else if (remoteParsed) {
    parsed = remoteParsed;
  }
  if (parsed && typeof parsed === "object") {
    state.route = parsed.route ?? state.route;
    if (state.route === "records") {
      state.route = "trade";
    }
    if (state.route === "introduction") {
      state.route = "account";
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
    state.trades = Array.isArray(parsed.trades) ? parsed.trades.map(normalizeTrade) : [];
  }
  if (!["month", "ytd", "total"].includes(state.stageRange)) {
    state.stageRange = "month";
  }
  if (!["preset", "custom"].includes(state.analysisRangeMode)) {
    state.analysisRangeMode = "preset";
  }
  if (!["both", "principal", "market"].includes(state.capitalTrendMode)) {
    state.capitalTrendMode = "both";
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
    trades: state.trades,
  };
  localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
  if (apiReady) {
    void pushSettingsToApi(payload);
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

  algoModeSelect.addEventListener("change", () => {
    state.algoMode = algoModeSelect.value;
    persistState();
    renderOverviewAndStockTable();
    renderAnalysis();
  });

  benchmarkSelect.addEventListener("change", () => {
    state.benchmark = benchmarkSelect.value;
    persistState();
    renderAnalysis();
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
      } else {
        state.analysisRangeMode = "preset";
        state.rangeDays = Number(value);
        state.analysisPanOffset = 0;
      }
      persistState();
      renderAnalysis();
      renderControls();
    });
  });

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
    renderAnalysis();
  });

  assetCurveModeSelect?.addEventListener("change", () => {
    state.capitalTrendMode = assetCurveModeSelect.value || "both";
    persistState();
    renderAnalysis();
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
    tradeAccountInput.value = DEFAULT_ACCOUNT.id;
  }
  tradeDateInput.value = toDateKey(new Date());
  tradeDialog.showModal();
}


function renderAll() {
  renderControls();
  renderRoute();
  renderOverviewAndStockTable();
  renderTradeTable();
  renderAnalysis();
  if (state.route === "stock-record" && state.activeRecordSymbol) {
    renderStockRecordPage(state.activeRecordSymbol);
  }
}

function renderControls() {
  algoModeSelect.value = state.algoMode;
  benchmarkSelect.value = state.benchmark;
  syncAccountSelectOptions();
  if (currencyTip) {
    const activeId = state.selectedAccountId === "all" ? DEFAULT_ACCOUNT.id : state.selectedAccountId;
    const account = getAccountById(activeId);
    currencyTip.textContent = `币种：${getCurrencyLabel(account.currency)}`;
  }
  if (stageRangeSelect) {
    stageRangeSelect.value = state.stageRange;
  }
  rangeChips.forEach((chip) => {
    const value = chip.dataset.range;
    const active =
      value === "custom"
        ? state.analysisRangeMode === "custom"
        : state.analysisRangeMode !== "custom" && Number(value) === state.rangeDays;
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
  const tradeDefault = state.accounts.some((item) => item.id === DEFAULT_ACCOUNT.id)
    ? DEFAULT_ACCOUNT.id
    : state.accounts[0]?.id || DEFAULT_ACCOUNT.id;
  setSelect(tradeAccountInput, tradeDefault, false);
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

function renderRoute() {
  if (!routePanes.some((pane) => pane.id === `route-${state.route}`)) {
    state.route = "earning";
  }
  routeButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.route === state.route);
  });
  routePanes.forEach((pane) => {
    pane.classList.toggle("active", pane.id === `route-${state.route}`);
  });
  const bottomTabs = document.querySelector(".bottom-tabs");
  if (bottomTabs) {
    bottomTabs.style.display = state.route === "stock-record" ? "none" : "grid";
  }
  if (state.route === "stock-record" && state.activeRecordSymbol) {
    renderStockRecordPage(state.activeRecordSymbol);
  }
}

function renderOverviewAndStockTable() {
  const scope = getPortfolioScope(state.selectedAccountId);
  const portfolio = computePortfolio(scope.trades);
  const history = buildPortfolioHistory(portfolio.positions, scope.trades);
  const stagePerf = computeStagePerformance(history, state.stageRange, state.algoMode, portfolio);
  const cards = [
    { label: "总市值", value: formatPlainMoney(portfolio.totalMarketValue) },
    { label: "本金", value: formatPlainMoney(portfolio.principal) },
    { label: "总资产", value: formatPlainMoney(portfolio.totalAssets) },
    { label: "现金", value: formatPlainMoney(portfolio.cash) },
  ];
  todayProfitMain.innerHTML = metricValueWithRate(portfolio.todayProfit, portfolio.todayRate);
  todayProfitMain.className = `profit-main ${portfolio.todayProfit >= 0 ? "up" : "down"}`;
  monthProfitMain.innerHTML = metricValueWithRate(stagePerf.profit, stagePerf.rate);
  monthProfitMain.className = `profit-main ${stagePerf.profit >= 0 ? "up" : "down"}`;

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
      const dayClass = row.todayProfit >= 0 ? "up" : "down";
      const changeClass = row.dayChangeRate >= 0 ? "up" : "down";
      const totalClass = row.totalProfit >= 0 ? "up" : "down";
      return `
        <tr>
          <td class="stock-name">
            <strong>${escapeHtml(getDisplayName(row.symbol, row.name))}</strong>
            <span><i class="market-tag">${tag}</i> ${stockCode}</span>
          </td>
          <td class="${dayClass}">${formatSignedMoney(row.todayProfit, 2)}</td>
          <td>
            <div class="cell-main">${formatNumber(row.currentPrice, 3)}</div>
            <div class="cell-sub ${changeClass}">${formatPercent(row.dayChangeRate)}</div>
          </td>
          <td>
            <div class="cell-main">${formatPlainMoney(row.marketValue)}</div>
            <div class="cell-sub">${formatNumber(row.quantity, 0)}</div>
          </td>
          <td>${formatPercent(row.weight)}</td>
          <td>${formatNumber(row.cost, 3)}</td>
          <td class="${row.monthProfit >= 0 ? "up" : "down"}">${formatSignedMoney(row.monthProfit, 2)}</td>
          <td>${formatPercent(row.monthWeight)}</td>
          <td class="${row.yearProfit >= 0 ? "up" : "down"}">${formatSignedMoney(row.yearProfit, 2)}</td>
          <td>${formatPercent(row.yearWeight)}</td>
          <td class="${totalClass}">${formatSignedMoney(row.totalProfit, 2)}</td>
          <td class="${totalClass}">${formatPercent(row.totalRate)}</td>
          <td class="${row.regretRate >= 0 ? "up" : "down"}">${formatPercent(row.regretRate)}</td>
          <td><a href="javascript:void(0)" class="record-link" data-stock-record="${row.symbol}">记录</a></td>
        </tr>
      `;
    })
    .join("");
}

function computeStagePerformance(history, stageRange, algoMode, portfolio) {
  if (!history.length) {
    return { profit: 0, rate: 0 };
  }
  if (stageRange === "day") {
    return {
      profit: portfolio.todayProfit,
      rate: portfolio.todayRate,
    };
  }

  const startKey = getStageStartKey(stageRange, history[0].date);
  const points = history.filter((point) => point.date >= startKey);
  if (!points.length) {
    return { profit: 0, rate: 0 };
  }
  const startClose = points[0].value - points[0].flow;
  const stageFlow = points.reduce((sum, point) => sum + point.flow, 0);
  const profit = points[points.length - 1].value - startClose - stageFlow;
  const rate = computeModeSeries(points, algoMode).at(-1)?.rate ?? 0;
  return { profit, rate };
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
  let stageFlow = 0;
  const currency = position.currency || getSymbolCurrency(position.symbol, position.market);
  symbolTrades.forEach((trade) => {
    const deltaQty = trade.side === "buy" ? trade.quantity : -trade.quantity;
    if (trade.date < startKey) {
      startQuantity += deltaQty;
    } else {
      stageFlow += signedAmount(trade) * getFxRateForDate(currency, trade.date);
    }
  });

  const startClose = getSymbolCloseBeforeDate(position.symbol, startKey, position.prevClose);
  const startFxRate = getFxRateForDate(currency, startKey);
  const startMarketValue = startQuantity * startClose * startFxRate;
  return position.marketValue - startMarketValue - stageFlow;
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

function renderAnalysis() {
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
    renderAnalysis();
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
  renderStockRecordPage(symbol);
  // wait for layout settle on mobile after route switch
  window.setTimeout(() => renderStockRecordPage(symbol), 40);
}

function renderStockRecordPage(symbol) {
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

  drawStockRecordChart(symbol, symbolTrades);
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
    }
  } catch (error) {
    console.error("加载个股实时行情失败", error);
  }
  if (!getQuoteBySymbol(symbol)?.current || !Number.isFinite(getQuoteBySymbol(symbol)?.current)) {
    const latest = await fetchLatestQuoteFromMinuteK(symbol);
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
  if (getKlineBySymbol(symbol).length) {
    return;
  }
  try {
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

function drawStockRecordChart(symbol, symbolTrades) {
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
  const values = visible.map((item) => {
    if (qtyByDate[item.date] != null) {
      rollingQty = qtyByDate[item.date];
    }
    return { date: item.date, price: validNumber(item.price, 0), qty: rollingQty };
  });
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
        label: "持仓股数",
        color: "#ff4d4f",
        axis: "right",
        values: values.map((item) => ({ date: item.date, value: item.qty })),
      },
    ],
    {
      labels: { price: "股价", qty: "持仓股数" },
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
    rightLabel: "股数",
    xLabel: "日期",
    valueFormatter: (value, axis, key) => {
      if (key === "qty" || axis === "right") {
        return formatNumber(value, 0);
      }
      return formatNumber(value, 2);
    },
  });
  drawCrosshairOverlay(ctx, payload, canvas.id, (value, key, axis) => {
    if (key === "qty" || axis === "right") {
      return formatNumber(value, 0);
    }
    return formatNumber(value, 2);
  });
  bindInteractiveChart(canvas, stockRecordTooltip, () => payload, {
    mode: "stock",
    onRefresh: () => drawStockRecordChart(symbol, symbolTrades),
    valueFormatter: (value, key, axis) => {
      if (key === "qty" || axis === "right") {
        return formatNumber(value, 0);
      }
      return formatNumber(value, 2);
    },
  });
}

function computePortfolio(trades = state.trades) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const grouped = new Map();
  const sortedTrades = [...tradeList].sort(sortTradeAsc);
  const tradingDateKey = getTradingDateKey(new Date());

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
    const marketValue = item.quantity * currentPrice * fxRate;
    const yesterdayValue = item.quantity * prevClose * fxRate;
    const sigmaAmountCny = item.sigmaAmount * fxRate;
    const cost = item.quantity !== 0 ? item.sigmaAmount / item.quantity : 0;
    const totalProfit = marketValue - sigmaAmountCny;
    const profitRate =
      Math.abs(sigmaAmountCny) > 0 ? totalProfit / Math.abs(sigmaAmountCny) : 0;
    const todayFlowForSymbol = tradeList
      .filter((trade) => trade.symbol === item.symbol && trade.date === tradingDateKey)
      .reduce((sum, trade) => sum + signedAmount(trade) * fxRate, 0);
    const todayProfit = marketValue - yesterdayValue - todayFlowForSymbol;
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
      marketValue,
      yesterdayValue,
      sigmaAmountCny,
      cost,
      totalProfit,
      profitRate,
      todayProfit,
      dayChangeRate,
      regretRate,
      totalRate: profitRate,
    };
  });

  positions.forEach((item) => {
    item.monthProfit = computePositionStageProfit(item, "month", tradeList);
    item.yearProfit = computePositionStageProfit(item, "ytd", tradeList);
  });
  const visiblePositions = positions.filter((item) => item.quantity > 0);
  const monthTotalProfit = visiblePositions.reduce((sum, item) => sum + item.monthProfit, 0);
  const yearTotalProfit = visiblePositions.reduce((sum, item) => sum + item.yearProfit, 0);
  visiblePositions.forEach((item) => {
    item.monthWeight = monthTotalProfit !== 0 ? item.monthProfit / monthTotalProfit : 0;
    item.yearWeight = yearTotalProfit !== 0 ? item.yearProfit / yearTotalProfit : 0;
  });

  const sigmaAmountAll = tradeList.reduce(
    (sum, trade) => sum + signedAmount(trade) * getTradeFxRate(trade),
    0
  );
  const principal = Math.max(state.capitalAmount, sigmaAmountAll, 0);
  const totalMarketValue = positions.reduce((sum, item) => sum + item.marketValue, 0);
  const yesterdayMarketValue = positions.reduce((sum, item) => sum + item.yesterdayValue, 0);
  const cash = principal - sigmaAmountAll;
  const totalAssets = totalMarketValue + cash;
  const todayFlow = tradeList
    .filter((trade) => trade.date === tradingDateKey)
    .reduce((sum, trade) => sum + signedAmount(trade) * getTradeFxRate(trade), 0);
  const todayProfit = totalMarketValue - yesterdayMarketValue - todayFlow;
  const todayRateDen = yesterdayMarketValue + Math.max(todayFlow, 0);
  const todayRate = todayRateDen !== 0 ? todayProfit / todayRateDen : 0;
  const totalProfit = totalMarketValue - sigmaAmountAll;

  positions.forEach((item) => {
    item.weight = totalAssets !== 0 ? item.marketValue / totalAssets : 0;
  });
  positions.sort((a, b) => Math.abs(b.marketValue) - Math.abs(a.marketValue));

  return {
    positions,
    visiblePositions,
    sigmaAmountAll,
    principal,
    totalMarketValue,
    yesterdayMarketValue,
    cash,
    totalAssets,
    todayFlow,
    todayProfit,
    todayRate,
    totalProfit,
  };
}

function buildPortfolioHistory(positions, trades = state.trades) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const end = new Date();
  const start = new Date(end);
  start.setDate(end.getDate() - 370);

  const dateKeys = [];
  const cursor = new Date(start);
  while (cursor <= end) {
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
    } else if (state.analysisRangeMode !== "custom") {
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
  const delta = scale > 1 ? -6 : 6;
  state.rangeDays = Math.max(7, Math.min(365, state.rangeDays + delta));
  state.analysisRangeMode = "preset";
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
    const symbols = collectSymbolsForMarket();
    if (!symbols.length) {
      state.marketLoading = false;
      return;
    }
    void fetchQuoteNames(symbols);

    let quoteMap = {};
    try {
      quoteMap = await fetchRealtimeQuotes(symbols);
    } catch (error) {
      // hq.sinajs.cn may be blocked by anti-hotlink on third-party domains.
      quoteMap = {};
    }
    if (Object.keys(quoteMap).length) {
      Object.entries(quoteMap).forEach(([symbol, quote]) => {
        const normalized = normalizeSymbol(symbol);
        const alias = normalized.replace(/^gb_/i, "");
        state.quoteMap[normalized] = quote;
        state.quoteMap[alias] = quote;
      });
      const times = Object.values(quoteMap)
        .map((item) => item.time)
        .filter(Boolean);
      state.quoteTime = times[0] || state.quoteTime;
    }

    const klineSymbols = symbols.filter(supportsKline);
    await Promise.all(
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
        // Fallback "realtime": use minute-kline last point when realtime endpoint is blocked.
        if (!Number.isFinite(getQuoteBySymbol(symbol)?.current)) {
          const latest = await fetchLatestQuoteFromMinuteK(symbol);
          if (latest) {
            const normalized = normalizeSymbol(symbol);
            const alias = normalized.replace(/^gb_/i, "");
            state.quoteMap[normalized] = latest;
            state.quoteMap[alias] = latest;
          }
        }
      })
    );
  } catch (error) {
    console.error("行情拉取失败，保留本地数据展示", error);
  } finally {
    state.marketLoading = false;
    renderAll();
  }
}

async function fetchLatestQuoteFromMinuteK(symbol) {
  try {
    const list = await fetchMinuteKData(symbol, 5, 2);
    if (!list.length) {
      return null;
    }
    const last = list[list.length - 1];
    const prev = list.length > 1 ? list[list.length - 2] : last;
    return {
      name: symbol,
      current: Number(last.close),
      prevClose: Number(prev.close || last.open || last.close),
      time: String(last.day || "--"),
    };
  } catch (error) {
    return null;
  }
}

async function fetchRealtimeQuotes(symbols) {
  const fromSina = await fetchRealtimeQuotesSina(symbols).catch(() => ({}));
  const fromTencent = await fetchRealtimeQuotesTencent(symbols).catch(() => ({}));
  return { ...fromTencent, ...fromSina };
}

async function fetchRealtimeQuotesSina(symbols) {
  const uniqSymbols = [...new Set(symbols)];
  const url = `https://hq.sinajs.cn/rn=${Date.now()}&list=${uniqSymbols.join(",")}`;
  await loadScript(url, "gbk");
  const parsed = {};

  uniqSymbols.forEach((symbol) => {
    const raw = window[`hq_str_${symbol}`];
    const record = parseQuoteRecord(symbol, raw);
    if (record) {
      parsed[symbol] = record;
    }
  });
  return parsed;
}

async function fetchKlineData(symbol) {
  const fromSina = await fetchKlineDataSina(symbol).catch(() => []);
  if (fromSina.length) {
    return fromSina;
  }
  return fetchKlineDataTencent(symbol);
}

async function fetchKlineDataSina(symbol) {
  const variableName = `__kline_${symbol.replace(/[^a-zA-Z0-9_]/g, "_")}_${Date.now()}`;
  const query = `https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20${variableName}=/CN_MarketDataService.getKLineData?symbol=${encodeURIComponent(
    symbol
  )}&scale=240&ma=no&datalen=${KLINE_DATALEN}`;
  await loadScript(query, "utf-8");
  const data = window[variableName];
  delete window[variableName];
  if (!Array.isArray(data)) {
    return [];
  }
  return data
    .map((item) => ({
      day: item.day,
      open: Number(item.open),
      high: Number(item.high),
      low: Number(item.low),
      close: Number(item.close),
      volume: Number(item.volume),
    }))
    .filter((item) => item.day && Number.isFinite(item.close));
}

async function fetchMinuteKData(symbol, scale = 5, datalen = 2) {
  const fromSina = await fetchMinuteKDataSina(symbol, scale, datalen).catch(() => []);
  if (fromSina.length) {
    return fromSina;
  }
  return fetchKlineDataTencent(symbol, scale, datalen);
}

async function fetchMinuteKDataSina(symbol, scale = 5, datalen = 2) {
  const variableName = `__minute_${symbol.replace(/[^a-zA-Z0-9_]/g, "_")}_${Date.now()}`;
  const query = `https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20${variableName}=/CN_MarketDataService.getKLineData?symbol=${encodeURIComponent(
    symbol
  )}&scale=${scale}&ma=no&datalen=${datalen}`;
  await loadScript(query, "utf-8");
  const data = window[variableName];
  delete window[variableName];
  if (!Array.isArray(data)) {
    return [];
  }
  return data
    .map((item) => ({
      day: item.day,
      open: Number(item.open),
      high: Number(item.high),
      low: Number(item.low),
      close: Number(item.close),
      volume: Number(item.volume),
    }))
    .filter((item) => item.day && Number.isFinite(item.close));
}

function parseQuoteRecord(symbol, rawText) {
  return parseQuoteRecordSina(symbol, rawText);
}

function parseQuoteRecordSina(symbol, rawText) {
  if (!rawText || typeof rawText !== "string") {
    return null;
  }
  const parts = rawText.split(",");
  if (!parts.length) {
    return null;
  }

  if (symbol.startsWith("gb_")) {
    const current = Number(parts[1]);
    const prevClose = Number(parts[8]);
    return {
      name: parts[0] || symbol,
      current: Number.isFinite(current) ? current : 0,
      prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
      time: parts[3] || "--",
    };
  }

  if (symbol.startsWith("hk") || symbol.startsWith("rt_hk")) {
    const current = Number(parts[6] || parts[2]);
    const prevClose = Number(parts[3] || parts[2]);
    return {
      name: parts[1] || parts[0] || symbol,
      current: Number.isFinite(current) ? current : 0,
      prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
      time: parts[17] || "--",
    };
  }

  const current = Number(parts[3]);
  const prevClose = Number(parts[2]);
  const dateText = parts[30] || "";
  const timeText = parts[31] || "";
  return {
    name: parts[0] || symbol,
    current: Number.isFinite(current) ? current : 0,
    prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
    time: `${dateText} ${timeText}`.trim() || "--",
  };
}

async function fetchRealtimeQuotesTencent(symbols) {
  const uniqSymbols = [...new Set(symbols)];
  if (!uniqSymbols.length) {
    return {};
  }
  const sourceToTarget = new Map();
  uniqSymbols.forEach((symbol) => {
    sourceToTarget.set(toTencentQuoteSymbol(symbol), symbol);
  });
  const url = `https://qt.gtimg.cn/q=${[...sourceToTarget.keys()].join(",")}&_=${Date.now()}`;
  await loadScript(url, "gbk");
  const parsed = {};
  sourceToTarget.forEach((target, sourceSymbol) => {
    const payload = window[`v_${sourceSymbol}`];
    const record = parseTencentQuoteRecord(target, payload);
    if (record) {
      parsed[target] = record;
    }
    try {
      delete window[`v_${sourceSymbol}`];
    } catch {
      // ignore cleanup failures on non-configurable globals
    }
  });
  return parsed;
}

async function fetchKlineDataTencent(symbol, scale = 240, datalen = KLINE_DATALEN) {
  const requestSymbol = toTencentKlineSymbol(symbol);
  const cycle = Number(scale) <= 60 ? `${Math.max(1, Number(scale))}min` : "day";
  const count = Math.max(2, Number(datalen) || KLINE_DATALEN);
  const url = `https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=${encodeURIComponent(
    requestSymbol
  )},${cycle},,,${count},qfq`;
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`腾讯K线失败: ${response.status}`);
  }
  const payload = await response.json();
  const root = payload?.data?.[requestSymbol] || payload?.data?.[symbol] || {};
  const source =
    root.qfqday ||
    root.day ||
    root.qfqweek ||
    root.week ||
    root.qfqmonth ||
    root.month ||
    root.qfqmin ||
    root.min ||
    [];
  return source
    .map((item) => ({
      day: String(item?.[0] || "").slice(0, 10).replace(/\//g, "-"),
      open: Number(item?.[1]),
      close: Number(item?.[2]),
      high: Number(item?.[3]),
      low: Number(item?.[4]),
      volume: Number(item?.[5]),
    }))
    .filter((item) => item.day && Number.isFinite(item.close));
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

function toDateKey(date) {
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
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
