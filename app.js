const SESSION_TAB_KEY = "stockreview_session_tabs_seeded";
const API_BASE = "/api";
const API_GET_TIMEOUT_MS = 12_000;
const API_MUTATION_TIMEOUT_MS = 10_000;

function apiFetch(input, init = {}) {
  const { timeoutMs, ...rest } = init || {};
  const method = String(rest.method || "GET").toUpperCase();
  const parsedTimeout = Number(timeoutMs);
  const resolvedTimeoutMs = Number.isFinite(parsedTimeout)
    ? parsedTimeout
    : method === "GET" || method === "HEAD"
      ? API_GET_TIMEOUT_MS
      : API_MUTATION_TIMEOUT_MS;
  if (resolvedTimeoutMs <= 0 || typeof AbortController === "undefined" || rest.signal) {
    return fetch(input, { ...rest, credentials: "include" });
  }
  const controller = new AbortController();
  const timerId = window.setTimeout(() => controller.abort(), resolvedTimeoutMs);
  return fetch(input, { ...rest, credentials: "include", signal: controller.signal }).finally(() => {
    window.clearTimeout(timerId);
  });
}

let sessionPhone = "";
let sessionUserId = "";
let sessionProfile = {
  nickname: null,
  communityPublic: true,
  displayName: "",
  phoneMasked: "",
};
let authSubmitting = false;
let analysisStockRankHelpListenersBound = false;

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

function markMarketDataDelayed(source = "cache") {
  state.marketDataDelayed = true;
  if (!state.marketDataDelaySource) {
    state.marketDataDelaySource = String(source || "cache");
  }
}

function readMarketDelayFromResponse(response) {
  if (!response || !response.headers || typeof response.headers.get !== "function") {
    return;
  }
  if (String(response.headers.get("x-market-data-delayed") || "") !== "1") {
    return;
  }
  markMarketDataDelayed(response.headers.get("x-market-data-source") || "cache");
}
const KLINE_DATALEN = 120;
const DAILY_CLOSE_HYDRATE_WINDOW_DAYS = 240;
const ANALYSIS_DAILY_REMOTE_CACHE_TTL_MS = 30_000;
const SYMBOL_NAME_MAP_TTL_MS = 6 * 60 * 60 * 1000;
const SETTINGS_SYNC_DEBOUNCE_MS = 650;
/** 首屏 hydrate 后跳过立即 PATCH；用户改设置或延迟到期后再同步 */
const INITIAL_SETTINGS_SYNC_DEFER_MS = 4000;
/** 持仓收益首页 home-bundle 一次拉回今日 + 月/年/总收益，切换阶段仅本地换展示 */
const METRICS_HOME_BUNDLE_STAGES = "today,mtd,ytd,inception";
const homeBundleInflightByKey = new Map();

function isEarningHomeRoute() {
  return state.appModule === "holdings" && state.route === "earning";
}

const STATE_SYNC_KEYS = [
  "route",
  "useDemoData",
  "algoMode",
  "benchmark",
  "stageRange",
  "rangeDays",
  "analysisRangeMode",
  "analysisPreset",
  "customRangeStart",
  "customRangeEnd",
  "capitalTrendMode", // total_assets | market | cash | cash_ratio（分析资产图下拉）
  "capitalAmount",
  "accounts",
  "selectedAccountId",
  "tradeFilterAccountId",
  "stockRecordAccountId",
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
const ALLOWED_PUBLIC_BENCHMARKS = new Set(["none", "sh000001", "sz399001", "rt_hkHSI", "gb_inx"]);
const FX_RATE_FALLBACK = {
  CNY: 1,
  HKD: 0.92,
  USD: 7.2,
};
/** 腾讯财经外汇实时：与主行情接口同源 qt.gtimg.cn */
const TENCENT_FOREX_SPOT_CODES = ["whUSDCNY", "whHKDCNY"];
const TENCENT_FOREX_CODE_TO_CCY = { whUSDCNY: "USD", whHKDCNY: "HKD" };
const DEFAULT_ACCOUNT = { id: "default", name: "默认账户", currency: "CNY", createdAt: 0 };
const MARKET_SORT_WEIGHT = { A股: 1, 港股: 2, 美股: 3, 其他: 9 };
const CHART_TOUCH_HOLD_MS = 80;
const CHART_MOUSE_HOLD_MS = 180;
const STOCK_RECORD_AXIS_MIN_FACTOR = 0.95;
const STOCK_RECORD_AXIS_MAX_FACTOR = 1.05;
const ANALYSIS_CHART_AXIS_MIN_FACTOR = 0.95;
const ANALYSIS_CHART_AXIS_MAX_FACTOR = 1.05;
const BROWSER_ROUTE_STATE_KEY = "__stockreview_route__";

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
  appModule: "holdings",
  communityProfileUserId: null,
  communityProfileReturnRoute: "community-feed",
  previousRoute: "earning",
  useDemoData: true,
  algoMode: "twr",
  benchmark: "none",
  stageRange: "month",
  rangeDays: 30,
  analysisRangeMode: "preset",
  /** 预设区间锚点：null=滚动最近 N 日；mtd=本月 1 日起；ytd=本年 1 月 1 日起 */
  analysisPreset: null,
  customRangeStart: "",
  customRangeEnd: "",
  /** 自定义区间输入框草稿，仅点「应用」后写入 customRangeStart/End 并刷新图表 */
  customRangeDraftStart: "",
  customRangeDraftEnd: "",
  capitalTrendMode: "total_assets",
  capitalAmount: 0,
  accounts: [DEFAULT_ACCOUNT],
  selectedAccountId: "all",
  tradeFilterAccountId: "all",
  stockRecordAccountId: "all",
  stockSortKey: "default",
  stockSortOrder: "default",
  stockAmountDisplay: "native",
  analysisPanOffset: 0,
  /** 分析图可见窗口（交易日点数），捏合只改此项，不重新请求 */
  analysisChartWindow: 30,
  trades: [],
  /** 银证转账 / 出入金 */
  cashTransfers: [],
  /** 交易页子 Tab：trades | cash */
  tradePanelTab: "trades",
  editingCashTransferId: null,
  quoteMap: {},
  klineMap: {},
  nameMap: {},
  quoteTime: "--",
  marketDataDelayed: false,
  marketDataDelaySource: "",
  marketLoading: false,
  editingTradeId: null,
  editingAccountId: null,
  activeRecordId: null,
  activeRecordSymbol: null,
  /** 个股记录页：仅该标的成交（非全量 state.trades） */
  stockRecordTrades: [],
  stockRecordTradesLoading: false,
  stockRecordPageLoading: false,
  stockRecordWindow: 30,
  stockRecordOffset: 0,
  stockRecordChartRange: "30",
  stockRecordBundle: null,
  stockRecordChartPointsCache: [],
  stockRecordChartHistoryComplete: false,
  stockRecordChartViewExpanded: false,
  stockRecordChartPagination: { limit: 30, nextOffset: 0, hasMore: false },
  stockRecordPointsLoading: false,
  stockRecordShowClose: true,
  stockRecordShowShares: true,
  stockRecordShowMarketValue: false,
  chartCrosshairMap: {},
  lastPinchDistanceMap: {},
  fxRatesToCnyByDate: {},
  /** 腾讯 qt 外汇实时：USD / HKD → 兑 CNY 中间价 */
  fxSpot: {},
  fxLoaded: false,
  communityProfileStage: "month",
  communityProfileTab: "earning",
  /** 查看他人主页时临时覆盖总览展示币种（与对方 selectedAccountId 一致） */
  _overviewBookCurrencyOverride: null,
  lastPublicProfileDetail: null,
  communityPublicTrades: [],
  publicEarningBundleUi: {
    ready: false,
    loading: false,
    key: "",
    bundle: null,
    meta: null,
  },
  publicAnalysisBundleUi: {
    ready: false,
    loading: false,
    key: "",
    bundle: null,
    meta: null,
  },
  /** 个股记录页：true 时走公开 metrics / 分页 trades API */
  stockRecordFromPublicProfile: false,
  /** 进入「搜索股票」页面前的 route，用于返回 */
  tradeSearchReturnRoute: "trade",
  /** 从「我的」子页返回时的父路由：从交易页打开 mine-algo / mine-accounts 时为 "trade" */
  mineReturnRoute: null,
  /** 首页 metrics a/b/f：齐套后才展示，避免本地 portfolio 占位闪屏 */
  overviewMetricsUi: {
    ready: false,
    loading: false,
    key: "",
    returns: null,
    assets: null,
    holdings: null,
  },
};
let apiReady = false;
let tradeSearchSuggestController = null;
let analysisRenderRequestSeq = 0;
/** 分析页 analysis-bundle 资产四曲线缓存，切换「总资产/市值/现金/占比」仅本地重绘 */
let cachedAnalysisAssetChartRows = null;
/** metrics 分析图：缓存序列与 payload，十字星 onRedraw 不重拉接口 */
let cachedAnalysisMetricsCharts = null;
let pendingSettingsSyncPayload = null;
let pendingSettingsSyncTimer = null;
const symbolNameFetchedAt = new Map();
const symbolNameHydrateInFlight = new Map();
const symbolNameSyncedAt = new Map();
const SYMBOL_NAME_UPSERT_DEBOUNCE_MS = 800;
const symbolNamePendingUpsertBySymbol = new Map();
let symbolNameUpsertFlushTimer = null;
/** 与 session 对齐，避免个股页重复拉全量成交/银证 */
let ledgerBootstrapCompleteForUid = "";
const TRADE_LIST_PAGE_SIZE = 10;
const tradeListPager = {
  gen: 0,
  offset: 0,
  hasMore: true,
  loading: false,
  loaded: false,
  accountId: "all",
};
const cashListPager = {
  gen: 0,
  offset: 0,
  hasMore: true,
  loading: false,
  loaded: false,
  accountId: "all",
};
let tradeListScrollListenerBound = false;
let stockRecordScrollListenerBound = false;
let stockRecordTradesLoadGen = 0;
let stockRecordPageLoadGen = 0;
let stockRecordChartLoadTimer = 0;
const stockRecordTradesPager = {
  gen: 0,
  offset: 0,
  hasMore: true,
  loading: false,
  loaded: false,
  symbol: "",
  accountId: "all",
};
const communityPublicTradesPager = {
  gen: 0,
  offset: 0,
  hasMore: true,
  loading: false,
  loaded: false,
  targetId: "",
};
let browserHistorySeeded = false;
let browserHistoryListenerBound = false;
let applyingBrowserRoutePopstate = false;
let lastBrowserRouteKey = "";
let lastRenderedRouteForScrollReset = "";
/** 用于离开/重新进入「收益」时失效首页日快照 UI 缓存 */
let previousRenderAllRouteForOverviewSnapshot = null;


const routePanes = [...document.querySelectorAll(".route-pane")];
const overviewGrid = document.getElementById("overviewGrid");
const quoteTime = document.getElementById("quoteTime");
const todayProfitMain = document.getElementById("todayProfitMain");
const monthProfitMain = document.getElementById("monthProfitMain");
const stageRangeSelect = document.getElementById("stageRangeSelect");
const accountFilterSelect = document.getElementById("accountFilterSelect");
const analysisAccountSelect = document.getElementById("analysisAccountSelect");
const tradeAccountFilterSelect = document.getElementById("tradeAccountFilterSelect");
const tradeCashAccountFilterSelect = document.getElementById("tradeCashAccountFilterSelect");
const stockTableBody = document.getElementById("stockTableBody");
/** 首页持仓表列宽缓存（不含 stockAmountDisplay，切换人民币不重算） */
let overviewStockColWidthCache = { key: "", widths: null };
const OVERVIEW_STOCK_TABLE_COL_COUNT = 15;
const OVERVIEW_STOCK_TABLE_HEADER_FALLBACK = [
  "名称",
  "今日收益",
  "现价/涨跌",
  "市值/数量",
  "仓位",
  "成本",
  "月收益",
  "月收益占比",
  "年收益",
  "年收益占比",
  "总收益",
  "总收益占比",
  "总收益率",
  "交易间隔",
  "操作",
];
const STOCK_TABLE_MEASURE_FONT_TH =
  '500 13px -apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", "Segoe UI", sans-serif';
const STOCK_TABLE_MEASURE_FONT_TD =
  '12px -apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", "Segoe UI", sans-serif';
const STOCK_TABLE_MEASURE_PAD_X = 20;
/** 首页持仓表名称列固定展示宽度（汉字个数，超出 clip） */
const OVERVIEW_STOCK_NAME_COL_CHARS = 7;
const OVERVIEW_STOCK_NAME_COL_PROBE = "测".repeat(OVERVIEW_STOCK_NAME_COL_CHARS);
let _stockTableMeasureCanvas;
const stockCurrencyToggle = document.getElementById("stockCurrencyToggle");
const stockSortButtons = [...document.querySelectorAll(".th-sort-btn")];
const accountForm = document.getElementById("accountForm");
const accountTableBody = document.getElementById("accountTableBody");
const analysisRateSummary = document.getElementById("analysisRateSummary");
const analysisProfitSummary = document.getElementById("analysisProfitSummary");
const analysisEodAccountCaption = document.getElementById("analysisEodAccountCaption");
const analysisRateChart = document.getElementById("analysisRateChart");
const analysisProfitChart = document.getElementById("analysisProfitChart");
const analysisAssetChart = document.getElementById("analysisAssetChart");
const analysisRateTooltip = document.getElementById("analysisRateTooltip");
const analysisProfitTooltip = document.getElementById("analysisProfitTooltip");
const analysisAssetTooltip = document.getElementById("analysisAssetTooltip");
const analysisStockRankBody = document.getElementById("analysisStockRankBody");
const demoToggleBtn = document.getElementById("demoToggleBtn");
const quickTradeBtn = document.getElementById("quickTradeBtn");
const recordTradeBtn = document.getElementById("recordTradeBtn");
const tradeHubAddTradeBtn = document.getElementById("tradeHubAddTradeBtn");
const tradeHubAddCashBtn = document.getElementById("tradeHubAddCashBtn");
const tradeHubOpenRecordsBtn = document.getElementById("tradeHubOpenRecordsBtn");
const tradeHubOpenCashBtn = document.getElementById("tradeHubOpenCashBtn");
const setCapitalBtn = document.getElementById("setCapitalBtn");
const algoModeSelectMine = document.getElementById("algoModeSelectMine");
const tradeHubAlgoSummary = document.getElementById("tradeHubAlgoSummary");
const mineUserPhone = document.getElementById("mineUserPhone");
const mineChangePasswordBtn = document.getElementById("mineChangePasswordBtn");
const mineLogoutBtn = document.getElementById("mineLogoutBtn");
const appMenuBtn = document.getElementById("appMenuBtn");
const appDrawer = document.getElementById("appDrawer");
const appDrawerBackdrop = document.getElementById("appDrawerBackdrop");
const appTopBar = document.querySelector(".app-top-bar");
const appHeaderTitle = document.getElementById("appHeaderTitle");
const mineNicknameInput = document.getElementById("mineNicknameInput");
const mineNicknameDisplay = document.getElementById("mineNicknameDisplay");
const tradeHubCommunityPublicToggle = document.getElementById("tradeHubCommunityPublicToggle");
const mineCommunitySaveBtn = document.getElementById("mineCommunitySaveBtn");
const mineCommunityProfileMsg = document.getElementById("mineCommunityProfileMsg");
const tradeHubCommunityMsg = document.getElementById("tradeHubCommunityMsg");
const communityFeedList = document.getElementById("communityFeedList");
const communityFollowingList = document.getElementById("communityFollowingList");
const communityLeaderboardList = document.getElementById("communityLeaderboardList");
const communityProfileBody = document.getElementById("communityProfileBody");
const communityProfileBackBtn = document.getElementById("communityProfileBackBtn");
const communityProfileTitle = document.getElementById("communityProfileTitle");
const communityProfileFollowSlot = document.getElementById("communityProfileFollowSlot");
const authGate = document.getElementById("authGate");
const appShell = document.getElementById("appShell");
const authLoginForm = document.getElementById("authLoginForm");
const authRegisterForm = document.getElementById("authRegisterForm");
const authLoginError = document.getElementById("authLoginError");
const authRegisterError = document.getElementById("authRegisterError");
const authShowRegister = document.getElementById("authShowRegister");
const authShowLogin = document.getElementById("authShowLogin");
const changePasswordDialog = document.getElementById("changePasswordDialog");
const changePasswordForm = document.getElementById("changePasswordForm");
const closeChangePasswordBtn = document.getElementById("closeChangePasswordBtn");
const changePwError = document.getElementById("changePwError");
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
const closeCapitalDialogBtn = document.getElementById("closeCapitalDialogBtn");
const closeStockRecordDialogBtn = document.getElementById("closeStockRecordDialogBtn");
const stockRecordAddTradeBtn = document.getElementById("stockRecordAddTradeBtn");
const stockRecordTitle = document.getElementById("stockRecordTitle");
const stockRecordTime = document.getElementById("stockRecordTime");
const stockRecordPrice = document.getElementById("stockRecordPrice");
const stockRecordChange = document.getElementById("stockRecordChange");
const stockRecordChart = document.getElementById("stockRecordChart");
const stockRecordWeightChart = document.getElementById("stockRecordWeightChart");
const stockRecordProfitChart = document.getElementById("stockRecordProfitChart");
const stockRecordToggleClose = document.getElementById("stockRecordToggleClose");
const stockRecordToggleShares = document.getElementById("stockRecordToggleShares");
const stockRecordToggleMarketValue = document.getElementById("stockRecordToggleMarketValue");
const stockRecordInterval = document.getElementById("stockRecordInterval");
const stockRecordRangeRow = document.getElementById("stockRecordRangeRow");
const stockRecordAccountSelect = document.getElementById("stockRecordAccountSelect");
const stockRecordListBody = document.getElementById("stockRecordListBody");
const stockRecordLoading = document.getElementById("stockRecordLoading");
const stockRecordBody = document.getElementById("stockRecordBody");
const stockRecordChartsLoading = document.getElementById("stockRecordChartsLoading");
const recordTradeActionsDialog = document.getElementById("recordTradeActionsDialog");
const closeRecordTradeActionsBtn = document.getElementById("closeRecordTradeActionsBtn");
const accountManageDialog = document.getElementById("accountManageDialog");
const closeAccountManageBtn = document.getElementById("closeAccountManageBtn");
const accountManageName = document.getElementById("accountManageName");
const accountManageCurrency = document.getElementById("accountManageCurrency");
const accountManageSaveBtn = document.getElementById("accountManageSaveBtn");
const accountManageDeleteBtn = document.getElementById("accountManageDeleteBtn");
const accountManageDefaultHint = document.getElementById("accountManageDefaultHint");
const tradeSymbolInput = document.getElementById("tradeSymbol");
const tradeNameInput = document.getElementById("tradeName");
const tradeDateInput = document.getElementById("tradeDate");
const tradeNoteInput = document.getElementById("tradeNote");
const tradeAccountInput = document.getElementById("tradeAccount");
const tradeSubtabTrades = document.getElementById("tradeSubtabTrades");
const tradeSubtabCash = document.getElementById("tradeSubtabCash");
const tradeRecordsPanel = document.getElementById("tradeRecordsPanel");
const cashRecordsPanel = document.getElementById("cashRecordsPanel");
const cashTransferTableBody = document.getElementById("cashTransferTableBody");
const cashTransferDialog = document.getElementById("cashTransferDialog");
const cashTransferForm = document.getElementById("cashTransferForm");
const cashTransferDialogTitle = document.getElementById("cashTransferDialogTitle");
const closeCashTransferDialogBtn = document.getElementById("closeCashTransferDialogBtn");
const cashTransferAccount = document.getElementById("cashTransferAccount");
const cashTransferDate = document.getElementById("cashTransferDate");
const cashTransferDirection = document.getElementById("cashTransferDirection");
const cashTransferAmount = document.getElementById("cashTransferAmount");
const cashTransferNote = document.getElementById("cashTransferNote");
const cashTransferSubmitBtn = document.getElementById("cashTransferSubmitBtn");
const cashTransferDeleteBtn = document.getElementById("cashTransferDeleteBtn");
const tradeSearchBackBtn = document.getElementById("tradeSearchBackBtn");
const tradeStockSearchInput = document.getElementById("tradeStockSearchInput");
const tradeStockSearchResults = document.getElementById("tradeStockSearchResults");
const stockRecordTooltip = document.getElementById("stockRecordTooltip");
const stockRecordWeightTooltip = document.getElementById("stockRecordWeightTooltip");
const stockRecordProfitTooltip = document.getElementById("stockRecordProfitTooltip");
const appRouteLoading = document.getElementById("appRouteLoading");
const appRouteLoadingText = document.getElementById("appRouteLoadingText");

const chartRuntimeMap = new Map();
let routeLoadingActiveCount = 0;

function dismissAppBootLoading() {
  const el = document.getElementById("appBootLoading");
  if (!el) {
    return;
  }
  el.classList.add("is-done");
  el.setAttribute("aria-busy", "false");
  window.setTimeout(() => {
    el.remove();
  }, 240);
  document.body.classList.add("app-ready");
}

function showRouteLoading(message = "数据正在加载中") {
  if (!appRouteLoading) {
    return;
  }
  routeLoadingActiveCount += 1;
  if (appRouteLoadingText) {
    appRouteLoadingText.textContent = message || "数据正在加载中";
  }
  appRouteLoading.classList.remove("hidden");
  appRouteLoading.setAttribute("aria-busy", "true");
}

function hideRouteLoading() {
  if (!appRouteLoading) {
    return;
  }
  routeLoadingActiveCount = Math.max(0, routeLoadingActiveCount - 1);
  if (routeLoadingActiveCount > 0) {
    return;
  }
  appRouteLoading.classList.add("hidden");
  appRouteLoading.setAttribute("aria-busy", "false");
}

async function refreshSessionFromServer() {
  try {
    const r = await apiFetch(`${getApiBaseForFetch()}/auth/me`, {
      cache: "no-store",
      timeoutMs: 4_000,
    });
    if (!r.ok) {
      sessionUserId = "";
      ledgerBootstrapCompleteForUid = "";
      sessionProfile = { nickname: null, communityPublic: true, displayName: "", phoneMasked: "" };
      return false;
    }
    const j = await r.json();
    if (!j?.ok || !j.user?.phone) {
      return false;
    }
    sessionPhone = String(j.user.phone);
    sessionUserId = String(j.user.id || "");
    sessionProfile = {
      nickname: j.user.nickname != null ? j.user.nickname : null,
      communityPublic: j.user.communityPublic !== false,
      displayName: String(j.user.displayName || ""),
      phoneMasked: String(j.user.phoneMasked || ""),
    };
    return true;
  } catch {
    return false;
  }
}

async function tryRestoreSession() {
  return refreshSessionFromServer();
}

function showAuthShell() {
  document.body.classList.add("auth-mode");
  if (authGate) {
    authGate.classList.remove("hidden");
    authGate.setAttribute("aria-hidden", "false");
  }
  if (appShell) {
    appShell.classList.add("hidden");
  }
}

function showAppShell() {
  document.body.classList.remove("auth-mode");
  if (authGate) {
    authGate.classList.add("hidden");
    authGate.setAttribute("aria-hidden", "true");
  }
  if (appShell) {
    appShell.classList.remove("hidden");
  }
}

function bindAuthUi() {
  authShowRegister?.addEventListener("click", () => {
    authLoginForm?.classList.add("hidden");
    authRegisterForm?.classList.remove("hidden");
    if (authLoginError) {
      authLoginError.classList.add("hidden");
    }
  });
  authShowLogin?.addEventListener("click", () => {
    authRegisterForm?.classList.add("hidden");
    authLoginForm?.classList.remove("hidden");
    if (authRegisterError) {
      authRegisterError.classList.add("hidden");
    }
  });

  authLoginForm?.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (authSubmitting) {
      return;
    }
    const phone = document.getElementById("authLoginPhone")?.value?.trim() || "";
    const password = document.getElementById("authLoginPassword")?.value || "";
    if (authLoginError) {
      authLoginError.classList.add("hidden");
    }
    authSubmitting = true;
    const loginSubmitBtn = authLoginForm.querySelector('button[type="submit"]');
    if (loginSubmitBtn) {
      loginSubmitBtn.disabled = true;
      loginSubmitBtn.textContent = "登录中...";
    }
    try {
      const r = await apiFetch(`${API_BASE}/auth/login`, {
        method: "POST",
        timeoutMs: 8_000,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ phone, password }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j?.ok) {
        if (authLoginError) {
          authLoginError.textContent = j?.error || "登录失败";
          authLoginError.classList.remove("hidden");
        }
        return;
      }
      sessionPhone = String(j.user?.phone || phone);
      ledgerBootstrapCompleteForUid = "";
      showAppShell();
      await startAppAfterAuth();
    } catch {
      if (authLoginError) {
        authLoginError.textContent = "网络超时或异常，请重试";
        authLoginError.classList.remove("hidden");
      }
    } finally {
      authSubmitting = false;
      if (loginSubmitBtn) {
        loginSubmitBtn.disabled = false;
        loginSubmitBtn.textContent = "登录";
      }
    }
  });

  authRegisterForm?.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (authSubmitting) {
      return;
    }
    const phone = document.getElementById("authRegPhone")?.value?.trim() || "";
    const password = document.getElementById("authRegPassword")?.value || "";
    const inviteCode = document.getElementById("authRegInvite")?.value?.trim() || "";
    if (authRegisterError) {
      authRegisterError.classList.add("hidden");
    }
    authSubmitting = true;
    const registerSubmitBtn = authRegisterForm.querySelector('button[type="submit"]');
    if (registerSubmitBtn) {
      registerSubmitBtn.disabled = true;
      registerSubmitBtn.textContent = "注册中...";
    }
    try {
      const r = await apiFetch(`${API_BASE}/auth/register`, {
        method: "POST",
        timeoutMs: 8_000,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ phone, password, inviteCode }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j?.ok) {
        if (authRegisterError) {
          authRegisterError.textContent = j?.error || "注册失败";
          authRegisterError.classList.remove("hidden");
        }
        return;
      }
      sessionPhone = String(j.user?.phone || phone);
      ledgerBootstrapCompleteForUid = "";
      showAppShell();
      await startAppAfterAuth();
    } catch {
      if (authRegisterError) {
        authRegisterError.textContent = "网络超时或异常，请重试";
        authRegisterError.classList.remove("hidden");
      }
    } finally {
      authSubmitting = false;
      if (registerSubmitBtn) {
        registerSubmitBtn.disabled = false;
        registerSubmitBtn.textContent = "注册";
      }
    }
  });

  mineLogoutBtn?.addEventListener("click", async () => {
    try {
      await apiFetch(`${API_BASE}/auth/logout`, { method: "POST" });
    } catch {
      // ignore
    }
    try {
      window.sessionStorage.removeItem(SESSION_TAB_KEY);
    } catch {
      // ignore
    }
    window.location.reload();
  });

  mineChangePasswordBtn?.addEventListener("click", () => {
    if (changePwError) {
      changePwError.classList.add("hidden");
    }
    const o = document.getElementById("changePwOld");
    const n = document.getElementById("changePwNew");
    if (o) {
      o.value = "";
    }
    if (n) {
      n.value = "";
    }
    changePasswordDialog?.showModal();
  });

  closeChangePasswordBtn?.addEventListener("click", () => {
    changePasswordDialog?.close();
  });

  changePasswordForm?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const oldPassword = document.getElementById("changePwOld")?.value || "";
    const newPassword = document.getElementById("changePwNew")?.value || "";
    if (changePwError) {
      changePwError.classList.add("hidden");
    }
    try {
      const r = await apiFetch(`${API_BASE}/auth/password`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ oldPassword, newPassword }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j?.ok) {
        if (changePwError) {
          changePwError.textContent = j?.error || "修改失败";
          changePwError.classList.remove("hidden");
        }
        return;
      }
      changePasswordDialog?.close();
    } catch {
      if (changePwError) {
        changePwError.textContent = "网络错误";
        changePwError.classList.remove("hidden");
      }
    }
  });
}

async function startAppAfterAuth(options = {}) {
  if (!options.sessionAlreadyFresh) {
    await refreshSessionFromServer();
  }
  await hydrateState();
  normalizeModuleHomeOnColdLoad();
  persistState({ skipSettingsSync: true });
  scheduleDeferredInitialSettingsSync();
  if (isEarningHomeRoute() && apiReady) {
    await refreshOverviewProfitRowFromSnapshots();
  }
  // 首屏先渲染：外链可能长久 pending，Previously 在此 await 会卡住「加载中…」遮罩
  void hydrateSymbolNameMap(
    state.route === "earning" || state.route === "analysis"
      ? collectSymbolsForMarket()
      : normalizeSymbolList(state.trades.map((trade) => trade.symbol))
  ).then(() => {
    renderAll();
  });
  renderAll();
  if (state.route !== "earning") {
    void refreshMarketData({ skipFinalRender: true }).finally(() => {
      renderAll();
      if (state.route === "community-profile" && state.communityProfileTab === "analysis" && state.lastPublicProfileDetail) {
        void openCommunityProfileAnalysisTab();
      }
    });
  }
  window.dumpMonthlyReturnAudit = dumpMonthlyReturnAudit;
  window.buildMonthlyReturnAuditRows = buildMonthlyReturnAuditRows;
}

initialize();

async function initialize() {
  bindEvents();
  bindAuthUi();
  bindBrowserRouteHistory();
  const authed = await tryRestoreSession();
  if (!authed) {
    showAuthShell();
    dismissAppBootLoading();
    return;
  }
  showAppShell();
  try {
    await startAppAfterAuth({ sessionAlreadyFresh: true });
  } finally {
    dismissAppBootLoading();
  }
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

const MINE_MODULE_ROUTES = new Set(["mine", "mine-community", "mine-algo", "mine-accounts"]);

function isMineModuleRoute(route) {
  const r = String(route || "");
  return MINE_MODULE_ROUTES.has(r) || r.startsWith("mine-");
}

function normalizeModuleHomeOnColdLoad() {
  if (isMineModuleRoute(state.route)) {
    state.appModule = "community";
    state.route = "community-feed";
    state.communityProfileUserId = null;
    return;
  }
  if (state.appModule === "community") {
    if (state.route !== "community-profile") {
      state.route = "community-feed";
    }
    return;
  }
  state.appModule = "holdings";
  if (state.route === "stock-record") {
    state.activeRecordSymbol = null;
    state.stockRecordFromPublicProfile = false;
    state.stockRecordBundle = null;
  }
  state.route = "earning";
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

function getPortfolioScope(overrideAccountId) {
  const activeAccountId = resolveValidAccountFilter(
    overrideAccountId != null ? overrideAccountId : state.selectedAccountId,
  );
  const trades = getFilteredTrades(activeAccountId);
  const cashTransfers = getFilteredCashTransfers(activeAccountId);
  return { accountId: activeAccountId, trades, cashTransfers };
}

function resolveStockSortKeyValue(row, key) {
  if (key === "currentPrice") {
    return Number(row.dayChangeRate) || 0;
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
    return nativeToOverviewBook(
      row,
      row.monthProfitNative ?? row.monthProfit,
      getOverviewBookCurrency(),
    );
  }
  if (key === "monthWeight") {
    return row.monthWeight;
  }
  if (key === "yearProfit") {
    return nativeToOverviewBook(
      row,
      row.yearProfitNative ?? row.yearProfit,
      getOverviewBookCurrency(),
    );
  }
  if (key === "yearWeight") {
    return row.yearWeight;
  }
  if (key === "totalProfit") {
    return nativeToOverviewBook(
      row,
      row.totalProfitNative ?? row.totalProfit,
      getOverviewBookCurrency(),
    );
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

/** 他人主页个股表排序：按对方展示账本币种 */
function resolvePublicProfileSortKeyValue(row, key, bookCcy, trades, denoms) {
  const tradeList = trades || state.trades;
  const toBk = (r, v) => nativeToOverviewBook(r, v, bookCcy);
  if (key === "currentPrice") {
    return row.currentPrice;
  }
  if (key === "weight") {
    return row.weight;
  }
  if (key === "cost") {
    return row.cost;
  }
  if (key === "monthWeight") {
    const d = Number(denoms?.monthDenPub) || 0;
    return d !== 0 ? toBk(row, row.monthProfitNative) / d : 0;
  }
  if (key === "yearWeight") {
    const d = Number(denoms?.yearDenPub) || 0;
    return d !== 0 ? toBk(row, row.yearProfitNative) / d : 0;
  }
  if (key === "totalRate") {
    return row.totalRate;
  }
  if (key === "regretRate") {
    return row.regretRate;
  }
  if (key === "lastTradeDate") {
    return Date.parse(row.lastTradeDate || 0);
  }
  return 0;
}

function sortPublicProfileStockRows(list, sortKey, sortOrder, bookCcy, trades, denoms) {
  const rows = [...list];
  if (!rows.length) {
    return rows;
  }
  if (sortOrder === "default" || sortKey === "default") {
    rows.sort((a, b) => {
      const w = (Number(b.weight) || 0) - (Number(a.weight) || 0);
      if (w !== 0) {
        return w;
      }
      const marketCmp = (MARKET_SORT_WEIGHT[a.market] || 99) - (MARKET_SORT_WEIGHT[b.market] || 99);
      if (marketCmp !== 0) {
        return marketCmp;
      }
      return Date.parse(b.lastTradeDate || 0) - Date.parse(a.lastTradeDate || 0);
    });
    return rows;
  }
  const key = sortKey;
  const direction = sortOrder === "asc" ? 1 : -1;
  if (key === "symbol" || key === "name") {
    rows.sort((a, b) => {
      const cmp = String(a.symbol || "").localeCompare(String(b.symbol || ""), "zh-CN");
      return cmp * direction;
    });
    return rows;
  }
  rows.sort((a, b) => {
    const av = resolvePublicProfileSortKeyValue(a, key, bookCcy, trades, denoms);
    const bv = resolvePublicProfileSortKeyValue(b, key, bookCcy, trades, denoms);
    return (av - bv) * direction;
  });
  return rows;
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

function formatTradeAccountCellHtml(trade, publicDetail) {
  const id = String(trade?.accountId || DEFAULT_ACCOUNT.id || "default").trim() || "default";
  const tradeAccountName = String(trade?.accountName || "").trim();
  if (tradeAccountName) {
    return escapeHtml(tradeAccountName);
  }
  if (publicDetail?.publicAccountNames && typeof publicDetail.publicAccountNames === "object") {
    const nm = publicDetail.publicAccountNames[id];
    const label = nm != null && String(nm).trim() ? String(nm) : id;
    return escapeHtml(label);
  }
  const acc = getAccountById(id);
  return escapeHtml(String(acc.name || id));
}

/** 个股页副标题：当前筛选下的股票账户名称（多账户时用顿号拼接） */
function stockRecordAccountCaption(scope, symbolTrades) {
  const aid = scope.accountId;
  if (aid && aid !== "all") {
    const acc = getAccountById(aid);
    return acc.name || "未命名账户";
  }
  const ids = [
    ...new Set(symbolTrades.map((t) => String(t.accountId || DEFAULT_ACCOUNT.id).trim()).filter(Boolean)),
  ];
  if (ids.length === 0) {
    return "—";
  }
  if (ids.length === 1) {
    return getAccountById(ids[0]).name || "未命名账户";
  }
  return ids
    .map((id) => getAccountById(id).name || id)
    .join("、");
}

function getFilteredTrades(accountId = "all") {
  if (accountId === "all") {
    return [...state.trades];
  }
  return state.trades.filter((trade) => trade.accountId === accountId);
}

function getFilteredCashTransfers(accountId = "all") {
  const list = Array.isArray(state.cashTransfers) ? state.cashTransfers : [];
  if (accountId === "all") {
    return [...list];
  }
  return list.filter((row) => String(row.accountId) === String(accountId));
}

/** 银证单条在账户记账币种下的 signed 金额（转入正、转出负）。 */
function cashTransferSignedNativeAmount(r) {
  const sign = r.direction === "out" ? -1 : 1;
  const nat = sign * Math.abs(Number(r.amount) || 0);
  return Number.isFinite(nat) ? nat : 0;
}

/**
 * 单条银证折算人民币净额：按 asOf 当日（估值日）汇率，不按发生日。
 * 总账户汇总、资金曲线、当日出入金流均用此口径。
 */
function cashTransferRowNetCnyAsOf(r, asOfDateKey) {
  const acc = getAccountById(r.accountId);
  const ccy = String((acc && acc.currency) || "CNY").toUpperCase();
  const nat = cashTransferSignedNativeAmount(r);
  if (nat === 0) {
    return 0;
  }
  const asOf = String(asOfDateKey || "").slice(0, 10);
  return ccy === "CNY" ? nat : nat * getFxRateForDate(ccy, asOf);
}

/** 全部账户：各子账户银证净额（原币）再按当前即期折人民币之和。 */
function aggregatePrincipalCnyAllAccountsAtSpot(ctf) {
  const byAcc = new Map();
  for (const r of Array.isArray(ctf) ? ctf : []) {
    const aid = String(r.accountId || "default");
    const nat = cashTransferSignedNativeAmount(r);
    if (nat === 0) {
      continue;
    }
    byAcc.set(aid, (byAcc.get(aid) || 0) + nat);
  }
  let sum = 0;
  for (const [aid, nat] of byAcc) {
    if (!Number.isFinite(nat) || nat === 0) {
      continue;
    }
    const acc = getAccountById(aid);
    const ccy = String((acc && acc.currency) || "CNY").toUpperCase();
    sum += ccy === "CNY" ? nat : nat * getFxRateToCny(ccy);
  }
  return sum;
}

/** 单个子账户：银证净额（原币，不按人民币中转）。 */
function principalNativeForFilteredAccount(ctf, accountId) {
  const aid = String(accountId || "default");
  let s = 0;
  for (const r of Array.isArray(ctf) ? ctf : []) {
    if (String(r.accountId || "default") !== aid) {
      continue;
    }
    s += cashTransferSignedNativeAmount(r);
  }
  return s;
}

/** 单笔成交对现金账的影响（标的币种）：买为负、卖为正；分红为正；拆股/合股/送股无现金流。 */
function tradeSignedCashNativeForLedger(trade) {
  const ty = String(trade.type || "trade");
  if (ty === "dividend") {
    return Math.abs(Number(trade.amount) || 0);
  }
  if (ty === "bonus" || ty === "split" || ty === "merge") {
    return 0;
  }
  // 与「发生额」买正卖负相反：现金账买入减少、卖出增加。
  return -signedAmount(trade);
}

/** 单笔成交现金流折算到账户记账币种（用成交日汇率）。 */
function tradeCashFlowInAccountCurrency(trade, accountCurrency) {
  const acc = String(accountCurrency || "CNY").toUpperCase();
  const dateKey = String(trade.date || "").slice(0, 10);
  const symCcy = String(getSymbolCurrency(trade.symbol, inferMarket(trade.symbol)) || "CNY").toUpperCase();
  const signedNat = tradeSignedCashNativeForLedger(trade);
  if (!Number.isFinite(signedNat) || signedNat === 0) {
    return 0;
  }
  const flowCny =
    symCcy === "CNY" ? signedNat : signedNat * getFxRateForDate(symCcy, dateKey);
  if (acc === "CNY") {
    return flowCny;
  }
  const fxAcc = getFxRateForDate(acc, dateKey);
  return Number.isFinite(fxAcc) && fxAcc > 0 ? flowCny / fxAcc : flowCny;
}

/** 资金记录在账户记账币种下的 signed 金额（转入正、转出负）。 */
function cashTransferDeltaNative(r) {
  const acc = getAccountById(r.accountId);
  const ccy = String((acc && acc.currency) || "CNY").toUpperCase();
  const sign = r.direction === "out" ? -1 : 1;
  const nat = sign * Math.abs(Number(r.amount) || 0);
  return { deltaNative: nat, accountCurrency: ccy };
}

function compareLedgerEvent(a, b) {
  const da = String(a.date || "").slice(0, 10);
  const db = String(b.date || "").slice(0, 10);
  if (da < db) {
    return -1;
  }
  if (da > db) {
    return 1;
  }
  const ca = Number(a.createdAt) || 0;
  const cb = Number(b.createdAt) || 0;
  if (ca !== cb) {
    return ca - cb;
  }
  if (a.kind !== b.kind) {
    return a.kind === "ct" ? -1 : 1;
  }
  return String(a.id || "").localeCompare(String(b.id || ""));
}

/**
 * 现金 = 各账户期初 0 起按时间滚：入金 − 出金 + 成交现金流（折记账币），期末余额再折人民币用即期汇率汇总。
 * 本金不在此计算：单账户用原币净额，全部账户用各账户原币净额按即期折人民币（见 computePortfolio）。
 */
function computeLedgerCashAndPrincipal(tradeList, ctf) {
  const rowsCtf = Array.isArray(ctf) ? ctf : [];
  const rowsTr = Array.isArray(tradeList) ? tradeList : [];

  const accountIds = new Set();
  for (const r of rowsCtf) {
    accountIds.add(String(r.accountId || "default"));
  }
  for (const t of rowsTr) {
    accountIds.add(String(t.accountId || "default"));
  }

  const endingNativeByAccount = new Map();
  for (const accId of accountIds) {
    const acc = getAccountById(accId);
    const accCcy = String((acc && acc.currency) || "CNY").toUpperCase();
    const events = [];
    for (const r of rowsCtf) {
      if (String(r.accountId || "default") !== accId) {
        continue;
      }
      const { deltaNative } = cashTransferDeltaNative(r);
      events.push({
        kind: "ct",
        id: String(r.id || ""),
        date: r.date,
        createdAt: r.createdAt,
        delta: deltaNative,
      });
    }
    for (const t of rowsTr) {
      if (String(t.accountId || "default") !== accId) {
        continue;
      }
      const flowAcc = tradeCashFlowInAccountCurrency(t, accCcy);
      events.push({
        kind: "tr",
        id: String(t.id || ""),
        date: t.date,
        createdAt: t.createdAt,
        delta: flowAcc,
      });
    }
    events.sort(compareLedgerEvent);
    let bal = 0;
    for (const ev of events) {
      bal += ev.delta;
    }
    endingNativeByAccount.set(accId, bal);
  }

  let cashCny = 0;
  for (const accId of accountIds) {
    const balNat = Number(endingNativeByAccount.get(accId)) || 0;
    const acc = getAccountById(accId);
    const ccy = String((acc && acc.currency) || "CNY").toUpperCase();
    if (ccy === "CNY") {
      cashCny += balNat;
    } else {
      const fx = getFxRateToCny(ccy);
      cashCny += balNat * (Number.isFinite(fx) && fx > 0 ? fx : 1);
    }
  }

  return { cashCny, endingNativeByAccount };
}

/** 截至 endDateKey（含）的资金记录净额 Σ资金（人民币计） */
function fundNetCnyUpToDate(ctf, endDateKey) {
  if (!Array.isArray(ctf) || !endDateKey) {
    return 0;
  }
  const end = String(endDateKey).slice(0, 10);
  let sum = 0;
  for (const row of ctf) {
    const d = String(row.date || "").slice(0, 10);
    if (d && d <= end) {
      sum += cashTransferRowNetCnyAsOf(row, end);
    }
  }
  return sum;
}

/**
 * 与 fundNetCnyUpToDate 同口径的逐日累计 Σ资金（仅用于与 points 等长的序列表，避免 O(n²)）
 */
function fundCnyCumulativeAlongDates(ctf, dateKeys) {
  const m = new Map();
  if (!Array.isArray(dateKeys) || !dateKeys.length) {
    return m;
  }
  if (!Array.isArray(ctf) || !ctf.length) {
    const keys = [...new Set(dateKeys.map((d) => String(d || "").slice(0, 10)).filter(Boolean))].sort();
    for (const d of keys) {
      m.set(d, 0);
    }
    return m;
  }
  const rows = (Array.isArray(ctf) ? ctf : [])
    .map((row) => ({
      d: String(row.date || "").slice(0, 10),
      aid: String(row.accountId || "default"),
      nat: cashTransferSignedNativeAmount(row),
    }))
    .filter((x) => x.d);
  const keys = [...new Set(dateKeys.map((d) => String(d || "").slice(0, 10)).filter(Boolean))].sort();
  for (const d of keys) {
    let cum = 0;
    for (const row of rows) {
      if (row.d <= d && row.nat !== 0) {
        const acc = getAccountById(row.aid);
        const ccy = String((acc && acc.currency) || "CNY").toUpperCase();
        cum += ccy === "CNY" ? row.nat : row.nat * getFxRateForDate(ccy, d);
      }
    }
    m.set(d, cum);
  }
  return m;
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
  const legacyAlias = getLegacyUsAlias(normalized);
  const fromMap = (state.nameMap[normalized] || (legacyAlias ? state.nameMap[legacyAlias] : "") || "").trim();
  const quoteName = quoteNameForDisplay(symbol, getQuoteBySymbol(symbol)?.name);
  const m = inferMarket(normalized);
  const fallbackCode = formatSymbolForDisplay(normalized);
  if (m === "A股" || m === "港股") {
    return (hasCnNameLabel(fromMap) ? fromMap : "") || quoteName || fallbackName || fallbackCode;
  }
  return fromMap || quoteName || fallbackName || fallbackCode;
}

function getQuoteBySymbol(symbol) {
  const normalized = normalizeSymbol(symbol || "");
  if (!normalized) {
    return {};
  }
  const legacyAlias = getLegacyUsAlias(normalized);
  return state.quoteMap[normalized] || (legacyAlias ? state.quoteMap[legacyAlias] : null) || {};
}

function getKlineBySymbol(symbol) {
  const normalized = normalizeSymbol(symbol || "");
  if (!normalized) {
    return [];
  }
  const legacyAlias = getLegacyUsAlias(normalized);
  return state.klineMap[normalized] || (legacyAlias ? state.klineMap[legacyAlias] : null) || [];
}

function normalizeSymbolList(input) {
  return [...new Set((input || []).map((s) => normalizeSymbol(String(s || ""))).filter(Boolean))];
}

function markSymbolNameFetched(symbol) {
  const normalized = normalizeSymbol(symbol);
  if (!normalized) {
    return;
  }
  symbolNameFetchedAt.set(normalized, Date.now());
}

function upsertNameMapEntry(symbol, name) {
  const normalized = normalizeSymbol(symbol);
  const label = String(name || "").trim();
  if (!normalized || !label) {
    return;
  }
  state.nameMap[normalized] = label;
  const legacyAlias = getLegacyUsAlias(normalized);
  if (legacyAlias) {
    state.nameMap[legacyAlias] = label;
  }
  markSymbolNameFetched(normalized);
}

async function hydrateSymbolNameMap(symbols, options = {}) {
  if (!apiReady) {
    return;
  }
  const force = options.force === true;
  const uniq = normalizeSymbolList(symbols);
  if (!uniq.length) {
    return;
  }
  const now = Date.now();
  const pending = force
    ? uniq
    : uniq.filter((symbol) => {
        if (state.nameMap[symbol]) {
          return false;
        }
        const lastTs = Number(symbolNameFetchedAt.get(symbol) || 0);
        return !lastTs || now - lastTs > SYMBOL_NAME_MAP_TTL_MS;
      });
  if (!pending.length) {
    return;
  }
  const key = pending.slice().sort().join(",");
  const inFlight = symbolNameHydrateInFlight.get(key);
  if (inFlight) {
    await inFlight;
    return;
  }
  const task = (async () => {
    try {
      const response = await apiFetch(
        `${getApiBaseForFetch()}/symbol-name-map?symbols=${encodeURIComponent(pending.join(","))}`,
        { cache: "no-store", timeoutMs: 10_000 }
      );
      const payload = await response.json().catch(() => ({}));
      const map = payload?.ok && payload.data && typeof payload.data === "object" ? payload.data : {};
      for (const symbol of pending) {
        const name = String(map[symbol] || "").trim();
        if (name) {
          upsertNameMapEntry(symbol, name);
        } else {
          markSymbolNameFetched(symbol);
        }
      }
    } catch {
      // ignore network failures; next cycle retries
    }
  })();
  symbolNameHydrateInFlight.set(key, task);
  try {
    await task;
  } finally {
    symbolNameHydrateInFlight.delete(key);
  }
}

function flushPendingSymbolNameUpserts() {
  symbolNameUpsertFlushTimer = null;
  if (!apiReady) {
    symbolNamePendingUpsertBySymbol.clear();
    return;
  }
  const rows = [...symbolNamePendingUpsertBySymbol.values()];
  symbolNamePendingUpsertBySymbol.clear();
  if (!rows.length) {
    return;
  }
  void apiFetch(`${getApiBaseForFetch()}/admin/upsert-symbol-name-map`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ rows }),
    cache: "no-store",
    timeoutMs: 12_000,
  }).catch(() => {});
}

function queueSymbolNameUpsertRows(rows) {
  if (!rows.length) {
    return;
  }
  for (const row of rows) {
    const sym = String(row?.symbol || "").trim();
    if (sym) {
      symbolNamePendingUpsertBySymbol.set(sym, row);
    }
  }
  if (!symbolNameUpsertFlushTimer) {
    symbolNameUpsertFlushTimer = window.setTimeout(flushPendingSymbolNameUpserts, SYMBOL_NAME_UPSERT_DEBOUNCE_MS);
  }
}

async function syncSymbolNamesFromQuotes(quoteMap = {}) {
  if (!apiReady || !quoteMap || typeof quoteMap !== "object") {
    return;
  }
  const now = Date.now();
  const rows = [];
  for (const [symbol, quote] of Object.entries(quoteMap)) {
    const normalized = normalizeSymbol(symbol);
    if (!normalized) {
      continue;
    }
    const display = quoteNameForDisplay(normalized, quote?.name);
    if (!display) {
      continue;
    }
    const syncKey = `${normalized}|${display}`;
    const lastTs = Number(symbolNameSyncedAt.get(syncKey) || 0);
    if (lastTs && now - lastTs < SYMBOL_NAME_MAP_TTL_MS) {
      continue;
    }
    rows.push({ symbol: normalized, nameCn: display, source: "tencent" });
    symbolNameSyncedAt.set(syncKey, now);
  }
  if (!rows.length) {
    return;
  }
  queueSymbolNameUpsertRows(rows);
}

function shiftDateKeyByDays(dateKey, deltaDays) {
  if (!dateKey) {
    return toDateKey(new Date());
  }
  const t = new Date(`${String(dateKey).slice(0, 10)}T12:00:00`);
  if (!Number.isFinite(t.getTime())) {
    return toDateKey(new Date());
  }
  t.setDate(t.getDate() + Number(deltaDays || 0));
  return toDateKey(t);
}

function symbolCloseRowsToKline(arr) {
  return (Array.isArray(arr) ? arr : [])
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
}

/** 按需从服务端 symbol_daily_close 快照灌入 klineMap（不在全站行情轮询里批量拉） */
async function fetchSymbolCloseIntoKlineMap(symbols = [], days = DAILY_CLOSE_HYDRATE_WINDOW_DAYS, opts = {}) {
  if (!apiReady) {
    return;
  }
  const picked = [...new Set((symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))].sort();
  if (!picked.length) {
    return;
  }
  const chunk = 40;
  const waveSize = Math.max(1, Math.min(8, Number(opts.parallelChunks) || 4));
  const d = Math.max(30, Math.min(2000, Number(days) || DAILY_CLOSE_HYDRATE_WINDOW_DAYS));
  const parts = [];
  for (let i = 0; i < picked.length; i += chunk) {
    parts.push(picked.slice(i, i + chunk));
  }
  async function fetchOnePart(part) {
    try {
      const r = await apiFetch(
        `${API_BASE}/snapshot/symbol-close?days=${encodeURIComponent(String(d))}&symbols=${encodeURIComponent(
          part.join(","),
        )}`,
        { cache: "no-store", timeoutMs: 22_000 },
      );
      if (!r.ok) {
        return;
      }
      const j = await r.json().catch(() => ({}));
      if (!j?.ok || !j.data || typeof j.data !== "object") {
        return;
      }
      Object.entries(j.data).forEach(([sym, rows]) => {
        const list = symbolCloseRowsToKline(rows);
        if (!list.length) {
          return;
        }
        const normalized = normalizeSymbol(sym);
        state.klineMap[normalized] = list;
        const legacyAlias = getLegacyUsAlias(normalized);
        if (legacyAlias) {
          state.klineMap[legacyAlias] = list;
        }
      });
    } catch {
      // ignore chunk
    }
  }
  for (let i = 0; i < parts.length; i += waveSize) {
    await Promise.all(parts.slice(i, i + waveSize).map((p) => fetchOnePart(p)));
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

/** 腾讯行情 `~` 分段里的金额，可能含千分位逗号 */
function parseTencentPriceField(segment) {
  if (segment == null) {
    return NaN;
  }
  const t = String(segment).trim().replace(/,/g, "");
  const n = Number(t);
  return Number.isFinite(n) ? n : NaN;
}

function nthWeekdayOfMonth(year, month, weekday, nth) {
  const firstDow = new Date(Date.UTC(year, month - 1, 1)).getUTCDay();
  return 1 + ((7 + weekday - firstDow) % 7) + (Math.max(1, nth) - 1) * 7;
}

function isUsEasternDstByLocalParts(parts) {
  if (!parts) {
    return false;
  }
  const year = Number(parts.year);
  const month = Number(parts.month);
  const day = Number(parts.day);
  const hour = Number(parts.hour || 0);
  if (![year, month, day, hour].every(Number.isFinite)) {
    return false;
  }
  if (month < 3 || month > 11) {
    return false;
  }
  if (month > 3 && month < 11) {
    return true;
  }
  const secondSundayInMarch = nthWeekdayOfMonth(year, 3, 0, 2);
  const firstSundayInNovember = nthWeekdayOfMonth(year, 11, 0, 1);
  if (month === 3) {
    if (day > secondSundayInMarch) {
      return true;
    }
    if (day < secondSundayInMarch) {
      return false;
    }
    return hour >= 2;
  }
  if (day < firstSundayInNovember) {
    return true;
  }
  if (day > firstSundayInNovember) {
    return false;
  }
  return hour < 2;
}

function convertUsEasternTimeToBeijing(timeStr) {
  const parts = parseQuoteTimeParts(timeStr);
  if (!parts) {
    return "";
  }
  const year = Number(parts.year);
  const month = Number(parts.month);
  const day = Number(parts.day);
  const hour = Number(parts.hour || 0);
  const minute = Number(parts.minute || 0);
  const second = Number(parts.second || 0);
  if (![year, month, day, hour, minute, second].every(Number.isFinite)) {
    return "";
  }
  const diffHours = isUsEasternDstByLocalParts(parts) ? 12 : 13;
  const baseMs = Date.UTC(year, month - 1, day, hour, minute, second);
  const bj = new Date(baseMs + diffHours * 60 * 60 * 1000);
  return [
    String(bj.getUTCFullYear()).padStart(4, "0"),
    String(bj.getUTCMonth() + 1).padStart(2, "0"),
    String(bj.getUTCDate()).padStart(2, "0"),
    String(bj.getUTCHours()).padStart(2, "0"),
    String(bj.getUTCMinutes()).padStart(2, "0"),
    String(bj.getUTCSeconds()).padStart(2, "0"),
  ].join("");
}

function normalizeQuoteTimeToBeijingBySymbol(timeStr, symbol) {
  const raw = String(timeStr || "").trim();
  if (!raw || raw === "--") {
    return "--";
  }
  const normalized = normalizeSymbol(symbol || "");
  if (!isUsTickerSymbol(normalized)) {
    return raw;
  }
  return convertUsEasternTimeToBeijing(raw) || raw;
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
  const rawTime = String(parts[30] || parts[31] || "--").trim();
  const time = normalizeQuoteTimeToBeijingBySymbol(rawTime, symbol);
  // 计算口径必须使用腾讯原始行情日期（市场日），展示时间才使用北京时间。
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
    // 兼容旧字段：保持 quoteDate 存在，值与 marketDate 一致。
    quoteDate: marketDate,
  };
}

/** 腾讯 qt 外汇：`whUSDCNY` / `whHKDCNY`，~ 分段 3 当前价、4 昨收 */
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

function toQuoteRequestSymbol(symbol) {
  if (!symbol) {
    return symbol;
  }
  return normalizeSymbol(symbol) || symbol;
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
  let staticParsed = null;
  const boot = await fetchApiStateBootstrap();
  apiReady = boot.apiReady;
  const bootstrapKind = boot.bootstrapKind || "none";
  if (apiReady) {
    remoteParsed = boot.data;
  }
  // Only use the baked-in snapshot on static hosts (GitHub Pages). When the API is up,
  if (!apiReady) {
    staticParsed = await fetchStaticSiteState();
  }
  // 服务端可用时：以 /api/settings 为准。
  if (remoteParsed && typeof remoteParsed === "object") {
    parsed = remoteParsed;
  } else if (staticParsed && Array.isArray(staticParsed.trades) && staticParsed.trades.length) {
    parsed = staticParsed;
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
    state.algoMode = normalizeProfitAlgoMode(parsed.algoMode ?? state.algoMode);
    state.benchmark = parsed.benchmark ?? state.benchmark;
    state.stageRange = parsed.stageRange ?? state.stageRange;
    state.rangeDays = parsed.rangeDays ?? state.rangeDays;
    state.analysisRangeMode = parsed.analysisRangeMode ?? state.analysisRangeMode;
    state.analysisPreset =
      parsed.analysisPreset === "mtd" || parsed.analysisPreset === "ytd" ? parsed.analysisPreset : null;
    state.customRangeStart = parsed.customRangeStart ?? state.customRangeStart;
    state.customRangeEnd = parsed.customRangeEnd ?? state.customRangeEnd;
    state.capitalTrendMode = parsed.capitalTrendMode ?? state.capitalTrendMode;
    state.capitalAmount = Number(parsed.capitalAmount ?? 0);
    state.accounts = normalizeAccounts(parsed.accounts);
    state.selectedAccountId = parsed.selectedAccountId ?? state.selectedAccountId;
    state.tradeFilterAccountId = parsed.tradeFilterAccountId ?? state.tradeFilterAccountId;
    state.stockRecordAccountId = parsed.stockRecordAccountId ?? state.stockRecordAccountId;
    state.stockSortKey = parsed.stockSortKey ?? state.stockSortKey;
    state.stockSortOrder = parsed.stockSortOrder ?? state.stockSortOrder;
    state.stockAmountDisplay =
      parsed.stockAmountDisplay === "cny" || parsed.stockAmountDisplay === "native"
        ? parsed.stockAmountDisplay
        : "native";
    state.trades = Array.isArray(parsed.trades) ? parsed.trades.map(normalizeTrade) : [];
    state.cashTransfers = Array.isArray(parsed.cashTransfers)
      ? parsed.cashTransfers.map(normalizeCashTransferRow)
      : [];
    state.tradePanelTab = parsed.tradePanelTab === "cash" ? "cash" : "trades";
    state.appModule = parsed.appModule === "community" ? "community" : "holdings";
  }
  // trades + cash-transfers are loaded lazily on trade/analysis routes, not on home page load
  if (!["month", "ytd", "total"].includes(state.stageRange)) {
    state.stageRange = "month";
  }
  if (!["preset", "custom", "all"].includes(state.analysisRangeMode)) {
    state.analysisRangeMode = "preset";
  }
  if (state.capitalTrendMode === "both" || state.capitalTrendMode === "principal") {
    state.capitalTrendMode = "total_assets";
  }
  if (!["total_assets", "market", "cash", "cash_ratio"].includes(state.capitalTrendMode)) {
    state.capitalTrendMode = "total_assets";
  }
  if (state.stockAmountDisplay !== "cny" && state.stockAmountDisplay !== "native") {
    state.stockAmountDisplay = "native";
  }
  if (!sessionPhone) {
    if (state.useDemoData && state.trades.length === 0) {
      state.trades = demoTrades.map((item) => ({ ...item }));
    }
    if (state.trades.length === 0) {
      state.useDemoData = true;
      state.trades = demoTrades.map((item) => ({ ...item }));
    }
  } else if (state.trades.length === 0) {
    state.useDemoData = false;
  }
  if (![7, 30, 90, 365].includes(Number(state.rangeDays))) {
    state.rangeDays = 30;
  }
  if (
    !state.analysisPreset &&
    state.analysisRangeMode === "preset" &&
    Number(state.rangeDays) === 365
  ) {
    state.analysisPreset = "ytd";
  }
  if (state.analysisPreset && state.analysisRangeMode !== "preset") {
    state.analysisPreset = null;
  }
  state.trades = state.trades.map((trade) => {
    if (!state.accounts.some((account) => account.id === trade.accountId)) {
      return { ...trade, accountId: DEFAULT_ACCOUNT.id };
    }
    return trade;
  });
  state.selectedAccountId = resolveValidAccountFilter(state.selectedAccountId);
  state.tradeFilterAccountId = resolveValidAccountFilter(state.tradeFilterAccountId);
  state.stockRecordAccountId = resolveValidAccountFilter(state.stockRecordAccountId);
  state.customRangeDraftStart = state.customRangeStart;
  state.customRangeDraftEnd = state.customRangeEnd;
  if (!["holdings", "community"].includes(state.appModule)) {
    state.appModule = "holdings";
  }
  if (state.route?.startsWith("community-") && state.route !== "community-profile") {
    state.appModule = "community";
  }
  const holdingsRoutes = new Set([
    "earning",
    "analysis",
    "trade",
    "trade-records",
    "trade-cash",
    "holdings-ai",
    "trade-search",
  ]);
  if (holdingsRoutes.has(state.route)) {
    state.appModule = "holdings";
  }
  if (state.route === "community-profile") {
    state.route = "community-feed";
    state.appModule = "community";
    state.communityProfileUserId = null;
  }
  if (state.route === "trade-search") {
    state.route = "trade";
  }
  normalizeModuleHomeOnColdLoad();
  invalidateOverviewMetricsUi();
}

function pickSettingsPayload(payload = {}) {
  return STATE_SYNC_KEYS.reduce((acc, key) => {
    if (Object.hasOwn(payload, key)) {
      acc[key] = payload[key];
    }
    return acc;
  }, {});
}

function queueSettingsSyncToApi(payload) {
  if (!apiReady) {
    return;
  }
  const patch = pickSettingsPayload(payload);
  if (!Object.keys(patch).length) {
    return;
  }
  pendingSettingsSyncPayload = { ...(pendingSettingsSyncPayload || {}), ...patch };
  if (pendingSettingsSyncTimer) {
    window.clearTimeout(pendingSettingsSyncTimer);
  }
  pendingSettingsSyncTimer = window.setTimeout(() => {
    const merged = pendingSettingsSyncPayload;
    pendingSettingsSyncPayload = null;
    pendingSettingsSyncTimer = null;
    if (merged && Object.keys(merged).length) {
      void pushSettingsToApi(merged);
    }
  }, SETTINGS_SYNC_DEBOUNCE_MS);
}

let initialSettingsSyncTimer = null;

function buildPersistStateSyncPayload() {
  let routeForSync = state.route;
  if (routeForSync === "trade-search" || routeForSync === "trade-records" || routeForSync === "trade-cash") {
    routeForSync = "trade";
  }
  return {
    route: routeForSync,
    appModule: state.appModule,
    useDemoData: state.useDemoData,
    algoMode: state.algoMode,
    benchmark: state.benchmark,
    stageRange: state.stageRange,
    rangeDays: state.rangeDays,
    analysisRangeMode: state.analysisRangeMode,
    analysisPreset: state.analysisPreset,
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
  };
}

function scheduleDeferredInitialSettingsSync() {
  if (!apiReady || !sessionPhone) {
    return;
  }
  if (initialSettingsSyncTimer) {
    window.clearTimeout(initialSettingsSyncTimer);
  }
  initialSettingsSyncTimer = window.setTimeout(() => {
    initialSettingsSyncTimer = null;
    queueSettingsSyncToApi(buildPersistStateSyncPayload());
  }, INITIAL_SETTINGS_SYNC_DEFER_MS);
}

function persistState(options = {}) {
  const payload = buildPersistStateSyncPayload();
  if (apiReady && !options.skipSettingsSync) {
    if (initialSettingsSyncTimer) {
      window.clearTimeout(initialSettingsSyncTimer);
      initialSettingsSyncTimer = null;
    }
    queueSettingsSyncToApi(payload);
  }
}

async function fetchApiStateBootstrap() {
  try {
    const response = await apiFetch(`${getApiBaseForFetch()}/settings`, {
      cache: "no-store",
      timeoutMs: 12_000,
    });
    if (!response.ok && response.status !== 401) {
      return { apiReady: false, data: null, bootstrapKind: "none" };
    }
    if (response.status === 401) {
      return { apiReady: true, data: null, bootstrapKind: "none" };
    }
    const result = await response.json().catch(() => ({}));
    if (result?.ok && result.data) {
      return { apiReady: true, data: result.data, bootstrapKind: "lite" };
    }
    return { apiReady: true, data: null, bootstrapKind: "none" };
  } catch {
    return { apiReady: false, data: null, bootstrapKind: "none" };
  }
}

function normalizeMetricsAccountId(aid) {
  return aid === "all" ? "all" : String(aid || "").trim() || "all";
}

function homeBundleInflightKey(aid, stages) {
  return `${normalizeMetricsAccountId(aid)}|${String(stages || METRICS_HOME_BUNDLE_STAGES).trim() || METRICS_HOME_BUNDLE_STAGES}`;
}

function invalidateHomeBundleInflight(accountId) {
  if (accountId == null || accountId === "") {
    homeBundleInflightByKey.clear();
    return;
  }
  const prefix = `${normalizeMetricsAccountId(accountId)}|`;
  for (const key of [...homeBundleInflightByKey.keys()]) {
    if (key.startsWith(prefix)) {
      homeBundleInflightByKey.delete(key);
    }
  }
}

/** home-bundle 单飞 + 短时结果复用；URL 仅 account_id + stages（仅持仓收益首页拉取） */
async function fetchHomeBundleMetrics(aid, opts = {}) {
  if (!opts.allowOffEarning && !isEarningHomeRoute()) {
    return null;
  }
  const id = normalizeMetricsAccountId(aid);
  const stages = String(opts.stages || METRICS_HOME_BUNDLE_STAGES).trim() || METRICS_HOME_BUNDLE_STAGES;
  const key = homeBundleInflightKey(id, stages);
  const slot = homeBundleInflightByKey.get(key);
  if (slot?.resolved !== undefined) {
    return slot.resolved;
  }
  if (slot?.promise) {
    return slot.promise;
  }
  if (!opts.allowBeforeApiReady && !apiReady) {
    return null;
  }
  const promise = (async () => {
    const qs = new URLSearchParams({ account_id: id, stages });
    const url = `${getApiBaseForFetch()}/metrics/home-bundle?${qs.toString()}`;
    const res = await apiFetch(url, { cache: "no-store", timeoutMs: 55_000 });
    const j = await res.json().catch(() => ({}));
    if (!res.ok || !j?.ok || !j.data) {
      return null;
    }
    return j.data;
  })();
  homeBundleInflightByKey.set(key, { promise });
  try {
    const data = await promise;
    homeBundleInflightByKey.set(key, { promise: Promise.resolve(data), resolved: data });
    return data;
  } catch {
    homeBundleInflightByKey.delete(key);
    return null;
  }
}

function resetTradeListPager() {
  tradeListPager.gen += 1;
  tradeListPager.offset = 0;
  tradeListPager.hasMore = true;
  tradeListPager.loading = false;
  tradeListPager.loaded = false;
  tradeListPager.accountId = resolveValidAccountFilter(state.tradeFilterAccountId);
  state.trades = [];
  ledgerBootstrapCompleteForUid = "";
}

function resetCashListPager() {
  cashListPager.gen += 1;
  cashListPager.offset = 0;
  cashListPager.hasMore = true;
  cashListPager.loading = false;
  cashListPager.loaded = false;
  cashListPager.accountId = resolveValidAccountFilter(state.tradeFilterAccountId);
  state.cashTransfers = [];
  ledgerBootstrapCompleteForUid = "";
}

function resetStockRecordTradesPager() {
  stockRecordTradesPager.gen += 1;
  stockRecordTradesPager.offset = 0;
  stockRecordTradesPager.hasMore = true;
  stockRecordTradesPager.loading = false;
  stockRecordTradesPager.loaded = false;
  stockRecordTradesPager.symbol = "";
  stockRecordTradesPager.accountId = "all";
}

function stockRecordTradesListQuery(symbol, accountId, offset) {
  const params = new URLSearchParams({
    symbol: normalizeSymbol(symbol),
    limit: String(TRADE_LIST_PAGE_SIZE),
    offset: String(offset),
  });
  const aid = resolveValidAccountFilter(accountId);
  if (aid && aid !== "all") {
    params.set("accountId", aid);
  }
  return params.toString();
}

function publicTradesListQuery(symbol, accountId, offset) {
  const params = new URLSearchParams({
    limit: String(TRADE_LIST_PAGE_SIZE),
    offset: String(offset),
  });
  const sym = symbol ? normalizeSymbol(symbol) : "";
  if (sym) {
    params.set("symbol", sym);
  }
  const aid = resolveValidAccountFilter(accountId);
  if (aid && aid !== "all") {
    params.set("account_id", aid);
  }
  return params.toString();
}

function resetCommunityPublicTradesPager() {
  communityPublicTradesPager.gen += 1;
  communityPublicTradesPager.offset = 0;
  communityPublicTradesPager.hasMore = true;
  communityPublicTradesPager.loading = false;
  communityPublicTradesPager.loaded = false;
  communityPublicTradesPager.targetId = "";
}

async function loadStockRecordTradesPage({ reset = false } = {}) {
  const symKey = normalizeSymbol(state.activeRecordSymbol);
  const isPublic = state.stockRecordFromPublicProfile;
  const publicTargetId = isPublic ? stockRecordPublicTargetId() : "";
  if (!symKey || !apiReady || !sessionPhone || (isPublic && !publicTargetId)) {
    state.stockRecordTrades = [];
    state.stockRecordTradesLoading = false;
    return;
  }
  if (state.route !== "stock-record") {
    return;
  }
  ensureStockRecordScrollListener();
  const aid = resolveValidAccountFilter(state.stockRecordAccountId);
  if (reset || stockRecordTradesPager.symbol !== symKey || stockRecordTradesPager.accountId !== aid) {
    resetStockRecordTradesPager();
    stockRecordTradesPager.symbol = symKey;
    stockRecordTradesPager.accountId = aid;
    state.stockRecordTrades = [];
  }
  if (stockRecordTradesPager.loading || (!stockRecordTradesPager.hasMore && stockRecordTradesPager.loaded)) {
    state.stockRecordTradesLoading = stockRecordTradesPager.loading;
    if (state.route === "stock-record" && normalizeSymbol(state.activeRecordSymbol) === symKey) {
      await renderStockRecordPage(symKey);
    }
    return;
  }
  const gen = stockRecordTradesPager.gen;
  stockRecordTradesPager.loading = true;
  state.stockRecordTradesLoading = true;
  if (stockRecordTradesPager.loaded) {
    await renderStockRecordPage(symKey);
  }
  try {
    const qs = isPublic
      ? publicTradesListQuery(symKey, aid, stockRecordTradesPager.offset)
      : stockRecordTradesListQuery(symKey, aid, stockRecordTradesPager.offset);
    const url = isPublic
      ? `${getApiBaseForFetch()}/public/${encodeURIComponent(publicTargetId)}/trades?${qs}`
      : `${API_BASE}/trades?${qs}`;
    const res = await apiFetch(url, { cache: "no-store", timeoutMs: 25_000 });
    const body = await res.json().catch(() => ({}));
    if (gen !== stockRecordTradesPager.gen || state.route !== "stock-record") {
      return;
    }
    if (!res.ok || body?.ok !== true || !Array.isArray(body.data)) {
      stockRecordTradesPager.hasMore = false;
      return;
    }
    const rows = body.data.map(normalizeTrade);
    const seen = new Set(state.stockRecordTrades.map((t) => String(t.id)));
    for (const row of rows) {
      const id = String(row.id);
      if (!seen.has(id)) {
        state.stockRecordTrades.push(row);
        seen.add(id);
      }
    }
    const pagination = body.pagination || {};
    stockRecordTradesPager.offset = Number(pagination.offset ?? stockRecordTradesPager.offset) + rows.length;
    stockRecordTradesPager.hasMore = pagination.hasMore === true;
    stockRecordTradesPager.loaded = true;
  } catch (error) {
    console.warn("loadStockRecordTradesPage failed", error);
    if (gen === stockRecordTradesPager.gen) {
      stockRecordTradesPager.hasMore = false;
    }
  } finally {
    if (gen === stockRecordTradesPager.gen) {
      stockRecordTradesPager.loading = false;
      state.stockRecordTradesLoading = false;
      if (state.route === "stock-record" && normalizeSymbol(state.activeRecordSymbol) === symKey) {
        await renderStockRecordPage(symKey);
      }
    }
  }
}

function setStockRecordPageLoading(loading) {
  state.stockRecordPageLoading = loading === true;
  stockRecordLoading?.classList.toggle("hidden", !state.stockRecordPageLoading);
  stockRecordBody?.classList.toggle("hidden", state.stockRecordPageLoading);
  if (stockRecordLoading) {
    stockRecordLoading.setAttribute("aria-busy", state.stockRecordPageLoading ? "true" : "false");
  }
  if (state.stockRecordPageLoading) {
    clearStockRecordChart();
  }
}

function setStockRecordChartPointsLoading(loading) {
  state.stockRecordPointsLoading = loading === true;
  stockRecordChartsLoading?.classList.toggle("hidden", !state.stockRecordPointsLoading);
  if (stockRecordChartsLoading) {
    stockRecordChartsLoading.setAttribute("aria-busy", state.stockRecordPointsLoading ? "true" : "false");
  }
  stockRecordRangeRow?.classList.toggle("is-loading", state.stockRecordPointsLoading);
}

function clearStockRecordChart() {
  for (const canvas of [stockRecordChart, stockRecordProfitChart, stockRecordWeightChart]) {
    if (!canvas) {
      continue;
    }
    const ctx = canvas.getContext("2d");
    if (ctx) {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
    }
  }
}

function parseBundlePlainNumber(value) {
  if (value == null || value === "—" || value === "--") {
    return null;
  }
  const n = Number(String(value).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function parseBundlePercent(value) {
  if (!value || value === "—") {
    return 0;
  }
  const m = String(value).match(/(-?\d+(?:\.\d+)?)/);
  return m ? Number(m[1]) / 100 : 0;
}

async function fetchStockRecordBundleMetrics(symKey, accountId = "all", publicTargetId = "", chartOpts = {}) {
  if (!apiReady) {
    return null;
  }
  const params = {
    symbol: normalizeSymbol(symKey),
    account_id: resolveValidAccountFilter(accountId),
  };
  if (chartOpts.range != null && String(chartOpts.range).trim() !== "") {
    params.range = String(chartOpts.range).trim();
  } else {
    params.limit = String(chartOpts.limit ?? STOCK_RECORD_CHART_PAGE_SIZE);
    params.offset = String(chartOpts.offset ?? 0);
  }
  return fetchMetricsApi("/metrics/stock-record-bundle", params, publicTargetId);
}

function stockRecordPublicTargetId() {
  if (!state.stockRecordFromPublicProfile) {
    return "";
  }
  return String(state.lastPublicProfileDetail?.userId || state.communityProfileUserId || "").trim();
}

function resetStockRecordChartCache() {
  state.stockRecordChartPointsCache = [];
  state.stockRecordChartHistoryComplete = false;
  state.stockRecordChartViewExpanded = false;
}

function resetStockRecordChartPagination() {
  state.stockRecordChartPagination = {
    limit: STOCK_RECORD_CHART_PAGE_SIZE,
    nextOffset: 0,
    hasMore: false,
  };
  setStockRecordChartPointsLoading(false);
}

function stockRecordChartEndDateKey() {
  const meta = state.stockRecordBundle?.meta || {};
  if (meta.tradingDay && meta.liveDate) {
    return String(meta.liveDate).slice(0, 10);
  }
  const frozen = String(meta.frozenThrough || "").slice(0, 10);
  if (frozen) {
    return frozen;
  }
  return toDateKey(new Date());
}

function stockRecordRangeFromDateKey(range, endDate) {
  const end = String(endDate || "").slice(0, 10);
  if (!end) {
    return null;
  }
  const preset = String(range || "30").trim().toLowerCase();
  if (preset === "all") {
    return null;
  }
  if (preset === "mtd") {
    const d = new Date(`${end}T12:00:00`);
    return toDateKey(new Date(d.getFullYear(), d.getMonth(), 1));
  }
  if (preset === "ytd") {
    const d = new Date(`${end}T12:00:00`);
    return toDateKey(new Date(d.getFullYear(), 0, 1));
  }
  const days = Number(preset);
  if (Number.isFinite(days) && days > 0) {
    return addCalendarDaysToDateKey(end, -days);
  }
  return addCalendarDaysToDateKey(end, -30);
}

function stockRecordChartCacheOldestDate() {
  const cache = state.stockRecordChartPointsCache;
  if (!Array.isArray(cache) || !cache.length) {
    return null;
  }
  let oldest = null;
  for (const row of cache) {
    const dk = String(row?.date || "").slice(0, 10);
    if (dk && (!oldest || dk < oldest)) {
      oldest = dk;
    }
  }
  return oldest;
}

function stockRecordChartCacheCoversRange(range) {
  const cache = state.stockRecordChartPointsCache;
  if (!Array.isArray(cache) || !cache.length) {
    return false;
  }
  const preset = String(range || "30").trim().toLowerCase();
  if (preset === "all") {
    return !!state.stockRecordChartHistoryComplete;
  }
  const from = stockRecordRangeFromDateKey(preset, stockRecordChartEndDateKey());
  if (!from) {
    return false;
  }
  const oldest = stockRecordChartCacheOldestDate();
  return !!oldest && oldest <= from;
}

function filterStockRecordChartCacheForDisplay() {
  const cache = state.stockRecordChartPointsCache || [];
  if (!cache.length) {
    return [];
  }
  if (state.stockRecordChartViewExpanded) {
    return [...cache];
  }
  const preset = String(state.stockRecordChartRange || "").trim().toLowerCase();
  if (!preset) {
    return [...cache];
  }
  if (preset === "all" && state.stockRecordChartHistoryComplete) {
    return [...cache];
  }
  const from = stockRecordRangeFromDateKey(preset, stockRecordChartEndDateKey());
  if (!from) {
    return [...cache];
  }
  return cache.filter((row) => String(row?.date || "").slice(0, 10) >= from);
}

function mergeStockRecordChartCache(incomingRows) {
  const existing = state.stockRecordChartPointsCache || [];
  const { merged } = mergeStockRecordChartPoints(existing, incomingRows);
  state.stockRecordChartPointsCache = merged;
  return merged;
}

function applyStockRecordChartDisplayFromCache() {
  if (!state.stockRecordBundle?.charts) {
    return;
  }
  const displayPoints = filterStockRecordChartCacheForDisplay();
  state.stockRecordBundle.charts.points = displayPoints;
  applyStockRecordChartFitAll(displayPoints.length);
  syncStockRecordRangeChipUi();
  drawStockRecordCharts(
    state.activeRecordSymbol,
    stockRecordTradesForActiveSymbol(state.activeRecordSymbol),
  );
}

function ingestStockRecordChartPoints(bundle, rangePreset) {
  const incoming = Array.isArray(bundle?.charts?.points) ? bundle.charts.points : [];
  mergeStockRecordChartCache(incoming);
  const preset = String(rangePreset || state.stockRecordChartRange || "30").trim().toLowerCase();
  if (preset === "all" && !bundle?.charts?.pagination?.hasMore) {
    state.stockRecordChartHistoryComplete = true;
  }
  state.stockRecordChartViewExpanded = false;
  if (state.stockRecordBundle?.charts) {
    state.stockRecordBundle.charts.points = filterStockRecordChartCacheForDisplay();
  }
}

function applyStockRecordChartPaginationFromBundle(bundle) {
  const pag = bundle?.charts?.pagination;
  const limit = Number(pag?.limit) || STOCK_RECORD_CHART_PAGE_SIZE;
  const offset = Number(pag?.offset) || 0;
  const returned = Number(pag?.returned) || (Array.isArray(bundle?.charts?.points) ? bundle.charts.points.length : 0);
  state.stockRecordChartPagination = {
    limit,
    nextOffset: offset + returned,
    hasMore: !!pag?.hasMore,
  };
}

function applyStockRecordChartFitAll(pointCount) {
  const n = Math.max(0, Number(pointCount) || 0);
  if (n <= 0) {
    return;
  }
  state.stockRecordWindow = Math.max(n, STOCK_RECORD_CHART_MIN_WINDOW);
  state.stockRecordOffset = 0;
}

function syncStockRecordRangeChipUi() {
  if (!stockRecordRangeRow) {
    return;
  }
  const active = String(state.stockRecordChartRange || "").trim();
  stockRecordRangeRow.querySelectorAll("[data-stock-record-range]").forEach((btn) => {
    btn.classList.toggle("active", !!active && String(btn.getAttribute("data-stock-record-range")) === active);
  });
}

function clearStockRecordRangeSelection() {
  if (!state.stockRecordChartRange) {
    return;
  }
  state.stockRecordChartRange = "";
  syncStockRecordRangeChipUi();
}

function stockRecordViewMatchesRangePreset() {
  const range = String(state.stockRecordChartRange || "").trim();
  if (!range || state.stockRecordChartViewExpanded) {
    return false;
  }
  const points = stockRecordChartPointsFromBundle(state.stockRecordBundle);
  const totalCount = points.length;
  if (!totalCount) {
    return false;
  }
  const offset = Number(state.stockRecordOffset) || 0;
  const fitAllWindow = Math.max(totalCount, STOCK_RECORD_CHART_MIN_WINDOW);
  const windowSize = Math.max(
    STOCK_RECORD_CHART_MIN_WINDOW,
    Math.min(totalCount, Number(state.stockRecordWindow || ANALYSIS_CHART_DEFAULT_WINDOW)),
  );
  if (offset !== 0 || windowSize < fitAllWindow) {
    return false;
  }
  if (range === "all") {
    return !!state.stockRecordChartHistoryComplete;
  }
  const expectedFrom = stockRecordRangeFromDateKey(range, stockRecordChartEndDateKey());
  if (!expectedFrom) {
    return true;
  }
  const oldestVisible = String(points[0]?.date || "").slice(0, 10);
  return oldestVisible >= expectedFrom;
}

function syncStockRecordRangeChipWithView() {
  if (!state.stockRecordChartRange) {
    return;
  }
  if (!stockRecordViewMatchesRangePreset()) {
    clearStockRecordRangeSelection();
  }
}

async function refreshStockRecordChartsOnly(symKey) {
  const key = normalizeSymbol(symKey);
  if (
    !key ||
    state.route !== "stock-record" ||
    normalizeSymbol(state.activeRecordSymbol) !== key ||
    state.stockRecordPageLoading
  ) {
    return;
  }
  const range = state.stockRecordChartRange || "30";
  resetStockRecordChartPagination();
  state.stockRecordChartViewExpanded = false;
  if (stockRecordChartCacheCoversRange(range)) {
    applyStockRecordChartDisplayFromCache();
    return;
  }
  const accountId = state.stockRecordFromPublicProfile ? "all" : resolveValidAccountFilter(state.stockRecordAccountId);
  const publicTargetId = stockRecordPublicTargetId();
  setStockRecordChartPointsLoading(true);
  const loadGen = stockRecordPageLoadGen;
  try {
    const partial = await fetchStockRecordBundleMetrics(key, accountId, publicTargetId, { range });
    if (
      loadGen !== stockRecordPageLoadGen ||
      state.route !== "stock-record" ||
      normalizeSymbol(state.activeRecordSymbol) !== key
    ) {
      return;
    }
    if (!partial?.charts || !state.stockRecordBundle) {
      return;
    }
    state.stockRecordBundle.charts = partial.charts;
    ingestStockRecordChartPoints(partial, range);
    applyStockRecordChartPaginationFromBundle(partial);
    applyStockRecordChartFitAll(stockRecordChartPointsFromBundle(state.stockRecordBundle).length);
    syncStockRecordRangeChipUi();
    drawStockRecordCharts(key, stockRecordTradesForActiveSymbol(key));
  } catch (error) {
    console.warn("refreshStockRecordChartsOnly failed", error);
  } finally {
    if (loadGen === stockRecordPageLoadGen) {
      setStockRecordChartPointsLoading(false);
    }
  }
}

function mergeStockRecordChartPoints(existingRows, incomingRows) {
  const byDate = new Map();
  for (const row of existingRows || []) {
    const dk = String(row.date || "").slice(0, 10);
    if (dk) {
      byDate.set(dk, row);
    }
  }
  const beforeCount = byDate.size;
  for (const row of incomingRows || []) {
    const dk = String(row.date || "").slice(0, 10);
    if (dk && !byDate.has(dk)) {
      byDate.set(dk, row);
    }
  }
  const merged = [...byDate.values()].sort((a, b) => String(a.date).localeCompare(String(b.date)));
  return { merged, added: merged.length - beforeCount };
}

function stockRecordTradesForActiveSymbol(symKey) {
  const key = normalizeSymbol(symKey);
  const usePub = useCommunityPublicStockRecord();
  const detail = state.lastPublicProfileDetail;
  const activeAccountId = usePub ? "all" : resolveValidAccountFilter(state.stockRecordAccountId);
  return stockRecordTradesForScope(activeAccountId, usePub, detail).filter(
    (t) => normalizeSymbol(t.symbol) === key,
  );
}

function scheduleStockRecordChartHistoryLoad(symKey) {
  if (stockRecordChartLoadTimer) {
    window.clearTimeout(stockRecordChartLoadTimer);
  }
  stockRecordChartLoadTimer = window.setTimeout(() => {
    stockRecordChartLoadTimer = 0;
    void loadStockRecordChartHistoryPage(symKey);
  }, 200);
}

async function loadStockRecordChartHistoryPage(symKey) {
  const key = normalizeSymbol(symKey);
  const pag = state.stockRecordChartPagination;
  if (
    !key ||
    !pag?.hasMore ||
    state.stockRecordPointsLoading ||
    state.route !== "stock-record" ||
    normalizeSymbol(state.activeRecordSymbol) !== key
  ) {
    return;
  }
  if (!state.stockRecordBundle?.charts) {
    return;
  }
  const accountId = state.stockRecordFromPublicProfile ? "all" : resolveValidAccountFilter(state.stockRecordAccountId);
  const publicTargetId = stockRecordPublicTargetId();
  setStockRecordChartPointsLoading(true);
  const loadGen = stockRecordPageLoadGen;
  try {
    const chunk = await fetchStockRecordBundleMetrics(key, accountId, publicTargetId, {
      limit: pag.limit,
      offset: pag.nextOffset,
    });
    if (
      loadGen !== stockRecordPageLoadGen ||
      state.route !== "stock-record" ||
      normalizeSymbol(state.activeRecordSymbol) !== key
    ) {
      return;
    }
    const chunkPag = chunk?.charts?.pagination;
    if (!chunk?.charts?.points?.length) {
      state.stockRecordChartPagination = { ...pag, hasMore: false };
      if (!chunkPag?.hasMore) {
        state.stockRecordChartHistoryComplete = true;
      }
      return;
    }
    const existing = filterStockRecordChartCacheForDisplay();
    const loadedBefore = existing.length;
    const wasFitAll =
      (Number(state.stockRecordOffset) || 0) === 0 &&
      Number(state.stockRecordWindow || 0) >= loadedBefore &&
      loadedBefore > 0;
    const wantedWindow = Number(state.stockRecordWindow || ANALYSIS_CHART_DEFAULT_WINDOW);
    const wasExpanding = wantedWindow > loadedBefore;
    mergeStockRecordChartCache(chunk.charts.points);
    if (!chunkPag?.hasMore) {
      state.stockRecordChartHistoryComplete = true;
    }
    state.stockRecordChartViewExpanded = true;
    clearStockRecordRangeSelection();
    const merged = filterStockRecordChartCacheForDisplay();
    state.stockRecordBundle.charts.points = merged;
    const limit = Number(chunkPag?.limit) || pag.limit;
    const offset = Number(chunkPag?.offset) || pag.nextOffset;
    const added = Math.max(0, merged.length - loadedBefore);
    state.stockRecordChartPagination = {
      limit,
      nextOffset: offset + (Number(chunkPag?.returned) || chunk.charts.points.length),
      hasMore: !!chunkPag?.hasMore,
    };
    if (wasFitAll) {
      applyStockRecordChartFitAll(merged.length);
    } else if (wasExpanding) {
      state.stockRecordOffset = 0;
      state.stockRecordWindow = Math.max(
        Math.min(wantedWindow, merged.length),
        STOCK_RECORD_CHART_MIN_WINDOW,
      );
    } else if (added > 0) {
      state.stockRecordOffset = (Number(state.stockRecordOffset) || 0) + added;
    }
    drawStockRecordCharts(key, stockRecordTradesForActiveSymbol(key));
  } catch (error) {
    console.warn("loadStockRecordChartHistoryPage failed", error);
  } finally {
    if (loadGen === stockRecordPageLoadGen) {
      setStockRecordChartPointsLoading(false);
    }
  }
}

function maybeLoadStockRecordChartHistory(symKey) {
  const pag = state.stockRecordChartPagination;
  if (!pag?.hasMore || state.stockRecordPointsLoading) {
    return;
  }
  const points = stockRecordChartPointsFromBundle(state.stockRecordBundle);
  const loadedCount = points.length;
  if (!loadedCount) {
    return;
  }
  const windowSize = Math.max(
    STOCK_RECORD_CHART_MIN_WINDOW,
    Math.min(loadedCount, Number(state.stockRecordWindow || ANALYSIS_CHART_DEFAULT_WINDOW)),
  );
  const offset = Number(state.stockRecordOffset) || 0;
  const maxOffset = Math.max(0, loadedCount - windowSize);
  const zoomNeedsMore = Number(state.stockRecordWindow || ANALYSIS_CHART_DEFAULT_WINDOW) > loadedCount;
  const atOldestLoaded = offset >= maxOffset;
  if (zoomNeedsMore || atOldestLoaded) {
    scheduleStockRecordChartHistoryLoad(symKey);
  }
}

function stockRecordChartPointsFromBundle(bundle) {
  const list = Array.isArray(bundle?.charts?.points) ? bundle.charts.points : [];
  return list
    .map((row) => ({
      date: String(row.date || "").slice(0, 10),
      close: parseBundlePlainNumber(row.close),
      shares: parseBundlePlainNumber(row.shares),
      marketValueNative: parseBundlePlainNumber(row.marketValueNative),
      totalProfit: parseBundlePlainNumber(row.totalProfit),
      weight: parseBundlePercent(row.weight),
    }))
    .filter((row) => row.date);
}

function applyStockRecordBundleDefaults(bundle) {
  const defaults = bundle?.charts?.defaults || {};
  if (defaults.showClose != null) {
    state.stockRecordShowClose = defaults.showClose !== false;
  }
  if (defaults.showShares != null) {
    state.stockRecordShowShares = defaults.showShares !== false;
  }
  if (defaults.showMarketValue != null) {
    state.stockRecordShowMarketValue = defaults.showMarketValue === true;
  }
  if (stockRecordToggleClose) {
    stockRecordToggleClose.checked = state.stockRecordShowClose;
  }
  if (stockRecordToggleShares) {
    stockRecordToggleShares.checked = state.stockRecordShowShares;
  }
  if (stockRecordToggleMarketValue) {
    stockRecordToggleMarketValue.checked = state.stockRecordShowMarketValue;
  }
}

async function refreshStockRecordPageData(symKey, accountId = "all") {
  const key = normalizeSymbol(symKey);
  if (!key || state.route !== "stock-record" || normalizeSymbol(state.activeRecordSymbol) !== key) {
    return;
  }
  const pageGen = ++stockRecordPageLoadGen;
  const isPublic = state.stockRecordFromPublicProfile;
  const publicTargetId = isPublic ? stockRecordPublicTargetId() : "";
  const bundleAccountId = isPublic ? "all" : accountId;
  setStockRecordPageLoading(true);
  state.stockRecordBundle = null;
  resetStockRecordChartCache();
  resetStockRecordChartPagination();
  state.stockRecordChartRange = "30";
  try {
    const bundle =
      !isPublic || publicTargetId
        ? await fetchStockRecordBundleMetrics(key, bundleAccountId, publicTargetId, {
            range: state.stockRecordChartRange,
          })
        : null;
    if (pageGen !== stockRecordPageLoadGen) {
      return;
    }
    state.stockRecordBundle = bundle;
    if (bundle) {
      applyStockRecordBundleDefaults(bundle);
      ingestStockRecordChartPoints(bundle, state.stockRecordChartRange);
      applyStockRecordChartPaginationFromBundle(bundle);
      applyStockRecordChartFitAll(stockRecordChartPointsFromBundle(bundle).length);
    }
    if (!state.stockRecordBundle) {
      await ensureSymbolData(key);
    }
    if (pageGen !== stockRecordPageLoadGen) {
      return;
    }
    if (state.route !== "stock-record" || normalizeSymbol(state.activeRecordSymbol) !== key) {
      return;
    }
    setStockRecordPageLoading(false);
    await renderStockRecordPage(key);
    await loadStockRecordTradesPage({ reset: true });
    if (pageGen !== stockRecordPageLoadGen) {
      return;
    }
    window.setTimeout(() => {
      if (
        pageGen === stockRecordPageLoadGen &&
        state.route === "stock-record" &&
        normalizeSymbol(state.activeRecordSymbol) === key
      ) {
        void renderStockRecordPage(key);
      }
    }, 40);
  } catch (error) {
    console.warn("refreshStockRecordPageData failed", error);
    if (pageGen === stockRecordPageLoadGen) {
      setStockRecordPageLoading(false);
      if (state.route === "stock-record" && normalizeSymbol(state.activeRecordSymbol) === key) {
        await renderStockRecordPage(key);
      }
    }
  }
}

function stockRecordTradesForScope(activeAccountId, usePub, detail) {
  const list = Array.isArray(state.stockRecordTrades) ? state.stockRecordTrades : [];
  if (activeAccountId === "all") {
    return list;
  }
  return list.filter((t) => String(t.accountId || DEFAULT_ACCOUNT.id) === activeAccountId);
}

function ledgerListQuery(accountId, offset) {
  const params = new URLSearchParams();
  params.set("limit", String(TRADE_LIST_PAGE_SIZE));
  params.set("offset", String(offset));
  const aid = resolveValidAccountFilter(accountId);
  if (aid && aid !== "all") {
    params.set("accountId", aid);
  }
  return params.toString();
}

function tradeListLoadingRowHtml(colspan) {
  return `
    <tr class="trade-list-loading-row" aria-busy="true">
      <td colspan="${colspan}">
        <div class="trade-list-loading">
          <span class="app-boot-spinner trade-list-spinner" aria-hidden="true"></span>
          <span>加载中…</span>
        </div>
      </td>
    </tr>
  `;
}

function isNearDocumentBottom(thresholdPx = 140) {
  const doc = document.documentElement;
  return window.innerHeight + window.scrollY >= doc.scrollHeight - thresholdPx;
}

function getStockRecordScrollRoot() {
  return document.getElementById("route-stock-record");
}

function isNearScrollContainerBottom(el, thresholdPx = 140) {
  if (!el) {
    return false;
  }
  return el.scrollTop + el.clientHeight >= el.scrollHeight - thresholdPx;
}

function ensureTradeListScrollListener() {
  if (tradeListScrollListenerBound) {
    return;
  }
  tradeListScrollListenerBound = true;
  window.addEventListener(
    "scroll",
    () => {
      if (state.route === "trade-records") {
        void maybeLoadMoreTradeListPage();
      } else if (state.route === "trade-cash") {
        void maybeLoadMoreCashListPage();
      } else if (state.route === "stock-record") {
        void maybeLoadMoreStockRecordTradesPage();
      } else if (state.route === "community-profile" && state.communityProfileTab === "trade") {
        void maybeLoadMoreCommunityPublicTradesPage();
      }
    },
    { passive: true },
  );
}

function ensureStockRecordScrollListener() {
  if (stockRecordScrollListenerBound) {
    return;
  }
  const root = getStockRecordScrollRoot();
  if (!root) {
    return;
  }
  stockRecordScrollListenerBound = true;
  root.addEventListener(
    "scroll",
    () => {
      if (state.route === "stock-record") {
        void maybeLoadMoreStockRecordTradesPage();
      }
    },
    { passive: true },
  );
}

async function maybeLoadMoreStockRecordTradesPage() {
  if (
    state.route !== "stock-record" ||
    state.stockRecordPageLoading ||
    !stockRecordTradesPager.hasMore ||
    stockRecordTradesPager.loading
  ) {
    return;
  }
  const scrollRoot = getStockRecordScrollRoot();
  if (!isNearScrollContainerBottom(scrollRoot)) {
    return;
  }
  await loadStockRecordTradesPage();
}

async function fetchTradeCountForAccount(accountId) {
  if (!apiReady) {
    return state.trades.filter((t) => String(t.accountId || DEFAULT_ACCOUNT.id) === accountId).length;
  }
  try {
    const qs = ledgerListQuery(accountId, 0);
    const res = await apiFetch(`${API_BASE}/trades?${qs}`, { cache: "no-store" });
    const body = await res.json().catch(() => ({}));
    if (res.ok && body?.ok && body.pagination) {
      return Number(body.pagination.total) || 0;
    }
  } catch {
    // ignore
  }
  return 0;
}

async function loadTradeListPage({ reset = false } = {}) {
  if (!apiReady || !sessionPhone) {
    return;
  }
  if (state.route !== "trade-records") {
    return;
  }
  ensureTradeListScrollListener();
  const accountId = resolveValidAccountFilter(state.tradeFilterAccountId);
  if (reset || tradeListPager.accountId !== accountId) {
    resetTradeListPager();
    tradeListPager.accountId = accountId;
  }
  if (tradeListPager.loading || (!tradeListPager.hasMore && tradeListPager.loaded)) {
    renderTradeTable();
    return;
  }
  const gen = tradeListPager.gen;
  tradeListPager.loading = true;
  renderTradeTable();
  try {
    const qs = ledgerListQuery(accountId, tradeListPager.offset);
    const res = await apiFetch(`${API_BASE}/trades?${qs}`, { cache: "no-store", timeoutMs: 25_000 });
    const body = await res.json().catch(() => ({}));
    if (gen !== tradeListPager.gen || state.route !== "trade-records") {
      return;
    }
    if (!res.ok || body?.ok !== true || !Array.isArray(body.data)) {
      tradeListPager.hasMore = false;
      return;
    }
    const rows = body.data.map(normalizeTrade);
    const seen = new Set(state.trades.map((t) => String(t.id)));
    for (const row of rows) {
      const id = String(row.id);
      if (!seen.has(id)) {
        state.trades.push(row);
        seen.add(id);
      }
    }
    const pagination = body.pagination || {};
    tradeListPager.offset = Number(pagination.offset ?? tradeListPager.offset) + rows.length;
    tradeListPager.hasMore = pagination.hasMore === true;
    tradeListPager.loaded = true;
  } catch (error) {
    console.warn("loadTradeListPage failed", error);
    if (gen === tradeListPager.gen) {
      tradeListPager.hasMore = false;
    }
  } finally {
    if (gen === tradeListPager.gen) {
      tradeListPager.loading = false;
      if (state.route === "trade-records") {
        renderTradeTable();
        if (tradeListPager.hasMore && isNearDocumentBottom()) {
          void loadTradeListPage();
        }
      }
    }
  }
}

async function loadCashListPage({ reset = false } = {}) {
  if (!apiReady || !sessionPhone) {
    return;
  }
  if (state.route !== "trade-cash") {
    return;
  }
  ensureTradeListScrollListener();
  const accountId = resolveValidAccountFilter(state.tradeFilterAccountId);
  if (reset || cashListPager.accountId !== accountId) {
    resetCashListPager();
    cashListPager.accountId = accountId;
  }
  if (cashListPager.loading || (!cashListPager.hasMore && cashListPager.loaded)) {
    renderTradeTable();
    return;
  }
  const gen = cashListPager.gen;
  cashListPager.loading = true;
  renderTradeTable();
  try {
    const qs = ledgerListQuery(accountId, cashListPager.offset);
    const res = await apiFetch(`${API_BASE}/cash-transfers?${qs}`, { cache: "no-store", timeoutMs: 25_000 });
    const body = await res.json().catch(() => ({}));
    if (gen !== cashListPager.gen || state.route !== "trade-cash") {
      return;
    }
    if (!res.ok || body?.ok !== true || !Array.isArray(body.data)) {
      cashListPager.hasMore = false;
      return;
    }
    const rows = body.data.map(normalizeCashTransferRow);
    const seen = new Set(state.cashTransfers.map((r) => String(r.id)));
    for (const row of rows) {
      const id = String(row.id);
      if (!seen.has(id)) {
        state.cashTransfers.push(row);
        seen.add(id);
      }
    }
    const pagination = body.pagination || {};
    cashListPager.offset = Number(pagination.offset ?? cashListPager.offset) + rows.length;
    cashListPager.hasMore = pagination.hasMore === true;
    cashListPager.loaded = true;
  } catch (error) {
    console.warn("loadCashListPage failed", error);
    if (gen === cashListPager.gen) {
      cashListPager.hasMore = false;
    }
  } finally {
    if (gen === cashListPager.gen) {
      cashListPager.loading = false;
      if (state.route === "trade-cash") {
        renderTradeTable();
        if (cashListPager.hasMore && isNearDocumentBottom()) {
          void loadCashListPage();
        }
      }
    }
  }
}

async function maybeLoadMoreTradeListPage() {
  if (state.route !== "trade-records" || tradeListPager.loading || !tradeListPager.hasMore) {
    return;
  }
  if (!isNearDocumentBottom()) {
    return;
  }
  await loadTradeListPage();
}

async function maybeLoadMoreCashListPage() {
  if (state.route !== "trade-cash" || cashListPager.loading || !cashListPager.hasMore) {
    return;
  }
  if (!isNearDocumentBottom()) {
    return;
  }
  await loadCashListPage();
}

async function ensureTradeListRouteReady() {
  if (state.route === "trade-records") {
    await loadTradeListPage({ reset: !tradeListPager.loaded });
    return;
  }
  if (state.route === "trade-cash") {
    await loadCashListPage({ reset: !cashListPager.loaded });
  }
}

async function pushSettingsToApi(payload) {
  if (!apiReady) {
    return;
  }
  try {
    const body = pickSettingsPayload(payload);
    if (!Object.keys(body).length) {
      return;
    }
    await apiFetch(`${API_BASE}/settings`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (error) {
    // ignore sync failure; next debounce flush will retry with latest state
  }
}

async function importCashTransfersToApi(rows, mode = "replace") {
  if (!apiReady || !Array.isArray(rows) || !rows.length) {
    return;
  }
  try {
    const response = await apiFetch(`${getApiBaseForFetch()}/cash-transfers/import`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        mode: mode === "replace" ? "replace" : "append",
        cashTransfers: rows.map(normalizeCashTransferRow),
      }),
    });
    if (!response.ok) {
      return;
    }
    const result = await response.json();
    if (result?.ok && Array.isArray(result.data)) {
      state.cashTransfers = result.data.map(normalizeCashTransferRow);
    }
  } catch (error) {
    console.error("同步资金记录失败", error);
  }
}

async function saveCashTransferToApi(row) {
  if (!apiReady) {
    return row;
  }
  const response = await apiFetch(`${getApiBaseForFetch()}/cash-transfers`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(normalizeCashTransferRow(row)),
  });
  if (!response.ok) {
    throw new Error("保存资金记录失败");
  }
  const result = await response.json();
  return result?.data ? normalizeCashTransferRow(result.data) : normalizeCashTransferRow(row);
}

async function deleteCashTransferFromApi(id) {
  if (!apiReady) {
    return true;
  }
  const response = await apiFetch(
    `${getApiBaseForFetch()}/cash-transfers/${encodeURIComponent(String(id || ""))}`,
    { method: "DELETE" },
  );
  return response.ok;
}


async function saveTradeToApi(trade) {
  if (!apiReady) {
    return trade;
  }
  const response = await apiFetch(`${API_BASE}/trades`, {
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
  const response = await apiFetch(`${API_BASE}/trades/import`, {
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
  const response = await apiFetch(`${API_BASE}/trades/${encodeURIComponent(String(tradeId || ""))}`, {
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

  appMenuBtn?.addEventListener("click", () => {
    if (appDrawer?.classList.contains("is-open")) {
      closeAppDrawer();
    } else {
      openAppDrawer();
    }
  });
  appDrawerBackdrop?.addEventListener("click", () => closeAppDrawer());
  document.querySelectorAll("[data-drawer-action]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const a = btn.getAttribute("data-drawer-action");
      if (a === "holdings") {
        state.appModule = "holdings";
        state.route = "earning";
      } else if (a === "community") {
        state.appModule = "community";
        state.route = "community-feed";
      } else if (a === "mine") {
        state.route = "mine";
      }
      closeAppDrawer();
      persistState();
      renderAll();
    });
  });

  appShell?.addEventListener("click", (e) => {
    const sortBtn = e.target.closest(".stock-table .th-sort-btn");
    if (
      sortBtn &&
      appShell.contains(sortBtn) &&
      state.route === "community-profile" &&
      state.communityProfileTab === "earning"
    ) {
      const key = sortBtn.dataset.sortKey || "default";
      if (state.stockSortKey !== key) {
        state.stockSortKey = key;
        state.stockSortOrder = "desc";
      } else {
        state.stockSortOrder = cycleSortOrder(state.stockSortOrder);
        if (state.stockSortOrder === "default") {
          state.stockSortKey = "default";
        }
      }
      repaintPublicEarningHoldingsFromCache();
      syncCommunityEarningSortHeaderUi();
      return;
    }
    const profileTabHit = e.target.closest(".bottom-tabs--profile .bottom-tab-btn");
    if (profileTabHit && appShell.contains(profileTabHit) && state.route === "community-profile") {
      const sub = profileTabHit.getAttribute("data-profile-subtab");
      if (sub) {
        state.communityProfileTab = sub;
        document.querySelectorAll(".bottom-tabs--profile .bottom-tab-btn").forEach((b) => {
          b.classList.toggle("active", b.getAttribute("data-profile-subtab") === sub);
        });
        document.querySelectorAll("[data-profile-panel]").forEach((p) => {
          p.classList.toggle("is-active", p.getAttribute("data-profile-panel") === sub);
        });
        if (sub === "analysis" && state.lastPublicProfileDetail) {
          unmountCommunityTradeRecordsPane();
          void openCommunityProfileAnalysisTab();
        } else {
          unmountCommunityAnalysisRoutePane();
        }
        if (sub === "earning" && state.communityProfileUserId) {
          unmountCommunityTradeRecordsPane();
          void loadPublicEarningTabData(state.communityProfileUserId);
        }
        if (sub === "trade" && state.communityProfileUserId) {
          unmountCommunityAnalysisRoutePane();
          void loadCommunityPublicTrades(state.communityProfileUserId);
        }
      }
      e.preventDefault();
      return;
    }
    const tab = e.target.closest(".bottom-tabs .bottom-tab-btn");
    if (tab && appShell.contains(tab)) {
      const r = tab.dataset.route;
      const mod = tab.dataset.module;
      if (!r) {
        return;
      }
      if (state.route !== "stock-record") {
        state.previousRoute = state.route;
      }
      if (mod === "community") {
        state.appModule = "community";
      } else if (mod === "holdings") {
        state.appModule = "holdings";
      }
      state.route = r;
      persistState();
      renderAll();
      return;
    }
    const fb = e.target.closest(".community-follow-btn");
    if (fb && appShell.contains(fb) && sessionUserId) {
      const uid = fb.getAttribute("data-user-id");
      void toggleFollowCommunity(uid, fb);
      return;
    }
    const feedStockAnalysis = e.target.closest("[data-community-feed-stock-analysis]");
    if (feedStockAnalysis && appShell.contains(feedStockAnalysis)) {
      e.preventDefault();
      const uid = feedStockAnalysis.getAttribute("data-community-user");
      const sym = feedStockAnalysis.getAttribute("data-community-symbol");
      if (uid && sym) {
        void openStockRecordDialog(sym, { fromPublicProfile: true, publicOwnerUserId: uid });
      }
      return;
    }
    const feedPortfolioAnalysis = e.target.closest("[data-community-feed-portfolio-analysis]");
    if (feedPortfolioAnalysis && appShell.contains(feedPortfolioAnalysis)) {
      e.preventDefault();
      const uid = feedPortfolioAnalysis.getAttribute("data-community-user");
      if (uid) {
        openCommunityProfile(uid);
      }
      return;
    }
    const profileCard = e.target.closest("[data-community-profile-card]");
    if (profileCard && appShell.contains(profileCard)) {
      if (e.target.closest(".stock-rank-help-wrap")) {
        return;
      }
      const uid = profileCard.getAttribute("data-community-user");
      if (uid) {
        openCommunityProfile(uid);
      }
    }
  });

  communityProfileBackBtn?.addEventListener("click", () => {
    state.route = state.communityProfileReturnRoute || "community-feed";
    state.communityProfileUserId = null;
    persistState();
    renderAll();
  });

  mineCommunitySaveBtn?.addEventListener("click", () => void saveMineCommunityProfile());

  tradeHubCommunityPublicToggle?.addEventListener("change", () => void quickSaveCommunityPublicFromHome());

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
      void refreshMarketData();
    });
  }

  algoModeSelectMine?.addEventListener("change", () => {
    state.algoMode = normalizeProfitAlgoMode(algoModeSelectMine.value);
    persistState();
    invalidateOverviewMetricsUi();
    renderOverviewAndStockTable();
    void renderAnalysis();
    renderMineSection();
  });

  document.querySelectorAll("[data-mine-open]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const target = btn.getAttribute("data-mine-open");
      if (target === "community") {
        state.route = "mine-community";
        persistState();
        renderAll();
      }
    });
  });
  document.querySelectorAll("[data-mine-back]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.route = state.mineReturnRoute || "mine";
      state.mineReturnRoute = null;
      persistState();
      renderAll();
    });
  });
  document.querySelectorAll("[data-mine-back-community]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.route = "mine";
      persistState();
      renderAll();
    });
  });

  benchmarkSelect.addEventListener("change", () => {
    state.benchmark = benchmarkSelect.value;
    persistState();
    if (analysisMetricsUiActive()) {
      state.publicAnalysisBundleUi.ready = false;
      void refreshAnalysisMetricsView({ showLoading: false });
    } else {
      void renderAnalysis();
    }
    void refreshMarketData();
  });

  stageRangeSelect?.addEventListener("change", () => {
    state.stageRange = stageRangeSelect.value;
    persistState();
    renderOverviewAndStockTable();
  });

  accountFilterSelect?.addEventListener("change", () => {
    state.selectedAccountId = resolveValidAccountFilter(accountFilterSelect.value);
    persistState();
    invalidateOverviewMetricsUi();
    renderAll();
    if (state.route !== "earning") {
      void refreshMarketData();
    }
  });
  analysisAccountSelect?.addEventListener("change", () => {
    state.selectedAccountId = resolveValidAccountFilter(analysisAccountSelect.value);
    persistState();
    renderAll();
    void refreshMarketData();
  });
  const onTradeFilterAccountChange = (value) => {
    state.tradeFilterAccountId = resolveValidAccountFilter(value);
    persistState();
    renderControls();
    if (state.route === "trade-records") {
      void loadTradeListPage({ reset: true });
    } else if (state.route === "trade-cash") {
      void loadCashListPage({ reset: true });
    } else {
      renderTradeTable();
    }
  };
  tradeAccountFilterSelect?.addEventListener("change", () => {
    onTradeFilterAccountChange(tradeAccountFilterSelect.value);
  });
  tradeCashAccountFilterSelect?.addEventListener("change", () => {
    onTradeFilterAccountChange(tradeCashAccountFilterSelect.value);
  });
  stockRecordAccountSelect?.addEventListener("change", () => {
    state.stockRecordAccountId = resolveValidAccountFilter(stockRecordAccountSelect.value);
    persistState();
    resetStockRecordTradesPager();
    if (state.route === "stock-record" && state.activeRecordSymbol) {
      void refreshStockRecordPageData(state.activeRecordSymbol, state.stockRecordAccountId);
    }
  });
  const stockRecordToggleHandler = () => {
    state.stockRecordShowClose = stockRecordToggleClose?.checked !== false;
    state.stockRecordShowShares = stockRecordToggleShares?.checked !== false;
    state.stockRecordShowMarketValue = stockRecordToggleMarketValue?.checked === true;
    if (state.route === "stock-record" && state.activeRecordSymbol) {
      const sym = normalizeSymbol(state.activeRecordSymbol);
      const detail = state.lastPublicProfileDetail;
      const usePub = useCommunityPublicStockRecord();
      const activeAccountId = usePub ? "all" : resolveValidAccountFilter(state.stockRecordAccountId);
      const scopeTrades = stockRecordTradesForScope(activeAccountId, usePub, detail).filter(
        (item) => normalizeSymbol(item.symbol) === sym,
      );
      drawStockRecordCharts(sym, scopeTrades);
    }
  };
  stockRecordToggleClose?.addEventListener("change", stockRecordToggleHandler);
  stockRecordToggleShares?.addEventListener("change", stockRecordToggleHandler);
  stockRecordToggleMarketValue?.addEventListener("change", stockRecordToggleHandler);
  stockRecordRangeRow?.addEventListener("click", (event) => {
    const btn = event.target.closest("[data-stock-record-range]");
    if (!btn || state.route !== "stock-record" || !state.activeRecordSymbol || state.stockRecordPointsLoading) {
      return;
    }
    const nextRange = String(btn.getAttribute("data-stock-record-range") || "").trim();
    if (!nextRange || nextRange === state.stockRecordChartRange) {
      return;
    }
    state.stockRecordChartRange = nextRange;
    syncStockRecordRangeChipUi();
    void refreshStockRecordChartsOnly(state.activeRecordSymbol);
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
        state.analysisPreset = null;
        state.customRangeDraftStart = state.customRangeStart;
        state.customRangeDraftEnd = state.customRangeEnd;
      } else if (value === "all") {
        state.analysisRangeMode = "all";
        state.analysisPreset = null;
        resetAnalysisChartViewport();
      } else if (value === "mtd") {
        state.analysisRangeMode = "preset";
        state.analysisPreset = "mtd";
        resetAnalysisChartViewport();
      } else {
        state.analysisRangeMode = "preset";
        const n = Number(value);
        if (n === 365) {
          state.analysisPreset = "ytd";
          state.rangeDays = 365;
        } else {
          state.analysisPreset = null;
          state.rangeDays = n;
        }
        resetAnalysisChartViewport();
      }
      persistState();
      if (analysisMetricsUiActive()) {
        state.publicAnalysisBundleUi.ready = false;
        void refreshAnalysisMetricsView({ showLoading: false });
      } else {
        void renderAnalysis();
      }
      renderControls();
    });
  });

  const syncCustomRangeDraftFromInputs = () => {
    if (customRangeStartInput) {
      state.customRangeDraftStart = customRangeStartInput.value || "";
    }
    if (customRangeEndInput) {
      state.customRangeDraftEnd = customRangeEndInput.value || "";
    }
  };
  customRangeStartInput?.addEventListener("input", syncCustomRangeDraftFromInputs);
  customRangeStartInput?.addEventListener("change", syncCustomRangeDraftFromInputs);
  customRangeEndInput?.addEventListener("input", syncCustomRangeDraftFromInputs);
  customRangeEndInput?.addEventListener("change", syncCustomRangeDraftFromInputs);

  applyCustomRangeBtn?.addEventListener("click", () => {
    syncCustomRangeDraftFromInputs();
    let start = state.customRangeDraftStart || "";
    let end = state.customRangeDraftEnd || "";
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
    state.customRangeDraftStart = start;
    state.customRangeDraftEnd = end;
    state.analysisRangeMode = "custom";
    state.analysisPreset = null;
    resetAnalysisChartViewport();
    persistState();
    renderControls();
    if (analysisMetricsUiActive()) {
      state.publicAnalysisBundleUi.ready = false;
      void refreshAnalysisMetricsView({ showLoading: false });
    } else {
      void renderAnalysis();
    }
  });

  assetCurveModeSelect?.addEventListener("change", () => {
    state.capitalTrendMode = assetCurveModeSelect.value || "total_assets";
    persistState();
    if (cachedAnalysisAssetChartRows?.length && analysisMetricsUiActive()) {
      repaintAnalysisAssetChartFromCache();
      return;
    }
    if (analysisMetricsUiActive()) {
      void refreshAnalysisMetricsView({ showLoading: false });
    } else {
      void renderAnalysis();
    }
  });

  [quickTradeBtn, recordTradeBtn].filter(Boolean).forEach((button) => {
    button.addEventListener("click", openTradeStockSearch);
  });
  tradeHubAddTradeBtn?.addEventListener("click", () => {
    state.tradeSearchReturnRoute = "trade";
    openTradeStockSearch();
  });
  tradeHubAddCashBtn?.addEventListener("click", () => {
    openNewCashTransferDialog();
  });
  tradeHubOpenRecordsBtn?.addEventListener("click", () => {
    resetTradeListPager();
    state.tradePanelTab = "trades";
    state.route = "trade-records";
    persistState();
    renderAll();
  });
  tradeHubOpenCashBtn?.addEventListener("click", () => {
    resetCashListPager();
    state.tradePanelTab = "cash";
    state.route = "trade-cash";
    persistState();
    renderAll();
  });
  document.querySelectorAll("[data-trade-list-back]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.route = "trade";
      persistState();
      renderAll();
    });
  });
  document.querySelectorAll("[data-trade-open]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const target = btn.getAttribute("data-trade-open");
      state.mineReturnRoute = "trade";
      if (target === "accounts") {
        state.route = "mine-accounts";
      } else {
        state.route = "mine-algo";
      }
      persistState();
      renderAll();
    });
  });
  cashTransferTableBody?.addEventListener("click", (e) => {
    const tr = e.target.closest("tr[data-cash-id]");
    if (!tr) {
      return;
    }
    openEditCashTransferDialog(tr.getAttribute("data-cash-id"));
  });
  closeCashTransferDialogBtn?.addEventListener("click", () => {
    state.editingCashTransferId = null;
    cashTransferDialog?.close();
  });
  cashTransferForm?.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!cashTransferForm) {
      return;
    }
    const formData = new FormData(cashTransferForm);
    const existing =
      state.editingCashTransferId &&
      state.cashTransfers.find((x) => x.id === state.editingCashTransferId);
    const row = {
      id: state.editingCashTransferId || crypto.randomUUID(),
      accountId: String(formData.get("accountId") || "default"),
      date: String(formData.get("date") || toDateKey(new Date())),
      direction: String(formData.get("direction") || "in") === "out" ? "out" : "in",
      amount: Number(formData.get("amount") || 0),
      note: String(formData.get("note") || "").trim(),
      createdAt: existing?.createdAt || Date.now(),
    };
    const normalized = normalizeCashTransferRow(row);
    let saved = normalized;
    if (apiReady) {
      try {
        saved = await saveCashTransferToApi(normalized);
      } catch (err) {
        console.error(err);
      }
    }
    const n = state.editingCashTransferId
      ? state.cashTransfers.map((x) => (x.id === saved.id ? saved : x))
      : [...state.cashTransfers, saved];
    state.cashTransfers = n;
    state.editingCashTransferId = null;
    cashTransferDialog?.close();
    state.useDemoData = false;
    persistState();
    renderAll();
    invalidateCashListAfterMutation();
    scheduleMetricsRebuildUiRefresh();
  });
  cashTransferDeleteBtn?.addEventListener("click", async () => {
    const id = state.editingCashTransferId;
    if (!id) {
      return;
    }
    if (!window.confirm("确定删除该条资金记录？")) {
      return;
    }
    try {
      await deleteCashTransferFromApi(id);
    } catch {
      // continue local delete
    }
    state.cashTransfers = state.cashTransfers.filter((x) => x.id !== id);
    state.editingCashTransferId = null;
    cashTransferDialog?.close();
    persistState();
    renderAll();
    invalidateCashListAfterMutation();
    scheduleMetricsRebuildUiRefresh();
  });
  tradeSearchBackBtn?.addEventListener("click", () => goBackFromTradeStockSearch());
  tradeStockSearchInput?.addEventListener("input", (e) => {
    void runTradeSearchSuggestQuery(e.target.value);
  });
  tradeStockSearchResults?.addEventListener("click", (e) => {
    const li = e.target.closest("li[data-symbol]");
    if (!li || !tradeStockSearchResults?.contains(li)) {
      return;
    }
    const symbol = li.getAttribute("data-symbol");
    const name = li.getAttribute("data-name") || "";
    if (!symbol) {
      return;
    }
    applyStockSearchPick(symbol, name);
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
  tradePriceInput?.addEventListener("input", syncTradeAmountFromPriceQuantity);
  tradeQuantityInput?.addEventListener("input", syncTradeAmountFromPriceQuantity);

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
      state.stockRecordTrades = state.stockRecordTrades.filter((item) => item.id !== state.editingTradeId);
    }
    state.trades.push(savedTrade);
    state.trades.sort(sortTradeAsc);
    const symKey = normalizeSymbol(savedTrade.symbol);
    if (symKey && normalizeSymbol(state.activeRecordSymbol) === symKey) {
      const recIdx = state.stockRecordTrades.findIndex((item) => item.id === savedTrade.id);
      if (recIdx >= 0) {
        state.stockRecordTrades[recIdx] = savedTrade;
      } else if (state.route === "stock-record") {
        void loadStockRecordTradesPage({ reset: true });
      }
    }
    persistState();
    clearEditState();
    tradeDialog.close();
    renderAll();
    invalidateTradeListAfterMutation();
    invalidateStockRecordTradesAfterMutation();
    scheduleMetricsRebuildUiRefresh();
    void refreshMarketData();
  });

  if (setCapitalBtn) {
    setCapitalBtn.addEventListener("click", () => {
      capitalDialog?.showModal();
    });
  }
  closeCapitalDialogBtn?.addEventListener("click", () => capitalDialog?.close());

  tradeTableBody?.addEventListener("click", (event) => {
    if (isCommunityPublicTradeTableActive()) {
      return;
    }
    const row = event.target.closest("tr[data-record-id]");
    if (!row) {
      return;
    }
    const id = row.dataset.recordId;
    if (!id) {
      return;
    }
    openTradeRecordActionsSheet(id);
  });

  stockRecordListBody?.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-record-id]");
    if (!row) {
      return;
    }
    const id = row.dataset.recordId;
    if (!id) {
      return;
    }
    openTradeRecordActionsSheet(id);
  });

  closeRecordTradeActionsBtn?.addEventListener("click", () => closeTradeRecordActionsSheet());

  recordTradeActionsDialog?.addEventListener("click", (event) => {
    const actionBtn = event.target.closest("button[data-action]");
    if (!actionBtn) {
      return;
    }
    const action = actionBtn.dataset.action;
    const tradeId = recordTradeActionsDialog.dataset.tradeId;
    closeTradeRecordActionsSheet();
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

  accountTableBody?.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-account-id]");
    if (!row) {
      return;
    }
    const id = row.dataset.accountId;
    if (!id) {
      return;
    }
    openAccountManageDialog(id);
  });

  closeAccountManageBtn?.addEventListener("click", () => {
    state.editingAccountId = null;
    accountManageDialog?.close();
  });

  accountManageSaveBtn?.addEventListener("click", () => void saveManagedAccount());

  accountManageDeleteBtn?.addEventListener("click", () => deleteManagedAccount());

  accountManageDialog?.addEventListener("close", () => {
    state.editingAccountId = null;
  });

  stockTableBody?.addEventListener("click", (event) => {
    const addTradeLink = event.target.closest("[data-stock-add-trade]");
    if (addTradeLink && stockTableBody.contains(addTradeLink)) {
      event.preventDefault();
      const sym = addTradeLink.getAttribute("data-stock-add-trade");
      if (sym) {
        void openNewTradeDialogPrefilledForSymbol(sym, { accountSource: "overview" });
      }
      return;
    }
    const link = event.target.closest("[data-stock-record]");
    if (!link || !stockTableBody.contains(link)) {
      return;
    }
    event.preventDefault();
    const symbol = link.getAttribute("data-stock-record");
    if (symbol) {
      void openStockRecordDialog(symbol);
    }
  });

  closeStockRecordDialogBtn?.addEventListener("click", () => {
    state.stockRecordFromPublicProfile = false;
    state.route = state.previousRoute || "earning";
    persistState();
    renderRoute();
  });

  stockRecordAddTradeBtn?.addEventListener("click", async () => {
    await openAddTradePrefilledForActiveRecordSymbol();
  });

  communityProfileBody?.addEventListener("click", (event) => {
    const link = event.target.closest("[data-stock-record]");
    if (!link || !communityProfileBody.contains(link)) {
      return;
    }
    if (state.route !== "community-profile") {
      return;
    }
    const sym = link.getAttribute("data-stock-record");
    if (sym) {
      void openStockRecordDialog(sym, { fromPublicProfile: true });
    }
  });

  bindAnalysisStockRankHelpOnce();
}

function resetStockRankHelpBubbleLayout(bubble) {
  if (!bubble) {
    return;
  }
  bubble.classList.remove("is-open", "is-fixed");
  bubble.style.position = "";
  bubble.style.left = "";
  bubble.style.top = "";
  bubble.style.right = "";
  bubble.style.bottom = "";
  bubble.style.zIndex = "";
  bubble.style.display = "";
  bubble.style.visibility = "";
  bubble.style.width = "";
  bubble.style.maxWidth = "";
  bubble.style.removeProperty("--help-arrow-left");
  if (bubble._helpWrap && bubble.parentNode === document.body) {
    bubble._helpWrap.appendChild(bubble);
  }
  bubble._helpWrap = null;
  bubble._helpHost = null;
}

function positionStockRankHelpBubble(btn, bubble, host) {
  if (!btn || !bubble) {
    return;
  }
  const wrap = btn.closest(".stock-rank-help-wrap");
  bubble._helpWrap = wrap;
  bubble._helpHost = host;
  document.body.appendChild(bubble);
  bubble.classList.add("is-open", "is-fixed");
  bubble.style.position = "fixed";
  bubble.style.zIndex = "100001";
  bubble.style.display = "block";
  bubble.style.visibility = "hidden";
  bubble.style.width = "max-content";
  bubble.style.maxWidth = "min(280px, 72vw)";
  bubble.style.left = "-9999px";
  bubble.style.top = "0";
  bubble.style.right = "auto";
  const br = btn.getBoundingClientRect();
  const bw = bubble.offsetWidth;
  const bh = bubble.offsetHeight;
  const btnCenterX = br.left + br.width / 2;
  let left = btnCenterX - bw / 2;
  left = Math.max(8, Math.min(left, window.innerWidth - bw - 8));
  let top = br.bottom + 7;
  if (top + bh > window.innerHeight - 8) {
    top = Math.max(8, br.top - bh - 7);
  }
  bubble.style.left = `${left}px`;
  bubble.style.top = `${top}px`;
  const arrowLeft = Math.max(10, Math.min(bw - 22, btnCenterX - left - 6));
  bubble.style.setProperty("--help-arrow-left", `${arrowLeft}px`);
  bubble.style.visibility = "visible";
}

function closeStockRankHelpInHost(host) {
  if (!host) {
    return;
  }
  host.querySelectorAll(".stock-rank-help-bubble.is-open").forEach((el) => {
    resetStockRankHelpBubbleLayout(el);
  });
  document.querySelectorAll(".stock-rank-help-bubble.is-open.is-fixed").forEach((el) => {
    if (el._helpHost === host) {
      resetStockRankHelpBubbleLayout(el);
    }
  });
  host.querySelectorAll(".stock-rank-help-btn").forEach((b) => {
    b.setAttribute("aria-expanded", "false");
  });
}

function bindAnalysisStockRankHelpOnce() {
  if (analysisStockRankHelpListenersBound) {
    return;
  }
  analysisStockRankHelpListenersBound = true;
  document.body.addEventListener("click", (e) => {
    const btn = e.target.closest(".stock-rank-help-btn");
    if (!btn) {
      return;
    }
    const host =
      btn.closest(".analysis-stock-rank-body") ||
      btn.closest(".stock-record-body") ||
      btn.closest(".stock-record-table--pub") ||
      btn.closest(".community-feed-card") ||
      btn.closest(".route-pane--community-trade");
    if (!host) {
      return;
    }
    e.preventDefault();
    e.stopPropagation();
    const wrap = btn.closest(".stock-rank-help-wrap");
    const bubble = wrap?.querySelector(".stock-rank-help-bubble");
    const wasOpen = bubble?.classList.contains("is-open");
    closeStockRankHelpInHost(host);
    if (!wasOpen && bubble) {
      positionStockRankHelpBubble(btn, bubble, host);
      btn.setAttribute("aria-expanded", "true");
    }
  });
  document.addEventListener("click", (e) => {
    if (e.target.closest(".stock-rank-help-wrap")) {
      return;
    }
    document.querySelectorAll(".analysis-stock-rank-body").forEach((host) => {
      closeStockRankHelpInHost(host);
    });
    document.querySelectorAll(".stock-record-body").forEach((host) => {
      closeStockRankHelpInHost(host);
    });
    document.querySelectorAll(".stock-record-table--pub").forEach((host) => {
      closeStockRankHelpInHost(host);
    });
    document.querySelectorAll(".community-feed-card").forEach((host) => {
      closeStockRankHelpInHost(host);
    });
    document.querySelectorAll(".route-pane--community-trade").forEach((host) => {
      closeStockRankHelpInHost(host);
    });
  });
  window.addEventListener(
    "scroll",
    () => {
      document.querySelectorAll(".stock-rank-help-bubble.is-open.is-fixed").forEach((el) => {
        resetStockRankHelpBubbleLayout(el);
      });
      document.querySelectorAll(".stock-rank-help-btn[aria-expanded='true']").forEach((b) => {
        b.setAttribute("aria-expanded", "false");
      });
    },
    true,
  );
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
  syncTradeAmountFromPriceQuantity();
}

/** 买入卖出：发生金额 = 价格×数量，随输入实时更新（与提交时默认金额口径一致） */
function syncTradeAmountFromPriceQuantity() {
  if (!tradePriceInput || !tradeQuantityInput || !tradeAmountInput || !tradeTypeInput) {
    return;
  }
  if (tradeTypeInput.value !== "trade") {
    return;
  }
  const pRaw = String(tradePriceInput.value || "").trim();
  const qRaw = String(tradeQuantityInput.value || "").trim();
  if (pRaw === "" || qRaw === "") {
    tradeAmountInput.value = "";
    return;
  }
  const p = Number(pRaw);
  const q = Number(qRaw);
  if (!Number.isFinite(p) || !Number.isFinite(q)) {
    tradeAmountInput.value = "";
    return;
  }
  const amt = Math.abs(p * q);
  tradeAmountInput.value = formatPlainMoney(amt);
}

function openNewTradeDialog(prefill) {
  clearEditState();
  tradeForm.reset();
  tradeTypeInput.value = "trade";
  applyTradeTypePreset();
  if (tradeAccountInput) {
    tradeAccountInput.value = resolveTradeFormDefaultAccountId();
  }
  tradeDateInput.value = toDateKey(new Date());
  if (prefill && typeof prefill === "object") {
    if (prefill.symbol != null) {
      tradeSymbolInput.value = String(prefill.symbol);
    }
    if (prefill.name != null) {
      tradeNameInput.value = String(prefill.name);
    }
    if (prefill.accountId != null && tradeAccountInput) {
      const aid = resolveValidAccountFilter(String(prefill.accountId));
      if (aid !== "all" && state.accounts.some((a) => a.id === aid)) {
        tradeAccountInput.value = aid;
      }
    }
  }
  tradeDialog.showModal();
  syncTradeAmountFromPriceQuantity();
}

function clearTradeSearchResults() {
  if (tradeSearchSuggestController) {
    tradeSearchSuggestController.abort();
    tradeSearchSuggestController = null;
  }
  if (tradeStockSearchResults) {
    tradeStockSearchResults.innerHTML = "";
    tradeStockSearchResults.hidden = true;
  }
}

function openTradeStockSearch() {
  if (state.route !== "trade-search") {
    state.tradeSearchReturnRoute = state.route;
  }
  state.appModule = "holdings";
  state.route = "trade-search";
  if (tradeStockSearchInput) {
    tradeStockSearchInput.value = "";
  }
  clearTradeSearchResults();
  persistState();
  renderRoute();
  requestAnimationFrame(() => {
    tradeStockSearchInput?.focus();
  });
}

function goBackFromTradeStockSearch() {
  const back = state.tradeSearchReturnRoute || "trade";
  state.route = back;
  clearTradeSearchResults();
  persistState();
  renderRoute();
}

async function runTradeSearchSuggestQuery(raw) {
  const q = String(raw || "").trim();
  if (!tradeStockSearchResults) {
    return;
  }
  if (tradeSearchSuggestController) {
    tradeSearchSuggestController.abort();
  }
  if (!q) {
    clearTradeSearchResults();
    if (tradeStockSearchInput) {
      tradeStockSearchInput.removeAttribute("aria-activedescendant");
    }
    return;
  }
  tradeSearchSuggestController = new AbortController();
  const c = tradeSearchSuggestController;
  const base = getApiBaseForFetch();
  tradeStockSearchResults.innerHTML = `<li class="trade-stock-search-loading" role="option">搜索中…</li>`;
  tradeStockSearchResults.hidden = false;
  try {
    const res = await apiFetch(
      `${base}/sina/suggest?key=${encodeURIComponent(q)}`,
      { signal: c.signal, cache: "no-store" }
    );
    if (c.signal.aborted) {
      return;
    }
    const data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error("bad");
    }
    const list = Array.isArray(data.results) ? data.results : [];
    if (c.signal.aborted) {
      return;
    }
    if (!list.length) {
      tradeStockSearchResults.innerHTML = `<li class="trade-stock-search-empty" role="presentation">无匹配标的</li>`;
      return;
    }
    tradeStockSearchResults.innerHTML = list
      .map((row, i) => {
        const sym = row.symbol != null ? String(row.symbol) : "";
        const code = formatSymbolForDisplay(sym);
        const name = row.name != null ? String(row.name) : sym;
        const mkt = row.market != null ? String(row.market) : "";
        return `<li role="option" id="tssr-${i}" data-symbol="${escapeHtml(sym)}" data-name="${escapeHtml(name)}">
          <div class="trade-stock-search-name">${escapeHtml(name)}</div>
          <div class="trade-stock-search-meta">
            <span class="trade-stock-search-code">${escapeHtml(code || sym)}</span><br />
            <span>${escapeHtml(mkt)}</span>
          </div>
        </li>`;
      })
      .join("");
  } catch (e) {
    if (e.name === "AbortError" || c.signal.aborted) {
      return;
    }
    tradeStockSearchResults.innerHTML = `<li class="trade-stock-search-empty" role="presentation">搜索失败，请检查网络后重试</li>`;
  } finally {
    if (c.signal.aborted) {
      return;
    }
    tradeSearchSuggestController = null;
  }
}

function applyStockSearchPick(symbol, name) {
  const sym = normalizeSymbol(String(symbol || "").trim());
  const n = String(name || "").trim() || sym;
  state.appModule = "holdings";
  state.route = "trade";
  state.tradeSearchReturnRoute = "trade";
  if (tradeStockSearchInput) {
    tradeStockSearchInput.value = "";
  }
  clearTradeSearchResults();
  persistState();
  renderRoute();
  openNewTradeDialog({ symbol: sym, name: n });
}


function renderAll() {
  const prevSnap = previousRenderAllRouteForOverviewSnapshot;
  if (
    (prevSnap === "earning" && state.route !== "earning") ||
    (state.route === "earning" && prevSnap != null && prevSnap !== "earning")
  ) {
    invalidateOverviewMetricsUi();
    if (prevSnap === "earning" && state.route !== "earning") {
      stopMetricsRebuildPoll();
    }
  }
  if (isEarningHomeRoute() && prevSnap != null && prevSnap !== "earning") {
    invalidateOverviewMetricsUi();
    void refreshOverviewProfitRowFromSnapshots();
  }
  previousRenderAllRouteForOverviewSnapshot = state.route;
  renderControls();
  renderRoute();
  clearHoldingsTradePaneDomIfHiddenRoute();
  if (state.route === "earning") {
    renderOverviewAndStockTable();
  } else if (state.route === "analysis") {
    void renderAnalysis();
  } else if (state.route === "trade-records" || state.route === "trade-cash") {
    renderTradeTable();
    void ensureTradeListRouteReady();
  } else if (state.route === "trade" || state.route === "trade-search") {
    /* 交易首页 / 搜索页不预拉 trades、cash-transfers */
  } else if (state.route === "stock-record" && state.activeRecordSymbol) {
    if (!state.stockRecordPageLoading) {
      void renderStockRecordPage(state.activeRecordSymbol);
    }
  } else if (state.route === "community-profile") {
    if (state.communityProfileTab === "earning" && state.communityProfileUserId) {
      void loadPublicEarningTabData(state.communityProfileUserId);
    } else if (state.communityProfileTab === "trade" && state.communityProfileUserId) {
      void loadCommunityPublicTrades(state.communityProfileUserId);
    } else if (state.communityProfileTab === "analysis" && state.lastPublicProfileDetail) {
      void openCommunityProfileAnalysisTab();
    }
  }
}

function renderMineSection() {
  if (mineUserPhone) {
    const line = sessionProfile.phoneMasked || sessionPhone;
    mineUserPhone.textContent = sessionPhone ? `已登录 ${line}` : "";
  }
  if (mineNicknameDisplay) {
    const nick = sessionProfile.nickname || "";
    mineNicknameDisplay.textContent = nick || "未设置";
  }
  if (mineNicknameInput) {
    if (document.activeElement !== mineNicknameInput) {
      mineNicknameInput.value = sessionProfile.nickname || "";
    }
    mineNicknameInput.disabled = !sessionPhone;
  }
  if (tradeHubCommunityPublicToggle) {
    tradeHubCommunityPublicToggle.checked = sessionProfile.communityPublic !== false;
    tradeHubCommunityPublicToggle.disabled = !sessionPhone;
  }
  if (tradeHubAlgoSummary) {
    const labels = { twr: "时间加权", mwr: "资金加权", time: "时间加权", money: "资金加权", cost: "时间加权" };
    tradeHubAlgoSummary.textContent = labels[state.algoMode] || labels.twr;
  }
  if (algoModeSelectMine) {
    algoModeSelectMine.value = normalizeProfitAlgoMode(state.algoMode);
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
    let active = false;
    if (value === "custom") {
      active = state.analysisRangeMode === "custom";
    } else if (value === "all") {
      active = state.analysisRangeMode === "all";
    } else if (value === "mtd") {
      active = state.analysisRangeMode === "preset" && state.analysisPreset === "mtd";
    } else if (value === "365") {
      active = state.analysisRangeMode === "preset" && state.analysisPreset === "ytd";
    } else {
      active =
        state.analysisRangeMode === "preset" &&
        state.analysisPreset !== "mtd" &&
        state.analysisPreset !== "ytd" &&
        Number(value) === state.rangeDays;
    }
    chip.classList.toggle("active", active);
  });
  if (customRangeRow) {
    customRangeRow.classList.toggle("hidden", state.analysisRangeMode !== "custom");
  }
  if (customRangeStartInput) {
    customRangeStartInput.value =
      state.analysisRangeMode === "custom"
        ? state.customRangeDraftStart || ""
        : state.customRangeStart || "";
  }
  if (customRangeEndInput) {
    customRangeEndInput.value =
      state.analysisRangeMode === "custom"
        ? state.customRangeDraftEnd || ""
        : state.customRangeEnd || "";
  }
  if (assetCurveModeSelect) {
    assetCurveModeSelect.value = ["total_assets", "market", "cash", "cash_ratio"].includes(state.capitalTrendMode)
      ? state.capitalTrendMode
      : "total_assets";
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
  setSelect(tradeCashAccountFilterSelect, state.tradeFilterAccountId, true);
  setSelect(stockRecordAccountSelect, state.stockRecordAccountId, true);
  setSelect(tradeAccountInput, resolveTradeFormDefaultAccountId(), false);
  setSelect(cashTransferAccount, resolveTradeFormDefaultAccountId(), false);
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
      const aid = escapeHtml(String(account.id));
      return `
        <tr class="account-table-row" data-account-id="${aid}">
          <td>${escapeHtml(account.name)}</td>
          <td>${getCurrencyLabel(account.currency)}</td>
          <td>${count}</td>
        </tr>
      `;
    })
    .join("");
}

function isMineRoute(route) {
  return (
    route === "mine" ||
    route === "mine-accounts" ||
    route === "mine-algo" ||
    route === "mine-community"
  );
}

function buildBrowserRouteSnapshot() {
  return {
    route: state.route,
    appModule: state.appModule,
    previousRoute: state.previousRoute,
    communityProfileUserId: state.communityProfileUserId || "",
    communityProfileReturnRoute: state.communityProfileReturnRoute || "community-feed",
    communityProfileTab: state.communityProfileTab || "earning",
    activeRecordSymbol: state.activeRecordSymbol || "",
    tradeSearchReturnRoute: state.tradeSearchReturnRoute || "trade",
  };
}

function buildBrowserRouteKey(snapshot = {}) {
  return [
    snapshot.route || "",
    snapshot.appModule || "",
    snapshot.communityProfileUserId || "",
    snapshot.activeRecordSymbol || "",
    snapshot.communityProfileTab || "",
  ].join("|");
}

function applyBrowserRouteSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== "object") {
    return false;
  }
  const route = String(snapshot.route || "").trim();
  if (!route) {
    return false;
  }
  const appModule = snapshot.appModule === "community" ? "community" : "holdings";
  state.appModule = appModule;
  state.route = route;
  state.previousRoute = String(snapshot.previousRoute || state.previousRoute || "earning");
  state.communityProfileReturnRoute = String(
    snapshot.communityProfileReturnRoute || state.communityProfileReturnRoute || "community-feed",
  );
  state.communityProfileUserId = snapshot.communityProfileUserId
    ? String(snapshot.communityProfileUserId)
    : null;
  state.communityProfileTab = ["earning", "analysis", "trade"].includes(snapshot.communityProfileTab)
    ? snapshot.communityProfileTab
    : "earning";
  state.activeRecordSymbol = snapshot.activeRecordSymbol ? normalizeSymbol(snapshot.activeRecordSymbol) : null;
  state.tradeSearchReturnRoute = String(snapshot.tradeSearchReturnRoute || state.tradeSearchReturnRoute || "trade");
  if (state.route !== "community-profile") {
    state.communityProfileUserId = null;
  }
  if (state.route !== "stock-record") {
    state.activeRecordSymbol = null;
  }
  return true;
}

function syncBrowserRouteHistory(mode = "push") {
  if (typeof window === "undefined" || !window.history) {
    return;
  }
  const snapshot = buildBrowserRouteSnapshot();
  const key = buildBrowserRouteKey(snapshot);
  try {
    if (mode === "replace") {
      window.history.replaceState({ [BROWSER_ROUTE_STATE_KEY]: snapshot }, "", window.location.href);
      lastBrowserRouteKey = key;
      return;
    }
    if (applyingBrowserRoutePopstate || key === lastBrowserRouteKey) {
      lastBrowserRouteKey = key;
      return;
    }
    window.history.pushState({ [BROWSER_ROUTE_STATE_KEY]: snapshot }, "", window.location.href);
    lastBrowserRouteKey = key;
  } catch {
    // ignore browser history failures
  }
}

function bindBrowserRouteHistory() {
  if (browserHistoryListenerBound || typeof window === "undefined") {
    return;
  }
  browserHistoryListenerBound = true;
  window.addEventListener("popstate", (event) => {
    const snapshot = event?.state?.[BROWSER_ROUTE_STATE_KEY];
    if (!snapshot) {
      return;
    }
    applyingBrowserRoutePopstate = true;
    const applied = applyBrowserRouteSnapshot(snapshot);
    if (applied) {
      renderAll();
    }
    applyingBrowserRoutePopstate = false;
  });
}

function formatTwrSignedHtml(x) {
  if (x == null || !Number.isFinite(Number(x))) {
    return "<strong>—</strong>";
  }
  const v = Number(x) * 100;
  const sign = v > 0 ? "+" : "";
  const cls = v > 0 ? "up" : v < 0 ? "down" : "";
  return `<strong class="${cls}">${sign}${v.toFixed(2)}%</strong>`;
}

function openAppDrawer() {
  appDrawerBackdrop?.classList.remove("hidden");
  appDrawer?.classList.add("is-open");
  appDrawerBackdrop?.classList.add("is-open");
  appDrawer?.setAttribute("aria-hidden", "false");
  appMenuBtn?.setAttribute("aria-expanded", "true");
}

function closeAppDrawer() {
  // 焦点若仍在抽屉内，先移出再标 aria-hidden，否则 Chrome 报
  // "Blocked aria-hidden… descendant retained focus"，并可能带来怪异交互。
  const active = typeof document !== "undefined" ? document.activeElement : null;
  if (appDrawer && active && appDrawer.contains(active)) {
    if (appMenuBtn && typeof appMenuBtn.focus === "function") {
      try {
        appMenuBtn.focus({ preventScroll: true });
      } catch {
        appMenuBtn.focus();
      }
    } else if (typeof active.blur === "function") {
      active.blur();
    }
  }
  appDrawer?.classList.remove("is-open");
  appDrawerBackdrop?.classList.remove("is-open");
  appDrawer?.setAttribute("aria-hidden", "true");
  appMenuBtn?.setAttribute("aria-expanded", "false");
  window.setTimeout(() => {
    appDrawerBackdrop?.classList.add("hidden");
  }, 220);
}

function openCommunityProfile(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  if (state.route.startsWith("community-") && state.route !== "community-profile") {
    state.communityProfileReturnRoute = state.route;
  } else {
    state.communityProfileReturnRoute = "community-feed";
  }
  state.communityProfileUserId = uid;
  state.route = "community-profile";
  state.appModule = "community";
  state.communityProfileStage = "month";
  state.communityProfileTab = "earning";
  state.stockSortKey = "weight";
  state.stockSortOrder = "desc";
  state.lastPublicProfileDetail = null;
  state.communityPublicTrades = [];
  resetCommunityPublicTradesPager();
  state.publicEarningBundleUi = {
    ready: false,
    loading: false,
    key: "",
    bundle: null,
    meta: null,
  };
  state.publicAnalysisBundleUi = {
    ready: false,
    loading: false,
    key: "",
    bundle: null,
    meta: null,
  };
  lastCommunityDataKey = "";
  persistState();
  renderAll();
}

function communityFollowButtonHtml(card) {
  if (!sessionUserId) {
    return "";
  }
  const uid = escapeHtml(card.userId);
  const fo = card.following ? "已关注" : "关注";
  const followCls = card.following ? "community-follow-btn is-on" : "community-follow-btn";
  return `<button type="button" class="${followCls}" data-user-id="${uid}">${escapeHtml(fo)}</button>`;
}

const COMMUNITY_CARD_RETURN_DEFS = [
  { key: "today", label: "今日收益", rateKey: "todayTwr" },
  { key: "mtd", label: "本月收益", rateKey: "mtdTwr" },
  { key: "ytd", label: "本年收益", rateKey: "ytdTwr" },
  { key: "inception", label: "总收益", rateKey: "totalTwr" },
];

function communityCardReturnTileTone(rateStr) {
  const text = bundleFmtText(rateStr);
  if (!text || text === "—") {
    return "flat";
  }
  const rate = parseBundlePercent(rateStr);
  if (!Number.isFinite(rate) || rate === 0) {
    return "flat";
  }
  return rate > 0 ? "up" : "down";
}

function buildCommunityCardReturnTileHtml(label, rateStr) {
  const tone = communityCardReturnTileTone(rateStr);
  const text = bundleFmtText(rateStr);
  return `<div class="community-card-return-tile community-card-return-tile--${tone}">
    <span class="community-card-return-label">${escapeHtml(label)}</span>
    <span class="community-card-return-value">${escapeHtml(text)}</span>
  </div>`;
}

function buildCommunityCardReturnsHtml(card) {
  const tiles = COMMUNITY_CARD_RETURN_DEFS.map((def) =>
    buildCommunityCardReturnTileHtml(def.label, card[def.rateKey]),
  ).join("");
  return `<div class="community-card-returns">${tiles}</div>`;
}

function communityTop3WeightHtml(weight) {
  if (weight == null || weight === "") {
    return "—";
  }
  if (typeof weight === "number" && Number.isFinite(weight)) {
    return `<span class="community-top3-pct">${(weight * 100).toFixed(1)}%</span>`;
  }
  const text = String(weight).trim();
  if (!text || text === "—") {
    return "—";
  }
  if (text.includes("%")) {
    return `<span class="community-top3-pct">${escapeHtml(text)}</span>`;
  }
  const ratio = parseBundlePercent(text);
  if (Number.isFinite(ratio)) {
    return `<span class="community-top3-pct">${(ratio * 100).toFixed(1)}%</span>`;
  }
  return `<span class="community-top3-pct">${escapeHtml(text)}</span>`;
}

function buildCommunityStockIdentityHtml({ marketTag, symbol, name, variant = "feed" }) {
  const code = escapeHtml(formatSymbolForDisplay(symbol || ""));
  const tag = escapeHtml(marketTag || "OT");
  const tagLower = String(marketTag || "ot").toLowerCase();
  const stockName = escapeHtml(getDisplayName(symbol, name));
  const variantClass =
    variant === "top3" ? "community-stock-identity--top3" : "community-stock-identity--feed";
  return `<div class="community-stock-identity ${variantClass}">
    <span class="community-market-tag community-market-tag--${tagLower}">${tag}</span>
    <strong class="community-stock-identity__name">${stockName}</strong>
    <span class="community-stock-identity__code">${code}</span>
  </div>`;
}

function buildTop3ListHtml(topPositions) {
  const top = (topPositions || []).slice(0, 3);
  if (!top.length) {
    return "";
  }
  const rows = top
    .map((p, i) => {
      const right = communityTop3WeightHtml(p.weight);
      const identity = buildCommunityStockIdentityHtml({
        marketTag: p.marketTag,
        symbol: p.symbol || p.displayCode || "",
        name: p.name,
        variant: "top3",
      });
      return `<div class="community-top3-row">
        <span class="community-top3-rank">${i + 1}</span>
        <div class="community-top3-mid">${identity}</div>
        <div class="community-top3-val">${right}</div>
      </div>`;
    })
    .join("");
  return `<div class="community-top3"><div class="community-top3-title">TOP3持仓</div>${rows}</div>`;
}

function buildCommunityCardInner(card, opts = {}) {
  const { showRank = null, followHtml = "" } = opts;
  const name = escapeHtml(card.displayName || "用户");
  const rankBlock =
    showRank != null
      ? `<div class="community-rank-index ${showRank <= 3 ? `top${showRank}` : ""}">${showRank}</div>`
      : "";
  const top3 = buildTop3ListHtml(card.topPositions);
  return `
    <div class="community-card__header-row">
      <div class="community-card__header-left">
        ${rankBlock}
        <div class="community-card-name-stack">
          <div class="community-card-name-line">${name}</div>
          ${card.mutual ? `<p class="community-card-meta">互相关注</p>` : ""}
        </div>
      </div>
      ${followHtml ? `<div class="community-card__header-follow">${followHtml}</div>` : ""}
    </div>
    ${buildCommunityCardReturnsHtml(card)}
    ${top3}
  `;
}

function wrapInteractiveCommunityCard(card, opts = {}) {
  const uid = escapeHtml(card.userId);
  const innerHtml = buildCommunityCardInner(card, {
    showRank: opts.showRank ?? null,
    followHtml: communityFollowButtonHtml(card),
  });
  return `<article class="community-card community-card--interactive" data-community-profile-card data-community-user="${uid}">
    <div class="community-card__main">${innerHtml}</div>
  </article>`;
}

function formatCommunityFeedTradeDate(dateStr) {
  const d = String(dateStr || "").slice(0, 10);
  const m = d.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m ? `${m[2]}-${m[3]}` : "—";
}

const COMMUNITY_FEED_USER_ICON_SVG = `<svg class="community-feed-user-icon" width="18" height="18" viewBox="0 0 24 24" aria-hidden="true" focusable="false"><circle cx="12" cy="8" r="4" fill="currentColor"/><path fill="currentColor" d="M4 20c0-4 3.6-6 8-6s8 2 8 6v1H4v-1z"/></svg>`;

function feedRowHtml(t) {
  const side = t.side === "sell" ? "sell" : "buy";
  const sideLabel = t.side === "sell" ? "卖出" : "买入";
  const uid = escapeHtml(t.userId);
  const symEsc = escapeHtml(String(t.symbol || "").trim());
  const priceStr =
    t.price != null && Number.isFinite(Number(t.price)) ? formatNumber(Number(t.price), 3) : "—";
  const shareRaw = t.amount_share_ratio ?? t.amountShareOfCurrentTotalMv;
  const shareNum = Number(shareRaw);
  const shareStr =
    shareRaw != null && Number.isFinite(shareNum) ? formatPercent(shareNum) : "—";
  const dateDisplay = formatCommunityFeedTradeDate(t.date);
  const stockIdentity = buildCommunityStockIdentityHtml({
    marketTag: t.marketTag,
    symbol: t.symbol || t.displayCode || "",
    name: t.name,
    variant: "feed",
  });
  const noteBlock = t.note
    ? `<p class="community-feed-note"><span class="community-feed-dt">备注：</span><span class="community-feed-dd">${escapeHtml(t.note)}</span></p>`
    : "";
  return `
    <article class="community-feed-card">
      <div class="community-feed-card__inner">
        <div class="community-feed-card__head">
          <div class="community-feed-card__user">
            ${COMMUNITY_FEED_USER_ICON_SVG}
            <span class="community-feed-user-name">${escapeHtml(t.displayName)}</span>
          </div>
          <div class="community-feed-card__actions">
            <a href="javascript:void(0)" class="community-feed-card__action-link" data-community-feed-stock-analysis data-community-user="${uid}" data-community-symbol="${symEsc}">个股分析</a>
            <a href="javascript:void(0)" class="community-feed-card__action-link" data-community-feed-portfolio-analysis data-community-user="${uid}">组合分析</a>
          </div>
        </div>
        <div class="community-feed-card__stock-row">
          ${stockIdentity}
          <span class="community-feed-side-pill community-feed-side-pill--${side}">${sideLabel}</span>
        </div>
        <div class="community-feed-card__metrics">
          <div class="community-feed-metric">
            <span class="community-feed-metric-label">交易价格</span>
            <span class="community-feed-metric-value">${escapeHtml(priceStr)}</span>
          </div>
          <div class="community-feed-metric">
            <span class="community-feed-metric-label community-feed-metric-label--with-help">
              <span>金额</span>
              <span class="stock-rank-help-wrap community-feed-amt-help-wrap">
                <button type="button" class="stock-rank-help-btn" aria-expanded="false" aria-label="金额占比说明">?</button>
                <div class="stock-rank-help-bubble" role="tooltip">本次交易金额占当前总资产比例</div>
              </span>
            </span>
            <span class="community-feed-metric-value">${escapeHtml(shareStr)}</span>
          </div>
          <div class="community-feed-metric">
            <span class="community-feed-metric-label">交易日期</span>
            <span class="community-feed-metric-value">${escapeHtml(dateDisplay)}</span>
          </div>
        </div>
        ${noteBlock}
      </div>
    </article>
  `;
}


async function toggleFollowCommunity(userId, btnEl) {
  const uid = String(userId || "").trim();
  if (!uid || !sessionUserId || !btnEl) {
    return;
  }
  const base = getApiBaseForFetch();
  const isOn = btnEl.classList.contains("is-on");
  try {
    const r = await apiFetch(`${base}/community/follow/${encodeURIComponent(uid)}`, {
      method: isOn ? "DELETE" : "POST",
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j?.ok) {
      return;
    }
    const nowOn = isOn ? false : j.following !== false;
    btnEl.classList.toggle("is-on", nowOn);
    btnEl.textContent = nowOn ? "已关注" : "关注";
    if (state.route === "community-profile" && state.communityProfileUserId === uid) {
      lastCommunityDataKey = "";
      void loadCommunityProfileDetail();
    }
  } catch {
    // ignore
  }
}

async function loadCommunityFeed() {
  if (!communityFeedList || !sessionPhone) {
    return;
  }
  if (!apiReady) {
    communityFeedList.innerHTML = `<p class="empty">连接服务端后可查看社区动态</p>`;
    return;
  }
  showRouteLoading("数据正在加载中");
  communityFeedList.innerHTML = `<p class="empty">加载中…</p>`;
  try {
    const r = await apiFetch(`${getApiBaseForFetch()}/community/feed`, { cache: "no-store" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j?.ok) {
      communityFeedList.innerHTML = `<p class="empty">${escapeHtml(j?.error || "加载失败")}</p>`;
      return;
    }
    const rows = Array.isArray(j.data) ? j.data : [];
    if (!rows.length) {
      communityFeedList.innerHTML = `<p class="empty">暂无已关注用户的交易动态，可在「排行」或他人主页关注用户后查看</p>`;
      return;
    }
    await hydrateSymbolNameMap(rows.map((row) => row.symbol));
    communityFeedList.innerHTML = rows.map((t) => feedRowHtml(t)).join("");
  } catch {
    communityFeedList.innerHTML = `<p class="empty">网络错误</p>`;
  } finally {
    hideRouteLoading();
  }
}

async function loadCommunityFollowing() {
  if (!communityFollowingList || !sessionPhone) {
    return;
  }
  if (!apiReady) {
    communityFollowingList.innerHTML = `<p class="empty">连接服务端后可查看</p>`;
    return;
  }
  showRouteLoading("数据正在加载中");
  communityFollowingList.innerHTML = `<p class="empty">加载中…</p>`;
  try {
    const r = await apiFetch(`${getApiBaseForFetch()}/community/following`, { cache: "no-store" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j?.ok) {
      communityFollowingList.innerHTML = `<p class="empty">${escapeHtml(j?.error || "加载失败")}</p>`;
      return;
    }
    const cards = Array.isArray(j.data) ? j.data : [];
    if (!cards.length) {
      communityFollowingList.innerHTML = `<p class="empty">还没有关注任何人</p>`;
      return;
    }
    await hydrateSymbolNameMap(cards.flatMap((card) => (card?.topPositions || []).map((p) => p?.symbol)));
    communityFollowingList.innerHTML = cards.map((c) => wrapInteractiveCommunityCard(c)).join("");
  } catch {
    communityFollowingList.innerHTML = `<p class="empty">网络错误</p>`;
  } finally {
    hideRouteLoading();
  }
}

async function loadCommunityLeaderboard() {
  if (!communityLeaderboardList || !sessionPhone) {
    return;
  }
  if (!apiReady) {
    communityLeaderboardList.innerHTML = `<p class="empty">连接服务端后可查看排行</p>`;
    return;
  }
  showRouteLoading("数据正在加载中");
  communityLeaderboardList.innerHTML = `<p class="empty">加载中…</p>`;
  try {
    const r = await apiFetch(`${getApiBaseForFetch()}/community/leaderboard`, { cache: "no-store" });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j?.ok) {
      communityLeaderboardList.innerHTML = `<p class="empty">${escapeHtml(j?.error || "加载失败")}</p>`;
      return;
    }
    const entries = j.data?.entries || [];
    if (!entries.length) {
      communityLeaderboardList.innerHTML = `<p class="empty">暂无排行（需公开社区、满足归一条件并有交易）</p>`;
      return;
    }
    await hydrateSymbolNameMap(entries.flatMap((card) => (card?.topPositions || []).map((p) => p?.symbol)));
    communityLeaderboardList.innerHTML = entries
      .map((c, idx) =>
        wrapInteractiveCommunityCard(c, { showRank: idx + 1 }),
      )
      .join("");
  } catch {
    communityLeaderboardList.innerHTML = `<p class="empty">网络错误</p>`;
  } finally {
    hideRouteLoading();
  }
}

function profitMainClassFromAmount(amount) {
  if (amount == null || !Number.isFinite(Number(amount))) {
    return "";
  }
  return Number(amount) > 0 ? "up" : Number(amount) < 0 ? "down" : "";
}

function twrColorClass(rate) {
  if (rate == null || !Number.isFinite(Number(rate))) {
    return "";
  }
  const v = Number(rate);
  return v > 0 ? "up" : v < 0 ? "down" : "";
}

function metricValueWithRateMoney(amount, rate) {
  const core = formatCurrency(amount);
  const amountText = Number(amount) > 0 ? `+${core}` : core;
  const rateText = formatPercent(rate);
  const amtCls = profitMainClassFromAmount(amount);
  const rateCls = twrColorClass(rate);
  return `<span class="profit-amt ${amtCls}">${amountText}</span><span class="profit-rate-inline ${rateCls}">${rateText}</span>`;
}

function metricValueWithRateMoneyOptional(amount, rate) {
  if (amount != null && Number.isFinite(Number(amount))) {
    return metricValueWithRateMoney(amount, rate);
  }
  const rateText =
    rate != null && Number.isFinite(Number(rate)) ? formatPercent(rate) : "—";
  const rateCls = twrColorClass(rate);
  return `<span class="profit-amt">—</span><span class="profit-rate-inline ${rateCls}">${rateText}</span>`;
}

/** 他人主页总览：仅展示收益率（无金额） */
function formatPublicProfileRateOnlyHtml(rate) {
  const rateText = rate != null && Number.isFinite(Number(rate)) ? formatPercent(rate) : "—";
  const rateCls = twrColorClass(rate);
  return `<span class="profit-rate-inline profit-rate-only ${rateCls}">${rateText}</span>`;
}

function useCommunityPublicStockRecord() {
  return !!(state.stockRecordFromPublicProfile && state.communityProfileUserId);
}

function publicTradeAmountShare(trade) {
  const stored = trade.amount_share_ratio ?? trade.amountShareRatio;
  if (stored != null && Number.isFinite(Number(stored))) {
    return Number(stored);
  }
  return null;
}

/** 他人主页个股表：排除已无持仓（含 A 股股数四舍五入为 0、浮点残差）的行 */
function isPublicProfileActiveHoldingRow(row) {
  const q = Number(row.quantity);
  if (!Number.isFinite(q) || q <= 0 || q < 1e-6) {
    return false;
  }
  if (row.market === "A股" && Math.round(q) <= 0) {
    return false;
  }
  return true;
}

/** 与首页总览一致：用对方脱敏后的 trades + 资金/资产口径，在当前行情下重算（与本人「全部账户」视图对齐）。 */
function withPublicTradesContext(d, fn) {
  const pubRows = state.communityPublicTrades;
  if (!Array.isArray(pubRows) || !pubRows.length) {
    return fn();
  }
  const prevTrades = state.trades;
  const prevAlgo = state.algoMode;
  const prevBook = state._overviewBookCurrencyOverride;
  state.trades = pubRows;
  state.algoMode = "twr";
  state._overviewBookCurrencyOverride = null;
  try {
    return fn();
  } finally {
    state.trades = prevTrades;
    state.algoMode = prevAlgo;
    state._overviewBookCurrencyOverride = prevBook;
  }
}

async function withPublicTradesContextAsync(d, asyncFn) {
  const pubRows = state.communityPublicTrades;
  if (!Array.isArray(pubRows) || !pubRows.length || typeof asyncFn !== "function") {
    return;
  }
  const prevTrades = state.trades;
  const prevAlgo = state.algoMode;
  const prevBook = state._overviewBookCurrencyOverride;
  state.trades = pubRows;
  state.algoMode = "twr";
  state._overviewBookCurrencyOverride = null;
  try {
    await asyncFn();
  } finally {
    state.trades = prevTrades;
    state.algoMode = prevAlgo;
    state._overviewBookCurrencyOverride = prevBook;
  }
}

/** 他人收益 Tab：物理渲染列（与私人表同列索引的 th/td 规则，不含金额列） */
const PUBLIC_EARNING_VISIBLE_COL_INDICES = [0, 2, 4, 5, 7, 9, 11, 12, 13, 14];

function stockTableAllColIndices() {
  return Array.from({ length: OVERVIEW_STOCK_TABLE_COL_COUNT }, (_, i) => i);
}

function stockTableVisibleColIndices(ctx) {
  return ctx?.visibleColIndices?.length ? ctx.visibleColIndices : stockTableAllColIndices();
}

function stockTableVisibleColCount(ctx) {
  return stockTableVisibleColIndices(ctx).length;
}
const PUBLIC_EARNING_STAGE_DEFS = [
  { key: "today", label: "今日收益" },
  { key: "mtd", label: "本月收益" },
  { key: "ytd", label: "本年收益" },
  { key: "inception", label: "总收益" },
];

function publicEarningBundleCacheKey(targetId) {
  return `pub-earn::${String(targetId || "").trim()}::all::${METRICS_HOME_BUNDLE_STAGES}`;
}

function paintCommunityReturnTile(valueEl, rateStr) {
  if (!valueEl) {
    return;
  }
  const tile = valueEl.closest(".community-card-return-tile");
  const tone = communityCardReturnTileTone(rateStr);
  valueEl.textContent = bundleFmtText(rateStr);
  valueEl.className = "community-card-return-value";
  if (tile) {
    tile.className = `community-card-return-tile community-card-return-tile--${tone}`;
  }
}

function buildCommunityProfileReturnsShellHtml() {
  const tiles = PUBLIC_EARNING_STAGE_DEFS.map(
    (s) => `<div class="community-card-return-tile community-card-return-tile--flat" data-pub-tile="${escapeHtml(s.key)}">
    <span class="community-card-return-label">${escapeHtml(s.label)}</span>
    <span class="community-card-return-value" data-pub-stage="${escapeHtml(s.key)}">--</span>
  </div>`,
  ).join("");
  return `<div class="community-card-returns community-profile-returns" id="pubReturnsGrid">${tiles}</div>`;
}

function getCommunityEarningPanelHtml() {
  return `
    <article class="overview-card community-profile-overview-min">
      <div class="overview-head overview-head--public-earn">
        <span id="pubQuoteTime" class="market-data-status" aria-live="polite">-- 更新</span>
      </div>
      ${buildCommunityProfileReturnsShellHtml()}
      <div id="pubOverviewGrid" class="overview-grid"></div>
    </article>
    <article class="stock-card">
      <div class="stock-head stock-head-row">
        <h2 class="stock-title">个股收益</h2>
      </div>
      <div class="table-scroll">
        <table class="stock-table stock-table--public-community">
          <tbody id="pubStockTableBody"></tbody>
        </table>
      </div>
    </article>
    <p class="community-profile-earning-disclaimer" role="note">
      组合公开页面金额数据均已归一化处理，持仓占比、收益率与真实情况一致。
    </p>
  `;
}

function mountPublicCommunityStockTableHead() {
  const pubTable = document.getElementById("pubStockTableBody")?.closest("table.stock-table");
  const srcThs = stockTableBody?.closest("table.stock-table")?.querySelectorAll("thead th");
  if (!pubTable || !srcThs?.length) {
    return;
  }
  let thead = pubTable.querySelector("thead");
  if (!thead) {
    thead = document.createElement("thead");
    pubTable.insertBefore(thead, pubTable.querySelector("tbody"));
  }
  const tr = document.createElement("tr");
  for (const col of PUBLIC_EARNING_VISIBLE_COL_INDICES) {
    const src = srcThs[col];
    if (!src) {
      continue;
    }
    const th = src.cloneNode(true);
    th.setAttribute("data-stock-col", String(col));
    tr.appendChild(th);
  }
  thead.replaceChildren(tr);
}

function getPrivateStockTableCtx() {
  return {
    tbody: stockTableBody,
    table: stockTableBody?.closest("table.stock-table") || null,
    visibleColIndices: null,
    showTradeLink: true,
    opProbeText: "记录  交易",
    layoutCacheKey: overviewStockTableLayoutCacheKey,
    widthCache: overviewStockColWidthCache,
  };
}

function getPublicStockTableCtx() {
  const tbody = document.getElementById("pubStockTableBody");
  return {
    tbody,
    table: tbody?.closest("table.stock-table") || null,
    visibleColIndices: PUBLIC_EARNING_VISIBLE_COL_INDICES,
    showTradeLink: false,
    opProbeText: "记录",
    layoutCacheKey: (rows) =>
      `${publicEarningBundleCacheKey(state.communityProfileUserId)}::${overviewStockTableLayoutCacheKey(rows).split("::").slice(1).join("::")}`,
    widthCache: publicEarningColWidthCache,
  };
}

let publicEarningColWidthCache = { key: "", widths: null };

function syncStockTableSortHeaderUi(ctx) {
  const table = ctx?.table;
  if (!table) {
    return;
  }
  table.querySelectorAll(".th-sort-btn").forEach((button) => {
    const key = button.dataset.sortKey || "";
    button.classList.remove("asc", "desc", "active");
    if (state.stockSortOrder !== "default" && key === state.stockSortKey) {
      button.classList.add("active", state.stockSortOrder);
    }
  });
}

function syncCommunityEarningSortHeaderUi() {
  syncStockTableSortHeaderUi(getPublicStockTableCtx());
}

function applyPublicEarningMetaToUi(meta) {
  if (!meta || typeof meta !== "object") {
    return;
  }
  if (meta.quoteTime) {
    state.quoteTime = String(meta.quoteTime);
  }
  const pubQuote = document.getElementById("pubQuoteTime");
  if (pubQuote) {
    if (meta.rebuilding) {
      pubQuote.textContent = "历史指标重算中，请稍候…";
      pubQuote.classList.add("is-rebuilding");
      return;
    }
    pubQuote.classList.remove("is-rebuilding");
    const timeText = state.quoteTime ? `${formatQuoteTimeForStatus(state.quoteTime)} 更新` : "-- 更新";
    pubQuote.textContent = timeText;
    pubQuote.classList.toggle("is-delayed", !!meta.delayed);
  }
}

function paintPublicEarningFromBundle(bundle) {
  return paintOverviewFromMetricsBundle(bundle.returns, bundle.assets, bundle.holdings, { mode: "public" });
}

function repaintPublicEarningHoldingsFromCache() {
  const bundle = state.publicEarningBundleUi.bundle;
  if (!bundle?.holdings?.rows) {
    return;
  }
  mountPublicCommunityStockTableHead();
  paintStockTableFromMetricsRows(bundle.holdings.rows, getPublicStockTableCtx());
  syncCommunityEarningSortHeaderUi();
}

async function fetchPublicHomeBundleMetrics(targetId) {
  const tid = String(targetId || "").trim();
  if (!tid || !apiReady) {
    return null;
  }
  const key = publicEarningBundleCacheKey(tid);
  if (state.publicEarningBundleUi.ready && state.publicEarningBundleUi.key === key && state.publicEarningBundleUi.bundle) {
    return state.publicEarningBundleUi.bundle;
  }
  if (state.publicEarningBundleUi.loading) {
    return null;
  }
  state.publicEarningBundleUi.loading = true;
  try {
    const data = await fetchMetricsApi(
      "/home-bundle",
      { account_id: "all", stages: METRICS_HOME_BUNDLE_STAGES },
      tid,
    );
    if (!data) {
      return null;
    }
    state.publicEarningBundleUi = {
      ready: true,
      loading: false,
      key,
      bundle: data,
      meta: data.meta || null,
    };
    return data;
  } finally {
    state.publicEarningBundleUi.loading = false;
  }
}

async function loadPublicEarningTabData(targetId) {
  const earningPane = document.querySelector('[data-profile-panel="earning"]');
  if (!earningPane) {
    return;
  }
  const pubColCount = PUBLIC_EARNING_VISIBLE_COL_INDICES.length;
  const tbody = document.getElementById("pubStockTableBody");
  if (tbody) {
    tbody.innerHTML = `<tr><td colspan="${pubColCount}"><p class="empty">数据加载中…</p></td></tr>`;
  }
  const bundle = await fetchPublicHomeBundleMetrics(targetId);
  if (!bundle) {
    if (tbody) {
      tbody.innerHTML = `<tr><td colspan="${pubColCount}"><p class="empty">加载失败</p></td></tr>`;
    }
    return;
  }
  mountPublicCommunityStockTableHead();
  paintPublicEarningFromBundle(bundle);
}

function renderPublicEarningProfileHtml(_d) {
  return getCommunityEarningPanelHtml();
}

function bindPublicProfileStageSelect() {}

function syncPublicProfileStageRow() {}

let analysisRouteHomeParent = null;
let communityTradeRecordsRouteParent = null;

const PRIVATE_TRADE_TABLE_HEAD_HTML = `
  <th class="trade-col-date">日期</th>
  <th class="trade-col-name">名称</th>
  <th class="trade-col-type">交易方向</th>
  <th class="trade-col-price num">价格</th>
  <th class="trade-col-qty num">数量</th>
  <th class="trade-col-amt num">发生金额</th>
  <th class="trade-col-account">股票账户</th>
`;

const PUBLIC_TRADE_TABLE_HEAD_HTML = `
  <th class="trade-col-date">日期</th>
  <th class="trade-col-name">名称</th>
  <th class="trade-col-type">交易方向</th>
  <th class="trade-col-price num">价格</th>
  <th class="trade-col-amt num stock-record-amt-th">
    <span class="stock-record-amt-th-inner">
      金额
      <span class="stock-rank-help-wrap stock-record-amt-help-wrap">
        <button type="button" class="stock-rank-help-btn" aria-expanded="false" aria-label="金额占比说明">?</button>
        <div class="stock-rank-help-bubble" role="tooltip">本次交易金额占当前总资产比例</div>
      </span>
    </span>
  </th>
  <th class="trade-col-account">股票账户</th>
`;

function isCommunityPublicTradeTableActive() {
  return (
    state.route === "community-profile" && (state.communityProfileTab || "earning") === "trade"
  );
}

function analysisMetricsUiActive() {
  return (
    state.route === "analysis" ||
    (state.route === "community-profile" && (state.communityProfileTab || "earning") === "analysis")
  );
}

function communityAnalysisTargetId() {
  if (state.route !== "community-profile") {
    return "";
  }
  return String(state.lastPublicProfileDetail?.userId || state.communityProfileUserId || "").trim();
}

function publicAnalysisBundleCacheKey(targetId) {
  const tid = String(targetId || "").trim();
  const stage = metricsStageFromAnalysis();
  const bench = state.benchmark === "none" ? "" : normalizeSymbol(state.benchmark);
  return `pub-analysis::${tid}::all::${stage}::${bench}`;
}

async function fetchPublicAnalysisBundleMetrics(targetId) {
  const tid = String(targetId || "").trim();
  if (!tid || !apiReady) {
    return null;
  }
  const stage = metricsStageFromAnalysis();
  const benchSym = state.benchmark === "none" ? "" : normalizeSymbol(state.benchmark);
  const key = publicAnalysisBundleCacheKey(tid);
  if (state.publicAnalysisBundleUi.ready && state.publicAnalysisBundleUi.key === key && state.publicAnalysisBundleUi.bundle) {
    return state.publicAnalysisBundleUi.bundle;
  }
  if (state.publicAnalysisBundleUi.loading) {
    return null;
  }
  state.publicAnalysisBundleUi.loading = true;
  try {
    const params = { account_id: "all", stage };
    if (benchSym) {
      params.symbol = benchSym;
    }
    const data = await fetchMetricsApi("/analysis-bundle", params, tid);
    if (!data) {
      return null;
    }
    state.publicAnalysisBundleUi = {
      ready: true,
      loading: false,
      key,
      bundle: data,
      meta: data.meta || null,
    };
    return data;
  } finally {
    state.publicAnalysisBundleUi.loading = false;
  }
}

function mountCommunityAnalysisRoutePane() {
  const mount = document.querySelector('[data-profile-panel="analysis"]');
  const pane = document.getElementById("route-analysis");
  if (!mount || !pane) {
    return;
  }
  if (!analysisRouteHomeParent) {
    analysisRouteHomeParent = pane.parentElement;
  }
  mount.querySelectorAll(".community-profile-analysis-loading").forEach((el) => el.remove());
  if (!mount.contains(pane)) {
    mount.appendChild(pane);
  }
  pane.classList.add("route-pane--community-analysis");
  const accWrap = pane.querySelector(".panel-head-account .head-select-wrap");
  if (accWrap) {
    accWrap.style.display = "none";
  }
}

function setPublicAnalysisPanelLoading(loading) {
  const mount = document.querySelector('[data-profile-panel="analysis"]');
  if (!mount || !loading) {
    return;
  }
  if (mount.querySelector("#route-analysis")) {
    return;
  }
  mount.innerHTML = `<p class="empty community-profile-analysis-loading">加载中…</p>`;
}

function unmountCommunityAnalysisRoutePane() {
  const pane = document.getElementById("route-analysis");
  if (!pane || !analysisRouteHomeParent) {
    return;
  }
  if (pane.parentElement !== analysisRouteHomeParent) {
    analysisRouteHomeParent.appendChild(pane);
  }
  pane.classList.remove("route-pane--community-analysis");
  const accWrap = pane.querySelector(".panel-head-account .head-select-wrap");
  if (accWrap) {
    accWrap.style.display = "";
  }
}

async function loadPublicAnalysisTabData(targetId) {
  const tid = String(targetId || "").trim();
  if (!tid) {
    return;
  }
  showRouteLoading("加载中…");
  setPublicAnalysisPanelLoading(true);
  try {
    mountCommunityAnalysisRoutePane();
    renderControls();
    const bundle = await fetchPublicAnalysisBundleMetrics(tid);
    if (!bundle) {
      clearAnalysisChartsToEmpty();
      if (analysisStockRankBody) {
        analysisStockRankBody.innerHTML = `<p class="empty">加载失败</p>`;
      }
      return;
    }
    const renderRequestId = ++analysisRenderRequestSeq;
    await paintAnalysisFromMetricsApi(renderRequestId, tid, { resetViewport: true, bundle });
  } finally {
    hideRouteLoading();
  }
}

async function openCommunityProfileAnalysisTab() {
  const tid = communityAnalysisTargetId();
  if (!tid) {
    return;
  }
  await loadPublicAnalysisTabData(tid);
}

function syncCommunityTradeRecordsTableHead(publicMode) {
  const table = tradeTableBody?.closest("table");
  const headRow = table?.querySelector("thead tr");
  if (!headRow) {
    return;
  }
  headRow.innerHTML = publicMode ? PUBLIC_TRADE_TABLE_HEAD_HTML : PRIVATE_TRADE_TABLE_HEAD_HTML;
  table?.classList.toggle("trade-table--ledger-6", publicMode);
}

function mountCommunityTradeRecordsPane() {
  const mount = document.querySelector('[data-profile-panel="trade"]');
  const pane = document.getElementById("route-trade-records");
  if (!mount || !pane) {
    return;
  }
  if (!communityTradeRecordsRouteParent) {
    communityTradeRecordsRouteParent = pane.parentElement;
  }
  mount.querySelectorAll(".community-profile-trade-loading").forEach((el) => el.remove());
  if (!mount.contains(pane)) {
    mount.appendChild(pane);
  }
  pane.classList.add("route-pane--community-trade");
  const subHead = pane.querySelector(".panel-head.mine-sub-head");
  if (subHead) {
    subHead.style.display = "none";
  }
  const filterRow = pane.querySelector(".trade-filter-row");
  if (filterRow) {
    filterRow.style.display = "none";
  }
  syncCommunityTradeRecordsTableHead(true);
}

function setPublicTradePanelLoading(loading) {
  const mount = document.querySelector('[data-profile-panel="trade"]');
  if (!mount || !loading) {
    return;
  }
  if (mount.querySelector("#route-trade-records")) {
    return;
  }
  mount.innerHTML = `<p class="empty community-profile-trade-loading">加载中…</p>`;
}

function unmountCommunityTradeRecordsPane() {
  const pane = document.getElementById("route-trade-records");
  if (!pane || !communityTradeRecordsRouteParent) {
    return;
  }
  if (pane.parentElement !== communityTradeRecordsRouteParent) {
    communityTradeRecordsRouteParent.appendChild(pane);
  }
  pane.classList.remove("route-pane--community-trade");
  const subHead = pane.querySelector(".panel-head.mine-sub-head");
  if (subHead) {
    subHead.style.display = "";
  }
  const filterRow = pane.querySelector(".trade-filter-row");
  if (filterRow) {
    filterRow.style.display = "";
  }
  syncCommunityTradeRecordsTableHead(false);
}

function renderCommunityProfilePageHtml(d) {
  const tab = state.communityProfileTab || "earning";
  const earningInner = renderPublicEarningProfileHtml(d);
  return `
    <div class="community-profile-tab-panel ${tab === "earning" ? "is-active" : ""}" data-profile-panel="earning">${earningInner}</div>
    <div class="community-profile-tab-panel ${tab === "analysis" ? "is-active" : ""}" data-profile-panel="analysis"></div>
    <div class="community-profile-tab-panel ${tab === "trade" ? "is-active" : ""}" data-profile-panel="trade"></div>
  `;
}

async function loadCommunityPublicTradesPage({ targetId, reset = false } = {}) {
  const uid = String(targetId || state.communityProfileUserId || "").trim();
  if (!uid || !apiReady || !sessionPhone) {
    state.communityPublicTrades = [];
    return;
  }
  if (state.route !== "community-profile" || state.communityProfileTab !== "trade") {
    return;
  }
  ensureTradeListScrollListener();
  if (reset || communityPublicTradesPager.targetId !== uid) {
    resetCommunityPublicTradesPager();
    communityPublicTradesPager.targetId = uid;
    state.communityPublicTrades = [];
  }
  if (communityPublicTradesPager.loading || (!communityPublicTradesPager.hasMore && communityPublicTradesPager.loaded)) {
    renderTradeTable();
    return;
  }
  const gen = communityPublicTradesPager.gen;
  communityPublicTradesPager.loading = true;
  renderTradeTable();
  try {
    const qs = publicTradesListQuery("", "all", communityPublicTradesPager.offset);
    const base = getApiBaseForFetch();
    const res = await apiFetch(`${base}/public/${encodeURIComponent(uid)}/trades?${qs}`, {
      cache: "no-store",
      timeoutMs: 25_000,
    });
    const body = await res.json().catch(() => ({}));
    if (gen !== communityPublicTradesPager.gen || state.route !== "community-profile") {
      return;
    }
    if (!res.ok || body?.ok !== true || !Array.isArray(body.data)) {
      communityPublicTradesPager.hasMore = false;
      return;
    }
    const rows = body.data.map(normalizeTrade);
    const seen = new Set(state.communityPublicTrades.map((t) => String(t.id)));
    for (const row of rows) {
      const id = String(row.id);
      if (!seen.has(id)) {
        state.communityPublicTrades.push(row);
        seen.add(id);
      }
    }
    const pagination = body.pagination || {};
    communityPublicTradesPager.offset =
      Number(pagination.offset ?? communityPublicTradesPager.offset) + rows.length;
    communityPublicTradesPager.hasMore = pagination.hasMore === true;
    communityPublicTradesPager.loaded = true;
    await hydrateSymbolNameMap(rows.map((row) => row?.symbol));
  } catch {
    if (gen === communityPublicTradesPager.gen) {
      communityPublicTradesPager.hasMore = false;
    }
  } finally {
    if (gen === communityPublicTradesPager.gen) {
      communityPublicTradesPager.loading = false;
      if (state.route === "community-profile" && state.communityProfileTab === "trade") {
        renderTradeTable();
      }
    }
  }
}

async function maybeLoadMoreCommunityPublicTradesPage() {
  if (
    state.route !== "community-profile" ||
    state.communityProfileTab !== "trade" ||
    !communityPublicTradesPager.hasMore ||
    communityPublicTradesPager.loading
  ) {
    return;
  }
  if (!isNearDocumentBottom()) {
    return;
  }
  await loadCommunityPublicTradesPage({ targetId: state.communityProfileUserId });
}

let lastCommunityDataKey = "";

async function loadCommunityPublicTrades(targetId) {
  setPublicTradePanelLoading(true);
  mountCommunityTradeRecordsPane();
  await loadCommunityPublicTradesPage({ targetId, reset: true });
}

async function loadCommunityProfileDetail() {
  if (!communityProfileBody || !state.communityProfileUserId) {
    return;
  }
  showRouteLoading("数据正在加载中");
  const uid = state.communityProfileUserId;
  communityProfileBody.innerHTML = `<p class="empty">加载中…</p>`;
  if (communityProfileFollowSlot) {
    communityProfileFollowSlot.innerHTML = "";
  }
  if (communityProfileTitle) {
    communityProfileTitle.textContent = "加载中…";
  }
  state.publicEarningBundleUi = {
    ready: false,
    loading: false,
    key: "",
    bundle: null,
    meta: null,
  };
  state.communityPublicTrades = [];
  resetCommunityPublicTradesPager();
  state.publicAnalysisBundleUi = {
    ready: false,
    loading: false,
    key: "",
    bundle: null,
    meta: null,
  };
  try {
    const base = getApiBaseForFetch();
    const enc = encodeURIComponent(uid);
    const profileRes = await apiFetch(`${base}/community/users/${enc}/profile`, { cache: "no-store" });
    const profileJson = await profileRes.json().catch(() => ({}));
    if (profileRes.status === 404) {
      communityProfileBody.innerHTML = `<p class="empty">用户未公开或不可见</p>`;
      return;
    }
    if (!profileRes.ok || !profileJson?.ok) {
      communityProfileBody.innerHTML = `<p class="empty">${escapeHtml(profileJson?.error || "加载失败")}</p>`;
      return;
    }
    const d = profileJson.data;
    state.lastPublicProfileDetail = d;

    if (communityProfileTitle) {
      communityProfileTitle.textContent = `${d.displayName || "用户"} 的持仓`;
    }
    if (communityProfileFollowSlot) {
      if (sessionUserId) {
        const uidEsc = escapeHtml(d.userId);
        const fu = d.following ? "已关注" : "关注";
        const cl = d.following ? "community-follow-btn is-on" : "community-follow-btn";
        communityProfileFollowSlot.innerHTML = `<button type="button" class="${cl}" data-user-id="${uidEsc}">${escapeHtml(
          fu,
        )}</button>`;
      } else {
        communityProfileFollowSlot.innerHTML = "";
      }
    }
    communityProfileBody.innerHTML = renderCommunityProfilePageHtml(d);
    mountPublicCommunityStockTableHead();
    renderRoute();
    const tab = state.communityProfileTab || "earning";
    if (tab === "earning") {
      void loadPublicEarningTabData(uid);
    } else if (tab === "trade") {
      void loadCommunityPublicTrades(uid);
    } else if (tab === "analysis") {
      void openCommunityProfileAnalysisTab();
    }
  } catch {
    communityProfileBody.innerHTML = `<p class="empty">网络错误</p>`;
  } finally {
    hideRouteLoading();
  }
}

function scheduleCommunityDataLoad() {
  if (!sessionPhone) {
    return;
  }
  if (state.appModule !== "community") {
    lastCommunityDataKey = "";
    return;
  }
  const uid = state.communityProfileUserId || "";
  const key = `${state.route}|${uid}`;
  if (key === lastCommunityDataKey) {
    return;
  }
  lastCommunityDataKey = key;
  if (state.route === "community-feed") {
    void loadCommunityFeed();
  } else if (state.route === "community-following") {
    void loadCommunityFollowing();
  } else if (state.route === "community-rank") {
    void loadCommunityLeaderboard();
  } else if (state.route === "community-profile" && state.communityProfileUserId) {
    void loadCommunityProfileDetail();
  }
}

async function quickSaveCommunityPublicFromHome() {
  if (!sessionPhone || !tradeHubCommunityPublicToggle) {
    return;
  }
  const want = tradeHubCommunityPublicToggle.checked;
  const revertTo = !want;
  tradeHubCommunityMsg?.classList.add("hidden");
  try {
    const r = await apiFetch(`${getApiBaseForFetch()}/me/community-profile`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        nickname: sessionProfile.nickname ?? null,
        communityPublic: want,
      }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j?.ok) {
      tradeHubCommunityPublicToggle.checked = revertTo;
      if (tradeHubCommunityMsg) {
        tradeHubCommunityMsg.textContent = j?.error || "保存失败";
        tradeHubCommunityMsg.classList.remove("hidden", "is-ok");
        tradeHubCommunityMsg.classList.add("is-error");
      }
      return;
    }
    sessionProfile.communityPublic = j.profile?.communityPublic !== false;
    lastCommunityDataKey = "";
    if (tradeHubCommunityMsg) {
      tradeHubCommunityMsg.textContent = "已更新";
      tradeHubCommunityMsg.classList.remove("hidden", "is-error");
      tradeHubCommunityMsg.classList.add("is-ok");
      window.setTimeout(() => tradeHubCommunityMsg.classList.add("hidden"), 1800);
    }
  } catch {
    tradeHubCommunityPublicToggle.checked = revertTo;
    if (tradeHubCommunityMsg) {
      tradeHubCommunityMsg.textContent = "网络错误";
      tradeHubCommunityMsg.classList.remove("hidden", "is-ok");
      tradeHubCommunityMsg.classList.add("is-error");
    }
  }
}

async function saveMineCommunityProfile() {
  if (!sessionPhone) {
    return;
  }
  const nickname = mineNicknameInput?.value?.trim() || "";
  const communityPublic = tradeHubCommunityPublicToggle?.checked ?? true;
  mineCommunityProfileMsg?.classList.add("hidden");
  try {
    const r = await apiFetch(`${getApiBaseForFetch()}/me/community-profile`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        nickname: nickname || null,
        communityPublic,
      }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j?.ok) {
      if (mineCommunityProfileMsg) {
        mineCommunityProfileMsg.textContent = j?.error || "保存失败";
        mineCommunityProfileMsg.classList.remove("hidden", "is-ok");
        mineCommunityProfileMsg.classList.add("is-error");
      }
      return;
    }
    sessionProfile.nickname = j.profile?.nickname ?? null;
    sessionProfile.communityPublic = j.profile?.communityPublic !== false;
    sessionProfile.displayName = String(j.profile?.displayName || "");
    if (mineNicknameInput) {
      mineNicknameInput.value = sessionProfile.nickname || "";
    }
    if (mineCommunityProfileMsg) {
      mineCommunityProfileMsg.textContent = "已保存";
      mineCommunityProfileMsg.classList.remove("hidden", "is-error");
      mineCommunityProfileMsg.classList.add("is-ok");
    }
    lastCommunityDataKey = "";
    renderMineSection();
  } catch {
    if (mineCommunityProfileMsg) {
      mineCommunityProfileMsg.textContent = "网络错误";
      mineCommunityProfileMsg.classList.remove("hidden", "is-ok");
      mineCommunityProfileMsg.classList.add("is-error");
    }
  }
}

function resetViewportScrollTop() {
  try {
    window.scrollTo({ top: 0, left: 0, behavior: "auto" });
  } catch {
    window.scrollTo(0, 0);
  }
  if (document.documentElement) {
    document.documentElement.scrollTop = 0;
  }
  if (document.body) {
    document.body.scrollTop = 0;
  }
}

function resetRoutePaneScrollTop(route) {
  const activePane = routePanes.find((pane) => String(pane.id || "").replace(/^route-/, "") === String(route || ""));
  if (!activePane) {
    return;
  }
  activePane.scrollTop = 0;
  activePane.querySelectorAll(".trade-table-wrap, .table-scroll, .community-profile-body, .stock-record-wrap").forEach((el) => {
    el.scrollTop = 0;
  });
}

function renderRoute() {
  const validRoutes = new Set([
    "earning",
    "analysis",
    "trade",
    "trade-records",
    "trade-cash",
    "trade-search",
    "holdings-ai",
    "mine",
    "mine-accounts",
    "mine-algo",
    "mine-community",
    "community-feed",
    "community-following",
    "community-rank",
    "community-ai",
    "community-profile",
    "stock-record",
  ]);
  if (!validRoutes.has(state.route)) {
    state.route = state.appModule === "community" ? "community-feed" : "earning";
  }
  const routeChanged = state.route !== lastRenderedRouteForScrollReset;
  if (
    routeChanged &&
    lastRenderedRouteForScrollReset === "community-profile" &&
    state.route !== "community-profile"
  ) {
    unmountCommunityAnalysisRoutePane();
    unmountCommunityTradeRecordsPane();
  }
  if (appHeaderTitle) {
    if (state.route === "trade-search") {
      appHeaderTitle.textContent = "搜索股票";
    } else if (state.route === "community-profile") {
      appHeaderTitle.textContent = "持仓收益";
    } else if (isMineRoute(state.route)) {
      appHeaderTitle.textContent = "我的";
    } else if (state.appModule === "community") {
      appHeaderTitle.textContent = "社区广场";
    } else {
      appHeaderTitle.textContent = "持仓收益";
    }
  }
  const secondaryToplessRoutes = new Set([
    "trade-search",
    "trade-records",
    "trade-cash",
    "mine-accounts",
    "mine-algo",
    "mine-community",
    "community-profile",
    "stock-record",
  ]);
  if (appTopBar) {
    appTopBar.style.display = secondaryToplessRoutes.has(state.route) ? "none" : "flex";
  }
  document.querySelectorAll(".bottom-tabs .bottom-tab-btn").forEach((button) => {
    const r = button.dataset.route;
    if (r) {
      const tradeTabActive =
        r === "trade" &&
        (state.route === "trade" ||
          state.route === "trade-records" ||
          state.route === "trade-cash" ||
          state.route === "trade-search");
      button.classList.toggle("active", r === state.route || tradeTabActive);
    }
  });
  document.querySelectorAll(".bottom-tabs--profile .bottom-tab-btn").forEach((button) => {
    const sub = button.dataset.profileSubtab;
    if (sub && state.route === "community-profile") {
      button.classList.toggle("active", sub === (state.communityProfileTab || "earning"));
    } else if (sub) {
      button.classList.remove("active");
    }
  });
  routePanes.forEach((pane) => {
    const id = String(pane.id || "").replace(/^route-/, "");
    pane.classList.toggle("active", id === state.route);
  });
  const hideMainBottom =
    state.route === "stock-record" ||
    state.route === "trade-search" ||
    state.route === "trade-records" ||
    state.route === "trade-cash" ||
    state.route === "community-profile" ||
    isMineRoute(state.route);
  document.querySelectorAll(".bottom-tabs").forEach((bar) => {
    const isProfile = bar.classList.contains("bottom-tabs--profile");
    const isCo = bar.classList.contains("bottom-tabs--community");
    const isHo = bar.classList.contains("bottom-tabs--holdings");
    if (isProfile) {
      bar.style.display = state.route === "community-profile" ? "grid" : "none";
      return;
    }
    let show = !hideMainBottom;
    if (show && isCo) {
      show = state.appModule === "community";
    }
    if (show && isHo) {
      show = state.appModule === "holdings";
    }
    bar.style.display = show ? "grid" : "none";
  });
  scheduleCommunityDataLoad();
  if (!browserHistorySeeded) {
    syncBrowserRouteHistory("replace");
    browserHistorySeeded = true;
  } else if (applyingBrowserRoutePopstate) {
    syncBrowserRouteHistory("replace");
  } else {
    syncBrowserRouteHistory("push");
  }
  if (routeChanged) {
    requestAnimationFrame(() => {
      resetViewportScrollTop();
      resetRoutePaneScrollTop(state.route);
    });
  }
  lastRenderedRouteForScrollReset = state.route;
}

function invalidateOverviewMetricsUi() {
  state.overviewMetricsUi.ready = false;
  state.overviewMetricsUi.loading = false;
  state.overviewMetricsUi.key = "";
  state.overviewMetricsUi.returns = null;
  state.overviewMetricsUi.assets = null;
  state.overviewMetricsUi.holdings = null;
  _overviewProfitInflight = null;
  invalidateHomeBundleInflight();
}


function overviewTradesLedgerKey() {
  let maxD = "";
  for (const t of state.trades) {
    const d = String(t.date || "").slice(0, 10);
    if (d > maxD) {
      maxD = d;
    }
  }
  return `${state.trades.length}|${state.cashTransfers.length}|${maxD}`;
}


function buildOverviewKpiEntries({
  totalAssets,
  marketValue,
  cash,
  stockRatio,
  cashRatio,
  principal,
  ratiosOnly,
}) {
  if (ratiosOnly) {
    return [
      { label: "现金占比", value: cashRatio },
      { label: "股票占比", value: stockRatio },
    ];
  }
  return [
    { label: "总资产", value: totalAssets },
    { label: "总市值", value: marketValue },
    { label: "现金", value: cash },
    { label: "本金", value: principal },
    { label: "现金占比", value: cashRatio },
    { label: "股票占比", value: stockRatio },
  ];
}

/**
 * 总览区 2 列栅格；条目数为奇数时末尾补空单元格。
 * @param {{ label: string, value: string }[]} entries
 */
function buildOverviewKpiGridInnerHtml(entries) {
  const cells = entries
    .map(
      (item) => `
      <article class="kpi-item">
        <p class="kpi-label">${escapeHtml(item.label)}</p>
        <p class="kpi-value">${escapeHtml(String(item.value))}</p>
      </article>
    `,
    )
    .join("");
  if (entries.length % 2 === 1) {
    return `${cells}<article class="kpi-item kpi-item--empty" aria-hidden="true"></article>`;
  }
  return cells;
}

function clearCanvasChart(canvas) {
  if (!canvas || typeof canvas.getContext !== "function") {
    return;
  }
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    return;
  }
  ctx.clearRect(0, 0, canvas.width, canvas.height);
}

function setOverviewProfitKpisDash() {
  if (!todayProfitMain || !monthProfitMain) {
    return;
  }
  todayProfitMain.textContent = "–";
  todayProfitMain.className = "profit-main";
  monthProfitMain.textContent = "–";
  monthProfitMain.className = "profit-main";
}

function setOverviewAssetsGridDash() {
  if (!overviewGrid) {
    return;
  }
  const dash = "–";
  overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml(
    buildOverviewKpiEntries({
      totalAssets: dash,
      marketValue: dash,
      cash: dash,
      stockRatio: dash,
      cashRatio: dash,
      principal: dash,
    }),
  );
}

function paintOverviewStockTableLoading(message = "数据加载中…") {
  if (!stockTableBody) {
    return;
  }
  stockTableBody.innerHTML = `<tr><td colspan="15"><p class="empty">${escapeHtml(message)}</p></td></tr>`;
}

let metricsRebuildPollTimer = null;

function stopMetricsRebuildPoll() {
  if (metricsRebuildPollTimer) {
    clearInterval(metricsRebuildPollTimer);
    metricsRebuildPollTimer = null;
  }
}

function applyMetricsRebuildStatusUi(rebuilding) {
  if (!quoteTime) {
    return;
  }
  if (rebuilding) {
    quoteTime.textContent = "历史指标重算中，请稍候…";
    quoteTime.classList.add("is-rebuilding");
    quoteTime.setAttribute("title", "成交或资金记录已变更，正在更新日终快照");
    return;
  }
  quoteTime.classList.remove("is-rebuilding");
  if (state.quoteTime) {
    const timeText = `${formatQuoteTimeForStatus(state.quoteTime)} 更新`;
    quoteTime.textContent = timeText;
    quoteTime.setAttribute(
      "title",
      state.marketDataDelayed
        ? "行情或指标延迟，数字为最近一次成功计算结果"
        : "数据来自 metrics 接口（昨日冻结 + 今日实时）",
    );
  }
}

function scheduleMetricsRebuildUiRefresh() {
  state.metricsRebuilding = true;
  applyMetricsRebuildStatusUi(true);
  stopMetricsRebuildPoll();
  if (!apiReady || !isEarningHomeRoute()) {
    return;
  }
  const aid = resolveValidAccountFilter(state.selectedAccountId) || "all";
  let polls = 0;
  const maxPolls = 120;
  metricsRebuildPollTimer = setInterval(() => {
    polls += 1;
    if (!apiReady || polls > maxPolls || !isEarningHomeRoute()) {
      if (polls > maxPolls || !isEarningHomeRoute()) {
        stopMetricsRebuildPoll();
        if (polls > maxPolls) {
          state.metricsRebuilding = false;
          applyMetricsRebuildStatusUi(false);
        }
      }
      return;
    }
    void (async () => {
      try {
        const bundle = await fetchHomeBundleMetrics(aid, { stages: METRICS_HOME_BUNDLE_STAGES });
        if (bundle?.meta?.rebuilding) {
          state.metricsRebuilding = true;
          applyMetricsRebuildStatusUi(true);
          return;
        }
        stopMetricsRebuildPoll();
        state.metricsRebuilding = false;
        invalidateOverviewMetricsUi();
        await refreshOverviewProfitRowFromSnapshots();
      } catch {
        /* keep polling */
      }
    })();
  }, 5000);
}

function applyOverviewMetricsMeta(meta) {
  if (!meta || typeof meta !== "object") {
    return;
  }
  if (meta.quoteTime) {
    state.quoteTime = String(meta.quoteTime);
  }
  state.marketDataDelayed = !!meta.delayed;
  state.marketDataDelaySource = meta.delayed ? "metrics-delayed" : "";
  if (meta.rebuilding) {
    state.metricsRebuilding = true;
    applyMetricsRebuildStatusUi(true);
    if (!metricsRebuildPollTimer) {
      scheduleMetricsRebuildUiRefresh();
    }
    return;
  }
  state.metricsRebuilding = false;
  if (!metricsRebuildPollTimer) {
    applyMetricsRebuildStatusUi(false);
  }
}

function bundleFmtText(val, fallback = "–") {
  const s = String(val ?? "").trim();
  return s || fallback;
}

function parseBundleSignedAmount(text) {
  let t = String(text ?? "").trim().replace(/,/g, "");
  if (!t || t === "–" || t === "—") {
    return NaN;
  }
  t = t.replace(/^¥\s*/, "");
  const neg = t.startsWith("-") || t.startsWith("−");
  const n = parseFloat(t.replace(/^[+−-]/, ""));
  if (!Number.isFinite(n)) {
    return NaN;
  }
  return neg ? -n : n;
}

function parseBundlePercentChart(text) {
  const r = parseBundlePercent(text);
  return Number.isFinite(r) ? r * 100 : 0;
}

function bundleSignedClass(text) {
  const t = String(text ?? "").trim();
  if (t.startsWith("+")) {
    return "up";
  }
  if (t.startsWith("-") || t.startsWith("−")) {
    return "down";
  }
  return "";
}

function paintOverviewFromMetricsBundle(returns, assets, holdings, stageKeyOrOpts) {
  let stageKey = metricsStageFromHome();
  let opts = {};
  if (typeof stageKeyOrOpts === "string") {
    stageKey = stageKeyOrOpts;
  } else if (stageKeyOrOpts && typeof stageKeyOrOpts === "object") {
    opts = stageKeyOrOpts;
    if (opts.stageKey) {
      stageKey = opts.stageKey;
    }
  }

  if (opts.mode === "public") {
    if (!returns?.stages || !assets || !holdings) {
      return false;
    }
    const stages = returns.stages;
    for (const def of PUBLIC_EARNING_STAGE_DEFS) {
      const el = document.querySelector(`[data-pub-stage="${def.key}"]`);
      if (el) {
        paintCommunityReturnTile(el, stages[def.key]?.rate);
      }
    }
    const grid = document.getElementById("pubOverviewGrid");
    if (grid) {
      grid.innerHTML = buildOverviewKpiGridInnerHtml(
        buildOverviewKpiEntries({
          stockRatio: bundleFmtText(assets.stockRatio),
          cashRatio: bundleFmtText(assets.cashRatio),
          ratiosOnly: true,
        }),
      );
    }
    const holdRows = holdings.rows || [];
    for (const row of holdRows) {
      const label = String(row.name || "").trim();
      if (label) {
        upsertNameMapEntry(row.symbol, label);
      }
    }
    mountPublicCommunityStockTableHead();
    paintStockTableFromMetricsRows(holdRows, getPublicStockTableCtx());
    applyPublicEarningMetaToUi(state.publicEarningBundleUi?.meta);
    return true;
  }

  if (!returns?.stages?.today || !returns?.stages?.[stageKey] || !assets || !holdings) {
    return false;
  }
  const today = returns.stages.today;
  const stage = returns.stages[stageKey];
  if (todayProfitMain && monthProfitMain) {
    todayProfitMain.innerHTML = metricHeadlineHtml(today.profit, today.rate);
    todayProfitMain.className = `profit-main ${bundleSignedClass(today.profit)}`;
    monthProfitMain.innerHTML = metricHeadlineHtml(stage.profit, stage.rate);
    monthProfitMain.className = `profit-main ${bundleSignedClass(stage.profit)}`;
  }
  if (overviewGrid) {
    overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml(
      buildOverviewKpiEntries({
        totalAssets: bundleFmtText(assets.totalAssets),
        marketValue: bundleFmtText(assets.marketValue),
        cash: bundleFmtText(assets.cash),
        stockRatio: bundleFmtText(assets.stockRatio),
        cashRatio: bundleFmtText(assets.cashRatio),
        principal: bundleFmtText(assets.principal),
      }),
    );
  }
  const holdRows = holdings.rows || [];
  for (const row of holdRows) {
    const label = String(row.name || "").trim();
    if (label) {
      upsertNameMapEntry(row.symbol, label);
    }
  }
  paintOverviewStockTableFromMetricsRows(holdRows);
  applyOverviewMetricsMeta(state.overviewMetricsUi?.meta);
  if (quoteTime) {
    const timeText = `${formatQuoteTimeForStatus(state.quoteTime)} 更新`;
    quoteTime.textContent = timeText;
    quoteTime.classList.toggle("is-delayed", !!state.marketDataDelayed);
    quoteTime.setAttribute(
      "title",
      state.marketDataDelayed
        ? "行情或指标延迟，数字为最近一次成功计算结果"
        : "数据来自 metrics 接口（昨日冻结 + 今日实时）",
    );
  }
  return true;
}

function setAnalysisSummariesDash() {
  if (analysisRateSummary) {
    analysisRateSummary.textContent =
      state.benchmark === "none" ? "我的收益率 –" : "我的 – / 基准 – / 对比 –";
  }
  if (analysisProfitSummary) {
    analysisProfitSummary.textContent = "累计收益 –";
  }
}

function clearAnalysisChartsToEmpty() {
  clearCanvasChart(analysisRateChart);
  clearCanvasChart(analysisProfitChart);
  clearCanvasChart(analysisAssetChart);
}

function renderOverviewAndStockTable() {
  if (!isEarningHomeRoute()) {
    return;
  }
  const aid = String(state.selectedAccountId === "all" ? "all" : resolveValidAccountFilter(state.selectedAccountId));
  const stageKey = metricsStageFromHome();
  const metricsKey = overviewMetricsBundleCacheKey(aid);

  if (!apiReady) {
    setOverviewProfitKpisDash();
    setOverviewAssetsGridDash();
    paintOverviewStockTableLoading("请先登录后查看持仓。");
    return;
  }

  if (
    state.overviewMetricsUi.ready &&
    state.overviewMetricsUi.key === metricsKey &&
    state.overviewMetricsUi.returns &&
    state.overviewMetricsUi.assets &&
    state.overviewMetricsUi.holdings
  ) {
    void paintOverviewFromMetricsBundle(
      state.overviewMetricsUi.returns,
      state.overviewMetricsUi.assets,
      state.overviewMetricsUi.holdings,
      stageKey,
    );
    return;
  }

  if (state.overviewMetricsUi.loading || _overviewProfitInflight?.promise) {
    setOverviewProfitKpisDash();
    setOverviewAssetsGridDash();
    paintOverviewStockTableLoading("数据加载中…");
    return;
  }

  setOverviewProfitKpisDash();
  setOverviewAssetsGridDash();
  paintOverviewStockTableLoading("数据加载中…");
  void refreshOverviewProfitRowFromSnapshots();
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

/** 分析页「年初至今」：本年 1 月 1 日起（analysisPreset=ytd，兼容旧数据 rangeDays=365）。 */
function isAnalysisYtdPreset() {
  return state.analysisRangeMode === "preset" && state.analysisPreset === "ytd";
}

function ytdStartDateKey() {
  const y = new Date();
  return toDateKey(new Date(y.getFullYear(), 0, 1));
}

function monthToDateStartKey() {
  const y = new Date();
  return toDateKey(new Date(y.getFullYear(), y.getMonth(), 1));
}

function isAnalysisMtdPreset() {
  return state.analysisRangeMode === "preset" && state.analysisPreset === "mtd";
}

function getDefaultAnalysisStartDate() {
  if (isAnalysisYtdPreset()) {
    return ytdStartDateKey();
  }
  if (isAnalysisMtdPreset()) {
    return monthToDateStartKey();
  }
  const dt = new Date();
  dt.setDate(dt.getDate() - Math.max(state.rangeDays - 1, 0));
  return toDateKey(dt);
}

/**
 * 总览区展示币种：单账户跟随该账户记账币种（原币）；「全部账户」时统一人民币。
 */
function getOverviewBookCurrency() {
  const o = state._overviewBookCurrencyOverride;
  if (o) {
    const c = String(o).toUpperCase();
    if (c === "USD" || c === "HKD" || c === "CNY") {
      return c;
    }
  }
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

/** 与总览 KPI 同账本币种，带正负号（个股表金额列） */
function formatSignedMoneyInBook(value, bookCcy) {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  const sign = safe > 0 ? "+" : safe < 0 ? "-" : "";
  const abs = Math.abs(safe).toFixed(2);
  const core = `${sign}${abs}`;
  const c = String(bookCcy || "CNY").toUpperCase();
  if (c === "USD") {
    return `$${core}`;
  }
  if (c === "HKD") {
    return `HK$${core}`;
  }
  return core;
}

function formatStockTableMoneyBook(row, valueNative, bookCcy) {
  const v = nativeToOverviewBook(row, valueNative, bookCcy);
  return formatSignedMoneyInBook(v, bookCcy);
}

function formatStockTableMarketValueBook(row, bookCcy) {
  const mvNative = Number.isFinite(Number(row.marketValueNative)) ? Number(row.marketValueNative) : 0;
  const v = nativeToOverviewBook(row, mvNative, bookCcy);
  return formatOverviewPlainMoney(v, bookCcy);
}

function parseQuoteTimeParts(timeStr) {
  if (!timeStr || typeof timeStr !== "string") {
    return null;
  }
  const raw = String(timeStr).trim();
  if (!raw || raw === "--") {
    return null;
  }
  const compact = raw.replace(/\D/g, "");
  if (compact.length >= 14) {
    return {
      year: compact.slice(0, 4),
      month: compact.slice(4, 6),
      day: compact.slice(6, 8),
      hour: compact.slice(8, 10),
      minute: compact.slice(10, 12),
      second: compact.slice(12, 14),
    };
  }
  const iso = /^(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})(?:\D+(\d{1,2})[:：](\d{1,2})(?:[:：](\d{1,2}))?)?/.exec(raw);
  if (iso) {
    return {
      year: iso[1],
      month: String(Number(iso[2])).padStart(2, "0"),
      day: String(Number(iso[3])).padStart(2, "0"),
      hour: String(Number(iso[4] || 0)).padStart(2, "0"),
      minute: String(Number(iso[5] || 0)).padStart(2, "0"),
      second: String(Number(iso[6] || 0)).padStart(2, "0"),
    };
  }
  return null;
}

function quoteTimeSortKey(timeStr) {
  const parts = parseQuoteTimeParts(timeStr);
  if (!parts) {
    return 0;
  }
  return Number(`${parts.year}${parts.month}${parts.day}${parts.hour}${parts.minute}${parts.second}`) || 0;
}

function pickLatestQuoteTime(times) {
  const list = Array.isArray(times) ? times : [];
  let best = "";
  let bestKey = 0;
  for (const item of list) {
    const current = String(item || "").trim();
    const key = quoteTimeSortKey(current);
    if (key > bestKey) {
      best = current;
      bestKey = key;
    }
  }
  return best || "--";
}

function formatQuoteTimeForStatus(timeStr) {
  const parts = parseQuoteTimeParts(timeStr);
  if (!parts) {
    return "--";
  }
  return `${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
}

/**
 * 从腾讯实时行情时间串解析日历日期（YYYY-MM-DD）。常见格式 YYYYMMDDHHMMSS。
 * 无法解析时返回 null。
 */
function parseQuoteTimeToDateKey(timeStr) {
  const parts = parseQuoteTimeParts(timeStr);
  if (!parts || !parts.year) {
    return null;
  }
  return `${parts.year}-${parts.month}-${parts.day}`;
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
    (quote && quote.marketDate) ||
    (quote && quote.quoteDate) ||
    (quote && parseQuoteTimeToDateKey(quote.rawTime)) ||
    (quote && parseQuoteTimeToDateKey(quote.time)) ||
    null;
  if (!quoteKey) {
    return false;
  }
  return quoteKey === tradingKey;
}

function getPositionDayTradeContext(symbol, dateKey, trades = state.trades) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const sym = normalizeSymbol(symbol);
  const dk = String(dateKey || "").slice(0, 10);
  const symbolTrades = tradeList
    .filter((trade) => normalizeSymbol(trade.symbol) === sym)
    .sort(sortTradeAsc);
  let startQuantity = 0;
  let endQuantity = 0;
  let dayFlowNative = 0;
  for (const trade of symbolTrades) {
    const d = String(trade.date || "").slice(0, 10);
    const deltaQty = trade.side === "buy" ? trade.quantity : -trade.quantity;
    if (d < dk) {
      startQuantity += deltaQty;
    }
    if (d <= dk) {
      endQuantity += deltaQty;
    }
    if (d === dk) {
      dayFlowNative += signedAmount(trade);
    }
  }
  return { startQuantity, endQuantity, dayFlowNative };
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

/** 阶段起点：期初持股、期初参考收盘、区间内现金流（原币） */
function computePositionStageStartState(position, stageRange, trades) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const firstTradeDate = tradeList.length
    ? [...tradeList].sort(sortTradeAsc)[0].date
    : toDateKey(new Date());
  const startKey = getStageStartKey(stageRange, firstTradeDate);
  const symbolTrades = tradeList
    .filter((trade) => trade.symbol === position.symbol)
    .sort(sortTradeAsc);
  if (!symbolTrades.length) {
    return {
      startKey,
      startQuantity: 0,
      startClose: validNumber(position.prevClose, 0),
      stageFlowNative: 0,
    };
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
  return { startKey, startQuantity, startClose, stageFlowNative };
}

function computePositionStageProfit(position, stageRange, trades) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const firstTradeDate = tradeList.length
    ? [...tradeList].sort(sortTradeAsc)[0].date
    : toDateKey(new Date());
  const startKey = getStageStartKey(stageRange, firstTradeDate);
  const endKey = toDateKey(new Date());
  return computePositionProfitInDateRange(position, startKey, endKey, tradeList);
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

/** 区间期末收盘价：最后一根 day ≤ dateKey 的 K 线收盘；含当日 bar；若 dateKey 为今日则可用行情现价。 */
function getSymbolCloseOnOrBeforeKey(symbol, dateKey, fallbackPrice) {
  const kline = getKlineBySymbol(symbol);
  for (let i = kline.length - 1; i >= 0; i -= 1) {
    const item = kline[i];
    if (item.day <= dateKey && Number.isFinite(Number(item.close))) {
      return Number(item.close);
    }
  }
  const todayKey = toDateKey(new Date());
  if (dateKey >= todayKey) {
    const quote = getQuoteBySymbol(symbol);
    return validNumber(quote.current, quote.prevClose, fallbackPrice, 0);
  }
  return validNumber(fallbackPrice, 0);
}

/**
 * 与 computePositionStageProfit 同口径：区间 [startKey, endKey] 内标的盈亏（原币）。
 * 期初用 startKey 之前持仓 × startKey 前一日收盘；期末用 endKey 日及以前持仓 × 期末价；区间内交易金额为现金流。
 */
function computePositionProfitInDateRange(position, startKey, endKey, trades) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const symbolTrades = tradeList.filter((t) => t.symbol === position.symbol).sort(sortTradeAsc);
  if (!symbolTrades.length) {
    return 0;
  }
  let startQuantity = 0;
  let endQuantity = 0;
  let stageFlowNative = 0;
  for (const trade of symbolTrades) {
    const delta = trade.side === "buy" ? trade.quantity : -trade.quantity;
    if (trade.date < startKey) {
      startQuantity += delta;
    }
    if (trade.date <= endKey) {
      endQuantity += delta;
    }
    if (trade.date >= startKey && trade.date <= endKey) {
      stageFlowNative += signedAmount(trade);
    }
  }
  const startClose = getSymbolCloseBeforeDate(position.symbol, startKey, position.prevClose);
  const endClose = getSymbolCloseOnOrBeforeKey(
    position.symbol,
    endKey,
    validNumber(position.currentPrice, position.prevClose)
  );
  const startMv = startQuantity * startClose;
  const endMv = endQuantity * endClose;
  return endMv - startMv - stageFlowNative;
}

function profitNativeToAnalysisCny(position, nativeProfit) {
  const n = Number.isFinite(Number(nativeProfit)) ? Number(nativeProfit) : 0;
  if (position.currency === "CNY" || position.market === "A股") {
    return n;
  }
  return n * (validNumber(position.fxRate, 1) || 1);
}

function addCalendarDaysToDateKey(dateKey, deltaDays) {
  const d = new Date(`${dateKey}T12:00:00`);
  if (Number.isNaN(d.getTime())) {
    return dateKey;
  }
  d.setDate(d.getDate() + deltaDays);
  return toDateKey(d);
}

/**
 * 分析周期 [periodStart, periodEnd] 内：按自然日日终持仓大于 0 连成连续段。
 */
function collectHoldingSegmentsInPeriod(symbolTrades, periodStart, periodEnd) {
  let qty = 0;
  for (const t of symbolTrades) {
    if (t.date < periodStart) {
      qty += t.side === "buy" ? t.quantity : -t.quantity;
    }
  }
  const startDate = new Date(`${periodStart}T12:00:00`);
  const endDate = new Date(`${periodEnd}T12:00:00`);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime()) || startDate > endDate) {
    return [];
  }
  const segments = [];
  let runStart = null;
  const cursor = new Date(startDate);
  while (cursor <= endDate) {
    const dk = toDateKey(cursor);
    for (const t of symbolTrades) {
      if (t.date === dk) {
        qty += t.side === "buy" ? t.quantity : -t.quantity;
      }
    }
    if (qty > 1e-6) {
      if (runStart === null) {
        runStart = dk;
      }
    } else if (runStart !== null) {
      const endSeg = addCalendarDaysToDateKey(dk, -1);
      if (endSeg >= runStart) {
        segments.push({ start: runStart, end: endSeg });
      }
      runStart = null;
    }
    cursor.setDate(cursor.getDate() + 1);
  }
  if (runStart !== null) {
    segments.push({ start: runStart, end: periodEnd });
  }
  return segments;
}

/**
 * 持仓区间展示：仅日期～日期；多段时每段后附（该段持仓天数、该段个股涨跌幅、该段区间收益¥），口径与排行表同列一致。
 */
function formatHoldingSegmentsLabel(position, symbolTrades, periodStart, periodEnd, trades) {
  const segments = collectHoldingSegmentsInPeriod(symbolTrades, periodStart, periodEnd);
  if (!segments.length) {
    return "";
  }
  if (segments.length === 1) {
    const s = segments[0];
    return `${s.start}～${s.end}`;
  }
  return segments
    .map((s) => {
      const m = computePositionPeriodMetrics(position, s.start, s.end, trades);
      const profitCny = profitNativeToAnalysisCny(position, m.profitNative);
      const pctStr = formatPercent(m.pxChange);
      const profitStr = `${profitCny >= 0 ? "+" : ""}¥${formatNumber(profitCny, 2)}`;
      return `${s.start}～${s.end}（${m.heldDays}天，${pctStr}，${profitStr}）`;
    })
    .join("\n");
}

/** 他人主页排行「持仓区间」多段时不展示区间收益金额 */
function formatHoldingSegmentsLabelPublic(position, symbolTrades, periodStart, periodEnd, trades) {
  const segments = collectHoldingSegmentsInPeriod(symbolTrades, periodStart, periodEnd);
  if (!segments.length) {
    return "";
  }
  if (segments.length === 1) {
    const s = segments[0];
    return `${s.start}～${s.end}`;
  }
  return segments
    .map((s) => {
      const m = computePositionPeriodMetrics(position, s.start, s.end, trades);
      const pctStr = formatPercent(m.pxChange);
      return `${s.start}～${s.end}（${m.heldDays}天，股价${pctStr}）`;
    })
    .join("\n");
}

/** 区间内自然日，按日终持仓大于 0 计一天（与区间内交易顺序一致）。 */
function countHeldDaysInRange(symbolTrades, startKey, endKey) {
  let qty = 0;
  for (const t of symbolTrades) {
    if (t.date < startKey) {
      qty += t.side === "buy" ? t.quantity : -t.quantity;
    }
  }
  const startDate = new Date(`${startKey}T12:00:00`);
  const endDate = new Date(`${endKey}T12:00:00`);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime()) || startDate > endDate) {
    return 0;
  }
  let held = 0;
  const cursor = new Date(startDate);
  while (cursor <= endDate) {
    const dk = toDateKey(cursor);
    for (const t of symbolTrades) {
      if (t.date === dk) {
        qty += t.side === "buy" ? t.quantity : -t.quantity;
      }
    }
    if (qty > 1e-6) {
      held += 1;
    }
    cursor.setDate(cursor.getDate() + 1);
  }
  return held;
}

/**
 * 区间内：盈亏（原币）、个股涨跌幅（区间内首笔买入成交价→期末价；无区间内买入则退回前收→期末）、持仓天数。
 */
function computePositionPeriodMetrics(position, startKey, endKey, trades) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const symbolTrades = tradeList.filter((t) => t.symbol === position.symbol).sort(sortTradeAsc);
  if (!symbolTrades.length) {
    return {
      profitNative: 0,
      pxChange: 0,
      heldDays: 0,
    };
  }
  const profitNative = computePositionProfitInDateRange(position, startKey, endKey, tradeList);
  const endClose = getSymbolCloseOnOrBeforeKey(
    position.symbol,
    endKey,
    validNumber(position.currentPrice, position.prevClose)
  );
  let entryPx = 0;
  for (const trade of symbolTrades) {
    if (trade.date < startKey) {
      continue;
    }
    if (trade.date > endKey) {
      break;
    }
    if (trade.side === "buy" && validNumber(trade.price, 0) > 0) {
      entryPx = Number(trade.price);
      break;
    }
  }
  const startPxForStockMove =
    entryPx > 1e-9 ? entryPx : getSymbolCloseBeforeDate(position.symbol, startKey, position.prevClose);
  const pxChange = startPxForStockMove > 1e-9 ? endClose / startPxForStockMove - 1 : 0;
  const heldDays = countHeldDaysInRange(symbolTrades, startKey, endKey);
  return { profitNative, pxChange, heldDays };
}

function bundleDisplayTone(displayText) {
  const s = String(displayText || "").trim();
  if (!s || s === "—") {
    return "";
  }
  if (s.startsWith("+")) {
    return "up";
  }
  if (s.startsWith("-")) {
    return "down";
  }
  return "";
}

function mapStockRankBundleRow(row) {
  const name = String(row.name || "").trim() || String(row.symbol || "").trim();
  const profitText = String(row.profit ?? row.profitCny ?? "").trim() || "—";
  const pxText = String(row.pxChange ?? row.pxChangeDisplay ?? "").trim() || "—";
  const shareText = String(row.profitShare ?? "").trim() || "—";
  const daysText = String(row.heldDays ?? "").trim() || "—";
  const profitTone = String(row.profitTone || "").trim();
  const pxTone = String(row.pxTone || "").trim();
  const profitSort = parseBundleSignedAmount(profitText);
  return {
    rank: Number(row.rank) || 0,
    symbol: row.symbol,
    name,
    holdIntervalsLabel: String(row.holdIntervalsLabel || ""),
    profit: profitText,
    pxChange: pxText,
    heldDays: daysText,
    profitShare: shareText,
    profitTone: profitTone || bundleDisplayTone(profitText),
    pxTone: pxTone || bundleDisplayTone(pxText),
    profitSort: Number.isFinite(profitSort) ? profitSort : 0,
  };
}

function buildAnalysisStockRankHtml(rows, rankOpts = {}) {
  const publicRank = rankOpts.publicStockRankLayout === true;
  const hideProfitCol = publicRank || rankOpts.hideProfitColumn === true;
  if (!rows.length) {
    return `<p class="empty">本分析周期内无持仓的标的。</p>`;
  }
  const profitTh = hideProfitCol
    ? ""
    : `<span class="col-profit" role="columnheader">区间收益</span>`;
  const profitShareTh = `<span class="col-profit-share col-profit-share-head" role="columnheader">收益占比</span>`;

  return `
    <div class="analysis-stock-rank-table${publicRank ? " analysis-stock-rank-table--public" : ""}" role="table" aria-label="个股收益排行">
      <div class="analysis-stock-rank-head" role="row">
        <span class="col-rank" role="columnheader">#</span>
        <span class="col-name" role="columnheader">名称</span>
        ${profitTh}
        ${profitShareTh}
        <span class="col-px col-with-help stock-rank-help-wrap" role="columnheader">
          <span class="col-th-label">个股涨跌幅</span>
          <button type="button" class="stock-rank-help-btn" aria-expanded="false" aria-label="个股涨跌幅说明">?</button>
          <div class="stock-rank-help-bubble" role="tooltip">
            有效持仓区间内，起点取时间顺序第一笔买入成交价，终点取区间末日收盘（含今日则用现价），涨跌幅为终点÷起点−1；区间内无买入则起点为区间首日前一交易日收盘。多笔买入仅首笔价，非摊薄成本。
          </div>
        </span>
        <span class="col-days col-with-help stock-rank-help-wrap" role="columnheader">
          <span class="col-th-label">持仓天数</span>
          <button type="button" class="stock-rank-help-btn" aria-expanded="false" aria-label="持仓天数说明">?</button>
          <div class="stock-rank-help-bubble" role="tooltip">
            在有效区间内按自然日逐日统计：当日全部成交完成后，若日终持股大于零则计一天并累加。清仓后再买回会分段，总天数与「持仓区间」各段有仓日之和一致。
          </div>
        </span>
        <span class="col-hold-interval" role="columnheader">持仓区间</span>
      </div>
      ${rows
        .map((row, idx) => {
          const cls = row.profitTone || "";
          const pCls = row.pxTone || "";
          const profitShareCell = `<span class="col-profit-share ${cls}" role="cell">${escapeHtml(
            row.profitShare,
          )}</span>`;
          const profitCell = hideProfitCol
            ? ""
            : `<span class="col-profit ${cls}" role="cell">${escapeHtml(row.profit)}</span>`;
          const rankNum = publicRank && row.rank ? row.rank : idx + 1;
          const rankCode = formatSymbolForDisplay(row.symbol);
          const rankCodeHtml = rankCode
            ? `<span class="rank-code">${escapeHtml(rankCode)}</span>`
            : "";
          return `
        <div class="analysis-stock-rank-row" role="row">
          <span class="col-rank" role="cell">${rankNum}</span>
          <div class="col-name" role="cell">
            <strong>${escapeHtml(getDisplayName(row.symbol, row.name))}</strong>
            ${rankCodeHtml}
          </div>
          ${profitCell}
          ${profitShareCell}
          <span class="col-px ${pCls}" role="cell">${escapeHtml(row.pxChange)}</span>
          <span class="col-days" role="cell">${escapeHtml(row.heldDays)}</span>
          <span class="col-hold-interval" role="cell">${escapeHtml(row.holdIntervalsLabel)}</span>
        </div>`;
        })
        .join("")}
    </div>`;
}

function paintStockRankFromBundle(rankPayload, targetBody, rankOpts = {}) {
  if (!targetBody) {
    return;
  }
  const rows = (Array.isArray(rankPayload?.rows) ? rankPayload.rows : []).map(mapStockRankBundleRow);
  if (!rows.length) {
    targetBody.innerHTML = `<p class="empty">暂无分析区间数据。</p>`;
    return;
  }
  if (rankOpts.publicStockRankLayout === true) {
    rows.sort((a, b) => (Number(a.rank) || 0) - (Number(b.rank) || 0));
  } else {
    rows.sort((a, b) => b.profitSort - a.profitSort);
  }
  targetBody.innerHTML = buildAnalysisStockRankHtml(rows, rankOpts);
}

function syncTradePanelTabUi() {
  const isCash = state.tradePanelTab === "cash";
  tradeSubtabTrades?.classList.toggle("is-active", !isCash);
  tradeSubtabCash?.classList.toggle("is-active", isCash);
  tradeSubtabTrades?.setAttribute("aria-selected", !isCash ? "true" : "false");
  tradeSubtabCash?.setAttribute("aria-selected", isCash ? "true" : "false");
  tradeRecordsPanel?.classList.toggle("hidden", isCash);
  cashRecordsPanel?.classList.toggle("hidden", !isCash);
}

function openNewCashTransferDialog() {
  state.editingCashTransferId = null;
  if (cashTransferDialogTitle) {
    cashTransferDialogTitle.textContent = "新增资金记录";
  }
  if (cashTransferSubmitBtn) {
    cashTransferSubmitBtn.textContent = "保存";
  }
  cashTransferDeleteBtn?.classList.add("hidden");
  cashTransferForm?.reset();
  if (cashTransferDate) {
    cashTransferDate.value = toDateKey(new Date());
  }
  if (cashTransferDirection) {
    cashTransferDirection.value = "in";
  }
  syncAccountSelectOptions();
  if (cashTransferAccount) {
    cashTransferAccount.value = resolveTradeFormDefaultAccountId();
  }
  cashTransferDialog?.showModal();
}

function openEditCashTransferDialog(rawId) {
  const r = state.cashTransfers.find((x) => String(x.id) === String(rawId));
  if (!r) {
    return;
  }
  state.editingCashTransferId = r.id;
  if (cashTransferDialogTitle) {
    cashTransferDialogTitle.textContent = "编辑资金记录";
  }
  if (cashTransferSubmitBtn) {
    cashTransferSubmitBtn.textContent = "保存";
  }
  cashTransferDeleteBtn?.classList.remove("hidden");
  syncAccountSelectOptions();
  if (cashTransferAccount) {
    cashTransferAccount.value = r.accountId;
  }
  if (cashTransferDate) {
    cashTransferDate.value = r.date;
  }
  if (cashTransferDirection) {
    cashTransferDirection.value = r.direction;
  }
  if (cashTransferAmount) {
    cashTransferAmount.value = String(r.amount);
  }
  if (cashTransferNote) {
    cashTransferNote.value = r.note;
  }
  cashTransferDialog?.showModal();
}

/** 交易/资金/个股记录：备注在数据行下方展示（无备注则不占行）。 */
function tradeRecordNoteSubrowHtml(note, colspan, rowAttrs = {}, opts = {}) {
  const text = String(note || "").trim();
  if (!text) {
    return "";
  }
  const clickable = opts.clickable !== false;
  const clickableClass = clickable ? " trade-row--clickable" : "";
  const attrs = Object.entries(rowAttrs)
    .filter(([, v]) => v != null && String(v).trim() !== "")
    .map(([k, v]) => `${k}="${escapeHtml(String(v))}"`)
    .join(" ");
  const attrStr = attrs ? ` ${attrs}` : "";
  return `
    <tr class="trade-note-subrow${clickableClass}"${attrStr}>
      <td colspan="${colspan}">
        <div class="trade-record-note-wrap">
          <p class="trade-record-note"><span class="trade-record-note-label">备注：</span><span class="trade-record-note-text">${escapeHtml(text)}</span></p>
        </div>
      </td>
    </tr>`;
}

/** 离开「交易」页后不再重绘该表；若不清理 tbody，大列表会一直占内存，切页时易触发移动端渲染进程崩溃（Chrome 错误代码 5）。 */
function clearHoldingsTradePaneDomIfHiddenRoute() {
  if (
    state.route === "trade" ||
    state.route === "trade-search" ||
    state.route === "trade-records" ||
    state.route === "trade-cash" ||
    state.route === "community-profile"
  ) {
    return;
  }
  resetTradeListPager();
  resetCashListPager();
  if (tradeTableBody) {
    tradeTableBody.innerHTML = "";
  }
  if (cashTransferTableBody) {
    cashTransferTableBody.innerHTML = "";
  }
}

function renderCashTransferTable() {
  if (state.route === "community-profile" || state.route === "stock-record" || state.route !== "trade-cash") {
    return;
  }
  if (!cashTransferTableBody) {
    return;
  }
  if (cashListPager.loading && !state.cashTransfers.length) {
    cashTransferTableBody.innerHTML = tradeListLoadingRowHtml(4);
    return;
  }
  const rows = getFilteredCashTransfers(state.tradeFilterAccountId);
  if (!rows.length) {
    cashTransferTableBody.innerHTML = cashListPager.loading
      ? tradeListLoadingRowHtml(4)
      : `
      <tr>
        <td colspan="4"><p class="empty">暂无资金记录，点击「新增资金记录」添加银证转账。</p></td>
      </tr>
    `;
    return;
  }
  const sorted = [...rows].sort((a, b) => {
    const c = String(b.date).localeCompare(String(a.date));
    return c !== 0 ? c : (b.createdAt || 0) - (a.createdAt || 0);
  });
  let html = sorted
    .map((row) => {
      const acc = getAccountById(row.accountId);
      const dirLabel = row.direction === "out" ? "银证转出" : "银证转入";
      const sign = row.direction === "in" ? "+" : "-";
      const id = escapeHtml(String(row.id));
      return `
        <tr class="cash-transfer-row trade-row--clickable" data-cash-id="${id}">
          <td>${String(row.date).replace(/-/g, "/")}</td>
          <td>${escapeHtml(acc.name || row.accountId)}</td>
          <td>${dirLabel}</td>
          <td class="num ${row.direction === "in" ? "up" : "down"}">${sign}${formatNumber(row.amount, 2)}</td>
        </tr>
        ${tradeRecordNoteSubrowHtml(row.note, 4, { "data-cash-id": row.id })}
      `;
    })
    .join("");
  if (cashListPager.loading) {
    html += tradeListLoadingRowHtml(4);
  }
  cashTransferTableBody.innerHTML = html;
}

function buildTradeRecordRowHtml(trade, ctx) {
  const id = escapeHtml(String(trade.id));
  const clickableClass = ctx.clickable ? " trade-row--clickable" : "";
  const noteOpts = ctx.clickable ? {} : { clickable: false };
  const accLabel =
    ctx.mode === "publicCommunity"
      ? formatTradeAccountCellHtml(trade, ctx.publicDetail)
      : escapeHtml(getAccountById(trade.accountId).name || trade.accountId || "default");
  let amountCells;
  if (ctx.mode === "publicCommunity") {
    const share = publicTradeAmountShare(trade);
    const shareStr = share != null && Number.isFinite(share) ? formatPercent(share) : "—";
    amountCells = `<td class="trade-col-amt num">${shareStr}</td>`;
  } else {
    amountCells = `
          <td class="trade-col-qty num">${formatNumber(trade.quantity, 0)}</td>
          <td class="trade-col-amt num ${trade.side === "buy" ? "down" : "up"}">${
            trade.side === "buy" ? "-" : "+"
          }${formatNumber(trade.amount, 2)}</td>`;
  }
  return `
        <tr class="trade-row${clickableClass}" data-record-id="${id}">
          <td class="trade-col-date">${trade.date.replace(/-/g, "/")}</td>
          <td class="trade-col-name">${escapeHtml(getDisplayName(trade.symbol, trade.name))}</td>
          <td class="trade-col-type type-cell">${tradeDirectionCellLabel(trade)}</td>
          <td class="trade-col-price num">${formatNumber(trade.price, 2)}</td>
          ${amountCells}
          <td class="trade-col-account trade-account-cell">${accLabel}</td>
        </tr>
        ${tradeRecordNoteSubrowHtml(trade.note, ctx.colspan, { "data-record-id": trade.id }, noteOpts)}`;
}

function renderTradeTable() {
  if (state.route === "stock-record") {
    return;
  }
  if (isCommunityPublicTradeTableActive()) {
    if (!tradeTableBody) {
      return;
    }
    const noteColspan = 6;
    const list = Array.isArray(state.communityPublicTrades)
      ? [...state.communityPublicTrades].sort(sortTradeDesc)
      : [];
    if (!list.length && !communityPublicTradesPager.loaded) {
      tradeTableBody.innerHTML = `
      <tr>
        <td colspan="${noteColspan}"><p class="empty">${communityPublicTradesPager.loading ? "加载中…" : "暂无交易数据"}</p></td>
      </tr>
    `;
      return;
    }
    if (!list.length) {
      tradeTableBody.innerHTML = `
      <tr>
        <td colspan="${noteColspan}"><p class="empty">暂无交易记录</p></td>
      </tr>
    `;
      return;
    }
    const ctx = {
      mode: "publicCommunity",
      colspan: noteColspan,
      clickable: false,
      publicDetail: state.lastPublicProfileDetail,
    };
    let html = list.map((trade) => buildTradeRecordRowHtml(trade, ctx)).join("");
    if (communityPublicTradesPager.loading) {
      html += tradeListLoadingRowHtml(noteColspan);
    }
    tradeTableBody.innerHTML = html;
    return;
  }
  if (state.route === "community-profile") {
    return;
  }
  if (state.route !== "trade-records" && state.route !== "trade-cash") {
    return;
  }
  if (state.route === "trade-records") {
    state.tradePanelTab = "trades";
  } else {
    state.tradePanelTab = "cash";
  }
  syncTradePanelTabUi();
  if (state.route === "trade-cash") {
    if (!cashTransferTableBody) {
      return;
    }
    renderCashTransferTable();
    return;
  }
  if (!tradeTableBody) {
    return;
  }
  if (tradeListPager.loading && !state.trades.length) {
    tradeTableBody.innerHTML = tradeListLoadingRowHtml(7);
    return;
  }
  const trades = getFilteredTrades(state.tradeFilterAccountId);
  if (!trades.length) {
    tradeTableBody.innerHTML = tradeListPager.loading
      ? tradeListLoadingRowHtml(7)
      : `
      <tr>
        <td colspan="7"><p class="empty">暂无交易记录，请点击「新增交易记录」添加。</p></td>
      </tr>
    `;
    return;
  }
  const sorted = [...trades].sort(sortTradeDesc);
  const ctx = { mode: "private", colspan: 7, clickable: true, publicDetail: null };
  let html = sorted.map((trade) => buildTradeRecordRowHtml(trade, ctx)).join("");
  if (tradeListPager.loading) {
    html += tradeListLoadingRowHtml(7);
  }
  tradeTableBody.innerHTML = html;
}

/** 交易记录页分页在 state.trades；个股记录在 state.stockRecordTrades */
function findTradeById(tradeId) {
  const id = String(tradeId || "");
  if (!id) {
    return null;
  }
  return (
    state.trades.find((item) => String(item.id) === id) ||
    state.stockRecordTrades.find((item) => String(item.id) === id) ||
    null
  );
}

function openTradeRecordActionsSheet(tradeId) {
  if (!recordTradeActionsDialog || !tradeId) {
    return;
  }
  recordTradeActionsDialog.dataset.tradeId = String(tradeId);
  recordTradeActionsDialog.showModal();
}

function closeTradeRecordActionsSheet() {
  if (!recordTradeActionsDialog) {
    return;
  }
  recordTradeActionsDialog.close();
  recordTradeActionsDialog.dataset.tradeId = "";
}

function openAccountManageDialog(accountId) {
  const acc = state.accounts.find((a) => a.id === accountId);
  if (!acc || !accountManageDialog) {
    return;
  }
  state.editingAccountId = accountId;
  if (accountManageName) {
    accountManageName.value = acc.name || "";
  }
  if (accountManageCurrency) {
    accountManageCurrency.value = acc.currency || "CNY";
  }
  const isDef = acc.id === DEFAULT_ACCOUNT.id;
  if (accountManageName) {
    accountManageName.disabled = isDef;
  }
  if (accountManageCurrency) {
    accountManageCurrency.disabled = isDef;
  }
  if (accountManageSaveBtn) {
    accountManageSaveBtn.disabled = isDef;
  }
  accountManageDefaultHint?.classList.toggle("hidden", !isDef);
  accountManageDeleteBtn?.classList.toggle("hidden", isDef);
  accountManageDialog.showModal();
}

function saveManagedAccount() {
  const id = state.editingAccountId;
  if (!id || id === DEFAULT_ACCOUNT.id || !accountManageName || !accountManageCurrency) {
    return;
  }
  const name = String(accountManageName.value || "").trim();
  let currency = String(accountManageCurrency.value || "CNY").toUpperCase();
  if (!name) {
    return;
  }
  if (!["CNY", "USD", "HKD"].includes(currency)) {
    currency = "CNY";
  }
  state.accounts = normalizeAccounts(
    state.accounts.map((a) => (a.id === id ? { ...a, name, currency } : a)),
  );
  state.editingAccountId = null;
  accountManageDialog?.close();
  persistState();
  renderControls();
  renderAccountSection();
}

function deleteManagedAccount() {
  const id = state.editingAccountId;
  if (!id || id === DEFAULT_ACCOUNT.id) {
    return;
  }
  if (!window.confirm("确定删除该股票账户？删除后不可恢复。")) {
    return;
  }
  void (async () => {
    const n = await fetchTradeCountForAccount(id);
    if (n > 0) {
      window.alert(`该账户下仍有 ${n} 条交易记录，请先删除或编辑交易改用其他账户。`);
      return;
    }
    deleteManagedAccountConfirmed(id);
  })();
}

function deleteManagedAccountConfirmed(id) {
  state.accounts = normalizeAccounts(state.accounts.filter((a) => a.id !== id));
  if (state.selectedAccountId === id) {
    state.selectedAccountId = "all";
  }
  if (state.tradeFilterAccountId === id) {
    state.tradeFilterAccountId = "all";
  }
  state.editingAccountId = null;
  accountManageDialog?.close();
  persistState();
  renderControls();
  renderAccountSection();
  renderAll();
}

function invalidateTradeListAfterMutation() {
  if (state.route === "trade-records") {
    void loadTradeListPage({ reset: true });
  }
}

function invalidateStockRecordTradesAfterMutation() {
  if (state.route !== "stock-record" || !state.activeRecordSymbol || state.stockRecordFromPublicProfile) {
    return;
  }
  void refreshStockRecordPageData(state.activeRecordSymbol, state.stockRecordAccountId);
}

function invalidateCashListAfterMutation() {
  if (state.route === "trade-cash") {
    void loadCashListPage({ reset: true });
  }
}

function openEditTradeDialog(tradeId) {
  closeTradeRecordActionsSheet();
  const trade = findTradeById(tradeId);
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
  syncTradeAmountFromPriceQuantity();
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
  closeTradeRecordActionsSheet();
  try {
    await deleteTradeFromApi(tradeId);
  } catch (error) {
    console.error("删除数据库交易失败，继续执行本地删除", error);
  }
  state.trades = state.trades.filter((item) => item.id !== tradeId);
  state.stockRecordTrades = state.stockRecordTrades.filter((item) => item.id !== tradeId);
  if (state.trades.length === 0) {
    if (sessionPhone) {
      state.useDemoData = false;
    } else {
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
  }
  persistState();
  renderAll();
  invalidateTradeListAfterMutation();
  invalidateStockRecordTradesAfterMutation();
  void refreshMarketData();
}

let overviewProfitRefreshSeq = 0;
let _overviewProfitInflight = null;

const _kpiInFlightByScope = new Map();


const METRICS_HOME_STAGE = { month: "mtd", ytd: "ytd", total: "inception" };
const METRICS_HOME_BUNDLE_STAGE_KEYS = ["today", "mtd", "ytd", "inception"];
function metricsStageFromHome() { return METRICS_HOME_STAGE[state.stageRange] || "mtd"; }
function overviewMetricsBundleCacheKey(aid) {
  return `${aid}|${state.algoMode}`;
}
function overviewReturnsHasAllHomeStages(ret) {
  if (!ret?.stages) return false;
  return METRICS_HOME_BUNDLE_STAGE_KEYS.every((k) => ret.stages[k]);
}
function resolvePerformancePresetKeyFromStateLike(like) {
  const arm = String(like?.analysisRangeMode ?? "preset");
  if (arm === "custom") {
    return null;
  }
  if (arm === "all") {
    return "inception";
  }
  if (arm !== "preset") {
    return null;
  }
  if (like?.analysisPreset === "mtd") {
    return "mtd";
  }
  if (like?.analysisPreset === "ytd") {
    return "ytd";
  }
  if (Number(like?.analysisPanOffset || 0) !== 0) {
    return null;
  }
  const rd = Number(like?.rangeDays);
  if (rd === 7) {
    return "last_7d";
  }
  if (rd === 30) {
    return "last_30d";
  }
  if (rd === 90) {
    return "last_90d";
  }
  return null;
}

function resolvePerformancePresetKeyFromAnalysisState() {
  return resolvePerformancePresetKeyFromStateLike(state);
}

function metricsStageFromAnalysis() {
  const preset = resolvePerformancePresetKeyFromAnalysisState();
  if (preset) return preset;
  if (state.analysisRangeMode === "all") return "inception";
  return "mtd";
}
const ANALYSIS_CHART_DEFAULT_WINDOW = 30;
const ANALYSIS_CHART_MIN_WINDOW = 7;
const ANALYSIS_CHART_MAX_WINDOW = 365;
const STOCK_RECORD_CHART_MIN_WINDOW = 12;
const STOCK_RECORD_CHART_PAGE_SIZE = 30;

function isStockRecordChartMode(mode) {
  return mode === "stock" || mode === "stock-profit" || mode === "stock-weight";
}

function resetAnalysisChartViewport() {
  state.analysisChartWindow = ANALYSIS_CHART_DEFAULT_WINDOW;
  state.analysisPanOffset = 0;
}

/** 在已缓存的全量序列上裁切可见窗口（最新在右，offset 越大越早） */
function trimMetricsSeriesPoints(points) {
  const list = Array.isArray(points) ? points : [];
  if (list.length < 2) {
    return list;
  }
  const windowSize = Math.max(
    2,
    Math.min(list.length, Number(state.analysisChartWindow) || ANALYSIS_CHART_DEFAULT_WINDOW),
  );
  const maxOffset = Math.max(0, list.length - windowSize);
  const offset = Math.max(0, Math.min(maxOffset, Number(state.analysisPanOffset) || 0));
  state.analysisPanOffset = offset;
  const end = list.length - offset;
  const start = Math.max(0, end - windowSize);
  return list.slice(start, end);
}

function analysisChartNavTotalFromCache() {
  const c = cachedAnalysisMetricsCharts;
  if (!c) {
    return 0;
  }
  const lens = [
    c.fullTwrPts?.length,
    c.fullProfitPts?.length,
    c.fullAssetRows?.length,
    c.fullBenchPts?.length,
  ].filter((n) => Number(n) > 0);
  return lens.length ? Math.max(...lens) : 0;
}
async function fetchMetricsApi(path, params = {}, publicTargetId = "") {
  if (!apiReady) return null;
  const qs = new URLSearchParams();
  const p = { ...params };
  if (p.account_id == null && p.accountScope != null) {
    p.account_id = p.accountScope;
  }
  delete p.accountScope;
  for (const [k, v] of Object.entries(p)) {
    if (v != null && String(v) !== "") {
      qs.set(k, String(v));
    }
  }
  const q = qs.toString();
  const prefix = publicTargetId ? `${getApiBaseForFetch()}/public/${encodeURIComponent(publicTargetId)}` : getApiBaseForFetch();
  const url = `${prefix}${path.startsWith("/") ? path : `/${path}`}${q ? `?${q}` : ""}`;
  const pathNorm = String(path || "");
  const timeoutMs =
    pathNorm.includes("home-bundle") ||
    pathNorm.includes("analysis-bundle") ||
    pathNorm.includes("stock-record-bundle")
      ? 55_000
      : 28_000;
  try {
    const res = await apiFetch(url, { cache: "no-store", timeoutMs });
    const j = await res.json().catch(() => ({}));
    if (!res.ok || !j?.ok || !j.data) return null;
    return j.data;
  } catch { return null; }
}
function metricHeadlineHtml(profitStr, rateStr) {
  const amtCls = bundleSignedClass(profitStr);
  const rate = parseBundlePercent(rateStr);
  return `<span class="profit-amt ${amtCls}">${escapeHtml(bundleFmtText(profitStr))}</span><span class="profit-rate-inline ${twrColorClass(rate)}">${escapeHtml(bundleFmtText(rateStr))}</span>`;
}

const METRICS_TABLE_MARKET_SORT = { "A股": 1, "港股": 2, "美股": 3 };

function resolveMetricsStockSortKeyValue(row, key) {
  if (key === "currentPrice") return parseBundlePercent(row.dayChange);
  if (key === "marketValue") return parseBundleSignedAmount(row.marketValueCny ?? row.marketValue);
  if (key === "weight") return parseBundlePercent(row.weight);
  if (key === "cost") return parseBundleSignedAmount(row.cost);
  if (key === "monthProfit") return metricsRowProfitSortAmount(row, "monthProfit");
  if (key === "monthWeight") return parseBundlePercent(row.monthWeight);
  if (key === "yearProfit") return metricsRowProfitSortAmount(row, "yearProfit");
  if (key === "yearWeight") return parseBundlePercent(row.yearWeight);
  if (key === "totalProfit") return metricsRowProfitSortAmount(row, "totalProfit");
  if (key === "totalWeight") return parseBundlePercent(row.totalWeight);
  if (key === "totalRate") return parseBundlePercent(row.totalRate);
  if (key === "todayProfit") return metricsRowProfitSortAmount(row, "todayProfit");
  if (key === "regretRate") {
    const raw = String(row.regret || "").replace(/\s+[BS]$/i, "").trim();
    return parseBundlePercent(raw);
  }
  if (key === "lastTradeDate") return Date.parse(row.lastTradeDate || 0) || 0;
  return 0;
}

function sortMetricsHoldingsRows(list) {
  const rows = [...(list || [])];
  if (!rows.length) return rows;
  if (state.stockSortOrder === "default" || state.stockSortKey === "default") {
    rows.sort((a, b) => {
      const marketCmp =
        (METRICS_TABLE_MARKET_SORT[a.market] || 99) - (METRICS_TABLE_MARKET_SORT[b.market] || 99);
      if (marketCmp !== 0) return marketCmp;
      return (Date.parse(b.lastTradeDate || 0) || 0) - (Date.parse(a.lastTradeDate || 0) || 0);
    });
    return rows;
  }
  const key = state.stockSortKey;
  const direction = state.stockSortOrder === "asc" ? 1 : -1;
  rows.sort((a, b) => {
    const av = resolveMetricsStockSortKeyValue(a, key);
    const bv = resolveMetricsStockSortKeyValue(b, key);
    return (av - bv) * direction;
  });
  return rows;
}

/** home-bundle 个股金额：native 列无符号；cny 列用服务端预计算字段，港美股加 ¥。 */

/** 个股表月/年/总收益排序：单账户按账户默认币种，全部账户按人民币（不受 ¥ 列切换影响） */
function metricsRowProfitSortAmount(row, fieldBase) {
  const book = getOverviewBookCurrency();
  const cny = parseBundleSignedAmount(row[`${fieldBase}Cny`]);
  if (!Number.isFinite(cny)) {
    return 0;
  }
  if (book === "CNY") {
    return cny;
  }
  const ccy = String(row.currency || "").toUpperCase();
  const native = parseBundleSignedAmount(row[fieldBase]);
  if (ccy === book && Number.isFinite(native)) {
    return native;
  }
  return amountBookFromCny(cny, book);
}

function stockAmountDisplayIsCny(displayMode) {
  if (displayMode === "cny" || displayMode === "native") {
    return displayMode === "cny";
  }
  return state.stockAmountDisplay === "cny";
}

function measureStockTableTextPx(text, font) {
  const s = String(text ?? "");
  if (!s) {
    return 0;
  }
  if (!_stockTableMeasureCanvas) {
    _stockTableMeasureCanvas = document.createElement("canvas");
  }
  const ctx = _stockTableMeasureCanvas.getContext("2d");
  if (!ctx) {
    return s.length * 8;
  }
  ctx.font = font;
  return ctx.measureText(s).width;
}

function readOverviewStockTableHeaderLabels() {
  const table = stockTableBody?.closest("table.stock-table");
  if (table) {
    const labels = [...table.querySelectorAll("thead th")].map((th) =>
      String(th.innerText || "")
        .replace(/\s+/g, " ")
        .trim(),
    );
    if (labels.length === OVERVIEW_STOCK_TABLE_COL_COUNT) {
      return labels;
    }
  }
  return OVERVIEW_STOCK_TABLE_HEADER_FALLBACK.slice();
}

function overviewStockTableLayoutCacheKey(rows) {
  const aid = state.selectedAccountId === "all" ? "all" : resolveValidAccountFilter(state.selectedAccountId);
  const sig = (rows || [])
    .map((r) =>
      [
        normalizeSymbol(r.symbol),
        r.todayProfit,
        r.todayProfitCny,
        r.marketValue,
        r.marketValueCny,
        r.monthProfit,
        r.monthProfitCny,
        r.yearProfit,
        r.yearProfitCny,
        r.totalProfit,
        r.totalProfitCny,
        r.price,
        r.dayChange,
        r.quantity,
        r.weight,
        r.cost,
        r.totalRate,
        r.regret,
      ].join("^"),
    )
    .join(";");
  return `${overviewMetricsBundleCacheKey(aid)}::${sig}`;
}

function overviewStockNameColWidthPx() {
  const nameW = Math.ceil(
    measureStockTableTextPx(OVERVIEW_STOCK_NAME_COL_PROBE, STOCK_TABLE_MEASURE_FONT_TH) +
      STOCK_TABLE_MEASURE_PAD_X,
  );
  const headW = Math.ceil(
    measureStockTableTextPx("名称", STOCK_TABLE_MEASURE_FONT_TH) + STOCK_TABLE_MEASURE_PAD_X,
  );
  return Math.max(nameW, headW);
}

function readStockTableHeaderLabelsForCtx(ctx) {
  const all = readOverviewStockTableHeaderLabels();
  const indices = stockTableVisibleColIndices(ctx);
  return indices.map((col) => all[col] || "");
}

function measureStockTableColWidths(measureCellText, ctx) {
  const indices = stockTableVisibleColIndices(ctx);
  const allHeaders = readOverviewStockTableHeaderLabels();
  const widths = indices.map((col) => {
    const label = allHeaders[col] || "";
    return Math.ceil(measureStockTableTextPx(label, STOCK_TABLE_MEASURE_FONT_TH) + STOCK_TABLE_MEASURE_PAD_X);
  });
  const modes = ["cny", "native"];
  for (const row of measureCellText.rows || []) {
    for (const mode of modes) {
      for (let vi = 0; vi < indices.length; vi += 1) {
        const col = indices[vi];
        if (col === 0) {
          continue;
        }
        const raw = measureCellText.fn(row, col, mode, measureCellText.ctx);
        const parts = Array.isArray(raw) ? raw : [raw];
        for (const part of parts) {
          const t = String(part ?? "").trim();
          if (!t) {
            continue;
          }
          widths[vi] = Math.max(
            widths[vi],
            Math.ceil(measureStockTableTextPx(t, STOCK_TABLE_MEASURE_FONT_TD) + STOCK_TABLE_MEASURE_PAD_X),
          );
        }
      }
    }
  }
  const nameVi = indices.indexOf(0);
  if (nameVi >= 0) {
    widths[nameVi] = overviewStockNameColWidthPx();
  }
  const opVi = indices.indexOf(14);
  if (opVi >= 0) {
    widths[opVi] = Math.max(
      widths[opVi],
      Math.ceil(measureStockTableTextPx(ctx.opProbeText, STOCK_TABLE_MEASURE_FONT_TD) + STOCK_TABLE_MEASURE_PAD_X),
    );
  }
  return widths.map((w) => w + 2);
}

function applyStockTableColWidths(ctx, widths) {
  const table = ctx.table;
  if (!table || !widths?.length) {
    return;
  }
  const indices = stockTableVisibleColIndices(ctx);
  let colgroup = table.querySelector("colgroup");
  if (!colgroup) {
    colgroup = document.createElement("colgroup");
    table.insertBefore(colgroup, table.firstChild);
  }
  colgroup.replaceChildren();
  let sum = 0;
  for (let vi = 0; vi < widths.length; vi += 1) {
    const w = widths[vi];
    const colEl = document.createElement("col");
    colEl.setAttribute("data-stock-col", String(indices[vi]));
    colEl.style.width = `${w}px`;
    colgroup.appendChild(colEl);
    sum += w;
  }
  table.classList.add("stock-table--layout-locked");
  table.style.setProperty("--stock-table-layout-width-px", `${sum}px`);
  const nameVi = indices.indexOf(0);
  const nameW = nameVi >= 0 ? widths[nameVi] : widths[0];
  table.style.setProperty("--stock-table-name-col-px", `${nameW}px`);
  if (ctx.visibleColIndices) {
    table.style.minWidth = `max(100%, ${sum}px)`;
  } else {
    table.style.removeProperty("min-width");
  }
}

function clearStockTableColLayout(ctx) {
  ctx.widthCache.key = "";
  ctx.widthCache.widths = null;
  const table = ctx.table;
  if (!table) {
    return;
  }
  table.classList.remove("stock-table--layout-locked");
  table.style.removeProperty("--stock-table-layout-width-px");
  table.style.removeProperty("--stock-table-name-col-px");
  table.style.removeProperty("min-width");
  table.querySelector("colgroup")?.remove();
}

function ensureStockTableColWidths(rows, ctx, measureCell) {
  if (!rows?.length) {
    clearStockTableColLayout(ctx);
    return;
  }
  const key = ctx.layoutCacheKey(rows);
  if (ctx.widthCache.key === key && ctx.widthCache.widths) {
    applyStockTableColWidths(ctx, ctx.widthCache.widths);
    return;
  }
  const widths = measureStockTableColWidths(
    {
      rows,
      fn: measureCell.fn,
      ctx: measureCell.ctx,
    },
    ctx,
  );
  ctx.widthCache.key = key;
  ctx.widthCache.widths = widths;
  applyStockTableColWidths(ctx, widths);
}

function clearOverviewStockTableColLayout() {
  clearStockTableColLayout(getPrivateStockTableCtx());
}

function applyOverviewStockTableColWidths(widths) {
  applyStockTableColWidths(getPrivateStockTableCtx(), widths);
}

function ensureOverviewStockTableColWidths(rows, measureCell) {
  ensureStockTableColWidths(rows, getPrivateStockTableCtx(), measureCell);
}

function metricsHoldingsRowCellTexts(row, col, displayMode) {
  const sym = normalizeSymbol(row.symbol);
  switch (col) {
    case 0:
      return [
        String(row.name || sym).trim(),
        `${row.marketTag || "OT"} ${row.stockCode || formatSymbolForDisplay(sym)}`,
      ];
    case 1:
      return metricsHoldingsMoneyCell(row, "todayProfit", displayMode);
    case 2:
      return [bundleFmtText(row.price), bundleFmtText(row.dayChange)];
    case 3:
      return [metricsHoldingsMoneyCell(row, "marketValue", displayMode), bundleFmtText(row.quantity)];
    case 4:
      return bundleFmtText(row.weight);
    case 5:
      return bundleFmtText(row.cost);
    case 6:
      return metricsHoldingsMoneyCell(row, "monthProfit", displayMode);
    case 7:
      return bundleFmtText(row.monthWeight);
    case 8:
      return metricsHoldingsMoneyCell(row, "yearProfit", displayMode);
    case 9:
      return bundleFmtText(row.yearWeight);
    case 10:
      return metricsHoldingsMoneyCell(row, "totalProfit", displayMode);
    case 11:
      return bundleFmtText(row.totalWeight);
    case 12:
      return bundleFmtText(row.totalRate);
    case 13:
      return bundleFmtText(String(row.regret || "").replace(/\s+[BS]$/i, ""));
    case 14:
      return "记录  交易";
    default:
      return "";
  }
}

function metricsRowProfitClass(row, fieldBase) {
  const cnyOn = state.stockAmountDisplay === "cny";
  const text = cnyOn ? row[`${fieldBase}Cny`] : row[fieldBase];
  return bundleSignedClass(text);
}

function metricsHoldingsMoneyCell(row, fieldBase, displayMode = null) {
  const cnyOn = stockAmountDisplayIsCny(displayMode);
  const isCn = row.isCnyStock === true || row.marketTag === "CN" || String(row.currency || "").toUpperCase() === "CNY";
  const nativeKey = fieldBase;
  const cnyKey = `${fieldBase}Cny`;
  let text = cnyOn ? String(row[cnyKey] ?? row[nativeKey] ?? "").trim() : String(row[nativeKey] ?? "").trim();
  if (!text) {
    return "–";
  }
  if (cnyOn && !isCn) {
    return `¥ ${text}`;
  }
  return text;
}

function buildMetricsHoldingsCellTd(row, col, ctx) {
  const sym = normalizeSymbol(row.symbol);
  const tag = row.marketTag === "CN" ? "cn" : row.marketTag === "HK" ? "hk" : row.marketTag === "US" ? "us" : "ot";
  const dayClass = bundleSignedClass(row.dayChange);
  const todayClass = metricsRowProfitClass(row, "todayProfit");
  const monthClass = metricsRowProfitClass(row, "monthProfit");
  const yearClass = metricsRowProfitClass(row, "yearProfit");
  const totalClass = metricsRowProfitClass(row, "totalProfit");
  const totalRateClass = bundleSignedClass(row.totalRate);
  const regretClass = bundleSignedClass(String(row.regret || "").replace(/\s+[BS]$/i, ""));
  const qty = bundleFmtText(row.quantity);
  const symEsc = escapeHtml(sym);
  const recordLink = `<a href="javascript:void(0)" class="record-link stock-table-record-link" data-stock-record="${symEsc}">记录</a>`;
  const tradeLink = ctx.showTradeLink
    ? `<a href="javascript:void(0)" class="record-link stock-table-trade-link" data-stock-add-trade="${symEsc}">交易</a>`
    : "";
  const attr = ` data-stock-col="${col}"`;
  switch (col) {
    case 0:
      return `<td${attr} class="stock-name"><strong>${escapeHtml(row.name || sym)}</strong><span><i class="market-tag market-tag--${tag}">${escapeHtml(row.marketTag || "OT")}</i> ${escapeHtml(row.stockCode || formatSymbolForDisplay(sym))}</span></td>`;
    case 1:
      return `<td${attr} class="${todayClass}">${escapeHtml(metricsHoldingsMoneyCell(row, "todayProfit"))}</td>`;
    case 2:
      return `<td${attr}><div class="cell-main">${escapeHtml(bundleFmtText(row.price))}</div><div class="cell-sub ${dayClass}">${escapeHtml(bundleFmtText(row.dayChange))}</div></td>`;
    case 3:
      return `<td${attr}><div class="cell-main">${escapeHtml(metricsHoldingsMoneyCell(row, "marketValue"))}</div><div class="cell-sub">${escapeHtml(qty)}</div></td>`;
    case 4:
      return `<td${attr}>${escapeHtml(bundleFmtText(row.weight))}</td>`;
    case 5:
      return `<td${attr}>${escapeHtml(bundleFmtText(row.cost))}</td>`;
    case 6:
      return `<td${attr} class="${monthClass}">${escapeHtml(metricsHoldingsMoneyCell(row, "monthProfit"))}</td>`;
    case 7:
      return `<td${attr}>${escapeHtml(bundleFmtText(row.monthWeight))}</td>`;
    case 8:
      return `<td${attr} class="${yearClass}">${escapeHtml(metricsHoldingsMoneyCell(row, "yearProfit"))}</td>`;
    case 9:
      return `<td${attr}>${escapeHtml(bundleFmtText(row.yearWeight))}</td>`;
    case 10:
      return `<td${attr} class="${totalClass}">${escapeHtml(metricsHoldingsMoneyCell(row, "totalProfit"))}</td>`;
    case 11:
      return `<td${attr}>${escapeHtml(bundleFmtText(row.totalWeight))}</td>`;
    case 12:
      return `<td${attr} class="${totalRateClass}">${escapeHtml(bundleFmtText(row.totalRate))}</td>`;
    case 13:
      return `<td${attr} class="${regretClass}">${escapeHtml(bundleFmtText(row.regret))}</td>`;
    case 14:
      return `<td${attr} class="stock-table-op-cell">${recordLink}${tradeLink}</td>`;
    default:
      return `<td${attr}></td>`;
  }
}

function buildMetricsHoldingsRowHtml(row, ctx) {
  const indices = stockTableVisibleColIndices(ctx);
  const cells = indices.map((col) => buildMetricsHoldingsCellTd(row, col, ctx)).join("");
  return `<tr>${cells}</tr>`;
}

function paintStockTableFromMetricsRows(rows, ctx) {
  const tbody = ctx.tbody;
  if (!tbody) {
    return;
  }
  const colCount = stockTableVisibleColCount(ctx);
  const sorted = sortMetricsHoldingsRows(rows);
  if (!sorted.length) {
    clearStockTableColLayout(ctx);
    tbody.innerHTML = `<tr><td colspan="${colCount}"><p class="empty">暂无持仓，点击“记一笔”开始记录。</p></td></tr>`;
    return;
  }
  ensureStockTableColWidths(sorted, ctx, {
    fn: metricsHoldingsRowCellTexts,
    ctx: null,
  });
  tbody.innerHTML = sorted.map((row) => buildMetricsHoldingsRowHtml(row, ctx)).join("");
  syncStockTableSortHeaderUi(ctx);
}

function paintOverviewStockTableFromMetricsRows(rows) {
  paintStockTableFromMetricsRows(rows, getPrivateStockTableCtx());
}
function analysisAssetChartRowsFromSeries(series, opts = {}) {
  if (!series || typeof series !== "object") {
    return [];
  }
  const normalized = opts.normalizedAmounts === true;
  const dateSet = new Set();
  for (const key of ["totalAssets", "marketValue", "cash", "cashRatio"]) {
    for (const p of series[key] || []) {
      if (p?.date) {
        dateSet.add(String(p.date).slice(0, 10));
      }
    }
  }
  const pick = (metric, date) => {
    const pts = series[metric] || [];
    const hit = pts.find((p) => String(p.date).slice(0, 10) === date);
    if (!hit) {
      return 0;
    }
    if (metric === "cashRatio") {
      return parseBundlePercentChart(hit.cashRatio);
    }
    if (normalized) {
      return parseBundlePlainNumber(hit[metric]) ?? 0;
    }
    return parseBundleSignedAmount(hit[metric]);
  };
  return [...dateSet]
    .sort((a, b) => a.localeCompare(b))
    .map((date) => ({
      date,
      totalAssets: pick("totalAssets", date),
      market: pick("marketValue", date),
      cash: pick("cash", date),
      cashRatio: pick("cashRatio", date),
    }));
}

function analysisRateSeriesForChart(points) {
  return (points || []).map((p) => ({
    date: p.date,
    rate:
      typeof p.rate === "string"
        ? parseBundlePercent(p.rate)
        : Number(p.rate) || 0,
  }));
}

function analysisRateCrosshairText(date, key) {
  const ctx = cachedAnalysisMetricsCharts;
  if (!date || !ctx) {
    return "–";
  }
  if (key === "benchmark") {
    const pt = (ctx.benchPts || []).find((p) => String(p.date).slice(0, 10) === String(date).slice(0, 10));
    return bundleFmtText(pt?.rate ?? pt?.rateDisplay);
  }
  const pt = (ctx.twrPts || []).find((p) => String(p.date).slice(0, 10) === String(date).slice(0, 10));
  return bundleFmtText(pt?.rate);
}

function repaintAnalysisAssetChartFromCache() {
  if (!cachedAnalysisAssetChartRows?.length || !analysisAssetChart || !cachedAnalysisMetricsCharts) {
    return;
  }
  cachedAnalysisMetricsCharts.payloads.asset = drawAssetChart(
    trimMetricsSeriesPoints(cachedAnalysisAssetChartRows),
    undefined,
    undefined,
    { normalizedAmounts: cachedAnalysisMetricsCharts.isPublicView === true },
  );
  bindInteractiveChart(analysisAssetChart, analysisAssetTooltip, () => cachedAnalysisMetricsCharts.payloads.asset, {
    mode: "analysis",
    onRefresh: cachedAnalysisMetricsCharts.refreshAnalysisView,
    onRedraw: cachedAnalysisMetricsCharts.redrawChartsOnly,
    valueFormatter: cachedAnalysisMetricsCharts.assetValueFormatter,
  });
}

function bindAnalysisMetricsChartsInteractive() {
  const ctx = cachedAnalysisMetricsCharts;
  if (!ctx) {
    return;
  }
  const chartNav = { chartNavTotal: analysisChartNavTotalFromCache };
  bindInteractiveChart(analysisRateChart, analysisRateTooltip, () => ctx.payloads.rate, {
    mode: "analysis",
    onRefresh: ctx.refreshAnalysisView,
    onRedraw: ctx.redrawChartsOnly,
    ...chartNav,
    valueFormatter: (_value, key) => {
      if (key === "benchmark" && state.benchmark === "none") {
        return "–";
      }
      const date = state.chartCrosshairMap[analysisRateChart?.id]?.date;
      return analysisRateCrosshairText(date, key);
    },
  });
  bindInteractiveChart(analysisProfitChart, analysisProfitTooltip, () => ctx.payloads.profit, {
    mode: "analysis",
    onRefresh: ctx.refreshAnalysisView,
    onRedraw: ctx.redrawChartsOnly,
    ...chartNav,
    valueFormatter: (value) =>
      typeof ctx.profitValueFormatter === "function" ? ctx.profitValueFormatter(value) : formatNumber(value, 2),
  });
  bindInteractiveChart(analysisAssetChart, analysisAssetTooltip, () => ctx.payloads.asset, {
    mode: "analysis",
    onRefresh: ctx.refreshAnalysisView,
    onRedraw: ctx.redrawChartsOnly,
    ...chartNav,
    valueFormatter: ctx.assetValueFormatter,
  });
}

async function refreshAnalysisMetricsView(opts = {}) {
  const pubTid = communityAnalysisTargetId();
  if (pubTid) {
    state.publicAnalysisBundleUi.ready = false;
    await loadPublicAnalysisTabData(pubTid);
    return;
  }
  if (state.route === "analysis") {
    await renderAnalysis(opts);
  }
}

async function paintAnalysisFromMetricsApi(renderRequestId, publicTargetId = "", opts = {}) {
  const isPublicView = !!String(publicTargetId || "").trim();
  const stage = metricsStageFromAnalysis();
  const aid = isPublicView ? "all" : state.selectedAccountId === "all" ? "all" : state.selectedAccountId;
  const benchSym = state.benchmark === "none" ? "" : normalizeSymbol(state.benchmark);
  let bundle = opts.bundle || null;
  if (!bundle) {
    const bundleParams = { account_id: aid, stage };
    if (benchSym) {
      bundleParams.symbol = benchSym;
    }
    const bundlePath = isPublicView ? "/analysis-bundle" : "/metrics/analysis-bundle";
    bundle = await fetchMetricsApi(bundlePath, bundleParams, publicTargetId);
  }
  const series = bundle?.series || {};
  const fullTwrPts = series.stageRate || series.dailyTwr || [];
  const fullProfitPts = series.stageProfit || series.dailyProfit || [];
  const benchPack = bundle?.benchmark;
  const fullBenchPts = benchPack?.points || [];
  const rankPack = bundle?.stockRank;
  const retPack = bundle?.returns;
  if (renderRequestId !== analysisRenderRequestSeq) return false;
  if (!fullTwrPts.length && !fullProfitPts.length) return false;
  if (opts.resetViewport) {
    resetAnalysisChartViewport();
  }
  const metaAlgo = String(bundle?.meta?.algoMode || "").trim();
  const useMwrUi = isPublicView
    ? normalizeProfitAlgoMode(metaAlgo || state.algoMode) === "mwr"
    : normalizeProfitAlgoMode(state.algoMode) === "mwr";
  const parseAmountPoint = (p) =>
    isPublicView ? parseBundlePlainNumber(p.profit) ?? 0 : parseBundleSignedAmount(p.profit);
  const twrPtsTrim = trimMetricsSeriesPoints(fullTwrPts);
  const benchPtsTrim = trimMetricsSeriesPoints(fullBenchPts);
  const mySeries = analysisRateSeriesForChart(twrPtsTrim);
  const benchSeries = analysisRateSeriesForChart(benchPtsTrim);
  const profitSeries = trimMetricsSeriesPoints(fullProfitPts).map((p) => ({
    date: p.date,
    value: parseAmountPoint(p),
  }));
  const fullAssetRows = analysisAssetChartRowsFromSeries(series, {
    normalizedAmounts: isPublicView,
  });
  cachedAnalysisAssetChartRows = trimMetricsSeriesPoints(fullAssetRows);

  const refreshAnalysisView = () => {
    renderControls();
    void refreshAnalysisMetricsView({ showLoading: false });
  };
  const assetValueFormatter = (value) =>
    state.capitalTrendMode === "cash_ratio"
      ? `${formatNumber(value, 2)}%`
      : isPublicView
        ? formatNumber(value, 4)
        : formatNumber(analysisSnapshotMoneyFromCny(value), 2);
  const profitValueFormatter = (v) => formatNumber(v, isPublicView ? 4 : 2);

  const rebuildVisibleAnalysisSeries = () => {
    const c = cachedAnalysisMetricsCharts;
    if (!c) {
      return;
    }
    const twrTrim = trimMetricsSeriesPoints(c.fullTwrPts);
    const benchTrim = trimMetricsSeriesPoints(c.fullBenchPts);
    c.twrPts = twrTrim;
    c.benchPts = benchTrim;
    c.mySeries = analysisRateSeriesForChart(twrTrim);
    c.benchSeries = analysisRateSeriesForChart(benchTrim);
    c.profitSeries = trimMetricsSeriesPoints(c.fullProfitPts).map((p) => ({
      date: p.date,
      value: c.isPublicView ? parseBundlePlainNumber(p.profit) ?? 0 : parseBundleSignedAmount(p.profit),
    }));
    cachedAnalysisAssetChartRows = trimMetricsSeriesPoints(c.fullAssetRows);
  };

  const redrawChartsOnly = () => {
    const c = cachedAnalysisMetricsCharts;
    if (!c) {
      return;
    }
    rebuildVisibleAnalysisSeries();
    c.payloads.rate = c.useMwrUi
      ? drawAnalysisMwrRatePlaceholder(analysisRateChart, "资金加权收益率只算总值、不算每日走势。")
      : drawLineChart(c.mySeries, c.benchSeries);
    c.payloads.profit = drawDualLineChart(
      analysisProfitChart,
      c.profitSeries.map((i) => ({ date: i.date, value: i.value })),
      null,
      "#f45a68",
      null,
      {
        keyA: "profit",
        labelA: "收益",
        yAxisMode: "left",
        leftLabel: "",
        xLabel: "",
        valueFormatter: profitValueFormatter,
        axisFormatter: profitValueFormatter,
        yRangePadding: {
          minFactor: ANALYSIS_CHART_AXIS_MIN_FACTOR,
          maxFactor: ANALYSIS_CHART_AXIS_MAX_FACTOR,
        },
      },
    );
    c.payloads.asset = drawAssetChart(cachedAnalysisAssetChartRows, undefined, undefined, {
      normalizedAmounts: c.isPublicView === true,
    });
  };

  cachedAnalysisMetricsCharts = {
    fullTwrPts,
    fullProfitPts,
    fullBenchPts,
    fullAssetRows,
    twrPts: twrPtsTrim,
    benchPts: benchPtsTrim,
    mySeries,
    benchSeries,
    profitSeries,
    useMwrUi,
    isPublicView,
    refreshAnalysisView,
    redrawChartsOnly,
    rebuildVisibleAnalysisSeries,
    assetValueFormatter,
    profitValueFormatter,
    payloads: {
      rate: null,
      profit: null,
      asset: null,
    },
  };
  redrawChartsOnly();
  if (analysisRateSummary) {
    const lastBenchPt = trimMetricsSeriesPoints(benchPack?.points || []).at(-1);
    if (useMwrUi) {
      analysisRateSummary.textContent = `我的收益率 ${bundleFmtText(retPack?.rate)}`;
    } else if (state.benchmark === "none") {
      analysisRateSummary.textContent = `我的收益率 ${bundleFmtText(retPack?.rate)}`;
    } else {
      const myPct = bundleFmtText(retPack?.rate);
      const benchPct = bundleFmtText(lastBenchPt?.rate ?? lastBenchPt?.rateDisplay);
      const diff =
        parseBundlePercent(retPack?.rate) -
        parseBundlePercent(lastBenchPt?.rate ?? lastBenchPt?.rateDisplay);
      analysisRateSummary.textContent = `我的 ${myPct} / 基准 ${benchPct} / 对比 ${formatPercent(diff)}`;
    }
  }
  if (analysisProfitSummary && retPack) {
    analysisProfitSummary.textContent = isPublicView
      ? `相对首日 ${bundleFmtText(retPack.profit)}`
      : `累计收益 ${bundleFmtText(retPack.profit)}`;
  }
  paintStockRankFromBundle(rankPack, analysisStockRankBody, {
    publicStockRankLayout: isPublicView,
  });
  if (analysisEodAccountCaption) {
    analysisEodAccountCaption.textContent = "";
    analysisEodAccountCaption.hidden = true;
  }
  bindAnalysisMetricsChartsInteractive();
  return true;
}


async function refreshOverviewProfitRowFromSnapshots() {
  if (!isEarningHomeRoute()) return;
  if (!todayProfitMain || !monthProfitMain) return;
  const aid = state.selectedAccountId === "all" ? "all" : state.selectedAccountId;
  const stageKey = metricsStageFromHome();
  const reqKey = overviewMetricsBundleCacheKey(aid);
  if (_overviewProfitInflight?.key === reqKey) return _overviewProfitInflight.promise;
  const seq = ++overviewProfitRefreshSeq;
  const promise = _doRefreshOverviewProfitRow(aid, stageKey, seq, reqKey);
  _overviewProfitInflight = { key: reqKey, promise };
  return promise;
}

function applyHomeBundleToOverviewUi(bundle, aid, seq) {
  const metricsKey = overviewMetricsBundleCacheKey(aid);
  const ret = bundle?.returns;
  const assets = bundle?.assets;
  const hold = bundle?.holdings;
  if (seq !== overviewProfitRefreshSeq) {
    return false;
  }
  const ok =
    overviewReturnsHasAllHomeStages(ret) &&
    bundleFmtText(assets?.totalAssets, "") !== "" &&
    Array.isArray(hold?.rows);
  if (!ok) {
    state.overviewMetricsUi.ready = false;
    setOverviewProfitKpisDash();
    setOverviewAssetsGridDash();
    paintOverviewStockTableLoading("暂时无法加载持仓数据，请稍后刷新页面。");
    return false;
  }
  state.overviewMetricsUi = {
    ready: true,
    loading: false,
    key: metricsKey,
    meta: bundle.meta,
    returns: ret,
    assets,
    holdings: hold,
  };
  if (state.route === "earning") {
    paintOverviewFromMetricsBundle(ret, assets, hold, metricsStageFromHome());
  }
  return true;
}

async function _doRefreshOverviewProfitRow(aid, stageKey, seq, reqKey) {
  state.overviewMetricsUi.loading = true;
  try {
    if (!apiReady) {
      return;
    }
    const bundle = await fetchHomeBundleMetrics(aid, { stages: METRICS_HOME_BUNDLE_STAGES });
    applyHomeBundleToOverviewUi(bundle, aid, seq);
  } catch {
    if (seq === overviewProfitRefreshSeq) {
      state.overviewMetricsUi.ready = false;
      setOverviewProfitKpisDash();
      setOverviewAssetsGridDash();
      paintOverviewStockTableLoading("暂时无法加载持仓数据，请稍后刷新页面。");
    }
  } finally {
    state.overviewMetricsUi.loading = false;
    if (_overviewProfitInflight?.key === reqKey) {
      _overviewProfitInflight = null;
    }
  }
}



async function renderAnalysis(options = {}) {
  if (state.route !== "analysis") {
    return;
  }
  const showLoading = options.showLoading !== false;
  if (showLoading) {
    cachedAnalysisAssetChartRows = null;
    cachedAnalysisMetricsCharts = null;
    showRouteLoading("数据正在加载中");
  }
  try {
    const renderRequestId = ++analysisRenderRequestSeq;
    setAnalysisSummariesDash();
    clearAnalysisChartsToEmpty();
    if (apiReady) {
      const metricsPainted = await paintAnalysisFromMetricsApi(renderRequestId, "", {
        resetViewport: showLoading,
      });
      if (metricsPainted && renderRequestId === analysisRenderRequestSeq) {
        return;
      }
    }
    if (renderRequestId === analysisRenderRequestSeq) {
      if (analysisStockRankBody) {
        analysisStockRankBody.innerHTML = `<p class="empty">暂无分析区间数据。</p>`;
      }
      if (analysisEodAccountCaption) {
        analysisEodAccountCaption.textContent = "";
        analysisEodAccountCaption.hidden = true;
      }
    }
  } finally {
    if (showLoading) {
      hideRouteLoading();
    }
  }
}

function buildProfitSeries(points) {
  if (!points.length) {
    return [{ date: toDateKey(new Date()), value: 0 }];
  }
  const startClose = points[0].value - points[0].flow;
  let sumFlow = 0;
  const raw = points.map((point) => {
    sumFlow += point.flow;
    return {
      date: point.date,
      value: point.value - startClose - sumFlow,
    };
  });
  return rebaseValueSeriesByFirstDay(raw, "value");
}

async function openNewTradeDialogPrefilledForSymbol(rawSymbol, opts = {}) {
  const accountSource = opts.accountSource === "stock-record" ? "stock-record" : "overview";
  if (state.stockRecordFromPublicProfile) {
    return;
  }
  const symKey = normalizeSymbol(rawSymbol);
  if (!symKey) {
    return;
  }
  const tradeSource =
    state.route === "stock-record" && Array.isArray(state.stockRecordTrades) && state.stockRecordTrades.length
      ? state.stockRecordTrades
      : state.trades;
  const trade = tradeSource.find((t) => normalizeSymbol(t.symbol) === symKey);
  const quote = getQuoteBySymbol(symKey);
  const positionName = (trade && trade.name) || (quote && quote.name) || "";
  let name = getDisplayName(symKey, positionName);
  const mkt = inferMarket(symKey);
  if (apiReady && (mkt === "A股" || mkt === "港股") && !hasCnNameLabel(name)) {
    await hydrateSymbolNameMap([symKey], { force: true });
    name = getDisplayName(symKey, positionName);
  }
  const prefill = { symbol: symKey, name: String(name).trim() || symKey };
  if (accountSource === "stock-record") {
    if (state.stockRecordAccountId && state.stockRecordAccountId !== "all") {
      prefill.accountId = resolveValidAccountFilter(state.stockRecordAccountId);
    }
  } else if (state.selectedAccountId && state.selectedAccountId !== "all") {
    prefill.accountId = resolveValidAccountFilter(state.selectedAccountId);
  }
  openNewTradeDialog(prefill);
}

async function openAddTradePrefilledForActiveRecordSymbol() {
  if (state.stockRecordFromPublicProfile || !state.activeRecordSymbol) {
    return;
  }
  await openNewTradeDialogPrefilledForSymbol(state.activeRecordSymbol, { accountSource: "stock-record" });
}

async function openStockRecordDialog(symbol, opts = {}) {
  const symKey = normalizeSymbol(symbol);
  if (!symKey) {
    return;
  }
  const fromPublicProfile = opts.fromPublicProfile === true;
  const publicOwnerUserId = String(opts.publicOwnerUserId || "").trim();
  if (fromPublicProfile) {
    const uid = publicOwnerUserId || String(state.communityProfileUserId || "").trim();
    if (uid) {
      state.communityProfileUserId = uid;
    }
  }
  state.stockRecordFromPublicProfile = fromPublicProfile;
  state.activeRecordSymbol = symKey;
  state.stockRecordAccountId = "all";
  state.previousRoute = state.route;
  state.stockRecordWindow = 30;
  state.stockRecordOffset = 0;
  state.stockRecordChartRange = "30";
  state.stockRecordBundle = null;
  resetStockRecordChartCache();
  resetStockRecordChartPagination();
  state.stockRecordTrades = [];
  resetStockRecordTradesPager();
  state.stockRecordTradesLoading = true;

  state.route = "stock-record";
  renderRoute();
  setStockRecordPageLoading(true);
  window.scrollTo(0, 0);
  persistState();

  void refreshStockRecordPageData(symKey, state.stockRecordAccountId);
}

async function renderStockRecordPage(symbol) {
  const symKey = normalizeSymbol(symbol);
  if (
    state.stockRecordPageLoading ||
    state.route !== "stock-record" ||
    normalizeSymbol(state.activeRecordSymbol) !== symKey
  ) {
    return;
  }
  const detail = state.lastPublicProfileDetail;
  const usePub = useCommunityPublicStockRecord();
  const activeAccountId = usePub ? "all" : resolveValidAccountFilter(state.stockRecordAccountId);
  if (!usePub && activeAccountId !== state.stockRecordAccountId) {
    state.stockRecordAccountId = activeAccountId;
  }
  const scopeTrades = stockRecordTradesForScope(activeAccountId, usePub, detail).filter(
    (item) => normalizeSymbol(item.symbol) === symKey,
  );
  const portfolio = computePortfolio(scopeTrades, []);
  const position = portfolio.positions.find((item) => normalizeSymbol(item.symbol) === symKey);
  const symbolTrades = [...scopeTrades].sort(sortTradeDesc);
  if (
    !position &&
    !symbolTrades.length &&
    !usePub &&
    activeAccountId === "all" &&
    !state.stockRecordTradesLoading &&
    stockRecordTradesPager.loaded
  ) {
    if (state.route === "stock-record" && state.activeRecordSymbol === symbol) {
      state.route = state.previousRoute || "earning";
      state.activeRecordSymbol = null;
      persistState();
      renderRoute();
      renderOverviewAndStockTable();
    }
    return;
  }
  const bundle = state.stockRecordBundle;
  const headline = bundle?.headline || null;
  const positionName = headline?.name || position?.name || symbolTrades[0]?.name || symbol;

  stockRecordTitle.textContent = `${getDisplayName(symbol, positionName)}(${formatSymbolForDisplay(symbol)})`;
  stockRecordTime.textContent = headline?.quoteTime ?? "—";
  stockRecordPrice.textContent = headline?.price ?? "—";
  const priceUp = headline?.changePct ? !String(headline.changePct).startsWith("-") : false;
  stockRecordPrice.className = `stock-record-price ${headline ? (priceUp ? "up" : "down") : ""}`;
  stockRecordChange.textContent = headline ? `${headline.change} ${headline.changePct}` : "—";
  stockRecordChange.className = `stock-record-change ${headline ? (priceUp ? "up" : "down") : ""}`;
  const intervalText = headline?.tradingInterval ?? "—";
  if (stockRecordInterval) {
    stockRecordInterval.textContent = intervalText;
    stockRecordInterval.className = `stock-record-interval-value ${
      intervalText !== "—" && intervalText !== "--" && !String(intervalText).startsWith("-")
        ? "up"
        : intervalText !== "—" && intervalText !== "--"
          ? "down"
          : ""
    }`;
  }
  if (stockRecordAccountSelect) {
    stockRecordAccountSelect.value = activeAccountId;
    stockRecordAccountSelect.disabled = usePub;
    stockRecordAccountSelect.closest(".stock-record-account-wrap")?.classList.toggle("hidden", usePub);
  }
  if (stockRecordAddTradeBtn) {
    stockRecordAddTradeBtn.disabled = usePub;
    stockRecordAddTradeBtn.classList.toggle("hidden", usePub);
  }
  syncStockRecordRangeChipUi();

  const recTable = stockRecordListBody?.closest("table");
  const headRow = recTable?.querySelector("thead tr");
  if (recTable) {
    recTable.classList.toggle("stock-record-table--pub", usePub);
  }
  if (headRow) {
    headRow.innerHTML = usePub
      ? `<th>日期</th><th>类型</th><th>价格</th><th class="num stock-record-amt-th"><span class="stock-record-amt-th-inner">金额<span class="stock-rank-help-wrap stock-record-amt-help-wrap"><button type="button" class="stock-rank-help-btn" aria-expanded="false" aria-label="金额占比说明">?</button><div class="stock-rank-help-bubble" role="tooltip">本次交易金额占当前总资产比例</div></span></span></th><th>股票账户</th>`
      : "<th>日期</th><th>类型</th><th>价格</th><th>数量</th><th>发生金额</th><th>股票账户</th>";
  }

  const noteColspan = usePub ? 5 : 6;
  const tradeRowsHtml = symbolTrades
    .map((trade) => {
      const id = escapeHtml(String(trade.id));
      const rowCore = `
      <tr class="stock-record-trade-row trade-row--clickable" data-record-id="${id}">
        <td>${trade.date.replace(/-/g, "/")}</td>
        <td>${trade.side === "buy" ? "买入" : "卖出"}</td>
        <td>${formatNumber(trade.price, 2)}</td>`;
      const accCell = `<td class="trade-account-cell">${formatTradeAccountCellHtml(trade, usePub ? detail : null)}</td>`;
      if (usePub) {
        const share = publicTradeAmountShare(trade);
        const shareCell =
          share != null && Number.isFinite(share) ? formatPercent(share) : "—";
        return `${rowCore}
        <td class="num">${shareCell}</td>
        ${accCell}
      </tr>
      ${tradeRecordNoteSubrowHtml(trade.note, noteColspan, { "data-record-id": trade.id })}`;
      }
      return `${rowCore}
        <td>${formatNumber(trade.quantity, 0)}</td>
        <td class="${trade.side === "buy" ? "down" : "up"}">${trade.side === "buy" ? "-" : "+"}${formatNumber(
          trade.amount,
          2,
        )}</td>
        ${accCell}
      </tr>
      ${tradeRecordNoteSubrowHtml(trade.note, noteColspan, { "data-record-id": trade.id })}`;
    })
    .join("");
  const loadingFooter =
    !usePub && stockRecordTradesPager.loading
      ? tradeListLoadingRowHtml(noteColspan)
      : "";
  stockRecordListBody.innerHTML = tradeRowsHtml + loadingFooter;

  drawStockRecordCharts(symbol, symbolTrades);
}

async function ensureSymbolData(symbol) {
  try {
    const quoteMap = await fetchRealtimeQuotes([symbol]);
    const normalizedSymbol = normalizeSymbol(symbol);
    const legacyAlias = getLegacyUsAlias(normalizedSymbol);
    if (quoteMap[symbol]) {
      state.quoteMap[normalizedSymbol] = quoteMap[symbol];
      if (legacyAlias) {
        state.quoteMap[legacyAlias] = quoteMap[symbol];
      }
      state.quoteTime = pickLatestQuoteTime([state.quoteTime, quoteMap[symbol].time]);
      const nm = String(quoteMap[symbol]?.name || "").trim();
      const display = quoteNameForDisplay(normalizedSymbol, nm);
      if (display) {
        upsertNameMapEntry(normalizedSymbol, display);
      }
    }
  } catch (error) {
    console.error("加载个股实时行情失败", error);
  }
  if (!getQuoteBySymbol(symbol)?.current || !Number.isFinite(getQuoteBySymbol(symbol)?.current)) {
    const nSym = normalizeSymbol(symbol);
    if (nSym) {
      await fetchSymbolCloseIntoKlineMap([nSym], 90);
    }
    const latest = await fetchLatestQuoteFromDailyKlineFallback(symbol);
    if (latest) {
      const normalizedSymbol = normalizeSymbol(symbol);
      const legacyAlias = getLegacyUsAlias(normalizedSymbol);
      state.quoteMap[normalizedSymbol] = latest;
      if (legacyAlias) {
        state.quoteMap[legacyAlias] = latest;
      }
      state.quoteTime = pickLatestQuoteTime([state.quoteTime, latest.time]);
    }
  }

  if (!supportsKline(symbol)) {
    return;
  }
  const normalizedSymbol = normalizeSymbol(symbol);
  const legacyAlias = getLegacyUsAlias(normalizedSymbol);
  const sourceTrades = state.stockRecordTrades;
  const latestTradeDate = sourceTrades
    .filter((trade) => normalizeSymbol(trade?.symbol) === normalizedSymbol)
    .reduce((acc, trade) => {
      const d = String(trade?.date || "").slice(0, 10);
      return d && (!acc || d > acc) ? d : acc;
    }, "");

  try {
    const currentList = getKlineBySymbol(symbol);
    const currentLatestDay = latestKlineDay(currentList);
    const needRefreshKline = !currentList.length || (latestTradeDate && currentLatestDay < latestTradeDate);
    if (needRefreshKline) {
      const listRows = await (async () => {
        await fetchSymbolCloseIntoKlineMap([normalizedSymbol], 720);
        return getKlineBySymbol(symbol);
      })();
      const list = Array.isArray(listRows) ? listRows : [];
      if (list.length) {
        state.klineMap[normalizedSymbol] = list;
        if (legacyAlias) {
          state.klineMap[legacyAlias] = list;
        }
      }
    }
  } catch (error) {
    console.error("加载个股K线失败", error);
  }
}

function ensureSymbolPrefixForQuote(symbol) {
  const normalized = normalizeSymbol(symbol || "");
  if (/^sh600750$/i.test(normalized)) {
    return "sz300750";
  }
  return normalized;
}

function latestKlineDay(rows = []) {
  if (!Array.isArray(rows) || !rows.length) {
    return "";
  }
  let latest = "";
  rows.forEach((row) => {
    const day = String(row?.day || "").slice(0, 10);
    if (day && (!latest || day > latest)) {
      latest = day;
    }
  });
  return latest;
}

function mergeStockRecordPriceSeriesWithTradeDates(sourceRows, sortedTrades) {
  const source = Array.isArray(sourceRows) ? sourceRows : [];
  const trades = Array.isArray(sortedTrades) ? sortedTrades : [];
  const priceByDate = new Map();
  const tradePriceByDate = new Map();

  source.forEach((row) => {
    const date = String(row?.date || "").slice(0, 10);
    const price = Number(row?.price);
    if (!date || !Number.isFinite(price) || price <= 0) {
      return;
    }
    priceByDate.set(date, price);
  });
  trades.forEach((trade) => {
    const date = String(trade?.date || "").slice(0, 10);
    const price = Number(trade?.price);
    if (!date || !Number.isFinite(price) || price <= 0) {
      return;
    }
    // 同日多笔成交取最后一笔价格。
    tradePriceByDate.set(date, price);
  });

  const allDates = [...new Set([...priceByDate.keys(), ...tradePriceByDate.keys()])].sort();
  if (!allDates.length) {
    return [];
  }

  const merged = [];
  let rollingPrice = 0;
  allDates.forEach((date) => {
    const sourcePrice = priceByDate.get(date);
    const tradePrice = tradePriceByDate.get(date);
    if (Number.isFinite(sourcePrice) && sourcePrice > 0) {
      rollingPrice = sourcePrice;
    } else if (Number.isFinite(tradePrice) && tradePrice > 0) {
      rollingPrice = tradePrice;
    }
    if (!(rollingPrice > 0)) {
      rollingPrice = Number.isFinite(sourcePrice)
        ? sourcePrice
        : Number.isFinite(tradePrice)
          ? tradePrice
          : 0;
    }
    merged.push({ date, price: rollingPrice });
  });
  return merged;
}

function stockRecordVisibleSlice(source) {
  const totalCount = source.length;
  const windowSize = Math.max(
    STOCK_RECORD_CHART_MIN_WINDOW,
    Math.min(totalCount, Number(state.stockRecordWindow || ANALYSIS_CHART_DEFAULT_WINDOW)),
  );
  const maxOffset = Math.max(0, totalCount - windowSize);
  const offset = Math.max(0, Math.min(maxOffset, Number(state.stockRecordOffset || 0)));
  state.stockRecordOffset = offset;
  const end = totalCount - offset;
  const start = Math.max(0, end - windowSize);
  return { visible: source.slice(start, end), totalCount };
}

function drawStockRecordCharts(symbol, symbolTrades) {
  const bundlePoints = stockRecordChartPointsFromBundle(state.stockRecordBundle);
  if (bundlePoints.length) {
    drawStockRecordChartsFromBundle(symbol, symbolTrades, bundlePoints);
    return;
  }
  drawStockRecordChartLegacy(symbol, symbolTrades);
}

function drawStockRecordChartsFromBundle(symbol, symbolTrades, points) {
  const canvas = stockRecordChart;
  const profitCanvas = stockRecordProfitChart;
  const weightCanvas = stockRecordWeightChart;
  if (!canvas) {
    return;
  }
  const isPubChart = useCommunityPublicStockRecord();
  const fmtShares = (value) => formatNumber(value, isPubChart ? 4 : 0);
  const fmtMarket = (value) => formatNumber(value, isPubChart ? 4 : 2);
  const fmtClose = (value) => formatNumber(value, 3);
  const fmtProfit = (value) => (isPubChart ? formatNumber(value, 4) : formatSignedMoney(value, 2));
  const { visible, totalCount } = stockRecordVisibleSlice(points);
  if (!visible.length) {
    clearStockRecordChart();
    return;
  }

  const series = [];
  if (state.stockRecordShowClose) {
    series.push({
      key: "close",
      label: "收盘价",
      color: "#4091e0",
      axis: "left",
      values: visible.map((item) => ({ date: item.date, value: item.close ?? 0 })),
    });
  }
  if (state.stockRecordShowShares) {
    series.push({
      key: "shares",
      label: "持仓股数",
      color: "#ff4d4f",
      axis: "right",
      values: visible.map((item) => ({ date: item.date, value: item.shares ?? 0 })),
    });
  }
  if (state.stockRecordShowMarketValue) {
    series.push({
      key: "marketValue",
      label: "持仓市值",
      color: "#f59e0b",
      axis: "right",
      values: visible.map((item) => ({ date: item.date, value: item.marketValueNative ?? 0 })),
    });
  }
  if (!series.length) {
    const ctx = canvas.getContext("2d");
    if (ctx) {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
    }
    return;
  }

  if (!series.some((s) => s.axis === "left")) {
    series[0].axis = "left";
  }
  const yAxisMode =
    series.some((s) => s.axis === "left") && series.some((s) => s.axis === "right")
      ? "left-right"
      : "single";
  const payload = buildChartPayload(series, {
    labels: Object.fromEntries(series.map((s) => [s.key, s.label])),
    yAxisMode,
    xMin: 2,
    xMax: canvas.width - 2,
    yMin: 20,
    yMax: canvas.height - 36,
    yRangePadding: {
      minFactor: STOCK_RECORD_AXIS_MIN_FACTOR,
      maxFactor: STOCK_RECORD_AXIS_MAX_FACTOR,
    },
  });
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  drawChartGrid(ctx, canvas.width, canvas.height, payload);
  payload.seriesList.forEach((s) => {
    drawSeries(ctx, s.values, payload.mapX, payload.mapY, s.color || "#2f80f6");
  });
  series.forEach((s) => {
    const mapped = payload.seriesMap?.[s.key];
    if (!mapped?.values?.length) {
      return;
    }
    const fmt =
      s.key === "shares" ? fmtShares : s.key === "close" ? fmtClose : s.key === "marketValue" ? fmtMarket : fmtMarket;
    drawSeriesExtrema(ctx, payload, mapped, fmt);
  });
  drawAxisLabels(ctx, payload, {
    leftLabel: "",
    rightLabel: "",
    xLabel: "",
    valueFormatter: (value, axis, key) => {
      if (key === "shares") {
        return fmtShares(value);
      }
      if (key === "close") {
        return fmtClose(value);
      }
      if (key === "marketValue") {
        return fmtMarket(value);
      }
      return fmtMarket(value);
    },
  });
  drawCrosshairOverlay(ctx, payload, canvas.id, (value, key, axis) => {
    if (key === "shares") {
      return fmtShares(value);
    }
    if (key === "close") {
      return fmtClose(value);
    }
    if (key === "marketValue") {
      return fmtMarket(value);
    }
    return fmtMarket(value);
  });
  bindInteractiveChart(canvas, stockRecordTooltip, () => payload, {
    mode: "stock",
    onRefresh: () => drawStockRecordCharts(symbol, symbolTrades),
    onRedraw: () => drawStockRecordCharts(symbol, symbolTrades),
    chartNavTotal: () => totalCount,
    valueFormatter: (value, key, axis) => {
      if (key === "shares") {
        return fmtShares(value);
      }
      if (key === "close") {
        return fmtClose(value);
      }
      if (key === "marketValue") {
        return fmtMarket(value);
      }
      return fmtMarket(value);
    },
  });

  if (profitCanvas) {
    const profitPayload = buildChartPayload(
      [
        {
          key: "totalProfit",
          label: "持仓收益",
          color: "#6366f1",
          axis: "left",
          values: visible.map((item) => ({ date: item.date, value: item.totalProfit ?? 0 })),
        },
      ],
      {
        labels: { totalProfit: "持仓收益" },
        yAxisMode: "single",
        xMin: 2,
        xMax: profitCanvas.width - 2,
        yMin: 20,
        yMax: profitCanvas.height - 36,
        yRangePadding: {
          minFactor: STOCK_RECORD_AXIS_MIN_FACTOR,
          maxFactor: STOCK_RECORD_AXIS_MAX_FACTOR,
        },
      },
    );
    const pctx = profitCanvas.getContext("2d");
    pctx.clearRect(0, 0, profitCanvas.width, profitCanvas.height);
    drawChartGrid(pctx, profitCanvas.width, profitCanvas.height, profitPayload);
    profitPayload.seriesList.forEach((s) => {
      drawSeries(pctx, s.values, profitPayload.mapX, profitPayload.mapY, s.color || "#6366f1");
    });
    if (profitPayload.seriesMap?.totalProfit?.values?.length) {
      drawSeriesExtrema(pctx, profitPayload, profitPayload.seriesMap.totalProfit, fmtProfit);
    }
    drawAxisLabels(pctx, profitPayload, {
      leftLabel: "",
      rightLabel: "",
      xLabel: "",
      valueFormatter: fmtProfit,
    });
    drawCrosshairOverlay(pctx, profitPayload, profitCanvas.id, fmtProfit);
    bindInteractiveChart(profitCanvas, stockRecordProfitTooltip, () => profitPayload, {
      mode: "stock-profit",
      onRefresh: () => drawStockRecordCharts(symbol, symbolTrades),
      onRedraw: () => drawStockRecordCharts(symbol, symbolTrades),
      chartNavTotal: () => totalCount,
      valueFormatter: fmtProfit,
    });
  }

  if (!weightCanvas) {
    return;
  }
  const weightPayload = buildChartPayload(
    [
      {
        key: "weight",
        label: "持仓占比",
        color: "#10b981",
        axis: "left",
        values: visible.map((item) => ({ date: item.date, value: (item.weight ?? 0) * 100 })),
      },
    ],
    {
      labels: { weight: "持仓占比" },
      yAxisMode: "single",
      xMin: 2,
      xMax: weightCanvas.width - 2,
      yMin: 20,
      yMax: weightCanvas.height - 36,
      yRangePadding: {
        minFactor: STOCK_RECORD_AXIS_MIN_FACTOR,
        maxFactor: STOCK_RECORD_AXIS_MAX_FACTOR,
      },
    },
  );
  const wctx = weightCanvas.getContext("2d");
  wctx.clearRect(0, 0, weightCanvas.width, weightCanvas.height);
  drawChartGrid(wctx, weightCanvas.width, weightCanvas.height, weightPayload);
  weightPayload.seriesList.forEach((s) => {
    drawSeries(wctx, s.values, weightPayload.mapX, weightPayload.mapY, s.color || "#10b981");
  });
  if (weightPayload.seriesMap?.weight?.values?.length) {
    drawSeriesExtrema(wctx, weightPayload, weightPayload.seriesMap.weight, (value) => `${formatNumber(value, 2)}%`);
  }
  drawAxisLabels(wctx, weightPayload, {
    leftLabel: "",
    rightLabel: "",
    xLabel: "",
    valueFormatter: (value) => `${formatNumber(value, 2)}%`,
  });
  drawCrosshairOverlay(wctx, weightPayload, weightCanvas.id, (value) => `${formatNumber(value, 2)}%`);
  bindInteractiveChart(weightCanvas, stockRecordWeightTooltip, () => weightPayload, {
    mode: "stock-weight",
    onRefresh: () => drawStockRecordCharts(symbol, symbolTrades),
    onRedraw: () => drawStockRecordCharts(symbol, symbolTrades),
    chartNavTotal: () => totalCount,
    valueFormatter: (value) => `${formatNumber(value, 2)}%`,
  });
}

function drawStockRecordChartLegacy(symbol, symbolTrades) {
  const canvas = stockRecordChart;
  if (!canvas) {
    return;
  }
  const kline = getKlineBySymbol(symbol);
  const sortedTrades = [...symbolTrades].sort(sortTradeAsc);
  const baseSource =
    kline.length > 1
      ? kline.map((item) => ({ date: item.day, price: Number(item.close) }))
      : sortedTrades.map((item) => ({ date: item.date, price: validNumber(item.price, 0) }));
  const source = mergeStockRecordPriceSeriesWithTradeDates(baseSource, sortedTrades);
  if (!source.length) {
    const ctx = canvas.getContext("2d");
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    return;
  }
  const { visible, totalCount } = stockRecordVisibleSlice(source);
  const qtyByDate = {};
  let qty = 0;
  sortedTrades.forEach((trade) => {
    qty += trade.side === "buy" ? trade.quantity : -trade.quantity;
    qtyByDate[trade.date] = qty;
  });
  let rollingQty = 0;
  const firstVisibleDate = String(visible[0]?.date || "");
  if (firstVisibleDate) {
    sortedTrades.forEach((trade) => {
      if (String(trade.date || "") <= firstVisibleDate) {
        rollingQty += trade.side === "buy" ? trade.quantity : -trade.quantity;
      }
    });
  }
  const values = visible.map((item) => {
    if (qtyByDate[item.date] != null) {
      rollingQty = qtyByDate[item.date];
    }
    return { date: item.date, price: validNumber(item.price, 0), qty: rollingQty };
  });
  const rightLabel = "持仓股数";
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
        values: values.map((item) => ({ date: item.date, value: item.qty })),
      },
    ],
    {
      labels: { price: "股价", qty: rightLabel },
      yAxisMode: "left-right",
      xMin: 2,
      xMax: canvas.width - 2,
      yMin: 20,
      yMax: canvas.height - 36,
      yRangePadding: {
        minFactor: STOCK_RECORD_AXIS_MIN_FACTOR,
        maxFactor: STOCK_RECORD_AXIS_MAX_FACTOR,
      },
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
  if (payload.seriesMap?.price?.values?.length) {
    drawSeriesExtrema(ctx, payload, payload.seriesMap.price, (value) => formatNumber(value, 2));
  }
  if (payload.seriesMap?.qty?.values?.length) {
    drawSeriesExtrema(ctx, payload, payload.seriesMap.qty, (value) => formatNumber(value, 0));
  }
  drawAxisLabels(ctx, payload, {
    leftLabel: "",
    rightLabel: "",
    xLabel: "",
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
    onRefresh: () => drawStockRecordCharts(symbol, symbolTrades),
    onRedraw: () => drawStockRecordCharts(symbol, symbolTrades),
    chartNavTotal: () => totalCount,
    valueFormatter: (value, key, axis) => {
      if (key === "qty" || axis === "right") {
        return formatNumber(value, 0);
      }
      return formatNumber(value, 2);
    },
  });
}

function buildSymbolHistoryPoints(symbol, symbolTrades, fallbackPrice = 0) {
  const trades = Array.isArray(symbolTrades) ? symbolTrades : [];
  if (!trades.length) {
    return [{ date: toDateKey(new Date()), value: 0, flow: 0 }];
  }
  const todayKey = toDateKey(new Date());
  const startDate = String(trades[0].date || todayKey).slice(0, 10);
  const startMidParsed = new Date(`${startDate}T12:00:00`);
  const endMid = new Date(`${todayKey}T12:00:00`);
  const startMid = Number.isNaN(startMidParsed.getTime()) ? new Date(endMid) : startMidParsed;

  const dateKeys = [];
  const cursor = new Date(startMid);
  while (cursor <= endMid) {
    dateKeys.push(toDateKey(cursor));
    cursor.setDate(cursor.getDate() + 1);
  }

  const byDate = Object.create(null);
  for (const trade of trades) {
    if (!byDate[trade.date]) {
      byDate[trade.date] = [];
    }
    byDate[trade.date].push(trade);
  }

  const kline = getKlineBySymbol(symbol);
  const klineMap = Object.fromEntries(kline.map((item) => [item.day, Number(item.close)]));
  const quote = getQuoteBySymbol(symbol);
  let lastPrice = validNumber(
    quote.prevClose,
    trades[trades.length - 1]?.price,
    trades[0]?.price,
    fallbackPrice,
    0,
  );
  let quantity = 0;
  const points = [];

  for (const dateKey of dateKeys) {
    const dailyTrades = byDate[dateKey] || [];
    let flow = 0;
    for (const trade of dailyTrades) {
      quantity += trade.side === "buy" ? trade.quantity : -trade.quantity;
      flow += signedAmount(trade);
    }
    const dayClose = Number(klineMap[dateKey]);
    if (Number.isFinite(dayClose) && dayClose > 0) {
      lastPrice = dayClose;
    } else if (dateKey === todayKey) {
      const current = validNumber(quote.current, 0);
      if (current > 0) {
        lastPrice = current;
      }
    }
    points.push({
      date: dateKey,
      value: quantity * validNumber(lastPrice, 0),
      flow,
    });
  }
  return points;
}

function computeSymbolTotalRateByMode(position, symbolTrades, mode) {
  if (!position) {
    return 0;
  }
  const resolvedMode = normalizeProfitAlgoMode(mode);
  const trades = Array.isArray(symbolTrades) ? symbolTrades : [];
  if (!trades.length) {
    return Number.isFinite(position.profitRate) ? position.profitRate : 0;
  }
  const points = buildSymbolHistoryPoints(position.symbol, trades, position.currentPrice);
  const rate = computeModeSeries(points, resolvedMode).at(-1)?.rate;
  return Number.isFinite(rate) ? rate : Number.isFinite(position.profitRate) ? position.profitRate : 0;
}

function computePortfolio(trades = state.trades, cashTransfersForScope = null) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const ctf = Array.isArray(cashTransfersForScope)
    ? cashTransfersForScope
    : getFilteredCashTransfers(resolveValidAccountFilter(state.selectedAccountId));
  const grouped = new Map();
  const sortedTrades = [...tradeList].sort(sortTradeAsc);
  const groupedTrades = new Map();
  const activeAlgoMode = normalizeProfitAlgoMode(state.algoMode);

  for (const trade of sortedTrades) {
    if (!groupedTrades.has(trade.symbol)) {
      groupedTrades.set(trade.symbol, []);
    }
    groupedTrades.get(trade.symbol).push(trade);
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
    const todayKey = toDateKey(new Date());
    const dayCtx = getPositionDayTradeContext(item.symbol, todayKey, tradeList);
    const todayStartMarketValueNative = dayCtx.startQuantity * prevClose;
    const todayProfitNative = countTodayPnl
      ? dayCtx.endQuantity * currentPrice - todayStartMarketValueNative - dayCtx.dayFlowNative
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
      todayStartMarketValueNative,
      todayProfitNative,
      dayChangeRate,
      regretRate,
      totalRate: profitRate,
      totalProfit: totalProfitNative,
      todayProfit: todayProfitNative,
    };
  });

  positions.forEach((item) => {
    item.totalRate = computeSymbolTotalRateByMode(item, groupedTrades.get(item.symbol), activeAlgoMode);
  });

  positions.forEach((item) => {
    item.monthProfitNative = computePositionStageProfit(item, "month", tradeList);
    item.yearProfitNative = computePositionStageProfit(item, "ytd", tradeList);
    item.monthProfit = item.monthProfitNative;
    item.yearProfit = item.yearProfitNative;
  });
  const visiblePositions = positions.filter((item) => Math.abs(Number(item.quantity) || 0) > 1e-6);
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

  /** Σ发生：交易记录带符号折人民币累计（买正卖负），仍供其它逻辑/调试引用 */
  const sigmaAmountAll = tradeList.reduce(
    (sum, trade) => sum + signedAmount(trade) * getTradeFxRate(trade),
    0
  );
  const { cashCny, endingNativeByAccount } = computeLedgerCashAndPrincipal(tradeList, ctf);
  const totalMarketValueCnyBook = visiblePositions.reduce((sum, item) => sum + item.marketValue, 0);
  const cash = cashCny;

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
    (sum, item) => sum + toBook(item, item.todayStartMarketValueNative),
    0
  );
  const todayRate = yesterdayMarketValueForRate !== 0 ? todayProfit / yesterdayMarketValueForRate : 0;
  const totalProfit = sumBookByCurrency((item) => item.totalProfitNative);

  const isAll = state.selectedAccountId === "all";
  const sel = String(resolveValidAccountFilter(state.selectedAccountId));
  let principal;
  let overviewPrincipal;
  let overviewCash;
  if (isAll) {
    principal = aggregatePrincipalCnyAllAccountsAtSpot(ctf);
    overviewPrincipal = principal;
    overviewCash = cash;
  } else {
    const principalNat = principalNativeForFilteredAccount(ctf, sel);
    const acc = getAccountById(sel);
    const accCcy = String((acc && acc.currency) || "CNY").toUpperCase();
    overviewPrincipal = principalNat;
    overviewCash = Number(endingNativeByAccount.get(sel)) || 0;
    principal = accCcy === "CNY" ? principalNat : principalNat * getFxRateToCny(accCcy);
  }

  const totalAssets = totalMarketValue + overviewCash;
  const totalAssetsCny = totalMarketValueCnyBook + cash;
  const cashRatioPct = totalAssetsCny > 0 ? (cash / totalAssetsCny) * 100 : 0;

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
    totalMarketValueCnyBook,
    yesterdayMarketValue: yesterdayMarketValueForRate,
    cash,
    totalAssets,
    totalAssetsCny,
    cashRatioPct,
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

function normalizeProfitAlgoMode(mode) {
  const m = String(mode || "twr").toLowerCase();
  if (m === "money" || m === "mwr") return "mwr";
  return "twr";
}

function computeModeSeries(historyPoints, mode) {
  if (!historyPoints.length) {
    return [{ date: toDateKey(new Date()), rate: 0 }];
  }
  const m = normalizeProfitAlgoMode(mode);
  if (m === "twr") {
    return computeTimeWeightedSeries(historyPoints);
  }
  return computeMoneyWeightedSeries(historyPoints);
}

function rebaseRateSeriesByFirstDay(series) {
  if (!Array.isArray(series) || !series.length) {
    return [{ date: toDateKey(new Date()), rate: 0 }];
  }
  const baseRate = Number(series[0]?.rate) || 0;
  const denom = 1 + baseRate;
  return series.map((item, index) => {
    const raw = Number(item?.rate) || 0;
    if (!Number.isFinite(denom) || Math.abs(denom) < 1e-9) {
      return { date: item.date, rate: index === 0 ? 0 : raw - baseRate };
    }
    return {
      date: item.date,
      // 区间收益率口径：与区间首日相比，(1+r_t)/(1+r_0)-1；首日强制为 0。
      rate: index === 0 ? 0 : (1 + raw) / denom - 1,
    };
  });
}

function rebaseValueSeriesByFirstDay(series, valueKey = "value") {
  if (!Array.isArray(series) || !series.length) {
    return [{ date: toDateKey(new Date()), [valueKey]: 0 }];
  }
  const first = Number(series[0]?.[valueKey]) || 0;
  return series.map((item) => ({
    ...item,
    [valueKey]: (Number(item?.[valueKey]) || 0) - first,
  }));
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

function drawAnalysisMwrRatePlaceholder(canvas, message) {
  const target = canvas || analysisRateChart;
  if (!target) {
    return {
      seriesList: [],
      xMin: 2,
      xMax: 400,
      yMin: 20,
      yMax: 200,
      yAxisMode: "left",
      leftRange: { min: 0, max: 1, range: 1 },
      rightRange: { min: 0, max: 1, range: 1 },
      mapX: () => 0,
      mapY: () => 0,
      pickNearestByX() {
        return { index: 0, x: 0, points: [] };
      },
    };
  }
  const ctx = target.getContext("2d");
  const width = target.width;
  const height = target.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#8f99a9";
  ctx.font = "15px system-ui,sans-serif";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  const msg = String(message || "");
  ctx.fillText(msg, width / 2, height / 2);
  return {
    seriesList: [],
    xMin: 2,
    xMax: width - 2,
    yMin: 20,
    yMax: height - 36,
    yAxisMode: "left",
    leftRange: { min: 0, max: 1, range: 1 },
    rightRange: { min: 0, max: 1, range: 1 },
    mapX: () => 0,
    mapY: () => 0,
    pickNearestByX() {
      return { index: 0, x: width / 2, points: [] };
    },
  };
}

function drawLineChart(mySeries, benchmarkSeries, canvas) {
  const targetCanvas = canvas || analysisRateChart;
  return drawDualLineChart(
    targetCanvas,
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
      leftLabel: "",
      rightLabel: "",
      xLabel: "",
      valueFormatter: (value) => `${formatNumber(value, 2)}%`,
      axisFormatter: (value) => `${formatNumber(value, 2)}%`,
      yRangePadding: {
        minFactor: ANALYSIS_CHART_AXIS_MIN_FACTOR,
        maxFactor: ANALYSIS_CHART_AXIS_MAX_FACTOR,
      },
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
      xMin: options.xMin ?? 2,
      xMax: options.xMax ?? width - 2,
      yMin: options.yMin ?? 20,
      yMax: options.yMax ?? height - 36,
      yAxisMode: options.yAxisMode || (seriesB && seriesB.length ? "left-right" : "left"),
      axisByKey: options.axisByKey || {},
      yRangePadding: options.yRangePadding,
    }
  );
  drawChartGrid(ctx, width, height, payload);
  payload.seriesList.forEach((series) => {
    drawSeries(ctx, series.values, payload.mapX, payload.mapY, series.color || "#2f80f6");
  });
  const extremaFormatter =
    options.axisFormatter || options.valueFormatter || ((value) => formatNumber(value, 2));
  for (const extremaSeries of payload.seriesList || []) {
    if (extremaSeries?.values?.length) {
      drawSeriesExtrema(ctx, payload, extremaSeries, (value, key, axis) =>
        extremaFormatter(value, axis, key),
      );
    }
  }
  drawAxisLabels(ctx, payload, {
    leftLabel: options.leftLabel ?? "",
    rightLabel: options.rightLabel ?? "",
    xLabel: options.xLabel ?? "日期",
    valueFormatter: options.axisFormatter,
  });
  drawCrosshairOverlay(ctx, payload, canvas.id, options.valueFormatter || options.axisFormatter);
  return payload;
}

function drawSingleLineChart(canvas, series, color) {
  return drawDualLineChart(canvas, series, null, color, null);
}

function analysisSnapshotMoneyFromCny(cnyVal) {
  return amountBookFromCny(Number(cnyVal) || 0, getOverviewBookCurrency());
}

const ASSET_CHART_Y_MIN_FACTOR = 0.95;
const ASSET_CHART_Y_MAX_FACTOR = 1.05;

function drawAssetChart(assetSeries, canvas, trendMode, chartOpts = {}) {
  const targetCanvas = canvas || analysisAssetChart;
  const mode = trendMode != null ? trendMode : state.capitalTrendMode;
  const normalizedAmounts = chartOpts.normalizedAmounts === true;
  const fmtMoney = (v) => formatNumber(analysisSnapshotMoneyFromCny(v), 2);
  const fmtIndex = (v) => formatNumber(v, 4);
  const fmtAmount = normalizedAmounts ? fmtIndex : fmtMoney;
  const cfg =
    mode === "market"
      ? { key: "market", label: "总市值", color: "#4f83f1", fmt: fmtAmount }
      : mode === "cash"
        ? { key: "cash", label: "现金", color: "#27ae60", fmt: fmtAmount }
        : mode === "cash_ratio"
          ? { key: "cashRatio", label: "现金占比", color: "#9b59b6", fmt: (v) => `${formatNumber(v, 2)}%` }
          : { key: "totalAssets", label: "总资产", color: "#5f6c82", fmt: fmtAmount };
  const series = assetSeries.map((item) => ({
    date: item.date,
    value: Number(item[cfg.key]) || 0,
  }));
  const assetYScale = {
    yRangePadding: { minFactor: ASSET_CHART_Y_MIN_FACTOR, maxFactor: ASSET_CHART_Y_MAX_FACTOR },
  };
  return drawDualLineChart(targetCanvas, series, null, cfg.color, null, {
    keyA: cfg.key,
    labelA: cfg.label,
    yAxisMode: "left",
    leftLabel: "",
    xLabel: "",
    valueFormatter: cfg.fmt,
    axisFormatter: cfg.fmt,
    ...assetYScale,
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

function pickSeriesExtremaPoints(seriesValues) {
  if (!Array.isArray(seriesValues) || !seriesValues.length) {
    return { minPoint: null, maxPoint: null };
  }
  let minPoint = seriesValues[0];
  let maxPoint = seriesValues[0];
  for (const point of seriesValues) {
    if (!Number.isFinite(Number(point?.value))) {
      continue;
    }
    if (!minPoint || Number(point.value) < Number(minPoint.value)) {
      minPoint = point;
    }
    if (!maxPoint || Number(point.value) > Number(maxPoint.value)) {
      maxPoint = point;
    }
  }
  return { minPoint, maxPoint };
}

const CHART_AXIS_FONT = "13px sans-serif";
const CHART_EXTREMA_FONT = "13px sans-serif";
const CHART_CROSSHAIR_FONT = "13px sans-serif";
const CHART_EXTREMA_TEXT_COLOR = "#20262f";
const CHART_EXTREMA_H_GAP = 12;
const CHART_EXTREMA_V_GAP = 16;
const CHART_EXTREMA_TEXT_HEIGHT = 14;
const CHART_LABEL_BOX_HEIGHT = 20;

/** 文字矩形上离数据点最近的角（单条直线连到该角） */
function chartTextRectCornerTowardPoint(left, top, right, bottom, px, py) {
  const corners = [
    [left, top],
    [right, top],
    [left, bottom],
    [right, bottom],
  ];
  let best = corners[0];
  let bestLen = Infinity;
  for (const [cx, cy] of corners) {
    const len = Math.hypot(cx - px, cy - py);
    if (len < bestLen) {
      bestLen = len;
      best = [cx, cy];
    }
  }
  return best;
}

function drawSeriesExtrema(ctx, payload, series, valueFormatter) {
  if (!ctx || !payload || !series?.values?.length) {
    return;
  }
  const formatter = valueFormatter || ((value) => formatNumber(value, 2));
  const { minPoint, maxPoint } = pickSeriesExtremaPoints(series.values);
  const entries = [];
  if (maxPoint) {
    entries.push({ point: maxPoint, isMax: true });
  }
  if (minPoint) {
    const same =
      maxPoint &&
      minPoint.date === maxPoint.date &&
      Number(minPoint.value) === Number(maxPoint.value);
    if (!same) {
      entries.push({ point: minPoint, isMax: false });
    }
  }
  ctx.save();
  ctx.font = CHART_EXTREMA_FONT;
  ctx.textBaseline = "middle";
  ctx.fillStyle = CHART_EXTREMA_TEXT_COLOR;
  ctx.strokeStyle = CHART_EXTREMA_TEXT_COLOR;
  ctx.lineWidth = 1;
  ctx.setLineDash([]);
  const midX = (payload.xMin + payload.xMax) / 2;
  entries.forEach(({ point, isMax }) => {
    const rawText = formatter(point.value, point.key || series.key, point.axis || series.axis || "left");
    const text = String(rawText || "");
    if (!text) {
      return;
    }
    const textWidth = ctx.measureText(text).width;
    const placeLeft = point.x > midX;
    let textLeft = placeLeft ? point.x - CHART_EXTREMA_H_GAP - textWidth : point.x + CHART_EXTREMA_H_GAP;
    textLeft = Math.max(payload.xMin + 4, Math.min(payload.xMax - textWidth - 4, textLeft));
    const vSign = isMax ? -1 : 1;
    let labelY = point.y + vSign * CHART_EXTREMA_V_GAP;
    if (Math.abs(labelY - point.y) < CHART_EXTREMA_V_GAP * 0.5) {
      labelY = point.y + vSign * CHART_EXTREMA_V_GAP;
    }
    labelY = Math.max(payload.yMin + CHART_EXTREMA_TEXT_HEIGHT, Math.min(payload.yMax - CHART_EXTREMA_TEXT_HEIGHT, labelY));
    const textTop = labelY - CHART_EXTREMA_TEXT_HEIGHT / 2;
    const textBottom = labelY + CHART_EXTREMA_TEXT_HEIGHT / 2;
    const textRight = textLeft + textWidth;
    const [attachX, attachY] = chartTextRectCornerTowardPoint(
      textLeft,
      textTop,
      textRight,
      textBottom,
      point.x,
      point.y,
    );
    ctx.beginPath();
    ctx.moveTo(point.x, point.y);
    ctx.lineTo(attachX, attachY);
    ctx.stroke();
    ctx.textAlign = "left";
    ctx.fillText(text, textLeft, labelY);
  });
  ctx.restore();
}

function buildChartPayload(seriesList, options = {}) {
  const labels = options.labels || {};
  const axisByKey = options.axisByKey || {};
  const xMin = options.xMin ?? 20;
  const xMax = options.xMax ?? 680;
  const yMin = options.yMin ?? 20;
  const yMax = options.yMax ?? 300;
  const yAxisMode = options.yAxisMode || "left";
  const yRangePadding = options.yRangePadding;
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
    if (
      yRangePadding &&
      Number.isFinite(yRangePadding.minFactor) &&
      Number.isFinite(yRangePadding.maxFactor) &&
      values.length
    ) {
      const rawMin = Math.min(...values);
      const rawMax = Math.max(...values);
      if (Number.isFinite(rawMin) && Number.isFinite(rawMax)) {
        const min = rawMin * yRangePadding.minFactor;
        const max = rawMax * yRangePadding.maxFactor;
        return { min, max, range: Math.max(max - min, 1e-9) };
      }
    }
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
  const ticks = 4;
  ctx.save();
  ctx.fillStyle = "#8f99a9";
  ctx.font = CHART_AXIS_FONT;
  ctx.textBaseline = "middle";
  // 纵轴刻度标签统一绘制在坐标轴内侧，避免跑到画布外侧。
  for (let i = 0; i <= ticks; i += 1) {
    const ratio = i / ticks;
    const y = payload.yMin + (payload.yMax - payload.yMin) * ratio;
    const leftValue = payload.leftRange.max - payload.leftRange.range * ratio;
    ctx.textAlign = "left";
    ctx.fillText(valueFormatter(leftValue, "left"), payload.xMin + 4, y);
  }
  if (payload.yAxisMode === "left-right") {
    for (let i = 0; i <= ticks; i += 1) {
      const ratio = i / ticks;
      const y = payload.yMin + (payload.yMax - payload.yMin) * ratio;
      const rightValue = payload.rightRange.max - payload.rightRange.range * ratio;
      ctx.textAlign = "right";
      ctx.fillText(valueFormatter(rightValue, "right"), payload.xMax - 4, y);
    }
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
  ctx.fillText(startDate, payload.xMin, payload.yMax + 10);
  ctx.textAlign = "center";
  ctx.fillText(midDate, (payload.xMin + payload.xMax) / 2, payload.yMax + 10);
  ctx.textAlign = "right";
  ctx.fillText(endDate, payload.xMax, payload.yMax + 10);
  if (options.xLabel) {
    ctx.textAlign = "right";
    ctx.fillText(options.xLabel, payload.xMax, payload.yMax + 26);
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
  ctx.font = CHART_CROSSHAIR_FONT;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  const dateText = cross.date || "--";
  const dateWidth = Math.max(48, ctx.measureText(dateText).width + 12);
  const dateX = Math.max(payload.xMin + dateWidth / 2, Math.min(payload.xMax - dateWidth / 2, cross.x));
  const dateY = payload.yMax + 18;
  ctx.fillRect(dateX - dateWidth / 2, dateY - CHART_LABEL_BOX_HEIGHT / 2, dateWidth, CHART_LABEL_BOX_HEIGHT);
  ctx.fillStyle = "#fff";
  ctx.fillText(dateText, dateX, dateY);
  cross.points.forEach((point, index) => {
    const y = point.y;
    const side = point.axis === "right" ? "right" : "left";
    const text = formatter(point.value, point.key, point.axis);
    const w = Math.max(46, ctx.measureText(text).width + 12);
    const baseX = side === "right" ? payload.xMax - 6 - w / 2 : payload.xMin + 6 + w / 2;
    const x = Math.max(payload.xMin + w / 2 + 2, Math.min(payload.xMax - w / 2 - 2, baseX));
    ctx.fillStyle = "#4d5769";
    ctx.fillRect(x - w / 2, y - CHART_LABEL_BOX_HEIGHT / 2, w, CHART_LABEL_BOX_HEIGHT);
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
  if (existing && existing.canvas === canvas) {
    existing.payloadBuilder = payloadBuilder;
    existing.options = { ...existing.options, ...options };
    return existing;
  }
  if (existing && existing.canvas !== canvas) {
    if (typeof existing.dispose === "function") {
      existing.dispose();
    } else {
      chartRuntimeMap.delete(canvas.id);
    }
  }
  const pointerCtl = new AbortController();
  const signal = pointerCtl.signal;
  let pressing = false;
  let pressTimer = null;
  let activePointerId = null;
  let crossVisible = !!state.chartCrosshairMap[canvas.id];
  let startX = 0;
  let lastMoveX = 0;
  let moved = false;
  let panStarted = false;
  let panAnchorClientX = null;
  let panAnchorOffset = 0;
  let refreshRafId = 0;
  const pointers = new Map();
  const runtime = {
    canvas,
    payloadBuilder,
    options,
    hideCrosshair() {
      crossVisible = false;
      tooltip.classList.remove("show");
      delete state.chartCrosshairMap[canvas.id];
      requestRefresh("redraw");
    },
  };

  const clearPressTimer = () => {
    if (pressTimer) {
      window.clearTimeout(pressTimer);
      pressTimer = null;
    }
  };

  const requestRefresh = (reason = "redraw") => {
    if (refreshRafId) {
      return;
    }
    refreshRafId = window.requestAnimationFrame(() => {
      refreshRafId = 0;
      const mode = runtime.options.mode;
      if (
        (mode === "analysis" || isStockRecordChartMode(mode)) &&
        reason === "redraw" &&
        typeof runtime.options.onRedraw === "function"
      ) {
        runtime.options.onRedraw();
        return;
      }
      runtime.options.onRefresh?.();
    });
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
    requestRefresh("redraw");
    const formatter = runtime.options.valueFormatter || ((value) => formatNumber(value, 2));
    const rows = state.chartCrosshairMap[canvas.id].points
      .map((item) => `<div>${escapeHtml(item.label)}：${formatter(item.value, item.key, item.axis)}</div>`)
      .join("");
    tooltip.innerHTML = `<div>${escapeHtml(first.date)}</div>${rows}`;
    positionChartTooltip(tooltip, canvas, picked.x);
    tooltip.classList.add("show");
  };

  const chartNavTotalCount = () => {
    if (typeof runtime.options.chartNavTotal === "function") {
      return Number(runtime.options.chartNavTotal()) || 0;
    }
    return 0;
  };

  const chartPxPerPoint = () => {
    const payload = runtime.payloadBuilder?.();
    const visibleLen = payload?.seriesList?.[0]?.values?.length || 0;
    const plotW = canvas.getBoundingClientRect().width || canvas.width || 1;
    if (visibleLen <= 1) {
      return plotW;
    }
    return plotW / (visibleLen - 1);
  };

  const beginPanAnchor = (clientX) => {
    if (panAnchorClientX != null) {
      return;
    }
    panAnchorClientX = clientX;
    if (isStockRecordChartMode(runtime.options.mode)) {
      panAnchorOffset = Number(state.stockRecordOffset) || 0;
    } else if (runtime.options.mode === "analysis") {
      panAnchorOffset = Number(state.analysisPanOffset) || 0;
    }
  };

  const clearPanAnchor = () => {
    panAnchorClientX = null;
    panAnchorOffset = 0;
  };

  /** 锚点拖拽：按下时的屏幕点与数据窗口绑定，滑动多少像素平移多少 */
  const applyChartPanFromAnchor = (clientX) => {
    if (panAnchorClientX == null) {
      return;
    }
    const pxPerPoint = chartPxPerPoint();
    const step = Math.round((clientX - panAnchorClientX) / pxPerPoint);
    if (isStockRecordChartMode(runtime.options.mode)) {
      const total = chartNavTotalCount();
      const windowSize = Math.max(
        STOCK_RECORD_CHART_MIN_WINDOW,
        Math.min(total, Number(state.stockRecordWindow || ANALYSIS_CHART_DEFAULT_WINDOW)),
      );
      const maxOffset = Math.max(0, total - windowSize);
      const next = Math.max(0, Math.min(maxOffset, panAnchorOffset + step));
      if (next === Number(state.stockRecordOffset || 0)) {
        if (step > 0 && state.stockRecordChartPagination?.hasMore) {
          scheduleStockRecordChartHistoryLoad(state.activeRecordSymbol);
        }
        return;
      }
      state.stockRecordOffset = next;
      syncStockRecordRangeChipWithView();
      requestRefresh("redraw");
      if (next >= maxOffset && state.stockRecordChartPagination?.hasMore) {
        scheduleStockRecordChartHistoryLoad(state.activeRecordSymbol);
      }
      return;
    }
    if (runtime.options.mode === "analysis") {
      const total = chartNavTotalCount();
      const windowSize = Math.max(
        2,
        Math.min(total || ANALYSIS_CHART_MAX_WINDOW, Number(state.analysisChartWindow) || ANALYSIS_CHART_DEFAULT_WINDOW),
      );
      const maxOffset = Math.max(0, total - windowSize);
      const next = Math.max(0, Math.min(maxOffset, panAnchorOffset + step));
      if (next === Number(state.analysisPanOffset || 0)) {
        return;
      }
      state.analysisPanOffset = next;
      requestRefresh("redraw");
    }
  };

  canvas.addEventListener(
    "pointerdown",
    (event) => {
      event.preventDefault();
      canvas.setPointerCapture(event.pointerId);
      pointers.set(event.pointerId, event);
      activePointerId = event.pointerId;
      pressing = true;
      moved = false;
      panStarted = false;
      clearPanAnchor();
      startX = event.clientX;
      lastMoveX = event.clientX;
      clearPressTimer();
      pressTimer = window.setTimeout(() => {
        if (!pressing || moved || crossVisible) {
          return;
        }
        updateCrosshair(event.clientX, event.clientY);
      }, event.pointerType === "mouse" ? CHART_MOUSE_HOLD_MS : CHART_TOUCH_HOLD_MS);
    },
    { signal },
  );

  canvas.addEventListener(
    "pointermove",
    (event) => {
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
        const total = chartNavTotalCount();
        if (isStockRecordChartMode(runtime.options.mode)) {
          updateStockRecordWindowByScale(scale, total);
        } else if (runtime.options.mode === "analysis") {
          updateAnalysisWindowByScale(scale, total);
        }
        requestRefresh("redraw");
      }
      state.lastPinchDistanceMap[canvas.id] = distance;
      return;
    }
    crossVisible = !!state.chartCrosshairMap[canvas.id];
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
        beginPanAnchor(startX);
        lastMoveX = event.clientX;
        applyChartPanFromAnchor(event.clientX);
      }
    }
    },
    { signal },
  );

  const clearPointer = (event) => {
    event.preventDefault();
    if (canvas.hasPointerCapture(event.pointerId)) {
      canvas.releasePointerCapture(event.pointerId);
    }
    pointers.delete(event.pointerId);
    if (pointers.size < 2) {
      delete state.lastPinchDistanceMap[canvas.id];
    }
    pressing = false;
    moved = false;
    panStarted = false;
    lastMoveX = 0;
    clearPanAnchor();
    clearPressTimer();
    if (event.pointerType !== "mouse" && pointers.size === 0) {
      runtime.hideCrosshair();
    }
  };
  canvas.addEventListener("pointerup", clearPointer, { signal });
  canvas.addEventListener("pointercancel", clearPointer, { signal });
  canvas.addEventListener(
    "pointerleave",
    (event) => {
      clearPointer(event);
      if (event.pointerType === "mouse") {
        runtime.hideCrosshair();
      }
    },
    { signal },
  );
  runtime.dispose = () => {
    if (chartRuntimeMap.get(canvas.id) === runtime) {
      chartRuntimeMap.delete(canvas.id);
    }
    pointerCtl.abort();
    clearPressTimer();
    if (refreshRafId) {
      window.cancelAnimationFrame(refreshRafId);
      refreshRafId = 0;
    }
    delete state.chartCrosshairMap[canvas.id];
    if (state.lastPinchDistanceMap) {
      delete state.lastPinchDistanceMap[canvas.id];
    }
    tooltip.classList.remove("show");
  };
  chartRuntimeMap.set(canvas.id, runtime);
  return runtime;
}

function updateAnalysisWindowByScale(scale, totalPoints) {
  if (!Number.isFinite(scale) || scale === 1) {
    return;
  }
  const delta = scale > 1 ? -6 : 6;
  const total = Math.max(0, Number(totalPoints) || 0);
  const maxWindow = Math.max(ANALYSIS_CHART_MIN_WINDOW, total || ANALYSIS_CHART_MAX_WINDOW);
  state.analysisChartWindow = Math.max(
    ANALYSIS_CHART_MIN_WINDOW,
    Math.min(maxWindow, Number(state.analysisChartWindow || ANALYSIS_CHART_DEFAULT_WINDOW) + delta),
  );
  const maxOffset = Math.max(0, total - state.analysisChartWindow);
  state.analysisPanOffset = Math.max(0, Math.min(maxOffset, Number(state.analysisPanOffset || 0)));
}

function updateStockRecordWindowByScale(scale, totalPoints) {
  if (!Number.isFinite(scale) || scale === 1) {
    return;
  }
  const total = Math.max(0, Number(totalPoints) || 0);
  const currentWindow = Number(state.stockRecordWindow || ANALYSIS_CHART_DEFAULT_WINDOW);
  const zoomInStep = 6;
  const zoomOutStep = Math.max(6, Math.round(Math.max(currentWindow, STOCK_RECORD_CHART_MIN_WINDOW) * 0.2));
  const delta = scale > 1 ? -zoomInStep : zoomOutStep;
  const maxWindow = Math.max(STOCK_RECORD_CHART_MIN_WINDOW, total);
  let nextWindow = Math.max(STOCK_RECORD_CHART_MIN_WINDOW, Math.min(maxWindow, currentWindow + delta));
  if (total > 0 && nextWindow >= total) {
    nextWindow = total;
    state.stockRecordWindow = nextWindow;
    state.stockRecordOffset = 0;
    syncStockRecordRangeChipWithView();
    if (state.stockRecordChartPagination?.hasMore) {
      scheduleStockRecordChartHistoryLoad(state.activeRecordSymbol);
    }
    return;
  }
  state.stockRecordWindow = nextWindow;
  const maxOffset = Math.max(0, total - state.stockRecordWindow);
  state.stockRecordOffset = Math.max(0, Math.min(maxOffset, Number(state.stockRecordOffset || 0)));
  syncStockRecordRangeChipWithView();
}

async function refreshMarketData(opts = {}) {
  const skipFinalRender = opts.skipFinalRender === true;
  if (state.marketLoading) {
    return;
  }
  state.marketLoading = true;
  state.marketDataDelayed = false;
  state.marketDataDelaySource = "";

  try {
    const symbols = collectSymbolsForMarket();
    const klineSymbols = collectKlineSymbolsForMarket();
    void hydrateSymbolNameMap([...symbols, ...klineSymbols]);
    if (!symbols.length) {
      state.marketLoading = false;
      if (!skipFinalRender && state.route !== "analysis") {
        renderAll();
      }
      return;
    }

    // 仅拉实时：腾讯 quote + 腾讯外汇；历史日K由离线快照落库后读取 snapshot/symbol-close。
    try {
      const [quoteMap, fxRes] = await Promise.all([fetchRealtimeQuotes(symbols), fetchRealtimeForexSpot()]);
      if (fxRes?.delayed) {
        markMarketDataDelayed("fx-cache");
      }
      if (fxRes?.rates && typeof fxRes.rates === "object") {
        Object.assign(state.fxSpot, fxRes.rates);
      }
      if (Object.keys(quoteMap).length) {
        Object.entries(quoteMap).forEach(([symbol, quote]) => {
          const normalized = normalizeSymbol(symbol);
          const legacyAlias = getLegacyUsAlias(normalized);
          state.quoteMap[normalized] = quote;
          if (legacyAlias) {
            state.quoteMap[legacyAlias] = quote;
          }
          const nm = String(quote?.name || "").trim();
          const display = quoteNameForDisplay(normalized, nm);
          if (display) {
            upsertNameMapEntry(normalized, display);
          }
        });
        void syncSymbolNamesFromQuotes(quoteMap);
      }
      const latestSnapshotQuoteTime = pickLatestQuoteTime([
        state.quoteTime,
        ...Object.values(quoteMap).map((item) => item?.time),
      ]);
      if (latestSnapshotQuoteTime !== "--") {
        state.quoteTime = latestSnapshotQuoteTime;
      }
      if (!skipFinalRender && state.route !== "analysis") {
        renderAll();
      }
    } catch (error) {
      console.warn("首屏实时行情拉取失败，保留本地数据展示", error);
    }

    const missClose = klineSymbols.filter((s) => !Number.isFinite(getQuoteBySymbol(s)?.current));
    if (missClose.length) {
      await fetchSymbolCloseIntoKlineMap(missClose, 60, { parallelChunks: 6 });
    }
    let stillNoRealtime = klineSymbols.filter((s) => !Number.isFinite(getQuoteBySymbol(s)?.current));
    if (stillNoRealtime.length) {
      await fetchSymbolCloseIntoKlineMap(stillNoRealtime, 90, { parallelChunks: 6 });
    }
    stillNoRealtime = klineSymbols.filter((s) => !Number.isFinite(getQuoteBySymbol(s)?.current));
    const fbConc = 6;
    for (let i = 0; i < stillNoRealtime.length; i += fbConc) {
      const slice = stillNoRealtime.slice(i, i + fbConc);
      await Promise.all(
        slice.map(async (symbol) => {
          const latest = await fetchLatestQuoteFromDailyKlineFallback(symbol, { skipExtraKlineFetch: true });
          if (latest) {
            const normalized = normalizeSymbol(symbol);
            const legacyAlias = getLegacyUsAlias(normalized);
            state.quoteMap[normalized] = latest;
            if (legacyAlias) {
              state.quoteMap[legacyAlias] = latest;
            }
          }
        }),
      );
    }

    // 名称由腾讯实时批量结果填充；停用东财兜底，避免产生与行情无关的超时红项。
  } catch (error) {
    console.error("行情拉取失败，保留本地数据展示", error);
  } finally {
    state.marketLoading = false;
    if (!skipFinalRender && state.route !== "analysis") {
      renderAll();
      if (state.route === "community-profile" && state.communityProfileTab === "analysis" && state.lastPublicProfileDetail) {
        void openCommunityProfileAnalysisTab();
      }
    }
  }
}

/**
 * 实时行情失败时的兜底：用「库内日终收盘快照」最后两根 K 线算现价与昨收（仅走 snapshot/symbol-close，不在浏览器拉新浪日 K）。
 * 勿用分钟线相邻两根代替昨收，否则涨跌幅会变成「几分钟内波动」，出现约 0.08% 这类与当日真实涨跌严重不符的数。
 */
async function fetchLatestQuoteFromDailyKlineFallback(symbol, options = {}) {
  const skipExtraKlineFetch = options.skipExtraKlineFetch === true;
  try {
    let list = getKlineBySymbol(symbol);
    if (!Array.isArray(list) || list.length < 2) {
      if (skipExtraKlineFetch) {
        return null;
      }
      const n = normalizeSymbol(symbol);
      if (n) {
        await fetchSymbolCloseIntoKlineMap([n], 90);
        list = getKlineBySymbol(symbol);
      }
    }
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
  if (tRes?.delayed) {
    markMarketDataDelayed("quote-cache");
  }
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

async function fetchRealtimeQuotesTencent(symbols) {
  const uniqSymbols = [...new Set(symbols)];
  if (!uniqSymbols.length) {
    return {
      parsed: {},
      delayed: false,
    };
  }
  if (!apiReady) {
    return {
      parsed: {},
      delayed: true,
    };
  }
  const sourceToTarget = new Map();
  uniqSymbols.forEach((symbol) => {
    const key = toTencentQuoteSymbol(symbol);
    if (key) {
      sourceToTarget.set(key, symbol);
    }
  });
  if (!sourceToTarget.size) {
    return {
      parsed: {},
      delayed: false,
    };
  }
  const keysJoined = [...sourceToTarget.keys()].join(",");
  const parsed = {};
  let delayed = false;

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

  try {
    const r = await apiFetch(`${getApiBaseForFetch()}/quote/tencent?q=${encodeURIComponent(keysJoined)}`, {
      cache: "no-store",
      timeoutMs: 20_000,
    });
    readMarketDelayFromResponse(r);
    delayed = String(r.headers.get("x-market-data-delayed") || "") === "1";
    if (r.ok) {
      fillFromQuoteText(await r.text());
    }
  } catch {
    delayed = true;
  }
  return {
    parsed,
    delayed,
  };
}

/** 实时外汇：统一使用腾讯 qt（whUSDCNY / whHKDCNY）。 */
async function fetchRealtimeForexSpot() {
  return fetchRealtimeForexTencent().catch(() => ({ rates: {}, delayed: true }));
}

/** 腾讯 qt 外汇实时：USDCNY / HKDCNY 当前价。 */
async function fetchRealtimeForexTencent() {
  const out = {};
  const q = TENCENT_FOREX_SPOT_CODES.join(",");
  if (!apiReady) {
    return { rates: out, delayed: true };
  }
  let delayed = false;

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

  try {
    const r = await apiFetch(`${getApiBaseForFetch()}/quote/tencent?q=${encodeURIComponent(q)}`, {
      cache: "no-store",
      timeoutMs: 20_000,
    });
    readMarketDelayFromResponse(r);
    delayed = String(r.headers.get("x-market-data-delayed") || "") === "1";
    if (r.ok) {
      fillFromText(await r.text());
    }
  } catch {
    delayed = true;
  }

  return { rates: out, delayed };
}

function collectSymbolsForMarket() {
  const out = [];
  if (state.route === "community-profile" && state.lastPublicProfileDetail) {
    const detail = state.lastPublicProfileDetail;
    for (const p of detail.positions || []) {
      if (p?.symbol) {
        out.push(ensureSymbolPrefixForQuote(p.symbol));
      }
    }
    for (const p of detail.topPositions || []) {
      if (p?.symbol) {
        out.push(ensureSymbolPrefixForQuote(p.symbol));
      }
    }
  } else {
    const scope = getPortfolioScope(state.selectedAccountId);
    const portfolio = computePortfolio(scope.trades, scope.cashTransfers);
    for (const p of portfolio.visiblePositions || []) {
      if (p?.symbol) {
        out.push(ensureSymbolPrefixForQuote(p.symbol));
      }
    }
    if (state.activeRecordSymbol) {
      out.push(ensureSymbolPrefixForQuote(state.activeRecordSymbol));
    }
  }
  if (state.benchmark !== "none") {
    out.push(state.benchmark);
  }
  return [...new Set(out.filter(Boolean))];
}

function collectKlineSymbolsForMarket() {
  const out = [];
  if (state.route === "community-profile" && state.lastPublicProfileDetail) {
    const detail = state.lastPublicProfileDetail;
    for (const p of detail.positions || []) {
      if (p?.symbol) {
        out.push(ensureSymbolPrefixForQuote(p.symbol));
      }
    }
    for (const p of detail.topPositions || []) {
      if (p?.symbol) {
        out.push(ensureSymbolPrefixForQuote(p.symbol));
      }
    }
  } else {
    const scope = getPortfolioScope(state.selectedAccountId);
    const portfolio = computePortfolio(scope.trades, scope.cashTransfers);
    for (const p of portfolio.visiblePositions || []) {
      if (p?.symbol) {
        out.push(ensureSymbolPrefixForQuote(p.symbol));
      }
    }
    if (state.activeRecordSymbol) {
      out.push(ensureSymbolPrefixForQuote(state.activeRecordSymbol));
    }
  }
  if (state.benchmark !== "none") {
    out.push(state.benchmark);
  }
  return [...new Set(out.filter((sym) => supportsKline(sym)))];
}

function supportsKline(symbol) {
  return /^(sh|sz)\d{6}$/i.test(symbol) || /^hk\d{5}$/i.test(symbol) || /^gb_[a-z0-9._-]+$/i.test(symbol) || /^[a-z][a-z0-9._-]*$/i.test(symbol);
}

function normalizeTrade(input) {
  const trade = { ...input };
  const rawSym = trade.symbol || trade.name || "";
  trade.symbol = normalizeSymbol(rawSym);
  if (!trade.symbol && trade.name) {
    trade.symbol = normalizeSymbol(trade.name);
  }
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

function normalizeCashTransferRow(input) {
  const r = input && typeof input === "object" ? { ...input } : {};
  const id = String(r.id || "").trim();
  const direction = String(r.direction || "").toLowerCase() === "out" ? "out" : "in";
  return {
    id: id || crypto.randomUUID(),
    accountId: String(r.accountId || "default").trim() || "default",
    date: toDateKey(r.date || new Date()),
    direction,
    amount: Math.abs(Number(r.amount) || 0),
    note: String(r.note || "").trim(),
    createdAt: Number(r.createdAt) || Date.now(),
  };
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

const US_SYMBOL_ALIASES = {
  英伟达: "nvda",
  苹果: "aapl",
  特斯拉: "tsla",
  谷歌: "goog",
  alphabet: "goog",
  微软: "msft",
  亚马逊: "amzn",
  台积电: "tsm",
  nvidia: "nvda",
};

function normalizeSymbol(rawSymbol) {
  const value = String(rawSymbol || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
  if (!value) {
    return "";
  }
  if (value.startsWith("fx_") || /^wh(usd|hkd)cny$/.test(value) || value === "usdcny" || value === "hkdcny") {
    return value;
  }
  if (US_SYMBOL_ALIASES[value]) {
    return US_SYMBOL_ALIASES[value];
  }
  if (/^us\.([a-z][a-z0-9._-]*)$/i.test(value)) {
    const ticker = value
      .slice(3)
      .replace(/\.(oq|n)$/i, "")
      .toLowerCase();
    if (ticker) {
      return ticker;
    }
  }
  if (value.startsWith("us_")) {
    const ticker = value
      .slice(3)
      .replace(/\.(oq|n)$/i, "")
      .toLowerCase();
    return ticker || "";
  }
  if (value.startsWith("gb_")) {
    const ticker = value
      .slice(3)
      .replace(/\.(oq|n)$/i, "")
      .toLowerCase();
    return ticker || "";
  }
  if (/^us[a-z0-9._-]+$/i.test(value)) {
    const ticker = value
      .slice(2)
      .replace(/\.(oq|n)$/i, "")
      .toLowerCase();
    if (ticker && ticker !== "dcny" && ticker !== "hkdcny") {
      return ticker;
    }
  }
  if (value.startsWith("sh") || value.startsWith("sz") || value.startsWith("hk")) {
    return value;
  }
  if (value.startsWith("rt_hk")) {
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

function isUsTickerSymbol(symbol) {
  const s = String(symbol || "").trim().toLowerCase();
  if (!s) {
    return false;
  }
  if (
    s.startsWith("sh") ||
    s.startsWith("sz") ||
    s.startsWith("hk") ||
    s.startsWith("rt_hk") ||
    s.startsWith("fx_") ||
    /^wh(usd|hkd)cny$/.test(s) ||
    s === "usdcny" ||
    s === "hkdcny"
  ) {
    return false;
  }
  return /^[a-z][a-z0-9._-]*$/i.test(s);
}

function getLegacyUsAlias(symbol) {
  const normalized = normalizeSymbol(symbol);
  if (!isUsTickerSymbol(normalized)) {
    return "";
  }
  return `gb_${normalized}`;
}

function formatSymbolForDisplay(symbol) {
  const normalized = normalizeSymbol(symbol);
  if (!normalized) {
    return "";
  }
  if (/^rt_hk/i.test(normalized)) {
    const digits = normalized.replace(/^rt_hk_?/i, "").replace(/\D/g, "").padStart(5, "0");
    return `HK${digits}`;
  }
  return normalized.toUpperCase();
}

function inferMarket(symbol) {
  const s = String(symbol || "").trim().toLowerCase();
  if (!s) {
    return "其他";
  }
  if (s.startsWith("sh") || s.startsWith("sz")) {
    return "A股";
  }
  if (s.startsWith("hk") || s.startsWith("rt_hk")) {
    return "港股";
  }
  if (isUsTickerSymbol(s)) {
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
  const dk = String(dateKey || "").slice(0, 10);
  const today = toDateKey(new Date());
  if (!dk || dk >= today) {
    return getFxRateToCny(currency);
  }
  const mapByDate = state.fxRatesToCnyByDate || {};
  const keys = Object.keys(mapByDate).sort();
  if (!keys.length) {
    return getFxRateToCny(currency);
  }
  for (let i = keys.length - 1; i >= 0; i -= 1) {
    if (keys[i] <= dk) {
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

/** 交易表「交易方向」列：普通成交读 side；其它类型仍用 typeLabel */
function tradeDirectionCellLabel(trade) {
  const ty = trade.type || "trade";
  if (ty && ty !== "trade") {
    return typeLabel(ty);
  }
  return String(trade.side || "buy").toLowerCase() === "sell" ? "卖出" : "买入";
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
function formatStockTableMoney(row, valueNative, fraction = 2, displayMode = null) {
  const isCnyBook = row.market === "A股" || row.currency === "CNY";
  const display = applyFxForOverview(row, valueNative);
  const body = formatSignedMoney(display, fraction);
  const cnyOn = stockAmountDisplayIsCny(displayMode);
  if (cnyOn) {
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

function formatStockTableMarketValue(row, displayMode = null) {
  const isCnyBook = row.market === "A股" || row.currency === "CNY";
  const mvNative = Number.isFinite(Number(row.marketValueNative)) ? Number(row.marketValueNative) : 0;
  const display = applyFxForOverview(row, mvNative);
  const text = display.toFixed(2);
  const cnyOn = stockAmountDisplayIsCny(displayMode);
  if (cnyOn) {
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
  return `${safe > 0 ? "+" : ""}${num}%`;
}

function formatRegretRateWithSide(rate, side) {
  const normalizedSide = String(side || "").trim().toLowerCase();
  const suffix = normalizedSide === "buy" ? "B" : normalizedSide === "sell" ? "S" : "";
  const rateText = formatPercent(rate);
  return suffix ? `${rateText} ${suffix}` : rateText;
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
