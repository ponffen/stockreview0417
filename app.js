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
/** 与 performance_series_cache / PERFORMANCE_RULE_VERSION 一致；低于此版本则前端回退本地计算 */
const ANALYSIS_PERFORMANCE_RULE_VERSION = 2;
const SYMBOL_NAME_MAP_TTL_MS = 6 * 60 * 60 * 1000;
const SETTINGS_SYNC_DEBOUNCE_MS = 650;
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
const CHART_EDGE_SCROLL_PX = 22;
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
  dailyReturns: [],
  trades: [],
  /** 银证转账 / 出入金 */
  cashTransfers: [],
  /** 服务端 materialized 账户 KPI 展示态（analysis_daily_snapshot v3），按 accountScope 索引 */
  accountKpisByScope: {},
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
  stockRecordWindow: 30,
  stockRecordOffset: 0,
  chartCrosshairMap: {},
  lastPinchDistanceMap: {},
  fxRatesToCnyByDate: {},
  /** 腾讯 qt 外汇实时：USD / HKD → 兑 CNY 中间价 */
  fxSpot: {},
  fxLoaded: false,
  communityProfileStage: "month",
  communityProfileTab: "earning",
  /** 他人主页个股表排序（不影响首页） */
  publicStockSortKey: "default",
  publicStockSortOrder: "default",
  /** 他人持仓「分析」Tab 独立区间/基准，避免改动本人分析页 state */
  publicProfileAnalysisUi: null,
  /** 与 publicProfileAnalysisUi 同步：仅对对应 userId 种子化一次，切换用户时重置 */
  publicProfileAnalysisUiSeededFor: null,
  /** 查看他人主页时临时覆盖总览展示币种（与对方 selectedAccountId 一致） */
  _overviewBookCurrencyOverride: null,
  lastPublicProfileDetail: null,
  /** 个股记录页：true 时用 lastPublicProfileDetail.publicTrades 展示对方成交 */
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
const analysisDailyResponseCache = new Map();
const analysisDailyInFlight = new Map();
/** 与 account-daily 类似：合并并发、短缓存，避免连续 renderAll 打出两条相同 symbol-daily */
const symbolDailyResponseCache = new Map();
const symbolDailyInFlight = new Map();
const SYMBOL_DAILY_REMOTE_CACHE_TTL_MS = 30_000;
/** 首页 home-summary：合并并发 + 短复用，避免 startApp 连续多次 renderAll 打三遍 */
let homeSummaryFetchGen = 0;
const homeSummaryInflightByKey = new Map();
let homeSummaryRpcMemo = { key: "", data: null, at: 0 };
const HOME_SUMMARY_RPC_REUSE_MS = 8000;
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
let browserHistorySeeded = false;
let browserHistoryListenerBound = false;
let applyingBrowserRoutePopstate = false;
let lastBrowserRouteKey = "";
let lastRenderedRouteForScrollReset = "";
/** 用于离开/重新进入「收益」时失效首页日快照 UI 缓存 */
let previousRenderAllRouteForOverviewSnapshot = null;

/**
 * URL 加 ?snapshotUi=1：性能/对照测试——不跑 refreshMarketData（无实时、无 symbol-close），
 * 首页 KPI 仅用日终 account-daily + home-summary；个股表用 home-summary 行占位（不做全量成交重算）。
 */
function isSnapshotUiTestMode() {
  if (typeof window === "undefined" || !window.location || typeof window.location.search !== "string") {
    return false;
  }
  try {
    const raw = new URLSearchParams(window.location.search).get("snapshotUi");
    const v = String(raw || "").trim().toLowerCase();
    return v === "1" || v === "true" || v === "yes";
  } catch {
    return false;
  }
}

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
const stockRecordMarket = document.getElementById("stockRecordMarket");
const stockRecordRegret = document.getElementById("stockRecordRegret");
const stockRecordAccountSelect = document.getElementById("stockRecordAccountSelect");
const stockRecordListBody = document.getElementById("stockRecordListBody");
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
  persistState();
  // 首屏先渲染：外链可能长久 pending，Previously 在此 await 会卡住「加载中…」遮罩
  void hydrateSymbolNameMap(
    state.route === "earning" || state.route === "analysis"
      ? collectSymbolsForMarket()
      : normalizeSymbolList(state.trades.map((trade) => trade.symbol))
  ).then(() => {
    renderAll();
  });
  renderAll();
  if (state.route === "earning") {
    if (apiReady) {
      void refreshOverviewProfitRowFromSnapshots();
    }
  } else {
    void refreshMarketData({ skipFinalRender: true }).finally(() => {
      renderAll();
      if (state.route === "community-profile" && state.lastPublicProfileDetail?.publicTrades) {
        refreshPublicProfileEarningPanel();
        if (state.communityProfileTab === "analysis") {
          void renderPublicProfileAnalysis(state.lastPublicProfileDetail);
        }
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
  if (state.route !== "stock-record") {
    state.route = "earning";
  }
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

function buildAnalysisDailyFetchRange(scope) {
  const today = toDateKey(new Date());
  const to = shiftDateKeyByDays(today, 1);
  const trades = Array.isArray(scope?.trades) ? scope.trades : [];
  const earliestTradeDate = trades
    .map((trade) => toDateKey(trade?.date))
    .filter(Boolean)
    .sort()[0];
  if (state.analysisRangeMode === "custom") {
    const customStart =
      state.customRangeStart || earliestTradeDate || shiftDateKeyByDays(today, -365);
    return {
      from: shiftDateKeyByDays(customStart, -20),
      to,
    };
  }
  if (isAnalysisMtdPreset()) {
    return { from: shiftDateKeyByDays(monthToDateStartKey(), -10), to };
  }
  if (isAnalysisYtdPreset()) {
    return { from: shiftDateKeyByDays(ytdStartDateKey(), -20), to };
  }
  if (state.analysisRangeMode === "all") {
    return {
      from: shiftDateKeyByDays(earliestTradeDate || today, -20),
      to,
    };
  }
  return {
    from: shiftDateKeyByDays(today, -(Math.max(Number(state.rangeDays) || 30, 7) + 20)),
    to,
  };
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
  // trust /api/state even if the DB is empty — otherwise site-state.json would mask a real empty database.
  if (!apiReady) {
    staticParsed = await fetchStaticSiteState();
  }
  // 服务端可用时：始终以当前用户的 /api/state 为准（含空持仓）。
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
    if (parsed.accountKpisByScope && typeof parsed.accountKpisByScope === "object") {
      state.accountKpisByScope = { ...parsed.accountKpisByScope };
    }
    state.trades = Array.isArray(parsed.trades) ? parsed.trades.map(normalizeTrade) : [];
    state.cashTransfers = Array.isArray(parsed.cashTransfers)
      ? parsed.cashTransfers.map(normalizeCashTransferRow)
      : [];
    state.tradePanelTab = parsed.tradePanelTab === "cash" ? "cash" : "trades";
    state.dailyReturns = Array.isArray(parsed.dailyReturns)
      ? parsed.dailyReturns.map(normalizeDailyReturnRow)
      : [];
    state.appModule = parsed.appModule === "community" ? "community" : "holdings";
    if (
      bootstrapKind === "home" &&
      parsed.homeSummary &&
      parsed.homeSummary.account &&
      Array.isArray(parsed.homeSummary.symbols)
    ) {
      const memoKey = `${state.selectedAccountId}|${overviewTradesLedgerKey()}`;
      homeSummaryRpcMemo = { key: memoKey, data: parsed.homeSummary, at: Date.now() };
    }
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

function persistState() {
  let routeForSync = state.route;
  if (routeForSync === "trade-search" || routeForSync === "trade-records" || routeForSync === "trade-cash") {
    routeForSync = "trade";
  }
  const payload = {
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
  if (apiReady) {
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

/**
 * 个股记录等需要全量成交/银证时再拉（交易列表页走分页接口）。
 */
async function ensureFullLedgerDataLoaded() {
  if (!apiReady || !sessionPhone) {
    return;
  }
  const uid = String(sessionUserId || "").trim() || String(sessionPhone || "").trim();
  if (!uid) {
    return;
  }
  if (ledgerBootstrapCompleteForUid === uid) {
    return;
  }
  async function pullLedgerOnce() {
    const settled = await Promise.allSettled([
      apiFetch(`${API_BASE}/trades`, { cache: "no-store", timeoutMs: 25_000 }),
      apiFetch(`${API_BASE}/cash-transfers`, { cache: "no-store", timeoutMs: 25_000 }),
    ]);
    const parseOne = async (idx) => {
      const s = settled[idx];
      if (!s || s.status !== "fulfilled") {
        return { ok: false, body: {} };
      }
      const res = s.value;
      const body = await res.json().catch(() => ({}));
      return { ok: res.ok && body?.ok === true, body };
    };
    const [trP, cashP] = await Promise.all([parseOne(0), parseOne(1)]);
    const trOk = trP.ok && Array.isArray(trP.body.data);
    const cashOk = cashP.ok && Array.isArray(cashP.body.data);
    if (trOk) {
      state.trades = trP.body.data.map(normalizeTrade);
    }
    if (cashOk) {
      state.cashTransfers = cashP.body.data.map(normalizeCashTransferRow);
    }
    return { trOk, cashOk, coreOk: trOk && cashOk };
  }
  try {
    let { trOk, cashOk, coreOk } = await pullLedgerOnce();
    if (!coreOk) {
      await new Promise((r) => window.setTimeout(r, 350));
      ({ trOk, cashOk, coreOk } = await pullLedgerOnce());
    }
    if (coreOk) {
      ledgerBootstrapCompleteForUid = uid;
    }
  } catch (error) {
    console.warn("ensureFullLedgerDataLoaded failed", error);
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
}

function resetCashListPager() {
  cashListPager.gen += 1;
  cashListPager.offset = 0;
  cashListPager.hasMore = true;
  cashListPager.loading = false;
  cashListPager.loaded = false;
  cashListPager.accountId = resolveValidAccountFilter(state.tradeFilterAccountId);
  state.cashTransfers = [];
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
      }
    },
    { passive: true },
  );
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

async function importDailyReturnsToApi(rows, mode = "replace") {
  if (!apiReady || !Array.isArray(rows) || !rows.length) {
    return;
  }
  try {
    const response = await apiFetch(`${API_BASE}/daily-returns/import`, {
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
    const pubSortBtn = e.target.closest(".public-profile-stock-table .th-sort-btn");
    if (pubSortBtn && appShell.contains(pubSortBtn) && state.route === "community-profile") {
      const key = pubSortBtn.dataset.sortKey || "default";
      if (state.publicStockSortKey !== key) {
        state.publicStockSortKey = key;
        state.publicStockSortOrder = "desc";
      } else {
        state.publicStockSortOrder = cycleSortOrder(state.publicStockSortOrder);
        if (state.publicStockSortOrder === "default") {
          state.publicStockSortKey = "default";
        }
      }
      refreshPublicProfileEarningPanel();
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
          void renderPublicProfileAnalysis(state.lastPublicProfileDetail);
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
    void renderAnalysis();
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
    if (state.route === "stock-record" && state.activeRecordSymbol) {
      void renderStockRecordPage(state.activeRecordSymbol);
    }
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
        state.analysisPanOffset = 0;
      } else if (value === "mtd") {
        state.analysisRangeMode = "preset";
        state.analysisPreset = "mtd";
        state.analysisPanOffset = 0;
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
        state.analysisPanOffset = 0;
      }
      persistState();
      void renderAnalysis();
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
    state.analysisPanOffset = 0;
    persistState();
    renderControls();
    void renderAnalysis();
  });

  assetCurveModeSelect?.addEventListener("change", () => {
    state.capitalTrendMode = assetCurveModeSelect.value || "total_assets";
    persistState();
    if (cachedAnalysisAssetChartRows?.length && state.route === "analysis") {
      repaintAnalysisAssetChartFromCache();
      return;
    }
    void renderAnalysis();
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
    }
    state.trades.push(savedTrade);
    state.trades.sort(sortTradeAsc);
    persistState();
    clearEditState();
    tradeDialog.close();
    renderAll();
    invalidateTradeListAfterMutation();
    void refreshMarketData();
  });

  if (setCapitalBtn) {
    setCapitalBtn.addEventListener("click", () => {
      capitalDialog?.showModal();
    });
  }
  closeCapitalDialogBtn?.addEventListener("click", () => capitalDialog?.close());

  tradeTableBody?.addEventListener("click", (event) => {
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
      btn.closest(".stock-record-table--pub") ||
      btn.closest(".community-feed-card") ||
      btn.closest(".public-profile-trade-table");
    if (!host) {
      return;
    }
    e.preventDefault();
    e.stopPropagation();
    const wrap = btn.closest(".stock-rank-help-wrap");
    const bubble = wrap?.querySelector(".stock-rank-help-bubble");
    const wasOpen = bubble?.classList.contains("is-open");
    host.querySelectorAll(".stock-rank-help-bubble.is-open").forEach((el) => {
      el.classList.remove("is-open");
    });
    host.querySelectorAll(".stock-rank-help-btn").forEach((b) => {
      b.setAttribute("aria-expanded", "false");
    });
    if (!wasOpen && bubble) {
      bubble.classList.add("is-open");
      btn.setAttribute("aria-expanded", "true");
    }
  });
  document.addEventListener("click", (e) => {
    if (e.target.closest(".stock-rank-help-wrap")) {
      return;
    }
    document.querySelectorAll(".analysis-stock-rank-body .stock-rank-help-bubble.is-open").forEach((el) => {
      el.classList.remove("is-open");
    });
    document.querySelectorAll(".analysis-stock-rank-body .stock-rank-help-btn").forEach((b) => {
      b.setAttribute("aria-expanded", "false");
    });
    document.querySelectorAll(".stock-record-table--pub .stock-rank-help-bubble.is-open").forEach((el) => {
      el.classList.remove("is-open");
    });
    document.querySelectorAll(".stock-record-table--pub .stock-rank-help-btn").forEach((b) => {
      b.setAttribute("aria-expanded", "false");
    });
    document.querySelectorAll(".community-feed-card .stock-rank-help-bubble.is-open").forEach((el) => {
      el.classList.remove("is-open");
    });
    document.querySelectorAll(".community-feed-card .stock-rank-help-btn").forEach((b) => {
      b.setAttribute("aria-expanded", "false");
    });
    document.querySelectorAll(".public-profile-trade-table .stock-rank-help-bubble.is-open").forEach((el) => {
      el.classList.remove("is-open");
    });
    document.querySelectorAll(".public-profile-trade-table .stock-rank-help-btn").forEach((b) => {
      b.setAttribute("aria-expanded", "false");
    });
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
  }
  if (state.route === "earning" && prevSnap != null && prevSnap !== "earning") {
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
    void renderStockRecordPage(state.activeRecordSymbol);
  } else if (state.route === "community-profile") {
    refreshPublicProfileEarningPanel();
    if (state.communityProfileTab === "analysis" && state.lastPublicProfileDetail) {
      void renderPublicProfileAnalysis(state.lastPublicProfileDetail);
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
  state.publicStockSortKey = "weight";
  state.publicStockSortOrder = "desc";
  state.publicProfileAnalysisUi = null;
  state.publicProfileAnalysisUiSeededFor = null;
  state.lastPublicProfileDetail = null;
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

function buildTop3ListHtml(topPositions) {
  const top = (topPositions || []).slice(0, 3);
  if (!top.length) {
    return "";
  }
  const rows = top
    .map((p, i) => {
      const w = Number(p.weight);
      const right = Number.isFinite(w)
        ? `<span class="community-top3-pct">${(w * 100).toFixed(1)}%</span>`
        : "—";
      const code = escapeHtml(formatSymbolForDisplay(p.symbol || p.displayCode || ""));
      const tag = escapeHtml(p.marketTag || "OT");
      const tagLower = String(p.marketTag || "ot").toLowerCase();
      const stockName = escapeHtml(getDisplayName(p.symbol, p.name));
      return `<div class="community-top3-row">
        <span class="community-top3-rank">${i + 1}</span>
        <div class="community-top3-mid">
          <strong>${stockName}</strong>
          <div class="community-top3-stock-sub">
            <span class="community-market-tag community-market-tag--${tagLower}">${tag}</span>
            <span class="community-top3-code">${code}</span>
          </div>
        </div>
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
    <div class="community-metrics">
      <div class="community-metric-cell">
        <span class="community-metric-label">今日</span>
        ${formatTwrSignedHtml(card.todayTwr)}
      </div>
      <div class="community-metric-cell">
        <span class="community-metric-label">本月</span>
        ${formatTwrSignedHtml(card.mtdTwr)}
      </div>
      <div class="community-metric-cell">
        <span class="community-metric-label">本年</span>
        ${formatTwrSignedHtml(card.ytdTwr)}
      </div>
      <div class="community-metric-cell">
        <span class="community-metric-label">累计</span>
        ${formatTwrSignedHtml(card.totalTwr)}
      </div>
    </div>
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

function feedRowHtml(t) {
  const side = t.side === "sell" ? "sell" : "buy";
  const sideLabel = t.side === "sell" ? "卖出" : "买入";
  const uid = escapeHtml(t.userId);
  const tag = escapeHtml(t.marketTag || "OT");
  const tagLower = String(t.marketTag || "ot").toLowerCase();
  const code = escapeHtml(formatSymbolForDisplay(t.symbol || t.displayCode || ""));
  const priceStr =
    t.price != null && Number.isFinite(Number(t.price)) ? formatNumber(Number(t.price), 3) : "—";
  const share = t.amountShareOfCurrentTotalMv;
  const shareStr =
    share != null && Number.isFinite(Number(share)) ? formatPercent(Number(share)) : "—";
  const dateDisplay = String(t.date || "—").replace(/-/g, "\u2013");
  const stockName = escapeHtml(getDisplayName(t.symbol, t.name));
  const noteBlock = t.note
    ? `<p class="community-feed-note"><span class="community-feed-dt">备注：</span><span class="community-feed-dd">${escapeHtml(t.note)}</span></p>`
    : "";
  return `
    <article class="community-feed-card community-card--interactive" data-community-profile-card data-community-user="${uid}">
      <div class="community-feed-card__inner">
        <div class="community-feed-card__head">
          <span class="community-feed-user-name">${escapeHtml(t.displayName)}</span>
          <span class="community-feed-side-text community-feed-side-${side}">${sideLabel}</span>
        </div>
        <div class="community-feed-card__body">
          <div class="community-feed-card__col community-feed-card__col--stock">
            <strong class="community-feed-stock-name">${stockName}</strong>
            <div class="community-feed-stock-sub">
              <span class="community-market-tag community-market-tag--${tagLower}">${tag}</span>
              <span class="community-feed-stock-code">${code}</span>
            </div>
          </div>
          <div class="community-feed-card__col community-feed-card__col--detail">
            <div class="community-feed-kv">
              <span class="community-feed-kv-label">交易价格</span>
              <span class="community-feed-kv-value">${escapeHtml(priceStr)}</span>
            </div>
            <div class="community-feed-kv">
              <span class="community-feed-kv-label community-feed-kv-label--with-help">
                <span>金额</span>
                <span class="stock-rank-help-wrap community-feed-amt-help-wrap">
                  <button type="button" class="stock-rank-help-btn" aria-expanded="false" aria-label="金额占比说明">?</button>
                  <div class="stock-rank-help-bubble" role="tooltip">本次交易金额占当前总市值比例</div>
                </span>
              </span>
              <span class="community-feed-kv-value">${escapeHtml(shareStr)}</span>
            </div>
            <div class="community-feed-kv">
              <span class="community-feed-kv-label">交易日期</span>
              <span class="community-feed-kv-value">${escapeHtml(dateDisplay)}</span>
            </div>
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

/** 他人主页个股记录：真实人民币成交额 ÷ 对方当前总市值（最近快照，接口 amountCnyRaw、publicLatestMarketValueCny） */
function publicTradeAmountShareOfLatestMv(trade, detail) {
  const mv = Number(detail?.publicLatestMarketValueCny);
  const a = Math.abs(Number(trade.amountCnyRaw) || 0);
  if (!Number.isFinite(mv) || mv < 1e-9) {
    return null;
  }
  if (!Number.isFinite(a)) {
    return null;
  }
  return a / mv;
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

/** 分析区：总资产按区间内首日归一为 1（脱敏，仅形态） */
function paintPublicProfileMarketIndexChart(canvas, tooltipEl, selectedPoints, onDataRefresh) {
  if (!canvas || !selectedPoints?.length) {
    return;
  }
  const v0 = Number(selectedPoints[0]?.value);
  const denom = Math.abs(v0) > 1e-9 ? v0 : 1;
  const series = selectedPoints.map((p) => ({
    date: p.date,
    value: (Number(p.value) || 0) / denom,
  }));
  const chartOpts = {
    keyA: "mvIdx",
    labelA: "总资产指数",
    yAxisMode: "left",
    leftLabel: "",
    xLabel: "",
    valueFormatter: (value) => formatNumber(value, 4),
    axisFormatter: (value) => formatNumber(value, 3),
    yRangePadding: { minFactor: 0.92, maxFactor: 1.08 },
  };
  const payloads = { mkt: drawDualLineChart(canvas, series, null, "#4f83f1", null, chartOpts) };
  if (tooltipEl && onDataRefresh) {
    bindInteractiveChart(canvas, tooltipEl, () => payloads.mkt, {
      mode: "analysis",
      onRefresh: onDataRefresh,
      onRedraw: () => {
        payloads.mkt = drawDualLineChart(canvas, series, null, "#4f83f1", null, chartOpts);
      },
      valueFormatter: (val) => formatNumber(val, 4),
    });
  }
}

function paintPublicProfileCashRatioChart(canvas, tooltipEl, sliceRows, onDataRefresh) {
  if (!canvas || !sliceRows?.length) {
    return;
  }
  const series = sliceRows.map((r) => ({
    date: r.date,
    value: Number(r.cashRatio) || 0,
  }));
  const chartOpts = {
    keyA: "cashRatio",
    labelA: "现金占比",
    yAxisMode: "left",
    leftLabel: "",
    xLabel: "",
    valueFormatter: (value) => `${formatNumber(value, 2)}%`,
    axisFormatter: (value) => `${formatNumber(value, 2)}%`,
    yRangePadding: { minFactor: 0.92, maxFactor: 1.08 },
  };
  const payloads = { ratio: drawDualLineChart(canvas, series, null, "#9b59b6", null, chartOpts) };
  if (tooltipEl && onDataRefresh) {
    bindInteractiveChart(canvas, tooltipEl, () => payloads.ratio, {
      mode: "analysis",
      onRefresh: onDataRefresh,
      onRedraw: () => {
        payloads.ratio = drawDualLineChart(canvas, series, null, "#9b59b6", null, chartOpts);
      },
      valueFormatter: (val) => `${formatNumber(val, 2)}%`,
    });
  }
}

/** 与首页总览一致：用对方脱敏后的 trades + 资金/资产口径，在当前行情下重算（与本人「全部账户」视图对齐）。 */
function withPublicTradesContext(d, fn) {
  if (!d || !Array.isArray(d.publicTrades)) {
    return fn();
  }
  const prevTrades = state.trades;
  const prevAlgo = state.algoMode;
  const prevBook = state._overviewBookCurrencyOverride;
  state.trades = d.publicTrades;
  state.algoMode = "twr";
  const book = d.publicOverviewBookCurrency;
  if (book && typeof book === "string") {
    const c = book.toUpperCase();
    if (c === "USD" || c === "HKD" || c === "CNY") {
      state._overviewBookCurrencyOverride = c;
    } else {
      state._overviewBookCurrencyOverride = null;
    }
  } else {
    state._overviewBookCurrencyOverride = null;
  }
  try {
    return fn();
  } finally {
    state.trades = prevTrades;
    state.algoMode = prevAlgo;
    state._overviewBookCurrencyOverride = prevBook;
  }
}

async function withPublicTradesContextAsync(d, asyncFn) {
  if (!d || !Array.isArray(d.publicTrades) || typeof asyncFn !== "function") {
    return;
  }
  const prevTrades = state.trades;
  const prevAlgo = state.algoMode;
  const prevBook = state._overviewBookCurrencyOverride;
  state.trades = d.publicTrades;
  state.algoMode = "twr";
  const book = d.publicOverviewBookCurrency;
  if (book && typeof book === "string") {
    const c = book.toUpperCase();
    if (c === "USD" || c === "HKD" || c === "CNY") {
      state._overviewBookCurrencyOverride = c;
    } else {
      state._overviewBookCurrencyOverride = null;
    }
  } else {
    state._overviewBookCurrencyOverride = null;
  }
  try {
    await asyncFn();
  } finally {
    state.trades = prevTrades;
    state.algoMode = prevAlgo;
    state._overviewBookCurrencyOverride = prevBook;
  }
}

function renderPublicEarningProfileHtml(d) {
  if (!d || !Array.isArray(d.publicTrades)) {
    return `<p class="empty">暂无脱敏持仓数据</p>`;
  }
  return withPublicTradesContext(d, () => {
    const scope = { accountId: "all", trades: state.trades, cashTransfers: [] };
    const portfolio = computePortfolio(scope.trades, []);
    const vis = portfolio.visiblePositions;
    const bookCcy = portfolio.overviewBookCurrency || "CNY";
    const toOb = (p, v) => nativeToOverviewBook(p, v, bookCcy);
    const stageMap = { month: "month", ytd: "ytd", total: "total" };
    const sr = stageMap[state.communityProfileStage] || "month";
    const prevSr = state.stageRange;
    state.stageRange = sr;
    let stockRows = "";
    let todayInner = "";
    let todayCls = "profit-main";
    let stageInner = "";
    let stageCls = "profit-main";
    try {
      const snap =
        Array.isArray(d.analysisDaily) && d.analysisDaily.length
          ? computeStageOverviewFromSnapshotRows(d.analysisDaily, portfolio, scope, state.stageRange, state.algoMode)
          : null;
      const stageRateOv =
        snap && Number.isFinite(Number(snap.stageRate)) ? Number(snap.stageRate) : null;
      todayInner = formatPublicProfileRateOnlyHtml(portfolio.todayRate);
      todayCls = `profit-main ${twrColorClass(portfolio.todayRate)}`;
      stageInner = formatPublicProfileRateOnlyHtml(stageRateOv);
      stageCls = `profit-main ${twrColorClass(stageRateOv)}`;
      const listed = vis.filter(isPublicProfileActiveHoldingRow);
      const monthDenPub = listed.reduce((s, p) => s + Math.abs(toOb(p, p.monthProfitNative)), 0);
      const yearDenPub = listed.reduce((s, p) => s + Math.abs(toOb(p, p.yearProfitNative)), 0);
      const pubDenoms = { monthDenPub, yearDenPub };
      const rows = sortPublicProfileStockRows(
        listed,
        state.publicStockSortKey,
        state.publicStockSortOrder,
        bookCcy,
        scope.trades,
        pubDenoms,
      );
      stockRows =
        rows.length === 0
          ? `<tr><td colspan="9"><p class="empty">暂无持仓</p></td></tr>`
          : rows
              .map((row) => {
                const stockCode = formatSymbolForDisplay(row.symbol);
                const tag =
                  row.market === "A股" ? "CN" : row.market === "港股" ? "HK" : row.market === "美股" ? "US" : "OT";
                const tagLower = tag.toLowerCase();
                const toBk = (v) => nativeToOverviewBook(row, v, bookCcy);
                const monthWPub = monthDenPub !== 0 ? toBk(row.monthProfitNative) / monthDenPub : 0;
                const yearWPub = yearDenPub !== 0 ? toBk(row.yearProfitNative) / yearDenPub : 0;
                const changeClass = row.dayChangeRate >= 0 ? "up" : "down";
                const totalRateCls = row.totalRate >= 0 ? "up" : "down";
                const symEsc = escapeHtml(row.symbol);
                return `
        <tr>
          <td class="stock-name">
            <strong>${escapeHtml(getDisplayName(row.symbol, row.name))}</strong>
            <span><i class="market-tag market-tag--${tagLower}">${tag}</i> ${escapeHtml(stockCode)}</span>
          </td>
          <td>
            <div class="cell-main">${formatNumber(row.currentPrice, 3)}</div>
            <div class="cell-sub ${changeClass}">${formatPercent(row.dayChangeRate)}</div>
          </td>
          <td>${formatPercent(row.weight)}</td>
          <td>${formatNumber(row.cost, 3)}</td>
          <td>${formatPercent(monthWPub)}</td>
          <td>${formatPercent(yearWPub)}</td>
          <td class="${totalRateCls}">${formatPercent(row.totalRate)}</td>
          <td class="${row.regretRate >= 0 ? "up" : "down"}">${formatRegretRateWithSide(row.regretRate, row.lastTradeSide)}</td>
          <td><a href="javascript:void(0)" class="record-link" data-stock-record="${symEsc}">记录</a></td>
        </tr>`;
              })
              .join("");
    } finally {
      state.stageRange = prevSr;
    }
    return `
    <article class="overview-card community-profile-overview-min">
      <div class="profit-row">
        <div class="profit-block">
          <p class="profit-label">今日收益</p>
          <p id="pubTodayProfitMain" class="${todayCls}">${todayInner}</p>
        </div>
        <div class="profit-block">
          <label class="profit-label stage-select-wrap">
            <select id="pubStageRangeSelect" class="stage-select">
              <option value="month">本月收益</option>
              <option value="ytd">本年收益</option>
              <option value="total">总收益</option>
            </select>
            <span class="arrow">▼</span>
          </label>
          <p id="pubStageProfitMain" class="${stageCls}">${stageInner}</p>
        </div>
      </div>
    </article>
    <article class="stock-card">
      <div class="stock-head stock-head-row">
        <h2 class="stock-title">个股收益</h2>
      </div>
      <div class="table-scroll">
        <table class="stock-table public-profile-stock-table">
          <thead>
            <tr>
              <th class="name-head"><button type="button" class="th-sort-btn th-sort-btn--name" data-sort-key="symbol">名称<span class="sort-icon"></span></button></th>
              <th><button type="button" class="th-sort-btn" data-sort-key="currentPrice">现价/涨跌<span class="sort-icon"></span></button></th>
              <th><button type="button" class="th-sort-btn" data-sort-key="weight">仓位<span class="sort-icon"></span></button></th>
              <th><button type="button" class="th-sort-btn" data-sort-key="cost">成本<span class="sort-icon"></span></button></th>
              <th><button type="button" class="th-sort-btn" data-sort-key="monthWeight">月收益占比<span class="sort-icon"></span></button></th>
              <th><button type="button" class="th-sort-btn" data-sort-key="yearWeight">年收益占比<span class="sort-icon"></span></button></th>
              <th><button type="button" class="th-sort-btn" data-sort-key="totalRate">总收益率<span class="sort-icon"></span></button></th>
              <th><button type="button" class="th-sort-btn" data-sort-key="regretRate">交易间隔<span class="sort-icon"></span></button></th>
              <th><span class="th-sort-static">记录</span></th>
            </tr>
          </thead>
          <tbody>${stockRows}</tbody>
        </table>
      </div>
    </article>
  `;
  });
}

function bindPublicProfileStageSelect() {
  const sel = document.getElementById("pubStageRangeSelect");
  if (!sel) {
    return;
  }
  sel.value = state.communityProfileStage;
  sel.onchange = () => {
    state.communityProfileStage = sel.value;
    syncPublicProfileStageRow();
  };
  syncPublicProfileStageRow();
}

function syncPublicProfileStageRow() {
  const d = state.lastPublicProfileDetail;
  if (!d?.publicTrades) {
    return;
  }
  const sel = document.getElementById("pubStageRangeSelect");
  if (sel) {
    sel.value = state.communityProfileStage;
  }
  const stageMap = { month: "month", ytd: "ytd", total: "total" };
  const sr = stageMap[state.communityProfileStage] || "month";
  const main = document.getElementById("pubStageProfitMain");
  if (!main) {
    return;
  }
  withPublicTradesContext(d, () => {
    const scope = { accountId: "all", trades: state.trades, cashTransfers: [] };
    const portfolio = computePortfolio(scope.trades, []);
    const prevSr = state.stageRange;
    state.stageRange = sr;
    try {
      const snap =
        Array.isArray(d.analysisDaily) && d.analysisDaily.length
          ? computeStageOverviewFromSnapshotRows(d.analysisDaily, portfolio, scope, sr, state.algoMode)
          : null;
      const stageRateOv =
        snap && Number.isFinite(Number(snap.stageRate)) ? Number(snap.stageRate) : null;
      main.innerHTML = formatPublicProfileRateOnlyHtml(stageRateOv);
      main.className = `profit-main ${twrColorClass(stageRateOv)}`;
    } finally {
      state.stageRange = prevSr;
    }
  });
}

function ensurePublicProfileAnalysisUi() {
  if (!state.publicProfileAnalysisUi) {
    state.publicProfileAnalysisUi = {
      analysisRangeMode: "preset",
      analysisPreset: null,
      rangeDays: 30,
      analysisPanOffset: 0,
      customRangeStart: "",
      customRangeEnd: "",
      customRangeDraftStart: "",
      customRangeDraftEnd: "",
      benchmark: "none",
      capitalTrendMode: "total_assets",
    };
  }
  return state.publicProfileAnalysisUi;
}

/** 用接口下发的对方设置初始化「分析」控件，使默认曲线/基准与本人首页一致。 */
function seedPublicProfileAnalysisUiFromDetail(d) {
  if (!d || d.isSelf || !Array.isArray(d.publicTrades)) {
    return;
  }
  const uid = String(d.userId || "");
  if (state.publicProfileAnalysisUiSeededFor !== uid) {
    state.publicProfileAnalysisUi = null;
    state.publicProfileAnalysisUiSeededFor = uid;
  }
  const ui = ensurePublicProfileAnalysisUi();
  const bench = String(d.publicBenchmark || "none");
  ui.benchmark = ALLOWED_PUBLIC_BENCHMARKS.has(bench) ? bench : "none";
  const tm = String(d.publicCapitalTrendMode || "total_assets");
  ui.capitalTrendMode =
    tm === "market" || tm === "cash" || tm === "cash_ratio" || tm === "total_assets"
      ? tm
      : "total_assets";
  const arm = String(d.publicAnalysisRangeMode || "preset");
  ui.analysisRangeMode = ["preset", "custom", "all"].includes(arm) ? arm : "preset";
  ui.analysisPreset = d.publicAnalysisPreset ?? null;
  ui.rangeDays = Number(d.publicRangeDays) || 30;
  const po = Number(d.publicAnalysisPanOffset);
  ui.analysisPanOffset = Number.isFinite(po) ? Math.max(0, po) : 0;
  ui.customRangeStart = String(d.publicCustomRangeStart || "");
  ui.customRangeEnd = String(d.publicCustomRangeEnd || "");
  ui.customRangeDraftStart = ui.customRangeStart;
  ui.customRangeDraftEnd = ui.customRangeEnd;
}

function withPublicProfileAnalysisUi(fn) {
  const ui = ensurePublicProfileAnalysisUi();
  const snap = {
    analysisRangeMode: state.analysisRangeMode,
    analysisPreset: state.analysisPreset,
    rangeDays: state.rangeDays,
    analysisPanOffset: state.analysisPanOffset,
    customRangeStart: state.customRangeStart,
    customRangeEnd: state.customRangeEnd,
    customRangeDraftStart: state.customRangeDraftStart,
    customRangeDraftEnd: state.customRangeDraftEnd,
    benchmark: state.benchmark,
    capitalTrendMode: state.capitalTrendMode,
  };
  Object.assign(state, {
    analysisRangeMode: ui.analysisRangeMode,
    analysisPreset: ui.analysisPreset,
    rangeDays: ui.rangeDays,
    analysisPanOffset: ui.analysisPanOffset,
    customRangeStart: ui.customRangeStart,
    customRangeEnd: ui.customRangeEnd,
    customRangeDraftStart: ui.customRangeDraftStart,
    customRangeDraftEnd: ui.customRangeDraftEnd,
    benchmark: ui.benchmark,
    capitalTrendMode: ui.capitalTrendMode,
  });
  try {
    return fn();
  } finally {
    Object.assign(state, snap);
  }
}

async function withPublicProfileAnalysisUiAsync(asyncFn) {
  const ui = ensurePublicProfileAnalysisUi();
  const snap = {
    analysisRangeMode: state.analysisRangeMode,
    analysisPreset: state.analysisPreset,
    rangeDays: state.rangeDays,
    analysisPanOffset: state.analysisPanOffset,
    customRangeStart: state.customRangeStart,
    customRangeEnd: state.customRangeEnd,
    customRangeDraftStart: state.customRangeDraftStart,
    customRangeDraftEnd: state.customRangeDraftEnd,
    benchmark: state.benchmark,
    capitalTrendMode: state.capitalTrendMode,
  };
  Object.assign(state, {
    analysisRangeMode: ui.analysisRangeMode,
    analysisPreset: ui.analysisPreset,
    rangeDays: ui.rangeDays,
    analysisPanOffset: ui.analysisPanOffset,
    customRangeStart: ui.customRangeStart,
    customRangeEnd: ui.customRangeEnd,
    customRangeDraftStart: ui.customRangeDraftStart,
    customRangeDraftEnd: ui.customRangeDraftEnd,
    benchmark: ui.benchmark,
    capitalTrendMode: ui.capitalTrendMode,
  });
  try {
    await asyncFn();
  } finally {
    Object.assign(state, snap);
  }
}

function syncCommunityProfileAnalysisControls() {
  const root = document.getElementById("pubAnalysisRoot");
  if (!root) {
    return;
  }
  const ui = ensurePublicProfileAnalysisUi();
  root.querySelectorAll(".range-chip").forEach((chip) => {
    const value = chip.dataset.range;
    let active = false;
    if (value === "custom") {
      active = ui.analysisRangeMode === "custom";
    } else if (value === "all") {
      active = ui.analysisRangeMode === "all";
    } else if (value === "mtd") {
      active = ui.analysisRangeMode === "preset" && ui.analysisPreset === "mtd";
    } else if (value === "365") {
      active = ui.analysisRangeMode === "preset" && ui.analysisPreset === "ytd";
    } else {
      active =
        ui.analysisRangeMode === "preset" &&
        ui.analysisPreset !== "mtd" &&
        ui.analysisPreset !== "ytd" &&
        Number(value) === ui.rangeDays;
    }
    chip.classList.toggle("active", active);
  });
  const cr = document.getElementById("pubCustomRangeRow");
  if (cr) {
    cr.classList.toggle("hidden", ui.analysisRangeMode !== "custom");
  }
  const sIn = document.getElementById("pubCustomRangeStart");
  const eIn = document.getElementById("pubCustomRangeEnd");
  if (sIn) {
    sIn.value =
      ui.analysisRangeMode === "custom" ? ui.customRangeDraftStart || "" : ui.customRangeStart || "";
  }
  if (eIn) {
    eIn.value =
      ui.analysisRangeMode === "custom" ? ui.customRangeDraftEnd || "" : ui.customRangeEnd || "";
  }
  const bs = document.getElementById("pubBenchmarkSelect");
  if (bs) {
    bs.value = ui.benchmark;
  }
}

function getPublicProfileAnalysisSectionHtml() {
  return `
    <article class="panel">
      <div class="panel-head">
        <h2>分析范围</h2>
      </div>
      <div class="form-row">
        <label for="pubBenchmarkSelect">指数对比</label>
        <select id="pubBenchmarkSelect">
          <option value="none">不对比</option>
          <option value="sh000001">上证指数</option>
          <option value="sz399001">深证成指</option>
          <option value="rt_hkHSI">恒生指数</option>
          <option value="gb_inx">标普500</option>
        </select>
      </div>
      <div id="pubAnalysisRoot">
        <div class="range-row">
          <button type="button" class="range-chip" data-range="7">最近一周</button>
          <button type="button" class="range-chip active" data-range="30">最近一月</button>
          <button type="button" class="range-chip" data-range="90">最近三月</button>
          <button type="button" class="range-chip" data-range="mtd">月初至今</button>
          <button type="button" class="range-chip" data-range="365">年初至今</button>
          <button type="button" class="range-chip" data-range="all">历史以来</button>
          <button type="button" class="range-chip" data-range="custom">自定义</button>
        </div>
        <div id="pubCustomRangeRow" class="custom-range-row hidden">
          <input id="pubCustomRangeStart" type="date" />
          <span>至</span>
          <input id="pubCustomRangeEnd" type="date" />
          <button id="pubApplyCustomRangeBtn" type="button" class="btn btn-ghost">应用</button>
        </div>
      </div>
    </article>
    <article class="panel">
      <div class="panel-head">
        <h2>收益率走势</h2>
        <span id="pubAnalysisRateSummary" class="caption"></span>
      </div>
      <div class="chart-wrap">
        <canvas id="pubAnalysisRateChart" width="700" height="320"></canvas>
        <div id="pubAnalysisRateTooltip" class="chart-tooltip"></div>
      </div>
    </article>
    <article class="panel">
      <div class="panel-head">
        <h2>总资产走势</h2>
        <span class="caption">以首日为基数1，不展示真实金额</span>
      </div>
      <div class="chart-wrap">
        <canvas id="pubAnalysisMarketIndexChart" width="700" height="320"></canvas>
        <div id="pubAnalysisMarketIndexTooltip" class="chart-tooltip"></div>
      </div>
    </article>
    <article class="panel">
      <div class="panel-head">
        <h2>现金占比</h2>
        <span class="caption">现金 / 总资产 × 100%</span>
      </div>
      <div class="chart-wrap">
        <canvas id="pubAnalysisCashRatioChart" width="700" height="280"></canvas>
        <div id="pubAnalysisCashRatioTooltip" class="chart-tooltip"></div>
      </div>
    </article>
    <article class="panel analysis-stock-rank-panel">
      <div class="panel-head">
        <h2>个股收益排行</h2>
      </div>
      <div id="pubAnalysisStockRankBody" class="analysis-stock-rank-body analysis-stock-rank-body--public"></div>
    </article>
  `;
}

function getPublicProfileTradeSectionHtml() {
  return `
    <article class="panel community-profile-trade-panel">
      <div class="trade-table-wrap">
        <table class="trade-table public-profile-trade-table">
          <thead>
            <tr>
              <th>日期</th>
              <th class="pub-trade-col-name">名称</th>
              <th>交易方向</th>
              <th>价格</th>
              <th class="num pub-trade-amt-th">
                <span class="pub-trade-amt-th-inner">
                  金额
                  <span class="stock-rank-help-wrap pub-trade-amt-help-wrap">
                    <button type="button" class="stock-rank-help-btn" aria-expanded="false" aria-label="金额占比说明">?</button>
                    <div class="stock-rank-help-bubble" role="tooltip">本次交易金额占当前总市值比例</div>
                  </span>
                </span>
              </th>
            </tr>
          </thead>
          <tbody id="pubTradeTableBody"></tbody>
        </table>
      </div>
    </article>
  `;
}

function renderCommunityProfilePageHtml(d) {
  const tab = state.communityProfileTab || "earning";
  const earningInner = renderPublicEarningProfileHtml(d);
  return `
    <div class="community-profile-tab-panel ${tab === "earning" ? "is-active" : ""}" data-profile-panel="earning">${earningInner}</div>
    <div class="community-profile-tab-panel ${tab === "analysis" ? "is-active" : ""}" data-profile-panel="analysis">${getPublicProfileAnalysisSectionHtml()}</div>
    <div class="community-profile-tab-panel ${tab === "trade" ? "is-active" : ""}" data-profile-panel="trade">${getPublicProfileTradeSectionHtml()}</div>
  `;
}

function renderPublicTradeTable(d) {
  const tb = document.getElementById("pubTradeTableBody");
  if (!tb || !d?.publicTrades) {
    return;
  }
  const list = [...d.publicTrades].sort(sortTradeDesc);
  if (!list.length) {
    tb.innerHTML = `
      <tr>
        <td colspan="5"><p class="empty">暂无交易记录</p></td>
      </tr>
    `;
    return;
  }
  tb.innerHTML = list
    .map((trade) => {
      const share = publicTradeAmountShareOfLatestMv(trade, d);
      const shareStr =
        share != null && Number.isFinite(share) ? formatPercent(share) : "—";
      return `
        <tr class="trade-row">
          <td>${trade.date.replace(/-/g, "/")}</td>
          <td class="pub-trade-col-name">${escapeHtml(getDisplayName(trade.symbol, trade.name))}</td>
          <td class="type-cell">${tradeDirectionCellLabel(trade)}</td>
          <td class="num">${formatNumber(trade.price, 2)}</td>
          <td class="num">${shareStr}</td>
        </tr>
      `;
    })
    .join("");
}

function bindPublicProfileAnalysisInteractions(d) {
  const ui = ensurePublicProfileAnalysisUi();
  const root = document.getElementById("pubAnalysisRoot");
  if (root) {
    root.querySelectorAll(".range-chip").forEach((chip) => {
      chip.addEventListener("click", () => {
        const value = chip.dataset.range;
        if (value === "custom") {
          ui.analysisRangeMode = "custom";
          ui.analysisPreset = null;
          ui.customRangeDraftStart = ui.customRangeStart;
          ui.customRangeDraftEnd = ui.customRangeEnd;
        } else if (value === "all") {
          ui.analysisRangeMode = "all";
          ui.analysisPreset = null;
          ui.analysisPanOffset = 0;
        } else if (value === "mtd") {
          ui.analysisRangeMode = "preset";
          ui.analysisPreset = "mtd";
          ui.analysisPanOffset = 0;
        } else {
          ui.analysisRangeMode = "preset";
          const n = Number(value);
          if (n === 365) {
            ui.analysisPreset = "ytd";
            ui.rangeDays = 365;
          } else {
            ui.analysisPreset = null;
            ui.rangeDays = n;
          }
          ui.analysisPanOffset = 0;
        }
        syncCommunityProfileAnalysisControls();
        void renderPublicProfileAnalysis(d);
      });
    });
  }
  const sIn = document.getElementById("pubCustomRangeStart");
  const eIn = document.getElementById("pubCustomRangeEnd");
  const syncDraft = () => {
    if (sIn) {
      ui.customRangeDraftStart = sIn.value || "";
    }
    if (eIn) {
      ui.customRangeDraftEnd = eIn.value || "";
    }
  };
  sIn?.addEventListener("input", syncDraft);
  sIn?.addEventListener("change", syncDraft);
  eIn?.addEventListener("input", syncDraft);
  eIn?.addEventListener("change", syncDraft);
  document.getElementById("pubApplyCustomRangeBtn")?.addEventListener("click", () => {
    syncDraft();
    let start = ui.customRangeDraftStart || "";
    let end = ui.customRangeDraftEnd || "";
    if (!start && !end) {
      return;
    }
    if (!start) {
      start = toDateKey(new Date(Date.now() - 29 * 86400000));
    }
    if (!end) {
      end = toDateKey(new Date());
    }
    if (start > end) {
      [start, end] = [end, start];
    }
    ui.customRangeStart = start;
    ui.customRangeEnd = end;
    ui.customRangeDraftStart = start;
    ui.customRangeDraftEnd = end;
    ui.analysisRangeMode = "custom";
    ui.analysisPreset = null;
    ui.analysisPanOffset = 0;
    syncCommunityProfileAnalysisControls();
    void renderPublicProfileAnalysis(d);
  });
  document.getElementById("pubBenchmarkSelect")?.addEventListener("change", (e) => {
    ui.benchmark = e.target.value || "none";
    syncCommunityProfileAnalysisControls();
    void renderPublicProfileAnalysis(d);
  });
}

function paintPublicProfileAnalysisCore(
  d,
  { useDbRows, dbRows, portfolio, scope, todayKey, liveByMode, perfSnap = null },
) {
  const pubRate = document.getElementById("pubAnalysisRateChart");
  const pubRateTip = document.getElementById("pubAnalysisRateTooltip");
  const pubRateSummary = document.getElementById("pubAnalysisRateSummary");
  const pubMkt = document.getElementById("pubAnalysisMarketIndexChart");
  const pubMktTip = document.getElementById("pubAnalysisMarketIndexTooltip");
  const pubRatio = document.getElementById("pubAnalysisCashRatioChart");
  const pubRatioTip = document.getElementById("pubAnalysisCashRatioTooltip");
  const pubRank = document.getElementById("pubAnalysisStockRankBody");
  if (!pubRate) {
    return;
  }

  const refresh = () => {
    void renderPublicProfileAnalysis(state.lastPublicProfileDetail);
  };

  const rankOpts = { publicStockRankLayout: true };

  const bindRateOnly = (mySeries, benchSeries, opts = {}) => {
    const mwrUi = opts.mwrUi === true;
    const headlineMwr = Number(opts.headlineMwr) || 0;
    const drawRate = () =>
      mwrUi
        ? drawAnalysisMwrRatePlaceholder(pubRate, "资金加权收益率只算总值、不算每日走势。")
        : drawLineChart(mySeries, benchSeries, pubRate);
    const payloads = { rate: drawRate() };
    const rateHasBenchmark = state.benchmark !== "none";
    bindInteractiveChart(pubRate, pubRateTip, () => payloads.rate, {
      mode: "analysis",
      onRefresh: refresh,
      onRedraw: () => {
        payloads.rate = drawRate();
      },
      valueFormatter: (_value, key) => {
        if (key === "benchmark" && !rateHasBenchmark) {
          return "--";
        }
        return `${formatNumber(_value, 2)}%`;
      },
    });
    const lastMyTwr = mySeries.at(-1)?.rate ?? 0;
    const lastBench = benchSeries.at(-1)?.rate ?? 0;
    const excess = lastMyTwr - lastBench;
    if (pubRateSummary) {
      if (mwrUi) {
        pubRateSummary.textContent =
          state.benchmark === "none"
            ? `我的收益率 ${formatPercent(headlineMwr)}`
            : `我的 ${formatPercent(headlineMwr)} / 基准 ${formatPercent(lastBench)} / 对比 –`;
      } else {
        pubRateSummary.textContent =
          state.benchmark === "none"
            ? `我的收益率 ${formatPercent(lastMyTwr)}`
            : `我的 ${formatPercent(lastMyTwr)} / 基准 ${formatPercent(lastBench)} / 对比 ${formatPercent(excess)}`;
      }
    }
  };

  if (!useDbRows || !dbRows.length) {
    clearCanvasChart(pubRate);
    clearCanvasChart(pubMkt);
    if (pubRatio) {
      clearCanvasChart(pubRatio);
    }
    if (pubRank) {
      pubRank.innerHTML = `<p class="empty">暂无日快照数据。</p>`;
    }
    if (pubRateSummary) {
      pubRateSummary.textContent =
        state.benchmark === "none" ? "我的收益率 –" : "我的 – / 基准 – / 对比 –";
    }
    return;
  }

  const sorted = [...dbRows].sort((a, b) => a.date.localeCompare(b.date));
  const mergedFull = mergeAnalysisSliceWithLive(
    sorted.map((row) => ({ ...row })),
    portfolio,
    todayKey,
    liveByMode,
    scope.cashTransfers,
  );
  const pseudoHistory = mergedFull.map((row) => ({
    date: row.date,
    value: analysisTotalAssetsFromRow(row),
    flow: Number(row.externalFlowCny ?? row.external_flow_cny ?? 0),
  }));
  const perfPresetKey = resolvePerformancePresetKeyFromStateLike(state);
  let selectedPh = resolveAnalysisRange(pseudoHistory);
  if (
    perfPresetKey &&
    (perfPresetKey === "last_7d" || perfPresetKey === "last_30d" || perfPresetKey === "last_90d") &&
    Number(state.analysisPanOffset || 0) === 0
  ) {
    const len = perfPresetKey === "last_7d" ? 7 : perfPresetKey === "last_30d" ? 30 : 90;
    const end = String(mergedFull[mergedFull.length - 1]?.date || todayKey).slice(0, 10);
    const start = addCalendarDaysToDateKey(end, -(len - 1));
    const alt = pseudoHistory.filter((p) => p.date >= start && p.date <= end);
    if (alt.length >= 2) {
      selectedPh = alt;
    }
  }
  const dateSet = new Set(selectedPh.map((p) => p.date));
  const sliceRows = mergedFull.filter((row) => dateSet.has(row.date));

  const modePts = sliceRows.map((r) => ({
    date: r.date,
    value: analysisTotalAssetsFromRow(r),
    flow: Number(r.externalFlowCny ?? r.external_flow_cny ?? 0),
  }));
  const useMwrUi = normalizeProfitAlgoMode(state.algoMode) === "mwr";
  const bundle = buildAnalysisChartsSeriesBundle({
    modePts,
    selectedPh,
    useMwrUi,
    algoMode: state.algoMode,
    perfSnap: perfSnap ?? null,
  });
  const mySeries = bundle.mySeries;
  const benchSeries = rebaseRateSeriesByFirstDay(buildBenchmarkSeries(selectedPh));
  let headlineMwrPub = 0;
  if (useMwrUi && sliceRows.length) {
    if (bundle.cacheHeadlineMwr != null && Number.isFinite(bundle.cacheHeadlineMwr)) {
      headlineMwrPub = bundle.cacheHeadlineMwr;
    } else {
      headlineMwrPub = analysisXirrForStage(
        mergedFull,
        String(sliceRows[0].date).slice(0, 10),
        String(sliceRows[sliceRows.length - 1].date).slice(0, 10),
      );
    }
  }
  bindRateOnly(mySeries, benchSeries, { mwrUi: useMwrUi, headlineMwr: headlineMwrPub });
  const pubAssetIdx = sliceRows.map((r) => ({
    date: r.date,
    value: Number(r.totalAssets) || Number(r.marketValue) || 0,
  }));
  paintPublicProfileMarketIndexChart(pubMkt, pubMktTip, pubAssetIdx, refresh);
  if (pubRatio) {
    paintPublicProfileCashRatioChart(pubRatio, pubRatioTip, sliceRows, refresh);
  }
  renderAnalysisStockRank(pseudoHistory, scope, portfolio, pubRank, rankOpts);
}

async function renderPublicProfileAnalysis(d) {
  const detail = d || state.lastPublicProfileDetail;
  if (!detail?.publicTrades || !Array.isArray(detail.publicTrades)) {
    return;
  }
  ensurePublicProfileAnalysisUi();
  const targetId = String(detail.userId || state.communityProfileUserId || "").trim();
  if (apiReady && targetId) {
    const stage = metricsStageFromAnalysis();
    const benchSym = state.benchmark === "none" ? "" : normalizeSymbol(state.benchmark);
    const pubParams = { accountScope: "all", stage };
    if (benchSym) {
      pubParams.symbol = benchSym;
    }
    const pubBundle = await fetchMetricsApi("/metrics/analysis-bundle", pubParams, targetId);
    const pubSeries = pubBundle?.series || {};
    const twrPts = pubSeries.dailyTwr || [];
    const profitPts = pubSeries.dailyProfit || [];
    const benchPack = pubBundle?.benchmark;
    const rankPack = pubBundle?.stockRank;
    const retPack = pubBundle?.returns;
    if (twrPts.length || profitPts.length) {
      await withPublicProfileAnalysisUiAsync(async () => {
        const pubRate = document.getElementById("pubAnalysisRateChart");
        const pubMkt = document.getElementById("pubAnalysisMarketChart");
        const pubRank = document.getElementById("pubAnalysisStockRankBody");
        const pubRateSummary = document.getElementById("pubAnalysisRateSummary");
        const useMwrUi = normalizeProfitAlgoMode(state.algoMode) === "mwr";
        const mySeries = analysisRateSeriesForChart(trimMetricsSeriesPoints(twrPts));
        const benchSeries = analysisRateSeriesForChart(trimMetricsSeriesPoints(benchPack?.points || []));
        const assetChartRows = analysisAssetChartRowsFromSeries(pubSeries);
        const assetIdx = trimMetricsSeriesPoints(assetChartRows).map((r) => ({
          date: r.date,
          value: Number(r.totalAssets) || 0,
        }));
        const pubTwrPts = trimMetricsSeriesPoints(twrPts);
        const pubBenchPts = trimMetricsSeriesPoints(benchPack?.points || []);
        const refresh = () => void renderPublicProfileAnalysis(state.lastPublicProfileDetail);
        let pubRatePayload = null;
        const redrawPubRate = () => {
          if (!pubRate) {
            return null;
          }
          pubRatePayload = useMwrUi
            ? drawAnalysisMwrRatePlaceholder(pubRate, "资金加权收益率只算总值、不算每日走势。")
            : drawLineChart(mySeries, benchSeries, pubRate);
          return pubRatePayload;
        };
        if (pubRate) {
          redrawPubRate();
          bindInteractiveChart(pubRate, document.getElementById("pubAnalysisRateTooltip"), () => pubRatePayload, {
            mode: "analysis",
            onRefresh: refresh,
            onRedraw: redrawPubRate,
            valueFormatter: (_value, key) => {
              const date = state.chartCrosshairMap[pubRate.id]?.date;
              if (key === "benchmark") {
                const pt = pubBenchPts.find((p) => String(p.date).slice(0, 10) === String(date).slice(0, 10));
                return bundleFmtText(pt?.rate ?? pt?.rateDisplay);
              }
              const pt = pubTwrPts.find((p) => String(p.date).slice(0, 10) === String(date).slice(0, 10));
              return bundleFmtText(pt?.rate);
            },
          });
          if (pubRateSummary) {
            pubRateSummary.textContent = useMwrUi
              ? `我的收益率 ${bundleFmtText(retPack?.rate)}`
              : `我的 ${bundleFmtText(mySeries.at(-1)?.rate)}`;
          }
        }
        if (pubMkt) paintPublicProfileMarketIndexChart(pubMkt, document.getElementById("pubAnalysisMarketTooltip"), assetIdx, refresh);
        paintStockRankFromBundle(rankPack, pubRank, { publicStockRankLayout: true });
      });
      syncCommunityProfileAnalysisControls();
      return;
    }
  }
  await withPublicTradesContextAsync(detail, async () => {
    await withPublicProfileAnalysisUiAsync(async () => {
      const scope = { accountId: "all", trades: state.trades, cashTransfers: [] };
      const portfolio = computePortfolio(scope.trades, []);
      const todayKey = toDateKey(new Date());
      const dbRows = Array.isArray(detail.analysisDaily) ? detail.analysisDaily : [];
      if (!dbRows.length) {
        paintPublicProfileAnalysisCore(detail, {
          useDbRows: false,
          dbRows: [],
          portfolio,
          scope,
          todayKey,
          liveByMode: { twr: 0, mwr: 0 },
        });
        return;
      }
      const sorted = [...dbRows].sort((a, b) => String(a.date || "").localeCompare(String(b.date || "")));
      const liveByMode = buildLiveByModeFromSnapshotDaily(sorted, portfolio, todayKey, []);
      const mergedFull = mergeAnalysisSliceWithLive(
        sorted.map((row) => ({ ...row })),
        portfolio,
        todayKey,
        liveByMode,
        scope.cashTransfers,
      );
      const pseudoHistory = mergedFull.map((row) => ({
        date: row.date,
        value: analysisTotalAssetsFromRow(row),
        flow: Number(row.externalFlowCny ?? row.external_flow_cny ?? 0),
      }));
      const perfPresetKey = resolvePerformancePresetKeyFromStateLike(state);
      let selectedPh = resolveAnalysisRange(pseudoHistory);
      if (
        perfPresetKey &&
        (perfPresetKey === "last_7d" || perfPresetKey === "last_30d" || perfPresetKey === "last_90d") &&
        Number(state.analysisPanOffset || 0) === 0
      ) {
        const len = perfPresetKey === "last_7d" ? 7 : perfPresetKey === "last_30d" ? 30 : 90;
        const end = String(mergedFull[mergedFull.length - 1]?.date || todayKey).slice(0, 10);
        const start = addCalendarDaysToDateKey(end, -(len - 1));
        const alt = pseudoHistory.filter((p) => p.date >= start && p.date <= end);
        if (alt.length >= 2) {
          selectedPh = alt;
        }
      }
      const symSet = new Set();
      for (const pos of portfolio.positions || []) {
        const ns = normalizeSymbol(pos.symbol);
        if (ns) {
          symSet.add(ns);
        }
      }
      if (state.benchmark !== "none") {
        const nb = normalizeSymbol(state.benchmark);
        if (nb) {
          symSet.add(nb);
        }
      }
      const win = Math.min(900, Math.max(120, selectedPh.length + 200));
      await fetchSymbolCloseIntoKlineMap([...symSet], win);
      let perfSnap = null;
      if (apiReady && perfPresetKey && detail.userId) {
        perfSnap = await fetchCommunityPerformancePresetRemote(String(detail.userId), perfPresetKey);
      }
      paintPublicProfileAnalysisCore(detail, {
        useDbRows: true,
        dbRows,
        portfolio,
        scope,
        todayKey,
        liveByMode,
        perfSnap,
      });
    });
  });
  syncCommunityProfileAnalysisControls();
}

let lastCommunityDataKey = "";

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
  try {
    const r = await apiFetch(
      `${getApiBaseForFetch()}/community/users/${encodeURIComponent(uid)}/profile`,
      { cache: "no-store" },
    );
    const j = await r.json().catch(() => ({}));
    if (r.status === 404) {
      communityProfileBody.innerHTML = `<p class="empty">用户未公开或不可见</p>`;
      return;
    }
    if (!r.ok || !j?.ok) {
      communityProfileBody.innerHTML = `<p class="empty">${escapeHtml(j?.error || "加载失败")}</p>`;
      return;
    }
    const d = j.data;
    state.lastPublicProfileDetail = d;
    mergeFxRatesFromAnalysisDailyRows(Array.isArray(d?.analysisDaily) ? d.analysisDaily : []);
    await hydrateSymbolNameMap([
      ...(d?.positions || []).map((row) => row?.symbol),
      ...(d?.topPositions || []).map((row) => row?.symbol),
      ...(d?.publicTrades || []).map((row) => row?.symbol),
    ]);
    seedPublicProfileAnalysisUiFromDetail(d);
    const psr = String(d.publicStageRange || "month");
    state.communityProfileStage = ["month", "ytd", "total"].includes(psr) ? psr : "month";
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
    await refreshMarketData({ skipFinalRender: true });
    communityProfileBody.innerHTML = renderCommunityProfilePageHtml(d);
    bindPublicProfileStageSelect();
    syncPublicProfileStockSortControls();
    bindPublicProfileAnalysisInteractions(d);
    renderPublicTradeTable(d);
    syncCommunityProfileAnalysisControls();
    renderRoute();
    window.setTimeout(() => {
      if (state.route !== "community-profile" || state.lastPublicProfileDetail !== d) {
        return;
      }
      void renderPublicProfileAnalysis(state.lastPublicProfileDetail);
    }, 0);
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
  if (state.route === "community-profile") {
    syncPublicProfileStockSortControls();
  }
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

function syncPublicProfileStockSortControls() {
  document.querySelectorAll(".public-profile-stock-table .th-sort-btn").forEach((button) => {
    const key = button.dataset.sortKey || "";
    button.classList.remove("asc", "desc", "active");
    if (state.publicStockSortOrder !== "default" && key === state.publicStockSortKey) {
      button.classList.add("active", state.publicStockSortOrder);
    }
  });
}

function refreshPublicProfileEarningPanel() {
  const d = state.lastPublicProfileDetail;
  const earningPane = document.querySelector('[data-profile-panel="earning"]');
  if (!earningPane || !d?.publicTrades) {
    return;
  }
  earningPane.innerHTML = renderPublicEarningProfileHtml(d);
  bindPublicProfileStageSelect();
  syncPublicProfileStockSortControls();
}

function invalidateOverviewMetricsUi() {
  state.overviewMetricsUi.ready = false;
  state.overviewMetricsUi.loading = false;
  state.overviewMetricsUi.key = "";
  state.overviewMetricsUi.returns = null;
  state.overviewMetricsUi.assets = null;
  state.overviewMetricsUi.holdings = null;
  _overviewProfitInflight = null;
}

function invalidateOverviewSnapshotUi() {
  invalidateOverviewMetricsUi();
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

function buildOverviewSnapshotCacheKey() {
  return [
    sessionUserId || "",
    state.selectedAccountId,
    state.stageRange,
    state.algoMode,
    state.stockAmountDisplay,
    overviewTradesLedgerKey(),
  ].join("::");
}

/** 不含 stageRange：home-summary 已带月/年/总，切换阶段只本地重算 headline */
function buildOverviewSnapshotDataKey() {
  return [
    sessionUserId || "",
    state.selectedAccountId,
    state.algoMode,
    state.stockAmountDisplay,
    overviewTradesLedgerKey(),
  ].join("::");
}

function overviewSnapshotSnapMapFromState() {
  return null;
}

/** 总览「现金占比」：现金 / 总资产 × 100%（与当前展示的 totalAssets、overviewCash 同口径）。 */
function formatOverviewCashRatioFromTotals(totalAssetsBook, cashBook) {
  const ta = Number(totalAssetsBook);
  const c = Number(cashBook);
  if (!Number.isFinite(ta) || ta <= 0 || !Number.isFinite(c)) {
    return "0.00%";
  }
  return `${formatNumber((c / ta) * 100, 2)}%`;
}

/** 总览「股票占比」：持仓总市值 / 总资产 × 100%（全部股票仓位合计）。 */
function formatOverviewStockRatioFromTotals(totalAssetsBook, marketValueBook) {
  const ta = Number(totalAssetsBook);
  const mv = Number(marketValueBook);
  if (!Number.isFinite(ta) || ta <= 0 || !Number.isFinite(mv)) {
    return "0.00%";
  }
  return `${formatNumber((mv / ta) * 100, 2)}%`;
}

/** 总览 KPI 六项：总资产、总市值 | 现金、本金 | 现金占比、股票占比。 */
function buildOverviewKpiEntries({
  totalAssets,
  marketValue,
  cash,
  stockRatio,
  cashRatio,
  principal,
}) {
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

function stubVisiblePositionsFromHomeSummarySymbols(symbols) {
  return (Array.isArray(symbols) ? symbols : []).map((s) => {
    const sym = normalizeSymbol(s.symbol);
    const market = inferMarket(sym);
    const currency = String(s.currency || getSymbolCurrency(sym, market) || "CNY").toUpperCase();
    const fx =
      currency === "CNY"
        ? 1
        : validNumber(state.fxSpot?.[currency], FX_RATE_FALLBACK[currency] || 1) || 1;
    return {
      symbol: sym,
      name: sym,
      market,
      currency,
      quantity: 0,
      currentPrice: 0,
      prevClose: 0,
      dayChangeRate: 0,
      marketValueNative: 0,
      marketValue: 0,
      fxRate: fx,
      sigmaAmount: 0,
      sigmaAmountNative: 0,
      todayProfitNative: 0,
      monthProfitNative: 0,
      yearProfitNative: 0,
      totalProfitNative: 0,
      weight: 0,
      cost: 0,
      regretRate: 0,
      lastTradeSide: "buy",
    };
  });
}

async function paintOverviewSnapshotUiTestMode() {
  if (!overviewGrid) {
    return;
  }
  const todayKey = toDateKey(new Date());
  const from = shiftDateKeyByDays(todayKey, -400);
  if (quoteTime) {
    quoteTime.textContent = "snapshotUi=1（无实时行情）";
    quoteTime.classList.remove("is-delayed");
    quoteTime.setAttribute("title", "URL 测试模式：已跳过 refreshMarketData（含 symbol-close 补水）");
  }

  if (state.selectedAccountId !== "all") {
    const bookCcy = getOverviewBookCurrency();
    const dash = "—";
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
    setOverviewProfitKpisDash();
    if (stockTableBody) {
      stockTableBody.innerHTML = `
      <tr>
        <td colspan="15"><p class="empty">「快照测试」请切换到<strong>全部账户</strong>；地址栏保留 <code>?snapshotUi=1</code>。</p></td>
      </tr>
    `;
    }
    return;
  }

  const bookCcy = "CNY";
  if (!apiReady) {
    const dash = "—";
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
    setOverviewProfitKpisDash();
    if (stockTableBody) {
      stockTableBody.innerHTML = `
      <tr><td colspan="15"><p class="empty">请先登录后再测 snapshotUi=1。</p></td></tr>
    `;
    }
    return;
  }

  let homeData = null;
  let dbRows = [];
  try {
    [homeData, dbRows] = await Promise.all([
      fetchHomeSummaryRemote(),
      fetchAnalysisDailyRowsRemote({ accountId: "all", from, to: todayKey }),
    ]);
  } catch {
    homeData = null;
    dbRows = [];
  }
  mergeFxRatesFromAnalysisDailyRows(Array.isArray(dbRows) ? dbRows : []);

  const last = Array.isArray(dbRows) && dbRows.length ? dbRows[dbRows.length - 1] : null;
  const mv =
    last && Number.isFinite(Number(last.marketValue)) && Number(last.marketValue) > 0
      ? Number(last.marketValue)
      : Number(homeData?.account?.last_market_value_cny) || 0;
  const principal = last ? Number(last.principal) || 0 : 0;
  const totalAssets = last ? Number(last.totalAssets) || 0 : 0;
  const cash = last ? Number(last.cash) || 0 : 0;
  const cashRatioStr = formatOverviewCashRatioFromTotals(totalAssets, cash);
  const stockRatioStr = formatOverviewStockRatioFromTotals(totalAssets, mv);

  overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml(
    buildOverviewKpiEntries({
      totalAssets: formatOverviewPlainMoney(totalAssets, bookCcy),
      marketValue: formatOverviewPlainMoney(mv, bookCcy),
      cash: formatOverviewPlainMoney(cash, bookCcy),
      stockRatio: stockRatioStr,
      cashRatio: cashRatioStr,
      principal: formatOverviewPlainMoney(principal, bookCcy),
    }),
  );

  setOverviewProfitKpisDash();

  if (stockTableBody) {
    if (homeData?.symbols?.length) {
      const vis = stubVisiblePositionsFromHomeSummarySymbols(homeData.symbols);
      const snapMap = buildSymbolSnapshotProfitMapFromHomeSummary(vis, homeData.symbols, todayKey);
      paintOverviewStockTableFromSnapshots({ visiblePositions: vis }, snapMap);
    } else {
      paintOverviewStockTableFromSnapshots({ visiblePositions: [] }, null);
    }
  }
}

function renderOverviewAndStockTable() {
  if (state.route === "community-profile" || state.route === "stock-record") {
    return;
  }
  if (isSnapshotUiTestMode()) {
    void paintOverviewSnapshotUiTestMode();
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

/**
 * 分析 Tab 所选完整周期 [a,b]（与顶部区间一致，不受图表横向平移窗口影响）。
 */
function resolveAnalysisPeriodAB(history) {
  const todayKey = toDateKey(new Date());
  if (!history.length) {
    return { a: todayKey, b: todayKey };
  }
  const lastH = history[history.length - 1].date;
  if (state.analysisRangeMode === "all") {
    return { a: history[0].date, b: lastH };
  }
  if (state.analysisRangeMode === "custom") {
    let start = state.customRangeStart || history[0].date;
    let end = state.customRangeEnd || lastH;
    if (start > end) {
      [start, end] = [end, start];
    }
    const picked = history.filter((p) => p.date >= start && p.date <= end);
    if (picked.length) {
      return { a: start, b: end };
    }
  }
  if (isAnalysisMtdPreset()) {
    return { a: monthToDateStartKey(), b: lastH };
  }
  if (isAnalysisYtdPreset()) {
    return { a: ytdStartDateKey(), b: lastH };
  }
  const windowSize = Math.min(Math.max(state.rangeDays, 2), history.length);
  const startIdx = Math.max(0, history.length - windowSize);
  return { a: history[startIdx].date, b: lastH };
}

/** 标的在 dateKey 当日及以前成交累计后的日终股数（含 dateKey 当天）。 */
function symbolEodQtyOnOrBefore(symbolTrades, dateKey) {
  let qty = 0;
  for (const t of symbolTrades) {
    if (t.date <= dateKey) {
      qty += t.side === "buy" ? t.quantity : -t.quantity;
    }
  }
  return qty;
}

function mapStockRankBundleRow(row) {
  const profitCnyRaw = Number(row.profitCny);
  const profitFromText = parseBundleSignedAmount(row.profit);
  const profitCny = Number.isFinite(profitCnyRaw) ? profitCnyRaw : profitFromText;
  let pxChange = Number(row.pxChange);
  if (!Number.isFinite(pxChange)) {
    pxChange = parseBundlePercent(row.pxChange ?? row.pxChangePct ?? row.pxChangeDisplay);
  }
  if (!Number.isFinite(pxChange)) {
    pxChange = 0;
  }
  let profitShare = Number(row.profitShare);
  if (!Number.isFinite(profitShare)) {
    profitShare = parseBundlePercent(row.profitShare);
  }
  const nameCn = String(row.nameCn || "").trim();
  const fallbackName = String(row.name || "").trim();
  return {
    symbol: row.symbol,
    name: nameCn || fallbackName,
    holdIntervalsLabel: String(row.holdIntervalsLabel || ""),
    profitCny: Number.isFinite(profitCny) ? profitCny : 0,
    pxChange,
    heldDays: Number(row.heldDays) || 0,
    profitShare: Number.isFinite(profitShare) ? profitShare : null,
  };
}

function buildAnalysisStockRankHtml(rows, rankOpts = {}) {
  const publicRank = rankOpts.publicStockRankLayout === true;
  const hideProfitCol = publicRank || rankOpts.hideProfitColumn === true;
  if (!rows.length) {
    return `<p class="empty">本分析周期内无持仓的标的。</p>`;
  }
  const totalProfitForShare = rows.reduce((s, r) => s + r.profitCny, 0);
  const profitTh = hideProfitCol
    ? ""
    : `<span class="col-profit" role="columnheader">区间收益(¥)</span>`;
  const profitShareTh = publicRank
    ? `<span class="col-profit-share" role="columnheader">收益占比</span>`
    : "";

  return `
    <div class="analysis-stock-rank-table${publicRank ? " analysis-stock-rank-table--public" : ""}" role="table" aria-label="个股收益排行">
      <div class="analysis-stock-rank-head" role="row">
        <span class="col-rank" role="columnheader">#</span>
        <span class="col-name" role="columnheader">名称</span>
        ${profitShareTh}
        ${profitTh}
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
          const cls = row.profitCny > 0 ? "up" : row.profitCny < 0 ? "down" : "";
          const pCls = row.pxChange > 0 ? "up" : row.pxChange < 0 ? "down" : "";
          const code = formatSymbolForDisplay(row.symbol);
          let profitShareCell = "";
          if (publicRank) {
            const shareRatio =
              row.profitShare != null && Number.isFinite(row.profitShare)
                ? row.profitShare
                : Math.abs(totalProfitForShare) < 1e-6
                  ? NaN
                  : row.profitCny / totalProfitForShare;
            const shareText = Number.isFinite(shareRatio) ? formatPercent(shareRatio) : "—";
            profitShareCell = `<span class="col-profit-share ${cls}" role="cell">${shareText}</span>`;
          }
          const profitCell = hideProfitCol
            ? ""
            : `<span class="col-profit ${cls}" role="cell">${row.profitCny >= 0 ? "+" : ""}¥${formatNumber(
                row.profitCny,
                2,
              )}</span>`;
          return `
        <div class="analysis-stock-rank-row" role="row">
          <span class="col-rank" role="cell">${idx + 1}</span>
          <div class="col-name" role="cell">
            <strong>${escapeHtml(getDisplayName(row.symbol, row.name))}</strong>
            <span class="rank-code">${escapeHtml(code)}</span>
          </div>
          ${profitShareCell}
          ${profitCell}
          <span class="col-px ${pCls}" role="cell">${formatPercent(row.pxChange)}</span>
          <span class="col-days" role="cell">${row.heldDays} 天</span>
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
  rows.sort((a, b) => b.profitCny - a.profitCny);
  targetBody.innerHTML = buildAnalysisStockRankHtml(rows, rankOpts);
}

/**
 * 个股排行：周期 a、b 来自顶部选择；仅展示 [a,b] 内至少有一天日终持仓大于 0 的标的。
 * 有效区间：effStart=A早于a则a否则A；effEnd 默认 B 早于 b 取 B 否则取 b；若周期末日 b 仍持仓则强制 effEnd=b，避免仅一笔买入时 B 停在买入日导致涨跌幅异常。
 */
function renderAnalysisStockRank(
  history,
  scope,
  portfolio,
  targetBody = analysisStockRankBody,
  rankOpts = {},
) {
  if (!targetBody) {
    return;
  }
  const publicHoldIntervals = rankOpts.publicStockRankLayout === true || rankOpts.publicHoldIntervals === true;
  if (!history.length) {
    targetBody.innerHTML = `<p class="empty">暂无分析区间数据。</p>`;
    return;
  }
  const { a, b } = resolveAnalysisPeriodAB(history);
  const rows = [];
  for (const pos of portfolio.positions) {
    const symbolTrades = scope.trades.filter((t) => t.symbol === pos.symbol).sort(sortTradeAsc);
    if (!symbolTrades.length) {
      continue;
    }
    const A = symbolTrades[0].date;
    const B = symbolTrades[symbolTrades.length - 1].date;
    if (countHeldDaysInRange(symbolTrades, a, b) < 1) {
      continue;
    }
    const effStart = A < a ? a : A;
    let effEnd = B < b ? B : b;
    if (symbolEodQtyOnOrBefore(symbolTrades, b) > 1e-6) {
      effEnd = b;
    }
    if (effStart > effEnd) {
      continue;
    }
    const m = computePositionPeriodMetrics(pos, effStart, effEnd, scope.trades);
    const profitCny = profitNativeToAnalysisCny(pos, m.profitNative);
    const holdIntervalsLabel = publicHoldIntervals
      ? formatHoldingSegmentsLabelPublic(pos, symbolTrades, a, b, scope.trades)
      : formatHoldingSegmentsLabel(pos, symbolTrades, a, b, scope.trades);
    rows.push({
      symbol: pos.symbol,
      name: pos.name,
      holdIntervalsLabel,
      profitCny,
      pxChange: m.pxChange,
      heldDays: m.heldDays,
    });
  }
  rows.sort((a, b) => b.profitCny - a.profitCny);
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
function tradeRecordNoteSubrowHtml(note, colspan, rowAttrs = {}) {
  const text = String(note || "").trim();
  if (!text) {
    return "";
  }
  const attrs = Object.entries(rowAttrs)
    .filter(([, v]) => v != null && String(v).trim() !== "")
    .map(([k, v]) => `${k}="${escapeHtml(String(v))}"`)
    .join(" ");
  const attrStr = attrs ? ` ${attrs}` : "";
  return `
    <tr class="trade-note-subrow trade-row--clickable"${attrStr}>
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
    state.route === "trade-cash"
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
      const ccy = getCurrencyLabel(acc.currency);
      const id = escapeHtml(String(row.id));
      return `
        <tr class="cash-transfer-row trade-row--clickable" data-cash-id="${id}">
          <td>${String(row.date).replace(/-/g, "/")}</td>
          <td>${escapeHtml(acc.name || row.accountId)}</td>
          <td>${dirLabel}</td>
          <td class="num ${row.direction === "in" ? "up" : "down"}">${sign}${formatNumber(row.amount, 2)} ${ccy}</td>
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

function renderTradeTable() {
  if (state.route === "community-profile" || state.route === "stock-record") {
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
  let html = sorted
    .map((trade) => {
      const acc = getAccountById(trade.accountId);
      const accLabel = escapeHtml(acc.name || trade.accountId || "default");
      const id = escapeHtml(String(trade.id));
      return `
        <tr class="trade-row trade-row--clickable" data-record-id="${id}">
          <td>${trade.date.replace(/-/g, "/")}</td>
          <td class="trade-col-name">${escapeHtml(getDisplayName(trade.symbol, trade.name))}</td>
          <td class="type-cell">${tradeDirectionCellLabel(trade)}</td>
          <td class="num">${formatNumber(trade.price, 2)}</td>
          <td class="num">${formatNumber(trade.quantity, 0)}</td>
          <td class="num ${trade.side === "buy" ? "down" : "up"}">${
            trade.side === "buy" ? "-" : "+"
          }${formatNumber(trade.amount, 2)}</td>
          <td class="trade-account-cell">${accLabel}</td>
        </tr>
        ${tradeRecordNoteSubrowHtml(trade.note, 7, { "data-record-id": trade.id })}
      `;
    })
    .join("");
  if (tradeListPager.loading) {
    html += tradeListLoadingRowHtml(7);
  }
  tradeTableBody.innerHTML = html;
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

function invalidateCashListAfterMutation() {
  if (state.route === "trade-cash") {
    void loadCashListPage({ reset: true });
  }
}

function openEditTradeDialog(tradeId) {
  closeTradeRecordActionsSheet();
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
  void refreshMarketData();
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

/** 用 analysis_daily 冻结序列 + 今日市值/出入金，重算当日末点 TWR/MWR（不依赖本机日 K 拼组合历史）。 */
function buildLiveByModeFromSnapshotDaily(sortedRows, portfolio, todayKey, scopeCash) {
  const tk = String(todayKey || "").slice(0, 10);
  const cash = Array.isArray(scopeCash)
    ? scopeCash
    : getFilteredCashTransfers(resolveValidAccountFilter(state.selectedAccountId));
  const extToday = cash.reduce((s, r) => {
    if (String(r.date).slice(0, 10) === tk) {
      return s + cashTransferRowNetCnyAsOf(r, tk);
    }
    return s;
  }, 0);
  const pts = [];
  for (const row of sortedRows || []) {
    const d = String(row.date || "").slice(0, 10);
    if (!d || d >= tk) {
      continue;
    }
    pts.push({
      date: d,
      value: analysisTotalAssetsFromRow(row),
      flow: Number(row.externalFlowCny ?? row.external_flow_cny ?? 0) || 0,
    });
  }
  const mvCny = Number(portfolio?.totalMarketValueCnyBook ?? portfolio?.totalMarketValue) || 0;
  const cashLive = Number(portfolio?.cash) || 0;
  pts.push({
    date: tk,
    value: mvCny + cashLive,
    flow: extToday,
  });
  if (!pts.length) {
    return { twr: 0, mwr: 0 };
  }
  return {
    twr: computeModeSeries(pts, "twr").at(-1)?.rate ?? 0,
    mwr: computeModeSeries(pts, "mwr").at(-1)?.rate ?? 0,
  };
}

/**
 * 分析 Tab 最后一行对齐首页总览：总市值、总资产/现金/占比、当日 profit_cny（与总览「今日」同口径）。
 * total_profit 仍按库里「日收益累加」延伸：昨日累计 + 今日 profit_cny，避免与历史点混用「持仓成本法 totalProfit」导致曲线断层。
 */
function mergeAnalysisSliceWithLive(sliceRows, portfolio, todayKey, liveByMode = {}, scopeCash) {
  const cashRows = Array.isArray(scopeCash)
    ? scopeCash
    : getFilteredCashTransfers(resolveValidAccountFilter(state.selectedAccountId));
  const extToday = cashRows.reduce((s, r) => {
    if (String(r.date).slice(0, 10) === todayKey) {
      return s + cashTransferRowNetCnyAsOf(r, todayKey);
    }
    return s;
  }, 0);
  const twr = Number(liveByMode.twr);
  const mwr = Number(liveByMode.mwr);
  const mvCny = Number(portfolio.totalMarketValueCnyBook) || 0;
  const cashCny = Number(portfolio.cash) || 0;
  const totalAssetsCny = mvCny + cashCny;
  const cashRatioPct = totalAssetsCny > 0 ? (cashCny / totalAssetsCny) * 100 : 0;
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
      marketValue: mvCny,
      totalAssets: totalAssetsCny,
      cash: cashCny,
      cashRatio: cashRatioPct,
      principal: 0,
      totalProfit: cumFromPrev(hit) + todayP,
      profitCny: todayP,
      externalFlowCny: extToday,
      twRCumulative: Number.isFinite(twr) ? twr : next[hit].twRCumulative,
      mwrCumulative: Number.isFinite(mwr) ? mwr : next[hit].mwrCumulative,
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
      marketValue: mvCny,
      totalAssets: totalAssetsCny,
      cash: cashCny,
      cashRatio: cashRatioPct,
      principal: 0,
      totalProfit: lastCum + todayP,
      externalFlowCny: extToday,
      twRCumulative: Number.isFinite(twr) ? twr : last.twRCumulative,
      mwrCumulative: Number.isFinite(mwr) ? mwr : last.mwrCumulative,
      fxHkdCny: last.fxHkdCny,
      fxUsdCny: last.fxUsdCny,
    });
  }
  return next;
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

function applyOverviewMetricsMeta(meta) {
  if (!meta || typeof meta !== "object") {
    return;
  }
  if (meta.quoteTime) {
    state.quoteTime = String(meta.quoteTime);
  }
  state.marketDataDelayed = !!meta.delayed;
  state.marketDataDelaySource = meta.delayed ? "metrics-delayed" : "";
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

function parseBundlePercent(text) {
  let t = String(text ?? "").trim().replace(/%/g, "");
  if (!t || t === "–" || t === "—") {
    return NaN;
  }
  const neg = t.startsWith("-") || t.startsWith("−");
  const n = parseFloat(t.replace(/^[+−-]/, ""));
  if (!Number.isFinite(n)) {
    return NaN;
  }
  const ratio = n / 100;
  return neg ? -ratio : ratio;
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

function paintOverviewFromMetricsBundle(returns, assets, holdings, stageKey) {
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

/** 拉取日快照的左边界：覆盖阶段起点、月初、年初与首笔交易，避免合并缺段。 */
function overviewAccountDailyFetchBounds(scope, stageRange) {
  const tradeList = Array.isArray(scope.trades) ? scope.trades : [];
  const firstTradeDate =
    tradeList.length > 0 ? [...tradeList].sort(sortTradeAsc)[0].date : toDateKey(new Date());
  const todayKey = toDateKey(new Date());
  const keys = [
    String(firstTradeDate).slice(0, 10),
    monthToDateStartKey(),
    ytdStartDateKey(),
    String(getStageStartKey(stageRange, firstTradeDate) || "").slice(0, 10),
  ].filter(Boolean);
  keys.sort();
  const earliest = keys[0] || String(todayKey).slice(0, 10);
  return {
    from: shiftDateKeyByDays(earliest, -25),
    to: shiftDateKeyByDays(todayKey, 1),
    firstTradeDate,
    todayKey,
  };
}

function todayProfitRateCnyFromMergedRows(mergedRows, todayKey) {
  const tk = String(todayKey || "").slice(0, 10);
  const idx = mergedRows.findIndex((r) => String(r.date || "").slice(0, 10) === tk);
  if (idx < 0) {
    return null;
  }
  const row = mergedRows[idx];
  const todayP = Number(row.profitCny ?? row.profit_cny) || 0;
  let prevTa = 0;
  if (idx > 0) {
    prevTa = analysisTotalAssetsFromRow(mergedRows[idx - 1]);
  }
  const rate = prevTa > 0 ? todayP / prevTa : 0;
  return { todayProfitCny: todayP, todayRate: rate };
}

/** 日快照行：优先 total_assets（总资产），否则总市值+现金 */
function analysisTotalAssetsFromRow(r) {
  const ta = Number(r?.totalAssets ?? r?.total_assets);
  if (Number.isFinite(ta) && Math.abs(ta) > 1e-8) {
    return ta;
  }
  const mv = Number(r?.marketValue ?? r?.market_value) || 0;
  const cash = Number(r?.cash ?? r?.cash_cny) || 0;
  return mv + cash;
}

function analysisNpv(rate, datedAmounts) {
  const t0 = new Date(`${datedAmounts[0].date}T12:00:00+08:00`).getTime();
  let s = 0;
  for (const { date, amt } of datedAmounts) {
    const years = (new Date(`${date}T12:00:00+08:00`).getTime() - t0) / (365.25 * 86400000);
    s += amt / (1 + rate) ** years;
  }
  return s;
}

function analysisXirr(datedAmounts, guess = 0.08) {
  if (!datedAmounts.length) {
    return 0;
  }
  let r = guess;
  for (let k = 0; k < 40; k += 1) {
    const t0 = new Date(`${datedAmounts[0].date}T12:00:00+08:00`).getTime();
    let f = 0;
    let df = 0;
    for (const { date, amt } of datedAmounts) {
      const years = (new Date(`${date}T12:00:00+08:00`).getTime() - t0) / (365.25 * 86400000);
      const den = (1 + r) ** years;
      f += amt / den;
      df += (-years * amt) / ((1 + r) ** (years + 1));
    }
    if (!Number.isFinite(f) || Math.abs(f) < 1e-8) {
      return r;
    }
    if (!Number.isFinite(df) || Math.abs(df) < 1e-12) {
      break;
    }
    const nr = r - f / df;
    if (!Number.isFinite(nr) || nr <= -0.9999 || nr > 100) {
      break;
    }
    r = nr;
  }
  let lo = -0.9999;
  let hi = 10;
  for (let k = 0; k < 80; k += 1) {
    const mid = (lo + hi) / 2;
    const v = analysisNpv(mid, datedAmounts);
    if (!Number.isFinite(v)) {
      break;
    }
    if (Math.abs(v) < 1e-7) {
      return mid;
    }
    if (v > 0) {
      lo = mid;
    } else {
      hi = mid;
    }
  }
  return r;
}

/** 资金加权（XIRR）：与 performance-cache-service.mwrPeriodFromSnapshots 同结构，价值用总资产 */
function analysisXirrForStage(mergedRows, startKey, endKey) {
  const rows = [...(mergedRows || [])]
    .map((r) => ({
      date: String(r.date || "").slice(0, 10),
      totalAssets: analysisTotalAssetsFromRow(r),
      externalFlowCny: Number(r.externalFlowCny ?? r.external_flow_cny ?? 0) || 0,
    }))
    .filter((r) => r.date)
    .sort((a, b) => a.date.localeCompare(b.date));
  const start = String(startKey || "").slice(0, 10);
  const end = String(endKey || "").slice(0, 10);
  const prev = rows.filter((r) => r.date < start);
  const bv = prev.length ? Number(prev[prev.length - 1].totalAssets) || 0 : 0;
  const anchor = prev.length ? String(prev[prev.length - 1].date).slice(0, 10) : start;
  const inWin = rows.filter((r) => r.date >= start && r.date <= end);
  if (!inWin.length) {
    return 0;
  }
  const ev = Number(inWin[inWin.length - 1].totalAssets) || 0;
  const lastD = String(inWin[inWin.length - 1].date).slice(0, 10);
  const dayMap = new Map();
  if (Number.isFinite(bv) && bv !== 0) {
    dayMap.set(anchor, (dayMap.get(anchor) || 0) - bv);
  }
  for (const r of inWin) {
    if (r.externalFlowCny) {
      dayMap.set(r.date, (dayMap.get(r.date) || 0) + r.externalFlowCny);
    }
  }
  if (Number.isFinite(ev)) {
    dayMap.set(lastD, (dayMap.get(lastD) || 0) + ev);
  }
  const dated = [...dayMap.entries()]
    .map(([date, amt]) => ({ date, amt }))
    .filter((x) => x.amt !== 0)
    .sort((a, b) => a.date.localeCompare(b.date));
  if (dated.length < 2) {
    return 0;
  }
  return analysisXirr(dated, 0.05);
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

/** 已对「今日」做完 merge 的日序列上，截取总览阶段算阶段收益/收益率（人民币口径）。 */
function computeStageOverviewFromMergedRows(mergedRows, stageRange, algoMode, firstTradeDate) {
  if (!Array.isArray(mergedRows) || !mergedRows.length) {
    return null;
  }
  const startKey = String(getStageStartKey(stageRange, firstTradeDate) || "").slice(0, 10);
  const sorted = [...mergedRows].sort((a, b) => String(a.date || "").localeCompare(String(b.date || "")));
  const idxStart = sorted.findIndex((r) => String(r.date || "").slice(0, 10) >= startKey);
  const prevRow = idxStart > 0 ? sorted[idxStart - 1] : null;
  const windowRows = sorted.filter((r) => String(r.date || "").slice(0, 10) >= startKey);
  if (!windowRows.length) {
    return null;
  }
  const extendedRows = prevRow ? [prevRow, ...windowRows] : windowRows;
  const modePts = extendedRows.map((r) => ({
    date: r.date,
    value: analysisTotalAssetsFromRow(r),
    flow: Number(r.externalFlowCny ?? r.external_flow_cny ?? 0),
  }));
  const profitSeries = buildProfitSeries(modePts);
  const endKey = String(windowRows[windowRows.length - 1].date || "").slice(0, 10);
  const norm = normalizeProfitAlgoMode(algoMode);
  let stageRate = 0;
  if (norm === "mwr") {
    stageRate = analysisXirrForStage(sorted, startKey, endKey);
  } else {
    const modeSeriesRaw = computeModeSeries(modePts, "twr");
    const modeSeriesRebased = rebaseRateSeriesByFirstDay(modeSeriesRaw);
    stageRate = modeSeriesRebased.at(-1)?.rate ?? 0;
  }
  return {
    stageProfit: profitSeries.at(-1)?.value ?? 0,
    stageRate,
  };
}

let overviewProfitRefreshSeq = 0;
let _overviewProfitInflight = null;

/**
 * 与 renderAnalysis 同链：全日快照 merge 今日后，再截阶段；dbRows 可为接口或社区内嵌 analysisDaily。
 */
function computeStageOverviewFromSnapshotRows(dbRows, portfolio, scope, stageRange, algoMode) {
  if (!Array.isArray(dbRows) || !dbRows.length) {
    return null;
  }
  const tradeList = Array.isArray(scope.trades) ? scope.trades : [];
  const firstTradeDate =
    tradeList.length > 0 ? [...tradeList].sort(sortTradeAsc)[0].date : toDateKey(new Date());
  const todayKey = toDateKey(new Date());
  const sorted = [...dbRows].sort((a, b) => String(a.date || "").localeCompare(String(b.date || "")));
  const liveByMode = buildLiveByModeFromSnapshotDaily(sorted, portfolio, todayKey, scope.cashTransfers);
  const merged = mergeAnalysisSliceWithLive(
    sorted.map((row) => ({ ...row })),
    portfolio,
    todayKey,
    liveByMode,
    scope.cashTransfers,
  );
  return computeStageOverviewFromMergedRows(merged, stageRange, algoMode, firstTradeDate);
}

function indexSymbolDailyPnlBySymbol(rows) {
  const m = new Map();
  for (const r of rows || []) {
    const s = normalizeSymbol(r.symbol);
    if (!s) {
      continue;
    }
    if (!m.has(s)) {
      m.set(s, []);
    }
    m.get(s).push(r);
  }
  for (const arr of m.values()) {
    arr.sort((a, b) => String(a.date || "").localeCompare(String(b.date || "")));
  }
  return m;
}

/** 库内「日终 day_pnl」累计到昨日；展示时再加当日 todayProfitNative（随行情变）。 */
function buildSymbolSnapshotProfitMap(positions, symbolPnlRows, trades, todayKey) {
  const tk = String(todayKey || "").slice(0, 10);
  const tradeList = Array.isArray(trades) ? trades : [];
  const firstTradeDate =
    tradeList.length > 0 ? [...tradeList].sort(sortTradeAsc)[0].date : tk;
  const monthStart = String(getStageStartKey("month", firstTradeDate) || "").slice(0, 10);
  const ytdStart = String(ytdStartDateKey() || "").slice(0, 10);
  const bySym = indexSymbolDailyPnlBySymbol(symbolPnlRows);
  const map = new Map();
  for (const row of positions || []) {
    const sym = normalizeSymbol(row.symbol);
    const arr = bySym.get(sym) || [];
    let monthHist = 0;
    let yearHist = 0;
    let totalHist = 0;
    for (const r of arr) {
      const d = String(r.date || "").slice(0, 10);
      if (!d || d >= tk) {
        continue;
      }
      const p = Number(r.dayPnlNative ?? r.day_pnl_native) || 0;
      totalHist += p;
      if (d >= ytdStart) {
        yearHist += p;
      }
      if (d >= monthStart) {
        monthHist += p;
      }
    }
    map.set(sym, {
      monthHistNative: monthHist,
      yearHistNative: yearHist,
      totalHistNative: totalHist,
    });
  }
  return map;
}

/** 服务端 symbol_daily_pnl 冻结日行（v3），与 buildSymbolSnapshotProfitMap 输出结构一致。 */
function buildSymbolSnapshotProfitMapFromHomeSummary(vis, homeSymbols, _todayKey) {
  const byNorm = new Map();
  for (const s of homeSymbols || []) {
    const sym = normalizeSymbol(s.symbol);
    if (sym) {
      byNorm.set(sym, s);
    }
  }
  const map = new Map();
  for (const row of vis || []) {
    const sym = normalizeSymbol(row.symbol);
    const r = byNorm.get(sym);
    if (!r) {
      continue;
    }
    map.set(sym, {
      monthHistNative: Number(r.month_profit_native) || 0,
      yearHistNative: Number(r.ytd_profit_native) || 0,
      totalHistNative: Number(r.total_profit_native) || 0,
      totalRateTwr: Number(r.total_rate_twr) || 0,
      totalRateMwr: Number(r.total_rate_mwr) || 0,
    });
  }
  return map;
}

function paintOverviewStockTableFromSnapshots(portfolio, snapMap) {
  if (!stockTableBody) {
    return;
  }
  const rows = sortPositions(portfolio.visiblePositions);
  if (!rows.length) {
    stockTableBody.innerHTML = `
      <tr>
        <td colspan="15"><p class="empty">暂无持仓，点击“记一笔”开始记录。</p></td>
      </tr>
    `;
    return;
  }
  const dash = snapMap == null;
  let monthDen = 0;
  let yearDen = 0;
  if (!dash) {
    for (const row of rows) {
      const sym = normalizeSymbol(row.symbol);
      const s = snapMap.get(sym);
      if (!s) {
        continue;
      }
      const liveToday = Number.isFinite(Number(row.todayProfitNative)) ? Number(row.todayProfitNative) : 0;
      const mN = Number(s.monthHistNative) + liveToday;
      const yN = Number(s.yearHistNative) + liveToday;
      monthDen += Math.abs(applyFxForOverview(row, mN));
      yearDen += Math.abs(applyFxForOverview(row, yN));
    }
  }
  stockTableBody.innerHTML = rows
    .map((row) => {
      const stockCode = formatSymbolForDisplay(row.symbol);
      const tag = row.market === "A股" ? "CN" : row.market === "港股" ? "HK" : row.market === "美股" ? "US" : "OT";
      const tagLower = tag.toLowerCase();
      const sym = normalizeSymbol(row.symbol);
      const s = dash ? null : snapMap.get(sym);
      const hasSnap = Boolean(s);
      const liveToday = Number.isFinite(Number(row.todayProfitNative)) ? Number(row.todayProfitNative) : 0;
      const todayN = liveToday;
      const monthN = hasSnap ? Number(s.monthHistNative) + liveToday : null;
      const yearN = hasSnap ? Number(s.yearHistNative) + liveToday : null;
      const totalN = hasSnap ? Number(s.totalHistNative) + liveToday : null;
      const monthW =
        hasSnap && monthDen > 0 ? applyFxForOverview(row, monthN) / monthDen : hasSnap ? 0 : null;
      const yearW = hasSnap && yearDen > 0 ? applyFxForOverview(row, yearN) / yearDen : hasSnap ? 0 : null;
      const sigmaAbs = Math.abs(Number(row.sigmaAmountNative) || 0);
      const useMwrRate = normalizeProfitAlgoMode(state.algoMode) === "mwr";
      const materializedRate = hasSnap
        ? useMwrRate
          ? Number(s.totalRateMwr)
          : Number(s.totalRateTwr)
        : NaN;
      const totalRateSnap = hasSnap
        ? Number.isFinite(materializedRate)
          ? materializedRate
          : sigmaAbs > 1e-9
            ? totalN / sigmaAbs
            : 0
        : null;

      const dayClass = hasSnap ? (applyFxForOverview(row, todayN) >= 0 ? "up" : "down") : "";
      const changeClass = row.dayChangeRate >= 0 ? "up" : "down";
      const monthClass = hasSnap ? (applyFxForOverview(row, monthN) >= 0 ? "up" : "down") : "";
      const yearClass = hasSnap ? (applyFxForOverview(row, yearN) >= 0 ? "up" : "down") : "";
      const totalClass = hasSnap ? (applyFxForOverview(row, totalN) >= 0 ? "up" : "down") : "";
      const totalRateClass = hasSnap ? (totalRateSnap >= 0 ? "up" : "down") : "";

      const tdToday = hasSnap ? formatStockTableMoney(row, todayN, 2) : "–";
      const tdMonth = hasSnap ? formatStockTableMoney(row, monthN, 2) : "–";
      const tdMonthW = hasSnap ? formatPercent(monthW) : "–";
      const tdYear = hasSnap ? formatStockTableMoney(row, yearN, 2) : "–";
      const tdYearW = hasSnap ? formatPercent(yearW) : "–";
      const tdTotal = hasSnap ? formatStockTableMoney(row, totalN, 2) : "–";
      const tdTotalR = hasSnap ? formatPercent(totalRateSnap) : "–";

      return `
        <tr>
          <td class="stock-name">
            <strong>${escapeHtml(getDisplayName(row.symbol, row.name))}</strong>
            <span><i class="market-tag market-tag--${tagLower}">${tag}</i> ${stockCode}</span>
          </td>
          <td class="${dayClass}">${tdToday}</td>
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
          <td class="${monthClass}">${tdMonth}</td>
          <td>${tdMonthW}</td>
          <td class="${yearClass}">${tdYear}</td>
          <td>${tdYearW}</td>
          <td class="${totalClass}">${tdTotal}</td>
          <td class="${totalRateClass}">${tdTotalR}</td>
          <td class="${row.regretRate >= 0 ? "up" : "down"}">${formatRegretRateWithSide(row.regretRate, row.lastTradeSide)}</td>
          <td class="stock-table-op-cell"><a href="javascript:void(0)" class="record-link stock-table-record-link" data-stock-record="${escapeHtml(row.symbol)}">记录</a><a href="javascript:void(0)" class="record-link stock-table-trade-link" data-stock-add-trade="${escapeHtml(row.symbol)}">交易</a></td>
        </tr>
      `;
    })
    .join("");
}

/**
 * 从已拉取的 home-summary account 块计算「今日 + 当前阶段」收益与收益率（人民币口径）。
 */
function computeOverviewHeadlineFromHomeAccount(homeData, portfolio) {
  const acc = homeData?.account;
  if (!acc) {
    return null;
  }
  const todayP = todayProfitCnyForAnalysisSnapshot(portfolio);
  const lastMv = Number(acc.last_market_value_cny) || 0;
  const tr = lastMv > 0 ? todayP / lastMv : 0;
  const mwr = state.algoMode === "mwr";
  let stageProfit = 0;
  let stageRate = 0;
  if (state.stageRange === "ytd") {
    stageProfit = Number(acc.ytd_profit_cny) + todayP;
    stageRate = mwr ? Number(acc.ytd_rate_mwr) || 0 : Number(acc.ytd_rate_twr) || 0;
  } else if (state.stageRange === "total") {
    stageProfit = Number(acc.total_profit_cny) + todayP;
    stageRate = mwr ? Number(acc.total_rate_mwr) || 0 : Number(acc.total_rate_twr) || 0;
  } else {
    stageProfit = Number(acc.month_profit_cny) + todayP;
    stageRate = mwr ? Number(acc.month_rate_mwr) || 0 : Number(acc.month_rate_twr) || 0;
  }
  return {
    todayProfitCny: todayP,
    todayRate: tr,
    stageProfit,
    stageRate,
  };
}

function paintOverviewHeadlineFromHomePayload(homeData, portfolio, bookCcy) {
  if (!todayProfitMain || !monthProfitMain) {
    return false;
  }
  const h = computeOverviewHeadlineFromHomeAccount(homeData, portfolio);
  if (
    !h ||
    !Number.isFinite(Number(h.stageProfit)) ||
    !Number.isFinite(Number(h.stageRate)) ||
    !Number.isFinite(Number(h.todayProfitCny)) ||
    !Number.isFinite(Number(h.todayRate))
  ) {
    return false;
  }
  const todayBook = amountBookFromCny(Number(h.todayProfitCny), bookCcy);
  const stageBook = amountBookFromCny(Number(h.stageProfit), bookCcy);
  const tr = Number(h.todayRate);
  const sr = Number(h.stageRate);
  todayProfitMain.innerHTML = metricValueWithRate(todayBook, tr);
  todayProfitMain.className = `profit-main ${todayBook >= 0 ? "up" : "down"}`;
  monthProfitMain.innerHTML = metricValueWithRate(stageBook, sr);
  monthProfitMain.className = `profit-main ${stageBook >= 0 ? "up" : "down"}`;
  return true;
}

const _kpiInFlightByScope = new Map();

async function ensureAccountKpiSurfaceLoaded(accountScope) {
  if (!apiReady || !sessionPhone) {
    return;
  }
  const sc = String(accountScope || "all").trim() || "all";
  if (state.accountKpisByScope[sc]) {
    return;
  }
  if (_kpiInFlightByScope.has(sc)) {
    return _kpiInFlightByScope.get(sc);
  }
  const promise = (async () => {
    try {
      const res = await apiFetch(
        `${API_BASE}/surface/account-kpis?accountScope=${encodeURIComponent(sc)}`,
        { cache: "no-store", timeoutMs: 18_000 },
      );
      if (!res.ok) {
        return;
      }
      const j = await res.json().catch(() => ({}));
      if (j?.ok && j.data != null && typeof j.data === "object") {
        state.accountKpisByScope[sc] = j.data;
      }
    } catch {
      /* ignore */
    } finally {
      _kpiInFlightByScope.delete(sc);
    }
  })();
  _kpiInFlightByScope.set(sc, promise);
  return promise;
}

/**
 * 仅 accountScope=all；与「当前账户筛选 + 成交账本」绑定，失效见 invalidateOverviewSnapshotUi。
 */
async function fetchHomeSummaryRemote() {
  if (!apiReady) {
    return null;
  }
  const memoKey = `${state.selectedAccountId}|${overviewTradesLedgerKey()}`;
  const now = Date.now();
  if (
    homeSummaryRpcMemo.data &&
    homeSummaryRpcMemo.key === memoKey &&
    now - homeSummaryRpcMemo.at < HOME_SUMMARY_RPC_REUSE_MS
  ) {
    return homeSummaryRpcMemo.data;
  }
  if (homeSummaryInflightByKey.has(memoKey)) {
    return homeSummaryInflightByKey.get(memoKey);
  }
  const gen = homeSummaryFetchGen;
  const p = (async () => {
    try {
      const res = await apiFetch(`${API_BASE}/snapshot/home-summary?accountScope=all`, {
        cache: "no-store",
        timeoutMs: 18_000,
      });
      if (!res.ok) {
        return null;
      }
      const j = await res.json().catch(() => ({}));
      if (!j.ok || !j.data?.account || !Array.isArray(j.data.symbols)) {
        return null;
      }
      if (gen !== homeSummaryFetchGen) {
        return null;
      }
      homeSummaryRpcMemo = { key: memoKey, data: j.data, at: Date.now() };
      return j.data;
    } catch {
      return null;
    } finally {
      homeSummaryInflightByKey.delete(memoKey);
    }
  })();
  homeSummaryInflightByKey.set(memoKey, p);
  return p;
}

const METRICS_HOME_STAGE = { month: "mtd", ytd: "ytd", total: "inception" };
/** 首屏 home-bundle 一次拉回今日 + 月/年/总收益，切换阶段仅本地换展示 */
const METRICS_HOME_BUNDLE_STAGES = "today,mtd,ytd,inception";
const METRICS_HOME_BUNDLE_STAGE_KEYS = ["today", "mtd", "ytd", "inception"];
function metricsStageFromHome() { return METRICS_HOME_STAGE[state.stageRange] || "mtd"; }
function overviewMetricsBundleCacheKey(aid) {
  return `${aid}|${state.algoMode}`;
}
function overviewReturnsHasAllHomeStages(ret) {
  if (!ret?.stages) return false;
  return METRICS_HOME_BUNDLE_STAGE_KEYS.every((k) => ret.stages[k]);
}
function metricsStageFromAnalysis() {
  const preset = resolvePerformancePresetKeyFromAnalysisState();
  if (preset) return preset;
  if (state.analysisRangeMode === "all") return "inception";
  return "mtd";
}
function trimMetricsSeriesPoints(points) {
  const off = Math.max(0, Number(state.analysisPanOffset) || 0);
  if (!off || !Array.isArray(points) || points.length < 3) return points || [];
  return points.slice(Math.min(off, points.length - 2));
}
async function fetchMetricsApi(path, params = {}, publicTargetId = "") {
  if (!apiReady) return null;
  const qs = new URLSearchParams();
  const p = { ...params };
  if (p.accountScope != null && p.account_id == null) {
    p.account_id = p.accountScope;
  }
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
    pathNorm.includes("home-bundle") || pathNorm.includes("analysis-bundle") ? 55_000 : 28_000;
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

function metricsRowProfitClass(row, fieldBase) {
  const cnyOn = state.stockAmountDisplay === "cny";
  const text = cnyOn ? row[`${fieldBase}Cny`] : row[fieldBase];
  return bundleSignedClass(text);
}

function metricsHoldingsMoneyCell(row, fieldBase) {
  const cnyOn = state.stockAmountDisplay === "cny";
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

function paintOverviewStockTableFromMetricsRows(rows) {
  if (!stockTableBody) return;
  const sorted = sortMetricsHoldingsRows(rows);
  if (!sorted.length) { stockTableBody.innerHTML = `<tr><td colspan="15"><p class="empty">暂无持仓，点击“记一笔”开始记录。</p></td></tr>`; return; }
  stockTableBody.innerHTML = sorted.map((row) => {
    const sym = normalizeSymbol(row.symbol);
    const tag = row.marketTag === "CN" ? "cn" : row.marketTag === "HK" ? "hk" : row.marketTag === "US" ? "us" : "ot";
    const dayClass = bundleSignedClass(row.dayChange);
    const todayClass = metricsRowProfitClass(row, "todayProfit");
    const monthClass = metricsRowProfitClass(row, "monthProfit");
    const yearClass = metricsRowProfitClass(row, "yearProfit");
    const totalClass = metricsRowProfitClass(row, "totalProfit");
    const totalRateClass = bundleSignedClass(row.totalRate);
    const regretClass = bundleSignedClass(String(row.regret || "").replace(/\s+[BS]$/i, ""));
    const mvSort = parseBundleSignedAmount(row.marketValueCny ?? row.marketValue);
    const priceSort = parseBundleSignedAmount(row.price);
    const qtyFromMv = mvSort > 0 && priceSort > 0 ? Math.round(mvSort / priceSort) : NaN;
    const qty = Number.isFinite(qtyFromMv) ? String(qtyFromMv) : bundleFmtText(row.quantity);
    return `<tr><td class="stock-name"><strong>${escapeHtml(row.name || sym)}</strong><span><i class="market-tag market-tag--${tag}">${escapeHtml(row.marketTag || "OT")}</i> ${escapeHtml(row.stockCode || formatSymbolForDisplay(sym))}</span></td><td class="${todayClass}">${escapeHtml(metricsHoldingsMoneyCell(row, "todayProfit"))}</td><td><div class="cell-main">${escapeHtml(bundleFmtText(row.price))}</div><div class="cell-sub ${dayClass}">${escapeHtml(bundleFmtText(row.dayChange))}</div></td><td><div class="cell-main">${escapeHtml(metricsHoldingsMoneyCell(row, "marketValue"))}</div><div class="cell-sub">${escapeHtml(qty)}</div></td><td>${escapeHtml(bundleFmtText(row.weight))}</td><td>${escapeHtml(bundleFmtText(row.cost))}</td><td class="${monthClass}">${escapeHtml(metricsHoldingsMoneyCell(row, "monthProfit"))}</td><td>${escapeHtml(bundleFmtText(row.monthWeight))}</td><td class="${yearClass}">${escapeHtml(metricsHoldingsMoneyCell(row, "yearProfit"))}</td><td>${escapeHtml(bundleFmtText(row.yearWeight))}</td><td class="${totalClass}">${escapeHtml(metricsHoldingsMoneyCell(row, "totalProfit"))}</td><td>${escapeHtml(bundleFmtText(row.totalWeight))}</td><td class="${totalRateClass}">${escapeHtml(bundleFmtText(row.totalRate))}</td><td class="${regretClass}">${escapeHtml(bundleFmtText(row.regret))}</td><td class="stock-table-op-cell"><a href="javascript:void(0)" class="record-link stock-table-record-link" data-stock-record="${escapeHtml(sym)}">记录</a><a href="javascript:void(0)" class="record-link stock-table-trade-link" data-stock-add-trade="${escapeHtml(sym)}">交易</a></td></tr>`;
  }).join("");
}
function analysisAssetChartRowsFromSeries(series) {
  if (!series || typeof series !== "object") {
    return [];
  }
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
  bindInteractiveChart(analysisRateChart, analysisRateTooltip, () => ctx.payloads.rate, {
    mode: "analysis",
    onRefresh: ctx.refreshAnalysisView,
    onRedraw: ctx.redrawChartsOnly,
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
    valueFormatter: (value) => formatNumber(value, 2),
  });
  bindInteractiveChart(analysisAssetChart, analysisAssetTooltip, () => ctx.payloads.asset, {
    mode: "analysis",
    onRefresh: ctx.refreshAnalysisView,
    onRedraw: ctx.redrawChartsOnly,
    valueFormatter: ctx.assetValueFormatter,
  });
}

async function paintAnalysisFromMetricsApi(renderRequestId, publicTargetId = "") {
  const stage = metricsStageFromAnalysis();
  const aid = state.selectedAccountId === "all" ? "all" : state.selectedAccountId;
  const benchSym = state.benchmark === "none" ? "" : normalizeSymbol(state.benchmark);
  const bundleParams = { accountScope: aid, stage };
  if (benchSym) {
    bundleParams.symbol = benchSym;
  }
  const bundle = await fetchMetricsApi("/metrics/analysis-bundle", bundleParams, publicTargetId);
  const series = bundle?.series || {};
  const twrPts = series.dailyTwr || [];
  const profitPts = series.dailyProfit || [];
  const benchPack = bundle?.benchmark;
  const rankPack = bundle?.stockRank;
  const retPack = bundle?.returns;
  if (renderRequestId !== analysisRenderRequestSeq) return false;
  if (!twrPts.length && !profitPts.length) return false;
  const useMwrUi = normalizeProfitAlgoMode(state.algoMode) === "mwr";
  const twrPtsTrim = trimMetricsSeriesPoints(twrPts);
  const benchPtsTrim = trimMetricsSeriesPoints(benchPack?.points || []);
  const mySeries = analysisRateSeriesForChart(twrPtsTrim);
  const benchSeries = analysisRateSeriesForChart(benchPtsTrim);
  const profitSeries = trimMetricsSeriesPoints(profitPts).map((p) => ({
    date: p.date,
    value: parseBundleSignedAmount(p.profit),
  }));
  cachedAnalysisAssetChartRows = analysisAssetChartRowsFromSeries(series);
  const assetSeries = trimMetricsSeriesPoints(cachedAnalysisAssetChartRows);

  const refreshAnalysisView = () => {
    renderControls();
    void renderAnalysis({ showLoading: false });
  };
  const assetValueFormatter = (value) =>
    state.capitalTrendMode === "cash_ratio"
      ? `${formatNumber(value, 2)}%`
      : formatNumber(analysisSnapshotMoneyFromCny(value), 2);

  const redrawChartsOnly = () => {
    const c = cachedAnalysisMetricsCharts;
    if (!c) {
      return;
    }
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
        valueFormatter: (v) => formatNumber(v, 2),
        axisFormatter: (v) => formatNumber(v, 2),
        yRangePadding: {
          minFactor: ANALYSIS_CHART_AXIS_MIN_FACTOR,
          maxFactor: ANALYSIS_CHART_AXIS_MAX_FACTOR,
        },
      },
    );
    c.payloads.asset = drawAssetChart(trimMetricsSeriesPoints(cachedAnalysisAssetChartRows));
  };

  cachedAnalysisMetricsCharts = {
    twrPts: twrPtsTrim,
    benchPts: benchPtsTrim,
    mySeries,
    benchSeries,
    profitSeries,
    useMwrUi,
    refreshAnalysisView,
    redrawChartsOnly,
    assetValueFormatter,
    payloads: {
      rate: null,
      profit: null,
      asset: null,
    },
  };
  redrawChartsOnly();
  if (analysisRateSummary) {
    const lastMyPt = trimMetricsSeriesPoints(twrPts).at(-1);
    const lastBenchPt = trimMetricsSeriesPoints(benchPack?.points || []).at(-1);
    if (useMwrUi) {
      analysisRateSummary.textContent = `我的收益率 ${bundleFmtText(retPack?.rate)}`;
    } else if (state.benchmark === "none") {
      analysisRateSummary.textContent = `我的收益率 ${bundleFmtText(lastMyPt?.rate)}`;
    } else {
      const myPct = bundleFmtText(lastMyPt?.rate);
      const benchPct = bundleFmtText(lastBenchPt?.rate ?? lastBenchPt?.rateDisplay);
      const diff = parseBundlePercent(lastMyPt?.rate) - parseBundlePercent(lastBenchPt?.rate ?? lastBenchPt?.rateDisplay);
      analysisRateSummary.textContent = `我的 ${myPct} / 基准 ${benchPct} / 对比 ${formatPercent(diff)}`;
    }
  }
  if (analysisProfitSummary && retPack) {
    analysisProfitSummary.textContent = `累计收益 ${bundleFmtText(retPack.profit)}`;
  }
  paintStockRankFromBundle(rankPack, analysisStockRankBody, {});
  if (analysisEodAccountCaption) {
    analysisEodAccountCaption.textContent = "";
    analysisEodAccountCaption.hidden = true;
  }
  bindAnalysisMetricsChartsInteractive();
  return true;
}


async function refreshOverviewProfitRowFromSnapshots() {
  if (state.route === "community-profile" || state.route === "stock-record") return;
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

async function _doRefreshOverviewProfitRow(aid, stageKey, seq, reqKey) {
  const metricsKey = overviewMetricsBundleCacheKey(aid);
  state.overviewMetricsUi.loading = true;
  try {
    if (!apiReady) {
      return;
    }
    const bundle = await fetchMetricsApi("/metrics/home-bundle", {
      accountScope: aid,
      stages: METRICS_HOME_BUNDLE_STAGES,
    });
    const ret = bundle?.returns;
    const assets = bundle?.assets;
    const hold = bundle?.holdings;
    if (seq !== overviewProfitRefreshSeq) {
      return;
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
      return;
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


async function fetchAnalysisDailyRowsRemote({ accountId, from, to }) {
  if (!apiReady) {
    return [];
  }
  const aid = accountId === "all" ? "all" : accountId;
  const key = `aid:${aid || "all"}|from:${from}|to:${to}`;
  const now = Date.now();
  const cached = analysisDailyResponseCache.get(key);
  if (cached && now - Number(cached.updatedAt || 0) < ANALYSIS_DAILY_REMOTE_CACHE_TTL_MS) {
    const rows = Array.isArray(cached.rows) ? cached.rows : [];
    mergeFxRatesFromAnalysisDailyRows(rows);
    return rows;
  }
  if (analysisDailyInFlight.has(key)) {
    return analysisDailyInFlight.get(key);
  }
  const fetchPromise = (async () => {
    try {
      const res = await apiFetch(
        `${API_BASE}/snapshot/account-daily?accountId=${encodeURIComponent(aid)}&from=${encodeURIComponent(
          from
        )}&to=${encodeURIComponent(to)}`,
        { cache: "no-store", timeoutMs: 18_000 }
      );
      if (!res.ok) {
        return [];
      }
      const j = await res.json().catch(() => ({}));
      const rows = j?.ok && Array.isArray(j.data) ? j.data : [];
      mergeFxRatesFromAnalysisDailyRows(rows);
      analysisDailyResponseCache.set(key, { rows, updatedAt: Date.now() });
      if (analysisDailyResponseCache.size > 24) {
        const oldestKey = analysisDailyResponseCache.keys().next().value;
        if (oldestKey) {
          analysisDailyResponseCache.delete(oldestKey);
        }
      }
      return rows;
    } catch {
      return [];
    }
  })();
  analysisDailyInFlight.set(key, fetchPromise);
  try {
    return await fetchPromise;
  } finally {
    analysisDailyInFlight.delete(key);
  }
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

function parsePerformanceTwrSeriesPayload(raw) {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const dates = Array.isArray(raw.dates) ? raw.dates.map((x) => String(x).slice(0, 10)) : null;
  const twrRebased = Array.isArray(raw.twrRebased) ? raw.twrRebased.map((x) => Number(x) || 0) : null;
  const cumulativeProfit = Array.isArray(raw.cumulativeProfit)
    ? raw.cumulativeProfit.map((x) => Number(x) || 0)
    : null;
  if (!dates?.length || !twrRebased || dates.length !== twrRebased.length) {
    return null;
  }
  const cp =
    cumulativeProfit && cumulativeProfit.length === dates.length ? cumulativeProfit : dates.map(() => 0);
  const ruleVersion = Number(raw.ruleVersion) || 1;
  return { ruleVersion, dates, twrRebased, cumulativeProfit: cp };
}

async function fetchAnalysisPerformancePresetRemote(accountId, preset) {
  if (!apiReady) {
    return null;
  }
  const aid = accountId === "all" ? "all" : accountId;
  try {
    const res = await apiFetch(
      `${getApiBaseForFetch()}/snapshot/performance-preset?accountId=${encodeURIComponent(
        aid,
      )}&preset=${encodeURIComponent(preset)}`,
      { cache: "no-store", timeoutMs: 15_000 },
    );
    if (!res.ok) {
      return null;
    }
    const j = await res.json().catch(() => ({}));
    if (!j?.ok || !j.data) {
      return null;
    }
    const d = j.data;
    const twrSeries = d.twr?.series && typeof d.twr.series === "object" ? d.twr.series : null;
    const twrPayload = parsePerformanceTwrSeriesPayload(twrSeries);
    if (!twrPayload || twrPayload.ruleVersion < ANALYSIS_PERFORMANCE_RULE_VERSION) {
      return null;
    }
    return {
      asOfDate: String(d.asOfDate || "").slice(0, 10),
      twrPayload,
      periodReturnTwr: Number(d.twr?.periodReturn) || 0,
      periodReturnMwr: Number(d.mwr?.periodReturn) || 0,
      mwrRuleVersion: Number(d.mwr?.ruleVersion) || 1,
    };
  } catch {
    return null;
  }
}

async function fetchCommunityPerformancePresetRemote(targetUserId, preset) {
  if (!apiReady) {
    return null;
  }
  const tid = String(targetUserId || "").trim();
  if (!tid || !preset) {
    return null;
  }
  try {
    const res = await apiFetch(
      `${getApiBaseForFetch()}/community/users/${encodeURIComponent(
        tid,
      )}/performance-preset?accountId=all&preset=${encodeURIComponent(preset)}`,
      { cache: "no-store", timeoutMs: 15_000 },
    );
    if (!res.ok) {
      return null;
    }
    const j = await res.json().catch(() => ({}));
    if (!j?.ok || !j.data) {
      return null;
    }
    const d = j.data;
    const twrSeries = d.twr?.series && typeof d.twr.series === "object" ? d.twr.series : null;
    const twrPayload = parsePerformanceTwrSeriesPayload(twrSeries);
    if (!twrPayload || twrPayload.ruleVersion < ANALYSIS_PERFORMANCE_RULE_VERSION) {
      return null;
    }
    return {
      asOfDate: String(d.asOfDate || "").slice(0, 10),
      twrPayload,
      periodReturnTwr: Number(d.twr?.periodReturn) || 0,
      periodReturnMwr: Number(d.mwr?.periodReturn) || 0,
      mwrRuleVersion: Number(d.mwr?.ruleVersion) || 1,
    };
  } catch {
    return null;
  }
}

/**
 * 用服务端 materialized series_json（TWR）对齐分析窗首日，并与 modePts 上「冻结日之后」的 live 日拼接。
 * @returns {{ mySeries: {date:string, rate:number}[], profitSeries: {date:string, value:number}[] } | null}
 */
function buildAnalysisRateProfitFromPerfCache(twrPayload, selectedOrderedDates, asOf, modePts) {
  if (!twrPayload || !modePts.length || !selectedOrderedDates.length) {
    return null;
  }
  const map = new Map();
  for (let i = 0; i < twrPayload.dates.length; i += 1) {
    map.set(twrPayload.dates[i], i);
  }
  const firstD = selectedOrderedDates[0];
  if (!map.has(firstD)) {
    return null;
  }
  const j = map.get(firstD);
  const twrR = twrPayload.twrRebased;
  const cumP = twrPayload.cumulativeProfit;
  const baseTr = twrR[j];
  const baseCum = cumP[j];
  const rebLive = rebaseRateSeriesByFirstDay(computeTimeWeightedSeries(modePts));
  const liveRateBy = new Map(rebLive.map((x) => [String(x.date).slice(0, 10), Number(x.rate) || 0]));
  const profLive = buildProfitSeries(modePts);
  const liveProfBy = new Map(profLive.map((x) => [String(x.date).slice(0, 10), Number(x.value) || 0]));
  const asOfS = String(asOf || "").slice(0, 10);
  let anchorL = "";
  for (let k = selectedOrderedDates.length - 1; k >= 0; k -= 1) {
    const dk = selectedOrderedDates[k];
    if (dk <= asOfS && map.has(dk)) {
      anchorL = dk;
      break;
    }
  }
  if (!anchorL) {
    return null;
  }
  const idxL = map.get(anchorL);
  const rateC_L = (1 + twrR[idxL]) / (1 + baseTr) - 1;
  const profC_L = cumP[idxL] - baseCum;
  const liveL = liveRateBy.get(anchorL) ?? 0;
  const liveProfL = liveProfBy.get(anchorL) ?? 0;
  const mySeries = [];
  const profitSeries = [];
  for (const dk of selectedOrderedDates) {
    const idx = map.get(dk);
    if (idx != null && dk <= asOfS) {
      const rate = (1 + twrR[idx]) / (1 + baseTr) - 1;
      const prof = cumP[idx] - baseCum;
      mySeries.push({ date: dk, rate });
      profitSeries.push({ date: dk, value: prof });
    } else {
      const CLd = liveRateBy.get(dk);
      if (CLd === undefined) {
        return null;
      }
      const rate = (1 + rateC_L) * (1 + CLd) / (1 + liveL) - 1;
      const PLd = liveProfBy.get(dk);
      if (PLd === undefined) {
        return null;
      }
      const prof = profC_L + (PLd - liveProfL);
      mySeries.push({ date: dk, rate });
      profitSeries.push({ date: dk, value: prof });
    }
  }
  return { mySeries, profitSeries };
}

function buildAnalysisChartsSeriesBundle({ modePts, selectedPh, useMwrUi, algoMode, perfSnap }) {
  const selectedOrderedDates = selectedPh.map((p) => String(p.date).slice(0, 10));
  const fromCache = perfSnap
    ? buildAnalysisRateProfitFromPerfCache(perfSnap.twrPayload, selectedOrderedDates, perfSnap.asOfDate, modePts)
    : null;
  if (fromCache) {
    let cacheHeadlineMwr = null;
    if (
      useMwrUi &&
      perfSnap &&
      Number(perfSnap.mwrRuleVersion) >= ANALYSIS_PERFORMANCE_RULE_VERSION &&
      Number.isFinite(perfSnap.periodReturnMwr)
    ) {
      cacheHeadlineMwr = perfSnap.periodReturnMwr;
    }
    return { mySeries: fromCache.mySeries, profitSeries: fromCache.profitSeries, cacheHeadlineMwr };
  }
  const mySeriesRaw = computeModeSeries(modePts, useMwrUi ? "twr" : algoMode);
  return {
    mySeries: rebaseRateSeriesByFirstDay(mySeriesRaw),
    profitSeries: buildProfitSeries(modePts),
    cacheHeadlineMwr: null,
  };
}

const SYMBOL_SNAPSHOT_CHUNK = 14;

async function fetchSymbolDailyChunkOnce(aid, fromKey, toKey, part) {
  const subKey = `${aid}|${fromKey}|${toKey}|${part.join(",")}`;
  const now = Date.now();
  const cached = symbolDailyResponseCache.get(subKey);
  if (cached && now - Number(cached.updatedAt || 0) < SYMBOL_DAILY_REMOTE_CACHE_TTL_MS) {
    return Array.isArray(cached.rows) ? cached.rows : [];
  }
  let p = symbolDailyInFlight.get(subKey);
  if (!p) {
    p = (async () => {
      const qs = new URLSearchParams({
        accountId: aid,
        from: fromKey,
        to: toKey,
        symbols: part.join(","),
      });
      const res = await apiFetch(`${API_BASE}/snapshot/symbol-daily?${qs.toString()}`, {
        cache: "no-store",
        timeoutMs: 22_000,
      });
      if (!res.ok) {
        return [];
      }
      const j = await res.json().catch(() => ({}));
      const rows = j?.ok && Array.isArray(j.data) ? j.data : [];
      symbolDailyResponseCache.set(subKey, { rows, updatedAt: Date.now() });
      if (symbolDailyResponseCache.size > 48) {
        const oldestKey = symbolDailyResponseCache.keys().next().value;
        if (oldestKey) {
          symbolDailyResponseCache.delete(oldestKey);
        }
      }
      return rows;
    })();
    symbolDailyInFlight.set(subKey, p);
    p.finally(() => {
      symbolDailyInFlight.delete(subKey);
    });
  }
  return p;
}

async function fetchSymbolDailyRowsRemote({ accountId, symbols, from, to }) {
  if (!apiReady) {
    return [];
  }
  const aid = accountId === "all" ? "all" : accountId;
  const uniq = [...new Set((symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))];
  if (!uniq.length) {
    return [];
  }
  const fromKey = String(from || "").slice(0, 10);
  const toKey = String(to || "").slice(0, 10);
  const out = [];
  for (let i = 0; i < uniq.length; i += SYMBOL_SNAPSHOT_CHUNK) {
    const part = uniq.slice(i, i + SYMBOL_SNAPSHOT_CHUNK);
    try {
      const rows = await fetchSymbolDailyChunkOnce(aid, fromKey, toKey, part);
      out.push(...rows);
    } catch {
      // 单批失败不丢弃其它批次
    }
  }
  return out;
}

/** 按「收益」总览 overviewAccountDailyFetchBounds 预热日快照，便于分析页与收益页共享缓存、少打重复请求 */
async function warmOverviewDailySnapshotsForEarning(scope, stageRange = state.stageRange) {
  if (!apiReady || !scope) {
    return;
  }
  const portfolio = computePortfolio(scope.trades, scope.cashTransfers);
  const vis = portfolio.visiblePositions;
  const bounds = overviewAccountDailyFetchBounds(scope, stageRange);
  const aid = state.selectedAccountId === "all" ? "all" : state.selectedAccountId;
  const symList = [...new Set(vis.map((p) => normalizeSymbol(p.symbol)).filter(Boolean))];
  try {
    await Promise.all([
      fetchAnalysisDailyRowsRemote({ accountId: aid, from: bounds.from, to: bounds.to }),
      symList.length
        ? fetchSymbolDailyRowsRemote({ accountId: aid, symbols: symList, from: bounds.from, to: bounds.to })
        : Promise.resolve([]),
    ]);
  } catch {
    // 预热失败不影响分析图
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
    const metricsPainted = await paintAnalysisFromMetricsApi(renderRequestId);
    if (metricsPainted && renderRequestId === analysisRenderRequestSeq) {
      return;
    }
  }
  const scope = getPortfolioScope();
  const portfolio = computePortfolio(scope.trades, scope.cashTransfers);
  const todayKey = toDateKey(new Date());
  const fetchRange = buildAnalysisDailyFetchRange(scope);

  let dbRows = [];
  if (apiReady) {
    try {
      const aid = state.selectedAccountId === "all" ? "all" : state.selectedAccountId;
      dbRows = await fetchAnalysisDailyRowsRemote({
        accountId: aid,
        from: fetchRange.from,
        to: fetchRange.to,
      });
    } catch (error) {
      console.warn("加载 analysis_daily 失败", error);
    }
  }

  if (renderRequestId !== analysisRenderRequestSeq) {
    return;
  }
  if (!dbRows.length) {
    if (renderRequestId === analysisRenderRequestSeq) {
      renderAnalysisStockRank([], scope, portfolio);
      if (analysisEodAccountCaption) {
        analysisEodAccountCaption.textContent = "";
        analysisEodAccountCaption.hidden = true;
      }
    }
    return;
  }

  const sorted = [...dbRows].sort((a, b) => a.date.localeCompare(b.date));
  const liveByMode = buildLiveByModeFromSnapshotDaily(sorted, portfolio, todayKey, scope.cashTransfers);
  const mergedFull = mergeAnalysisSliceWithLive(
    sorted.map((row) => ({ ...row })),
    portfolio,
    todayKey,
    liveByMode,
    scope.cashTransfers,
  );
  const pseudoHistory = mergedFull.map((row) => ({
    date: row.date,
    value: analysisTotalAssetsFromRow(row),
    flow: Number(row.externalFlowCny ?? row.external_flow_cny ?? 0),
  }));
  const perfPresetKey = resolvePerformancePresetKeyFromAnalysisState();
  let selectedPh = resolveAnalysisRange(pseudoHistory);
  if (
    perfPresetKey &&
    (perfPresetKey === "last_7d" || perfPresetKey === "last_30d" || perfPresetKey === "last_90d") &&
    Number(state.analysisPanOffset || 0) === 0
  ) {
    const len = perfPresetKey === "last_7d" ? 7 : perfPresetKey === "last_30d" ? 30 : 90;
    const end = String(mergedFull[mergedFull.length - 1]?.date || todayKey).slice(0, 10);
    const start = addCalendarDaysToDateKey(end, -(len - 1));
    const alt = pseudoHistory.filter((p) => p.date >= start && p.date <= end);
    if (alt.length >= 2) {
      selectedPh = alt;
    }
  }
  const dateSet = new Set(selectedPh.map((p) => p.date));
  let sliceRows = mergedFull.filter((row) => dateSet.has(row.date));
  const symSet = new Set();
  for (const pos of portfolio.positions || []) {
    const ns = normalizeSymbol(pos.symbol);
    if (ns) {
      symSet.add(ns);
    }
  }
  if (state.benchmark !== "none") {
    const nb = normalizeSymbol(state.benchmark);
    if (nb) {
      symSet.add(nb);
    }
  }
  const win = Math.min(900, Math.max(120, selectedPh.length + 200));
  await fetchSymbolCloseIntoKlineMap([...symSet], win);

  if (renderRequestId !== analysisRenderRequestSeq) {
    return;
  }

  const modePts = sliceRows.map((r) => ({
    date: r.date,
    value: analysisTotalAssetsFromRow(r),
    flow: Number(r.externalFlowCny ?? r.external_flow_cny ?? 0),
  }));
  const useMwrUi = normalizeProfitAlgoMode(state.algoMode) === "mwr";
  const aid = state.selectedAccountId === "all" ? "all" : state.selectedAccountId;
  let perfSnap = null;
  if (apiReady && perfPresetKey) {
    perfSnap = await fetchAnalysisPerformancePresetRemote(aid, perfPresetKey);
  }
  if (renderRequestId !== analysisRenderRequestSeq) {
    return;
  }
  const bundle = buildAnalysisChartsSeriesBundle({
    modePts,
    selectedPh,
    useMwrUi,
    algoMode: state.algoMode,
    perfSnap,
  });
  const mySeries = bundle.mySeries;
  const profitSeries = bundle.profitSeries;
  const benchSeries = rebaseRateSeriesByFirstDay(buildBenchmarkSeries(selectedPh));
  const assetSeries = sliceRows.map((row) => {
    const mv = Number(row.marketValue) || 0;
    const cash = Number(row.cash) || 0;
    const taDb = Number(row.totalAssets);
    const totalAssets = Number.isFinite(taDb) && Math.abs(taDb) > 1e-9 ? taDb : mv + cash;
    const ratioDb = Number(row.cashRatio);
    const cashRatio = Number.isFinite(ratioDb)
      ? ratioDb
      : totalAssets > 0
        ? (cash / totalAssets) * 100
        : 0;
    return {
      date: row.date,
      totalAssets,
      market: mv,
      cash,
      cashRatio,
    };
  });

  const ratePayload = useMwrUi
    ? drawAnalysisMwrRatePlaceholder(analysisRateChart, "资金加权收益率只算总值、不算每日走势。")
    : drawLineChart(mySeries, benchSeries);
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
      leftLabel: "",
      xLabel: "",
      valueFormatter: (value) => formatNumber(value, 2),
      axisFormatter: (value) => formatNumber(value, 2),
      yRangePadding: {
        minFactor: ANALYSIS_CHART_AXIS_MIN_FACTOR,
        maxFactor: ANALYSIS_CHART_AXIS_MAX_FACTOR,
      },
    }
  );
  const assetPayload = drawAssetChart(assetSeries);

  const refreshAnalysisView = () => {
    renderControls();
    void renderAnalysis({ showLoading: false });
  };

  const analysisChartPayloads = {
    rate: ratePayload,
    profit: profitPayload,
    asset: assetPayload,
  };
  const redrawAnalysisChartsOnly = () => {
    analysisChartPayloads.rate = useMwrUi
      ? drawAnalysisMwrRatePlaceholder(analysisRateChart, "资金加权收益率只算总值、不算每日走势。")
      : drawLineChart(mySeries, benchSeries);
    analysisChartPayloads.profit = drawDualLineChart(
      analysisProfitChart,
      profitSeries.map((item) => ({ date: item.date, value: item.value })),
      null,
      "#f45a68",
      null,
      {
        keyA: "profit",
        labelA: "收益",
        yAxisMode: "left",
        leftLabel: "",
        xLabel: "",
        valueFormatter: (value) => formatNumber(value, 2),
        axisFormatter: (value) => formatNumber(value, 2),
        yRangePadding: {
          minFactor: ANALYSIS_CHART_AXIS_MIN_FACTOR,
          maxFactor: ANALYSIS_CHART_AXIS_MAX_FACTOR,
        },
      }
    );
    analysisChartPayloads.asset = drawAssetChart(assetSeries);
  };

  const rateHasBenchmark = state.benchmark !== "none";
  bindInteractiveChart(analysisRateChart, analysisRateTooltip, () => analysisChartPayloads.rate, {
    mode: "analysis",
    onRefresh: refreshAnalysisView,
    onRedraw: redrawAnalysisChartsOnly,
    valueFormatter: (_value, key) => {
      if (key === "benchmark" && !rateHasBenchmark) {
        return "--";
      }
      return `${formatNumber(_value, 2)}%`;
    },
  });
  bindInteractiveChart(analysisProfitChart, analysisProfitTooltip, () => analysisChartPayloads.profit, {
    mode: "analysis",
    onRefresh: refreshAnalysisView,
    onRedraw: redrawAnalysisChartsOnly,
    valueFormatter: (value) => formatNumber(value, 2),
  });
  bindInteractiveChart(analysisAssetChart, analysisAssetTooltip, () => analysisChartPayloads.asset, {
    mode: "analysis",
    onRefresh: refreshAnalysisView,
    onRedraw: redrawAnalysisChartsOnly,
    valueFormatter: (value) =>
      state.capitalTrendMode === "cash_ratio"
        ? `${formatNumber(value, 2)}%`
        : formatNumber(analysisSnapshotMoneyFromCny(value), 2),
  });

  /** 与曲线、tooltip 同一序列 */
  let headlineMwr = 0;
  if (useMwrUi && sliceRows.length) {
    if (bundle.cacheHeadlineMwr != null && Number.isFinite(bundle.cacheHeadlineMwr)) {
      headlineMwr = bundle.cacheHeadlineMwr;
    } else {
      headlineMwr = analysisXirrForStage(
        mergedFull,
        String(sliceRows[0].date).slice(0, 10),
        String(sliceRows[sliceRows.length - 1].date).slice(0, 10),
      );
    }
  }
  const lastMyTwr = mySeries.at(-1)?.rate ?? 0;
  const lastBench = benchSeries.at(-1)?.rate ?? 0;
  const lastProfit = profitSeries.at(-1)?.value ?? 0;
  const excess = lastMyTwr - lastBench;
  if (analysisRateSummary) {
    if (useMwrUi) {
      analysisRateSummary.textContent =
        state.benchmark === "none"
          ? `我的收益率 ${formatPercent(headlineMwr)}`
          : `我的 ${formatPercent(headlineMwr)} / 基准 ${formatPercent(lastBench)} / 对比 –`;
    } else {
      analysisRateSummary.textContent =
        state.benchmark === "none"
          ? `我的收益率 ${formatPercent(lastMyTwr)}`
          : `我的 ${formatPercent(lastMyTwr)} / 基准 ${formatPercent(lastBench)} / 对比 ${formatPercent(excess)}`;
    }
  }
  if (analysisProfitSummary) {
    analysisProfitSummary.textContent = `累计收益 ${formatSignedMoney(lastProfit, 2)}`;
  }
  if (analysisEodAccountCaption) {
    analysisEodAccountCaption.textContent = "";
    analysisEodAccountCaption.hidden = true;
  }
  renderAnalysisStockRank(pseudoHistory, scope, portfolio);
  void warmOverviewDailySnapshotsForEarning(scope, state.stageRange);
  } finally {
    if (showLoading) {
      hideRouteLoading();
    }
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
  if (isAnalysisMtdPreset()) {
    const mtdKey = monthToDateStartKey();
    const filtered = history.filter((point) => point.date >= mtdKey);
    if (!filtered.length) {
      return [{ date: toDateKey(new Date()), value: 0, flow: 0 }];
    }
    const windowSize = Math.min(Math.max(Math.min(filtered.length, 62), 2), filtered.length);
    const maxOffset = Math.max(0, filtered.length - windowSize);
    const offset = Math.max(0, Math.min(maxOffset, Number(state.analysisPanOffset || 0)));
    state.analysisPanOffset = offset;
    const end = filtered.length - offset;
    const start = Math.max(0, end - windowSize);
    return filtered.slice(start, end);
  }
  if (isAnalysisYtdPreset()) {
    const ytdKey = ytdStartDateKey();
    const filtered = history.filter((point) => point.date >= ytdKey);
    if (!filtered.length) {
      return [{ date: toDateKey(new Date()), value: 0, flow: 0 }];
    }
    const windowSize = Math.min(Math.max(Math.min(filtered.length, 365), 2), filtered.length);
    const maxOffset = Math.max(0, filtered.length - windowSize);
    const offset = Math.max(0, Math.min(maxOffset, Number(state.analysisPanOffset || 0)));
    state.analysisPanOffset = offset;
    const end = filtered.length - offset;
    const start = Math.max(0, end - windowSize);
    return filtered.slice(start, end);
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
  const trade = state.trades.find((t) => normalizeSymbol(t.symbol) === symKey);
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
  state.stockRecordFromPublicProfile = opts.fromPublicProfile === true;
  state.activeRecordSymbol = symbol;
  state.stockRecordAccountId = "all";
  state.previousRoute = state.route;
  state.stockRecordWindow = 30;
  state.stockRecordOffset = 0;

  await ensureFullLedgerDataLoaded();
  await ensureSymbolData(symbol);

  state.route = "stock-record";
  renderAll();
  window.scrollTo(0, 0);
  persistState();

  await renderStockRecordPage(symbol);
  // wait for layout settle on mobile after route switch
  window.setTimeout(() => void renderStockRecordPage(symbol), 40);
}

async function renderStockRecordPage(symbol) {
  const detail = state.lastPublicProfileDetail;
  const usePub = state.stockRecordFromPublicProfile && detail?.publicTrades;
  const activeAccountId = usePub ? "all" : resolveValidAccountFilter(state.stockRecordAccountId);
  if (!usePub && activeAccountId !== state.stockRecordAccountId) {
    state.stockRecordAccountId = activeAccountId;
  }
  let portfolio;
  let scope;
  if (usePub) {
    withPublicTradesContext(detail, () => {
      scope = { accountId: "all", trades: state.trades };
      portfolio = computePortfolio(scope.trades, []);
    });
  } else {
    scope = getPortfolioScope(activeAccountId);
    portfolio = computePortfolio(scope.trades, scope.cashTransfers);
  }
  const symKey = normalizeSymbol(symbol);
  const position = portfolio.positions.find((item) => normalizeSymbol(item.symbol) === symKey);
  const symbolTrades = scope.trades
    .filter((item) => normalizeSymbol(item.symbol) === symKey)
    .sort(sortTradeDesc);
  if (!position && !symbolTrades.length && !usePub && activeAccountId === "all") {
    if (state.route === "stock-record" && state.activeRecordSymbol === symbol) {
      state.route = state.previousRoute || "earning";
      state.activeRecordSymbol = null;
      persistState();
      renderRoute();
      renderOverviewAndStockTable();
    }
    return;
  }
  const quote = getQuoteBySymbol(symbol);
  const current = validNumber(quote.current, position?.currentPrice, 0);
  const prev = validNumber(quote.prevClose, position?.prevClose, current);
  const change = prev > 0 ? (current - prev) / prev : 0;
  const positionName = position?.name || symbolTrades[0]?.name || quote?.name || symbol;

  stockRecordTitle.textContent = `${getDisplayName(symbol, positionName)}(${formatSymbolForDisplay(symbol)})`;
  stockRecordTime.textContent = quote.time || state.quoteTime || "--";
  stockRecordPrice.textContent = formatNumber(current, 3);
  stockRecordPrice.className = `stock-record-price ${change >= 0 ? "up" : "down"}`;
  stockRecordChange.textContent = `${formatSignedMoney(current - prev, 2)} ${formatPercent(change)}`;
  stockRecordChange.className = `stock-record-change ${change >= 0 ? "up" : "down"}`;
  const intervalText = position
    ? formatRegretRateWithSide(position.regretRate, position.lastTradeSide)
    : "--";
  stockRecordMarket.textContent = `交易间隔 ${intervalText}`;
  stockRecordRegret.textContent = "";
  stockRecordRegret.className = "hidden";
  if (stockRecordAccountSelect) {
    stockRecordAccountSelect.value = activeAccountId;
    stockRecordAccountSelect.disabled = usePub;
    stockRecordAccountSelect.closest(".stock-record-account-wrap")?.classList.toggle("hidden", usePub);
  }
  if (stockRecordAddTradeBtn) {
    stockRecordAddTradeBtn.disabled = usePub;
    stockRecordAddTradeBtn.classList.toggle("hidden", usePub);
  }

  const recTable = stockRecordListBody?.closest("table");
  const headRow = recTable?.querySelector("thead tr");
  if (recTable) {
    recTable.classList.toggle("stock-record-table--pub", usePub);
  }
  if (headRow) {
    headRow.innerHTML = usePub
      ? `<th>日期</th><th>类型</th><th>价格</th><th class="num stock-record-amt-th"><span class="stock-record-amt-th-inner">金额<span class="stock-rank-help-wrap stock-record-amt-help-wrap"><button type="button" class="stock-rank-help-btn" aria-expanded="false" aria-label="金额占比说明">?</button><div class="stock-rank-help-bubble" role="tooltip">本次交易金额占当前总市值比例</div></span></span></th><th>股票账户</th>`
      : "<th>日期</th><th>类型</th><th>价格</th><th>数量</th><th>发生金额</th><th>股票账户</th>";
  }

  stockRecordListBody.innerHTML = symbolTrades
    .map((trade) => {
      const id = escapeHtml(String(trade.id));
      const noteColspan = usePub ? 5 : 6;
      const rowCore = `
      <tr class="stock-record-trade-row trade-row--clickable" data-record-id="${id}">
        <td>${trade.date.replace(/-/g, "/")}</td>
        <td>${trade.side === "buy" ? "买入" : "卖出"}</td>
        <td>${formatNumber(trade.price, 2)}</td>`;
      const accCell = `<td class="trade-account-cell">${formatTradeAccountCellHtml(trade, usePub ? detail : null)}</td>`;
      if (usePub) {
        const share = publicTradeAmountShareOfLatestMv(trade, detail);
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

  drawStockRecordChart(symbol, symbolTrades);
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
  const sourceTrades =
    state.stockRecordFromPublicProfile && Array.isArray(state.lastPublicProfileDetail?.publicTrades)
      ? state.lastPublicProfileDetail.publicTrades
      : state.trades;
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

function drawStockRecordChart(symbol, symbolTrades) {
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
    onRefresh: () => drawStockRecordChart(symbol, symbolTrades),
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

function drawAssetChart(assetSeries, canvas, trendMode) {
  const targetCanvas = canvas || analysisAssetChart;
  const mode = trendMode != null ? trendMode : state.capitalTrendMode;
  const fmtMoney = (v) => formatNumber(analysisSnapshotMoneyFromCny(v), 2);
  const cfg =
    mode === "market"
      ? { key: "market", label: "总市值", color: "#4f83f1", fmt: fmtMoney }
      : mode === "cash"
        ? { key: "cash", label: "现金", color: "#27ae60", fmt: fmtMoney }
        : mode === "cash_ratio"
          ? { key: "cashRatio", label: "现金占比", color: "#9b59b6", fmt: (v) => `${formatNumber(v, 2)}%` }
          : { key: "totalAssets", label: "总资产", color: "#5f6c82", fmt: fmtMoney };
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
const CHART_LABEL_BOX_HEIGHT = 20;

function drawSeriesExtrema(ctx, payload, series, valueFormatter) {
  if (!ctx || !payload || !series?.values?.length) {
    return;
  }
  const formatter = valueFormatter || ((value) => formatNumber(value, 2));
  const { minPoint, maxPoint } = pickSeriesExtremaPoints(series.values);
  const points = [maxPoint, minPoint].filter(Boolean);
  const samePoint =
    points.length === 2 &&
    points[0].date === points[1].date &&
    Number(points[0].value) === Number(points[1].value);
  const uniquePoints = samePoint ? [points[0]] : points;
  ctx.save();
  ctx.font = CHART_EXTREMA_FONT;
  ctx.textBaseline = "middle";
  uniquePoints.forEach((point, idx) => {
    const rawText = formatter(point.value, point.key || series.key, point.axis || series.axis || "left");
    const text = String(rawText || "");
    const textWidth = Math.max(44, ctx.measureText(text).width + 12);
    const preferRight = point.x <= (payload.xMin + payload.xMax) / 2;
    let x = preferRight ? point.x + textWidth / 2 + 8 : point.x - textWidth / 2 - 8;
    x = Math.max(payload.xMin + textWidth / 2 + 2, Math.min(payload.xMax - textWidth / 2 - 2, x));
    let y = point.y + (idx === 0 ? -16 : 16);
    y = Math.max(payload.yMin + 10, Math.min(payload.yMax - 10, y));
    ctx.fillStyle = series.color || "#2f80f6";
    ctx.beginPath();
    ctx.arc(point.x, point.y, 3.5, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = "rgb(33 41 54 / 86%)";
    ctx.fillRect(x - textWidth / 2, y - CHART_LABEL_BOX_HEIGHT / 2, textWidth, CHART_LABEL_BOX_HEIGHT);
    ctx.fillStyle = "#fff";
    ctx.textAlign = "center";
    ctx.fillText(text, x, y);
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
      if (mode === "analysis" && reason === "redraw" && typeof runtime.options.onRedraw === "function") {
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
    requestRefresh(runtime.options.mode === "analysis" ? "data" : "redraw");
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
      startX = event.clientX;
      lastMoveX = event.clientX;
      if (event.pointerType !== "mouse") {
        updateCrosshair(event.clientX, event.clientY);
      }
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
        if (runtime.options.mode === "stock") {
          const total = payload?.seriesList?.[0]?.values?.length || 0;
          updateStockRecordWindowByScale(scale, total);
        } else {
          updateAnalysisWindowByScale(scale);
          renderControls();
        }
        requestRefresh(runtime.options.mode === "analysis" ? "data" : "redraw");
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
        const deltaX = event.clientX - lastMoveX;
        lastMoveX = event.clientX;
        handlePan(deltaX, payload);
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
    if (panStarted && runtime.options.mode === "analysis") {
      renderControls();
    }
    pressing = false;
    moved = false;
    panStarted = false;
    lastMoveX = 0;
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

function updateAnalysisWindowByScale(scale) {
  if (!Number.isFinite(scale) || scale === 1) {
    return;
  }
  if (state.analysisRangeMode !== "preset") {
    return;
  }
  if (state.analysisPreset === "mtd" || state.analysisPreset === "ytd") {
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

async function refreshMarketData(opts = {}) {
  const skipFinalRender = opts.skipFinalRender === true;
  if (isSnapshotUiTestMode()) {
    state.marketLoading = false;
    state.marketDataDelayed = false;
    state.marketDataDelaySource = "";
    if (!skipFinalRender && state.route !== "analysis") {
      renderAll();
    }
    return;
  }
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
      if (state.route === "community-profile" && state.lastPublicProfileDetail?.publicTrades) {
        refreshPublicProfileEarningPanel();
        if (state.communityProfileTab === "analysis") {
          void renderPublicProfileAnalysis(state.lastPublicProfileDetail);
        }
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

async function fetchMarketSnapshot({ quoteSymbols = [], klineSymbols = [], klineLen = KLINE_DATALEN, includeFx = true } = {}) {
  if (!apiReady) {
    throw new Error("api not ready");
  }
  const body = {
    quoteSymbols: [...new Set((quoteSymbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))],
    klineSymbols: [...new Set((klineSymbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))],
    klineLen: Math.max(2, Math.min(5000, Number(klineLen) || KLINE_DATALEN)),
    includeFx: includeFx !== false,
  };
  const response = await apiFetch(`${getApiBaseForFetch()}/market/snapshot`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    cache: "no-store",
    timeoutMs: 35_000,
  });
  readMarketDelayFromResponse(response);
  if (!response.ok) {
    throw new Error(`market snapshot ${response.status}`);
  }
  const result = await response.json();
  if (!result?.ok) {
    throw new Error(String(result?.error || "market snapshot failed"));
  }
  return result;
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

/**
 * 历史 USD/HKD→CNY：只合并 analysis 日快照（服务端已写入的 fxUsdCny / fxHkdCny），
 * 不再在浏览器请求 /api/daily-close 外汇或新浪日 K。
 */
function mergeFxRatesFromAnalysisDailyRows(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    return;
  }
  const map = { ...(state.fxRatesToCnyByDate || {}) };
  for (const row of rows) {
    const dk = String(row.date || "").slice(0, 10);
    if (!dk) {
      continue;
    }
    const usdRaw =
      row.fxUsdCny != null
        ? Number(row.fxUsdCny)
        : row.fx_usd_cny != null
          ? Number(row.fx_usd_cny)
          : NaN;
    const hkdRaw =
      row.fxHkdCny != null
        ? Number(row.fxHkdCny)
        : row.fx_hkd_cny != null
          ? Number(row.fx_hkd_cny)
          : NaN;
    const slot = { ...(map[dk] || {}) };
    if (Number.isFinite(usdRaw) && usdRaw > 0) {
      slot.USD = usdRaw;
    }
    if (Number.isFinite(hkdRaw) && hkdRaw > 0) {
      slot.HKD = hkdRaw;
    }
    if ((slot.USD != null && slot.USD > 0) || (slot.HKD != null && slot.HKD > 0)) {
      map[dk] = slot;
    }
  }
  state.fxRatesToCnyByDate = map;
  state.fxLoaded = true;
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
