const STORAGE_KEY = "earning-clone-state-v2";
const SESSION_TAB_KEY = "stockreview_session_tabs_seeded";
const API_BASE = "/api";
const API_GET_TIMEOUT_MS = 12_000;

function apiFetch(input, init = {}) {
  const { timeoutMs, ...rest } = init || {};
  const method = String(rest.method || "GET").toUpperCase();
  const parsedTimeout = Number(timeoutMs);
  const resolvedTimeoutMs = Number.isFinite(parsedTimeout)
    ? parsedTimeout
    : method === "GET" || method === "HEAD"
      ? API_GET_TIMEOUT_MS
      : 0;
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
let quoteIntervalStarted = false;
let analysisStockRankHelpListenersBound = false;

/** 登录后按手机号隔离本地缓存，避免切换账号仍读到上一账号的 localStorage */
function getSessionStateStorageKey() {
  const phone = String(sessionPhone || "").trim();
  return phone ? `${STORAGE_KEY}::${phone}` : STORAGE_KEY;
}

/**
 * 旧版未按用户区分的 localStorage 仅一份。
 * 一次性写入 earning-clone-state-v2::18310270720（与当前登录谁无关，避免先登录别的号导致误绑）。
 * 应用启动即执行；若 183 的 scoped 已有内容则跳过，不覆盖。
 */
const LEGACY_STATE_OWNER_PHONE = "18310270720";

function migrateLegacyGlobalStateTo183ScopedOnce() {
  const scopedKey = `${STORAGE_KEY}::${LEGACY_STATE_OWNER_PHONE}`;
  if (localStorage.getItem(scopedKey)) {
    return;
  }
  const legacyRaw = localStorage.getItem(STORAGE_KEY);
  if (!legacyRaw) {
    return;
  }
  try {
    JSON.parse(legacyRaw);
    localStorage.setItem(scopedKey, legacyRaw);
    localStorage.removeItem(STORAGE_KEY);
  } catch {
    // 损坏的全局缓存不迁移
  }
}

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
  "analysisPreset",
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
const ALLOWED_PUBLIC_BENCHMARKS = new Set(["none", "sh000001", "sz399001", "rt_hkHSI", "gb_inx"]);
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
  appModule: "holdings",
  communityProfileUserId: null,
  communityProfileReturnRoute: "community-feed",
  previousRoute: "earning",
  useDemoData: true,
  algoMode: "cost",
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
  capitalTrendMode: "principal",
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
  /** 银证转账 / 出入金 */
  cashTransfers: [],
  /** 交易页子 Tab：trades | cash */
  tradePanelTab: "trades",
  editingCashTransferId: null,
  quoteMap: {},
  klineMap: {},
  nameMap: {},
  quoteTime: "--",
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
  fxLoading: false,
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
};
let apiReady = false;
let tradeSearchSuggestController = null;

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
const analysisStockRankBody = document.getElementById("analysisStockRankBody");
const demoToggleBtn = document.getElementById("demoToggleBtn");
const quickTradeBtn = document.getElementById("quickTradeBtn");
const recordTradeBtn = document.getElementById("recordTradeBtn");
const tradeAddBtn = document.getElementById("tradeAddBtn");
const setCapitalBtn = document.getElementById("setCapitalBtn");
const algoModeSelectMine = document.getElementById("algoModeSelectMine");
const mineAlgoSummary = document.getElementById("mineAlgoSummary");
const mineUserPhone = document.getElementById("mineUserPhone");
const mineChangePasswordBtn = document.getElementById("mineChangePasswordBtn");
const mineLogoutBtn = document.getElementById("mineLogoutBtn");
const appMenuBtn = document.getElementById("appMenuBtn");
const appDrawer = document.getElementById("appDrawer");
const appDrawerBackdrop = document.getElementById("appDrawerBackdrop");
const appHeaderTitle = document.getElementById("appHeaderTitle");
const mineNicknameInput = document.getElementById("mineNicknameInput");
const mineNicknameDisplay = document.getElementById("mineNicknameDisplay");
const mineCommunityPublicToggle = document.getElementById("mineCommunityPublicToggle");
const mineCommunitySaveBtn = document.getElementById("mineCommunitySaveBtn");
const mineCommunityProfileMsg = document.getElementById("mineCommunityProfileMsg");
const mineCommunityHomeMsg = document.getElementById("mineCommunityHomeMsg");
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
const stockRecordTitle = document.getElementById("stockRecordTitle");
const stockRecordTime = document.getElementById("stockRecordTime");
const stockRecordPrice = document.getElementById("stockRecordPrice");
const stockRecordChange = document.getElementById("stockRecordChange");
const stockRecordChart = document.getElementById("stockRecordChart");
const stockRecordMarket = document.getElementById("stockRecordMarket");
const stockRecordRegret = document.getElementById("stockRecordRegret");
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

const chartRuntimeMap = new Map();

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

async function refreshSessionFromServer() {
  try {
    const r = await apiFetch(`${getApiBaseForFetch()}/auth/me`, {
      cache: "no-store",
      timeoutMs: 4_000,
    });
    if (!r.ok) {
      sessionUserId = "";
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
    const phone = document.getElementById("authLoginPhone")?.value?.trim() || "";
    const password = document.getElementById("authLoginPassword")?.value || "";
    if (authLoginError) {
      authLoginError.classList.add("hidden");
    }
    try {
      const r = await apiFetch(`${API_BASE}/auth/login`, {
        method: "POST",
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
      showAppShell();
      await startAppAfterAuth();
    } catch {
      if (authLoginError) {
        authLoginError.textContent = "网络错误";
        authLoginError.classList.remove("hidden");
      }
    }
  });

  authRegisterForm?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const phone = document.getElementById("authRegPhone")?.value?.trim() || "";
    const password = document.getElementById("authRegPassword")?.value || "";
    const inviteCode = document.getElementById("authRegInvite")?.value?.trim() || "";
    if (authRegisterError) {
      authRegisterError.classList.add("hidden");
    }
    try {
      const r = await apiFetch(`${API_BASE}/auth/register`, {
        method: "POST",
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
      showAppShell();
      await startAppAfterAuth();
    } catch {
      if (authRegisterError) {
        authRegisterError.textContent = "网络错误";
        authRegisterError.classList.remove("hidden");
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
  try {
    if (!window.sessionStorage.getItem(SESSION_TAB_KEY)) {
      state.route = state.appModule === "community" ? "community-feed" : "earning";
      persistState();
      window.sessionStorage.setItem(SESSION_TAB_KEY, "1");
    }
  } catch {
    state.route = state.appModule === "community" ? "community-feed" : "earning";
    persistState();
  }
  try {
    await Promise.allSettled([
      initializeFxRates({ skipFinalRender: true }),
      refreshMarketData({ skipFinalRender: true }),
    ]);
  } catch {
    // ignore
  }
  renderAll();
  if (!quoteIntervalStarted) {
    quoteIntervalStarted = true;
    window.setInterval(() => {
      void refreshMarketData();
    }, QUOTE_REFRESH_MS);
  }
  window.dumpMonthlyReturnAudit = dumpMonthlyReturnAudit;
  window.buildMonthlyReturnAuditRows = buildMonthlyReturnAuditRows;
}

initialize();

async function initialize() {
  migrateLegacyGlobalStateTo183ScopedOnce();
  bindEvents();
  bindAuthUi();
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

async function initializeFxRates(opts = {}) {
  const skipFinalRender = opts.skipFinalRender === true;
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
    if (!skipFinalRender) {
      renderAll();
    }
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

/** 单条银证资金记录折算人民币净额（转入为正、转出为负） */
function cashTransferRowNetCny(r) {
  const acc = getAccountById(r.accountId);
  const ccy = String((acc && acc.currency) || "CNY").toUpperCase();
  const sign = r.direction === "out" ? -1 : 1;
  const nat = sign * Math.abs(Number(r.amount) || 0);
  if (!Number.isFinite(nat) || nat === 0) {
    return 0;
  }
  return ccy === "CNY" ? nat : nat * getFxRateForDate(ccy, r.date);
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
      sum += cashTransferRowNetCny(row);
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
    for (const d of dateKeys) {
      m.set(d, 0);
    }
    return m;
  }
  const dayDelta = new Map();
  for (const row of ctf) {
    const d = String(row.date || "").slice(0, 10);
    if (!d) {
      continue;
    }
    dayDelta.set(d, (dayDelta.get(d) || 0) + cashTransferRowNetCny(row));
  }
  let cum = 0;
  for (const d of dateKeys) {
    cum += dayDelta.get(d) || 0;
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
    const r = await apiFetch(`${API_BASE}/daily-close/for-trades`, { cache: "no-store" });
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
        const response = await apiFetch(`${API_BASE}/stock/name?symbol=${encodeURIComponent(sym)}`, {
          cache: "no-store",
        });
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
  const raw = localStorage.getItem(getSessionStateStorageKey());
  if (raw) {
    try {
      localParsed = JSON.parse(raw);
    } catch (error) {
      console.error("读取本地数据失败，已使用默认配置", error);
    }
  }
  // 服务端已鉴权时：始终以当前用户的 /api/state 为准（含空持仓），避免新账号落到未隔离的本地缓存
  if (remoteParsed && typeof remoteParsed === "object") {
    parsed = remoteParsed;
  } else if (localParsed) {
    parsed = localParsed;
    // Auto-migrate local state to DB-backed API when backend is available.
    if (apiReady) {
      const localTrades = Array.isArray(localParsed.trades) ? localParsed.trades : [];
      if (localTrades.length) {
        void importTradesToApi(localTrades, "replace");
      }
      const localCash = Array.isArray(localParsed.cashTransfers) ? localParsed.cashTransfers : [];
      if (localCash.length) {
        void importCashTransfersToApi(localCash, "replace");
      }
      void pushSettingsToApi(localParsed);
      const localDaily = Array.isArray(localParsed.dailyReturns) ? localParsed.dailyReturns : [];
      if (localDaily.length) {
        void importDailyReturnsToApi(localDaily, "replace");
      }
    }
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
    state.algoMode = parsed.algoMode ?? state.algoMode;
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
    state.dailyReturns = Array.isArray(parsed.dailyReturns)
      ? parsed.dailyReturns.map(normalizeDailyReturnRow)
      : [];
    state.appModule = parsed.appModule === "community" ? "community" : "holdings";
  }
  if (!["month", "ytd", "total"].includes(state.stageRange)) {
    state.stageRange = "month";
  }
  if (!["preset", "custom", "all"].includes(state.analysisRangeMode)) {
    state.analysisRangeMode = "preset";
  }
  if (state.capitalTrendMode === "both") {
    state.capitalTrendMode = "principal";
  }
  if (!["principal", "market"].includes(state.capitalTrendMode)) {
    state.capitalTrendMode = "principal";
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
  state.customRangeDraftStart = state.customRangeStart;
  state.customRangeDraftEnd = state.customRangeEnd;
  if (!["holdings", "community"].includes(state.appModule)) {
    state.appModule = "holdings";
  }
  if (state.route?.startsWith("community-") && state.route !== "community-profile") {
    state.appModule = "community";
  }
  const holdingsRoutes = new Set(["earning", "analysis", "trade", "holdings-ai", "trade-search"]);
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
}

function persistState() {
  const payload = {
    route: state.route === "trade-search" ? "trade" : state.route,
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
    trades: state.trades,
    cashTransfers: state.cashTransfers,
    tradePanelTab: state.tradePanelTab,
    dailyReturns: state.dailyReturns,
  };
  localStorage.setItem(getSessionStateStorageKey(), JSON.stringify(payload));
  if (apiReady) {
    void pushSettingsToApi(payload);
    void pushDailyReturnsToApi(payload.dailyReturns);
    void pushCashTransfersToApi(payload.cashTransfers);
  }
}

async function checkApiHealth() {
  try {
    const response = await apiFetch(`${API_BASE}/health`, { cache: "no-store", timeoutMs: 4_000 });
    return response.ok;
  } catch (error) {
    return false;
  }
}

async function fetchRemoteState() {
  try {
    const response = await apiFetch(`${API_BASE}/state`, { cache: "no-store", timeoutMs: 6_000 });
    if (response.status === 401) {
      return null;
    }
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
    await apiFetch(`${API_BASE}/settings`, {
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
    await apiFetch(`${API_BASE}/daily-returns/import`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "replace", rows }),
    });
  } catch (error) {
    // localStorage remains source of truth when API is down
  }
}

async function pushCashTransfersToApi(rows) {
  if (!apiReady || !Array.isArray(rows)) {
    return;
  }
  try {
    await apiFetch(`${getApiBaseForFetch()}/cash-transfers/import`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "replace", cashTransfers: rows }),
    });
  } catch (error) {
    // 离线时以 localStorage 为准
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

  mineCommunityPublicToggle?.addEventListener("change", () => void quickSaveCommunityPublicFromHome());

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
      if (target === "accounts") {
        state.route = "mine-accounts";
      } else if (target === "community") {
        state.route = "mine-community";
      } else {
        state.route = "mine-algo";
      }
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
    state.capitalTrendMode = assetCurveModeSelect.value || "principal";
    persistState();
    void renderAnalysis();
  });

  [quickTradeBtn, recordTradeBtn].filter(Boolean).forEach((button) => {
    button.addEventListener("click", openTradeStockSearch);
  });
  tradeAddBtn?.addEventListener("click", () => {
    if (state.tradePanelTab === "cash") {
      openNewCashTransferDialog();
    } else {
      openTradeStockSearch();
    }
  });
  tradeSubtabTrades?.addEventListener("click", () => {
    state.tradePanelTab = "trades";
    syncTradePanelTabUi();
    renderTradeTable();
    persistState();
  });
  tradeSubtabCash?.addEventListener("click", () => {
    state.tradePanelTab = "cash";
    syncTradePanelTabUi();
    renderTradeTable();
    persistState();
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
    refreshMarketData();
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
    const link = event.target.closest("[data-stock-record]");
    if (!link) {
      return;
    }
    const symbol = link.dataset.stockRecord;
    openStockRecordDialog(symbol);
  });

  closeStockRecordDialogBtn?.addEventListener("click", () => {
    state.stockRecordFromPublicProfile = false;
    state.route = state.previousRoute || "earning";
    persistState();
    renderRoute();
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
        const name = row.name != null ? String(row.name) : sym;
        const mkt = row.market != null ? String(row.market) : "";
        return `<li role="option" id="tssr-${i}" data-symbol="${escapeHtml(sym)}" data-name="${escapeHtml(name)}">
          <div class="trade-stock-search-name">${escapeHtml(name)}</div>
          <div class="trade-stock-search-meta">
            <span class="trade-stock-search-code">${escapeHtml(sym)}</span><br />
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
  if (mineCommunityPublicToggle) {
    mineCommunityPublicToggle.checked = sessionProfile.communityPublic !== false;
    mineCommunityPublicToggle.disabled = !sessionPhone;
  }
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
  if (uid === sessionUserId) {
    state.route = "mine";
    persistState();
    renderAll();
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
  if (!sessionUserId || card.userId === sessionUserId) {
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
      const code = escapeHtml(p.displayCode || p.symbol || "");
      const tag = escapeHtml(p.marketTag || "OT");
      const tagLower = String(p.marketTag || "ot").toLowerCase();
      return `<div class="community-top3-row">
        <span class="community-top3-rank">${i + 1}</span>
        <div class="community-top3-mid">
          <strong>${escapeHtml(p.name)}</strong>
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
  const code = escapeHtml(t.displayCode || t.symbol || "");
  const priceStr =
    t.price != null && Number.isFinite(Number(t.price)) ? formatNumber(Number(t.price), 3) : "—";
  const share = t.amountShareOfCurrentTotalMv;
  const shareStr =
    share != null && Number.isFinite(Number(share)) ? formatPercent(Number(share)) : "—";
  const dateDisplay = String(t.date || "—").replace(/-/g, "\u2013");
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
            <strong class="community-feed-stock-name">${escapeHtml(t.name || t.symbol)}</strong>
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
  if (!uid || !sessionUserId || uid === sessionUserId || !btnEl) {
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
      communityFeedList.innerHTML = `<p class="empty">暂无动态，试试「关注的人」或「排行」</p>`;
      return;
    }
    communityFeedList.innerHTML = rows.map((t) => feedRowHtml(t)).join("");
  } catch {
    communityFeedList.innerHTML = `<p class="empty">网络错误</p>`;
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
    communityFollowingList.innerHTML = cards.map((c) => wrapInteractiveCommunityCard(c)).join("");
  } catch {
    communityFollowingList.innerHTML = `<p class="empty">网络错误</p>`;
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
    communityLeaderboardList.innerHTML = entries
      .map((c, idx) =>
        wrapInteractiveCommunityCard(c, { showRank: idx + 1 }),
      )
      .join("");
  } catch {
    communityLeaderboardList.innerHTML = `<p class="empty">网络错误</p>`;
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

/** 分析区：总市值按区间内首日归一为 1（脱敏，仅形态） */
function paintPublicProfileMarketIndexChart(canvas, tooltipEl, selectedPoints, onRefresh) {
  if (!canvas || !selectedPoints?.length) {
    return;
  }
  const v0 = Number(selectedPoints[0]?.value);
  const denom = Math.abs(v0) > 1e-9 ? v0 : 1;
  const series = selectedPoints.map((p) => ({
    date: p.date,
    value: (Number(p.value) || 0) / denom,
  }));
  const payload = drawDualLineChart(canvas, series, null, "#4f83f1", null, {
    keyA: "mvIdx",
    labelA: "总市值指数",
    yAxisMode: "left",
    leftLabel: "",
    xLabel: "",
    valueFormatter: (value) => formatNumber(value, 4),
    axisFormatter: (value) => formatNumber(value, 3),
    yRangePadding: { minFactor: 0.92, maxFactor: 1.08 },
  });
  if (tooltipEl && onRefresh) {
    bindInteractiveChart(canvas, tooltipEl, () => payload, {
      mode: "analysis",
      onRefresh,
      valueFormatter: (val) => formatNumber(val, 4),
    });
  }
}

/** 与首页总览一致：用对方脱敏后的 trades + 本金 + 对方算法/展示币种，在当前行情下重算（与本人「全部账户」视图对齐）。 */
function withPublicTradesContext(d, fn) {
  if (!d || !Array.isArray(d.publicTrades)) {
    return fn();
  }
  const prevTrades = state.trades;
  const prevAlgo = state.algoMode;
  const prevBook = state._overviewBookCurrencyOverride;
  state.trades = d.publicTrades;
  const m = String(d.publicAlgoMode ?? "cost");
  if (m === "cost" || m === "time" || m === "money") {
    state.algoMode = m;
  }
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

function renderPublicEarningProfileHtml(d) {
  if (!d || !Array.isArray(d.publicTrades)) {
    return `<p class="empty">暂无脱敏持仓数据</p>`;
  }
  return withPublicTradesContext(d, () => {
    const scope = { accountId: "all", trades: state.trades };
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
      let stageProfitOv = 0;
      if (state.stageRange === "month") {
        stageProfitOv = vis.reduce((s, p) => s + toOb(p, p.monthProfitNative), 0);
      } else if (state.stageRange === "ytd") {
        stageProfitOv = vis.reduce((s, p) => s + toOb(p, p.yearProfitNative), 0);
      } else {
        stageProfitOv = vis.reduce((s, p) => s + toOb(p, p.totalProfitNative), 0);
      }
      let stageRateOv =
        portfolio.overviewPrincipal > 0 ? stageProfitOv / portfolio.overviewPrincipal : 0;
      if (state.algoMode === "time" || state.algoMode === "money") {
        const fullHist = buildPortfolioHistory(portfolio.positions, scope.trades);
        const firstTradeDate =
          scope.trades.length > 0 ? [...scope.trades].sort(sortTradeAsc)[0].date : fullHist[0]?.date ?? null;
        const startKey = getStageStartKey(state.stageRange, firstTradeDate);
        const stageHist = fullHist.filter((p) => p.date >= startKey);
        const histForMode = stageHist.length ? stageHist : fullHist;
        stageRateOv = computeModeSeries(histForMode, state.algoMode).at(-1)?.rate ?? 0;
      }
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
                const stockCode = row.symbol.replace(/^(sh|sz|hk|gb_)/i, "").toUpperCase();
                const tag =
                  row.market === "A股" ? "CN" : row.market === "港股" ? "HK" : row.market === "美股" ? "US" : "OT";
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
            <span><i class="market-tag">${tag}</i> ${escapeHtml(stockCode)}</span>
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
          <td class="${row.regretRate >= 0 ? "up" : "down"}">${formatPercent(row.regretRate)}</td>
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
              <th><button type="button" class="th-sort-btn" data-sort-key="regretRate">后悔率<span class="sort-icon"></span></button></th>
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
    const scope = { accountId: "all", trades: state.trades };
    const portfolio = computePortfolio(scope.trades, []);
    const vis = portfolio.visiblePositions;
    const bookCcy = portfolio.overviewBookCurrency || "CNY";
    const toOb = (p, v) => nativeToOverviewBook(p, v, bookCcy);
    const prevSr = state.stageRange;
    state.stageRange = sr;
    try {
      let stageProfitOv = 0;
      if (state.stageRange === "month") {
        stageProfitOv = vis.reduce((s, p) => s + toOb(p, p.monthProfitNative), 0);
      } else if (state.stageRange === "ytd") {
        stageProfitOv = vis.reduce((s, p) => s + toOb(p, p.yearProfitNative), 0);
      } else {
        stageProfitOv = vis.reduce((s, p) => s + toOb(p, p.totalProfitNative), 0);
      }
      let stageRateOv =
        portfolio.overviewPrincipal > 0 ? stageProfitOv / portfolio.overviewPrincipal : 0;
      if (state.algoMode === "time" || state.algoMode === "money") {
        const fullHist = buildPortfolioHistory(portfolio.positions, scope.trades);
        const firstTradeDate =
          scope.trades.length > 0 ? [...scope.trades].sort(sortTradeAsc)[0].date : fullHist[0]?.date ?? null;
        const startKey = getStageStartKey(state.stageRange, firstTradeDate);
        const stageHist = fullHist.filter((p) => p.date >= startKey);
        const histForMode = stageHist.length ? stageHist : fullHist;
        stageRateOv = computeModeSeries(histForMode, state.algoMode).at(-1)?.rate ?? 0;
      }
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
      capitalTrendMode: "principal",
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
  ui.capitalTrendMode = d.publicCapitalTrendMode === "market" ? "market" : "principal";
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
        <h2>总市值走势</h2>
        <span class="caption">以首日为基数1，不展示真实金额</span>
      </div>
      <div class="chart-wrap">
        <canvas id="pubAnalysisMarketIndexChart" width="700" height="320"></canvas>
        <div id="pubAnalysisMarketIndexTooltip" class="chart-tooltip"></div>
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

function paintPublicProfileAnalysisCore(d, { useDbRows, dbRows, portfolio, scope, todayKey, liveModeRate }) {
  const pubRate = document.getElementById("pubAnalysisRateChart");
  const pubRateTip = document.getElementById("pubAnalysisRateTooltip");
  const pubRateSummary = document.getElementById("pubAnalysisRateSummary");
  const pubMkt = document.getElementById("pubAnalysisMarketIndexChart");
  const pubMktTip = document.getElementById("pubAnalysisMarketIndexTooltip");
  const pubRank = document.getElementById("pubAnalysisStockRankBody");
  if (!pubRate) {
    return;
  }

  const refresh = () => {
    void renderPublicProfileAnalysis(state.lastPublicProfileDetail);
  };

  const rankOpts = { publicStockRankLayout: true };

  const bindRateOnly = (mySeries, benchSeries) => {
    const ratePayload = drawLineChart(mySeries, benchSeries, pubRate);
    const rateHasBenchmark = state.benchmark !== "none";
    bindInteractiveChart(pubRate, pubRateTip, () => ratePayload, {
      mode: "analysis",
      onRefresh: refresh,
      valueFormatter: (_value, key) => {
        if (key === "benchmark" && !rateHasBenchmark) {
          return "--";
        }
        return `${formatNumber(_value, 2)}%`;
      },
    });
    const lastMy = mySeries.at(-1)?.rate ?? 0;
    const lastBench = benchSeries.at(-1)?.rate ?? 0;
    const excess = lastMy - lastBench;
    if (pubRateSummary) {
      pubRateSummary.textContent =
        state.benchmark === "none"
          ? `我的收益率 ${formatPercent(lastMy)}`
          : `我的 ${formatPercent(lastMy)} / 基准 ${formatPercent(lastBench)} / 对比 ${formatPercent(excess)}`;
    }
  };

  if (!useDbRows || !dbRows.length) {
    const history = buildPortfolioHistory(portfolio.positions, scope.trades);
    const selected = resolveAnalysisRange(history);
    const mySeries = computeModeSeries(selected, state.algoMode);
    const benchSeries = buildBenchmarkSeries(selected);
    bindRateOnly(mySeries, benchSeries);
    paintPublicProfileMarketIndexChart(pubMkt, pubMktTip, selected, refresh);
    renderAnalysisStockRank(history, scope, portfolio, pubRank, rankOpts);
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
  bindRateOnly(mySeries, benchSeries);
  paintPublicProfileMarketIndexChart(pubMkt, pubMktTip, selectedPh, refresh);
  renderAnalysisStockRank(pseudoHistory, scope, portfolio, pubRank, rankOpts);
}

async function renderPublicProfileAnalysis(d) {
  const detail = d || state.lastPublicProfileDetail;
  if (!detail?.publicTrades || !Array.isArray(detail.publicTrades)) {
    return;
  }
  ensurePublicProfileAnalysisUi();
  withPublicTradesContext(detail, () => {
    const scope = { accountId: "all", trades: state.trades };
    const portfolio = computePortfolio(scope.trades, []);
    const todayKey = toDateKey(new Date());
    const historyFull = buildPortfolioHistory(portfolio.positions, scope.trades);
    const liveModeRate = computeModeSeries(historyFull, state.algoMode).at(-1)?.rate ?? 0;
    const dbRows = Array.isArray(detail.analysisDaily) ? detail.analysisDaily : [];

    withPublicProfileAnalysisUi(() => {
      if (!dbRows.length) {
        paintPublicProfileAnalysisCore(detail, {
          useDbRows: false,
          dbRows: [],
          portfolio,
          scope,
          todayKey,
          liveModeRate,
        });
      } else {
        paintPublicProfileAnalysisCore(detail, {
          useDbRows: true,
          dbRows,
          portfolio,
          scope,
          todayKey,
          liveModeRate,
        });
      }
    });
  });
  syncCommunityProfileAnalysisControls();
}

let lastCommunityDataKey = "";

async function loadCommunityProfileDetail() {
  if (!communityProfileBody || !state.communityProfileUserId) {
    return;
  }
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
    if (d.isSelf) {
      state.route = "mine";
      state.communityProfileUserId = null;
      persistState();
      renderAll();
      return;
    }
    state.lastPublicProfileDetail = d;
    seedPublicProfileAnalysisUiFromDetail(d);
    const psr = String(d.publicStageRange || "month");
    state.communityProfileStage = ["month", "ytd", "total"].includes(psr) ? psr : "month";
    if (communityProfileTitle) {
      communityProfileTitle.textContent = `${d.displayName || "用户"} 的持仓`;
    }
    if (communityProfileFollowSlot) {
      if (sessionUserId && d.userId !== sessionUserId) {
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
  if (!sessionPhone || !mineCommunityPublicToggle) {
    return;
  }
  const want = mineCommunityPublicToggle.checked;
  const revertTo = !want;
  mineCommunityHomeMsg?.classList.add("hidden");
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
      mineCommunityPublicToggle.checked = revertTo;
      if (mineCommunityHomeMsg) {
        mineCommunityHomeMsg.textContent = j?.error || "保存失败";
        mineCommunityHomeMsg.classList.remove("hidden", "is-ok");
        mineCommunityHomeMsg.classList.add("is-error");
      }
      return;
    }
    sessionProfile.communityPublic = j.profile?.communityPublic !== false;
    lastCommunityDataKey = "";
    if (mineCommunityHomeMsg) {
      mineCommunityHomeMsg.textContent = "已更新";
      mineCommunityHomeMsg.classList.remove("hidden", "is-error");
      mineCommunityHomeMsg.classList.add("is-ok");
      window.setTimeout(() => mineCommunityHomeMsg.classList.add("hidden"), 1800);
    }
  } catch {
    mineCommunityPublicToggle.checked = revertTo;
    if (mineCommunityHomeMsg) {
      mineCommunityHomeMsg.textContent = "网络错误";
      mineCommunityHomeMsg.classList.remove("hidden", "is-ok");
      mineCommunityHomeMsg.classList.add("is-error");
    }
  }
}

async function saveMineCommunityProfile() {
  if (!sessionPhone) {
    return;
  }
  const nickname = mineNicknameInput?.value?.trim() || "";
  const communityPublic = mineCommunityPublicToggle?.checked ?? true;
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

function renderRoute() {
  const validRoutes = new Set([
    "earning",
    "analysis",
    "trade",
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
  document.querySelectorAll(".bottom-tabs .bottom-tab-btn").forEach((button) => {
    const r = button.dataset.route;
    if (r) {
      const onSearch = state.route === "trade-search" && r === "trade";
      button.classList.toggle("active", r === state.route || onSearch);
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
  if (state.route === "stock-record" && state.activeRecordSymbol) {
    void renderStockRecordPage(state.activeRecordSymbol);
  }
  scheduleCommunityDataLoad();
  if (state.route === "community-profile") {
    syncPublicProfileStockSortControls();
  }
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

function renderOverviewAndStockTable() {
  if (state.route === "community-profile" || state.route === "stock-record") {
    return;
  }
  const scope = getPortfolioScope(state.selectedAccountId);
  const portfolio = computePortfolio(scope.trades, scope.cashTransfers);
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
  let stageRateOv =
    portfolio.overviewPrincipal > 0 ? stageProfitOv / portfolio.overviewPrincipal : 0;
  if (state.algoMode === "time" || state.algoMode === "money") {
    const fullHist = buildPortfolioHistory(portfolio.positions, scope.trades);
    const firstTradeDate =
      scope.trades.length > 0 ? [...scope.trades].sort(sortTradeAsc)[0].date : fullHist[0]?.date ?? null;
    const startKey = getStageStartKey(state.stageRange, firstTradeDate);
    const stageHist = fullHist.filter((p) => p.date >= startKey);
    const histForMode = stageHist.length ? stageHist : fullHist;
    stageRateOv = computeModeSeries(histForMode, state.algoMode).at(-1)?.rate ?? 0;
  }
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
            2,
          )}</td>
          <td>${formatPercent(row.monthWeight)}</td>
          <td class="${applyFxForOverview(row, row.yearProfitNative) >= 0 ? "up" : "down"}">${formatStockTableMoney(
            row,
            row.yearProfitNative,
            2,
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
 * 总览区展示币种：跟随当前筛选「股票账户」的默认币种；「全部账户」时统一按人民币。
 * 注意：这仅影响总览/汇总用何种货币展示；单条交易的发生额、标的按各自原币（A/CNY、港/HKD、美/USD）在逻辑里已区分。
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
  const { startQuantity, startClose, stageFlowNative } = computePositionStageStartState(
    position,
    stageRange,
    trades,
  );
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
    .join("，");
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
  const publicRank = rankOpts.publicStockRankLayout === true;
  const hideProfitCol = publicRank || rankOpts.hideProfitColumn === true;
  const publicHoldIntervals = publicRank || rankOpts.publicHoldIntervals === true;
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

  if (!rows.length) {
    targetBody.innerHTML = `<p class="empty">本分析周期内无持仓的标的。</p>`;
    return;
  }

  const totalProfitForShare = rows.reduce((s, r) => s + r.profitCny, 0);
  const profitTh = hideProfitCol
    ? ""
    : `<span class="col-profit" role="columnheader">区间收益(¥)</span>`;
  const profitShareTh = publicRank
    ? `<span class="col-profit-share" role="columnheader">收益占比</span>`
    : "";

  targetBody.innerHTML = `
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
          const code = row.symbol.replace(/^(sh|sz|hk|gb_)/i, "").toUpperCase();
          let profitShareCell = "";
          if (publicRank) {
            const shareText =
              Math.abs(totalProfitForShare) < 1e-6
                ? "—"
                : formatPercent(row.profitCny / totalProfitForShare);
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

function syncTradePanelTabUi() {
  const isCash = state.tradePanelTab === "cash";
  tradeSubtabTrades?.classList.toggle("is-active", !isCash);
  tradeSubtabCash?.classList.toggle("is-active", isCash);
  tradeSubtabTrades?.setAttribute("aria-selected", !isCash ? "true" : "false");
  tradeSubtabCash?.setAttribute("aria-selected", isCash ? "true" : "false");
  tradeRecordsPanel?.classList.toggle("hidden", isCash);
  cashRecordsPanel?.classList.toggle("hidden", !isCash);
  if (tradeAddBtn) {
    tradeAddBtn.textContent = isCash ? "新增资金记录" : "新增交易";
  }
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

function renderCashTransferTable() {
  if (state.route === "community-profile" || state.route === "stock-record") {
    return;
  }
  if (!cashTransferTableBody) {
    return;
  }
  const rows = getFilteredCashTransfers(state.tradeFilterAccountId);
  if (!rows.length) {
    cashTransferTableBody.innerHTML = `
      <tr>
        <td colspan="5"><p class="empty">暂无资金记录，点击「新增资金记录」添加银证转账。</p></td>
      </tr>
    `;
    return;
  }
  const sorted = [...rows].sort((a, b) => {
    const c = String(b.date).localeCompare(String(a.date));
    return c !== 0 ? c : (b.createdAt || 0) - (a.createdAt || 0);
  });
  cashTransferTableBody.innerHTML = sorted
    .map((row) => {
      const acc = getAccountById(row.accountId);
      const dirLabel = row.direction === "out" ? "银证转出" : "银证转入";
      const sign = row.direction === "in" ? "+" : "-";
      const ccy = getCurrencyLabel(acc.currency);
      return `
        <tr class="cash-transfer-row" data-cash-id="${escapeHtml(String(row.id))}">
          <td>${String(row.date).replace(/-/g, "/")}</td>
          <td>${escapeHtml(acc.name || row.accountId)}</td>
          <td>${dirLabel}</td>
          <td class="num ${row.direction === "in" ? "up" : "down"}">${sign}${formatNumber(row.amount, 2)} ${ccy}</td>
          <td>${escapeHtml(row.note || "—")}</td>
        </tr>
      `;
    })
    .join("");
}

function renderTradeTable() {
  if (state.route === "community-profile" || state.route === "stock-record") {
    return;
  }
  syncTradePanelTabUi();
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
        <tr class="trade-row trade-row--clickable" data-record-id="${escapeHtml(String(trade.id))}">
          <td>${trade.date.replace(/-/g, "/")}</td>
          <td class="trade-col-name">${escapeHtml(getDisplayName(trade.symbol, trade.name))}</td>
          <td class="type-cell">${tradeDirectionCellLabel(trade)}</td>
          <td class="num">${formatNumber(trade.price, 2)}</td>
          <td class="num">${formatNumber(trade.quantity, 0)}</td>
          <td class="num ${trade.side === "buy" ? "down" : "up"}">${
            trade.side === "buy" ? "-" : "+"
          }${formatNumber(trade.amount, 2)}</td>
        </tr>
      `;
    })
    .join("");
  renderCashTransferTable();
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
  const n = state.trades.filter((t) => String(t.accountId || DEFAULT_ACCOUNT.id) === id).length;
  if (n > 0) {
    window.alert(`该账户下仍有 ${n} 条交易记录，请先删除或编辑交易改用其他账户。`);
    return;
  }
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
  const portfolio = computePortfolio(scope.trades, scope.cashTransfers);
  const history = buildPortfolioHistory(portfolio.positions, scope.trades);
  const selected = resolveAnalysisRange(history);
  const mySeries = computeModeSeries(selected, state.algoMode);
  const benchSeries = buildBenchmarkSeries(selected);
  const profitSeries = buildProfitSeries(selected);
  const assetSeries = buildAssetSeries(selected, scope.cashTransfers);

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
  renderAnalysisStockRank(history, scope, portfolio);
}

async function renderAnalysis() {
  if (state.route === "community-profile" || state.route === "stock-record") {
    return;
  }
  const scope = getPortfolioScope();
  const portfolio = computePortfolio(scope.trades, scope.cashTransfers);
  const todayKey = toDateKey(new Date());
  const historyFull = buildPortfolioHistory(portfolio.positions, scope.trades);
  const liveModeRate = computeModeSeries(historyFull, state.algoMode).at(-1)?.rate ?? 0;

  let dbRows = [];
  if (apiReady) {
    try {
      const aid = state.selectedAccountId === "all" ? "all" : state.selectedAccountId;
      const res = await apiFetch(
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
  renderAnalysisStockRank(pseudoHistory, scope, portfolio);
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
  return points.map((point) => {
    sumFlow += point.flow;
    return {
      date: point.date,
      value: point.value - startClose - sumFlow,
    };
  });
}

/** 走势：逐日 Σ发生 与 逐日 Σ资金，本金 = max(二者)（与 computePortfolio 一致） */
function buildAssetSeries(points, ctf) {
  const list = Array.isArray(ctf) ? ctf : [];
  if (!points.length) {
    return [{ date: toDateKey(new Date()), principal: 0, market: 0 }];
  }
  const dateKeys = points.map((p) => p.date);
  const fundCumByDate = fundCnyCumulativeAlongDates(list, dateKeys);
  let sigmaFlow = 0;
  return points.map((point) => {
    sigmaFlow += point.flow;
    const fundCum = fundCumByDate.get(point.date) ?? 0;
    const principal = Math.max(sigmaFlow, fundCum, 0);
    return {
      date: point.date,
      principal,
      market: point.value,
    };
  });
}

async function openStockRecordDialog(symbol, opts = {}) {
  state.stockRecordFromPublicProfile = opts.fromPublicProfile === true;
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
  const detail = state.lastPublicProfileDetail;
  const usePub = state.stockRecordFromPublicProfile && detail?.publicTrades;
  let portfolio;
  let scope;
  if (usePub) {
    withPublicTradesContext(detail, () => {
      scope = { accountId: "all", trades: state.trades };
      portfolio = computePortfolio(scope.trades, []);
    });
  } else {
    scope = getPortfolioScope();
    portfolio = computePortfolio(scope.trades, scope.cashTransfers);
  }
  const symKey = normalizeSymbol(symbol);
  const position = portfolio.positions.find((item) => normalizeSymbol(item.symbol) === symKey);
  const symbolTrades = scope.trades
    .filter((item) => normalizeSymbol(item.symbol) === symKey)
    .sort(sortTradeDesc);
  if (!position) {
    if (state.route === "stock-record" && state.activeRecordSymbol === symbol) {
      state.route = state.previousRoute || "earning";
      state.activeRecordSymbol = null;
      persistState();
      renderRoute();
      renderOverviewAndStockTable();
      renderTradeTable();
    }
    return;
  }
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
  stockRecordMarket.textContent = stockRecordAccountCaption(scope, symbolTrades);
  stockRecordRegret.textContent = `后悔率 ${formatPercent(position.regretRate)}`;
  stockRecordRegret.className = `${position.regretRate >= 0 ? "up" : "down"}`;

  const recTable = stockRecordListBody?.closest("table");
  const headRow = recTable?.querySelector("thead tr");
  if (recTable) {
    recTable.classList.toggle("stock-record-table--pub", usePub);
  }
  if (headRow) {
    headRow.innerHTML = usePub
      ? `<th>日期</th><th>类型</th><th>价格</th><th class="num stock-record-amt-th"><span class="stock-record-amt-th-inner">金额<span class="stock-rank-help-wrap stock-record-amt-help-wrap"><button type="button" class="stock-rank-help-btn" aria-expanded="false" aria-label="金额占比说明">?</button><div class="stock-rank-help-bubble" role="tooltip">本次交易金额占当前总市值比例</div></span></span></th>`
      : "<th>日期</th><th>类型</th><th>价格</th><th>数量</th><th>发生金额</th>";
  }

  stockRecordListBody.innerHTML = symbolTrades
    .map((trade) => {
      const rowCore = `
      <tr class="stock-record-trade-row" data-record-id="${escapeHtml(String(trade.id))}">
        <td>${trade.date.replace(/-/g, "/")}</td>
        <td>${trade.side === "buy" ? "买入" : "卖出"}</td>
        <td>${formatNumber(trade.price, 2)}</td>`;
      if (usePub) {
        const share = publicTradeAmountShareOfLatestMv(trade, detail);
        const shareCell =
          share != null && Number.isFinite(share) ? formatPercent(share) : "—";
        return `${rowCore}
        <td class="num">${shareCell}</td>
      </tr>`;
      }
      return `${rowCore}
        <td>${formatNumber(trade.quantity, 0)}</td>
        <td class="${trade.side === "buy" ? "down" : "up"}">${trade.side === "buy" ? "-" : "+"}${formatNumber(
          trade.amount,
          2,
        )}</td>
      </tr>`;
    })
    .join("");

  let pnlByDate = {};
  if (apiReady && !usePub) {
    try {
      const aid = state.selectedAccountId === "all" ? "all" : state.selectedAccountId;
      const res = await apiFetch(
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

function computePortfolio(trades = state.trades, cashTransfersForScope = null) {
  const tradeList = Array.isArray(trades) ? trades : state.trades;
  const ctf = Array.isArray(cashTransfersForScope)
    ? cashTransfersForScope
    : getFilteredCashTransfers(resolveValidAccountFilter(state.selectedAccountId));
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

  /** Σ发生：交易记录带符号折人民币累计（买正卖负），与「现金=本金−Σ发生」同一口径 */
  const sigmaAmountAll = tradeList.reduce(
    (sum, trade) => sum + signedAmount(trade) * getTradeFxRate(trade),
    0
  );
  /** Σ资金：资金记录银证净额（人民币计，转入正、转出负） */
  const sigmaFundCny = Array.isArray(ctf) ? ctf.reduce((sum, r) => sum + cashTransferRowNetCny(r), 0) : 0;
  /** 本金 = max(Σ发生, Σ资金)；现金 = 本金 − Σ发生（不在此重复加银证，避免与本金双计） */
  const principal = Math.max(sigmaAmountAll, sigmaFundCny, 0);
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
      yRangePadding: options.yRangePadding,
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

const ASSET_CHART_Y_MIN_FACTOR = 0.95;
const ASSET_CHART_Y_MAX_FACTOR = 1.05;

function drawAssetChart(assetSeries, canvas, trendMode) {
  const targetCanvas = canvas || analysisAssetChart;
  const mode = trendMode != null ? trendMode : state.capitalTrendMode;
  const principalSeries = assetSeries.map((item) => ({ date: item.date, value: item.principal }));
  const marketSeries = assetSeries.map((item) => ({ date: item.date, value: item.market }));
  const assetYScale = {
    yRangePadding: { minFactor: ASSET_CHART_Y_MIN_FACTOR, maxFactor: ASSET_CHART_Y_MAX_FACTOR },
  };
  if (mode === "market") {
    return drawDualLineChart(targetCanvas, marketSeries, null, "#4f83f1", null, {
      keyA: "market",
      labelA: "总市值",
      yAxisMode: "left",
      leftLabel: "总市值",
      valueFormatter: (value) => formatNumber(value, 2),
      axisFormatter: (value) => formatNumber(value, 2),
      ...assetYScale,
    });
  }
  return drawDualLineChart(targetCanvas, principalSeries, null, "#5f6c82", null, {
    keyA: "principal",
    labelA: "本金",
    yAxisMode: "left",
    leftLabel: "本金",
    valueFormatter: (value) => formatNumber(value, 2),
    axisFormatter: (value) => formatNumber(value, 2),
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
  if (existing && existing.canvas === canvas) {
    existing.payloadBuilder = payloadBuilder;
    existing.options = { ...existing.options, ...options };
    return existing;
  }
  if (existing && existing.canvas !== canvas) {
    chartRuntimeMap.delete(canvas.id);
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
    canvas,
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
      if (!skipFinalRender) {
        renderAll();
      }
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
    if (!skipFinalRender) {
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
 * 实时行情失败时的兜底：用日 K 最后两根 K 线算现价与昨收。
 * 勿用分钟线相邻两根代替昨收，否则涨跌幅会变成「几分钟内波动」，出现约 0.08% 这类与当日真实涨跌严重不符的数。
 */
async function fetchLatestQuoteFromDailyKlineFallback(symbol) {
  try {
    let list = getKlineBySymbol(symbol);
    if (!Array.isArray(list) || list.length < 2) {
      list = await fetchKlineData(symbol);
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
  let response = useProxy
    ? await apiFetch(url, { cache: "no-store" })
    : await fetch(url, {
        cache: "no-store",
        headers: SINA_KLINE_HEADERS,
      });
  if (!response.ok && useProxy) {
    response = await apiFetch(`${apiB}/sina_kline?${qs}`, { cache: "no-store" });
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
  // 已覆盖「本月起点」之前的日 K 则不必再拉新浪（含美股；否则每次 refresh / 定时刷新都会重复打满 1023 根）
  if (list.some((item) => item.day && item.day < monthStartKey)) {
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
      const r = await apiFetch(`${API_BASE}/quote/tencent?q=${encodeURIComponent(keysJoined)}`, {
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
      const r = await apiFetch(`${API_BASE}/quote/tencent?q=${encodeURIComponent(q)}`, {
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
  if (state.route === "community-profile" && state.lastPublicProfileDetail?.publicTrades?.length) {
    for (const t of state.lastPublicProfileDetail.publicTrades) {
      fromTrades.push(ensureSymbolPrefixForQuote(t.symbol));
    }
  }
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
