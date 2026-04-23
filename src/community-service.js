/**
 * 社区：排行缓存、脱敏系数、公开卡片与动态（业务逻辑，依赖 db 导出方法）。
 */

const {
  getTrades,
  getUserCommunityRow,
  selectAnalysisSnapshotsForPublicMetrics,
  selectLatestSymbolDailyDate,
  listPublicCommunityUserIds,
  getCommunityLeaderboardCache,
  setCommunityLeaderboardCache,
  isCommunityFollowing,
  listCommunityFolloweeIds,
  getCommunityFeedTradesRecent,
  selectSymbolDailyPositionsOnDate,
  getLatestSymbolDailyClose,
  normalizeSymbol,
  getAnalysisDailySnapshots,
  getSettings,
} = require("./db");
const { fetchTencentQuoteMetaForSymbols } = require("./tencent-quote-meta");

const NORMALIZATION_VERSION = 1;
/** 排行缓存：过长会导致 TOP3 等与个人页（按人民币市值）脱节；1h 折中 */
const CACHE_TTL_MS = 3600000;
const FX_USD_CNY = 7.2;
const FX_HKD_CNY = 0.92;

function maskPhone(phone) {
  const p = String(phone || "").trim();
  if (p.length === 11) {
    return `${p.slice(0, 3)}****${p.slice(7)}`;
  }
  if (p.length >= 7) {
    return `${p.slice(0, 2)}****${p.slice(-2)}`;
  }
  return "****";
}

function inferMarket(symbol) {
  const s = String(symbol || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
  if (!s) {
    return "其他";
  }
  if (s.startsWith("sh") || s.startsWith("sz")) {
    return "A股";
  }
  if (s.startsWith("hk") || s.startsWith("rt_hk")) {
    return "港股";
  }
  if (s.startsWith("gb_")) {
    return "美股";
  }
  /** 库中可能仅存 5 位港股代码（未带 hk 前缀） */
  if (/^\d{5}$/.test(s)) {
    return "港股";
  }
  /** A 股 6 位数字未带前缀 */
  if (/^\d{6}$/.test(s)) {
    return "A股";
  }
  /** 常见美股 ticker：字母开头，可含点、连字符 */
  if (/^[a-z][a-z0-9.\-]{0,14}$/.test(s)) {
    return "美股";
  }
  return "其他";
}

function displayStockMeta(symbol) {
  const normalized = normalizeSymbol(symbol) || String(symbol || "").trim().toLowerCase();
  const m = inferMarket(normalized);
  const marketTag = m === "A股" ? "CN" : m === "港股" ? "HK" : m === "美股" ? "US" : "OT";
  const s = String(normalized || "").toLowerCase();
  let displayCode = String(symbol || "");
  if (s.startsWith("sh") || s.startsWith("sz")) {
    displayCode = s.slice(2).toUpperCase();
  } else if (s.startsWith("hk")) {
    displayCode = s.slice(2).toUpperCase();
  } else if (s.startsWith("gb_")) {
    displayCode = s.slice(3).toUpperCase();
  } else {
    displayCode = s.toUpperCase();
  }
  return { marketTag, displayCode };
}

function tradeAmountCny(trade) {
  const m = inferMarket(normalizeSymbol(trade.symbol));
  const amt = Math.abs(Number(trade.amount) || 0);
  if (m === "A股" || m === "其他") {
    return amt;
  }
  if (m === "美股") {
    return amt * FX_USD_CNY;
  }
  if (m === "港股") {
    return amt * FX_HKD_CNY;
  }
  return amt;
}

function ytdStartKey() {
  const y = new Date().getFullYear();
  return `${y}-01-01`;
}

function monthStartKey() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-01`;
}

function displayNameForUser(row) {
  if (!row) {
    return "用户";
  }
  const nick = String(row.nickname || "").trim();
  if (nick) {
    return nick;
  }
  return maskPhone(row.phone);
}

function findNormalizationBaseTrade(userId) {
  const trades = getTrades(userId)
    .filter((t) => t.type === "trade")
    .sort((a, b) => a.createdAt - b.createdAt);
  for (const t of trades) {
    const cny = tradeAmountCny(t);
    if (cny >= 1000) {
      return { trade: t, amountCny: cny };
    }
  }
  for (const t of trades) {
    const cny = tradeAmountCny(t);
    if (cny > 0) {
      return { trade: t, amountCny: Math.max(cny, 100) };
    }
  }
  return null;
}

function getNormalizationMeta(userId) {
  const base = findNormalizationBaseTrade(userId);
  if (!base) {
    return null;
  }
  return {
    factor: 50000 / base.amountCny,
    normalizationVersion: NORMALIZATION_VERSION,
    baseAmountCny: base.amountCny,
  };
}

function subperiodCumulativeReturn(rows, startIdx, endIdx, field) {
  if (startIdx < 0 || endIdx < startIdx || !rows.length) {
    return null;
  }
  const r0 = Number(rows[startIdx][field]);
  const r1 = Number(rows[endIdx][field]);
  if (!Number.isFinite(r0) || !Number.isFinite(r1)) {
    return null;
  }
  return (1 + r1) / (1 + r0) - 1;
}

/**
 * 区间收益率：优先 TWR，缺失时回落为成本法累计收益率（与仅回填/未开 TWR 时仍可能有列）。
 */
function subperiodForMetrics(rows, startIdx, endIdx) {
  let s = subperiodCumulativeReturn(rows, startIdx, endIdx, "total_rate_twr");
  if (s == null) {
    s = subperiodCumulativeReturn(rows, startIdx, endIdx, "total_rate_cost");
  }
  return s;
}

function metricsFromSnapshots(userId) {
  const ytd = ytdStartKey();
  const mtd = monthStartKey();
  const rowsAll = selectAnalysisSnapshotsForPublicMetrics(userId);
  if (!rowsAll.length) {
    return { today: null, mtd: null, ytd: null, total: null };
  }
  const last = rowsAll.length - 1;
  let total = Number(rowsAll[last].total_rate_twr);
  if (!Number.isFinite(total)) {
    total = Number(rowsAll[last].total_rate_cost);
  }
  let iy = rowsAll.findIndex((r) => r.date >= ytd);
  if (iy < 0) {
    iy = 0;
  }
  const ytdR = subperiodForMetrics(rowsAll, iy, last);
  let im = rowsAll.findIndex((r) => r.date >= mtd);
  if (im < 0) {
    im = 0;
  }
  const mtdR = subperiodForMetrics(rowsAll, im, last);
  let today = null;
  if (rowsAll.length >= 2) {
    const a = rowsAll[rowsAll.length - 2];
    const b = rowsAll[rowsAll.length - 1];
    const pmv = Number(a.market_value);
    if (pmv > 0) {
      today = Number(b.profit_cny || 0) / pmv;
    }
  }
  return {
    today,
    mtd: mtdR,
    ytd: ytdR,
    total: Number.isFinite(total) ? total : null,
  };
}

function resolveNameForSymbol(trades, symbolNorm) {
  const tnorm = String(symbolNorm || "").trim();
  const hit = [...trades]
    .filter((t) => String(t.type || "trade") === "trade" && (normalizeSymbol(t.symbol) || "") === tnorm)
    .sort((a, b) => b.createdAt - a.createdAt)[0];
  return hit?.name || tnorm;
}

function bookCurrencyForSymbolNorm(symbolNorm) {
  const m = inferMarket(symbolNorm);
  if (m === "美股") {
    return "USD";
  }
  if (m === "港股") {
    return "HKD";
  }
  return "CNY";
}

function lastTradePriceForSymbol(trades, normSym) {
  const hits = trades.filter(
    (t) => String(t.type || "trade") === "trade" && (normalizeSymbol(t.symbol) || "") === normSym,
  );
  if (!hits.length) {
    return null;
  }
  hits.sort((a, b) => {
    if (a.date !== b.date) {
      return b.date.localeCompare(a.date);
    }
    return (b.createdAt || 0) - (a.createdAt || 0);
  });
  const p = Number(hits[0]?.price);
  return Number.isFinite(p) && p > 0 ? p : null;
}

function netQtyBySymbolFromTrades(trades) {
  const map = new Map();
  for (const t of trades) {
    if (String(t.type || "trade") !== "trade") {
      continue;
    }
    const sym = normalizeSymbol(t.symbol);
    if (!sym) {
      continue;
    }
    const q = Number(t.quantity) || 0;
    const delta = t.side === "buy" ? q : -q;
    map.set(sym, (map.get(sym) || 0) + delta);
  }
  return map;
}

function isActiveHoldQty(symbolNorm, qty) {
  const q = Number(qty);
  if (!Number.isFinite(q) || q <= 0 || q < 1e-6) {
    return false;
  }
  if (inferMarket(symbolNorm) === "A股" && Math.round(q) <= 0) {
    return false;
  }
  return true;
}

/** 与前端 computePortfolio 一致：持仓原币市值 → 人民币，再比大小、算权重 */
function mvNativeToCny(mvNative, currency, fxUsd, fxHkd) {
  const c = String(currency || "CNY").toUpperCase();
  const m = Math.abs(Number(mvNative) || 0);
  if (c === "USD") {
    return m * fxUsd;
  }
  if (c === "HKD") {
    return m * fxHkd;
  }
  return m;
}

function snapshotFxForDate(userId, dateKey) {
  const rows = selectAnalysisSnapshotsForPublicMetrics(userId).filter(
    (r) => String(r.date) === String(dateKey),
  );
  const hit = rows.length ? rows[rows.length - 1] : null;
  if (!hit) {
    return { fxUsd: FX_USD_CNY, fxHkd: FX_HKD_CNY };
  }
  const fxUsd = Number(hit.fx_usd_cny) > 0 ? Number(hit.fx_usd_cny) : FX_USD_CNY;
  const fxHkd = Number(hit.fx_hkd_cny) > 0 ? Number(hit.fx_hkd_cny) : FX_HKD_CNY;
  return { fxUsd, fxHkd };
}

/**
 * TOP3 与「当前成交」一致：删单后不会仍显示已清仓标的。
 * 股数来自 trades 汇总；市值 = 股数 ×（symbol_daily_close 最新收盘，缺省用最近一笔成交价）；再折人民币排序。
 */
function buildTopPositions(userId, factor) {
  const trades = getTrades(userId);
  if (!trades.some((t) => String(t.type || "trade") === "trade")) {
    return [];
  }
  const qtyMap = netQtyBySymbolFromTrades(trades);
  const snapRows = selectAnalysisSnapshotsForPublicMetrics(userId);
  const lastSnapD = snapRows.length ? String(snapRows[snapRows.length - 1].date) : null;
  const { fxUsd, fxHkd } = lastSnapD
    ? snapshotFxForDate(userId, lastSnapD)
    : { fxUsd: FX_USD_CNY, fxHkd: FX_HKD_CNY };

  const scored = [];
  for (const [symNorm, rawQty] of qtyMap.entries()) {
    if (!isActiveHoldQty(symNorm, rawQty)) {
      continue;
    }
    const closeRow = getLatestSymbolDailyClose(symNorm);
    const px = closeRow?.close ?? lastTradePriceForSymbol(trades, symNorm);
    if (!Number.isFinite(px) || px <= 0) {
      continue;
    }
    const mvNat = Math.abs(Number(rawQty)) * px;
    const ccy = bookCurrencyForSymbolNorm(symNorm);
    const mvCny = mvNativeToCny(mvNat, ccy, fxUsd, fxHkd);
    const meta = displayStockMeta(symNorm);
    scored.push({ symNorm, rawQty: Number(rawQty), mvNat, mvCny, meta, ccy });
  }

  if (!scored.length) {
    return [];
  }
  const denom = scored.reduce((s, x) => s + x.mvCny, 0);
  scored.sort((a, b) => b.mvCny - a.mvCny);
  const top = scored.slice(0, 3);

  return top.map((x) => ({
    symbol: x.symNorm,
    name: resolveNameForSymbol(trades, x.symNorm),
    weight: denom > 0 ? x.mvCny / denom : 0,
    quantity: x.rawQty * factor,
    marketValue: x.mvNat * factor,
    currency: x.ccy,
    dayPnl: 0,
    displayCode: x.meta.displayCode,
    marketTag: x.meta.marketTag,
  }));
}

function buildUserCard(targetId, viewerId, options = {}) {
  const { applyScale = true } = options;
  const row = getUserCommunityRow(targetId);
  if (!row || !Number(row.community_public)) {
    return null;
  }
  const norm = getNormalizationMeta(targetId);
  if (!norm) {
    return null;
  }
  const trades = getTrades(targetId);
  if (!trades.some((t) => t.type === "trade")) {
    return null;
  }
  const m = metricsFromSnapshots(targetId);
  const factor = applyScale ? norm.factor : 1;
  const topPositions = buildTopPositions(targetId, factor);
  const vid = String(viewerId || "").trim();
  const following = vid ? isCommunityFollowing(vid, targetId) : false;
  const followsMe = vid ? isCommunityFollowing(targetId, vid) : false;
  return {
    userId: targetId,
    displayName: displayNameForUser(row),
    todayTwr: m.today,
    mtdTwr: m.mtd,
    ytdTwr: m.ytd,
    totalTwr: m.total,
    topPositions,
    following,
    mutual: Boolean(following && followsMe),
    normalizationVersion: norm.normalizationVersion,
  };
}

function buildLeaderboardPayload() {
  const ids = listPublicCommunityUserIds();
  const entries = [];
  for (const id of ids) {
    const card = buildUserCard(id, null, { applyScale: true });
    if (!card) {
      continue;
    }
    const ytdSort = card.ytdTwr != null ? card.ytdTwr : card.totalTwr != null ? card.totalTwr : -1e9;
    entries.push({
      ...card,
      _sort: ytdSort,
    });
  }
  entries.sort((a, b) => {
    if (b._sort !== a._sort) {
      return b._sort - a._sort;
    }
    return Math.random() - 0.5;
  });
  for (const e of entries) {
    delete e._sort;
  }
  return {
    schemaVersion: 2,
    entries: entries.slice(0, 10),
    updatedAt: Date.now(),
  };
}

function getLeaderboard() {
  const cached = getCommunityLeaderboardCache();
  const now = Date.now();
  if (cached && now - Number(cached.updated_at) < CACHE_TTL_MS) {
    try {
      const p = JSON.parse(cached.payload);
      if (p && Number(p.schemaVersion) === 2) {
        return p;
      }
    } catch {
      // fall through
    }
  }
  const payload = buildLeaderboardPayload();
  setCommunityLeaderboardCache(JSON.stringify(payload), now);
  return payload;
}

const ALLOWED_BENCHMARKS = new Set(["none", "sh000001", "sz399001", "rt_hkHSI", "gb_inx"]);

function normalizePublicAlgoMode(v) {
  const s = String(v || "").trim();
  if (s === "time" || s === "money" || s === "cost") {
    return s;
  }
  return "cost";
}

function normalizePublicBenchmark(v) {
  const s = String(v || "none");
  return ALLOWED_BENCHMARKS.has(s) ? s : "none";
}

function normalizePublicCapitalTrendMode(v) {
  return v === "market" ? "market" : "principal";
}

function normalizePublicStageRange(v) {
  const s = String(v || "month");
  if (s === "ytd" || s === "total" || s === "month") {
    return s;
  }
  return "month";
}

/** 与客户端 getOverviewBookCurrency：按选中账户默认币种；全部账户为 CNY */
function overviewBookCurrencyFromSettings(settings) {
  const sel = String(settings?.selectedAccountId ?? "all");
  if (sel === "all") {
    return "CNY";
  }
  const accounts = Array.isArray(settings?.accounts) ? settings.accounts : [];
  const acc = accounts.find((a) => String(a.id) === sel);
  const c = String(acc?.currency || "CNY").toUpperCase();
  if (c === "USD" || c === "HKD" || c === "CNY") {
    return c;
  }
  return "CNY";
}

function getPublicProfileDetail(viewerId, targetId) {
  const vid = String(viewerId || "").trim();
  const tid = String(targetId || "").trim();
  if (!vid || !tid) {
    return { error: "unauthorized" };
  }
  if (vid === tid) {
    return { isSelf: true, userId: tid };
  }
  const row = getUserCommunityRow(tid);
  if (!row || !Number(row.community_public)) {
    return { error: "hidden" };
  }
  const card = buildUserCard(tid, vid, { applyScale: true });
  if (!card) {
    return { error: "hidden" };
  }
  const norm = getNormalizationMeta(tid);
  if (!norm) {
    return { error: "hidden" };
  }
  const dk = selectLatestSymbolDailyDate(tid, "all");
  const trades = getTrades(tid);
  let positions = [];
  if (dk) {
    const rows = selectSymbolDailyPositionsOnDate(tid, "all", dk);
    let sumMv = 0;
    const staged = rows.map((r) => {
      const q = Number(r.eod_shares);
      const px = Number(r.day_close_price) || 0;
      const mv = Math.abs(q * px);
      sumMv += mv;
      return { r, q, px, mv };
    });
    positions = staged.map(({ r, q, px, mv }) => ({
      symbol: r.symbol,
      name: resolveNameForSymbol(trades, r.symbol),
      quantity: q,
      close: px,
      marketValue: mv * norm.factor,
      dayPnl: Number(r.day_pnl_native) * norm.factor,
      currency: r.currency || "CNY",
      weight: sumMv > 0 ? mv / sumMv : 0,
    }));
  }
  const snaps = selectAnalysisSnapshotsForPublicMetrics(tid);
  const lastSnap = snaps.length ? snaps[snaps.length - 1] : null;
  const overview = lastSnap
    ? {
        marketValue: Number(lastSnap.market_value || 0) * norm.factor,
        profitCnyDay: Number(lastSnap.profit_cny || 0) * norm.factor,
      }
    : { marketValue: 0, profitCnyDay: 0 };

  const settings = getSettings(tid);
  const capRaw = Number(settings.capitalAmount) || 0;
  const panOff = Number(settings.analysisPanOffset);
  const analysisPanOffset = Number.isFinite(panOff) ? panOff : 0;

  const f = norm.factor;
  const publicTrades = trades.map((t) => ({
    ...t,
    quantity: Number(t.quantity) * f,
    amount: Number(t.amount) * f,
    // 该笔成交金额折算人民币（未乘脱敏系数），供他人主页「金额占比」列
    amountCnyRaw: tradeAmountCny(t),
  }));
  const analysisDaily = getAnalysisDailySnapshots(
    { accountId: "all", from: "1970-01-01", to: "2099-12-31" },
    tid,
  ).map((row) => ({
    ...row,
    profitCny: Number(row.profitCny) * f,
    totalProfit: Number(row.totalProfit) * f,
    principal: Number(row.principal) * f,
    marketValue: Number(row.marketValue) * f,
  }));

  return {
    isSelf: false,
    userId: tid,
    displayName: card.displayName,
    todayTwr: card.todayTwr,
    mtdTwr: card.mtdTwr,
    ytdTwr: card.ytdTwr,
    totalTwr: card.totalTwr,
    topPositions: card.topPositions,
    positions,
    overview,
    following: card.following,
    mutual: card.mutual,
    normalizationVersion: norm.normalizationVersion,
    normalizationFactor: f,
    // 对方最近一日快照总市值（人民币，未乘脱敏系数），与 amountCnyRaw 同口径
    publicLatestMarketValueCny: lastSnap ? Number(lastSnap.market_value || 0) : 0,
    publicCapitalAmount: capRaw * f,
    publicTrades,
    analysisDaily,
    publicAlgoMode: normalizePublicAlgoMode(settings.algoMode),
    publicOverviewBookCurrency: overviewBookCurrencyFromSettings(settings),
    publicBenchmark: normalizePublicBenchmark(settings.benchmark),
    publicCapitalTrendMode: normalizePublicCapitalTrendMode(settings.capitalTrendMode),
    publicStageRange: normalizePublicStageRange(settings.stageRange),
    publicAnalysisRangeMode: String(settings.analysisRangeMode || "preset"),
    publicAnalysisPreset: settings.analysisPreset ?? null,
    publicRangeDays: Number(settings.rangeDays) || 30,
    publicAnalysisPanOffset: analysisPanOffset,
    publicCustomRangeStart: String(settings.customRangeStart || ""),
    publicCustomRangeEnd: String(settings.customRangeEnd || ""),
  };
}

async function enrichPublicProfileDetailWithTencent(detail) {
  if (!detail || detail.isSelf || detail.error) {
    return;
  }
  const syms = [];
  const seen = new Set();
  for (const t of detail.publicTrades || []) {
    const s = String(t.symbol || "").trim();
    if (s && !seen.has(s)) {
      seen.add(s);
      syms.push(s);
    }
  }
  for (const p of detail.topPositions || []) {
    const s = String(p.symbol || "").trim();
    if (s && !seen.has(s)) {
      seen.add(s);
      syms.push(s);
    }
  }
  if (!syms.length) {
    return;
  }
  const meta = await fetchTencentQuoteMetaForSymbols(syms);
  for (const t of detail.publicTrades || []) {
    const m = meta.get(t.symbol);
    if (m?.name) {
      t.name = m.name;
    }
  }
  for (const p of detail.topPositions || []) {
    const m = meta.get(p.symbol);
    if (m?.name) {
      p.name = m.name;
    }
    if (m?.marketTag) {
      p.marketTag = m.marketTag;
    }
    if (m?.displayCode) {
      p.displayCode = m.displayCode;
    }
  }
}

function getFollowingCards(viewerId) {
  const vid = String(viewerId || "").trim();
  if (!vid) {
    return [];
  }
  const ids = listCommunityFolloweeIds(vid);
  const out = [];
  for (const tid of ids) {
    const card = buildUserCard(tid, vid, { applyScale: true });
    if (card) {
      out.push(card);
    }
  }
  return out;
}

function getFeedTrades(viewerId) {
  const raw = getCommunityFeedTradesRecent(viewerId, 800);
  const out = [];
  const mvByUser = Object.create(null);
  for (const t of raw) {
    const row = getUserCommunityRow(t.userId);
    if (!row || !Number(row.community_public)) {
      continue;
    }
    const uid = t.userId;
    if (mvByUser[uid] === undefined) {
      const snaps = selectAnalysisSnapshotsForPublicMetrics(uid);
      const last = snaps.length ? snaps[snaps.length - 1] : null;
      mvByUser[uid] = last ? Number(last.market_value || 0) : 0;
    }
    const mv = mvByUser[uid];
    const amountCnyRaw = tradeAmountCny({ symbol: t.symbol, amount: t.amount });
    const amountShareOfCurrentTotalMv =
      mv > 1e-9 && Number.isFinite(amountCnyRaw) ? Math.abs(amountCnyRaw) / mv : null;
    const note = String(t.note || "");
    const meta = displayStockMeta(t.symbol);
    out.push({
      id: t.id,
      userId: t.userId,
      displayName: displayNameForUser(row),
      symbol: t.symbol,
      name: t.name || t.symbol,
      price: t.price,
      side: t.side,
      date: t.date,
      note: note.length > 300 ? `${note.slice(0, 300)}…` : note,
      createdAt: t.createdAt,
      marketTag: meta.marketTag,
      displayCode: meta.displayCode,
      amountShareOfCurrentTotalMv,
    });
    if (out.length >= 50) {
      break;
    }
  }
  return out;
}

async function enrichFeedRowsWithTencent(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    return;
  }
  const meta = await fetchTencentQuoteMetaForSymbols(rows.map((r) => r.symbol));
  for (const row of rows) {
    const m = meta.get(row.symbol);
    if (!m) {
      continue;
    }
    row.name = m.name;
    row.marketTag = m.marketTag;
    if (m.displayCode) {
      row.displayCode = m.displayCode;
    }
  }
}

async function enrichCardsTopPositionsWithTencent(cards) {
  if (!Array.isArray(cards) || !cards.length) {
    return;
  }
  const syms = [];
  for (const c of cards) {
    for (const p of c.topPositions || []) {
      if (p.symbol) {
        syms.push(p.symbol);
      }
    }
  }
  const meta = await fetchTencentQuoteMetaForSymbols(syms);
  for (const c of cards) {
    for (const p of c.topPositions || []) {
      const m = meta.get(p.symbol);
      if (!m) {
        continue;
      }
      p.name = m.name;
      p.marketTag = m.marketTag;
      if (m.displayCode) {
        p.displayCode = m.displayCode;
      }
    }
  }
}

async function enrichLeaderboardPayloadWithTencent(payload) {
  if (!payload || !Array.isArray(payload.entries)) {
    return;
  }
  await enrichCardsTopPositionsWithTencent(payload.entries);
}

module.exports = {
  maskPhone,
  displayNameForUser,
  getNormalizationMeta,
  getLeaderboard,
  buildUserCard,
  getPublicProfileDetail,
  getFollowingCards,
  getFeedTrades,
  enrichFeedRowsWithTencent,
  enrichCardsTopPositionsWithTencent,
  enrichLeaderboardPayloadWithTencent,
  enrichPublicProfileDetailWithTencent,
  NORMALIZATION_VERSION,
};
