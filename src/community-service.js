/**
 * 社区：排行缓存、脱敏系数、公开卡片与动态（业务逻辑，依赖 db 导出方法）。
 */

const {
  getTrades,
  selectLatestSymbolDailyDate,
  listPublicCommunityUserIds,
  getUserCommunityRow,
  getCommunityLeaderboardCache,
  setCommunityLeaderboardCache,
  isCommunityFollowing,
  listCommunityFolloweeIds,
  getCommunityFeedTradesRecent,
  selectSymbolDailyPositionsOnDate,
  getLatestSymbolDailyClose,
  normalizeSymbol,
  formatSymbolForDisplay,
  getAnalysisDailySnapshots,
  getSettings,
  getAccounts,
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
  const displayCode = formatSymbolForDisplay(normalized) || normalized;
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

async function findNormalizationBaseTrade(userId) {
  const trades = (await getTrades(userId))
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

async function getNormalizationMeta(userId) {
  const base = await findNormalizationBaseTrade(userId);
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

/** 区间收益率：累计 TWR 截取 (1+R1)/(1+R0)-1 */
function subperiodForMetrics(rows, startIdx, endIdx) {
  return subperiodCumulativeReturn(rows, startIdx, endIdx, "tw_r_cumulative");
}

async function metricsFromSnapshots(userId) {
  const ytd = ytdStartKey();
  const mtd = monthStartKey();
  const rowsAll = await selectAnalysisSnapshotsForPublicMetrics(userId);
  if (!rowsAll.length) {
    return { today: null, mtd: null, ytd: null, total: null };
  }
  const last = rowsAll.length - 1;
  let total = Number(rowsAll[last].tw_r_cumulative);
  if (!Number.isFinite(total)) {
    total = 0;
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

async function snapshotFxForDate(userId, dateKey) {
  const rows = (await selectAnalysisSnapshotsForPublicMetrics(userId)).filter(
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
async function buildTopPositions(userId, factor) {
  const trades = await getTrades(userId);
  if (!trades.some((t) => String(t.type || "trade") === "trade")) {
    return [];
  }
  const qtyMap = netQtyBySymbolFromTrades(trades);
  const snapRows = await selectAnalysisSnapshotsForPublicMetrics(userId);
  const lastSnapD = snapRows.length ? String(snapRows[snapRows.length - 1].date) : null;
  const { fxUsd, fxHkd } = lastSnapD
    ? await snapshotFxForDate(userId, lastSnapD)
    : { fxUsd: FX_USD_CNY, fxHkd: FX_HKD_CNY };

  const scored = [];
  for (const [symNorm, rawQty] of qtyMap.entries()) {
    if (!isActiveHoldQty(symNorm, rawQty)) {
      continue;
    }
    const closeRow = await getLatestSymbolDailyClose(symNorm);
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

async function buildUserCard(targetId, viewerId, options = {}) {
  const { applyScale = true, allowHidden = false } = options;
  const row = await getUserCommunityRow(targetId);
  if (!row || (!allowHidden && !Number(row.community_public))) {
    return null;
  }
  const norm = await getNormalizationMeta(targetId);
  if (!norm) {
    return null;
  }
  const trades = await getTrades(targetId);
  if (!trades.some((t) => t.type === "trade")) {
    return null;
  }
  const m = await metricsFromSnapshots(targetId);
  const factor = applyScale ? norm.factor : 1;
  const topPositions = await buildTopPositions(targetId, factor);
  const vid = String(viewerId || "").trim();
  const following = vid ? await isCommunityFollowing(vid, targetId) : false;
  const followsMe = vid ? await isCommunityFollowing(targetId, vid) : false;
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

async function buildLeaderboardPayload() {
  const ids = await listPublicCommunityUserIds();
  const entries = [];
  for (const id of ids) {
    const card = await buildUserCard(id, null, { applyScale: true });
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

async function getLeaderboard() {
  const cached = await getCommunityLeaderboardCache();
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
  const payload = await buildLeaderboardPayload();
  await setCommunityLeaderboardCache(JSON.stringify(payload), now);
  return payload;
}

const ALLOWED_BENCHMARKS = new Set(["none", "sh000001", "sz399001", "rt_hkHSI", "gb_inx"]);

function normalizePublicAlgoMode(v) {
  const s = String(v || "").trim().toLowerCase();
  if (s === "time" || s === "twr") return "twr";
  if (s === "money" || s === "mwr") return "mwr";
  if (s === "cost") return "twr";
  return "twr";
}

function normalizePublicBenchmark(v) {
  const s = String(v || "none");
  return ALLOWED_BENCHMARKS.has(s) ? s : "none";
}

function normalizePublicCapitalTrendMode(v) {
  const s = String(v || "total_assets");
  if (s === "market" || s === "cash" || s === "cash_ratio" || s === "total_assets") {
    return s;
  }
  if (s === "principal") {
    return "total_assets";
  }
  return "total_assets";
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

async function assertPublicCommunityTarget(viewerId, targetId) {
  const vid = String(viewerId || "").trim();
  const tid = String(targetId || "").trim();
  if (!vid || !tid) {
    return { error: "unauthorized" };
  }
  const isSelf = vid === tid;
  const row = await getUserCommunityRow(tid);
  if (!row || (!isSelf && !Number(row.community_public))) {
    return { error: "hidden" };
  }
  return { ok: true, userId: tid, isSelf };
}

async function getPublicProfileDetail(viewerId, targetId) {
  const gate = await assertPublicCommunityTarget(viewerId, targetId);
  if (gate.error) {
    return { error: gate.error };
  }
  const tid = gate.userId;
  const card = await buildUserCard(tid, viewerId, { applyScale: false, allowHidden: gate.isSelf });
  if (!card) {
    return { error: "hidden" };
  }
  return {
    isSelf: gate.isSelf,
    userId: tid,
    displayName: card.displayName,
    following: card.following,
    mutual: card.mutual,
  };
}

async function getPublicAnalysisUiPrefs(viewerId, targetId) {
  const gate = await assertPublicCommunityTarget(viewerId, targetId);
  if (gate.error) {
    return { error: gate.error };
  }
  const settings = await getSettings(gate.userId);
  const panOff = Number(settings.analysisPanOffset);
  const analysisPanOffset = Number.isFinite(panOff) ? panOff : 0;
  return {
    publicAlgoMode: normalizePublicAlgoMode(settings.algoMode),
    publicBenchmark: normalizePublicBenchmark(settings.benchmark),
    publicCapitalTrendMode: normalizePublicCapitalTrendMode(settings.capitalTrendMode),
    publicAnalysisRangeMode: String(settings.analysisRangeMode || "preset"),
    publicAnalysisPreset: settings.analysisPreset ?? null,
    publicRangeDays: Number(settings.rangeDays) || 30,
    publicAnalysisPanOffset: analysisPanOffset,
    publicCustomRangeStart: String(settings.customRangeStart || ""),
    publicCustomRangeEnd: String(settings.customRangeEnd || ""),
  };
}

async function getPublicTrades(viewerId, targetId) {
  const gate = await assertPublicCommunityTarget(viewerId, targetId);
  if (gate.error) {
    return { error: gate.error };
  }
  const tid = gate.userId;
  const trades = (await getTrades(tid)).filter((t) => t.type === "trade");
  const rows = trades.map((t) => ({
    id: t.id,
    date: t.date,
    symbol: t.symbol,
    name: t.name,
    side: t.side,
    price: t.price,
    quantity: t.quantity,
    amount: t.amount,
    accountId: t.accountId,
    amount_share_ratio: t.amountShareRatio,
  }));
  return { rows };
}

async function enrichPublicTradesWithTencent(payload) {
  if (!payload || !Array.isArray(payload.rows)) {
    return;
  }
  const syms = [];
  const seen = new Set();
  for (const t of payload.rows) {
    const s = String(t.symbol || "").trim();
    if (s && !seen.has(s)) {
      seen.add(s);
      syms.push(s);
    }
  }
  if (!syms.length) {
    return;
  }
  const meta = await fetchTencentQuoteMetaForSymbols(syms);
  for (const t of payload.rows) {
    const m = meta.get(t.symbol);
    if (m?.name) {
      t.name = m.name;
    }
  }
}

async function enrichPublicProfileDetailWithTencent(_detail) {
  /* identity-only profile; symbol names enriched via trades bundle */
}

async function getFollowingCards(viewerId) {
  const vid = String(viewerId || "").trim();
  if (!vid) {
    return [];
  }
  const ids = await listCommunityFolloweeIds(vid);
  const out = [];
  for (const tid of ids) {
    const card = await buildUserCard(tid, vid, { applyScale: true });
    if (card) {
      out.push(card);
    }
  }
  return out;
}

async function getFeedTrades(viewerId) {
  const raw = await getCommunityFeedTradesRecent(viewerId, 800);
  const out = [];
  for (const t of raw) {
    const note = String(t.note || "");
    const meta = displayStockMeta(t.symbol);
    const ratio = t.amountShareRatio;
    out.push({
      id: t.id,
      userId: t.userId,
      displayName: displayNameForUser({ nickname: t.nickname, phone: t.phone }),
      symbol: t.symbol,
      name: t.name || t.symbol,
      price: t.price,
      side: t.side,
      date: t.date,
      note: note.length > 300 ? `${note.slice(0, 300)}…` : note,
      createdAt: t.createdAt,
      marketTag: meta.marketTag,
      displayCode: meta.displayCode,
      amount_share_ratio: ratio != null && Number.isFinite(Number(ratio)) ? Number(ratio) : null,
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
  getPublicTrades,
  getPublicAnalysisUiPrefs,
  getFollowingCards,
  getFeedTrades,
  enrichFeedRowsWithTencent,
  enrichCardsTopPositionsWithTencent,
  enrichLeaderboardPayloadWithTencent,
  enrichPublicProfileDetailWithTencent,
  enrichPublicTradesWithTencent,
  NORMALIZATION_VERSION,
};
