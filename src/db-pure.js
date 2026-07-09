const { randomUUID } = require("node:crypto");

const SEED_USER_PHONE = "18310270720";

const DEFAULT_SETTINGS = {
  route: "community-feed",
  useDemoData: true,
  algoMode: "twr",
  benchmark: "none",
  stageRange: "month",
  rangeDays: 30,
  analysisRangeMode: "preset",
  analysisPreset: null,
  customRangeStart: "",
  customRangeEnd: "",
  capitalTrendMode: "total_assets",
  capitalAmount: 0,
  accounts: [{ id: "default", name: "默认账户", currency: "CNY", createdAt: 0 }],
  selectedAccountId: "all",
  tradeFilterAccountId: "all",
  stockSortKey: "default",
  stockSortOrder: "default",
  stockAmountDisplay: "native",
};

function nowMs() {
  return Date.now();
}

function toDateKey(input) {
  const date = input ? new Date(input) : new Date();
  if (Number.isNaN(date.getTime())) {
    const fallback = new Date();
    return `${fallback.getFullYear()}-${String(fallback.getMonth() + 1).padStart(2, "0")}-${String(
      fallback.getDate()
    ).padStart(2, "0")}`;
  }
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(
    date.getDate()
  ).padStart(2, "0")}`;
}

function validNumber(...values) {
  for (const value of values) {
    const num = Number(value);
    if (Number.isFinite(num)) {
      return num;
    }
  }
  return 0;
}

function normalizeAccountRecords(rawList) {
  const seen = new Set();
  const base = [];
  const input = Array.isArray(rawList) ? rawList : [];
  for (const raw of input) {
    const id = String(raw?.id || "").trim();
    if (!id || seen.has(id)) {
      continue;
    }
    const name = String(raw?.name || "").trim() || "未命名账户";
    const currency = ["CNY", "USD", "HKD"].includes(String(raw?.currency || "CNY").toUpperCase())
      ? String(raw.currency).toUpperCase()
      : "CNY";
    const createdAt = validNumber(raw?.createdAt, raw?.created_at, Date.now());
    base.push({ id, name, currency, createdAt });
    seen.add(id);
  }
  if (!seen.has("default")) {
    base.unshift({ id: "default", name: "默认账户", currency: "CNY", createdAt: 0 });
  } else {
    const idx = base.findIndex((item) => item.id === "default");
    base[idx] = {
      ...base[idx],
      name: "默认账户",
      currency: base[idx].currency || "CNY",
      createdAt: base[idx].createdAt || 0,
    };
  }
  base.sort((a, b) => {
    if (a.id === "default") {
      return -1;
    }
    if (b.id === "default") {
      return 1;
    }
    return Number(a.createdAt) - Number(b.createdAt);
  });
  return base;
}

/** 中文简称 / 常见误写 → 美股 ticker（记一笔若只填中文名可落到此表） */
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
  if (
    value.startsWith("sh") ||
    value.startsWith("sz") ||
    value.startsWith("hk") ||
    value.startsWith("rt_hk")
  ) {
    return value;
  }
  if (/^\d{6}$/.test(value)) {
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

/** 库内 symbol → 小写 market_tag（cn/hk/us）；sh/sz 前缀一律 cn */
function inferMarketTagFromSymbol(rawSymbol) {
  const s = normalizeSymbol(rawSymbol);
  if (!s) {
    return "";
  }
  if (s.startsWith("sh") || s.startsWith("sz")) {
    return "cn";
  }
  if (s.startsWith("hk") || s.startsWith("rt_hk")) {
    return "hk";
  }
  if (isUsTickerSymbol(s)) {
    return "us";
  }
  return "";
}

function resolveMarketTagForSymbol(rawSymbol, storedTag) {
  const prefixTag = inferMarketTagFromSymbol(rawSymbol);
  if (prefixTag === "cn" || prefixTag === "hk" || prefixTag === "us") {
    return prefixTag;
  }
  const v = String(storedTag || "")
    .trim()
    .toLowerCase();
  if (v === "cn" || v === "hk" || v === "us" || v === "ot") {
    return v || prefixTag || "ot";
  }
  return prefixTag || v || "ot";
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

function normalizedSide(type, side) {
  if (type === "dividend" || type === "merge") {
    return "sell";
  }
  if (type === "bonus" || type === "split") {
    return "buy";
  }
  return side === "sell" ? "sell" : "buy";
}

function parseSide(raw, fallbackType) {
  const value = String(raw || "").trim().toLowerCase();
  if (value === "buy" || value === "b" || value.includes("买")) {
    return "buy";
  }
  if (value === "sell" || value === "s" || value.includes("卖")) {
    return "sell";
  }
  return normalizedSide(fallbackType, "buy");
}

function parseType(rawType) {
  const value = String(rawType || "").trim().toLowerCase();
  if (!value) {
    return "trade";
  }
  if (value === "dividend" || value.includes("分红")) {
    return "dividend";
  }
  if (value === "bonus" || value.includes("送股")) {
    return "bonus";
  }
  if (value === "split" || value.includes("拆股")) {
    return "split";
  }
  if (value === "merge" || value.includes("合股")) {
    return "merge";
  }
  return "trade";
}

const NOTE_MAX_LENGTH = 500;

function normalizeNoteText(raw) {
  const text = String(raw ?? "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .trim();
  if (!text) {
    return "";
  }
  return text.length > NOTE_MAX_LENGTH ? text.slice(0, NOTE_MAX_LENGTH) : text;
}

function parseJsonStringArray(raw, maxLen = 9) {
  if (raw == null || raw === "") {
    return [];
  }
  if (Array.isArray(raw)) {
    return raw.map((v) => String(v || "").trim()).filter(Boolean).slice(0, maxLen);
  }
  try {
    const parsed = JSON.parse(String(raw));
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.map((v) => String(v || "").trim()).filter(Boolean).slice(0, maxLen);
  } catch {
    return [];
  }
}

function serializeJsonStringArray(list, maxLen = 9) {
  return JSON.stringify(parseJsonStringArray(list, maxLen));
}

function normalizeTrade(input) {
  const raw = input || {};
  const type = parseType(raw.type || raw.tradeType || raw["类型"]);
  const symbol = normalizeSymbol(
    raw.symbol || raw.code || raw.stockCode || raw.ts_code || raw["证券代码"] || raw["代码"]
  );
  const name = String(
    raw.name || raw.stockName || raw.securityName || raw["证券名称"] || raw["名称"] || symbol || "unknown"
  ).trim();
  const side = parseSide(
    raw.side || raw.direction || raw.action || raw["方向"] || raw["买卖"] || raw.type,
    type
  );
  const price = validNumber(
    raw.price,
    raw.cost,
    raw.tradePrice,
    raw.dealPrice,
    raw.avgPrice,
    raw["成交价"],
    raw["价格"],
    0
  );
  const quantity = validNumber(
    raw.quantity,
    raw.share,
    raw.qty,
    raw.volume,
    raw.shares,
    raw["数量"],
    raw["成交数量"],
    0
  );
  const amount = Math.abs(
    validNumber(
      raw.amount,
      raw.payment,
      raw.tradeAmount,
      raw.turnover,
      raw["发生金额"],
      raw["成交金额"],
      price * quantity
    )
  );
  const date = toDateKey(
    raw.date || raw.trade_date || raw.tradeDate || raw.dealDate || raw["日期"] || raw["成交日期"]
  );
  const note = normalizeNoteText(raw.note || raw.remark || raw["备注"]);
  const createdAt = validNumber(raw.createdAt, raw.created_at, raw.timestamp, Date.parse(date), nowMs());
  const amountShareRatioRaw = raw.amountShareRatio ?? raw.amount_share_ratio;
  const amountShareRatio =
    amountShareRatioRaw == null || amountShareRatioRaw === ""
      ? null
      : Number(amountShareRatioRaw);
  const imageUrls = parseJsonStringArray(raw.imageUrls ?? raw.image_urls);

  return {
    id: String(raw.id || raw.tradeId || raw.ts_id || randomUUID()),
    accountId: String(raw.accountId || raw.account_id || raw.account || "default").trim() || "default",
    type,
    symbol,
    name,
    side,
    price,
    quantity,
    amount,
    date,
    note,
    createdAt,
    amountShareRatio: Number.isFinite(amountShareRatio) ? amountShareRatio : null,
    imageUrls,
  };
}

function tradeToRow(trade, userId) {
  const safe = normalizeTrade(trade);
  const updatedAt = nowMs();
  const ratio = safe.amountShareRatio;
  return {
    id: safe.id,
    user_id: String(userId || "").trim(),
    account_id: safe.accountId || "default",
    type: safe.type,
    symbol: safe.symbol,
    name: safe.name,
    side: safe.side,
    price: safe.price,
    quantity: safe.quantity,
    amount: safe.amount,
    trade_date: safe.date,
    note: safe.note,
    created_at: safe.createdAt,
    updated_at: updatedAt,
    amount_share_ratio: ratio == null || !Number.isFinite(Number(ratio)) ? null : Number(ratio),
    image_urls: serializeJsonStringArray(safe.imageUrls),
  };
}

function rowToTrade(row) {
  const ratioRaw = row.amount_share_ratio;
  const amountShareRatio =
    ratioRaw == null || ratioRaw === "" ? null : Number(ratioRaw);
  return {
    id: row.id,
    accountId: row.account_id || "default",
    type: row.type,
    symbol: normalizeSymbol(row.symbol),
    name: row.name,
    side: row.side,
    price: Number(row.price),
    quantity: Number(row.quantity),
    amount: Number(row.amount),
    date: row.trade_date,
    note: row.note || "",
    createdAt: Number(row.created_at),
    amountShareRatio: Number.isFinite(amountShareRatio) ? amountShareRatio : null,
    imageUrls: parseJsonStringArray(row.image_urls),
  };
}

function rowToAccount(row) {
  return {
    id: row.id,
    name: row.name,
    currency: row.currency,
    createdAt: Number(row.created_at),
  };
}

function normalizeDailyReturn(input) {
  const raw = input || {};
  const accountId = String(raw.accountId || raw.account_id || "default").trim() || "default";
  const date = toDateKey(raw.date || raw.day);
  const profit = validNumber(raw.profit, raw.pnl, raw.earning, 0);
  const returnRate = validNumber(raw.returnRate, raw.return_rate, raw.rate, 0);
  let totalAsset = null;
  if (raw.totalAsset != null || raw.total_asset != null) {
    const n = Number(raw.totalAsset != null ? raw.totalAsset : raw.total_asset);
    totalAsset = Number.isFinite(n) ? n : null;
  }
  const createdAt = validNumber(raw.createdAt, raw.created_at, nowMs());
  return {
    accountId,
    date,
    profit,
    returnRate,
    totalAsset,
    createdAt,
  };
}

function rowToDailyReturn(row) {
  const totalAsset = row.total_asset == null ? null : Number(row.total_asset);
  return {
    accountId: row.account_id,
    date: row.date,
    profit: Number(row.profit),
    returnRate: Number(row.return_rate),
    totalAsset: Number.isFinite(totalAsset) ? totalAsset : null,
    createdAt: Number(row.created_at),
  };
}

function normalizeCashTransfer(raw) {
  const r = raw && typeof raw === "object" ? raw : {};
  const id = String(r.id || "").trim() || randomUUID();
  const dir = String(r.direction || "").toLowerCase();
  const direction = dir === "out" || dir === "transfer_out" || dir === "转出" ? "out" : "in";
  const amount = Math.abs(validNumber(r.amount, 0));
  return {
    id,
    accountId: String(r.accountId || r.account_id || "default").trim() || "default",
    date: toDateKey(r.date || r.transfer_date),
    direction: direction === "out" ? "out" : "in",
    amount,
    note: normalizeNoteText(r.note),
    createdAt: validNumber(r.createdAt, r.created_at, nowMs()),
  };
}

function cashTransferToRow(ct, userId) {
  const safe = normalizeCashTransfer(ct);
  const updatedAt = nowMs();
  return {
    id: safe.id,
    user_id: String(userId || "").trim(),
    account_id: safe.accountId,
    transfer_date: safe.date,
    direction: safe.direction,
    amount: safe.amount,
    note: safe.note,
    created_at: safe.createdAt,
    updated_at: updatedAt,
  };
}

function rowToCashTransfer(row) {
  return {
    id: row.id,
    accountId: row.account_id || "default",
    date: row.transfer_date,
    direction: row.direction === "out" ? "out" : "in",
    amount: Number(row.amount),
    note: row.note || "",
    createdAt: Number(row.created_at),
  };
}

function addCalendarDays(dateStr, delta) {
  const base = String(dateStr || "").slice(0, 10);
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(base);
  if (!m) {
    return base;
  }
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  d.setDate(d.getDate() + Number(delta) || 0);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(
    d.getDate()
  ).padStart(2, "0")}`;
}

module.exports = {
  SEED_USER_PHONE,
  DEFAULT_SETTINGS,
  randomUUID,
  nowMs,
  toDateKey,
  validNumber,
  normalizeAccountRecords,
  normalizeSymbol,
  inferMarketTagFromSymbol,
  resolveMarketTagForSymbol,
  isUsTickerSymbol,
  getLegacyUsAlias,
  formatSymbolForDisplay,
  normalizedSide,
  parseSide,
  parseType,
  parseJsonStringArray,
  serializeJsonStringArray,
  normalizeTrade,
  tradeToRow,
  rowToTrade,
  rowToAccount,
  normalizeDailyReturn,
  rowToDailyReturn,
  normalizeCashTransfer,
  cashTransferToRow,
  rowToCashTransfer,
  addCalendarDays,
};
