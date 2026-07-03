/**
 * Build unified dynamics cards from raw feed rows.
 */

const { displayNameForUser, displayStockMeta } = require("../community-service");
const { parseImageUrlsField } = require("./blob-images");
const { parseSymbolsField } = require("./community-posts-db");
const { normalizeSymbol } = require("../db");

function tradeSortMs(tradeDate) {
  const d = String(tradeDate || "").slice(0, 10);
  const m = d.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) {
    return 0;
  }
  const y = Number(m[1]);
  const mo = Number(m[2]) - 1;
  const day = Number(m[3]);
  return Date.UTC(y, mo, day, 15, 59, 59, 999);
}

function formatDynamicsPrice(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return v.toLocaleString("zh-CN", { minimumFractionDigits: 3, maximumFractionDigits: 3 });
}

function formatDynamicsAmount(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return v.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatDynamicsQuantity(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return v.toLocaleString("zh-CN", { maximumFractionDigits: 4 });
}

function formatDynamicsPercent(ratio) {
  const v = Number(ratio);
  if (!Number.isFinite(v)) {
    return "—";
  }
  const pct = v * 100;
  const sign = pct > 0 ? "+" : "";
  return `${sign}${pct.toFixed(2)}%`;
}

function formatDynamicsTradeDate(dateStr) {
  const d = String(dateStr || "").slice(0, 10);
  const m = d.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m ? `${m[2]}-${m[3]}` : "—";
}

function formatDynamicsDateTime(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n <= 0) {
    return "—";
  }
  const d = new Date(n);
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  return `${mm}-${dd} ${hh}:${mi}`;
}

function buildSymbolTags(symbols, nameBySymbol) {
  const list = parseSymbolsField(symbols);
  return list.map((sym) => {
    const meta = displayStockMeta(sym);
    return {
      symbol: sym,
      name: nameBySymbol?.[sym] || sym,
      marketTag: meta.marketTag,
      displayCode: meta.displayCode,
    };
  });
}

function buildTradeCard(row, ctx) {
  const meta = displayStockMeta(row.symbol);
  const accountId = String(row.account_id || "default").trim() || "default";
  const accountName = ctx.accountNameById?.[accountId] || accountId;
  const ratioRaw = row.amount_share_ratio;
  const ratio = ratioRaw == null || ratioRaw === "" ? null : Number(ratioRaw);
  return {
    cardKind: "trade",
    id: row.id,
    userId: row.user_id,
    displayName: displayNameForUser({ nickname: row.nickname, phone: row.phone }),
    createdAt: Number(row.created_at),
    sortMs: tradeSortMs(row.trade_date),
    symbol: row.symbol,
    name: row.name || row.symbol,
    marketTag: meta.marketTag,
    displayCode: meta.displayCode,
    side: row.side === "sell" ? "sell" : "buy",
    tradeDate: formatDynamicsTradeDate(row.trade_date),
    price: formatDynamicsPrice(row.price),
    quantity: formatDynamicsQuantity(row.quantity),
    amount: formatDynamicsAmount(row.amount),
    amountShareRatio: formatDynamicsPercent(ratio),
    note: String(row.note || "").trim(),
    accountId,
    accountName,
    imageUrls: parseImageUrlsField(row.image_urls),
    bottomTime: formatDynamicsDateTime(row.created_at),
  };
}

function buildPostCard(row, ctx) {
  const symbols = buildSymbolTags(row.symbols, ctx.nameBySymbol);
  return {
    cardKind: "post",
    id: row.id,
    userId: row.user_id,
    displayName: displayNameForUser({ nickname: row.nickname, phone: row.phone }),
    createdAt: Number(row.created_at),
    sortMs: Number(row.created_at),
    content: String(row.content || ""),
    symbols,
    imageUrls: parseImageUrlsField(row.image_urls),
    bottomTime: formatDynamicsDateTime(row.created_at),
  };
}

function buildCardFromFeedRow(row, ctx) {
  if (row.card_kind === "post") {
    return buildPostCard(row, ctx);
  }
  return buildTradeCard(row, ctx);
}

module.exports = {
  tradeSortMs,
  buildCardFromFeedRow,
  formatDynamicsDateTime,
};
