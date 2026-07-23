/**
 * 搜索页历史：全部账户当前持仓 + community_posts 手动发帖 symbols。
 */
const { dbQuery, normalizeSymbol, getTrades, getLastEodSharesForUser, getSymbolMetaMap } = require("./db");
const { holdingsSymbolsFromTrades } = require("./metrics/holdings-active-symbols");
const { parseSymbolsField } = require("./dynamics/community-posts-db");
const { resolveMetaFromMap } = require("./symbol-name-resolve");

const MAX_ITEMS = 50;

function latestTradeMsBySymbol(trades) {
  const map = new Map();
  for (const trade of trades || []) {
    const sym = normalizeSymbol(trade.symbol);
    if (!sym) {
      continue;
    }
    const d = String(trade.date || "").slice(0, 10);
    if (!d) {
      continue;
    }
    const ts = Date.parse(`${d}T12:00:00+08:00`) || 0;
    const prev = map.get(sym) || 0;
    if (ts > prev) {
      map.set(sym, ts);
    }
  }
  return map;
}

async function fetchPostSymbolTimestamps(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return new Map();
  }
  const { rows } = await dbQuery(
    `SELECT symbols, created_at, updated_at
     FROM community_posts
     WHERE user_id = $1
       AND symbols IS NOT NULL
       AND symbols <> ''
       AND symbols <> '[]'`,
    [uid],
  );
  const map = new Map();
  for (const row of rows) {
    const syms = parseSymbolsField(row.symbols);
    const ts = Math.max(Number(row.updated_at) || 0, Number(row.created_at) || 0);
    for (const sym of syms) {
      const prev = map.get(sym) || 0;
      if (ts > prev) {
        map.set(sym, ts);
      }
    }
  }
  return map;
}

function mergeTradeSearchHistoryItems({ holdingSymbols, postSymbolTs, tradeLatestTs }) {
  const bySym = new Map();
  for (const sym of holdingSymbols || []) {
    bySym.set(sym, {
      symbol: sym,
      sources: ["holding"],
      lastSeenAt: tradeLatestTs.get(sym) || 0,
      holding: true,
    });
  }
  for (const [sym, ts] of postSymbolTs || []) {
    const cur = bySym.get(sym);
    if (cur) {
      if (!cur.sources.includes("post")) {
        cur.sources.push("post");
      }
      cur.lastSeenAt = Math.max(cur.lastSeenAt, ts);
    } else {
      bySym.set(sym, {
        symbol: sym,
        sources: ["post"],
        lastSeenAt: ts,
        holding: false,
      });
    }
  }
  const items = [...bySym.values()];
  items.sort((a, b) => {
    if (a.holding !== b.holding) {
      return a.holding ? -1 : 1;
    }
    return (b.lastSeenAt || 0) - (a.lastSeenAt || 0);
  });
  return items.slice(0, MAX_ITEMS);
}

async function getTradeSearchHistoryForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return [];
  }
  const [trades, lastEodRows, postSymbolTs] = await Promise.all([
    getTrades(uid),
    getLastEodSharesForUser(uid),
    fetchPostSymbolTimestamps(uid),
  ]);
  const holdingSymbols = holdingsSymbolsFromTrades(trades, "all", lastEodRows);
  const tradeLatestTs = latestTradeMsBySymbol(trades);
  const merged = mergeTradeSearchHistoryItems({
    holdingSymbols,
    postSymbolTs,
    tradeLatestTs,
  });
  const metaMap = await getSymbolMetaMap(merged.map((item) => item.symbol));
  return merged.map((item) => {
    const meta = resolveMetaFromMap(item.symbol, metaMap);
    return {
      symbol: item.symbol,
      name: meta.nameCn,
      marketTag: meta.marketTag,
      stockCode: meta.stockCode,
      sources: item.sources,
      lastSeenAt: item.lastSeenAt || null,
    };
  });
}

module.exports = {
  MAX_ITEMS,
  mergeTradeSearchHistoryItems,
  getTradeSearchHistoryForUser,
};
