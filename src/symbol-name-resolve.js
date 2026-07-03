const {
  getSymbolMetaMap,
  hasSymbolNameMapEntry,
  upsertSymbolNameMapBatch,
  normalizeSymbol,
  formatSymbolForDisplay,
} = require("./db");
const { fetchTencentQuoteMetaForSymbols, marketTagForApi } = require("./tencent-quote-meta");

const SYMBOL_NAME_MAP_MISSING = "-";

function stockCodeForDisplay(symbol) {
  const sym = normalizeSymbol(symbol);
  if (!sym) {
    return "";
  }
  return formatSymbolForDisplay(sym) || sym.toUpperCase();
}

function resolveDisplayNameFromMap(symbol, nameMap) {
  const sym = normalizeSymbol(symbol);
  const raw = nameMap?.[sym];
  const name = typeof raw === "string" ? raw : String(raw?.nameCn || raw?.name || "").trim();
  return name || SYMBOL_NAME_MAP_MISSING;
}

function resolveMetaFromMap(symbol, metaMap) {
  const sym = normalizeSymbol(symbol);
  const meta = metaMap?.[sym] || {};
  const nameCn = String(meta.nameCn || meta.name || "").trim() || SYMBOL_NAME_MAP_MISSING;
  const marketTag = marketTagForApi(meta.marketTag || "ot");
  const stockCode = stockCodeForDisplay(sym);
  return { nameCn, marketTag, stockCode };
}

function collectSymbolsFromRows(rows) {
  const out = [];
  const seen = new Set();
  for (const row of rows || []) {
    const sym = normalizeSymbol(row?.symbol);
    if (sym && !seen.has(sym)) {
      seen.add(sym);
      out.push(sym);
    }
    if (Array.isArray(row?.symbols)) {
      for (const item of row.symbols) {
        const s = normalizeSymbol(typeof item === "string" ? item : item?.symbol);
        if (s && !seen.has(s)) {
          seen.add(s);
          out.push(s);
        }
      }
    }
  }
  return out;
}

function applyMetaToRow(row, metaMap) {
  const sym = normalizeSymbol(row?.symbol);
  if (!sym) {
    return row;
  }
  const meta = resolveMetaFromMap(sym, metaMap);
  row.name = meta.nameCn;
  row.marketTag = meta.marketTag;
  row.stockCode = meta.stockCode;
  return row;
}

async function enrichRowsWithSymbolMeta(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    return rows;
  }
  const metaMap = await getSymbolMetaMap(collectSymbolsFromRows(rows));
  for (const row of rows) {
    if (row?.symbol) {
      applyMetaToRow(row, metaMap);
    }
    if (Array.isArray(row?.symbols)) {
      row.symbols = row.symbols.map((item) => {
        const sym = normalizeSymbol(typeof item === "string" ? item : item?.symbol);
        if (!sym) {
          return item;
        }
        const meta = resolveMetaFromMap(sym, metaMap);
        return {
          ...(typeof item === "object" && item ? item : {}),
          symbol: sym,
          name: meta.nameCn,
          marketTag: meta.marketTag,
          stockCode: meta.stockCode,
        };
      });
    }
  }
  return rows;
}

async function enrichRowsWithSymbolNames(rows) {
  return enrichRowsWithSymbolMeta(rows);
}

async function enrichTradesWithSymbolNames(trades) {
  return enrichRowsWithSymbolMeta(trades);
}

async function enrichTopPositionsOnCards(cards) {
  if (!Array.isArray(cards) || !cards.length) {
    return cards;
  }
  const positions = [];
  for (const card of cards) {
    for (const pos of card?.topPositions || []) {
      positions.push(pos);
    }
  }
  await enrichRowsWithSymbolMeta(positions);
  return cards;
}

function applyMetaToDynamicsCard(card, metaMap) {
  if (!card) {
    return card;
  }
  if (card.cardKind === "trade" && card.symbol) {
    const meta = resolveMetaFromMap(card.symbol, metaMap);
    card.name = meta.nameCn;
    card.marketTag = meta.marketTag;
    card.stockCode = meta.stockCode;
    return card;
  }
  if (card.cardKind === "post" && Array.isArray(card.symbols)) {
    card.symbols = card.symbols.map((item) => {
      const sym = normalizeSymbol(typeof item === "string" ? item : item?.symbol);
      if (!sym) {
        return item;
      }
      const meta = resolveMetaFromMap(sym, metaMap);
      return {
        symbol: sym,
        name: meta.nameCn,
        marketTag: meta.marketTag,
        stockCode: meta.stockCode,
      };
    });
  }
  return card;
}

async function enrichDynamicsCards(cards) {
  if (!Array.isArray(cards) || !cards.length) {
    return cards;
  }
  const metaMap = await getSymbolMetaMap(collectSymbolsFromRows(cards));
  for (const card of cards) {
    applyMetaToDynamicsCard(card, metaMap);
  }
  return cards;
}

async function ensureSymbolNameMapForSymbols(symbols, { source = "tencent" } = {}) {
  const uniq = [...new Set((symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))];
  if (!uniq.length) {
    return 0;
  }
  const missing = [];
  for (const sym of uniq) {
    if (!(await hasSymbolNameMapEntry(sym))) {
      missing.push(sym);
    }
  }
  if (!missing.length) {
    return 0;
  }
  const fetched = await fetchTencentQuoteMetaForSymbols(missing);
  const batch = [];
  for (const sym of missing) {
    const hit = fetched.get(sym);
    if (!hit?.name) {
      continue;
    }
    batch.push({
      symbol: sym,
      nameCn: hit.name,
      marketTag: hit.marketTag || "ot",
      source,
    });
  }
  if (!batch.length) {
    return 0;
  }
  return upsertSymbolNameMapBatch(batch);
}

async function ensureSymbolNameMapOnNewTrade(symbol, _tradeName) {
  const sym = normalizeSymbol(symbol);
  if (!sym) {
    return;
  }
  await ensureSymbolNameMapForSymbols([sym], { source: "tencent" });
}

module.exports = {
  SYMBOL_NAME_MAP_MISSING,
  stockCodeForDisplay,
  resolveDisplayNameFromMap,
  resolveMetaFromMap,
  enrichRowsWithSymbolMeta,
  enrichRowsWithSymbolNames,
  enrichTradesWithSymbolNames,
  enrichTopPositionsOnCards,
  enrichDynamicsCards,
  ensureSymbolNameMapForSymbols,
  ensureSymbolNameMapOnNewTrade,
};
