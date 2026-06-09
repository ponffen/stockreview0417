const {
  getSymbolNameMap,
  hasSymbolNameMapEntry,
  upsertSymbolNameMapBatch,
  normalizeSymbol,
} = require("./db");

const SYMBOL_NAME_MAP_MISSING = "-";

function resolveDisplayNameFromMap(symbol, nameMap) {
  const sym = normalizeSymbol(symbol);
  const name = String(nameMap?.[sym] || "").trim();
  return name || SYMBOL_NAME_MAP_MISSING;
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
  }
  return out;
}

async function enrichRowsWithSymbolNames(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    return rows;
  }
  const nameMap = await getSymbolNameMap(collectSymbolsFromRows(rows));
  for (const row of rows) {
    row.name = resolveDisplayNameFromMap(row.symbol, nameMap);
  }
  return rows;
}

async function enrichTradesWithSymbolNames(trades) {
  return enrichRowsWithSymbolNames(trades);
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
  await enrichRowsWithSymbolNames(positions);
  return cards;
}

async function ensureSymbolNameMapOnNewTrade(symbol, tradeName) {
  const sym = normalizeSymbol(symbol);
  if (!sym) {
    return;
  }
  if (await hasSymbolNameMapEntry(sym)) {
    return;
  }
  const nameCn = String(tradeName || "").trim();
  if (!nameCn) {
    return;
  }
  await upsertSymbolNameMapBatch([{ symbol: sym, nameCn, source: "trade" }]);
}

module.exports = {
  SYMBOL_NAME_MAP_MISSING,
  resolveDisplayNameFromMap,
  enrichRowsWithSymbolNames,
  enrichTradesWithSymbolNames,
  enrichTopPositionsOnCards,
  ensureSymbolNameMapOnNewTrade,
};
