/**
 * 从成交 + 可选 EOD 快照推导「当前应展示的持仓代码」。
 * EOD 仅剔除快照中明确为 0 仓的标的；快照未出现的标的（如子账户 frozen 落后时当日新开仓）保留成交净仓。
 */
const { normalizeSymbol } = require("../db");

function netHoldingsBySymbol(trades, accountScope) {
  const wanted = String(accountScope || "all").trim() || "all";
  const list =
    wanted === "all" ? trades || [] : (trades || []).filter((t) => String(t.accountId || "default") === wanted);
  const holdings = new Map();
  for (const trade of list) {
    const symbol = normalizeSymbol(trade.symbol);
    if (!symbol) continue;
    holdings.set(
      symbol,
      (holdings.get(symbol) || 0) +
        (trade.side === "buy" ? Number(trade.quantity || 0) : -Number(trade.quantity || 0)),
    );
  }
  return holdings;
}

function eodSharesBySymbol(lastEodRows, accountScope) {
  const wanted = String(accountScope || "all").trim() || "all";
  const eodSharesBySym = new Map();
  for (const row of lastEodRows || []) {
    const acc = String(row.accountId || row.account_id || "default");
    if (wanted !== "all" && acc !== wanted) continue;
    const sym = normalizeSymbol(row.symbol);
    if (!sym) continue;
    const sh = Number(row.eodShares ?? row.eod_shares) || 0;
    eodSharesBySym.set(sym, (eodSharesBySym.get(sym) || 0) + sh);
  }
  return eodSharesBySym;
}

function applyEodFilterToHoldingsSymbols(symbols, eodSharesBySym) {
  if (!eodSharesBySym?.size) {
    return symbols;
  }
  return symbols.filter((s) => {
    const eod = eodSharesBySym.get(s);
    if (eod === undefined) {
      return true;
    }
    return eod > 1e-6;
  });
}

function holdingsSymbolsFromTrades(trades, accountScope, lastEodRows = null) {
  const holdings = netHoldingsBySymbol(trades, accountScope);
  let symbols = [...holdings.entries()].filter(([, q]) => q > 1e-6).map(([s]) => s);
  if (!lastEodRows?.length) {
    return symbols;
  }
  return applyEodFilterToHoldingsSymbols(symbols, eodSharesBySymbol(lastEodRows, accountScope));
}

module.exports = {
  netHoldingsBySymbol,
  eodSharesBySymbol,
  applyEodFilterToHoldingsSymbols,
  holdingsSymbolsFromTrades,
};
