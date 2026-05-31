/**
 * 从成交 + symbol_daily_pnl 冻结 EOD 推导持仓代码与股数。
 * 股数 = 冻结日 eod_shares + 仅当日成交净变动。
 */
const { normalizeSymbol } = require("../db");

const POSITION_EPS = 1e-6;

/** 非清仓：仅股数≈0 视为无持仓；空头（负股数）仍展示。 */
function hasOpenPositionQuantity(qty) {
  return Math.abs(Number(qty) || 0) > POSITION_EPS;
}

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

function applyEodFilterToHoldingsSymbols(symbols, eodSharesBySym, netHoldingsBySym = null) {
  if (!eodSharesBySym?.size) {
    return symbols;
  }
  return symbols.filter((s) => {
    const net = netHoldingsBySym?.get(s);
    if (net !== undefined && hasOpenPositionQuantity(net)) {
      return true;
    }
    const eod = eodSharesBySym.get(s);
    if (eod === undefined) {
      return true;
    }
    return hasOpenPositionQuantity(eod);
  });
}

/** 冻结日 symbol_daily_pnl：按标的汇总 eod 股数与冻结市值（eod_shares * eod_price）。 */
function aggregateFrozenEodBySymbol(frozenEodRows, accountScope, frozenDate) {
  const wanted = String(accountScope || "all").trim() || "all";
  const fd = String(frozenDate || "").slice(0, 10);
  const rows = (frozenEodRows || []).filter((row) => {
    const d = String(row.date || "").slice(0, 10);
    return !fd || d === fd;
  });
  let useRows = rows;
  if (wanted === "all") {
    const hasAllAcc = rows.some((row) => String(row.accountId || row.account_id || "") === "all");
    useRows = hasAllAcc
      ? rows.filter((row) => String(row.accountId || row.account_id || "") === "all")
      : rows.filter((row) => {
          const acc = String(row.accountId || row.account_id || "default");
          return acc !== "all";
        });
  } else {
    useRows = rows.filter((row) => String(row.accountId || row.account_id || "default") === wanted);
  }
  const map = new Map();
  for (const row of useRows) {
    const sym = normalizeSymbol(row.symbol);
    if (!sym) continue;
    const sh = Number(row.eodShares ?? row.eod_shares) || 0;
    const px = Number(row.eodPrice ?? row.eod_price) || 0;
    const cur = map.get(sym) || { eodShares: 0, frozenMvNat: 0 };
    cur.eodShares += sh;
    cur.frozenMvNat += sh * px;
    map.set(sym, cur);
  }
  return map;
}

function todayTradeQtyDelta(trades, symbol, todayKey, accountScope) {
  const sym = normalizeSymbol(symbol);
  const dk = String(todayKey || "").slice(0, 10);
  if (!sym || !dk) return 0;
  const wanted = String(accountScope || "all").trim() || "all";
  const list =
    wanted === "all" ? trades || [] : (trades || []).filter((t) => String(t.accountId || "default") === wanted);
  let delta = 0;
  for (const trade of list) {
    if (normalizeSymbol(trade.symbol) !== sym) continue;
    if (String(trade.date || "").slice(0, 10) !== dk) continue;
    delta += trade.side === "buy" ? Number(trade.quantity || 0) : -Number(trade.quantity || 0);
  }
  return delta;
}

function currentQuantityFromFrozenEod(frozenBySym, trades, symbol, todayKey, accountScope, tradingDay) {
  const sym = normalizeSymbol(symbol);
  const frozen = frozenBySym?.get(sym);
  const eodSh = frozen ? Number(frozen.eodShares) || 0 : 0;
  if (!tradingDay || !todayKey) {
    if (frozen && hasOpenPositionQuantity(eodSh)) {
      return eodSh;
    }
    return Number(netHoldingsBySymbol(trades, accountScope).get(sym)) || 0;
  }
  const delta = todayTradeQtyDelta(trades, sym, todayKey, accountScope);
  if (frozen) {
    return eodSh + delta;
  }
  return Number(netHoldingsBySymbol(trades, accountScope).get(sym)) || 0;
}

function frozenMvNatForSymbol(frozenBySym, symbol) {
  const frozen = frozenBySym?.get(normalizeSymbol(symbol));
  return frozen ? Number(frozen.frozenMvNat) || 0 : 0;
}

function holdingsSymbolsFromTrades(trades, accountScope, lastEodRows = null) {
  const holdings = netHoldingsBySymbol(trades, accountScope);
  let symbols = [...holdings.entries()].filter(([, q]) => hasOpenPositionQuantity(q)).map(([s]) => s);
  if (!lastEodRows?.length) {
    return symbols;
  }
  return applyEodFilterToHoldingsSymbols(
    symbols,
    eodSharesBySymbol(lastEodRows, accountScope),
    holdings,
  );
}

module.exports = {
  POSITION_EPS,
  hasOpenPositionQuantity,
  netHoldingsBySymbol,
  eodSharesBySymbol,
  aggregateFrozenEodBySymbol,
  todayTradeQtyDelta,
  currentQuantityFromFrozenEod,
  frozenMvNatForSymbol,
  applyEodFilterToHoldingsSymbols,
  holdingsSymbolsFromTrades,
};
