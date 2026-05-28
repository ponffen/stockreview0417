const {
  inferMarket,
  getSymbolCurrency,
  signedAmount,
  fxToCnyOnDate,
  normalizeTradeCalendarDateKey,
} = require("./return-calcs");

function tradeSignedCashNativeForLedger(trade) {
  const ty = String(trade.type || "trade");
  if (ty === "dividend") {
    return Math.abs(Number(trade.amount) || 0);
  }
  if (ty === "bonus" || ty === "split" || ty === "merge") {
    return 0;
  }
  return -signedAmount(trade);
}

function compareLedgerEvent(a, b) {
  const da = String(a.date || "").slice(0, 10);
  const db = String(b.date || "").slice(0, 10);
  if (da < db) return -1;
  if (da > db) return 1;
  const ca = Number(a.createdAt) || 0;
  const cb = Number(b.createdAt) || 0;
  if (ca !== cb) return ca - cb;
  if (a.kind !== b.kind) return a.kind === "ct" ? -1 : 1;
  return String(a.id || "").localeCompare(String(b.id || ""));
}

function cashTransferDeltaNative(r, accById) {
  const acc = accById.get(String(r.accountId)) || { currency: "CNY" };
  const ccy = String(acc.currency || "CNY").toUpperCase();
  const sign = String(r.direction || "").toLowerCase() === "out" ? -1 : 1;
  return sign * Math.abs(Number(r.amount) || 0);
}

function tradeCashFlowInAccountCurrency(trade, accountCurrency, fxUsdMap, fxHkdMap) {
  const acc = String(accountCurrency || "CNY").toUpperCase();
  const dateKey = String(normalizeTradeCalendarDateKey(trade.date)).slice(0, 10);
  const symCcy = String(getSymbolCurrency(trade.symbol, inferMarket(trade.symbol)) || "CNY").toUpperCase();
  const signedNat = tradeSignedCashNativeForLedger(trade);
  if (!Number.isFinite(signedNat) || signedNat === 0) {
    return 0;
  }
  const flowCny =
    symCcy === "CNY" ? signedNat : signedNat * fxToCnyOnDate(fxUsdMap, fxHkdMap, symCcy, dateKey);
  if (acc === "CNY") {
    return flowCny;
  }
  const fxAcc = fxToCnyOnDate(fxUsdMap, fxHkdMap, acc, dateKey);
  return Number.isFinite(fxAcc) && fxAcc > 0 ? flowCny / fxAcc : flowCny;
}

/**
 * 与前端 computeLedgerCashAndPrincipal 一致：各账户滚账本位币期末余额，
 * 再按 asOfDateKey 当日汇率折 CNY（与日快照其它人民币字段一致）。
 */
function computeLedgerCashCnyUpToDate(tradeList, cashRows, accounts, accountId, fxUsdMap, fxHkdMap, asOfDateKey) {
  const end = String(asOfDateKey || "").slice(0, 10);
  const accById = new Map((accounts || []).map((a) => [String(a.id), a]));
  const rowsTr = (Array.isArray(tradeList) ? tradeList : [])
    .map((t) => ({ ...t, date: normalizeTradeCalendarDateKey(t.date) }))
    .filter((t) => String(t.date).slice(0, 10) <= end);
  const rowsCtf = (Array.isArray(cashRows) ? cashRows : []).filter(
    (r) => String(r.date || "").slice(0, 10) <= end,
  );

  const filterTr = accountId === "all" ? rowsTr : rowsTr.filter((t) => String(t.accountId || "default") === String(accountId));
  const filterCtf =
    accountId === "all" ? rowsCtf : rowsCtf.filter((c) => String(c.accountId || "default") === String(accountId));

  const accountIds = new Set();
  for (const r of filterCtf) accountIds.add(String(r.accountId || "default"));
  for (const t of filterTr) accountIds.add(String(t.accountId || "default"));

  let cashCny = 0;
  for (const accId of accountIds) {
    const acc = accById.get(accId) || { currency: "CNY" };
    const accCcy = String(acc.currency || "CNY").toUpperCase();
    const events = [];
    for (const r of filterCtf) {
      if (String(r.accountId || "default") !== accId) continue;
      events.push({
        kind: "ct",
        id: String(r.id || ""),
        date: String(r.date || "").slice(0, 10),
        createdAt: Number(r.createdAt) || 0,
        delta: cashTransferDeltaNative(r, accById),
      });
    }
    for (const t of filterTr) {
      if (String(t.accountId || "default") !== accId) continue;
      events.push({
        kind: "tr",
        id: String(t.id || ""),
        date: String(t.date || "").slice(0, 10),
        createdAt: Number(t.createdAt) || 0,
        delta: tradeCashFlowInAccountCurrency(t, accCcy, fxUsdMap, fxHkdMap),
      });
    }
    events.sort(compareLedgerEvent);
    let balNat = 0;
    for (const ev of events) {
      balNat += ev.delta;
    }
    if (accCcy === "CNY") {
      cashCny += balNat;
    } else {
      cashCny += balNat * fxToCnyOnDate(fxUsdMap, fxHkdMap, accCcy, end);
    }
  }
  return cashCny;
}

function principalCnyUpToDate(cashRows, accounts, accountId, fxUsdMap, fxHkdMap, asOfDateKey) {
  const end = String(asOfDateKey || "").slice(0, 10);
  const accById = new Map((accounts || []).map((a) => [String(a.id), a]));
  const rows = (Array.isArray(cashRows) ? cashRows : []).filter((r) => String(r.date || "").slice(0, 10) <= end);
  const filtered = accountId === "all" ? rows : rows.filter((c) => String(c.accountId || "default") === String(accountId));
  let sum = 0;
  for (const r of filtered) {
    const acc = accById.get(String(r.accountId)) || { currency: "CNY" };
    const ccy = String(acc.currency || "CNY").toUpperCase();
    const sign = String(r.direction || "").toLowerCase() === "out" ? -1 : 1;
    const nat = sign * Math.abs(Number(r.amount) || 0);
    if (!Number.isFinite(nat) || nat === 0) continue;
    sum += ccy === "CNY" ? nat : nat * fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, end);
  }
  return sum;
}

/** 单日银证出入金（CNY），与 analysis_daily_snapshot.external_flow_cny 口径一致。 */
function externalFlowCnyForDate(cashRows, accounts, accountId, fxUsdMap, fxHkdMap, dateKey) {
  const dk = String(dateKey || "").slice(0, 10);
  const accById = new Map((accounts || []).map((a) => [String(a.id), a]));
  const filtered =
    String(accountId || "all").trim() === "all"
      ? cashRows || []
      : (cashRows || []).filter((c) => String(c.accountId || "default") === String(accountId));
  let sum = 0;
  for (const r of filtered) {
    if (String(r.date || "").slice(0, 10) !== dk) {
      continue;
    }
    const acc = accById.get(String(r.accountId || "default")) || { currency: "CNY" };
    const ccy = String(acc.currency || "CNY").toUpperCase();
    const sign = String(r.direction || "").toLowerCase() === "out" ? -1 : 1;
    const nat = sign * Math.abs(Number(r.amount) || 0);
    if (!Number.isFinite(nat) || nat === 0) {
      continue;
    }
    sum += ccy === "CNY" ? nat : nat * fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, dk);
  }
  return sum;
}

module.exports = {
  computeLedgerCashCnyUpToDate,
  principalCnyUpToDate,
  externalFlowCnyForDate,
  tradeSignedCashNativeForLedger,
  tradeCashFlowInAccountCurrency,
};
