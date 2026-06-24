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

function bookCurrencyForScope(accounts, accountId) {
  const scope = String(accountId || "all").trim() || "all";
  if (scope === "all") {
    return "CNY";
  }
  const accById = new Map((accounts || []).map((a) => [String(a.id), a]));
  const acc = accById.get(scope) || { currency: "CNY" };
  const c = String(acc.currency || "CNY").toUpperCase();
  if (c === "USD" || c === "HKD" || c === "CNY") {
    return c;
  }
  return "CNY";
}

function nativeAmountToBook(nat, nativeCcy, bookCcy, fxUsdMap, fxHkdMap, dateKey) {
  const n = Number(nat) || 0;
  if (!n) {
    return 0;
  }
  const from = String(nativeCcy || "CNY").toUpperCase();
  const book = String(bookCcy || "CNY").toUpperCase();
  if (from === book) {
    return n;
  }
  const dk = String(dateKey || "").slice(0, 10);
  const cny = from === "CNY" ? n : n * fxToCnyOnDate(fxUsdMap, fxHkdMap, from, dk);
  if (book === "CNY") {
    return cny;
  }
  const bookFx = fxToCnyOnDate(fxUsdMap, fxHkdMap, book, dk);
  return Number.isFinite(bookFx) && bookFx > 0 ? cny / bookFx : cny;
}

function ledgerCashBalanceNativeForAccount(filterTr, filterCtf, accId, accById, fxUsdMap, fxHkdMap) {
  const acc = accById.get(accId) || { currency: "CNY" };
  const accCcy = String(acc.currency || "CNY").toUpperCase();
  const events = [];
  for (const r of filterCtf) {
    if (String(r.accountId || "default") !== accId) {
      continue;
    }
    events.push({
      kind: "ct",
      id: String(r.id || ""),
      date: String(r.date || "").slice(0, 10),
      createdAt: Number(r.createdAt) || 0,
      delta: cashTransferDeltaNative(r, accById),
    });
  }
  for (const t of filterTr) {
    if (String(t.accountId || "default") !== accId) {
      continue;
    }
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
  return { balNat, accCcy };
}

function filterLedgerRowsUpToDate(tradeList, cashRows, accountId, asOfDateKey) {
  const end = String(asOfDateKey || "").slice(0, 10);
  const scope = String(accountId || "all").trim() || "all";
  const rowsTr = (Array.isArray(tradeList) ? tradeList : [])
    .map((t) => ({ ...t, date: normalizeTradeCalendarDateKey(t.date) }))
    .filter((t) => String(t.date).slice(0, 10) <= end);
  const rowsCtf = (Array.isArray(cashRows) ? cashRows : []).filter(
    (r) => String(r.date || "").slice(0, 10) <= end,
  );
  const filterTr =
    scope === "all" ? rowsTr : rowsTr.filter((t) => String(t.accountId || "default") === scope);
  const filterCtf =
    scope === "all" ? rowsCtf : rowsCtf.filter((c) => String(c.accountId || "default") === scope);
  const accountIds = new Set();
  for (const r of filterCtf) {
    accountIds.add(String(r.accountId || "default"));
  }
  for (const t of filterTr) {
    accountIds.add(String(t.accountId || "default"));
  }
  return { end, scope, filterTr, filterCtf, accountIds };
}

/**
 * 与前端 computeLedgerCashAndPrincipal 一致：各账户滚账本位币期末余额，
 * 再按 asOfDateKey 当日汇率折 CNY（与日快照其它人民币字段一致）。
 */
function computeLedgerCashCnyUpToDate(tradeList, cashRows, accounts, accountId, fxUsdMap, fxHkdMap, asOfDateKey) {
  const accById = new Map((accounts || []).map((a) => [String(a.id), a]));
  const { end, filterTr, filterCtf, accountIds } = filterLedgerRowsUpToDate(
    tradeList,
    cashRows,
    accountId,
    asOfDateKey,
  );
  let cashCny = 0;
  for (const accId of accountIds) {
    const acc = accById.get(accId) || { currency: "CNY" };
    const accCcy = String(acc.currency || "CNY").toUpperCase();
    const events = [];
    for (const r of filterCtf) {
      if (String(r.accountId || "default") !== accId) {
        continue;
      }
      events.push({
        kind: "ct",
        id: String(r.id || ""),
        date: String(r.date || "").slice(0, 10),
        createdAt: Number(r.createdAt) || 0,
        delta: cashTransferDeltaNative(r, accById),
      });
    }
    for (const t of filterTr) {
      if (String(t.accountId || "default") !== accId) {
        continue;
      }
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

/**
 * 滚账现金：全部账户=人民币汇总；单账户=该账户记账币（不再折人民币）。
 */
function computeLedgerCashBookUpToDate(tradeList, cashRows, accounts, accountId, fxUsdMap, fxHkdMap, asOfDateKey) {
  const scope = String(accountId || "all").trim() || "all";
  if (scope === "all") {
    return computeLedgerCashCnyUpToDate(tradeList, cashRows, accounts, accountId, fxUsdMap, fxHkdMap, asOfDateKey);
  }
  const accById = new Map((accounts || []).map((a) => [String(a.id), a]));
  const { filterTr, filterCtf } = filterLedgerRowsUpToDate(tradeList, cashRows, accountId, asOfDateKey);
  const { balNat } = ledgerCashBalanceNativeForAccount(filterTr, filterCtf, scope, accById, fxUsdMap, fxHkdMap);
  return balNat;
}

function principalBookUpToDate(cashRows, accounts, accountId, fxUsdMap, fxHkdMap, asOfDateKey) {
  const scope = String(accountId || "all").trim() || "all";
  const book = bookCurrencyForScope(accounts, scope);
  const end = String(asOfDateKey || "").slice(0, 10);
  const accById = new Map((accounts || []).map((a) => [String(a.id), a]));
  const rows = (Array.isArray(cashRows) ? cashRows : []).filter((r) => String(r.date || "").slice(0, 10) <= end);
  const filtered =
    scope === "all" ? rows : rows.filter((c) => String(c.accountId || "default") === scope);
  let sum = 0;
  for (const r of filtered) {
    const acc = accById.get(String(r.accountId || "default")) || { currency: "CNY" };
    const ccy = String(acc.currency || "CNY").toUpperCase();
    const sign = String(r.direction || "").toLowerCase() === "out" ? -1 : 1;
    const nat = sign * Math.abs(Number(r.amount) || 0);
    if (!Number.isFinite(nat) || nat === 0) {
      continue;
    }
    sum += nativeAmountToBook(nat, ccy, book, fxUsdMap, fxHkdMap, end);
  }
  return sum;
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

/**
 * 区间 (fromExclusive, toInclusive] 内银证出入金合计（CNY）。
 * 用于 live 单期 TWR 的「外部流入」：覆盖「上一个冻结日之后 → 今天」整段（含周末/节假日），
 * 避免空档期转入被误算成投资收益。外币按 toInclusive 日汇率换算，与 live 现金的计价基准一致。
 */
function externalFlowCnyForRange(cashRows, accounts, accountId, fxUsdMap, fxHkdMap, fromExclusive, toInclusive) {
  const from = String(fromExclusive || "").slice(0, 10);
  const to = String(toInclusive || "").slice(0, 10);
  if (!to) {
    return 0;
  }
  const accById = new Map((accounts || []).map((a) => [String(a.id), a]));
  const filtered =
    String(accountId || "all").trim() === "all"
      ? cashRows || []
      : (cashRows || []).filter((c) => String(c.accountId || "default") === String(accountId));
  let sum = 0;
  for (const r of filtered) {
    const dk = String(r.date || "").slice(0, 10);
    if (!dk || dk <= from || dk > to) {
      continue;
    }
    const acc = accById.get(String(r.accountId || "default")) || { currency: "CNY" };
    const ccy = String(acc.currency || "CNY").toUpperCase();
    const sign = String(r.direction || "").toLowerCase() === "out" ? -1 : 1;
    const nat = sign * Math.abs(Number(r.amount) || 0);
    if (!Number.isFinite(nat) || nat === 0) {
      continue;
    }
    sum += ccy === "CNY" ? nat : nat * fxToCnyOnDate(fxUsdMap, fxHkdMap, ccy, to);
  }
  return sum;
}

function externalFlowBookForRange(cashRows, accounts, accountId, fxUsdMap, fxHkdMap, fromExclusive, toInclusive) {
  const scope = String(accountId || "all").trim() || "all";
  const book = bookCurrencyForScope(accounts, scope);
  const from = String(fromExclusive || "").slice(0, 10);
  const to = String(toInclusive || "").slice(0, 10);
  if (!to) {
    return 0;
  }
  const accById = new Map((accounts || []).map((a) => [String(a.id), a]));
  const filtered =
    scope === "all" ? cashRows || [] : (cashRows || []).filter((c) => String(c.accountId || "default") === scope);
  let sum = 0;
  for (const r of filtered) {
    const dk = String(r.date || "").slice(0, 10);
    if (!dk || dk <= from || dk > to) {
      continue;
    }
    const acc = accById.get(String(r.accountId || "default")) || { currency: "CNY" };
    const ccy = String(acc.currency || "CNY").toUpperCase();
    const sign = String(r.direction || "").toLowerCase() === "out" ? -1 : 1;
    const nat = sign * Math.abs(Number(r.amount) || 0);
    if (!Number.isFinite(nat) || nat === 0) {
      continue;
    }
    sum += nativeAmountToBook(nat, ccy, book, fxUsdMap, fxHkdMap, to);
  }
  return sum;
}

module.exports = {
  bookCurrencyForScope,
  nativeAmountToBook,
  tradeCashFlowInAccountCurrency,
  computeLedgerCashCnyUpToDate,
  computeLedgerCashBookUpToDate,
  principalCnyUpToDate,
  principalBookUpToDate,
  externalFlowCnyForDate,
  externalFlowCnyForRange,
  externalFlowBookForRange,
  tradeSignedCashNativeForLedger,
};
