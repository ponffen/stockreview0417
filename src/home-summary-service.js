const crypto = require("node:crypto");
const {
  ensureHomeSummaryTables,
  getTrades,
  getCashTransfers,
  getAccounts,
  getLatestAnalysisSnapshotDate,
  getAnalysisDailySnapshots,
  getSymbolDailyPnl,
  deleteSymbolHomeSummaryForScope,
  upsertAccountHomeSummaryRow,
  upsertSymbolHomeSummaryBatch,
} = require("./db");
const {
  monthStartKeyShanghai,
  yearStartKeyShanghai,
  computeAccountHomeSummaryFromSnapshots,
  symbolRatesFromPnlPoints,
} = require("./home-summary-maths");

function sortTradeAsc(a, b) {
  const da = String(a.date || "").localeCompare(String(b.date || ""));
  if (da !== 0) return da;
  return Number(a.createdAt || a.created_at || 0) - Number(b.createdAt || b.created_at || 0);
}

function buildSourceVersion(trades, cash) {
  const t = [...(trades || [])]
    .map((tr) => `${String(tr.id || "").trim()}:${String(tr.date || "").slice(0, 10)}:${Number(tr.amount) || 0}`)
    .sort()
    .join("|");
  const c = [...(cash || [])]
    .map((r) => `${String(r.id || "").trim()}:${String(r.date || "").slice(0, 10)}:${Number(r.amount) || 0}`)
    .sort()
    .join("|");
  return crypto.createHash("sha256").update(`${t}::${c}`).digest("hex").slice(0, 32);
}

function firstTradeDateFromTrades(trades) {
  if (!trades.length) return null;
  return String([...trades].sort(sortTradeAsc)[0].date || "").slice(0, 10);
}

function firstTradeDateForSymbol(trades, sym) {
  const list = trades.filter((t) => String(t.symbol || "").toLowerCase() === sym).sort(sortTradeAsc);
  if (!list.length) return null;
  return String(list[0].date || "").slice(0, 10);
}

function tradesForScope(trades, accountScope) {
  const scope = String(accountScope || "all").trim() || "all";
  if (scope === "all") {
    return trades || [];
  }
  return (trades || []).filter((t) => String(t.accountId || "default") === scope);
}

/**
 * 重算单个 account_scope 的首页汇总（account_home_summary + symbol_home_summary）。
 */
async function rebuildHomeSummaryForScope(userId, accountScope, shared) {
  const uid = String(userId || "").trim();
  const scope = String(accountScope || "all").trim() || "all";
  const { trades, cash, todayShanghai, sourceVersion, globalFrozen } = shared;
  const scopedTrades = tradesForScope(trades, scope);

  let frozen = await getLatestAnalysisSnapshotDate(uid, scope);
  if (!frozen) {
    frozen = globalFrozen;
  }
  if (!frozen) {
    return { ok: false, skip: true, scope, reason: "no analysis snapshot" };
  }
  if (!scopedTrades.length && scope !== "all") {
    return { ok: false, skip: true, scope, reason: "no trades for scope" };
  }

  const firstTrade = firstTradeDateFromTrades(scopedTrades.length ? scopedTrades : trades);
  const analysisRows = await getAnalysisDailySnapshots({ accountId: scope, from: "1970-01-01", to: frozen }, uid);
  if (!analysisRows.length) {
    return { ok: false, skip: true, scope, reason: "no analysis rows" };
  }

  const acc = computeAccountHomeSummaryFromSnapshots(analysisRows, frozen, firstTrade, todayShanghai);
  const now = Date.now();
  const F = String(frozen).slice(0, 10);
  const ms = monthStartKeyShanghai(todayShanghai);
  const ys = yearStartKeyShanghai(todayShanghai);

  const symPnl = await getSymbolDailyPnl({ accountId: scope, from: firstTrade || F, to: frozen }, uid);
  const bySym = new Map();
  for (const r of symPnl) {
    const sym = String(r.symbol || "").toLowerCase().trim();
    if (!sym) continue;
    if (!bySym.has(sym)) bySym.set(sym, []);
    bySym.get(sym).push(r);
  }
  for (const arr of bySym.values()) {
    arr.sort((a, b) => String(a.date).localeCompare(String(b.date)));
  }

  const symKeys = new Set(bySym.keys());
  for (const tr of scopedTrades) {
    const s = String(tr.symbol || "").toLowerCase().trim();
    if (s) symKeys.add(s);
  }

  const symbolBatch = [];
  for (const sym of [...symKeys].sort()) {
    const arr = bySym.get(sym) || [];
    const symFirst = firstTradeDateForSymbol(scopedTrades.length ? scopedTrades : trades, sym) || firstTrade || F;
    let monthP = 0;
    let ytdP = 0;
    let totalP = 0;
    const pts = [];
    for (const r of arr) {
      const d = String(r.date || "").slice(0, 10);
      if (!d || d > F) continue;
      const pnl = Number(r.dayPnlNative ?? r.day_pnl_native) || 0;
      const px = Number(r.dayClosePrice ?? r.day_close_price);
      const sh = Number(r.eodShares ?? r.eod_shares) || 0;
      const flow = Number(r.dayTradeFlowNative ?? r.day_trade_flow_native) || 0;
      totalP += pnl;
      if (d >= ys) ytdP += pnl;
      if (d >= ms) monthP += pnl;
      if (Number.isFinite(px) && px > 0) {
        pts.push({ date: d, value: sh * px, flow });
      }
    }
    const rates = symbolRatesFromPnlPoints(pts);
    const ccy = String(arr[0]?.currency || "CNY").toUpperCase().slice(0, 3) || "CNY";
    symbolBatch.push({
      symbol: sym,
      frozenThrough: F,
      monthProfitNative: monthP,
      ytdProfitNative: ytdP,
      totalProfitNative: totalP,
      totalRateTwr: rates.rateTwr,
      totalRateMwr: rates.rateMwr,
      currency: ccy,
      firstTradeDate: symFirst,
      sourceVersion,
      computedAt: now,
    });
  }

  await deleteSymbolHomeSummaryForScope(uid, scope);
  if (symbolBatch.length) {
    await upsertSymbolHomeSummaryBatch(symbolBatch, uid, scope);
  }
  await upsertAccountHomeSummaryRow(
    {
      accountScope: scope,
      frozenThrough: F,
      firstTradeDate: firstTrade,
      lastMarketValueCny: acc.lastMarketValueCny,
      monthProfitCny: acc.monthProfitCny,
      monthRateTwr: acc.monthRateTwr,
      monthRateMwr: acc.monthRateMwr,
      ytdProfitCny: acc.ytdProfitCny,
      ytdRateTwr: acc.ytdRateTwr,
      ytdRateMwr: acc.ytdRateMwr,
      totalProfitCny: acc.totalProfitCny,
      totalRateTwr: acc.totalRateTwr,
      totalRateMwr: acc.totalRateMwr,
      sourceVersion,
      computedAt: now,
    },
    uid,
  );

  return {
    ok: true,
    scope,
    frozenThrough: F,
    symbolCount: symbolBatch.length,
  };
}

/**
 * 重算并写入全部 account_scope（all + 各子账户）的首页汇总。
 */
async function rebuildHomeSummaryForUser(userId) {
  await ensureHomeSummaryTables();
  const uid = String(userId || "").trim();
  if (!uid) {
    return { ok: false, error: "empty user" };
  }
  const globalFrozen = await getLatestAnalysisSnapshotDate(uid, "all");
  if (!globalFrozen) {
    return { ok: false, skip: true, reason: "no analysis snapshot" };
  }
  const trades = await getTrades(uid);
  if (!trades.length) {
    return { ok: false, skip: true, reason: "no trades" };
  }
  const cash = await getCashTransfers(uid);
  const accounts = await getAccounts(uid);
  const todayShanghai = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Shanghai" }).format(new Date());
  const sourceVersion = buildSourceVersion(trades, cash);
  const shared = { trades, cash, todayShanghai, sourceVersion, globalFrozen };

  const scopes = ["all", ...accounts.map((a) => String(a.id || "").trim()).filter(Boolean)];
  const scopeResults = [];
  let totalSymbols = 0;
  for (const scope of scopes) {
    const r = await rebuildHomeSummaryForScope(uid, scope, shared);
    scopeResults.push(r);
    if (r.ok) {
      totalSymbols += Number(r.symbolCount) || 0;
    }
  }

  const anyOk = scopeResults.some((r) => r.ok);
  if (!anyOk) {
    const first = scopeResults.find((r) => r.reason) || scopeResults[0];
    return { ok: false, skip: first?.skip, reason: first?.reason || "no scope rebuilt", scopeResults };
  }

  return {
    ok: true,
    userId: uid,
    frozenThrough: String(globalFrozen).slice(0, 10),
    symbolCount: totalSymbols,
    scopeCount: scopeResults.filter((r) => r.ok).length,
    scopeResults,
    sourceVersion,
  };
}

module.exports = {
  rebuildHomeSummaryForUser,
  rebuildHomeSummaryForScope,
  buildSourceVersion,
  tradesForScope,
};
