const crypto = require("node:crypto");
const {
  ensureHomeSummaryTables,
  getTrades,
  getCashTransfers,
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

/**
 * 重算并写入 account_scope=all 的首页汇总（截止到 analysis 日序列最大日期）。
 */
async function rebuildHomeSummaryForUser(userId) {
  await ensureHomeSummaryTables();
  const uid = String(userId || "").trim();
  if (!uid) {
    return { ok: false, error: "empty user" };
  }
  const frozen = await getLatestAnalysisSnapshotDate(uid, "all");
  if (!frozen) {
    return { ok: false, skip: true, reason: "no analysis snapshot" };
  }
  const trades = await getTrades(uid);
  if (!trades.length) {
    return { ok: false, skip: true, reason: "no trades" };
  }
  const cash = await getCashTransfers(uid);
  const firstTrade = firstTradeDateFromTrades(trades);
  const analysisRows = await getAnalysisDailySnapshots({ accountId: "all", from: "1970-01-01", to: frozen }, uid);
  if (!analysisRows.length) {
    return { ok: false, skip: true, reason: "no analysis rows" };
  }
  const acc = computeAccountHomeSummaryFromSnapshots(analysisRows, frozen, firstTrade);
  const sourceVersion = buildSourceVersion(trades, cash);
  const now = Date.now();
  const F = String(frozen).slice(0, 10);
  const ms = monthStartKeyShanghai(F);
  const ys = yearStartKeyShanghai(F);

  const allSymPnl = await getSymbolDailyPnl({ accountId: "all", from: firstTrade || F, to: frozen }, uid);
  const bySym = new Map();
  for (const r of allSymPnl) {
    const sym = String(r.symbol || "").toLowerCase().trim();
    if (!sym) continue;
    if (!bySym.has(sym)) bySym.set(sym, []);
    bySym.get(sym).push(r);
  }
  for (const arr of bySym.values()) {
    arr.sort((a, b) => String(a.date).localeCompare(String(b.date)));
  }

  const symKeys = new Set(bySym.keys());
  for (const tr of trades) {
    const s = String(tr.symbol || "").toLowerCase().trim();
    if (s) symKeys.add(s);
  }

  const symbolBatch = [];
  for (const sym of [...symKeys].sort()) {
    const arr = bySym.get(sym) || [];
    const symFirst = firstTradeDateForSymbol(trades, sym) || firstTrade || F;
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

  await deleteSymbolHomeSummaryForScope(uid, "all");
  if (symbolBatch.length) {
    await upsertSymbolHomeSummaryBatch(symbolBatch, uid, "all");
  }
  await upsertAccountHomeSummaryRow(
    {
      accountScope: "all",
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
    uid
  );

  return {
    ok: true,
    userId: uid,
    frozenThrough: F,
    symbolCount: symbolBatch.length,
    sourceVersion,
  };
}

module.exports = {
  rebuildHomeSummaryForUser,
  buildSourceVersion,
};
