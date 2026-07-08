const { toDateKey } = require("../scripts/lib/market-fetch");
const {
  normalizeSymbol,
  addCalendarDays,
  listAllUserIds,
  getTrades,
  upsertSymbolDailyCloseBatch,
  getSymbolDailyCloseBounds,
  getSymbolDailyCloseRange,
} = require("./db");
const {
  fetchRemoteDailyClosesForSymbol,
  diffMissingCloseDates,
  summarizeGapRanges,
} = require("./daily-close-backfill");

const POSITION_EPSILON = 1e-6;
const FX_DAILY_SYNC_SYMBOLS = ["fx_usdcny", "fx_hkdcny"];
const BENCHMARK_DAILY_SYMBOLS = ["sh000001", "sz399001", "rt_hkHSI", "gb_inx"];
const FX_DAILY_LOOKBACK_DAYS = 7;
const BENCHMARK_LOOKBACK_DAYS = 400;

function normalizeLifecycleTrade(raw) {
  const symbol = normalizeSymbol(raw?.symbol);
  const side = String(raw?.side || "").trim().toLowerCase();
  const quantity = Number(raw?.quantity);
  const date = String(raw?.date || "").slice(0, 10);
  const createdAt = Number(raw?.createdAt) || 0;
  if (!symbol || !date) {
    return null;
  }
  if (side !== "buy" && side !== "sell") {
    return null;
  }
  if (!Number.isFinite(quantity) || quantity <= 0) {
    return null;
  }
  return { symbol, side, quantity, date, createdAt };
}

function sortTradeAsc(a, b) {
  if (a.date !== b.date) {
    return String(a.date).localeCompare(String(b.date));
  }
  return Number(a.createdAt || 0) - Number(b.createdAt || 0);
}

function buildSymbolLifecycleForUser(trades, asOfDate) {
  const sorted = (Array.isArray(trades) ? trades : []).slice().sort(sortTradeAsc);
  if (!sorted.length) {
    return null;
  }
  let eodQty = 0;
  let everHeld = false;
  let i = 0;
  while (i < sorted.length) {
    const day = sorted[i].date;
    while (i < sorted.length && sorted[i].date === day) {
      const tr = sorted[i];
      eodQty += tr.side === "buy" ? tr.quantity : -tr.quantity;
      i += 1;
    }
    if (eodQty > POSITION_EPSILON) {
      everHeld = true;
    }
  }
  if (!everHeld) {
    return null;
  }
  const firstTradeDate = sorted[0].date;
  const lastTradeDate = sorted[sorted.length - 1].date;
  const currentlyHolding = eodQty > POSITION_EPSILON;
  const from = addCalendarDays(firstTradeDate, -1);
  // 曾持仓标的：日 K 一旦纳入同步就持续拉到 asOfDate，清仓后也不停。
  return {
    from,
    to: asOfDate,
    active: true,
    currentlyHolding,
    lastTradeDate,
    extensionUntil: null,
  };
}

async function buildGlobalDailyClosePlan(asOfDate) {
  const dateKey = String(asOfDate || "").slice(0, 10) || toDateKey(new Date());
  const userIds = await listAllUserIds();
  const bySymbol = new Map();
  for (const userId of userIds) {
    const normalizedTrades = (await getTrades(userId)).map(normalizeLifecycleTrade).filter(Boolean);
    if (!normalizedTrades.length) {
      continue;
    }
    const byUserSymbol = new Map();
    for (const tr of normalizedTrades) {
      if (!byUserSymbol.has(tr.symbol)) {
        byUserSymbol.set(tr.symbol, []);
      }
      byUserSymbol.get(tr.symbol).push(tr);
    }
    for (const [symbol, trades] of byUserSymbol.entries()) {
      const lifecycle = buildSymbolLifecycleForUser(trades, dateKey);
      if (!lifecycle || !lifecycle.from || !lifecycle.to || lifecycle.from > lifecycle.to) {
        continue;
      }
      const prev = bySymbol.get(symbol);
      if (!prev) {
        bySymbol.set(symbol, {
          symbol,
          from: lifecycle.from,
          to: lifecycle.to,
          active: lifecycle.active,
          userCount: 1,
          currentHolderUsers: lifecycle.currentlyHolding ? 1 : 0,
          latestTradeDate: lifecycle.lastTradeDate,
        });
        continue;
      }
      prev.from = prev.from < lifecycle.from ? prev.from : lifecycle.from;
      prev.to = prev.to > lifecycle.to ? prev.to : lifecycle.to;
      prev.active = prev.active || lifecycle.active;
      prev.userCount += 1;
      if (lifecycle.currentlyHolding) {
        prev.currentHolderUsers += 1;
      }
      if (!prev.latestTradeDate || lifecycle.lastTradeDate > prev.latestTradeDate) {
        prev.latestTradeDate = lifecycle.lastTradeDate;
      }
    }
  }
  const fxFrom = addCalendarDays(dateKey, -FX_DAILY_LOOKBACK_DAYS);
  for (const fxSymbol of FX_DAILY_SYNC_SYMBOLS) {
    const prev = bySymbol.get(fxSymbol);
    if (!prev) {
      bySymbol.set(fxSymbol, {
        symbol: fxSymbol,
        from: fxFrom,
        to: dateKey,
        active: true,
        userCount: 0,
        currentHolderUsers: 0,
        latestTradeDate: null,
      });
      continue;
    }
    // 即使该 symbol 已由交易生命周期计划覆盖，也强制把范围延伸到 asOfDate，确保每日外汇更新。
    prev.from = prev.from < fxFrom ? prev.from : fxFrom;
    prev.to = prev.to > dateKey ? prev.to : dateKey;
    prev.active = true;
  }
  return [...bySymbol.values()].sort((a, b) => a.symbol.localeCompare(b.symbol));
}

async function auditDailyCloseGapsForSymbol(symbol, fromDate, toDate) {
  const sym = normalizeSymbol(symbol);
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  if (!sym || !from || !to || from > to) {
    return {
      symbol: sym,
      from,
      to,
      localCount: 0,
      remoteCount: 0,
      missingCount: 0,
      missingRanges: [],
      error: "invalid range",
    };
  }
  const local = await getSymbolDailyCloseRange(sym, from, to);
  let remote = [];
  let error = "";
  try {
    remote = await fetchRemoteDailyClosesForSymbol(sym, from, to);
  } catch (e) {
    error = String(e?.message || e || "fetch failed");
  }
  const missingDates = diffMissingCloseDates(local, remote, from, to);
  return {
    symbol: sym,
    from,
    to,
    localCount: local.length,
    remoteCount: remote.length,
    missingCount: missingDates.length,
    missingRanges: summarizeGapRanges(missingDates),
    missingSample: missingDates.slice(0, 5),
    error,
  };
}

async function auditDailyCloseGapsForPlan(plan, options = {}) {
  const list = Array.isArray(plan) ? plan : [];
  const onlyMissing = options.onlyMissing === true;
  const symbolFilter = new Set(
    (Array.isArray(options.symbols) ? options.symbols : [])
      .map((s) => normalizeSymbol(String(s || "")))
      .filter(Boolean),
  );
  const targets = symbolFilter.size ? list.filter((item) => symbolFilter.has(item.symbol)) : list;
  const audits = [];
  for (const item of targets) {
    const audit = await auditDailyCloseGapsForSymbol(item.symbol, item.from, item.to);
    if (!onlyMissing || audit.missingCount > 0 || audit.error) {
      audits.push(audit);
    }
  }
  audits.sort((a, b) => (b.missingCount || 0) - (a.missingCount || 0));
  return audits;
}

async function symbolDailyCloseNeedsSync(item) {
  if (item.active) {
    const local = await getSymbolDailyCloseRange(item.symbol, item.from, item.to);
    let remote = [];
    try {
      remote = await fetchRemoteDailyClosesForSymbol(item.symbol, item.from, item.to);
    } catch {
      return { shouldSync: true, reason: "active-fetch-failed" };
    }
    const missing = diffMissingCloseDates(local, remote, item.from, item.to);
    if (missing.length > 0) {
      return { shouldSync: true, reason: "active-missing", missingCount: missing.length };
    }
    return { shouldSync: false, reason: "active-complete" };
  }
  const bounds = await getSymbolDailyCloseBounds(item.symbol, item.from, item.to);
  const edgeCovered = !!(
    bounds &&
    bounds.minDate &&
    bounds.maxDate &&
    bounds.minDate <= item.from &&
    bounds.maxDate >= item.to
  );
  if (!edgeCovered) {
    return { shouldSync: true, reason: "dormant-backfill" };
  }
  const local = await getSymbolDailyCloseRange(item.symbol, item.from, item.to);
  let remote = [];
  try {
    remote = await fetchRemoteDailyClosesForSymbol(item.symbol, item.from, item.to);
  } catch {
    return { shouldSync: true, reason: "dormant-fetch-failed" };
  }
  const missing = diffMissingCloseDates(local, remote, item.from, item.to);
  if (missing.length > 0) {
    return { shouldSync: true, reason: "dormant-gap", missingCount: missing.length };
  }
  return { shouldSync: false, reason: "dormant-complete" };
}

async function runDailyCloseSync(options = {}) {
  const logger = options.logger || console;
  const asOfDate = toDateKey(options.asOfDate || new Date());
  const symbolFilter = new Set(
    (Array.isArray(options.symbols) ? options.symbols : [])
      .map((s) => normalizeSymbol(String(s || "")))
      .filter(Boolean)
  );
  const plan = await buildGlobalDailyClosePlan(asOfDate);
  const targets = symbolFilter.size ? plan.filter((item) => symbolFilter.has(item.symbol)) : plan;
  const perSymbol = [];
  let rowsFetched = 0;
  let rowsWritten = 0;
  let failedSymbols = 0;
  let skippedSymbols = 0;
  for (const item of targets) {
    const syncDecision = await symbolDailyCloseNeedsSync(item);
    const shouldSync = syncDecision.shouldSync;
    const reason = syncDecision.reason;
    if (!shouldSync) {
      skippedSymbols += 1;
      perSymbol.push({
        symbol: item.symbol,
        from: item.from,
        to: item.to,
        active: item.active,
        synced: false,
        reason,
        fetched: 0,
        written: 0,
      });
      continue;
    }
    try {
      const rows = await fetchRemoteDailyClosesForSymbol(item.symbol, item.from, item.to);
      rowsFetched += rows.length;
      const payload = rows.map((row) => ({
        symbol: item.symbol,
        date: String(row.date || "").slice(0, 10),
        close: Number(row.close),
        source: String(row.source || "sina"),
      }));
      const written = payload.length ? await upsertSymbolDailyCloseBatch(payload) : 0;
      rowsWritten += written;
      perSymbol.push({
        symbol: item.symbol,
        from: item.from,
        to: item.to,
        active: item.active,
        synced: true,
        reason,
        fetched: rows.length,
        written,
      });
    } catch (error) {
      failedSymbols += 1;
      logger.warn?.(`[daily-close-sync] failed symbol=${item.symbol}`, error?.message || error);
      perSymbol.push({
        symbol: item.symbol,
        from: item.from,
        to: item.to,
        active: item.active,
        synced: false,
        reason: "failed",
        fetched: 0,
        written: 0,
        error: String(error?.message || error || "unknown error"),
      });
    }
  }
  return {
    ok: true,
    asOfDate,
    symbolsPlanned: targets.length,
    symbolsSynced: perSymbol.filter((item) => item.synced).length,
    symbolsSkipped: skippedSymbols,
    symbolsFailed: failedSymbols,
    rowsFetched,
    rowsWritten,
    plan: perSymbol,
  };
}

module.exports = {
  buildGlobalDailyClosePlan,
  auditDailyCloseGapsForSymbol,
  auditDailyCloseGapsForPlan,
  runDailyCloseSync,
};
