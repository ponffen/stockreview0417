const { toDateKey } = require("../scripts/lib/market-fetch");
const {
  normalizeSymbol,
  addCalendarDays,
  listAllUserIds,
  getTrades,
  upsertSymbolDailyCloseBatch,
  getSymbolDailyCloseRange,
} = require("./db");
const { resolveFrozenDate } = require("./eod-freeze-service");
const {
  fetchRemoteDailyClosesForSymbol,
  diffMissingCloseDates,
  summarizeGapRanges,
} = require("./daily-close-backfill");
const {
  SOURCE_SINA,
  SOURCE_TENCENT,
  LEN_INCREMENTAL,
  pickBarOnDate,
  tencentQuoteMatchesFrozenDate,
  fetchSinaDailyKBatchMulti,
  fetchBackfillClosesForSymbol,
} = require("./daily-close-fetch");
const { fetchTencentQuoteMap } = require("./quotes/tencent-quote");

const POSITION_EPSILON = 1e-6;
const FX_DAILY_SYNC_SYMBOLS = ["fx_usdcny", "fx_hkdcny"];
const FX_DAILY_LOOKBACK_DAYS = 7;
const SINA_BATCH_CHUNK = 25;

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

function symbolsInPlanForFrozenDate(plan, frozenDate) {
  const fd = String(frozenDate || "").slice(0, 10);
  return (plan || []).filter((item) => item.from <= fd && item.to >= fd);
}

/**
 * 定时增量：冻结日单行；新浪 DailyK_Batch(len=2) + 腾讯 qt 兜底。
 */
async function runIncrementalDailyCloseSync(frozenDate, targets, logger) {
  const fd = String(frozenDate || "").slice(0, 10);
  const symbols = targets.map((t) => t.symbol);
  const sinaBarsBySymbol = new Map();

  for (let i = 0; i < symbols.length; i += SINA_BATCH_CHUNK) {
    const chunk = symbols.slice(i, i + SINA_BATCH_CHUNK);
    const batch = await fetchSinaDailyKBatchMulti(chunk, { len: LEN_INCREMENTAL });
    for (const [sym, bars] of batch.entries()) {
      sinaBarsBySymbol.set(sym, bars);
    }
  }

  const rowsToWrite = [];
  const perSymbol = [];
  const tencentCandidates = [];

  for (const item of targets) {
    const sym = item.symbol;
    const bars = sinaBarsBySymbol.get(sym) || [];
    const bar = pickBarOnDate(bars, fd);
    if (bar) {
      rowsToWrite.push({ symbol: sym, date: fd, close: bar.close, source: SOURCE_SINA });
      perSymbol.push({
        symbol: sym,
        synced: true,
        source: SOURCE_SINA,
        close: bar.close,
        frozenDate: fd,
      });
    } else {
      tencentCandidates.push({ item, bars });
    }
  }

  if (tencentCandidates.length) {
    const tencentRes = await fetchTencentQuoteMap(tencentCandidates.map((c) => c.item.symbol));
    for (const { item, bars } of tencentCandidates) {
      const sym = item.symbol;
      const quote = tencentRes.map?.get(sym);
      const current = Number(quote?.current);
      if (!quote || !(current > 0)) {
        perSymbol.push({
          symbol: sym,
          synced: false,
          reason: "no-sina-bar-and-no-tencent-quote",
          frozenDate: fd,
          sinaDays: bars.map((b) => b.date),
        });
        continue;
      }
      if (!tencentQuoteMatchesFrozenDate(quote, fd, sym)) {
        perSymbol.push({
          symbol: sym,
          synced: false,
          reason: "tencent-date-mismatch",
          frozenDate: fd,
          quoteTime: String(quote.time || ""),
          sinaDays: bars.map((b) => b.date),
        });
        continue;
      }
      rowsToWrite.push({ symbol: sym, date: fd, close: current, source: SOURCE_TENCENT });
      perSymbol.push({
        symbol: sym,
        synced: true,
        source: SOURCE_TENCENT,
        close: current,
        frozenDate: fd,
        quoteTime: String(quote.time || ""),
      });
    }
  }

  const rowsWritten = rowsToWrite.length ? await upsertSymbolDailyCloseBatch(rowsToWrite) : 0;
  const failedSymbols = perSymbol.filter((r) => !r.synced).length;
  if (failedSymbols) {
    logger.warn?.(
      `[daily-close-sync] incremental frozenDate=${fd} failed=${failedSymbols}`,
      perSymbol.filter((r) => !r.synced).slice(0, 5),
    );
  }
  return {
    mode: "incremental",
    frozenDate: fd,
    symbolsPlanned: targets.length,
    symbolsSynced: perSymbol.filter((r) => r.synced).length,
    symbolsFailed: failedSymbols,
    symbolsSkipped: 0,
    rowsFetched: rowsToWrite.length,
    rowsWritten,
    plan: perSymbol,
  };
}

/**
 * 多天回填：DailyK_Batch(len=5000, start/end)，按 bar 日期逐日写库。
 */
async function runBackfillDailyCloseSync(targets, logger) {
  const perSymbol = [];
  let rowsFetched = 0;
  let rowsWritten = 0;
  let failedSymbols = 0;

  for (const item of targets) {
    try {
      const rows = await fetchBackfillClosesForSymbol(item.symbol, item.from, item.to);
      rowsFetched += rows.length;
      const payload = rows.map((row) => ({
        symbol: item.symbol,
        date: row.date,
        close: row.close,
        source: row.source || SOURCE_SINA,
      }));
      const written = payload.length ? await upsertSymbolDailyCloseBatch(payload) : 0;
      rowsWritten += written;
      perSymbol.push({
        symbol: item.symbol,
        from: item.from,
        to: item.to,
        synced: true,
        fetched: rows.length,
        written,
      });
    } catch (error) {
      failedSymbols += 1;
      logger.warn?.(`[daily-close-sync] backfill failed symbol=${item.symbol}`, error?.message || error);
      perSymbol.push({
        symbol: item.symbol,
        from: item.from,
        to: item.to,
        synced: false,
        fetched: 0,
        written: 0,
        error: String(error?.message || error || "unknown error"),
      });
    }
  }

  return {
    mode: "backfill",
    symbolsPlanned: targets.length,
    symbolsSynced: perSymbol.filter((r) => r.synced).length,
    symbolsFailed: failedSymbols,
    symbolsSkipped: 0,
    rowsFetched,
    rowsWritten,
    plan: perSymbol,
  };
}

async function runDailyCloseSync(options = {}) {
  const logger = options.logger || console;
  const mode = String(options.mode || (options.backfill ? "backfill" : "incremental")).trim() || "incremental";
  const asOfDate = toDateKey(options.asOfDate || new Date());
  const frozenDate = options.frozenDate
    ? String(options.frozenDate).slice(0, 10)
    : resolveFrozenDate(options.asOfDate || new Date());
  const symbolFilter = new Set(
    (Array.isArray(options.symbols) ? options.symbols : [])
      .map((s) => normalizeSymbol(String(s || "")))
      .filter(Boolean),
  );
  const plan = await buildGlobalDailyClosePlan(asOfDate);
  const targets = symbolFilter.size ? plan.filter((item) => symbolFilter.has(item.symbol)) : plan;

  if (mode === "backfill") {
    const result = await runBackfillDailyCloseSync(targets, logger);
    return { ok: true, asOfDate, frozenDate, ...result };
  }

  const incrementalTargets = symbolsInPlanForFrozenDate(targets, frozenDate);
  const result = await runIncrementalDailyCloseSync(frozenDate, incrementalTargets, logger);
  return { ok: true, asOfDate, ...result };
}

module.exports = {
  buildGlobalDailyClosePlan,
  auditDailyCloseGapsForSymbol,
  auditDailyCloseGapsForPlan,
  runDailyCloseSync,
  runIncrementalDailyCloseSync,
  runBackfillDailyCloseSync,
  symbolsInPlanForFrozenDate,
};
