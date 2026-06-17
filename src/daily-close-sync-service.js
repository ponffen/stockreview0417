const { toDateKey } = require("../scripts/lib/market-fetch");
const {
  normalizeSymbol,
  addCalendarDays,
  listAllUserIds,
  getTrades,
  upsertSymbolDailyCloseBatch,
  getSymbolDailyCloseBounds,
} = require("./db");
const { fetchRemoteDailyClosesForSymbol } = require("./daily-close-backfill");

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
  let lastExitDate = null;
  let i = 0;
  while (i < sorted.length) {
    const day = sorted[i].date;
    const prevEodQty = eodQty;
    while (i < sorted.length && sorted[i].date === day) {
      const tr = sorted[i];
      eodQty += tr.side === "buy" ? tr.quantity : -tr.quantity;
      i += 1;
    }
    if (eodQty > POSITION_EPSILON) {
      everHeld = true;
    }
    if (prevEodQty > POSITION_EPSILON && eodQty <= POSITION_EPSILON) {
      lastExitDate = day;
    }
  }
  if (!everHeld) {
    return null;
  }
  const firstTradeDate = sorted[0].date;
  const lastTradeDate = sorted[sorted.length - 1].date;
  const currentlyHolding = eodQty > POSITION_EPSILON;
  const from = addCalendarDays(firstTradeDate, -1);
  if (currentlyHolding) {
    return {
      from,
      to: asOfDate,
      active: true,
      currentlyHolding: true,
      lastTradeDate,
      extensionUntil: null,
    };
  }
  const flatThrough = lastExitDate || lastTradeDate;
  return {
    from,
    to: flatThrough,
    active: false,
    currentlyHolding: false,
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
    let shouldSync = item.active;
    let reason = item.active ? "active" : "dormant-complete";
    if (!item.active) {
      const bounds = await getSymbolDailyCloseBounds(item.symbol, item.from, item.to);
      const covered = !!(
        bounds &&
        bounds.minDate &&
        bounds.maxDate &&
        bounds.minDate <= item.from &&
        bounds.maxDate >= item.to
      );
      shouldSync = !covered;
      reason = shouldSync ? "dormant-backfill" : "dormant-complete";
    }
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
  runDailyCloseSync,
};
