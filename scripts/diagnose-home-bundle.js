/**
 * Segment timing for GET /api/metrics/home-bundle internals.
 * Usage: node scripts/diagnose-home-bundle.js [userId] [accountScope]
 */
require("dotenv").config();
const {
  getSettings,
  getHomeSummaryForUser,
  getUserMetricsMeta,
  getTrades,
  getCashTransfers,
  getAccounts,
  getCliUserId,
  closeDatabase,
} = require("../src/db");
const { getComputeLiveMetrics, fetchTencentQuotePayloadMap, toTencentQuoteKey } = require("../src/market-realtime-pnl");
const { getMetricsHomeBundle } = require("../src/metrics-api-service");

async function time(label, fn) {
  const t0 = Date.now();
  const value = await fn();
  const ms = Date.now() - t0;
  return { label, ms, value };
}

async function collectSymbols(trades, scope) {
  const wanted = String(scope || "all").trim() || "all";
  const list =
    wanted === "all" ? trades : trades.filter((t) => String(t.accountId || "default") === wanted);
  const holdings = new Map();
  for (const trade of list.sort((a, b) => {
    const ad = new Date(a.date).getTime();
    const bd = new Date(b.date).getTime();
    if (ad !== bd) return ad - bd;
    return Number(a.createdAt || 0) - Number(b.createdAt || 0);
  })) {
    const sym = String(trade.symbol || "").trim().toUpperCase();
    if (!sym) continue;
    holdings.set(sym, (holdings.get(sym) || 0) + (trade.side === "buy" ? Number(trade.quantity || 0) : -Number(trade.quantity || 0)));
  }
  return [...holdings.entries()].filter(([, q]) => q > 1e-6).map(([s]) => s);
}

async function main() {
  const userId = process.argv[2] || (await getCliUserId());
  const scope = process.argv[3] || "acc_1778565482231_45";
  console.log("userId:", userId, "scope:", scope);

  const segments = [];

  segments.push(
    await time("db.parallel (settings+home+um+trades+cash+accounts)", async () => {
      const [settings, home, um, trades, cashTransfers, accounts] = await Promise.all([
        getSettings(userId),
        getHomeSummaryForUser(userId, scope),
        getUserMetricsMeta(userId),
        getTrades(userId),
        getCashTransfers(userId),
        getAccounts(userId),
      ]);
      return {
        symbolCount: home?.symbols?.length ?? 0,
        hasAccount: !!home?.account,
        tradeCount: trades.length,
        frozenThrough: home?.account?.frozen_through || home?.account?.frozenThrough,
      };
    }),
  );

  const trades = await getTrades(userId);
  const symbols = await collectSymbols(trades, scope);
  segments.push(
    await time(`quotes.tencent (${symbols.length} symbols + FX)`, async () => {
      const keys = [...symbols.map((s) => toTencentQuoteKey(s)).filter(Boolean), "whUSDCNY", "whHKDCNY"];
      const r = await fetchTencentQuotePayloadMap(keys);
      return { delayed: !!r.delayed, keys: keys.length, got: r.payloadMap?.size ?? 0 };
    }),
  );

  segments.push(
    await time("computeLiveMetrics (getComputeLiveMetrics)", async () => {
      const live = await getComputeLiveMetrics(userId, scope);
      return {
        tradingDay: live.tradingDay,
        delayed: live.delayed,
        positions: live.positions?.length ?? 0,
        todayProfitCny: live.todayProfitCny,
        totalAssetsCny: live.totalAssetsCny,
      };
    }),
  );

  segments.push(
    await time("getMetricsHomeBundle (full)", async () => {
      const b = await getMetricsHomeBundle(userId, scope, "today,ytd");
      const stages = b?.returns?.stages || {};
      return {
        today: stages.today?.profitCny,
        ytd: stages.ytd?.profitCny,
        holdingsRows: b?.holdings?.rows?.length ?? 0,
        totalAssets: b?.assets?.totalAssetsCny,
      };
    }),
  );

  console.log("\n--- segment ms ---");
  let sum = 0;
  for (const s of segments) {
    sum += s.ms;
    console.log(`${String(s.ms).padStart(6)} ms  ${s.label}`);
    console.log("       ", JSON.stringify(s.value));
  }
  console.log(`${String(sum).padStart(6)} ms  (sum of segments, not wall-clock parallel)`);
  await closeDatabase();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
