#!/usr/bin/env node
/**
 * 方案三：首笔入金作为期初本金（前一交易日），不与首日买入同日；
 * 为首日无 K 线的标的补 2026-01-01 收盘价（用当日买入均价），再全量重跑冻结。
 */
require("dotenv").config();
const path = require("node:path");
const {
  initPool,
  findUserByPhone,
  upsertSymbolDailyCloseBatch,
  backfillTradeAmountShareRatiosForUser,
  getTrades,
  closeDatabase,
} = require("../src/db");
const { runFreezeV3ForUser } = require(path.join(__dirname, "..", "src", "metrics", "freeze-v3"));
const { resolveFrozenDate } = require(path.join(__dirname, "..", "src", "eod-freeze-service"));
const { previousSessionDate } = require(path.join(__dirname, "..", "src/metrics/freeze-calendar"));
const { fetchRemoteDailyClosesForSymbol } = require(path.join(__dirname, "..", "src/daily-close-backfill"));

const PHONE = "18320260702";
const OPENING_NOTE = "期初本金（基于交易记录按当日汇总算出来的出入金。）";
const CASH_NOTE = "基于交易记录按当日汇总算出来的出入金。";

async function moveOpeningDeposit(pool, uid) {
  const firstTrade = await pool.query(
    `SELECT MIN(trade_date)::text d FROM trades WHERE user_id = $1 AND type = 'trade'`,
    [uid],
  );
  const firstDate = firstTrade.rows[0]?.d;
  if (!firstDate) throw new Error("no trades");

  const openingDate = previousSessionDate(firstDate);
  const { rows } = await pool.query(
    `SELECT id, transfer_date, direction, amount
     FROM cash_transfers
     WHERE user_id = $1 AND transfer_date = $2 AND direction = 'in'
     ORDER BY amount DESC LIMIT 1`,
    [uid, firstDate],
  );
  if (!rows[0]) throw new Error(`no opening inflow on ${firstDate}`);
  const nowMs = Date.now();
  await pool.query(
    `UPDATE cash_transfers
     SET transfer_date = $1, note = $2, updated_at = $3
     WHERE id = $4 AND user_id = $5`,
    [openingDate, OPENING_NOTE, nowMs, rows[0].id, uid],
  );
  console.log(`[opening] moved inflow ${rows[0].amount} ${firstDate} -> ${openingDate}`);
  return { openingDate, firstDate };
}

async function fillJan1Klines(pool, uid, firstDate) {
  const { rows: trades } = await pool.query(
    `SELECT symbol, price, quantity, amount
     FROM trades
     WHERE user_id = $1 AND trade_date = $2 AND type = 'trade' AND side = 'buy'`,
    [uid, firstDate],
  );
  const bySym = new Map();
  for (const t of trades) {
    const sym = String(t.symbol || "").trim();
    if (!sym) continue;
    const qty = Number(t.quantity) || 0;
    const amt = Number(t.amount) || 0;
    const px = Number(t.price) || (qty > 0 ? amt / qty : 0);
    if (!bySym.has(sym)) bySym.set(sym, { qty: 0, amt: 0 });
    const slot = bySym.get(sym);
    slot.qty += qty;
    slot.amt += amt;
  }

  const toWrite = [];
  for (const [symbol, slot] of bySym.entries()) {
    const existing = await pool.query(
      `SELECT close FROM symbol_daily_close WHERE symbol = $1 AND date = $2`,
      [symbol, firstDate],
    );
    if (existing.rows[0]?.close > 0) continue;

    const prior = await pool.query(
      `SELECT close, date FROM symbol_daily_close
       WHERE symbol = $1 AND date < $2 AND close > 0
       ORDER BY date DESC LIMIT 1`,
      [symbol, firstDate],
    );
    let close = prior.rows[0]?.close > 0 ? Number(prior.rows[0].close) : 0;
    let source = prior.rows[0] ? "sina-fill-prior" : "trade-open-estimate";

    if (!(close > 0) && slot.qty > 0 && slot.amt > 0) {
      close = slot.amt / slot.qty;
      source = "trade-open-estimate";
    }
    if (!(close > 0)) {
      const remote = await fetchRemoteDailyClosesForSymbol(symbol, firstDate, firstDate);
      close = remote[0]?.close > 0 ? remote[0].close : 0;
      source = "sina";
    }
    if (close > 0) {
      toWrite.push({ symbol, date: firstDate, close, source });
    } else {
      console.warn(`[kline] skip ${symbol} no price for ${firstDate}`);
    }
  }

  if (toWrite.length) {
    await upsertSymbolDailyCloseBatch(toWrite);
  }
  console.log(`[kline] filled ${toWrite.length} rows for ${firstDate}`);
}

async function main() {
  const user = await findUserByPhone(PHONE);
  if (!user?.id) throw new Error(`User not found: ${PHONE}`);
  const uid = user.id;
  const pool = await initPool();

  const { firstDate } = await moveOpeningDeposit(pool, uid);
  await fillJan1Klines(pool, uid, firstDate);

  const frozenDate = resolveFrozenDate();
  const trades = await getTrades(uid);
  const minD = trades.map((t) => t.date).sort()[0];
  const symbols = [...new Set(trades.map((t) => t.symbol).filter(Boolean))];
  for (const sym of symbols) {
    const rows = await fetchRemoteDailyClosesForSymbol(sym, minD, frozenDate);
    if (rows.length) {
      await upsertSymbolDailyCloseBatch(
        rows.map((r) => ({ symbol: sym, date: r.date, close: r.close, source: r.source || "sina" })),
      );
    }
  }
  console.log(`[kline] refreshed ${symbols.length} symbols ${minD}..${frozenDate}`);

  console.log("[freeze] full rebuild...");
  const freezeResult = await runFreezeV3ForUser(uid, {
    frozenDate,
    force: true,
    fullRebuild: true,
    syncDailyClose: true,
    logger: console,
  });
  console.log("[freeze] done", JSON.stringify(freezeResult, null, 2));

  const ratioResult = await backfillTradeAmountShareRatiosForUser(uid, { logger: console });
  console.log("[amountShareRatio]", ratioResult);

  const check = await pool.query(
    `SELECT date, total_assets, external_flow_cny, stage_inception_rate_twr, stage_inception_profit
     FROM analysis_daily_snapshot
     WHERE user_id = $1 AND account_id = 'all'
     ORDER BY date LIMIT 5`,
    [uid],
  );
  const last = await pool.query(
    `SELECT date, total_assets, stage_inception_rate_twr, stage_inception_profit, tw_r_cumulative
     FROM analysis_daily_snapshot
     WHERE user_id = $1 AND account_id = 'all'
     ORDER BY date DESC LIMIT 3`,
    [uid],
  );
  console.log("[check] first", check.rows);
  console.log("[check] last", last.rows);

  await closeDatabase?.();
}

main().catch(async (e) => {
  console.error(e);
  try {
    await closeDatabase?.();
  } catch {
    // ignore
  }
  process.exit(1);
});
