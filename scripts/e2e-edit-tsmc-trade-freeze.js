#!/usr/bin/env node
/**
 * E2E：修改台积电成交并等待 freeze 完成，验证 rebuilding 清除。
 * 用法: node scripts/e2e-edit-tsmc-trade-freeze.js
 */
require("dotenv").config();

const USER_ID = "d175359f-a856-478d-a45d-3112c10227fa";
const TRADE_ID = "49d49b61-e9cb-4283-8039-dce4ef9dbfc7";
const TARGET_AMOUNT = 2275.05;

async function readMeta(pool) {
  const r = await pool.query(
    "SELECT rebuilding, rebuild_from, frozen_through, data_version FROM user_metrics_meta WHERE user_id = $1",
    [USER_ID],
  );
  return r.rows[0] || null;
}

async function runLocalPipeline() {
  const { initPool, getTradeByIdForUser, upsertTrade } = require("../src/db");
  const { hintDatesFromTradeMutation } = require("../src/metrics-invalidate");
  const { prepareLedgerMetricsFreeze, dispatchFreezeEodJobAsync } = require("../src/metrics-rebuild-trigger");

  const pool = await initPool();
  const before = await readMeta(pool);
  console.log("[e2e] meta before:", before);

  const prior = await getTradeByIdForUser(TRADE_ID, USER_ID);
  if (!prior) {
    throw new Error("trade not found");
  }
  console.log("[e2e] prior trade:", {
    id: prior.id,
    date: prior.date,
    symbol: prior.symbol,
    amount: prior.amount,
  });

  const next = {
    ...prior,
    amount: TARGET_AMOUNT,
  };
  const saved = await upsertTrade(next, USER_ID);
  console.log("[e2e] saved trade amount:", saved.amount);

  const hintDates = hintDatesFromTradeMutation(prior, saved);
  console.log("[e2e] hintDates:", hintDates);

  const prepared = await prepareLedgerMetricsFreeze(USER_ID, { hintDates, fullRebuild: false });
  console.log("[e2e] prepared:", prepared);

  if (!prepared.payload) {
    throw new Error(`freeze not prepared: ${prepared.reason || "unknown"}`);
  }

  const marked = await readMeta(pool);
  console.log("[e2e] meta after mark:", marked);
  if (!marked?.rebuilding) {
    throw new Error("rebuilding not set after prepare");
  }

  await dispatchFreezeEodJobAsync(prepared.payload);

  const after = await readMeta(pool);
  console.log("[e2e] meta after freeze:", after);

  if (after?.rebuilding) {
    throw new Error("rebuilding still true after freeze");
  }
  if (!(Number(after?.data_version) > Number(before?.data_version || 0))) {
    throw new Error("data_version did not increase");
  }

  const amt = await pool.query("SELECT amount FROM trades WHERE id = $1 AND user_id = $2", [TRADE_ID, USER_ID]);
  console.log("[e2e] final amount in db:", amt.rows[0]?.amount);
  console.log("[e2e] SUCCESS");
}

runLocalPipeline().catch((e) => {
  console.error("[e2e] FAILED", e?.message || e);
  process.exit(1);
});
