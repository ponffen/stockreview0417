#!/usr/bin/env node
/**
 * 生产 E2E：登录 higcc.com → 改台积电成交 → 校验 rebuilding / freeze 日志侧效应。
 */
require("dotenv").config();

const BASE = process.env.E2E_BASE_URL || "https://www.higcc.com";
const PHONE = process.env.E2E_PHONE || "18310270720";
const PASSWORD = process.env.E2E_PASSWORD || "123456";
const USER_ID = "d175359f-a856-478d-a45d-3112c10227fa";
const TRADE_ID = "49d49b61-e9cb-4283-8039-dce4ef9dbfc7";

function parseCookie(setCookieHeader) {
  if (!setCookieHeader) return "";
  const parts = Array.isArray(setCookieHeader) ? setCookieHeader : [setCookieHeader];
  return parts.map((p) => p.split(";")[0]).join("; ");
}

async function jsonFetch(url, opts = {}) {
  const res = await fetch(url, opts);
  const text = await res.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { raw: text.slice(0, 200) };
  }
  return { res, body };
}

async function readDbMeta() {
  const { initPool } = require("../src/db");
  const pool = await initPool();
  const r = await pool.query(
    "SELECT rebuilding, rebuild_from, data_version FROM user_metrics_meta WHERE user_id = $1",
    [USER_ID],
  );
  return r.rows[0];
}

async function readDbAmount() {
  const { initPool } = require("../src/db");
  const pool = await initPool();
  const r = await pool.query("SELECT amount FROM trades WHERE id = $1 AND user_id = $2", [TRADE_ID, USER_ID]);
  return r.rows[0]?.amount;
}

async function main() {
  const beforeMeta = await readDbMeta();
  const beforeAmount = await readDbAmount();
  console.log("[prod-e2e] db before:", { meta: beforeMeta, amount: beforeAmount });

  const login = await jsonFetch(`${BASE}/api/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ phone: PHONE, password: PASSWORD }),
  });
  if (!login.res.ok) {
    throw new Error(`login failed ${login.res.status} ${JSON.stringify(login.body)}`);
  }
  const cookie = parseCookie(login.res.headers.getSetCookie?.() || login.res.headers.get("set-cookie"));
  console.log("[prod-e2e] login ok userId=", login.body?.data?.id || login.body?.userId);

  const tradesGet = await jsonFetch(`${BASE}/api/trades`, { headers: { Cookie: cookie } });
  if (!tradesGet.res.ok) {
    throw new Error(`trades get failed ${tradesGet.res.status}`);
  }
  const trade = (tradesGet.body?.data || []).find((t) => t.id === TRADE_ID);
  if (!trade) {
    throw new Error("trade not found in api list");
  }
  console.log("[prod-e2e] current trade amount:", trade.amount);

  const nextAmount = Number(beforeAmount) === 2275.05 ? 2275.04 : 2275.05;
  const payload = {
    ...trade,
    amount: nextAmount,
  };
  const t0 = Date.now();
  const save = await jsonFetch(`${BASE}/api/trades`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Cookie: cookie },
    body: JSON.stringify(payload),
  });
  console.log("[prod-e2e] save status=", save.res.status, "wallMs=", Date.now() - t0, "body=", JSON.stringify(save.body).slice(0, 120));

  if (!save.res.ok) {
    throw new Error(`save failed ${save.res.status}`);
  }

  await new Promise((r) => setTimeout(r, 2000));
  const afterMeta = await readDbMeta();
  const afterAmount = await readDbAmount();
  console.log("[prod-e2e] db after:", { meta: afterMeta, amount: afterAmount });

  if (afterMeta?.rebuilding) {
    throw new Error("rebuilding still true 2s after save — freeze may not have completed");
  }
  if (!(Number(afterMeta?.data_version) > Number(beforeMeta?.data_version || 0))) {
    throw new Error("data_version did not increase");
  }
  if (Number(afterAmount) !== nextAmount) {
    throw new Error(`amount mismatch expected ${nextAmount} got ${afterAmount}`);
  }
  console.log("[prod-e2e] SUCCESS save+freeze verified on production");
}

main().catch((e) => {
  console.error("[prod-e2e] FAILED", e?.message || e);
  process.exit(1);
});
