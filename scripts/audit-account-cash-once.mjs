/**
 * One-off: load trades + cash_transfers for an account and print ledger cash / principal
 * matching app.js computeLedgerCashAndPrincipal (FX from symbol_daily_close + snapshot; no hardcoded fallback).
 */
import pg from "pg";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
require("dotenv").config();

const { initPool } = require("../src/db");
const {
  loadFxCloseMapsFromDb,
  backwardThenForwardFillFxMap,
  resolveFxRatesCny,
} = require("../src/metrics/fx-maps");

const { Client } = pg;

function inferMarket(symbol) {
  const s = String(symbol || "");
  if (s.startsWith("sh") || s.startsWith("sz")) return "A股";
  if (s.startsWith("hk") || s.startsWith("rt_hk")) return "港股";
  if (s.startsWith("gb_") || /^[a-z]/i.test(s)) return "美股";
  return "其他";
}
function getSymbolCurrency(symbol) {
  const m = inferMarket(symbol);
  if (m === "港股") return "HKD";
  if (m === "美股") return "USD";
  return "CNY";
}
function signedAmount(trade) {
  return trade.side === "buy" ? trade.amount : -trade.amount;
}
function tradeSignedCashNativeForLedger(trade) {
  const ty = String(trade.type || "trade");
  if (ty === "dividend") return Math.abs(Number(trade.amount) || 0);
  if (ty === "bonus" || ty === "split" || ty === "merge") return 0;
  return -signedAmount(trade);
}

function buildFxMapFromSnapshots(rows) {
  const map = {};
  for (const row of rows) {
    const dk = String(row.date || "").slice(0, 10);
    if (!dk) continue;
    const usd = row.fx_usd_cny != null ? Number(row.fx_usd_cny) : NaN;
    const hkd = row.fx_hkd_cny != null ? Number(row.fx_hkd_cny) : NaN;
    const slot = { ...(map[dk] || {}) };
    if (Number.isFinite(usd) && usd > 0) slot.USD = usd;
    if (Number.isFinite(hkd) && hkd > 0) slot.HKD = hkd;
    if (slot.USD || slot.HKD) map[dk] = slot;
  }
  return map;
}

async function loadFxByDateFromDb(dateKeys) {
  const sorted = [...new Set((dateKeys || []).map((d) => String(d || "").slice(0, 10)).filter(Boolean))].sort();
  if (!sorted.length) {
    return {};
  }
  const { fxUsdMap, fxHkdMap } = await loadFxCloseMapsFromDb(sorted[0], sorted[sorted.length - 1]);
  backwardThenForwardFillFxMap(fxUsdMap, sorted);
  backwardThenForwardFillFxMap(fxHkdMap, sorted);
  const map = {};
  for (const dk of sorted) {
    const usd = Number(fxUsdMap[dk]) || 0;
    const hkd = Number(fxHkdMap[dk]) || 0;
    if (usd > 0 || hkd > 0) {
      map[dk] = {};
      if (usd > 0) map[dk].USD = usd;
      if (hkd > 0) map[dk].HKD = hkd;
    }
  }
  return map;
}

function mergeFxMaps(primary, supplemental) {
  const out = { ...primary };
  for (const [dk, slot] of Object.entries(supplemental || {})) {
    const merged = { ...(out[dk] || {}) };
    for (const ccy of ["USD", "HKD"]) {
      const hit = Number(slot?.[ccy]);
      if (!(Number(merged[ccy]) > 0) && Number.isFinite(hit) && hit > 0) {
        merged[ccy] = hit;
      }
    }
    if (merged.USD || merged.HKD) {
      out[dk] = merged;
    }
  }
  return out;
}

function getFxRateToCny(currency, fxSpot) {
  if (currency === "CNY") return 1;
  const s = fxSpot?.[currency];
  if (Number.isFinite(s) && s > 0) return s;
  return 0;
}

function getFxRateForDate(currency, dateKey, fxByDate, fxSpot) {
  if (currency === "CNY") return 1;
  const dk = String(dateKey || "").slice(0, 10);
  const today = new Date().toISOString().slice(0, 10);
  if (!dk || dk >= today) return getFxRateToCny(currency, fxSpot);
  const keys = Object.keys(fxByDate).sort();
  if (!keys.length) return getFxRateToCny(currency, fxSpot);
  for (let i = keys.length - 1; i >= 0; i -= 1) {
    if (keys[i] <= dk) {
      const hit = Number(fxByDate[keys[i]]?.[currency]);
      if (Number.isFinite(hit) && hit > 0) return hit;
    }
  }
  for (const key of keys) {
    const hit = Number(fxByDate[key]?.[currency]);
    if (Number.isFinite(hit) && hit > 0) return hit;
  }
  return getFxRateToCny(currency, fxSpot);
}

function cashTransferSignedNativeAmount(r) {
  const sign = r.direction === "out" ? -1 : 1;
  const nat = sign * Math.abs(Number(r.amount) || 0);
  return Number.isFinite(nat) ? nat : 0;
}

/** 按估值日 asOf 的汇率折人民币（不按发生日）。 */
function cashTransferRowNetCnyAsOf(r, asOfDateKey, accById, fxByDate, fxSpot) {
  const acc = accById.get(String(r.accountId)) || { currency: "CNY" };
  const ccy = String(acc.currency || "CNY").toUpperCase();
  const nat = cashTransferSignedNativeAmount(r);
  if (nat === 0) return 0;
  const asOf = String(asOfDateKey || "").slice(0, 10);
  return ccy === "CNY" ? nat : nat * getFxRateForDate(ccy, asOf, fxByDate, fxSpot);
}

function cashTransferDeltaNative(r, accById) {
  const acc = accById.get(String(r.accountId)) || { currency: "CNY" };
  const ccy = String(acc.currency || "CNY").toUpperCase();
  const sign = String(r.direction || "").toLowerCase() === "out" ? -1 : 1;
  return sign * Math.abs(Number(r.amount) || 0);
}

function tradeCashFlowInAccountCurrency(trade, accountCurrency, fxByDate, fxSpot) {
  const acc = String(accountCurrency || "CNY").toUpperCase();
  const dateKey = String(trade.date || "").slice(0, 10);
  const symCcy = String(getSymbolCurrency(trade.symbol) || "CNY").toUpperCase();
  const signedNat = tradeSignedCashNativeForLedger(trade);
  if (!Number.isFinite(signedNat) || signedNat === 0) return 0;
  const flowCny =
    symCcy === "CNY" ? signedNat : signedNat * getFxRateForDate(symCcy, dateKey, fxByDate, fxSpot);
  if (acc === "CNY") return flowCny;
  const fxAcc = getFxRateForDate(acc, dateKey, fxByDate, fxSpot);
  return Number.isFinite(fxAcc) && fxAcc > 0 ? flowCny / fxAcc : flowCny;
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

function computeLedgerCashAndPrincipal(tradeList, ctf, accById, fxByDate, fxSpot) {
  const rowsCtf = Array.isArray(ctf) ? ctf : [];
  const rowsTr = Array.isArray(tradeList) ? tradeList : [];

  const accountIds = new Set();
  for (const r of rowsCtf) accountIds.add(String(r.accountId || "default"));
  for (const t of rowsTr) accountIds.add(String(t.accountId || "default"));

  let cashCny = 0;
  for (const accId of accountIds) {
    const acc = accById.get(accId) || { currency: "CNY" };
    const accCcy = String(acc.currency || "CNY").toUpperCase();
    const events = [];
    for (const r of rowsCtf) {
      if (String(r.accountId || "default") !== accId) continue;
      events.push({
        kind: "ct",
        id: String(r.id || ""),
        date: r.date,
        createdAt: r.createdAt,
        delta: cashTransferDeltaNative(r, accById),
      });
    }
    for (const t of rowsTr) {
      if (String(t.accountId || "default") !== accId) continue;
      events.push({
        kind: "tr",
        id: String(t.id || ""),
        date: t.date,
        createdAt: t.createdAt,
        delta: tradeCashFlowInAccountCurrency(t, accCcy, fxByDate, fxSpot),
      });
    }
    events.sort(compareLedgerEvent);
    let balNat = 0;
    for (const ev of events) balNat += ev.delta;
    if (accCcy === "CNY") cashCny += balNat;
    else {
      const fx = getFxRateToCny(accCcy, fxSpot);
      cashCny += balNat * (Number.isFinite(fx) && fx > 0 ? fx : 1);
    }
  }
  return { cashCny };
}

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error("Set DATABASE_URL");
    process.exit(1);
  }
  const arg1 = process.argv[2] || "";
  const arg2 = process.argv[3] || "";
  const client = new Client({ connectionString: url, ssl: { rejectUnauthorized: false } });
  await client.connect();
  await initPool();

  let uid;
  let aid;
  let name;
  let currency;
  let otherMatches = [];
  if (arg1.includes("-") && arg1.length > 20 && arg2) {
    uid = arg1;
    const { rows: one } = await client.query(
      `SELECT user_id, id, name, currency FROM accounts WHERE user_id = $1 AND id = $2`,
      [uid, arg2],
    );
    if (!one.length) {
      console.log("No account for user_id + id", uid, arg2);
      await client.end();
      return;
    }
    ({ user_id: uid, id: aid, name, currency } = one[0]);
  } else {
    const needle = arg1 || "18310270720";
    const digits = String(needle).replace(/\D/g, "");
    const { rows: accRows } = await client.query(
      `SELECT a.user_id, a.id, a.name, a.currency
       FROM accounts a
       JOIN users u ON u.id = a.user_id
       WHERE ($3 <> '' AND u.phone LIKE $1) OR a.name ILIKE $2 OR a.id ILIKE $2
       ORDER BY a.user_id, a.id`,
      [`%${digits}%`, `%${needle}%`, digits],
    );
    if (!accRows.length) {
      console.log("No account match for", needle);
      await client.end();
      return;
    }
    ({ user_id: uid, id: aid, name, currency } = accRows[0]);
    otherMatches = accRows.slice(1);
    if (otherMatches.length) {
      console.log("Multiple matches, using first. Others:", otherMatches);
    }
  }
  console.log("Using account:", { uid, aid, name, currency });

  const { rows: allAcc } = await client.query(
    `SELECT id, name, currency, created_at FROM accounts WHERE user_id = $1 ORDER BY created_at`,
    [uid],
  );
  const accById = new Map(allAcc.map((a) => [String(a.id), { id: a.id, name: a.name, currency: a.currency }]));

  const { rows: snapRows } = await client.query(
    `SELECT date, fx_usd_cny, fx_hkd_cny FROM analysis_daily_snapshot
     WHERE user_id = $1 AND (account_id = $2 OR account_id = 'all')
     ORDER BY date ASC`,
    [uid, aid],
  );

  const { rows: trades } = await client.query(
    `SELECT id, account_id, type, symbol, name, side, price, quantity, amount, trade_date AS date, note, created_at
     FROM trades WHERE user_id = $1 AND account_id = $2 ORDER BY trade_date, created_at, id`,
    [uid, aid],
  );
  const { rows: cash } = await client.query(
    `SELECT id, account_id, transfer_date AS date, direction, amount, note, created_at
     FROM cash_transfers WHERE user_id = $1 AND account_id = $2 ORDER BY transfer_date, created_at, id`,
    [uid, aid],
  );

  const tradeList = trades.map((t) => ({
    id: t.id,
    accountId: t.account_id,
    type: t.type,
    symbol: t.symbol,
    name: t.name,
    side: t.side,
    price: Number(t.price),
    quantity: Number(t.quantity),
    amount: Number(t.amount),
    date: String(t.date).slice(0, 10),
    note: t.note,
    createdAt: Number(t.created_at),
  }));
  const ctf = cash.map((r) => ({
    id: r.id,
    accountId: r.account_id,
    date: String(r.date).slice(0, 10),
    direction: r.direction,
    amount: Number(r.amount),
    note: r.note,
    createdAt: Number(r.created_at),
  }));

  const snapFx = buildFxMapFromSnapshots(snapRows);
  const today = new Date().toISOString().slice(0, 10);
  const dateKeys = new Set([today]);
  for (const t of tradeList) dateKeys.add(t.date);
  for (const r of ctf) dateKeys.add(r.date);
  const dbFx = await loadFxByDateFromDb([...dateKeys]);
  const fxByDate = mergeFxMaps(dbFx, snapFx);
  const fxResolved = await resolveFxRatesCny({ dateKey: today });
  const fxSpot = { USD: fxResolved.USD, HKD: fxResolved.HKD };
  if (!(fxSpot.USD > 0) || !(fxSpot.HKD > 0)) {
    console.warn("[audit] missing spot FX from symbol_daily_close", fxSpot);
  }

  const sumIn = ctf.filter((r) => r.direction === "in").reduce((s, r) => s + r.amount, 0);
  const sumOut = ctf.filter((r) => r.direction === "out").reduce((s, r) => s + r.amount, 0);
  const buyAmt = tradeList.filter((t) => t.side === "buy").reduce((s, t) => s + Math.abs(t.amount || 0), 0);
  const sellAmt = tradeList.filter((t) => t.side === "sell").reduce((s, t) => s + Math.abs(t.amount || 0), 0);

  const { cashCny } = computeLedgerCashAndPrincipal(tradeList, ctf, accById, fxByDate, fxSpot);

  let principalNat = 0;
  for (const r of ctf) {
    if (String(r.accountId || "default") !== String(aid)) continue;
    principalNat += cashTransferSignedNativeAmount(r);
  }
  const accRow = accById.get(String(aid)) || { currency: "CNY" };
  const accCcy = String(accRow.currency || "CNY").toUpperCase();
  const principalCnyAtSpot =
    accCcy === "CNY" ? principalNat : principalNat * getFxRateToCny(accCcy, fxSpot);

  console.log("\n--- Raw counts ---");
  console.log({ trades: tradeList.length, cashRows: ctf.length });
  console.log({ cashInNativeSum: sumIn, cashOutNativeSum: sumOut, buyNotionalAbs: buyAmt, sellNotionalAbs: sellAmt });
  console.log("\n--- Ledger (matches app: 子账户本金=原币净额; 全部账户本金=各账户原币×即期折人民币) ---");
  console.log({
    principalNative: Math.round(principalNat * 100) / 100,
    accountCurrency: accCcy,
    principalCnyAtSpot: Math.round(principalCnyAtSpot * 100) / 100,
    cashCny: Math.round(cashCny * 100) / 100,
  });

  await client.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
