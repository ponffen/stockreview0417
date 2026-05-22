/**
 * g 个股排行：与分析 tab 同一 stage 窗口；服务端计算，前端只展示。
 */
const { getTrades, getSymbolDailyPnl, normalizeSymbol } = require("../db");
const { fmtMoney, fmtPercentRatio } = require("../account-kpi-surface");
const { resolveStageRange } = require("./stages");

function sortTradeAsc(a, b) {
  return String(a.date).localeCompare(String(b.date)) || Number(a.createdAt || 0) - Number(b.createdAt || 0);
}

function countHeldDaysInRange(symbolTrades, a, b) {
  const days = new Set();
  for (const t of symbolTrades) {
    const d = String(t.date).slice(0, 10);
    if (d >= a && d <= b) {
      days.add(d);
    }
  }
  let held = 0;
  let qty = 0;
  const sorted = [...symbolTrades].sort(sortTradeAsc);
  for (let d = a; d <= b; d = addDay(d)) {
    for (const t of sorted) {
      if (String(t.date).slice(0, 10) === d) {
        qty += t.side === "buy" ? Number(t.quantity) : -Number(t.quantity);
      }
    }
    if (qty > 1e-6) {
      held += 1;
    }
  }
  return held;
}

function addDay(dateKey) {
  const d = new Date(`${dateKey}T12:00:00+08:00`);
  d.setDate(d.getDate() + 1);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function symbolEodQtyOnOrBefore(symbolTrades, b) {
  let qty = 0;
  for (const t of symbolTrades.sort(sortTradeAsc)) {
    if (String(t.date).slice(0, 10) <= b) {
      qty += t.side === "buy" ? Number(t.quantity) : -Number(t.quantity);
    }
  }
  return qty;
}

async function buildStockRankPayload({ userId, accountScope, stage, live }) {
  const scope = String(accountScope || "all").trim() || "all";
  const asOf = live.frozenThrough || live.liveDate || "";
  const trades = await getTrades(userId);
  const scopeTrades =
    scope === "all" ? trades : trades.filter((t) => String(t.accountId || "default") === scope);
  const firstTrade =
    scopeTrades.length > 0 ? [...scopeTrades].sort(sortTradeAsc)[0].date : asOf;
  const { start: a, end: b } = resolveStageRange(stage, asOf, firstTrade);
  const periodEnd = live.tradingDay && live.liveDate ? live.liveDate : b;

  const symSet = new Set(scopeTrades.map((t) => normalizeSymbol(t.symbol)).filter(Boolean));
  const rows = [];
  for (const sym of symSet) {
    const symbolTrades = scopeTrades.filter((t) => normalizeSymbol(t.symbol) === sym).sort(sortTradeAsc);
    if (!symbolTrades.length) {
      continue;
    }
    if (countHeldDaysInRange(symbolTrades, a, periodEnd) < 1) {
      continue;
    }
    const A = symbolTrades[0].date;
    const B = symbolTrades[symbolTrades.length - 1].date;
    let effStart = A < a ? a : A;
    let effEnd = B < periodEnd ? B : periodEnd;
    if (symbolEodQtyOnOrBefore(symbolTrades, periodEnd) > 1e-6) {
      effEnd = periodEnd;
    }
    if (effStart > effEnd) {
      continue;
    }
    const pnlRows = await getSymbolDailyPnl(
      { accountId: scope, symbol: sym, from: effStart, to: effEnd },
      userId,
    );
    let profitCny = 0;
    let startPx = null;
    let endPx = null;
    for (const r of pnlRows) {
      profitCny += Number(r.dayPnlNative ?? r.day_pnl_native) || 0;
      const px = Number(r.dayClosePrice ?? r.day_close_price);
      if (Number.isFinite(px) && px > 0) {
        if (!startPx) {
          startPx = px;
        }
        endPx = px;
      }
    }
    if (live.tradingDay) {
      const lp = (live.positions || []).find((p) => normalizeSymbol(p.symbol) === sym);
      if (lp) {
        profitCny += Number(lp.todayProfitCny) || 0;
        endPx = Number(lp.current) || endPx;
      }
    }
    const pxChange = startPx > 0 && endPx != null ? (endPx - startPx) / startPx : 0;
    const heldDays = Math.max(1, Math.round((new Date(`${effEnd}T12:00:00+08:00`) - new Date(`${effStart}T12:00:00+08:00`)) / 86400000) + 1);
    rows.push({
      symbol: sym,
      name: symbolTrades[0].name || sym,
      holdIntervalsLabel: `${effStart.slice(5)} – ${effEnd.slice(5)}`,
      profitCny,
      profitDisplay: fmtMoney(profitCny, "CNY"),
      pxChange,
      pxChangeDisplay: fmtPercentRatio(pxChange),
      heldDays,
    });
  }
  rows.sort((x, y) => y.profitCny - x.profitCny);
  const total = rows.reduce((s, r) => s + r.profitCny, 0);
  return {
    stage,
    periodStart: a,
    periodEnd,
    rows: rows.map((r, i) => ({
      rank: i + 1,
      symbol: r.symbol,
      name: r.name,
      holdIntervalsLabel: r.holdIntervalsLabel,
      profitDisplay: r.profitDisplay,
      profitShareDisplay: total !== 0 ? fmtPercentRatio(r.profitCny / total) : "0.00%",
      pxChangeDisplay: r.pxChangeDisplay,
      heldDays: r.heldDays,
    })),
  };
}

module.exports = { buildStockRankPayload };
