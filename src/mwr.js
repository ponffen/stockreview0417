/**
 * 统一 MWR：现金流 → 区间持有期收益率（不年化）。
 * 全站 freeze / bundle 应经本模块计算 MWR。
 */

function normalizeCashflows(cashflows) {
  return (cashflows || [])
    .map((c) => ({
      date: String(c?.date || "").slice(0, 10),
      amt: Number(c?.amt ?? c?.amount ?? 0),
    }))
    .filter((c) => c.date && Number.isFinite(c.amt) && c.amt !== 0)
    .sort((a, b) => a.date.localeCompare(b.date));
}

function npvAnnual(rate, datedAmounts) {
  const t0 = new Date(`${datedAmounts[0].date}T12:00:00+08:00`).getTime();
  let s = 0;
  for (const { date, amt } of datedAmounts) {
    const years = (new Date(`${date}T12:00:00+08:00`).getTime() - t0) / (365.25 * 86400000);
    s += amt / (1 + rate) ** years;
  }
  return s;
}

/** 不规则现金流 XIRR（年化），仅作 mwrFromCashflows 内部步骤 */
function xirrAnnualized(datedAmounts, guess = 0.08) {
  if (!datedAmounts.length) return 0;
  let r = guess;
  for (let k = 0; k < 40; k += 1) {
    const t0 = new Date(`${datedAmounts[0].date}T12:00:00+08:00`).getTime();
    let f = 0;
    let df = 0;
    for (const { date, amt } of datedAmounts) {
      const years = (new Date(`${date}T12:00:00+08:00`).getTime() - t0) / (365.25 * 86400000);
      const den = (1 + r) ** years;
      f += amt / den;
      df += (-years * amt) / ((1 + r) ** (years + 1));
    }
    if (!Number.isFinite(f) || Math.abs(f) < 1e-8) return r;
    if (!Number.isFinite(df) || Math.abs(df) < 1e-12) break;
    const nr = r - f / df;
    if (!Number.isFinite(nr) || nr <= -0.9999 || nr > 100) break;
    r = nr;
  }
  let lo = -0.9999;
  let hi = 10;
  for (let k = 0; k < 80; k += 1) {
    const mid = (lo + hi) / 2;
    const v = npvAnnual(mid, datedAmounts);
    if (!Number.isFinite(v)) break;
    if (Math.abs(v) < 1e-7) return mid;
    if (v > 0) lo = mid;
    else hi = mid;
  }
  return r;
}

/**
 * 统一 MWR：输入现金流，输出区间持有期收益率（不年化）。
 * @param {Array<{date:string, amount?:number, amt?:number}>} cashflows
 */
function mwrFromCashflows(cashflows) {
  const dated = normalizeCashflows(cashflows);
  if (dated.length < 2) {
    return 0;
  }
  const r = xirrAnnualized(dated, 0.05);
  const t0 = new Date(`${dated[0].date}T12:00:00+08:00`).getTime();
  const t1 = new Date(`${dated[dated.length - 1].date}T12:00:00+08:00`).getTime();
  const years = (t1 - t0) / (365.25 * 86400000);
  if (!(years > 1e-6)) {
    return 0;
  }
  return (1 + r) ** years - 1;
}

/** 账户：期初 TA、窗口内银证、期末 TA → 现金流 */
function buildAccountMwrCashflows(rowsSortedAsc, windowStart, windowEnd) {
  const start = String(windowStart).slice(0, 10);
  const end = String(windowEnd).slice(0, 10);
  const prev = rowsSortedAsc.filter((r) => String(r.date).slice(0, 10) < start);
  const bv = prev.length ? Number(prev[prev.length - 1].totalAssets ?? 0) : 0;
  const anchor = prev.length ? String(prev[prev.length - 1].date).slice(0, 10) : start;
  const inWin = rowsSortedAsc.filter((r) => String(r.date).slice(0, 10) >= start && String(r.date).slice(0, 10) <= end);
  if (!inWin.length) {
    return null;
  }
  const ev = Number(inWin[inWin.length - 1].totalAssets ?? 0);
  const lastD = String(inWin[inWin.length - 1].date).slice(0, 10);
  const dayMap = new Map();
  if (Number.isFinite(bv) && bv !== 0) {
    dayMap.set(anchor, (dayMap.get(anchor) || 0) - bv);
  }
  for (const r of inWin) {
    const ef = Number(r.externalFlowCny ?? r.external_flow_cny ?? 0) || 0;
    if (ef) {
      const d = String(r.date).slice(0, 10);
      dayMap.set(d, (dayMap.get(d) || 0) + ef);
    }
  }
  if (Number.isFinite(ev)) {
    dayMap.set(lastD, (dayMap.get(lastD) || 0) + ev);
  }
  const dated = [...dayMap.entries()]
    .map(([date, amt]) => ({ date, amt }))
    .filter((x) => x.amt !== 0)
    .sort((a, b) => a.date.localeCompare(b.date));
  return dated.length >= 2 ? dated : null;
}

/** 账户阶段 + live 期末 TA */
function buildAccountMwrCashflowsStageToLive(rowsSortedAsc, windowStart, liveDate, liveTotalAssetsCny) {
  const start = String(windowStart).slice(0, 10);
  const end = String(liveDate).slice(0, 10);
  const prev = rowsSortedAsc.filter((r) => String(r.date).slice(0, 10) < start);
  const bv = prev.length ? Number(prev[prev.length - 1].totalAssets ?? 0) : 0;
  const anchor = prev.length ? String(prev[prev.length - 1].date).slice(0, 10) : start;
  const dayMap = new Map();
  if (Number.isFinite(bv) && bv !== 0) {
    dayMap.set(anchor, (dayMap.get(anchor) || 0) - bv);
  }
  for (const r of rowsSortedAsc) {
    const d = String(r.date).slice(0, 10);
    if (d < start || d > end) {
      continue;
    }
    if (d === end) {
      continue;
    }
    const ef = Number(r.externalFlowCny ?? r.external_flow_cny ?? 0) || 0;
    if (ef) {
      dayMap.set(d, (dayMap.get(d) || 0) + ef);
    }
  }
  const endRow = rowsSortedAsc.find((r) => String(r.date).slice(0, 10) === end);
  const efEnd = Number(endRow?.externalFlowCny ?? endRow?.external_flow_cny ?? 0) || 0;
  if (efEnd) {
    dayMap.set(end, (dayMap.get(end) || 0) + efEnd);
  }
  const ev = Number(liveTotalAssetsCny);
  if (Number.isFinite(ev)) {
    dayMap.set(end, (dayMap.get(end) || 0) + ev);
  }
  const dated = [...dayMap.entries()]
    .map(([date, amt]) => ({ date, amt }))
    .filter((x) => x.amt !== 0)
    .sort((a, b) => a.date.localeCompare(b.date));
  return dated.length >= 2 ? dated : null;
}

/** 今日：期初 TA + 当日银证 + 期末 TA */
function buildAccountMwrCashflowsToday(frozenDate, frozenTotalAssetsCny, liveDate, liveTotalAssetsCny, externalFlowTodayCny) {
  const fd = String(frozenDate || "").slice(0, 10);
  const ld = String(liveDate || "").slice(0, 10);
  const bv = Number(frozenTotalAssetsCny) || 0;
  const ev = Number(liveTotalAssetsCny) || 0;
  const ef = Number(externalFlowTodayCny) || 0;
  const dayMap = new Map();
  if (Number.isFinite(bv) && bv !== 0) {
    dayMap.set(fd, (dayMap.get(fd) || 0) - bv);
  }
  if (ef) {
    dayMap.set(ld, (dayMap.get(ld) || 0) + ef);
  }
  if (Number.isFinite(ev)) {
    dayMap.set(ld, (dayMap.get(ld) || 0) + ev);
  }
  const dated = [...dayMap.entries()]
    .map(([date, amt]) => ({ date, amt }))
    .filter((x) => x.amt !== 0)
    .sort((a, b) => a.date.localeCompare(b.date));
  return dated.length >= 2 ? dated : null;
}

/** 个股：市值 + 买卖流 */
function buildSymbolMwrCashflows(ptsSorted, endDate, endValue) {
  const list = Array.isArray(ptsSorted) ? ptsSorted.filter((p) => p && p.date) : [];
  if (!list.length && !(Number(endValue) > 0)) {
    return null;
  }
  const end = String(endDate || list[list.length - 1]?.date || "").slice(0, 10);
  const dayMap = new Map();
  if (list.length) {
    const first = list[0];
    const bv = Number(first.value) - Number(first.flow || 0);
    const anchor = String(first.date).slice(0, 10);
    if (Number.isFinite(bv) && bv !== 0) {
      dayMap.set(anchor, (dayMap.get(anchor) || 0) - bv);
    }
    for (const p of list) {
      const d = String(p.date).slice(0, 10);
      if (d === end) {
        continue;
      }
      const ef = Number(p.flow || 0) || 0;
      if (ef) {
        dayMap.set(d, (dayMap.get(d) || 0) + ef);
      }
    }
    const endPt = list.find((p) => String(p.date).slice(0, 10) === end);
    const efEnd = Number(endPt?.flow || 0) || 0;
    if (efEnd) {
      dayMap.set(end, (dayMap.get(end) || 0) + efEnd);
    }
  }
  const ev = Number(endValue);
  if (Number.isFinite(ev)) {
    dayMap.set(end, (dayMap.get(end) || 0) + ev);
  }
  const dated = [...dayMap.entries()]
    .map(([date, amt]) => ({ date, amt }))
    .filter((x) => x.amt !== 0)
    .sort((a, b) => a.date.localeCompare(b.date));
  return dated.length >= 2 ? dated : null;
}

function accountMwrFromSnapshotWindow(rowsSortedAsc, windowStart, windowEnd) {
  const cfs = buildAccountMwrCashflows(rowsSortedAsc, windowStart, windowEnd);
  if (!cfs) {
    return 0;
  }
  return mwrFromCashflows(cfs);
}

function accountMwrStageToLive(rowsSortedAsc, windowStart, liveDate, liveTotalAssetsCny) {
  const cfs = buildAccountMwrCashflowsStageToLive(rowsSortedAsc, windowStart, liveDate, liveTotalAssetsCny);
  if (!cfs) {
    return 0;
  }
  return mwrFromCashflows(cfs);
}

function accountMwrTodayOnly(frozenDate, frozenTotalAssetsCny, liveDate, liveTotalAssetsCny, externalFlowTodayCny) {
  const cfs = buildAccountMwrCashflowsToday(
    frozenDate,
    frozenTotalAssetsCny,
    liveDate,
    liveTotalAssetsCny,
    externalFlowTodayCny,
  );
  if (!cfs) {
    return 0;
  }
  return mwrFromCashflows(cfs);
}

function symbolMwrFromValueFlowPoints(ptsSorted, endDate, endValue) {
  const cfs = buildSymbolMwrCashflows(ptsSorted, endDate, endValue);
  if (!cfs) {
    return 0;
  }
  return mwrFromCashflows(cfs);
}

module.exports = {
  mwrFromCashflows,
  buildAccountMwrCashflows,
  buildAccountMwrCashflowsStageToLive,
  buildAccountMwrCashflowsToday,
  buildSymbolMwrCashflows,
  accountMwrFromSnapshotWindow,
  accountMwrStageToLive,
  accountMwrTodayOnly,
  symbolMwrFromValueFlowPoints,
};
