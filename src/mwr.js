/**
 * 统一 MWR：现金流 → 区间持有期收益率（不年化）。
 * 全站 freeze / bundle 应经本模块计算 MWR；算不出返回 null（展示「—」）。
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

function terminalNav(totalAssets) {
  return Math.max(0, Number(totalAssets) || 0);
}

function cashflowsHaveOppositeSigns(dated) {
  let hasPos = false;
  let hasNeg = true;
  for (const { amt } of dated) {
    if (amt > 0) {
      hasPos = true;
    }
    if (amt < 0) {
      hasNeg = true;
    }
  }
  return hasPos && hasNeg;
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

/** 不规则现金流 XIRR（年化），仅作 mwrFromCashflows 内部步骤；无解返回 null */
function xirrAnnualized(datedAmounts, guess = 0.08) {
  if (!datedAmounts.length) {
    return null;
  }
  if (!cashflowsHaveOppositeSigns(datedAmounts)) {
    return null;
  }
  const probes = [-0.99, -0.5, -0.1, 0, 0.05, 0.1, 0.5, 1, 5, 10, 50, 100, 500];
  let lo = null;
  let hi = null;
  for (let i = 0; i < probes.length - 1; i += 1) {
    const v0 = npvAnnual(probes[i], datedAmounts);
    const v1 = npvAnnual(probes[i + 1], datedAmounts);
    if (Number.isFinite(v0) && Number.isFinite(v1) && v0 * v1 < 0) {
      lo = probes[i];
      hi = probes[i + 1];
      break;
    }
  }
  if (lo == null || hi == null) {
    return null;
  }

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
    if (!Number.isFinite(f) || Math.abs(f) < 1e-8) {
      return Number.isFinite(r) && Math.abs(npvAnnual(r, datedAmounts)) < 1e-5 ? r : null;
    }
    if (!Number.isFinite(df) || Math.abs(df) < 1e-12) {
      break;
    }
    const nr = r - f / df;
    if (!Number.isFinite(nr) || nr <= -0.9999 || nr > 100) {
      break;
    }
    r = nr;
  }
  let loB = lo;
  let hiB = hi;
  for (let k = 0; k < 80; k += 1) {
    const mid = (loB + hiB) / 2;
    const v = npvAnnual(mid, datedAmounts);
    if (!Number.isFinite(v)) {
      break;
    }
    if (Math.abs(v) < 1e-7) {
      return mid;
    }
    if (v > 0) {
      loB = mid;
    } else {
      hiB = mid;
    }
  }
  return null;
}

/**
 * 统一 MWR：输入现金流，输出区间持有期收益率（不年化）；无解返回 null。
 * @param {Array<{date:string, amount?:number, amt?:number}>} cashflows
 * @returns {number|null}
 */
function mwrFromCashflows(cashflows) {
  const dated = normalizeCashflows(cashflows);
  if (dated.length < 2) {
    return null;
  }
  const r = xirrAnnualized(dated, 0.05);
  if (r == null || !Number.isFinite(r)) {
    return null;
  }
  const t0 = new Date(`${dated[0].date}T12:00:00+08:00`).getTime();
  const t1 = new Date(`${dated[dated.length - 1].date}T12:00:00+08:00`).getTime();
  const years = (t1 - t0) / (365.25 * 86400000);
  if (!(years > 1e-6)) {
    return null;
  }
  const period = (1 + r) ** years - 1;
  return Number.isFinite(period) ? period : null;
}

function mergeDayMap(dayMap, date, amt) {
  if (!date || !Number.isFinite(amt) || amt === 0) {
    return;
  }
  dayMap.set(date, (dayMap.get(date) || 0) + amt);
}

function dayMapToCashflows(dayMap) {
  const dated = [...dayMap.entries()]
    .map(([date, amt]) => ({ date, amt }))
    .filter((x) => x.amt !== 0)
    .sort((a, b) => a.date.localeCompare(b.date));
  return dated.length >= 2 ? dated : null;
}

/** 账户窗口：期初净资产/本金、银证、期末净资产（≥0） */
function buildAccountMwrCashflows(rowsSortedAsc, windowStart, windowEnd) {
  const start = String(windowStart).slice(0, 10);
  const end = String(windowEnd).slice(0, 10);
  const prev = rowsSortedAsc.filter((r) => String(r.date).slice(0, 10) < start);
  const bv = prev.length ? Number(prev[prev.length - 1].totalAssets ?? 0) : 0;
  const anchor = prev.length ? String(prev[prev.length - 1].date).slice(0, 10) : start;
  const inWin = rowsSortedAsc.filter((r) => {
    const d = String(r.date).slice(0, 10);
    return d >= start && d <= end;
  });
  if (!inWin.length) {
    return null;
  }
  const lastRow = inWin[inWin.length - 1];
  const lastD = String(lastRow.date).slice(0, 10);
  const ev = terminalNav(lastRow.totalAssets);
  const dayMap = new Map();

  let skipFlowOn = null;
  if (Number.isFinite(bv) && bv !== 0) {
    mergeDayMap(dayMap, anchor, -bv);
  } else {
    const firstDay = String(inWin[0].date).slice(0, 10);
    const pr = Number(inWin[0].principal ?? inWin[0].principalCny ?? 0);
    if (pr > 0) {
      mergeDayMap(dayMap, firstDay, -pr);
      skipFlowOn = firstDay;
    }
  }

  for (const r of inWin) {
    const d = String(r.date).slice(0, 10);
    if (d === skipFlowOn) {
      continue;
    }
    const ef = Number(r.externalFlowCny ?? r.external_flow_cny ?? 0) || 0;
    if (ef) {
      mergeDayMap(dayMap, d, ef);
    }
  }
  mergeDayMap(dayMap, lastD, ev);
  return dayMapToCashflows(dayMap);
}

function buildAccountMwrCashflowsStageToLive(rowsSortedAsc, windowStart, liveDate, liveTotalAssetsCny) {
  const start = String(windowStart).slice(0, 10);
  const end = String(liveDate).slice(0, 10);
  const prev = rowsSortedAsc.filter((r) => String(r.date).slice(0, 10) < start);
  const bv = prev.length ? Number(prev[prev.length - 1].totalAssets ?? 0) : 0;
  const anchor = prev.length ? String(prev[prev.length - 1].date).slice(0, 10) : start;
  const inWin = rowsSortedAsc.filter((r) => {
    const d = String(r.date).slice(0, 10);
    return d >= start && d <= end;
  });
  const dayMap = new Map();

  let skipFlowOn = null;
  if (Number.isFinite(bv) && bv !== 0) {
    mergeDayMap(dayMap, anchor, -bv);
  } else if (inWin.length) {
    const firstDay = String(inWin[0].date).slice(0, 10);
    const pr = Number(inWin[0].principal ?? inWin[0].principalCny ?? 0);
    if (pr > 0) {
      mergeDayMap(dayMap, firstDay, -pr);
      skipFlowOn = firstDay;
    }
  }

  for (const r of rowsSortedAsc) {
    const d = String(r.date).slice(0, 10);
    if (d < start || d > end || d === end) {
      continue;
    }
    if (d === skipFlowOn) {
      continue;
    }
    const ef = Number(r.externalFlowCny ?? r.external_flow_cny ?? 0) || 0;
    if (ef) {
      mergeDayMap(dayMap, d, ef);
    }
  }
  const endRow = rowsSortedAsc.find((r) => String(r.date).slice(0, 10) === end);
  const efEnd = Number(endRow?.externalFlowCny ?? endRow?.external_flow_cny ?? 0) || 0;
  if (efEnd && end !== skipFlowOn) {
    mergeDayMap(dayMap, end, efEnd);
  }
  mergeDayMap(dayMap, end, terminalNav(liveTotalAssetsCny));
  return dayMapToCashflows(dayMap);
}

function buildAccountMwrCashflowsToday(frozenDate, frozenTotalAssetsCny, liveDate, liveTotalAssetsCny, externalFlowTodayCny) {
  const fd = String(frozenDate || "").slice(0, 10);
  const ld = String(liveDate || "").slice(0, 10);
  const bv = Number(frozenTotalAssetsCny) || 0;
  const ev = terminalNav(liveTotalAssetsCny);
  const ef = Number(externalFlowTodayCny) || 0;
  const dayMap = new Map();
  if (Number.isFinite(bv) && bv !== 0) {
    mergeDayMap(dayMap, fd, -bv);
  }
  if (ef) {
    mergeDayMap(dayMap, ld, ef);
  }
  mergeDayMap(dayMap, ld, ev);
  return dayMapToCashflows(dayMap);
}

/** 个股：期初成本（首日 value−flow）、买卖流、期末市值（≥0） */
function buildSymbolMwrCashflows(ptsSorted, endDate, endValue) {
  const list = Array.isArray(ptsSorted) ? ptsSorted.filter((p) => p && p.date) : [];
  const end = String(endDate || list[list.length - 1]?.date || "").slice(0, 10);
  if (!end) {
    return null;
  }
  const dayMap = new Map();
  let skipFlowOn = null;

  if (list.length) {
    const first = list[0];
    const firstDay = String(first.date).slice(0, 10);
    const openingCost = Number(first.value) - Number(first.flow || 0);
    if (Number.isFinite(openingCost) && openingCost > 0) {
      mergeDayMap(dayMap, firstDay, -openingCost);
      skipFlowOn = firstDay;
    }
    for (const p of list) {
      const d = String(p.date).slice(0, 10);
      if (d === end || d === skipFlowOn) {
        continue;
      }
      const ef = Number(p.flow || 0) || 0;
      if (ef) {
        mergeDayMap(dayMap, d, ef);
      }
    }
    const endPt = list.find((p) => String(p.date).slice(0, 10) === end);
    const efEnd = Number(endPt?.flow || 0) || 0;
    if (efEnd && end !== skipFlowOn) {
      mergeDayMap(dayMap, end, efEnd);
    }
  }
  mergeDayMap(dayMap, end, terminalNav(endValue));
  return dayMapToCashflows(dayMap);
}

function accountMwrFromSnapshotWindow(rowsSortedAsc, windowStart, windowEnd) {
  const cfs = buildAccountMwrCashflows(rowsSortedAsc, windowStart, windowEnd);
  if (!cfs) {
    return null;
  }
  return mwrFromCashflows(cfs);
}

function accountMwrStageToLive(rowsSortedAsc, windowStart, liveDate, liveTotalAssetsCny) {
  const cfs = buildAccountMwrCashflowsStageToLive(rowsSortedAsc, windowStart, liveDate, liveTotalAssetsCny);
  if (!cfs) {
    return null;
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
    return null;
  }
  return mwrFromCashflows(cfs);
}

function symbolMwrFromValueFlowPoints(ptsSorted, endDate, endValue) {
  const cfs = buildSymbolMwrCashflows(ptsSorted, endDate, endValue);
  if (!cfs) {
    return null;
  }
  return mwrFromCashflows(cfs);
}

/** 冻结表写入：null → 0（列 NOT NULL）；bundle 展示不读该字段 */
function mwrForFreezeStorage(rate) {
  return rate == null || !Number.isFinite(rate) ? 0 : rate;
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
  mwrForFreezeStorage,
};
