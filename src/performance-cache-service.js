const {
  getAnalysisDailySnapshots,
  deletePerformanceSeriesCacheForUser,
  upsertPerformanceSeriesCacheRow,
} = require("./db");
const {
  buildProfitSeries,
  rebaseRateSeriesByFirstDay,
  computeTimeWeightedSeries,
} = require("./home-summary-maths");

const PRESETS = ["last_7d", "last_30d", "last_90d", "mtd", "ytd", "inception"];
const ALGOS = ["twr", "mwr"];
/** 与 analysis / home-summary 序列口径一致；变更后须全量重算缓存 */
const PERFORMANCE_RULE_VERSION = 2;

function addCalendarDays(dateKey, days) {
  const d = new Date(`${String(dateKey || "").slice(0, 10)}T12:00:00+08:00`);
  if (Number.isNaN(d.getTime())) {
    return String(dateKey || "").slice(0, 10);
  }
  d.setDate(d.getDate() + Number(days || 0));
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function monthStartKeyShanghai(asOf) {
  const d = new Date(`${String(asOf).slice(0, 10)}T12:00:00+08:00`);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  return `${y}-${m}-01`;
}

function yearStartKeyShanghai(asOf) {
  const d = new Date(`${String(asOf).slice(0, 10)}T12:00:00+08:00`);
  const y = d.getFullYear();
  return `${y}-01-01`;
}

/** 自然日窗口，右端含 asOf；长度 len */
function windowNaturalDays(asOf, len) {
  const end = String(asOf).slice(0, 10);
  const start = addCalendarDays(end, -(len - 1));
  return { start, end };
}

function resolvePresetRange(preset, asOf, firstDataDate) {
  const R = String(asOf).slice(0, 10);
  if (preset === "last_7d") return windowNaturalDays(R, 7);
  if (preset === "last_30d") return windowNaturalDays(R, 30);
  if (preset === "last_90d") return windowNaturalDays(R, 90);
  if (preset === "mtd") return { start: monthStartKeyShanghai(R), end: R };
  if (preset === "ytd") return { start: yearStartKeyShanghai(R), end: R };
  if (preset === "inception") {
    const s = firstDataDate && String(firstDataDate).slice(0, 10) <= R ? String(firstDataDate).slice(0, 10) : R;
    return { start: s, end: R };
  }
  return { start: R, end: R };
}

function npv(rate, datedAmounts) {
  const t0 = new Date(`${datedAmounts[0].date}T12:00:00+08:00`).getTime();
  let s = 0;
  for (const { date, amt } of datedAmounts) {
    const years = (new Date(`${date}T12:00:00+08:00`).getTime() - t0) / (365.25 * 86400000);
    s += amt / (1 + rate) ** years;
  }
  return s;
}

/** 不规则现金流 XIRR：牛顿法 + 二分回退 */
function xirr(datedAmounts, guess = 0.08) {
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
    const v = npv(mid, datedAmounts);
    if (!Number.isFinite(v)) break;
    if (Math.abs(v) < 1e-7) return mid;
    if (v > 0) lo = mid;
    else hi = mid;
  }
  return r;
}

/** 区间 TWR：(1+cumE)/(1+cumS0)-1，S0 为 start 前一日的累计指数 */
function twrPeriodFromSnapshots(rows, start, end) {
  if (!rows.length) return 0;
  const byDate = new Map(rows.map((r) => [r.date, r]));
  const start0 = addCalendarDays(start, -1);
  let cumBefore = 0;
  let hit = false;
  for (const r of rows) {
    if (r.date <= start0) {
      cumBefore = Number(r.twRCumulative ?? r.tw_r_cumulative ?? 0);
      hit = true;
    }
  }
  if (!hit) cumBefore = 0;
  let cumEnd = cumBefore;
  for (const r of rows) {
    if (r.date <= end) {
      cumEnd = Number(r.twRCumulative ?? r.tw_r_cumulative ?? 0);
    }
  }
  const d0 = 1 + cumBefore;
  const d1 = 1 + cumEnd;
  if (!Number.isFinite(d0) || Math.abs(d0) < 1e-12) return Number.isFinite(cumEnd) ? cumEnd : 0;
  return d1 / d0 - 1;
}

/** MWR：期初 -BV、区间内 external_flow_cny、期末 +EV（同日合并后 XIRR）；BV/EV 用 total_assets（总资产） */
function mwrPeriodFromSnapshots(rows, start, end) {
  if (!rows.length) return 0;
  const prev = rows.filter((r) => r.date < start);
  const bv = prev.length ? Number(prev[prev.length - 1].totalAssets ?? 0) : 0;
  const anchor = prev.length ? String(prev[prev.length - 1].date).slice(0, 10) : String(start).slice(0, 10);
  const inWin = rows.filter((r) => r.date >= start && r.date <= end);
  if (!inWin.length) return 0;
  const ev = Number(inWin[inWin.length - 1].totalAssets ?? 0);
  const lastD = String(inWin[inWin.length - 1].date).slice(0, 10);
  const dayMap = new Map();
  if (Number.isFinite(bv) && bv !== 0) {
    dayMap.set(anchor, (dayMap.get(anchor) || 0) - bv);
  }
  for (const r of inWin) {
    const ef = Number(r.externalFlowCny ?? 0);
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
  if (dated.length < 2) return 0;
  return xirr(dated, 0.05);
}

async function deletePerformanceSeriesForUser(userId) {
  return deletePerformanceSeriesCacheForUser(userId);
}

function buildTwrPresetSeriesJson(sliceRows) {
  if (!sliceRows.length) return null;
  const pts = sliceRows.map((r) => ({
    date: r.date,
    value: Number(r.totalAssets ?? 0) || 0,
    flow: Number(r.externalFlowCny ?? 0) || 0,
  }));
  const profitSeries = buildProfitSeries(pts);
  const twRaw = computeTimeWeightedSeries(pts);
  const twRebased = rebaseRateSeriesByFirstDay(twRaw);
  return JSON.stringify({
    ruleVersion: PERFORMANCE_RULE_VERSION,
    dates: pts.map((p) => p.date),
    totalAssets: pts.map((p) => p.value),
    externalFlowCny: pts.map((p) => p.flow),
    cumulativeProfit: profitSeries.map((p) => p.value),
    twrRebased: twRebased.map((p) => p.rate),
  });
}

/**
 * 为某用户、若干账户、在 as_of_date 上预计算各 preset × twr/mwr。
 * @param {object} opts
 * @param {string} opts.userId
 * @param {string} opts.asOfDate 冻结日（右端）
 * @param {string[]} opts.accountIds
 * @param {{ communityOnly?: boolean }} opts
 */
async function rebuildPerformanceSeriesCache(opts) {
  const userId = String(opts.userId || "").trim();
  const asOf = String(opts.asOfDate || "").slice(0, 10);
  const accountIds = Array.isArray(opts.accountIds) ? opts.accountIds : [];
  const communityOnly = opts.communityOnly === true;
  if (!userId || !asOf || !accountIds.length) return { ok: true, written: 0 };

  let written = 0;
  for (const accountId of accountIds) {
    const rowsRaw = await getAnalysisDailySnapshots({ accountId, from: "2000-01-01", to: asOf }, userId);
    const rows = rowsRaw.map((r) => ({
      date: r.date,
      marketValue: r.marketValue,
      totalAssets: Number(r.totalAssets ?? r.marketValue ?? 0),
      twRCumulative: r.twRCumulative != null ? r.twRCumulative : r.tw_r_cumulative,
      externalFlowCny: r.externalFlowCny != null ? r.externalFlowCny : r.external_flow_cny,
    }));
    if (!rows.length) continue;
    const firstDate = rows[0].date;
    const algos = communityOnly ? ["twr"] : ALGOS;

    for (const preset of PRESETS) {
      const { start, end } = resolvePresetRange(preset, asOf, firstDate);
      const win = rows.filter((r) => r.date >= start && r.date <= end);
      if (!win.length) continue;

      for (const algo of algos) {
        if (algo === "mwr") {
          continue;
        }
        let pr = 0;
        let seriesJson = null;
        const start0 = addCalendarDays(start, -1);
        const sliceForSeries = rows.filter((r) => r.date >= start0 && r.date <= end);
        pr = twrPeriodFromSnapshots(rows, start, end);
        seriesJson = buildTwrPresetSeriesJson(sliceForSeries);
        await upsertPerformanceSeriesCacheRow({
          user_id: userId,
          account_id: accountId,
          as_of_date: asOf,
          preset,
          algo,
          period_return: pr,
          series_json: seriesJson,
          source_frozen_through: asOf,
          rule_version: PERFORMANCE_RULE_VERSION,
        });
        written += 1;
      }
    }
  }
  return { ok: true, written };
}

module.exports = {
  rebuildPerformanceSeriesCache,
  deletePerformanceSeriesForUser,
  PRESETS,
  resolvePresetRange,
  twrPeriodFromSnapshots,
  mwrPeriodFromSnapshots,
  PERFORMANCE_RULE_VERSION,
};
