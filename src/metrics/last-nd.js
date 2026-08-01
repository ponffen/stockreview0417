/**
 * 近 N 日（自然日窗口 [今−N+1, 今]）收益/TWR 收益率。
 *
 * 口径：从「成立以来累计」曲线相对窗口锚点 rebase，而非滑窗逐日加减。
 *   近Nd 收益   = inception(d) − inception(锚点)
 *   近Nd TWR    = (1 + twrCum(d)) / (1 + twrCum(锚点)) − 1
 * 其中锚点 = 窗口前一交易日的累计值（窗口首行累计 − 首行当日值；账户不足 N 天时为 0）。
 *
 * 算法若变更，只改本文件即可（7/30/90 天共用同一函数）。
 */

const { addCalendarDays, resolveStageRange } = require("./stages");
const { rowStageValue } = require("./profit-tracks");

const LAST_ND_STAGE_DAYS = {
  last_7d: 7,
  last_30d: 30,
  last_90d: 90,
};

function lastNdStageDays(stageKey) {
  return LAST_ND_STAGE_DAYS[String(stageKey || "").trim()] || 0;
}

function isLastNdStage(stageKey) {
  return lastNdStageDays(stageKey) > 0;
}

/** 近Nd 收益 = 当日累计收益 − 锚点累计收益。 */
function lastNdProfit(inceptionAtPoint, inceptionAtAnchor) {
  return (Number(inceptionAtPoint) || 0) - (Number(inceptionAtAnchor) || 0);
}

/** 近Nd TWR = (1+当日累计TWR)/(1+锚点累计TWR) − 1。 */
function lastNdRateTwr(twrCumAtPoint, twrCumAtAnchor) {
  const denom = 1 + (Number(twrCumAtAnchor) || 0);
  if (!Number.isFinite(denom) || Math.abs(denom) < 1e-12) {
    return 0;
  }
  return (1 + (Number(twrCumAtPoint) || 0)) / denom - 1;
}

/**
 * 锚点累计收益：窗口首行累计 − 首行当日收益（= 窗口前一交易日累计）。
 * 窗口为空 → 0；账户不足 N 天（首行即成立首日）→ 自动得 0。
 */
function anchorInceptionProfitFromWindowRows(windowRowsAsc) {
  const rows = Array.isArray(windowRowsAsc) ? windowRowsAsc : [];
  const first = rows[0];
  if (!first) {
    return 0;
  }
  return (Number(first.stageInceptionProfit) || 0) - (Number(first.dailyProfit) || 0);
}

/**
 * 锚点累计TWR：(1+首行累计TWR)/(1+首行当日TWR) − 1（= 窗口前一交易日累计TWR）。
 */
function anchorInceptionRateTwrFromWindowRows(windowRowsAsc) {
  const rows = Array.isArray(windowRowsAsc) ? windowRowsAsc : [];
  const first = rows[0];
  if (!first) {
    return 0;
  }
  const day = 1 + (Number(first.dailyRateTwr) || 0);
  if (!Number.isFinite(day) || Math.abs(day) < 1e-12) {
    return 0;
  }
  return (1 + (Number(first.stageInceptionRateTwr) || 0)) / day - 1;
}

/** 自然日窗口起点的前一自然日（= 个股分析锚点 asOf）。 */
function lastNdAnchorAsOf(periodStart) {
  return addCalendarDays(String(periodStart || "").slice(0, 10), -1);
}

function lastNdAnchorAsOfForStage(stageKey, asOf, firstTrade) {
  const R = String(asOf || "").slice(0, 10);
  const ft = String(firstTrade || R).slice(0, 10);
  const { start } = resolveStageRange(stageKey, R, ft);
  return lastNdAnchorAsOf(start);
}

function inceptionOnOrBefore(series, asOf) {
  const target = String(asOf || "").slice(0, 10);
  let best = null;
  for (const row of series || []) {
    const d = String(row.date || "").slice(0, 10);
    if (d <= target) {
      best = row;
    } else {
      break;
    }
  }
  return best;
}

function lastNdProfitFromFrozenRows(endRow, anchorRow, track = "native") {
  return lastNdProfit(
    rowStageValue(endRow, "inception", "Profit", track),
    rowStageValue(anchorRow, "inception", "Profit", track),
  );
}

function lastNdRateTwrFromFrozenRows(endRow, anchorRow, track = "native") {
  return lastNdRateTwr(
    rowStageValue(endRow, "inception", "RateTwr", track),
    rowStageValue(anchorRow, "inception", "RateTwr", track),
  );
}

const LAST_ND_STAGE_SUFFIX = {
  last_7d: "Last7d",
  last_30d: "Last30d",
  last_90d: "Last90d",
};

/** 冻结回放：用成立以来累计序列覆盖 stage_last_*d（与个股分析一致）。 */
function lastNdStageSnapshotsFromInception(snap, inceptionSeries, asOf, firstTrade) {
  const out = {};
  const R = String(asOf || "").slice(0, 10);
  const ft = String(firstTrade || R).slice(0, 10);
  for (const [stageKey, suffix] of Object.entries(LAST_ND_STAGE_SUFFIX)) {
    const { start } = resolveStageRange(stageKey, R, ft);
    const anchor = inceptionOnOrBefore(inceptionSeries, lastNdAnchorAsOf(start));
    out[`stage${suffix}Profit`] = lastNdProfit(
      snap?.stageInceptionProfit,
      anchor?.profit,
    );
    out[`stage${suffix}RateTwr`] = lastNdRateTwr(
      snap?.stageInceptionRateTwr,
      anchor?.rate,
    );
  }
  return out;
}

module.exports = {
  LAST_ND_STAGE_DAYS,
  lastNdStageDays,
  isLastNdStage,
  lastNdProfit,
  lastNdRateTwr,
  lastNdAnchorAsOf,
  lastNdAnchorAsOfForStage,
  inceptionOnOrBefore,
  lastNdProfitFromFrozenRows,
  lastNdRateTwrFromFrozenRows,
  lastNdStageSnapshotsFromInception,
  anchorInceptionProfitFromWindowRows,
  anchorInceptionRateTwrFromWindowRows,
};
