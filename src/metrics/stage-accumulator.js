/**
 * stage_* 递推：mtd/ytd/inception 累加；last_7d/30d/90d 滑窗减项 + TWR 连乘。
 */
const { addCalendarDays, monthStartKeyShanghai, yearStartKeyShanghai } = require("./stages");

function chainTwrRate(prevRate, dailyRate) {
  const p = Number(prevRate) || 0;
  const d = Number(dailyRate) || 0;
  return (1 + p) * (1 + d) - 1;
}

class StageAccumulator {
  constructor() {
    this.mtd = { profit: 0, rate: 0 };
    this.ytd = { profit: 0, rate: 0 };
    this.inception = { profit: 0, rate: 0 };
    this.last7 = { profit: 0, rate: 0 };
    this.last30 = { profit: 0, rate: 0 };
    this.last90 = { profit: 0, rate: 0 };
    this.profitByDate = new Map();
    this.curMonth = "";
    this.curYear = "";
    this.twrWindow7 = [];
    this.twrWindow30 = [];
    this.twrWindow90 = [];
  }

  onDay(dateKey, dailyProfit, dailyRateTwr) {
    const d = String(dateKey || "").slice(0, 10);
    const p = Number(dailyProfit) || 0;
    const r = Number(dailyRateTwr) || 0;
    const ms = monthStartKeyShanghai(d);
    const ys = yearStartKeyShanghai(d);

    if (ms !== this.curMonth) {
      this.mtd = { profit: 0, rate: 0 };
      this.curMonth = ms;
    }
    if (ys !== this.curYear) {
      this.ytd = { profit: 0, rate: 0 };
      this.curYear = ys;
    }

    this.mtd.profit += p;
    this.mtd.rate = chainTwrRate(this.mtd.rate, r);
    this.ytd.profit += p;
    this.ytd.rate = chainTwrRate(this.ytd.rate, r);
    this.inception.profit += p;
    this.inception.rate = chainTwrRate(this.inception.rate, r);

    const slide = (win, days) => {
      const outKey = addCalendarDays(d, -days);
      const outP = this.profitByDate.get(outKey) || 0;
      win.profit = win.profit + p - outP;
    };
    slide(this.last7, 7);
    slide(this.last30, 30);
    slide(this.last90, 90);

    this._pushTwrWindow(this.twrWindow7, r, 7);
    this._pushTwrWindow(this.twrWindow30, r, 30);
    this._pushTwrWindow(this.twrWindow90, r, 90);

    this.profitByDate.set(d, p);
  }

  _pushTwrWindow(arr, dailyRate, maxLen) {
    arr.push(Number(dailyRate) || 0);
    while (arr.length > maxLen) {
      arr.shift();
    }
    let rate = 0;
    for (const r of arr) {
      rate = chainTwrRate(rate, r);
    }
    if (maxLen === 7) {
      this.last7.rate = rate;
    } else if (maxLen === 30) {
      this.last30.rate = rate;
    } else if (maxLen === 90) {
      this.last90.rate = rate;
    }
  }

  snapshotTwr() {
    return {
      stageMtdProfit: this.mtd.profit,
      stageMtdRateTwr: this.mtd.rate,
      stageYtdProfit: this.ytd.profit,
      stageYtdRateTwr: this.ytd.rate,
      stageInceptionProfit: this.inception.profit,
      stageInceptionRateTwr: this.inception.rate,
      stageLast7dProfit: this.last7.profit,
      stageLast7dRateTwr: this.last7.rate,
      stageLast30dProfit: this.last30.profit,
      stageLast30dRateTwr: this.last30.rate,
      stageLast90dProfit: this.last90.profit,
      stageLast90dRateTwr: this.last90.rate,
    };
  }

  applyMwr(mwrPatch) {
    return { ...this.snapshotTwr(), ...mwrPatch };
  }
}

/** 从昨日快照行恢复 stage 状态；mtd/ytd 仅在行日期与目标日同月/同年时继承。 */
function hydrateStageAccFromRow(stageAcc, row, targetDateKey) {
  if (!row) return;
  const dk = String(targetDateKey || "").slice(0, 10);
  const rowDate = String(row.date || dk).slice(0, 10);
  if (!rowDate) return;

  stageAcc.curMonth = monthStartKeyShanghai(rowDate);
  stageAcc.curYear = yearStartKeyShanghai(rowDate);

  stageAcc.inception.profit = Number(row.stageInceptionProfit) || 0;
  stageAcc.inception.rate = Number(row.stageInceptionRateTwr) || 0;
  stageAcc.last7.profit = Number(row.stageLast7dProfit) || 0;
  stageAcc.last7.rate = Number(row.stageLast7dRateTwr) || 0;
  stageAcc.last30.profit = Number(row.stageLast30dProfit) || 0;
  stageAcc.last30.rate = Number(row.stageLast30dRateTwr) || 0;
  stageAcc.last90.profit = Number(row.stageLast90dProfit) || 0;
  stageAcc.last90.rate = Number(row.stageLast90dRateTwr) || 0;

  const rowMonth = monthStartKeyShanghai(rowDate);
  const rowYear = yearStartKeyShanghai(rowDate);
  if (rowMonth === monthStartKeyShanghai(dk)) {
    stageAcc.mtd.profit = Number(row.stageMtdProfit) || 0;
    stageAcc.mtd.rate = Number(row.stageMtdRateTwr) || 0;
  }
  if (rowYear === yearStartKeyShanghai(dk)) {
    stageAcc.ytd.profit = Number(row.stageYtdProfit) || 0;
    stageAcc.ytd.rate = Number(row.stageYtdRateTwr) || 0;
  }
}

/** 空仓/无成交日：用 0 收益推进日历，确保 mtd/ytd/滑窗在跨月跨年间隙正确重置。 */
function advanceStageAccSessionGap(stageAcc, lastRowDate, targetDateKey) {
  const last = String(lastRowDate || "").slice(0, 10);
  const target = String(targetDateKey || "").slice(0, 10);
  if (!last || !target || last >= target) return;

  const { enumerateFreezeSessionDates, previousSessionDate } = require("./freeze-calendar");
  const gapEnd = previousSessionDate(target);
  if (!gapEnd || gapEnd <= last) return;

  const gapStart = addCalendarDays(last, 1);
  for (const d of enumerateFreezeSessionDates(gapStart, gapEnd)) {
    stageAcc.onDay(d, 0, 0);
  }
}

function windowStartForStage(stageKey, asOf, firstTrade) {
  const R = String(asOf).slice(0, 10);
  if (stageKey === "mtd") {
    return monthStartKeyShanghai(R);
  }
  if (stageKey === "ytd") {
    return yearStartKeyShanghai(R);
  }
  if (stageKey === "inception") {
    const ft = String(firstTrade || R).slice(0, 10);
    return ft <= R ? ft : R;
  }
  if (stageKey === "last_7d") {
    return addCalendarDays(R, -6);
  }
  if (stageKey === "last_30d") {
    return addCalendarDays(R, -29);
  }
  if (stageKey === "last_90d") {
    return addCalendarDays(R, -89);
  }
  return R;
}

module.exports = {
  StageAccumulator,
  chainTwrRate,
  windowStartForStage,
  hydrateStageAccFromRow,
  advanceStageAccSessionGap,
};
