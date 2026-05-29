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
};
