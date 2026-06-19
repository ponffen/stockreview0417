/**
 * stage_*_trade_count 递推：mtd/ytd/inception 累加；last_7d/30d/90d 滑窗减项。
 */
const { addCalendarDays, monthStartKeyShanghai, yearStartKeyShanghai } = require("./stages");

function isTradeRecord(trade) {
  return String(trade?.type || "trade").trim() === "trade";
}

function countTradeRecordsOnDate(trades, dateKey) {
  const dk = String(dateKey || "").slice(0, 10);
  if (!dk) {
    return 0;
  }
  return (trades || []).filter(
    (t) => isTradeRecord(t) && String(t.date || t.trade_date || "").slice(0, 10) === dk,
  ).length;
}

function countTradeRecordsInRange(trades, startKey, endKey) {
  const a = String(startKey || "").slice(0, 10);
  const b = String(endKey || "").slice(0, 10);
  if (!a || !b || a > b) {
    return 0;
  }
  return (trades || []).filter((t) => {
    const d = String(t.date || t.trade_date || "").slice(0, 10);
    return isTradeRecord(t) && d >= a && d <= b;
  }).length;
}

class StageTradeCounter {
  constructor() {
    this.mtd = 0;
    this.ytd = 0;
    this.inception = 0;
    this.last7 = 0;
    this.last30 = 0;
    this.last90 = 0;
    this.countByDate = new Map();
    this.curMonth = "";
    this.curYear = "";
  }

  onDay(dateKey, dailyTradeCount) {
    const d = String(dateKey || "").slice(0, 10);
    const c = Math.max(0, Math.floor(Number(dailyTradeCount) || 0));
    const ms = monthStartKeyShanghai(d);
    const ys = yearStartKeyShanghai(d);

    if (ms !== this.curMonth) {
      this.mtd = 0;
      this.curMonth = ms;
    }
    if (ys !== this.curYear) {
      this.ytd = 0;
      this.curYear = ys;
    }

    this.mtd += c;
    this.ytd += c;
    this.inception += c;

    const slide = (field, days) => {
      const outKey = addCalendarDays(d, -days);
      const outC = this.countByDate.get(outKey) || 0;
      this[field] = this[field] + c - outC;
    };
    slide("last7", 7);
    slide("last30", 30);
    slide("last90", 90);

    this.countByDate.set(d, c);
  }

  snapshot() {
    return {
      stageMtdTradeCount: this.mtd,
      stageYtdTradeCount: this.ytd,
      stageInceptionTradeCount: this.inception,
      stageLast7dTradeCount: this.last7,
      stageLast30dTradeCount: this.last30,
      stageLast90dTradeCount: this.last90,
    };
  }
}

function hydrateStageTradeAccFromRow(stageAcc, row, targetDateKey) {
  if (!row || !stageAcc) {
    return;
  }
  const dk = String(targetDateKey || "").slice(0, 10);
  const rowDate = String(row.date || dk).slice(0, 10);
  if (!rowDate) {
    return;
  }

  stageAcc.curMonth = monthStartKeyShanghai(rowDate);
  stageAcc.curYear = yearStartKeyShanghai(rowDate);

  stageAcc.inception = Number(row.stageInceptionTradeCount ?? row.stage_inception_trade_count) || 0;
  stageAcc.last7 = Number(row.stageLast7dTradeCount ?? row.stage_last_7d_trade_count) || 0;
  stageAcc.last30 = Number(row.stageLast30dTradeCount ?? row.stage_last_30d_trade_count) || 0;
  stageAcc.last90 = Number(row.stageLast90dTradeCount ?? row.stage_last_90d_trade_count) || 0;

  const rowMonth = monthStartKeyShanghai(rowDate);
  const rowYear = yearStartKeyShanghai(rowDate);
  if (rowMonth === monthStartKeyShanghai(dk)) {
    stageAcc.mtd = Number(row.stageMtdTradeCount ?? row.stage_mtd_trade_count) || 0;
  }
  if (rowYear === yearStartKeyShanghai(dk)) {
    stageAcc.ytd = Number(row.stageYtdTradeCount ?? row.stage_ytd_trade_count) || 0;
  }
}

function advanceStageTradeAccSessionGap(stageAcc, lastRowDate, targetDateKey) {
  const last = String(lastRowDate || "").slice(0, 10);
  const target = String(targetDateKey || "").slice(0, 10);
  if (!last || !target || last >= target) {
    return;
  }

  const { enumerateFreezeSessionDates, previousSessionDate } = require("./freeze-calendar");
  const gapEnd = previousSessionDate(target);
  if (!gapEnd || gapEnd <= last) {
    return;
  }

  const gapStart = addCalendarDays(last, 1);
  for (const d of enumerateFreezeSessionDates(gapStart, gapEnd)) {
    stageAcc.onDay(d, 0);
  }
}

function stageTradeSnapFromRow(row) {
  if (!row) {
    return null;
  }
  return {
    stageMtdTradeCount: Number(row.stageMtdTradeCount ?? row.stage_mtd_trade_count) || 0,
    stageYtdTradeCount: Number(row.stageYtdTradeCount ?? row.stage_ytd_trade_count) || 0,
    stageInceptionTradeCount: Number(row.stageInceptionTradeCount ?? row.stage_inception_trade_count) || 0,
    stageLast7dTradeCount: Number(row.stageLast7dTradeCount ?? row.stage_last_7d_trade_count) || 0,
    stageLast30dTradeCount: Number(row.stageLast30dTradeCount ?? row.stage_last_30d_trade_count) || 0,
    stageLast90dTradeCount: Number(row.stageLast90dTradeCount ?? row.stage_last_90d_trade_count) || 0,
  };
}

/** 无成交日：stage 笔数累计字段沿用昨日冻结行；有成交日用 counter 快照。 */
function resolveTradeSnapForFreezeDay(dailyTradeCount, tradeSnap, yesterdayRow) {
  const daily = Math.max(0, Math.floor(Number(dailyTradeCount) || 0));
  if (daily > 0) {
    return tradeSnap;
  }
  const prev = stageTradeSnapFromRow(yesterdayRow);
  return prev || tradeSnap;
}

function stageTradeCountFromRow(row, stageKey) {
  const st = String(stageKey || "last_30d").trim() || "last_30d";
  if (!row) {
    return 0;
  }
  if (st === "today") {
    return Number(row.dailyTradeCount ?? row.daily_trade_count) || 0;
  }
  if (st === "mtd") {
    return Number(row.stageMtdTradeCount ?? row.stage_mtd_trade_count) || 0;
  }
  if (st === "ytd") {
    return Number(row.stageYtdTradeCount ?? row.stage_ytd_trade_count) || 0;
  }
  if (st === "inception") {
    return Number(row.stageInceptionTradeCount ?? row.stage_inception_trade_count) || 0;
  }
  if (st === "last_7d") {
    return Number(row.stageLast7dTradeCount ?? row.stage_last_7d_trade_count) || 0;
  }
  if (st === "last_30d") {
    return Number(row.stageLast30dTradeCount ?? row.stage_last_30d_trade_count) || 0;
  }
  if (st === "last_90d") {
    return Number(row.stageLast90dTradeCount ?? row.stage_last_90d_trade_count) || 0;
  }
  return Number(row.stageLast30dTradeCount ?? row.stage_last_30d_trade_count) || 0;
}

module.exports = {
  StageTradeCounter,
  isTradeRecord,
  countTradeRecordsOnDate,
  countTradeRecordsInRange,
  hydrateStageTradeAccFromRow,
  advanceStageTradeAccSessionGap,
  stageTradeSnapFromRow,
  resolveTradeSnapForFreezeDay,
  stageTradeCountFromRow,
};
