/**
 * 交易日历：自然日用于展示；「今日」盈亏/实时点以 08:30 北京切日的交易日期为准。
 */
const { getTradingDateKeyBy0830 } = require("../position-today-pnl");

function shanghaiYmd(now = new Date()) {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return fmt.format(now);
}

function isWeekendShanghai(now = new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Shanghai",
    weekday: "short",
  }).formatToParts(now);
  const wd = parts.find((p) => p.type === "weekday")?.value || "";
  return wd === "Sat" || wd === "Sun";
}

/** 交易日期（08:30 前算昨日）是否为周六日 */
function isWeekendSessionDate(sessionDateKey) {
  const d = new Date(`${String(sessionDateKey || "").slice(0, 10)}T12:00:00+08:00`);
  if (Number.isNaN(d.getTime())) {
    return false;
  }
  const day = d.getDay();
  return day === 0 || day === 6;
}

/**
 * 是否计「今日」盘中收益并走 tradingDay 路径。
 * 周六 00:00–08:29 交易日期仍为周五 → true；周六 08:30 起交易日期为周六 → false。
 */
function shouldEmitTodayLivePoint(now = new Date()) {
  const session = getTradingDateKeyBy0830(now);
  return !isWeekendSessionDate(session);
}

/** 当前交易日期（与持仓今日收益、liveDate 一致） */
function liveDateKeyShanghai(now = new Date()) {
  return getTradingDateKeyBy0830(now);
}

module.exports = {
  shanghaiYmd,
  isWeekendShanghai,
  isWeekendSessionDate,
  shouldEmitTodayLivePoint,
  liveDateKeyShanghai,
  getTradingDateKeyBy0830,
};
