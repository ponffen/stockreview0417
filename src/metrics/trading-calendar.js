/** 北京日历：周六日不出 today 点；工作日（含休市）可出点。 */

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

/** 是否应在 a/b/f/h 上展示「今日」实时点 */
function shouldEmitTodayLivePoint(now = new Date()) {
  return !isWeekendShanghai(now);
}

function liveDateKeyShanghai(now = new Date()) {
  return shanghaiYmd(now);
}

module.exports = {
  shanghaiYmd,
  isWeekendShanghai,
  shouldEmitTodayLivePoint,
  liveDateKeyShanghai,
};
