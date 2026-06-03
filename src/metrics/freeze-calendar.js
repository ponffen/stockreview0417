/**
 * 冻结/指标用交易日：不写周六日快照；休市日用最近有效日 K；周末流水归入下一交易日。
 */
const { addCalendarDays } = require("./stages");

function normDateKey(d) {
  const s = String(d || "").slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

function shanghaiWeekday(dateKeyOrDate) {
  const dk = normDateKey(dateKeyOrDate);
  if (!dk) return -1;
  const d = new Date(`${dk}T12:00:00+08:00`);
  return Number.isNaN(d.getTime()) ? -1 : d.getDay();
}

function isWeekendDateKey(dateKey) {
  const wd = shanghaiWeekday(dateKey);
  return wd === 0 || wd === 6;
}

/** 冻结产出行：仅周一至周五自然日（休市日仍产行，价格用最近 K）。 */
function enumerateFreezeSessionDates(fromStr, toStr) {
  const out = [];
  const a = new Date(`${normDateKey(fromStr)}T12:00:00+08:00`);
  const b = new Date(`${normDateKey(toStr)}T12:00:00+08:00`);
  const cur = new Date(a);
  while (cur <= b) {
    const dk = normDateKey(cur.toISOString().slice(0, 10));
    if (dk && !isWeekendDateKey(dk)) {
      out.push(dk);
    }
    cur.setDate(cur.getDate() + 1);
  }
  return out;
}

/** 周六日流水 → 下周一（同一周末的多笔都归到周一）。 */
function ledgerSessionDateKey(naturalDate) {
  const d = normDateKey(naturalDate);
  if (!d) return "";
  if (!isWeekendDateKey(d)) return d;
  let cur = addCalendarDays(d, 1);
  while (isWeekendDateKey(cur)) {
    cur = addCalendarDays(cur, 1);
  }
  return cur;
}


/** 上一交易日（跳过周末）。 */
function previousSessionDate(dateKey) {
  let cur = addCalendarDays(normDateKey(dateKey), -1);
  while (cur && isWeekendDateKey(cur)) {
    cur = addCalendarDays(cur, -1);
  }
  return cur;
}

/** latest 之后、frozen 及之前的交易日列表（用于日终增量，不含 latest 本身）。 */
function sessionDatesAfterLatest(latestKey, frozenKey) {
  const latest = normDateKey(latestKey);
  const frozen = normDateKey(frozenKey);
  if (!latest || !frozen || latest >= frozen) {
    return [];
  }
  return enumerateFreezeSessionDates(addCalendarDays(latest, 1), frozen);
}

function hintDatesForRebuild(dates) {
  const out = [];
  for (const raw of dates || []) {
    const eff = ledgerSessionDateKey(raw);
    if (eff) out.push(eff);
  }
  return out;
}

/** Vercel 日终 cron：周日、周一早上（北京）跳过；周六早上照常（冻周五）。 */
function shouldSkipScheduledFreezeCron(now = new Date()) {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Shanghai",
    weekday: "short",
  });
  const wd = fmt.format(now);
  return wd === "Sun" || wd === "Mon";
}

/** meta 冻结日不得晚于库里最后一条快照（周六日不写行时 meta 可能是「日历昨日」）。 */
function capFrozenThroughToSnapshot(metaFrozen, snapshotDate) {
  const m = normDateKey(metaFrozen);
  const s = normDateKey(snapshotDate);
  if (!s) {
    return m;
  }
  if (!m) {
    return s;
  }
  return m > s ? s : m;
}

function forwardFillFxMap(fxMap, dateKeys, fallback) {
  const sorted = [...(dateKeys || [])].sort();
  let last = Number(fallback) || 0;
  for (const dk of sorted) {
    const v = Number(fxMap[dk]);
    if (Number.isFinite(v) && v > 0) {
      last = v;
      fxMap[dk] = v;
    } else {
      fxMap[dk] = last;
    }
  }
}

module.exports = {
  normDateKey,
  isWeekendDateKey,
  enumerateFreezeSessionDates,
  previousSessionDate,
  sessionDatesAfterLatest,
  ledgerSessionDateKey,
  hintDatesForRebuild,
  shouldSkipScheduledFreezeCron,
  forwardFillFxMap,
  capFrozenThroughToSnapshot,
};
