/** 指标重算用日期键（YYYY-MM-DD），无其它模块依赖，避免 invalidate ↔ rebuild 循环引用。 */

function normDateKey(d) {
  const s = String(d || "").slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

function minDateKey(dates) {
  let minD = null;
  for (const raw of dates || []) {
    const d = normDateKey(raw);
    if (!d) continue;
    if (!minD || d < minD) minD = d;
  }
  return minD;
}

module.exports = { normDateKey, minDateKey };
