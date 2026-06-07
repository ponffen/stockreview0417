/** 全局 stage 枚举与窗口解析（Asia/Shanghai 自然日）。 */

const ALL_STAGES = ["today", "last_7d", "last_30d", "last_90d", "mtd", "ytd", "inception"];

const HOME_STAGE_TO_API = {
  month: "mtd",
  ytd: "ytd",
  total: "inception",
};

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
  return `${d.getFullYear()}-01-01`;
}

function windowNaturalDays(asOf, len) {
  const end = String(asOf).slice(0, 10);
  const start = addCalendarDays(end, -(len - 1));
  return { start, end };
}

function resolveStageRange(stage, asOf, firstDataDate, customRange = null) {
  const key = String(stage || "").trim();
  const R = String(asOf).slice(0, 10);
  if (key === "custom") {
    const from = String(customRange?.from || "").slice(0, 10);
    const to = String(customRange?.to || "").slice(0, 10);
    if (from && to) {
      return from <= to ? { start: from, end: to } : { start: to, end: from };
    }
    return { start: R, end: R };
  }
  if (key === "today") {
    return { start: R, end: R };
  }
  if (key === "last_7d") {
    return windowNaturalDays(R, 7);
  }
  if (key === "last_30d") {
    return windowNaturalDays(R, 30);
  }
  if (key === "last_90d") {
    return windowNaturalDays(R, 90);
  }
  if (key === "mtd") {
    return { start: monthStartKeyShanghai(R), end: R };
  }
  if (key === "ytd") {
    return { start: yearStartKeyShanghai(R), end: R };
  }
  if (key === "inception") {
    const s =
      firstDataDate && String(firstDataDate).slice(0, 10) <= R ? String(firstDataDate).slice(0, 10) : R;
    return { start: s, end: R };
  }
  return { start: R, end: R };
}

function parseStagesParam(stagesRaw) {
  if (stagesRaw == null || String(stagesRaw).trim() === "") {
    return [...ALL_STAGES];
  }
  const out = [];
  for (const p of String(stagesRaw).split(",")) {
    const k = String(p || "").trim();
    if (ALL_STAGES.includes(k) && !out.includes(k)) {
      out.push(k);
    }
  }
  return out.length ? out : [...ALL_STAGES];
}

function homeUiStageToApi(stageRange) {
  const k = String(stageRange || "month").trim();
  return HOME_STAGE_TO_API[k] || "mtd";
}

/** 阶段起点是否晚于冻结日：新自然月/新年等，不得沿用冻结快照里的阶段累计字段。 */
function isFreshStagePeriod(stageStart, frozenThrough) {
  const s = String(stageStart || "").slice(0, 10);
  const ft = String(frozenThrough || "").slice(0, 10);
  if (!s || !ft) {
    return false;
  }
  return s > ft;
}

function stageUsesFrozenCumulativeFields(stageKey) {
  const k = String(stageKey || "").trim();
  return k === "mtd" || k === "ytd" || k === "inception";
}

module.exports = {
  ALL_STAGES,
  HOME_STAGE_TO_API,
  homeUiStageToApi,
  addCalendarDays,
  monthStartKeyShanghai,
  yearStartKeyShanghai,
  resolveStageRange,
  parseStagesParam,
  isFreshStagePeriod,
  stageUsesFrozenCumulativeFields,
};
