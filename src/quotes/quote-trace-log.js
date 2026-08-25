/**
 * 行情链路诊断日志：仅在 quoteError 或非预期 close-fallback 时输出。
 */
const crypto = require("node:crypto");

function maskUserId(userId) {
  const s = String(userId || "").trim();
  if (!s) {
    return "";
  }
  if (s.length <= 8) {
    return s;
  }
  return `${s.slice(0, 4)}…${s.slice(-4)}`;
}

function newTraceId() {
  return `qt_${Date.now().toString(36)}_${crypto.randomBytes(3).toString("hex")}`;
}

function createQuoteTrace(ctx = {}) {
  return {
    traceId: String(ctx.traceId || newTraceId()).trim() || newTraceId(),
    userId: maskUserId(ctx.userId),
    scope: String(ctx.scope || "all").trim() || "all",
    symbolCount: Number(ctx.symbolCount) || 0,
    tradingDay: !!ctx.tradingDay,
    liveDate: ctx.liveDate ? String(ctx.liveDate).slice(0, 10) : null,
    frozenThrough: ctx.frozenThrough ? String(ctx.frozenThrough).slice(0, 10) : null,
    startedAt: Date.now(),
    steps: [],
  };
}

function recordQuoteStep(trace, step, data = {}) {
  if (!trace || !step) {
    return;
  }
  trace.steps.push({
    step: String(step),
    at: Date.now(),
    ...data,
  });
}

function errorCause(error) {
  if (!error) {
    return null;
  }
  const cause = error.cause;
  if (cause && typeof cause === "object") {
    return {
      code: cause.code || null,
      message: cause.message || null,
    };
  }
  return null;
}

function sampleList(list, limit = 3) {
  const arr = Array.isArray(list) ? list.filter(Boolean) : [];
  if (!arr.length) {
    return [];
  }
  return arr.slice(0, limit);
}

function shouldEmitQuoteTrace(summary = {}) {
  if (String(summary.quoteError || "").trim()) {
    return true;
  }
  if (!summary.tradingDay) {
    return false;
  }
  const positionQuoteCount = Number(summary.positionQuoteCount) || 0;
  const liveQuoteCount = Number(summary.liveQuoteCount) || 0;
  const closeFallbackCount = Number(summary.closeFallbackCount) || 0;
  if (positionQuoteCount > 0 && liveQuoteCount === 0) {
    return true;
  }
  if (closeFallbackCount > 0 && summary.quoteOk === false) {
    return true;
  }
  return false;
}

function emitQuoteTrace(trace, summary = {}) {
  if (!trace || !shouldEmitQuoteTrace(summary)) {
    return;
  }
  const payload = {
    tag: "quote",
    traceId: trace.traceId,
    userId: trace.userId,
    scope: trace.scope,
    tradingDay: trace.tradingDay,
    liveDate: trace.liveDate,
    frozenThrough: trace.frozenThrough,
    symbolCount: trace.symbolCount,
    ms: Date.now() - Number(trace.startedAt || Date.now()),
    summary,
    steps: trace.steps,
  };
  console.warn("[quote-trace]", JSON.stringify(payload));
}

module.exports = {
  createQuoteTrace,
  recordQuoteStep,
  errorCause,
  sampleList,
  shouldEmitQuoteTrace,
  emitQuoteTrace,
};
