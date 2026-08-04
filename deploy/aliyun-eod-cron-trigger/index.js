/**
 * 阿里云 FC 定时器：北京时间 08:00（周二～六）触发 Vercel EOD 流水线。
 * 环境变量：
 *   EOD_CRON_TARGET_URL — 默认 https://www.higcc.com/api/cron/freeze-eod
 *   CRON_SECRET — 与 Vercel 生产环境 CRON_SECRET 一致
 *   EOD_CRON_TIMEOUT_MS — 等待 Vercel 响应超时，默认 280000
 */
const DEFAULT_TARGET = "https://www.higcc.com/api/cron/freeze-eod";
const DEFAULT_TIMEOUT_MS = 280000;

function targetUrl() {
  return String(process.env.EOD_CRON_TARGET_URL || DEFAULT_TARGET).trim() || DEFAULT_TARGET;
}

function cronSecret() {
  return String(process.env.CRON_SECRET || process.env.VERCEL_CRON_SECRET || "").trim();
}

function timeoutMs() {
  const n = Number(process.env.EOD_CRON_TIMEOUT_MS || DEFAULT_TIMEOUT_MS);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_TIMEOUT_MS;
}

function isHttpEvent(event) {
  return !!(event?.requestContext || event?.headers || event?.httpMethod);
}

function httpResponse(statusCode, bodyObj) {
  return {
    statusCode,
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(bodyObj),
  };
}

async function invokeEodPipeline(meta = {}) {
  const secret = cronSecret();
  if (!secret) {
    throw new Error("CRON_SECRET is not configured on FC");
  }
  const url = targetUrl();
  const startedAt = Date.now();
  console.log("[eod-cron-trigger] invoke start", { url, ...meta });

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-cron-secret": secret,
    },
    body: JSON.stringify({
      source: "aliyun-fc-timer",
      triggeredAt: new Date().toISOString(),
    }),
    signal: AbortSignal.timeout(timeoutMs()),
  });

  const text = await response.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = { raw: text.slice(0, 500) };
  }

  const result = {
    ok: response.ok,
    status: response.status,
    elapsedMs: Date.now() - startedAt,
    body: parsed,
  };
  console.log("[eod-cron-trigger] invoke done", {
    status: result.status,
    elapsedMs: result.elapsedMs,
    ok: parsed?.ok,
    frozenDate: parsed?.data?.frozenDate,
    dailyCloseRows: parsed?.data?.dailyClose?.rowsWritten,
  });

  if (!response.ok) {
    const errMsg =
      parsed?.error ||
      parsed?.body?.error ||
      (typeof parsed?.raw === "string" ? parsed.raw : "") ||
      `HTTP ${response.status}`;
    throw new Error(String(errMsg).slice(0, 500));
  }
  return result;
}

exports.handler = async (event, context) => {
  if (isHttpEvent(event)) {
    const path = String(
      event?.requestContext?.http?.path || event?.path || event?.rawPath || "/",
    ).split("?")[0];
    if (path === "/health" || path === "/api/health") {
      return httpResponse(200, {
        ok: true,
        service: "eod-cron-trigger",
        targetUrl: targetUrl(),
        hasSecret: !!cronSecret(),
      });
    }
    if (path === "/run" && String(event?.requestContext?.http?.method || event?.httpMethod || "GET") === "POST") {
      const hdr = event?.headers || {};
      const provided =
        String(hdr["x-cron-secret"] || hdr["X-Cron-Secret"] || "").trim() ||
        String(hdr["authorization"] || "").replace(/^Bearer\s+/i, "").trim();
      if (!provided || provided !== cronSecret()) {
        return httpResponse(401, { ok: false, error: "unauthorized" });
      }
      const data = await invokeEodPipeline({ manual: true });
      return httpResponse(200, { ok: true, data });
    }
    return httpResponse(404, { ok: false, error: "not found" });
  }

  const triggerName = event?.triggerName || "timer";
  const data = await invokeEodPipeline({
    triggerName,
    triggerTime: event?.triggerTime || null,
  });
  return { ok: true, ...data };
};
