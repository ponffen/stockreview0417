/**
 * 阿里云 FC 定时器：北京时间 08:00（周二～六）触发 Vercel EOD 流水线。
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

function parseHttpEvent(event) {
  const base = { method: "GET", path: "/", headers: {} };
  if (!event) {
    return base;
  }
  let parsed = event;
  if (Buffer.isBuffer(event)) {
    try {
      parsed = JSON.parse(event.toString("utf8"));
    } catch {
      return base;
    }
  } else if (typeof event === "string") {
    try {
      parsed = JSON.parse(event);
    } catch {
      return base;
    }
  }
  const method = String(
    parsed?.httpMethod || parsed?.requestContext?.http?.method || "GET",
  ).toUpperCase();
  const path = String(parsed?.path || parsed?.rawPath || parsed?.requestContext?.http?.path || "/");
  const headers =
    parsed?.headers && typeof parsed.headers === "object"
      ? parsed.headers
      : parsed?.requestContext?.http?.headers && typeof parsed.requestContext.http.headers === "object"
        ? parsed.requestContext.http.headers
        : {};
  return { method, path: path.split("?")[0], headers };
}

function headerValue(headers, name) {
  const want = String(name || "").toLowerCase();
  for (const [k, v] of Object.entries(headers || {})) {
    if (String(k).toLowerCase() === want) {
      return String(v || "").trim();
    }
  }
  return "";
}

function httpResponse(statusCode, bodyObj) {
  return {
    statusCode,
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(bodyObj),
    isBase64Encoded: false,
  };
}

function isTimerEvent(event) {
  if (!event || typeof event !== "object") {
    return false;
  }
  if (Buffer.isBuffer(event) || typeof event === "string") {
    return false;
  }
  return !!(event.triggerName || event.triggerTime || event.payload != null);
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

exports.handler = async (event) => {
  if (!isTimerEvent(event)) {
    const req = parseHttpEvent(event);
    if (req.path === "/health" || req.path === "/api/health") {
      return httpResponse(200, {
        ok: true,
        service: "eod-cron-trigger",
        targetUrl: targetUrl(),
        hasSecret: !!cronSecret(),
      });
    }
    if (req.path === "/run" && req.method === "POST") {
      const provided =
        headerValue(req.headers, "x-cron-secret") ||
        headerValue(req.headers, "authorization").replace(/^Bearer\s+/i, "").trim();
      if (!provided || provided !== cronSecret()) {
        return httpResponse(401, { ok: false, error: "unauthorized" });
      }
      const data = await invokeEodPipeline({ manual: true });
      return httpResponse(200, { ok: true, data });
    }
    return httpResponse(404, { ok: false, error: "not found", path: req.path });
  }

  const data = await invokeEodPipeline({
    triggerName: event?.triggerName || "timer",
    triggerTime: event?.triggerTime || null,
  });
  return { ok: true, ...data };
};
