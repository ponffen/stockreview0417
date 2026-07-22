function parseHttpEvent(event) {
  const base = { method: "GET", path: "/", query: {}, headers: {} };
  if (!event) {
    return base;
  }
  try {
    const rawText = Buffer.isBuffer(event) ? event.toString("utf8") : String(event);
    const parsed = JSON.parse(rawText);
    const method = String(parsed?.httpMethod || parsed?.requestContext?.http?.method || "GET").toUpperCase();
    const path = String(parsed?.path || parsed?.rawPath || parsed?.requestContext?.http?.path || "/");
    const query =
      parsed?.queryParameters && typeof parsed.queryParameters === "object"
        ? parsed.queryParameters
        : parsed?.queries && typeof parsed.queries === "object"
          ? parsed.queries
          : {};
    const headers =
      parsed?.headers && typeof parsed.headers === "object"
        ? parsed.headers
        : parsed?.requestContext?.http?.headers && typeof parsed.requestContext.http.headers === "object"
          ? parsed.requestContext.http.headers
          : {};
    return { method, path, query, headers };
  } catch {
    return base;
  }
}

function jsonResponse(statusCode, bodyObj, extraHeaders = {}) {
  return {
    statusCode,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,OPTIONS",
      "access-control-allow-headers": "content-type,authorization",
      ...extraHeaders,
    },
    body: JSON.stringify(bodyObj),
    isBase64Encoded: false,
  };
}

function textResponse(statusCode, text, contentType = "text/plain; charset=utf-8", extraHeaders = {}) {
  return {
    statusCode,
    headers: {
      "content-type": contentType,
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
      ...extraHeaders,
    },
    body: String(text || ""),
    isBase64Encoded: false,
  };
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

module.exports = {
  parseHttpEvent,
  jsonResponse,
  textResponse,
  headerValue,
};
