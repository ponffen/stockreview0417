function getQuery(req) {
  try {
    const u = new URL(String(req.url || "/"), "http://localhost");
    return u.searchParams;
  } catch {
    return new URLSearchParams();
  }
}

async function readRequestBody(req) {
  if (req.body && typeof req.body === "object" && !Buffer.isBuffer(req.body)) {
    return req.body;
  }
  const bodyStr = await new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => (data += chunk));
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
  if (!bodyStr) {
    return {};
  }
  const contentType = String(req.headers?.["content-type"] || "").toLowerCase();
  if (contentType.includes("application/json")) {
    try {
      return JSON.parse(bodyStr);
    } catch {
      return {};
    }
  }
  if (contentType.includes("application/x-www-form-urlencoded")) {
    const params = new URLSearchParams(bodyStr);
    const out = {};
    for (const [k, v] of params.entries()) {
      out[k] = v;
    }
    return out;
  }
  try {
    return JSON.parse(bodyStr);
  } catch {
    return {};
  }
}

function sendJson(res, status, payload, headers = {}) {
  for (const [k, v] of Object.entries(headers)) {
    res.setHeader(k, v);
  }
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(JSON.stringify(payload));
}

function sendHtml(res, status, html, headers = {}) {
  for (const [k, v] of Object.entries(headers)) {
    res.setHeader(k, v);
  }
  res.statusCode = status;
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(html);
}

function sendText(res, status, text, headers = {}) {
  for (const [k, v] of Object.entries(headers)) {
    res.setHeader(k, v);
  }
  res.statusCode = status;
  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(text);
}

function extractBearerToken(req) {
  const auth = String(req.headers?.authorization || "").trim();
  if (!auth.toLowerCase().startsWith("bearer ")) {
    return "";
  }
  return auth.slice(7).trim();
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function clientAcceptsEventStream(req) {
  const accept = String(req.headers?.accept || "").toLowerCase();
  return accept.includes("text/event-stream");
}

/** Prefer JSON when client accepts both (Claude/ChatGPT often send both). */
function preferSseResponse(req) {
  const accept = String(req.headers?.accept || "").toLowerCase();
  if (!accept.includes("text/event-stream")) {
    return false;
  }
  return !accept.includes("application/json");
}

function sendSseJsonRpcMessages(res, messages, headers = {}) {
  for (const [k, v] of Object.entries(headers)) {
    res.setHeader(k, v);
  }
  res.statusCode = 200;
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-store");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  for (const message of messages) {
    res.write(`event: message\ndata: ${JSON.stringify(message)}\n\n`);
  }
  res.end();
}

function mcpCorsHeaders(req) {
  const origin = String(req.headers?.origin || "").trim();
  const headers = {
    "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Headers":
      "Authorization, Content-Type, Accept, Mcp-Session-Id, MCP-Session-Id, Last-Event-ID, MCP-Protocol-Version",
    "Access-Control-Max-Age": "86400",
  };
  if (origin) {
    headers["Access-Control-Allow-Origin"] = origin;
    headers.Vary = "Origin";
  } else {
    headers["Access-Control-Allow-Origin"] = "*";
  }
  return headers;
}

function sendOptions(res, headers = {}) {
  for (const [k, v] of Object.entries(headers)) {
    res.setHeader(k, v);
  }
  res.statusCode = 204;
  res.setHeader("Cache-Control", "no-store");
  res.end();
}

function startMcpSseStream(res, headers = {}, options = {}) {
  const maxDurationMs = Number(options.maxDurationMs) || 0;
  for (const [k, v] of Object.entries(headers)) {
    res.setHeader(k, v);
  }
  res.statusCode = 200;
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-store");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  res.write(": ok\n\n");

  const keepAliveMs = 25000;
  const interval = setInterval(() => {
    try {
      if (res.writableEnded) {
        clearInterval(interval);
        return;
      }
      res.write(": keepalive\n\n");
    } catch {
      clearInterval(interval);
    }
  }, keepAliveMs);

  let maxTimer = null;
  if (maxDurationMs > 0) {
    maxTimer = setTimeout(() => {
      try {
        if (!res.writableEnded) {
          res.write(": closing\n\n");
          res.end();
        }
      } catch {
        // ignore
      }
    }, maxDurationMs);
  }

  const cleanup = () => {
    clearInterval(interval);
    if (maxTimer) {
      clearTimeout(maxTimer);
    }
  };
  res.on("close", cleanup);
  res.on("finish", cleanup);
}

module.exports = {
  getQuery,
  readRequestBody,
  sendJson,
  sendHtml,
  sendText,
  extractBearerToken,
  escapeHtml,
  clientAcceptsEventStream,
  preferSseResponse,
  sendSseJsonRpcMessages,
  mcpCorsHeaders,
  sendOptions,
  startMcpSseStream,
};
