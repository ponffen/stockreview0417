const { verifyAccessToken } = require("./oauth-tokens");
const { TOOL_DEFS, callMcpTool } = require("./tools");
const { mcpResourceUrl, DEFAULT_SCOPE } = require("./config");
const {
  extractBearerToken,
  readRequestBody,
  sendJson,
  clientAcceptsEventStream,
  preferSseResponse,
  sendSseJsonRpcMessages,
  mcpCorsHeaders,
  sendOptions,
  startMcpSseStream,
} = require("./http-utils");
const {
  MCP_SUBSCRIPTION_EXPIRED_MESSAGE,
  assertMcpUserActive,
  isMcpSubscriptionExpiredError,
} = require("./subscription-gate");

const SERVER_INFO = { name: "麻雀", version: "1.0.0" };
const DEFAULT_PROTOCOL_VERSION = "2025-03-26";
const SUPPORTED_PROTOCOL_VERSIONS = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const VERCEL_SSE_MAX_MS = 55_000;
const { randomUUID } = require("crypto");

function negotiateProtocolVersion(clientRequested) {
  const requested = String(clientRequested || "").trim();
  if (requested && SUPPORTED_PROTOCOL_VERSIONS.includes(requested)) {
    return requested;
  }
  return DEFAULT_PROTOCOL_VERSION;
}

function readMcpProtocolVersion(req, messages = []) {
  const headerVersion = String(req.headers?.["mcp-protocol-version"] || req.headers?.["MCP-Protocol-Version"] || "").trim();
  if (headerVersion) {
    return headerVersion;
  }
  for (const message of messages) {
    if (String(message?.method || "") === "initialize") {
      const bodyVersion = String(message?.params?.protocolVersion || "").trim();
      if (bodyVersion) {
        return bodyVersion;
      }
    }
  }
  return "";
}

function extractSessionId(req, hasInitialize) {
  const incoming = String(req.headers?.["mcp-session-id"] || req.headers?.["MCP-Session-Id"] || "").trim();
  if (hasInitialize) {
    return incoming || randomUUID();
  }
  return incoming;
}

function normalizeResourceUrl(value) {
  try {
    const u = new URL(String(value || "").trim());
    u.hash = "";
    if ((u.protocol === "https:" && u.port === "443") || (u.protocol === "http:" && u.port === "80")) {
      u.port = "";
    }
    const path = u.pathname.replace(/\/+$/, "") || "";
    return `${u.protocol}//${u.hostname.toLowerCase()}${u.port ? `:${u.port}` : ""}${path}`;
  } catch {
    return String(value || "").trim().replace(/\/+$/, "");
  }
}
function mcpAuthChallengeHeaders(req) {
  const meta = `${getPublicBaseUrl(req)}/.well-known/oauth-protected-resource/mcp`;
  return {
    "WWW-Authenticate": `Bearer realm="maque", resource_metadata="${meta}", scope="${DEFAULT_SCOPE}"`,
  };
}

function getPublicBaseUrl(req) {
  const { getPublicBaseUrl: base } = require("./config");
  return base(req);
}

function rpcResult(id, result) {
  return { jsonrpc: "2.0", id, result };
}

function rpcError(id, code, message, data) {
  const out = { jsonrpc: "2.0", id, error: { code, message } };
  if (data != null) {
    out.error.data = data;
  }
  return out;
}

async function dispatchMcpMethod(viewerId, method, params) {
  if (method === "initialize") {
    const protocolVersion = negotiateProtocolVersion(params?.protocolVersion);
    return {
      protocolVersion,
      capabilities: { tools: { listChanged: false } },
      serverInfo: SERVER_INFO,
    };
  }
  if (method === "notifications/initialized" || method === "initialized") {
    return null;
  }
  if (method === "tools/list") {
    return { tools: TOOL_DEFS };
  }
  if (method === "tools/call") {
    const name = String(params?.name || "").trim();
    const args = params?.arguments && typeof params.arguments === "object" ? params.arguments : {};
    try {
      const data = await callMcpTool(viewerId, name, args);
      return {
        content: [{ type: "text", text: JSON.stringify(data, null, 2) }],
        isError: false,
      };
    } catch (error) {
      if (isMcpSubscriptionExpiredError(error)) {
        return {
          content: [{ type: "text", text: MCP_SUBSCRIPTION_EXPIRED_MESSAGE }],
          isError: true,
        };
      }
      const status = Number(error?.status) || 500;
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({
              error: error?.message || "tool failed",
              status,
            }),
          },
        ],
        isError: true,
      };
    }
  }
  if (method === "ping") {
    return {};
  }
  const err = new Error(`Method not found: ${method}`);
  err.code = -32601;
  throw err;
}

async function handleSingleMcpMessage(viewerId, message) {
  if (!message || typeof message !== "object") {
    return rpcError(null, -32600, "Invalid Request");
  }
  const { jsonrpc, method, id, params } = message;
  if (jsonrpc !== "2.0" || !method) {
    return rpcError(id ?? null, -32600, "Invalid Request");
  }
  if (id == null) {
    try {
      await dispatchMcpMethod(viewerId, method, params);
    } catch {
      // notifications: no response
    }
    return null;
  }
  try {
    const result = await dispatchMcpMethod(viewerId, method, params);
    if (result == null && (method === "notifications/initialized" || method === "initialized")) {
      return null;
    }
    return rpcResult(id, result);
  } catch (error) {
    if (isMcpSubscriptionExpiredError(error)) {
      return rpcError(id, -32003, MCP_SUBSCRIPTION_EXPIRED_MESSAGE);
    }
    const code = Number(error?.code) || -32603;
    return rpcError(id, code, error?.message || "Internal error");
  }
}

async function handleMcpRequest(req, res) {
  try {
    await handleMcpRequestInner(req, res);
  } catch (error) {
    console.error("[mcp] unhandled request error:", error);
    if (!res.headersSent) {
      sendJson(res, 500, {
        jsonrpc: "2.0",
        error: { code: -32603, message: "Internal error" },
        id: null,
      });
    }
  }
}

async function handleMcpRequestInner(req, res) {
  const cors = mcpCorsHeaders(req);

  if (req.method === "OPTIONS") {
    sendOptions(res, cors);
    return;
  }

  if (req.method === "DELETE") {
    res.statusCode = 200;
    for (const [k, v] of Object.entries(cors)) {
      res.setHeader(k, v);
    }
    res.setHeader("Cache-Control", "no-store");
    res.end();
    return;
  }

  const token = extractBearerToken(req);
  const auth = token ? verifyAccessToken(token) : null;
  const expectedResource = mcpResourceUrl(req);
  if (!auth?.userId) {
    sendJson(
      res,
      401,
      {
        jsonrpc: "2.0",
        error: { code: -32001, message: "Unauthorized" },
        id: null,
      },
      { ...cors, ...mcpAuthChallengeHeaders(req) }
    );
    return;
  }
  if (auth.resource && normalizeResourceUrl(auth.resource) !== normalizeResourceUrl(expectedResource)) {
    sendJson(
      res,
      401,
      {
        jsonrpc: "2.0",
        error: { code: -32001, message: "Unauthorized" },
        id: null,
      },
      { ...cors, ...mcpAuthChallengeHeaders(req) }
    );
    return;
  }

  const gate = await assertMcpUserActive(auth.userId);
  if (!gate.ok) {
    const status = gate.status || 403;
    const challenge = { ...cors, ...mcpAuthChallengeHeaders(req) };
    if (req.method === "GET") {
      sendJson(
        res,
        status,
        {
          ok: false,
          error: gate.error,
          code: gate.code || null,
        },
        challenge,
      );
      return;
    }
    sendJson(
      res,
      status,
      {
        jsonrpc: "2.0",
        error: { code: -32003, message: gate.error },
        id: null,
      },
      challenge,
    );
    return;
  }

  if (req.method === "GET") {
    if (!clientAcceptsEventStream(req)) {
      sendJson(
        res,
        405,
        {
          jsonrpc: "2.0",
          error: { code: -32000, message: "Method Not Allowed" },
          id: null,
        },
        { ...cors, Allow: "GET, POST, DELETE, OPTIONS" }
      );
      return;
    }
    const sessionId = String(req.headers?.["mcp-session-id"] || req.headers?.["MCP-Session-Id"] || "").trim();
    console.log("[mcp] GET sse", {
      userId: auth.userId,
      sessionId: sessionId || null,
      vercel: !!process.env.VERCEL,
    });
    const sseMaxMs = process.env.VERCEL ? VERCEL_SSE_MAX_MS : 0;
    startMcpSseStream(res, cors, { maxDurationMs: sseMaxMs });
    return;
  }

  if (req.method !== "POST") {
    sendJson(res, 405, { ok: false, error: "Method Not Allowed" }, { ...cors, Allow: "GET, POST, DELETE, OPTIONS" });
    return;
  }

  const body = await readRequestBody(req);
  const messages = Array.isArray(body) ? body : [body];
  const rpcMethods = messages.map((m) => String(m?.method || "").trim()).filter(Boolean);
  const hasInitialize = rpcMethods.includes("initialize");
  const sessionId = extractSessionId(req, hasInitialize);
  const sessionHeader = sessionId ? { "Mcp-Session-Id": sessionId } : {};
  const protocolVersion = readMcpProtocolVersion(req, messages);
  const toolCalls = messages
    .filter((m) => String(m?.method || "") === "tools/call")
    .map((m) => String(m?.params?.name || "").trim())
    .filter(Boolean);
  console.log("[mcp] POST", {
    userId: auth.userId,
    rpcMethods,
    toolCalls: toolCalls.length ? toolCalls : null,
    sessionId: sessionId || null,
    protocolVersion: protocolVersion || null,
    accept: String(req.headers?.accept || "").slice(0, 120),
    responseMode: preferSseResponse(req) ? "sse" : "json",
  });

  const responses = [];
  for (const message of messages) {
    const out = await handleSingleMcpMessage(auth.userId, message);
    if (out != null) {
      responses.push(out);
    }
  }
  if (!responses.length) {
    res.statusCode = 202;
    for (const [k, v] of Object.entries({ ...cors, ...sessionHeader })) {
      res.setHeader(k, v);
    }
    res.setHeader("Cache-Control", "no-store");
    res.end();
    return;
  }
  if (preferSseResponse(req)) {
    sendSseJsonRpcMessages(res, responses, { ...cors, ...sessionHeader });
    return;
  }
  sendJson(res, 200, responses.length === 1 ? responses[0] : responses, { ...cors, ...sessionHeader });
}

module.exports = {
  handleMcpRequest,
};
