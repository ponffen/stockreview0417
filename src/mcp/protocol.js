const { verifyAccessToken } = require("./oauth-tokens");
const { TOOL_DEFS, callMcpTool } = require("./tools");
const { mcpResourceUrl } = require("./config");
const { extractBearerToken, readRequestBody, sendJson } = require("./http-utils");

const SERVER_INFO = { name: "麻雀", version: "1.0.0" };
const PROTOCOL_VERSION = "2024-11-05";

function mcpAuthChallengeHeaders(req) {
  const meta = `${getPublicBaseUrl(req)}/.well-known/oauth-protected-resource`;
  return {
    "WWW-Authenticate": `Bearer realm="maque", resource_metadata="${meta}"`,
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
    return {
      protocolVersion: PROTOCOL_VERSION,
      capabilities: { tools: {} },
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
    const code = Number(error?.code) || -32603;
    return rpcError(id, code, error?.message || "Internal error");
  }
}

async function handleMcpRequest(req, res) {
  const token = extractBearerToken(req);
  const auth = token ? verifyAccessToken(token) : null;
  if (!auth?.userId) {
    sendJson(
      res,
      401,
      {
        jsonrpc: "2.0",
        error: { code: -32001, message: "Unauthorized" },
        id: null,
      },
      mcpAuthChallengeHeaders(req)
    );
    return;
  }

  if (req.method === "GET") {
    sendJson(res, 200, {
      ok: true,
      name: SERVER_INFO.name,
      resource: mcpResourceUrl(req),
      transport: "http",
    });
    return;
  }

  if (req.method !== "POST") {
    sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
    return;
  }

  const body = await readRequestBody(req);
  const messages = Array.isArray(body) ? body : [body];
  const responses = [];
  for (const message of messages) {
    const out = await handleSingleMcpMessage(auth.userId, message);
    if (out != null) {
      responses.push(out);
    }
  }
  if (!responses.length) {
    res.statusCode = 202;
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    res.end();
    return;
  }
  sendJson(res, 200, responses.length === 1 ? responses[0] : responses);
}

module.exports = {
  handleMcpRequest,
};
