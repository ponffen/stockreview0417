const { readUserIdFromRequest } = require("../auth-session");
const { getUserValidUntil } = require("../db");
const { isSubscriptionExpired } = require("../user-subscription");
const {
  MCP_CONNECTOR_NAME,
  CHATGPT_NEW_CHAT_URL,
  CHATGPT_CONNECT_URL,
  claudeInstallDeepLink,
  mcpResourceUrl,
} = require("./config");
const {
  hasActiveClaudeConnection,
  hasActiveChatGptConnection,
  revokeClaudeConnection,
  revokeChatGptConnection,
} = require("./oauth-store");
const { sendJson, getQuery } = require("./http-utils");

async function resolveSessionUser(req) {
  const userId = readUserIdFromRequest(req);
  if (!userId) {
    return { ok: false, status: 401, error: "请先登录" };
  }
  const validUntil = await getUserValidUntil(userId);
  if (isSubscriptionExpired(validUntil)) {
    return { ok: false, status: 403, error: "免费使用已结束", code: "subscription_expired" };
  }
  return { ok: true, userId };
}

function buildProviderPayload({ claudeStatus, chatgptStatus, mcpUrl }) {
  return {
    mcpUrl,
    connectorName: MCP_CONNECTOR_NAME,
    claude: {
      connected: !!claudeStatus.connected,
      expiresAt: claudeStatus.expiresAt || null,
      installDeepLink: claudeInstallDeepLink(mcpUrl),
      newChatUrl: "https://claude.ai/new",
    },
    chatgpt: {
      connected: !!chatgptStatus.connected,
      expiresAt: chatgptStatus.expiresAt || null,
      connectUrl: CHATGPT_CONNECT_URL,
      newChatUrl: CHATGPT_NEW_CHAT_URL,
    },
  };
}

async function handleConnectionStatus(req, res) {
  if (req.method !== "GET") {
    sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
    return;
  }
  const gate = await resolveSessionUser(req);
  if (!gate.ok) {
    sendJson(res, gate.status, { ok: false, error: gate.error, code: gate.code || null });
    return;
  }
  const [claudeStatus, chatgptStatus] = await Promise.all([
    hasActiveClaudeConnection(gate.userId),
    hasActiveChatGptConnection(gate.userId),
  ]);
  const mcpUrl = mcpResourceUrl(req);
  sendJson(res, 200, {
    ok: true,
    data: buildProviderPayload({ claudeStatus, chatgptStatus, mcpUrl }),
  });
}

function resolveRevokeProvider(req) {
  const q = getQuery(req);
  const raw = String(q.get("provider") || "").trim().toLowerCase();
  if (raw === "claude" || raw === "chatgpt") {
    return raw;
  }
  return "claude";
}

async function handleConnectionRevoke(req, res) {
  if (req.method !== "DELETE") {
    sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
    return;
  }
  const gate = await resolveSessionUser(req);
  if (!gate.ok) {
    sendJson(res, gate.status, { ok: false, error: gate.error, code: gate.code || null });
    return;
  }
  const provider = resolveRevokeProvider(req);
  if (provider === "chatgpt") {
    await revokeChatGptConnection(gate.userId);
  } else {
    await revokeClaudeConnection(gate.userId);
  }
  sendJson(res, 200, { ok: true, provider });
}

module.exports = {
  handleConnectionStatus,
  handleConnectionRevoke,
  buildProviderPayload,
};
