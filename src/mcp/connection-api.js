const { readUserIdFromRequest } = require("../auth-session");
const { getUserValidUntil } = require("../db");
const { isSubscriptionExpired } = require("../user-subscription");
const { claudeInstallDeepLink, mcpResourceUrl } = require("./config");
const { hasActiveClaudeConnection, revokeClaudeConnection } = require("./oauth-store");
const { sendJson } = require("./http-utils");

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
  const status = await hasActiveClaudeConnection(gate.userId);
  const mcpUrl = mcpResourceUrl(req);
  sendJson(res, 200, {
    ok: true,
    data: {
      connected: !!status.connected,
      expiresAt: status.expiresAt || null,
      mcpUrl,
      installDeepLink: claudeInstallDeepLink(mcpUrl),
      claudeNewChatUrl: "https://claude.ai/new",
    },
  });
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
  await revokeClaudeConnection(gate.userId);
  sendJson(res, 200, { ok: true });
}

module.exports = {
  handleConnectionStatus,
  handleConnectionRevoke,
};
