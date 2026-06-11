const { getUserValidUntil } = require("../db");
const { isSubscriptionExpired } = require("../user-subscription");

const MCP_SUBSCRIPTION_EXPIRED_MESSAGE = "麻雀使用期已结束，请联系微信：ponffen。";
const MCP_SUBSCRIPTION_EXPIRED_CODE = "subscription_expired";

class McpSubscriptionExpiredError extends Error {
  constructor() {
    super(MCP_SUBSCRIPTION_EXPIRED_MESSAGE);
    this.name = "McpSubscriptionExpiredError";
    this.code = MCP_SUBSCRIPTION_EXPIRED_CODE;
    this.status = 403;
  }
}

function isMcpSubscriptionExpiredError(error) {
  return (
    error instanceof McpSubscriptionExpiredError ||
    error?.code === MCP_SUBSCRIPTION_EXPIRED_CODE
  );
}

/** Claude MCP/OAuth：每次请求查 users.valid_until；过期不删 refresh token。 */
async function assertMcpUserActive(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { ok: false, status: 401, error: "请先登录", code: "unauthorized" };
  }
  const validUntil = await getUserValidUntil(uid);
  if (isSubscriptionExpired(validUntil)) {
    return {
      ok: false,
      status: 403,
      error: MCP_SUBSCRIPTION_EXPIRED_MESSAGE,
      code: MCP_SUBSCRIPTION_EXPIRED_CODE,
    };
  }
  return { ok: true, userId: uid };
}

module.exports = {
  MCP_SUBSCRIPTION_EXPIRED_MESSAGE,
  MCP_SUBSCRIPTION_EXPIRED_CODE,
  McpSubscriptionExpiredError,
  isMcpSubscriptionExpiredError,
  assertMcpUserActive,
};
