const DEFAULT_SCOPE = "read:portfolio";
const DEFAULT_CLIENT_ID = "claude-mcp";
const ACCESS_TOKEN_TTL_SEC = 60 * 60; // 1h
const REFRESH_TOKEN_TTL_SEC = 60 * 60 * 24 * 90; // 90d
const AUTH_CODE_TTL_SEC = 10 * 60;

function getPublicBaseUrl(req) {
  const env = String(process.env.PUBLIC_BASE_URL || process.env.APP_BASE_URL || "").trim().replace(/\/+$/, "");
  if (env) {
    return env;
  }
  try {
    const host = String(req?.headers?.["x-forwarded-host"] || req?.headers?.host || "").trim();
    const proto = String(req?.headers?.["x-forwarded-proto"] || "https").split(",")[0].trim();
    if (host) {
      return `${proto}://${host}`;
    }
  } catch {
    // ignore
  }
  return "https://www.higcc.com";
}

function mcpResourceUrl(req) {
  return `${getPublicBaseUrl(req)}/mcp`;
}

function isAllowedOAuthClientId(clientId) {
  const id = String(clientId || "").trim();
  if (!id) {
    return false;
  }
  if (id === DEFAULT_CLIENT_ID) {
    return true;
  }
  try {
    const u = new URL(id);
    return u.protocol === "https:";
  } catch {
    return false;
  }
}

function claudeInstallDeepLink(mcpUrl = "https://www.higcc.com/mcp") {
  const params = new URLSearchParams({
    modal: "add-custom-connector",
    connectorName: "麻雀",
    connectorUrl: mcpUrl,
  });
  return `https://claude.ai/customize/connectors?${params.toString()}`;
}

module.exports = {
  DEFAULT_SCOPE,
  DEFAULT_CLIENT_ID,
  isAllowedOAuthClientId,
  ACCESS_TOKEN_TTL_SEC,
  REFRESH_TOKEN_TTL_SEC,
  AUTH_CODE_TTL_SEC,
  getPublicBaseUrl,
  mcpResourceUrl,
  claudeInstallDeepLink,
};
