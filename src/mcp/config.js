const DEFAULT_SCOPE = "read:portfolio";
const DEFAULT_CLIENT_ID = "claude-mcp";
const CLAUDE_CIMD_CLIENT_ID = "https://claude.ai/oauth/mcp-oauth-client-metadata";
const MCP_CONNECTOR_NAME = "麻雀";
const CHATGPT_NEW_CHAT_URL = "https://chatgpt.com/";
const CHATGPT_CONNECT_URL = "https://chatgpt.com/apps#settings/Connectors";
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
    connectorName: MCP_CONNECTOR_NAME,
    connectorUrl: mcpUrl,
  });
  return `https://claude.ai/customize/connectors?${params.toString()}`;
}

function isClaudeOAuthClientId(clientId) {
  const id = String(clientId || "").trim();
  if (!id) {
    return false;
  }
  if (id === DEFAULT_CLIENT_ID) {
    return true;
  }
  const lower = id.toLowerCase();
  if (/claude\.ai|anthropic\.com/.test(lower)) {
    return true;
  }
  try {
    const host = new URL(id).hostname.toLowerCase();
    return /claude\.ai|anthropic\.com/.test(host);
  } catch {
    return false;
  }
}

function isChatGptOAuthClientId(clientId) {
  const id = String(clientId || "").trim();
  if (!id || isClaudeOAuthClientId(id)) {
    return false;
  }
  const lower = id.toLowerCase();
  if (/openai\.com|chatgpt\.com|chat\.openai\.com/.test(lower)) {
    return true;
  }
  try {
    const host = new URL(id).hostname.toLowerCase();
    return /openai\.com|chatgpt\.com/.test(host);
  } catch {
    return false;
  }
}

function isMcpResourceOAuthClientId(clientId) {
  const id = String(clientId || "").trim();
  try {
    const u = new URL(id);
    return u.pathname.replace(/\/+$/, "") === "/mcp";
  } catch {
    return false;
  }
}

function inferOAuthProvider(clientId) {
  const id = String(clientId || "").trim();
  if (!id) {
    return null;
  }
  if (isClaudeOAuthClientId(id)) {
    return "claude";
  }
  if (isChatGptOAuthClientId(id)) {
    return "chatgpt";
  }
  if (/^mcp-/.test(id)) {
    return "chatgpt";
  }
  if (isMcpResourceOAuthClientId(id)) {
    return "claude";
  }
  return null;
}

function sqlOAuthClientProviderClause(provider, defaultClientParamIndex = null) {
  if (provider === "claude") {
    const clientParam = Number(defaultClientParamIndex) || 3;
    return `(
      provider = 'claude'
      OR (
        COALESCE(provider, '') = ''
        AND (
          client_id = $${clientParam}
          OR client_id ILIKE '%claude.ai%'
          OR client_id ILIKE '%anthropic.com%'
          OR (
            client_id ~ '^https://[^/]+/mcp/?$'
            AND client_id NOT ILIKE '%openai.com%'
            AND client_id NOT ILIKE '%chatgpt.com%'
            AND client_id NOT LIKE 'mcp-%'
          )
        )
      )
    )`;
  }
  if (provider === "chatgpt") {
    return `(
      provider = 'chatgpt'
      OR (
        COALESCE(provider, '') = ''
        AND (
          client_id ILIKE '%openai.com%'
          OR client_id ILIKE '%chatgpt.com%'
          OR client_id LIKE 'mcp-%'
          OR EXISTS (
            SELECT 1 FROM mcp_oauth_client c
            WHERE c.client_id = mcp_oauth_refresh_token.client_id
          )
        )
      )
    )`;
  }
  return "FALSE";
}

module.exports = {
  DEFAULT_SCOPE,
  DEFAULT_CLIENT_ID,
  CLAUDE_CIMD_CLIENT_ID,
  MCP_CONNECTOR_NAME,
  CHATGPT_NEW_CHAT_URL,
  CHATGPT_CONNECT_URL,
  isAllowedOAuthClientId,
  isClaudeOAuthClientId,
  isChatGptOAuthClientId,
  isMcpResourceOAuthClientId,
  inferOAuthProvider,
  sqlOAuthClientProviderClause,
  ACCESS_TOKEN_TTL_SEC,
  REFRESH_TOKEN_TTL_SEC,
  AUTH_CODE_TTL_SEC,
  getPublicBaseUrl,
  mcpResourceUrl,
  claudeInstallDeepLink,
};
