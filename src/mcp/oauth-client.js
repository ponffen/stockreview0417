const {
  DEFAULT_CLIENT_ID,
  CLAUDE_CIMD_CLIENT_ID,
  isClaudeOAuthClientId,
  isChatGptOAuthClientId,
} = require("./config");

const KNOWN_CLAUDE_CIMD = {
  clientId: CLAUDE_CIMD_CLIENT_ID,
  clientName: "Claude",
  redirectUris: ["https://claude.ai/api/mcp/auth_callback"],
  tokenEndpointAuthMethod: "none",
  grantTypes: ["authorization_code", "refresh_token"],
  responseTypes: ["code"],
  source: "known-cimd",
};

const CIMD_FETCH_TIMEOUT_MS = 8000;
const CIMD_CACHE_TTL_MS = 5 * 60 * 1000;
const cimdCache = new Map();

function isHttpsUrl(url) {
  try {
    return new URL(String(url || "")).protocol === "https:";
  } catch {
    return false;
  }
}

function isChatGptRedirectUri(uri) {
  try {
    const u = new URL(String(uri || ""));
    if (u.protocol !== "https:") {
      return false;
    }
    const host = u.hostname.toLowerCase();
    if (!/^(chatgpt\.com|openai\.com)$/.test(host)) {
      return false;
    }
    if (u.pathname.startsWith("/connector/oauth/")) {
      return true;
    }
    if (u.pathname === "/connector_platform_oauth_redirect") {
      return true;
    }
    return false;
  } catch {
    return false;
  }
}

function isClaudeRedirectUri(uri) {
  try {
    const u = new URL(String(uri || ""));
    if (u.protocol !== "https:") {
      return false;
    }
    const host = u.hostname.toLowerCase();
    return /claude\.ai|anthropic\.com/.test(host);
  } catch {
    return false;
  }
}

function isWorkBuddyRedirectUri(uri) {
  try {
    const u = new URL(String(uri || ""));
    if (u.protocol !== "workbuddy:") {
      return false;
    }
    const host = u.hostname.toLowerCase();
    if (host && host !== "workbuddy") {
      return false;
    }
    const path = u.pathname.toLowerCase();
    return path.includes("/mcp/") && path.endsWith("/oauth/callback");
  } catch {
    return false;
  }
}

function isAllowedOAuthRedirectUri(uri) {
  return isHttpsUrl(uri) || isWorkBuddyRedirectUri(uri);
}

function isAllowedDcrRedirectUri(uri) {
  return isChatGptRedirectUri(uri) || isClaudeRedirectUri(uri) || isWorkBuddyRedirectUri(uri);
}

function normalizeRedirectUris(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((item) => String(item || "").trim()).filter((item) => isAllowedOAuthRedirectUri(item));
}

function detectOAuthProvider({ clientId, redirectUris = [] }) {
  const id = String(clientId || "").trim();
  if (redirectUris.some(isWorkBuddyRedirectUri)) {
    return "workbuddy";
  }
  if (isChatGptOAuthClientId(id) || /^mcp-/.test(id) || redirectUris.some(isChatGptRedirectUri)) {
    return "chatgpt";
  }
  if (isClaudeOAuthClientId(id) || redirectUris.some(isClaudeRedirectUri)) {
    return "claude";
  }
  try {
    const u = new URL(id);
    if (u.pathname.replace(/\/+$/, "") === "/mcp") {
      return "claude";
    }
  } catch {
    // ignore
  }
  return "other";
}

function isLoopbackHost(hostname) {
  const host = String(hostname || "").toLowerCase();
  return host === "127.0.0.1" || host === "localhost" || host === "[::1]";
}

/** RFC 8252 §7.3: loopback redirect URIs match with port ignored. */
function loopbackRedirectUriMatch(redirectUri, allowedUri) {
  try {
    const a = new URL(String(redirectUri || ""));
    const b = new URL(String(allowedUri || ""));
    if (!isLoopbackHost(a.hostname) || !isLoopbackHost(b.hostname)) {
      return false;
    }
    return (
      a.protocol === b.protocol &&
      a.hostname.toLowerCase() === b.hostname.toLowerCase() &&
      a.pathname === b.pathname &&
      a.search === b.search
    );
  } catch {
    return false;
  }
}

function redirectUriAllowed(redirectUri, allowedUris) {
  const target = String(redirectUri || "").trim();
  if (!target) {
    return false;
  }
  return allowedUris.some((item) => item === target || loopbackRedirectUriMatch(target, item));
}

function buildCimdMetadata(url, json, source = "cimd") {
  return {
    clientId: url,
    clientName: String(json?.client_name || json?.client_id || "OAuth Client").trim() || "OAuth Client",
    redirectUris: normalizeRedirectUris(json?.redirect_uris),
    tokenEndpointAuthMethod: String(json?.token_endpoint_auth_method || "none").trim() || "none",
    grantTypes: Array.isArray(json?.grant_types) ? json.grant_types.map(String) : ["authorization_code", "refresh_token"],
    responseTypes: Array.isArray(json?.response_types) ? json.response_types.map(String) : ["code"],
    source,
  };
}

async function fetchCimdMetadata(clientIdUrl) {
  const url = String(clientIdUrl || "").trim();
  if (!isHttpsUrl(url)) {
    return null;
  }
  if (url === CLAUDE_CIMD_CLIENT_ID) {
    return { ...KNOWN_CLAUDE_CIMD };
  }
  const cached = cimdCache.get(url);
  if (cached && Date.now() - cached.at < CIMD_CACHE_TTL_MS) {
    return cached.value;
  }
  let metadata = null;
  try {
    const response = await fetch(url, {
      headers: { accept: "application/json" },
      signal: AbortSignal.timeout(CIMD_FETCH_TIMEOUT_MS),
    });
    if (response.ok) {
      const json = await response.json();
      metadata = buildCimdMetadata(url, json, "cimd");
    }
  } catch {
    // fall through to known-client fallback
  }
  if (!metadata && url === CLAUDE_CIMD_CLIENT_ID) {
    metadata = { ...KNOWN_CLAUDE_CIMD };
  }
  if (metadata) {
    cimdCache.set(url, { at: Date.now(), value: metadata });
  }
  return metadata;
}

async function resolveOAuthClient({ clientId, redirectUri, registeredClient = null }) {
  const id = String(clientId || "").trim();
  if (!id) {
    return { ok: false, error: "invalid_client", description: "缺少 client_id" };
  }

  if (id === DEFAULT_CLIENT_ID) {
    const redirectUris = normalizeRedirectUris(registeredClient?.redirectUris);
    if (redirectUri && redirectUris.length && !redirectUriAllowed(redirectUri, redirectUris)) {
      return { ok: false, error: "invalid_client", description: "redirect_uri 未注册" };
    }
    if (redirectUri && !isClaudeRedirectUri(redirectUri)) {
      return { ok: false, error: "invalid_client", description: "redirect_uri 不被允许" };
    }
    return {
      ok: true,
      client: {
        clientId: id,
        clientName: "Claude",
        redirectUris: redirectUris.length ? redirectUris : redirectUri ? [redirectUri] : [],
        provider: "claude",
        source: "static",
      },
    };
  }

  if (registeredClient) {
    const redirectUris = normalizeRedirectUris(registeredClient.redirectUris);
    if (redirectUri && !redirectUriAllowed(redirectUri, redirectUris)) {
      return { ok: false, error: "invalid_client", description: "redirect_uri 未注册" };
    }
    const provider = detectOAuthProvider({ clientId: id, redirectUris });
    return {
      ok: true,
      client: {
        clientId: id,
        clientName: String(registeredClient.clientName || "OAuth Client").trim() || "OAuth Client",
        redirectUris,
        provider,
        source: registeredClient.source || "dcr",
      },
    };
  }

  if (isHttpsUrl(id)) {
    let metadata;
    try {
      metadata = await fetchCimdMetadata(id);
    } catch {
      return { ok: false, error: "invalid_client", description: "无法获取 client_id 元数据" };
    }
    if (!metadata) {
      return { ok: false, error: "invalid_client", description: "client_id 元数据无效" };
    }
    if (redirectUri && !redirectUriAllowed(redirectUri, metadata.redirectUris)) {
      return { ok: false, error: "invalid_client", description: "redirect_uri 不在 client 元数据中" };
    }
    const provider = detectOAuthProvider({ clientId: id, redirectUris: metadata.redirectUris });
    return {
      ok: true,
      client: {
        clientId: id,
        clientName: metadata.clientName,
        redirectUris: metadata.redirectUris,
        provider,
        source: "cimd",
      },
    };
  }

  return { ok: false, error: "invalid_client", description: "未知 client_id" };
}

function providerLabel(provider) {
  if (provider === "chatgpt") {
    return "ChatGPT";
  }
  if (provider === "claude") {
    return "Claude";
  }
  if (provider === "workbuddy") {
    return "WorkBuddy";
  }
  return "客户端";
}

module.exports = {
  isHttpsUrl,
  isChatGptRedirectUri,
  isClaudeRedirectUri,
  isWorkBuddyRedirectUri,
  isAllowedOAuthRedirectUri,
  isAllowedDcrRedirectUri,
  normalizeRedirectUris,
  redirectUriAllowed,
  detectOAuthProvider,
  fetchCimdMetadata,
  resolveOAuthClient,
  providerLabel,
};
