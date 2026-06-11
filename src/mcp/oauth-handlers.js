const { readUserIdFromRequest } = require("../auth-session");
const {
  MCP_SUBSCRIPTION_EXPIRED_CODE,
  MCP_SUBSCRIPTION_EXPIRED_MESSAGE,
  assertMcpUserActive,
} = require("./subscription-gate");
const { maskPhone } = require("../community-service");
const { getAuthSessionUserPayload } = require("../db");
const {
  DEFAULT_SCOPE,
  DEFAULT_CLIENT_ID,
  isAllowedOAuthClientId,
  getPublicBaseUrl,
  mcpResourceUrl,
} = require("./config");
const {
  randomToken,
  saveAuthCode,
  consumeAuthCode,
  saveRefreshToken,
  findRefreshToken,
  verifyPkce,
  ACCESS_TOKEN_TTL_SEC,
  REFRESH_TOKEN_TTL_SEC,
  AUTH_CODE_TTL_SEC,
} = require("./oauth-store");
const { signAccessToken } = require("./oauth-tokens");
const { getQuery, readRequestBody, sendJson, sendHtml, escapeHtml } = require("./http-utils");

function isHttpsUrl(url) {
  try {
    const u = new URL(String(url || ""));
    return u.protocol === "https:";
  } catch {
    return false;
  }
}

function oauthError(res, status, error, description = "") {
  sendJson(res, status, {
    error,
    error_description: description || error,
  });
}

async function assertOAuthUser(req) {
  const userId = readUserIdFromRequest(req);
  if (!userId) {
    return { ok: false, status: 401, error: "请先登录" };
  }
  return assertMcpUserActive(userId);
}

function handleWellKnownProtectedResource(req, res) {
  const base = getPublicBaseUrl(req);
  sendJson(res, 200, {
    resource: mcpResourceUrl(req),
    authorization_servers: [base],
    scopes_supported: [DEFAULT_SCOPE],
    bearer_methods_supported: ["header"],
  });
}

function handleWellKnownAuthServer(req, res) {
  const base = getPublicBaseUrl(req);
  sendJson(res, 200, {
    issuer: base,
    authorization_endpoint: `${base}/oauth/authorize`,
    token_endpoint: `${base}/oauth/token`,
    response_types_supported: ["code"],
    grant_types_supported: ["authorization_code", "refresh_token"],
    code_challenge_methods_supported: ["S256"],
    token_endpoint_auth_methods_supported: ["none"],
    client_id_metadata_document_supported: true,
    scopes_supported: [DEFAULT_SCOPE],
  });
}

async function handleOAuthAuthorize(req, res) {
  if (req.method !== "GET") {
    sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
    return;
  }
  const q = getQuery(req);
  const responseType = String(q.get("response_type") || "").trim();
  const clientId = String(q.get("client_id") || "").trim();
  const redirectUri = String(q.get("redirect_uri") || "").trim();
  const codeChallenge = String(q.get("code_challenge") || "").trim();
  const challengeMethod = String(q.get("code_challenge_method") || "S256").trim();
  const scope = String(q.get("scope") || DEFAULT_SCOPE).trim() || DEFAULT_SCOPE;
  const state = String(q.get("state") || "").trim();

  if (responseType !== "code") {
    oauthError(res, 400, "unsupported_response_type", "仅支持 response_type=code");
    return;
  }
  if (!isAllowedOAuthClientId(clientId)) {
    oauthError(res, 400, "invalid_client", "未知 client_id");
    return;
  }
  if (!redirectUri || !isHttpsUrl(redirectUri)) {
    oauthError(res, 400, "invalid_request", "redirect_uri 必须为 https");
    return;
  }
  if (!codeChallenge || challengeMethod !== "S256") {
    oauthError(res, 400, "invalid_request", "需要 PKCE S256");
    return;
  }

  const gate = await assertOAuthUser(req);
  if (!gate.ok) {
    if (gate.code === MCP_SUBSCRIPTION_EXPIRED_CODE) {
      sendHtml(
        res,
        403,
        `<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>麻雀 · 授权 Claude</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;margin:0;padding:32px 20px;background:#f6f7f9;color:#111}
.card{max-width:420px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:24px}
h1{font-size:20px;margin:0 0 12px}
p{color:#6b7280;line-height:1.6;margin:0 0 16px}
a.btn{display:inline-block;background:#111;color:#fff;text-decoration:none;padding:10px 16px;border-radius:8px}
</style></head><body><div class="card">
<h1>无法授权</h1>
<p>${escapeHtml(MCP_SUBSCRIPTION_EXPIRED_MESSAGE)}</p>
<a class="btn" href="/">返回麻雀</a>
</div></body></html>`,
      );
      return;
    }
    const returnTo = `${getPublicBaseUrl(req)}${String(req.url || "/oauth/authorize")}`;
    const loginUrl = `/?auth=login&returnTo=${encodeURIComponent(returnTo)}`;
    sendHtml(
      res,
      401,
      `<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>麻雀 · 授权 Claude</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;margin:0;padding:32px 20px;background:#f6f7f9;color:#111}
.card{max-width:420px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:24px}
h1{font-size:20px;margin:0 0 12px}
p{color:#6b7280;line-height:1.6;margin:0 0 16px}
a.btn{display:inline-block;background:#111;color:#fff;text-decoration:none;padding:10px 16px;border-radius:8px}
</style></head><body><div class="card">
<h1>请先登录麻雀</h1>
<p>授权 Claude 读取持仓数据前，需要登录你的麻雀账户。</p>
<a class="btn" href="${escapeHtml(loginUrl)}">去登录</a>
</div></body></html>`,
    );
    return;
  }

  const confirm = String(q.get("confirm") || "").trim() === "1";
  if (!confirm) {
    const user = await getAuthSessionUserPayload(gate.userId);
    const display = escapeHtml(user?.displayName || maskPhone(user?.phone || "") || "用户");
    const actionUrl = escapeHtml(String(req.url || "").split("?")[0] + "?" + q.toString() + "&confirm=1");
    sendHtml(
      res,
      200,
      `<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>麻雀 · 授权 Claude</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;margin:0;padding:32px 20px;background:#f6f7f9;color:#111}
.card{max-width:440px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:24px}
h1{font-size:20px;margin:0 0 12px}
p{color:#4b5563;line-height:1.6;margin:0 0 10px}
ul{color:#6b7280;padding-left:18px;margin:0 0 18px}
.actions{display:flex;gap:10px;flex-wrap:wrap}
a.btn,button.btn{display:inline-block;background:#111;color:#fff;text-decoration:none;padding:10px 16px;border-radius:8px;border:none;font-size:14px;cursor:pointer}
a.ghost{background:#fff;color:#111;border:1px solid #d1d5db}
</style></head><body><div class="card">
<h1>授权 Claude 读取数据</h1>
<p><strong>${display}</strong>，Claude 将通过「麻雀」连接器读取：</p>
<ul>
<li>持仓与资产摘要</li>
<li>成交与银证转账（本人）</li>
<li>分析图表与个股排名</li>
</ul>
<p>只读访问，不会修改你的账户或交易记录。</p>
<div class="actions">
<a class="btn" href="${actionUrl}">确认授权</a>
<a class="ghost btn" href="/">取消</a>
</div>
</div></body></html>`
    );
    return;
  }

  const code = randomToken(24);
  const now = Date.now();
  await saveAuthCode({
    code,
    userId: gate.userId,
    clientId,
    redirectUri,
    codeChallenge,
    scope,
    expiresAt: now + AUTH_CODE_TTL_SEC * 1000,
    createdAt: now,
  });

  const redirect = new URL(redirectUri);
  redirect.searchParams.set("code", code);
  if (state) {
    redirect.searchParams.set("state", state);
  }
  res.statusCode = 302;
  res.setHeader("Location", redirect.toString());
  res.setHeader("Cache-Control", "no-store");
  res.end();
}

async function issueTokens({ userId, clientId, scope }) {
  const now = Date.now();
  const accessToken = signAccessToken({
    userId,
    clientId,
    scope,
    expMs: now + ACCESS_TOKEN_TTL_SEC * 1000,
  });
  const refreshToken = randomToken(32);
  await saveRefreshToken({
    userId,
    clientId,
    scope,
    refreshToken,
    expiresAt: now + REFRESH_TOKEN_TTL_SEC * 1000,
  });
  return {
    access_token: accessToken,
    token_type: "Bearer",
    expires_in: ACCESS_TOKEN_TTL_SEC,
    refresh_token: refreshToken,
    scope,
  };
}

async function handleOAuthToken(req, res) {
  if (req.method !== "POST") {
    sendJson(res, 405, { ok: false, error: "Method Not Allowed" });
    return;
  }
  const body = await readRequestBody(req);
  const grantType = String(body.grant_type || "").trim();

  if (grantType === "authorization_code") {
    const code = String(body.code || "").trim();
    const redirectUri = String(body.redirect_uri || "").trim();
    const codeVerifier = String(body.code_verifier || "").trim();
    const clientId = String(body.client_id || DEFAULT_CLIENT_ID).trim();
    if (!code || !redirectUri || !codeVerifier) {
      oauthError(res, 400, "invalid_request", "缺少 code / redirect_uri / code_verifier");
      return;
    }
    if (!isAllowedOAuthClientId(clientId)) {
      oauthError(res, 400, "invalid_client", "未知 client_id");
      return;
    }
    const row = await consumeAuthCode(code);
    if (!row) {
      oauthError(res, 400, "invalid_grant", "授权码无效或已过期");
      return;
    }
    if (row.clientId !== clientId || row.redirectUri !== redirectUri) {
      oauthError(res, 400, "invalid_grant", "client 或 redirect_uri 不匹配");
      return;
    }
    if (!verifyPkce(codeVerifier, row.codeChallenge)) {
      oauthError(res, 400, "invalid_grant", "PKCE 校验失败");
      return;
    }
    const subGate = await assertMcpUserActive(row.userId);
    if (!subGate.ok) {
      oauthError(res, 403, "access_denied", subGate.error);
      return;
    }
    const tokens = await issueTokens({
      userId: row.userId,
      clientId: row.clientId,
      scope: row.scope || DEFAULT_SCOPE,
    });
    sendJson(res, 200, tokens);
    return;
  }

  if (grantType === "refresh_token") {
    const refreshToken = String(body.refresh_token || "").trim();
    const clientId = String(body.client_id || DEFAULT_CLIENT_ID).trim();
    if (!refreshToken) {
      oauthError(res, 400, "invalid_request", "缺少 refresh_token");
      return;
    }
    if (!isAllowedOAuthClientId(clientId)) {
      oauthError(res, 400, "invalid_client", "未知 client_id");
      return;
    }
    const row = await findRefreshToken(refreshToken);
    if (!row || row.clientId !== clientId) {
      oauthError(res, 400, "invalid_grant", "refresh_token 无效或已过期");
      return;
    }
    const subGate = await assertMcpUserActive(row.userId);
    if (!subGate.ok) {
      oauthError(res, 403, "access_denied", subGate.error);
      return;
    }
    const tokens = await issueTokens({
      userId: row.userId,
      clientId: row.clientId,
      scope: row.scope || DEFAULT_SCOPE,
    });
    sendJson(res, 200, tokens);
    return;
  }

  oauthError(res, 400, "unsupported_grant_type", "不支持的 grant_type");
}

module.exports = {
  handleWellKnownProtectedResource,
  handleWellKnownAuthServer,
  handleOAuthAuthorize,
  handleOAuthToken,
  assertOAuthUser,
};
