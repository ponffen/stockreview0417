const crypto = require("node:crypto");
const { initPool } = require("../db");
const {
  ACCESS_TOKEN_TTL_SEC,
  REFRESH_TOKEN_TTL_SEC,
  AUTH_CODE_TTL_SEC,
  DEFAULT_CLIENT_ID,
  sqlOAuthClientProviderClause,
} = require("./config");

let schemaReady = false;

async function ensureMcpOAuthSchema() {
  if (schemaReady) {
    return;
  }
  const pool = await initPool();
  await pool.query(`
    CREATE TABLE IF NOT EXISTS mcp_oauth_auth_code (
      code TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      redirect_uri TEXT NOT NULL,
      code_challenge TEXT NOT NULL,
      scope TEXT NOT NULL,
      expires_at BIGINT NOT NULL,
      created_at BIGINT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS mcp_oauth_refresh_token (
      token_hash TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      scope TEXT NOT NULL,
      expires_at BIGINT NOT NULL,
      created_at BIGINT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_mcp_oauth_refresh_user ON mcp_oauth_refresh_token (user_id, client_id);
    CREATE TABLE IF NOT EXISTS mcp_oauth_client (
      client_id TEXT PRIMARY KEY,
      client_name TEXT NOT NULL DEFAULT '',
      redirect_uris JSONB NOT NULL DEFAULT '[]'::jsonb,
      token_endpoint_auth_method TEXT NOT NULL DEFAULT 'none',
      grant_types JSONB NOT NULL DEFAULT '[]'::jsonb,
      response_types JSONB NOT NULL DEFAULT '[]'::jsonb,
      source TEXT NOT NULL DEFAULT 'dcr',
      created_at BIGINT NOT NULL
    );
    ALTER TABLE mcp_oauth_auth_code ADD COLUMN IF NOT EXISTS resource TEXT NOT NULL DEFAULT '';
  `);
  schemaReady = true;
}

function hashToken(token) {
  return crypto.createHash("sha256").update(String(token || "")).digest("hex");
}

function randomToken(bytes = 32) {
  return crypto.randomBytes(bytes).toString("base64url");
}

async function saveAuthCode(row) {
  await ensureMcpOAuthSchema();
  const pool = await initPool();
  await pool.query(
    `INSERT INTO mcp_oauth_auth_code (code, user_id, client_id, redirect_uri, code_challenge, scope, resource, expires_at, created_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
    [
      row.code,
      row.userId,
      row.clientId,
      row.redirectUri,
      row.codeChallenge,
      row.scope,
      row.resource || "",
      row.expiresAt,
      row.createdAt,
    ],
  );
}

async function consumeAuthCode(code) {
  await ensureMcpOAuthSchema();
  const pool = await initPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query(
      `DELETE FROM mcp_oauth_auth_code WHERE code = $1 RETURNING user_id, client_id, redirect_uri, code_challenge, scope, resource, expires_at`,
      [String(code || "")]
    );
    await client.query("COMMIT");
    const row = rows[0];
    if (!row) {
      return null;
    }
    if (Number(row.expires_at) < Date.now()) {
      return null;
    }
    return {
      userId: row.user_id,
      clientId: row.client_id,
      redirectUri: row.redirect_uri,
      codeChallenge: row.code_challenge,
      scope: row.scope,
      resource: row.resource || "",
    };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function saveRefreshToken({ userId, clientId, scope, refreshToken, expiresAt }) {
  await ensureMcpOAuthSchema();
  const pool = await initPool();
  await pool.query(
    `INSERT INTO mcp_oauth_refresh_token (token_hash, user_id, client_id, scope, expires_at, created_at)
     VALUES ($1,$2,$3,$4,$5,$6)
     ON CONFLICT (token_hash) DO UPDATE SET expires_at = EXCLUDED.expires_at`,
    [hashToken(refreshToken), userId, clientId, scope, expiresAt, Date.now()]
  );
}

async function findRefreshToken(refreshToken) {
  await ensureMcpOAuthSchema();
  const pool = await initPool();
  const { rows } = await pool.query(
    `SELECT user_id, client_id, scope, expires_at FROM mcp_oauth_refresh_token WHERE token_hash = $1`,
    [hashToken(refreshToken)]
  );
  const row = rows[0];
  if (!row) {
    return null;
  }
  if (Number(row.expires_at) < Date.now()) {
    await pool.query(`DELETE FROM mcp_oauth_refresh_token WHERE token_hash = $1`, [hashToken(refreshToken)]);
    return null;
  }
  return {
    userId: row.user_id,
    clientId: row.client_id,
    scope: row.scope,
    expiresAt: Number(row.expires_at),
  };
}

async function hasActiveOAuthConnection(userId, provider) {
  await ensureMcpOAuthSchema();
  const uid = String(userId || "").trim();
  if (!uid || (provider !== "claude" && provider !== "chatgpt")) {
    return { connected: false };
  }
  const pool = await initPool();
  const now = Date.now();
  const clause = sqlOAuthClientProviderClause(provider, 3);
  const { rows } = await pool.query(
    `SELECT MAX(expires_at)::bigint AS exp FROM mcp_oauth_refresh_token
     WHERE user_id = $1 AND expires_at > $2 AND ${clause}`,
    [uid, now, DEFAULT_CLIENT_ID],
  );
  const exp = Number(rows[0]?.exp) || 0;
  return {
    connected: exp > now,
    expiresAt: exp > now ? exp : null,
  };
}

async function hasActiveClaudeConnection(userId) {
  return hasActiveOAuthConnection(userId, "claude");
}

async function hasActiveChatGptConnection(userId) {
  return hasActiveOAuthConnection(userId, "chatgpt");
}

async function revokeOAuthConnection(userId, provider) {
  await ensureMcpOAuthSchema();
  const uid = String(userId || "").trim();
  if (!uid || (provider !== "claude" && provider !== "chatgpt")) {
    return;
  }
  const pool = await initPool();
  const clause = sqlOAuthClientProviderClause(provider, 2);
  await pool.query(`DELETE FROM mcp_oauth_refresh_token WHERE user_id = $1 AND ${clause}`, [
    uid,
    DEFAULT_CLIENT_ID,
  ]);
}

async function revokeClaudeConnection(userId) {
  await revokeOAuthConnection(userId, "claude");
}

async function revokeChatGptConnection(userId) {
  await revokeOAuthConnection(userId, "chatgpt");
}

async function saveRegisteredClient(row) {
  await ensureMcpOAuthSchema();
  const pool = await initPool();
  await pool.query(
    `INSERT INTO mcp_oauth_client (
      client_id, client_name, redirect_uris, token_endpoint_auth_method, grant_types, response_types, source, created_at
    ) VALUES ($1,$2,$3::jsonb,$4,$5::jsonb,$6::jsonb,$7,$8)
    ON CONFLICT (client_id) DO UPDATE SET
      client_name = EXCLUDED.client_name,
      redirect_uris = EXCLUDED.redirect_uris,
      token_endpoint_auth_method = EXCLUDED.token_endpoint_auth_method,
      grant_types = EXCLUDED.grant_types,
      response_types = EXCLUDED.response_types,
      source = EXCLUDED.source`,
    [
      row.clientId,
      row.clientName || "",
      JSON.stringify(row.redirectUris || []),
      row.tokenEndpointAuthMethod || "none",
      JSON.stringify(row.grantTypes || []),
      JSON.stringify(row.responseTypes || []),
      row.source || "dcr",
      row.createdAt || Date.now(),
    ],
  );
}

async function findRegisteredClient(clientId) {
  await ensureMcpOAuthSchema();
  const id = String(clientId || "").trim();
  if (!id) {
    return null;
  }
  const pool = await initPool();
  const { rows } = await pool.query(
    `SELECT client_id, client_name, redirect_uris, token_endpoint_auth_method, grant_types, response_types, source, created_at
     FROM mcp_oauth_client WHERE client_id = $1`,
    [id],
  );
  const row = rows[0];
  if (!row) {
    return null;
  }
  return {
    clientId: row.client_id,
    clientName: row.client_name || "",
    redirectUris: Array.isArray(row.redirect_uris) ? row.redirect_uris.map(String) : [],
    tokenEndpointAuthMethod: row.token_endpoint_auth_method || "none",
    grantTypes: Array.isArray(row.grant_types) ? row.grant_types.map(String) : [],
    responseTypes: Array.isArray(row.response_types) ? row.response_types.map(String) : [],
    source: row.source || "dcr",
    createdAt: Number(row.created_at) || 0,
  };
}

function verifyPkce(codeVerifier, codeChallenge) {
  const verifier = String(codeVerifier || "");
  const challenge = String(codeChallenge || "");
  if (!verifier || !challenge) {
    return false;
  }
  const digest = crypto.createHash("sha256").update(verifier).digest("base64url");
  return digest === challenge;
}

module.exports = {
  ensureMcpOAuthSchema,
  randomToken,
  saveAuthCode,
  consumeAuthCode,
  saveRefreshToken,
  findRefreshToken,
  hasActiveOAuthConnection,
  hasActiveClaudeConnection,
  hasActiveChatGptConnection,
  revokeOAuthConnection,
  revokeClaudeConnection,
  revokeChatGptConnection,
  saveRegisteredClient,
  findRegisteredClient,
  verifyPkce,
  ACCESS_TOKEN_TTL_SEC,
  REFRESH_TOKEN_TTL_SEC,
  AUTH_CODE_TTL_SEC,
};
