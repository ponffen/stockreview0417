const crypto = require("node:crypto");
const { initPool } = require("../db");
const { ACCESS_TOKEN_TTL_SEC, REFRESH_TOKEN_TTL_SEC, AUTH_CODE_TTL_SEC, DEFAULT_CLIENT_ID } = require("./config");

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
    `INSERT INTO mcp_oauth_auth_code (code, user_id, client_id, redirect_uri, code_challenge, scope, expires_at, created_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [row.code, row.userId, row.clientId, row.redirectUri, row.codeChallenge, row.scope, row.expiresAt, row.createdAt]
  );
}

async function consumeAuthCode(code) {
  await ensureMcpOAuthSchema();
  const pool = await initPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query(
      `DELETE FROM mcp_oauth_auth_code WHERE code = $1 RETURNING user_id, client_id, redirect_uri, code_challenge, scope, expires_at`,
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

async function hasActiveClaudeConnection(userId) {
  await ensureMcpOAuthSchema();
  const uid = String(userId || "").trim();
  if (!uid) {
    return { connected: false };
  }
  const pool = await initPool();
  const { rows } = await pool.query(
    `SELECT MAX(expires_at)::bigint AS exp FROM mcp_oauth_refresh_token
     WHERE user_id = $1 AND expires_at > $2`,
    [uid, Date.now()]
  );
  const exp = Number(rows[0]?.exp) || 0;
  return {
    connected: exp > Date.now(),
    expiresAt: exp > Date.now() ? exp : null,
  };
}

async function revokeClaudeConnection(userId) {
  await ensureMcpOAuthSchema();
  const uid = String(userId || "").trim();
  if (!uid) {
    return;
  }
  const pool = await initPool();
  await pool.query(`DELETE FROM mcp_oauth_refresh_token WHERE user_id = $1`, [uid]);
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
  hasActiveClaudeConnection,
  revokeClaudeConnection,
  verifyPkce,
  ACCESS_TOKEN_TTL_SEC,
  REFRESH_TOKEN_TTL_SEC,
  AUTH_CODE_TTL_SEC,
};
