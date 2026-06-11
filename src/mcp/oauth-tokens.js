const crypto = require("node:crypto");

function getSecret() {
  return String(process.env.MCP_OAUTH_SECRET || process.env.AUTH_SECRET || "stockreview-dev-secret-change-in-production");
}

function signAccessToken({ userId, clientId, scope, expMs }) {
  const payload = Buffer.from(
    JSON.stringify({
      typ: "mcp_access",
      u: String(userId || ""),
      c: String(clientId || ""),
      s: String(scope || ""),
      exp: Number(expMs) || 0,
    }),
    "utf8"
  ).toString("base64url");
  const sig = crypto.createHmac("sha256", getSecret()).update(payload).digest("base64url");
  return `${payload}.${sig}`;
}

function verifyAccessToken(token) {
  const raw = String(token || "");
  const dot = raw.indexOf(".");
  if (dot < 1) {
    return null;
  }
  const payload = raw.slice(0, dot);
  const sig = raw.slice(dot + 1);
  const expected = crypto.createHmac("sha256", getSecret()).update(payload).digest("base64url");
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return null;
  }
  try {
    const json = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    if (json?.typ !== "mcp_access" || !json.u) {
      return null;
    }
    if (typeof json.exp === "number" && json.exp < Date.now()) {
      return null;
    }
    return {
      userId: json.u,
      clientId: json.c || "",
      scope: json.s || "",
    };
  } catch {
    return null;
  }
}

module.exports = {
  signAccessToken,
  verifyAccessToken,
};
