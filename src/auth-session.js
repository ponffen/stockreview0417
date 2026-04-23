const crypto = require("node:crypto");

const COOKIE_NAME = "stockreview_session";
const MAX_AGE_SEC = 60 * 60 * 24 * 30;

function getSecret() {
  return String(process.env.AUTH_SECRET || "stockreview-dev-secret-change-in-production");
}

function parseCookies(header) {
  const out = {};
  if (!header || typeof header !== "string") {
    return out;
  }
  for (const part of header.split(";")) {
    const idx = part.indexOf("=");
    if (idx === -1) {
      continue;
    }
    const k = part.slice(0, idx).trim();
    const v = part.slice(idx + 1).trim();
    if (k) {
      try {
        out[k] = decodeURIComponent(v);
      } catch {
        out[k] = v;
      }
    }
  }
  return out;
}

function signPayload(payloadObj) {
  const payload = Buffer.from(JSON.stringify(payloadObj), "utf8").toString("base64url");
  const sig = crypto.createHmac("sha256", getSecret()).update(payload).digest("base64url");
  return `${payload}.${sig}`;
}

function verifyToken(token) {
  if (!token || typeof token !== "string") {
    return null;
  }
  const dot = token.indexOf(".");
  if (dot < 1) {
    return null;
  }
  const payload = token.slice(0, dot);
  const sig = token.slice(dot + 1);
  const expected = crypto.createHmac("sha256", getSecret()).update(payload).digest("base64url");
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return null;
  }
  try {
    const json = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    if (!json || typeof json.u !== "string" || !json.u) {
      return null;
    }
    if (typeof json.exp === "number" && json.exp < Date.now()) {
      return null;
    }
    return json.u;
  } catch {
    return null;
  }
}

function readUserIdFromRequest(req) {
  const raw = parseCookies(req.headers.cookie || "")[COOKIE_NAME];
  return verifyToken(raw);
}

function setSessionCookie(res, userId) {
  const token = signPayload({ u: userId, exp: Date.now() + MAX_AGE_SEC * 1000 });
  const cookie = `${COOKIE_NAME}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${MAX_AGE_SEC}`;
  res.setHeader("Set-Cookie", cookie);
}

function clearSessionCookie(res) {
  res.setHeader("Set-Cookie", `${COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`);
}

module.exports = {
  COOKIE_NAME,
  parseCookies,
  readUserIdFromRequest,
  setSessionCookie,
  clearSessionCookie,
};
