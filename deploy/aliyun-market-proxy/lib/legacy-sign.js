const crypto = require("node:crypto");

const SIGNED_HEADERS = ["authorization", "x-api-key", "x-timestamp"];
const ALG = "HMAC-SHA256";

function signLegacyRequest({ method, path, queryString = "", headers, body = "", appSecret }) {
  const mtd = String(method || "GET").toUpperCase();
  const normalized = {};
  for (const [k, v] of Object.entries(headers || {})) {
    normalized[String(k).toLowerCase()] = String(v || "").trim();
  }
  let plain = `${mtd}|${path}|${queryString}|`;
  for (const name of SIGNED_HEADERS) {
    plain += `${name}:${normalized[name] || ""}\n`;
  }
  plain += `|${SIGNED_HEADERS.join(";")}|`;
  if (body) {
    plain += crypto.createHash("sha1").update(body, "utf8").digest("hex");
  }
  const textToSign = `${ALG}|${crypto.createHash("sha1").update(plain, "utf8").digest("hex")}`;
  const signature = crypto.createHmac("sha256", String(appSecret || "")).update(textToSign, "utf8").digest("hex");
  return `${ALG} SignedHeaders=${SIGNED_HEADERS.join(";")}, Signature=${signature}`;
}

module.exports = {
  signLegacyRequest,
};
