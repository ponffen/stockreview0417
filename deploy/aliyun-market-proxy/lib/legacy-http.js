const { signLegacyRequest } = require("./legacy-sign");

async function legacyGet(baseUrl, path, creds, timeoutMs = 15000) {
  const appKey = String(creds?.appKey || "").trim();
  const appSecret = String(creds?.appSecret || "").trim();
  const accessToken = String(creds?.accessToken || "").trim();
  if (!appKey || !appSecret || !accessToken) {
    throw new Error("longport credentials missing");
  }
  const uri = String(path || "").startsWith("/") ? String(path) : `/${path}`;
  const url = `${String(baseUrl || "").replace(/\/+$/, "")}${uri}`;
  const timestamp = String(Date.now());
  const headers = {
    authorization: accessToken,
    "x-api-key": appKey,
    "x-timestamp": timestamp,
    "content-type": "application/json; charset=utf-8",
  };
  headers["x-api-signature"] = signLegacyRequest({
    method: "GET",
    path: uri,
    queryString: "",
    headers,
    body: "",
    appSecret,
  });
  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: accessToken,
      "X-Api-Key": appKey,
      "X-Timestamp": timestamp,
      "X-Api-Signature": headers["x-api-signature"],
      "Content-Type": "application/json; charset=utf-8",
    },
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  let payload = {};
  try {
    payload = text ? JSON.parse(text) : {};
  } catch {
    payload = { raw: text };
  }
  if (!response.ok) {
    throw new Error(`longport http ${response.status}: ${payload?.message || payload?.msg || text.slice(0, 200)}`);
  }
  if (Number(payload?.code) !== 0) {
    throw new Error(String(payload?.message || payload?.msg || `longport api code ${payload?.code}`));
  }
  return payload?.data && typeof payload.data === "object" ? payload.data : payload;
}

module.exports = {
  legacyGet,
};
