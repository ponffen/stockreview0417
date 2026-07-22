const { parseHttpEvent, jsonResponse, textResponse } = require("./lib/http");
const { fetchLongportQuotes, longportCredsFromHeaders } = require("./lib/longport");
const { fetchTencentQuoteText } = require("./lib/tencent");

function parseSymbolList(raw) {
  return [...new Set(String(raw || "").split(",").map((s) => String(s || "").trim()).filter(Boolean))];
}

exports.handler = async (event) => {
  const req = parseHttpEvent(event);
  const path = String(req.path || "/").replace(/\/+$/, "") || "/";

  if (req.method === "OPTIONS") {
    return jsonResponse(204, { ok: true });
  }

  if (req.method !== "GET") {
    return jsonResponse(405, { ok: false, error: "method not allowed" });
  }

  if (path === "/api/health" || path === "/health") {
    return jsonResponse(200, {
      ok: true,
      service: "market-proxy",
      credentialMode: "forwarded-from-vercel",
      longportTransport: "websocket",
      node: process.version,
      platform: process.platform,
      arch: process.arch,
    });
  }

  if (path === "/api/quote/longport" || path === "/quote/longport") {
    const symbols = parseSymbolList(req.query?.symbols || req.query?.symbol || "");
    if (!symbols.length || symbols.length > 500) {
      return jsonResponse(400, { ok: false, error: "invalid symbols" });
    }
    const creds = longportCredsFromHeaders(req.headers);
    if (!creds?.appKey || !creds?.appSecret || !creds?.accessToken) {
      return jsonResponse(401, { ok: false, error: "longport credentials missing in request headers" });
    }
    const result = await fetchLongportQuotes(symbols, creds);
    const status = result.ok ? 200 : 502;
    return jsonResponse(status, result);
  }

  if (path === "/api/quote/tencent" || path === "/quote/tencent") {
    const q = String(req.query?.q || "").trim();
    if (!q) {
      return jsonResponse(400, { ok: false, error: "invalid q" });
    }
    try {
      const text = await fetchTencentQuoteText(q);
      return textResponse(200, text, "text/plain; charset=utf-8", {
        "x-market-data-source": "tencent",
      });
    } catch (error) {
      return jsonResponse(502, { ok: false, error: error?.message || "tencent quote failed" });
    }
  }

  return jsonResponse(404, { ok: false, error: "not found", path, method: req.method });
};
