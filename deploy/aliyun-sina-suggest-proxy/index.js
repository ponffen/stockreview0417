const SINA_TIMEOUT_MS = 6500;

function normalizeSymbol(rawSymbol) {
  const value = String(rawSymbol || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
  if (!value) {
    return "";
  }
  if (/^sh\d{6}$/.test(value) || /^sz\d{6}$/.test(value) || /^hk\d{5}$/.test(value)) {
    return value;
  }
  if (/^\d{6}$/.test(value)) {
    return ["5", "6", "9"].includes(value[0]) ? `sh${value}` : `sz${value}`;
  }
  if (/^\d{1,5}$/.test(value)) {
    return `hk${value.padStart(5, "0")}`;
  }
  return value;
}

function suggestLineToItem(line) {
  const parts = String(line || "").split(",");
  if (parts.length < 3) {
    return null;
  }
  const typeCode = Number.parseInt(String(parts[1] || ""), 10);
  if (!Number.isFinite(typeCode)) {
    return null;
  }
  const rawCode = String(parts[2] || "").trim();
  if (!rawCode) {
    return null;
  }
  const name = String(
    (parts.length > 6 && String(parts[6] || "").trim()) || (parts[4] || parts[3] || "").trim() || rawCode
  );
  let symbol = "";
  if (typeCode === 31) {
    const digits = rawCode.replace(/\D/g, "");
    if (digits) {
      symbol = `hk${digits.length <= 5 ? digits.padStart(5, "0") : digits.slice(-5).padStart(5, "0")}`;
    }
  } else if (typeCode === 41) {
    const al = rawCode.toUpperCase().replace(/[^A-Z0-9._-]/g, "");
    if (al) {
      symbol = al.replace(/^GB_/i, "").toLowerCase();
    }
  } else {
    symbol = normalizeSymbol(rawCode);
  }
  symbol = normalizeSymbol(symbol);
  if (!symbol) {
    return null;
  }
  const market =
    typeCode === 31 ? "港" : typeCode === 41 ? "美" : typeCode === 101 || typeCode === 111 ? "沪/深" : "其他";
  return { typeCode, code: rawCode, name, symbol, market };
}

function parseSinaSuggestText(text) {
  const m = /var suggest="([^"]*)"/.exec(String(text || ""));
  if (!m) {
    return [];
  }
  const raw = m[1].trim();
  if (!raw) {
    return [];
  }
  return raw
    .split(/;/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function parseHttpEvent(event) {
  const base = { method: "GET", path: "/", query: {} };
  if (!event) {
    return base;
  }
  try {
    const rawText = Buffer.isBuffer(event) ? event.toString("utf8") : String(event);
    const parsed = JSON.parse(rawText);
    const method = String(parsed?.httpMethod || parsed?.requestContext?.http?.method || "GET").toUpperCase();
    const path = String(parsed?.path || parsed?.rawPath || parsed?.requestContext?.http?.path || "/");
    const query = parsed?.queryParameters && typeof parsed.queryParameters === "object" ? parsed.queryParameters : {};
    return { method, path, query };
  } catch {
    return base;
  }
}

function toHttpResponse(statusCode, bodyObj) {
  return {
    statusCode,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,OPTIONS",
      "access-control-allow-headers": "content-type,authorization",
    },
    body: JSON.stringify(bodyObj),
    isBase64Encoded: false,
  };
}

async function handleSuggest(key) {
  const url = `https://suggest3.sinajs.cn/suggest/?key=${encodeURIComponent(
    key
  )}&type=111,41,31,101&name=suggest&num=50`;
  const response = await fetch(url, {
    headers: { "user-agent": "stockreview/1" },
    signal: AbortSignal.timeout(SINA_TIMEOUT_MS),
  });
  if (!response.ok) {
    throw new Error(`sina suggest http ${response.status}`);
  }
  const buf = Buffer.from(await response.arrayBuffer());
  let text = new TextDecoder("gbk").decode(buf);
  if (!/var suggest\s*=/.test(text)) {
    text = buf.toString("utf8");
  }
  const lines = parseSinaSuggestText(text);
  const results = [];
  for (const line of lines) {
    const item = suggestLineToItem(line);
    if (item) {
      results.push(item);
    }
  }
  return results;
}

exports.handler = async (event) => {
  const req = parseHttpEvent(event);
  if (req.method === "OPTIONS") {
    return toHttpResponse(204, { ok: true });
  }
  if (req.path !== "/api/sina/suggest" && req.path !== "/sina/suggest" && req.path !== "/") {
    return toHttpResponse(404, { ok: false, error: "not found", path: req.path, method: req.method });
  }
  const key = String(req.query?.key || "").trim();
  if (!key || key.length > 64) {
    return toHttpResponse(400, { ok: false, error: "invalid key" });
  }
  try {
    const results = await handleSuggest(key);
    return toHttpResponse(200, { ok: true, results });
  } catch (error) {
    return toHttpResponse(502, { ok: false, error: error?.message || "sina suggest failed" });
  }
};
