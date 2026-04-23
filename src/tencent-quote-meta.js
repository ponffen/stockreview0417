/**
 * 腾讯 qt.gtimg.cn 实时行情：解析中文简称、市场类型（首字段）、展示代码。
 * 规则与浏览器端 toTencentQuoteSymbol 一致：sh/sz/hk、美股 usTICKER、gb_ → us。
 */

const iconv = require("iconv-lite");
const { normalizeSymbol } = require("./db");

function toTencentQuoteKey(rawSymbol) {
  const normalized =
    normalizeSymbol(rawSymbol) || String(rawSymbol || "").trim().toLowerCase().replace(/\s+/g, "");
  if (!normalized) {
    return "";
  }
  const raw = normalized;
  const orig = String(rawSymbol || "").trim().replace(/\s+/g, "");

  if (/^sh\d{6}$/.test(raw) || /^sz\d{6}$/.test(raw) || /^hk\d{5}$/.test(raw)) {
    return raw;
  }
  if (/^us[A-Z0-9._-]+$/i.test(orig)) {
    const base = orig
      .replace(/^us/i, "")
      .replace(/\.(OQ|N)$/i, "");
    return `us${base.toUpperCase()}`;
  }
  if (/^gb_/i.test(raw)) {
    return `us${raw.slice(3).toUpperCase()}`;
  }
  if (/^rt_hk/i.test(raw)) {
    const code = raw.replace(/^rt_hk/i, "").replace(/\D/g, "").padStart(5, "0");
    return `hk${code}`;
  }
  if (/^[a-z][a-z0-9._-]*$/i.test(raw)) {
    return `us${raw.toUpperCase()}`;
  }
  return raw;
}

function marketTagFromTencentFirstField(first) {
  const f = String(first || "").trim();
  if (f === "1" || f === "51") {
    return "CN";
  }
  if (f === "100") {
    return "HK";
  }
  if (f === "200") {
    return "US";
  }
  return null;
}

function displayCodeFromTencentParts2(parts2) {
  let c = String(parts2 || "").trim().replace(/,/g, "");
  c = c.replace(/\.(OQ|N)$/i, "");
  if (!c) {
    return "";
  }
  return c.toUpperCase();
}

function parseTencentTildeRecord(payload) {
  if (!payload || typeof payload !== "string") {
    return null;
  }
  const parts = payload.split("~");
  if (parts.length < 3) {
    return null;
  }
  const name = String(parts[1] || "").trim();
  if (!name) {
    return null;
  }
  const tag = marketTagFromTencentFirstField(parts[0]) || "OT";
  const codeRaw = String(parts[2] || "").trim();
  const displayCode = displayCodeFromTencentParts2(codeRaw);
  return { name, marketTag: tag, displayCode: displayCode || codeRaw.toUpperCase() };
}

const CHUNK = 55;

/**
 * @param {string[]} symbols 库内 symbol（可混用 sh/hk/gb 等）
 * @returns {Promise<Map<string, { name: string, marketTag: string, displayCode: string }>>}
 */
async function fetchTencentQuoteMetaForSymbols(symbols) {
  const out = new Map();
  const unique = [...new Set((symbols || []).filter(Boolean).map((s) => String(s).trim()))];
  if (!unique.length) {
    return out;
  }

  const keyToOriginals = new Map();
  for (const sym of unique) {
    const key = toTencentQuoteKey(sym);
    if (!key) {
      continue;
    }
    if (!keyToOriginals.has(key)) {
      keyToOriginals.set(key, []);
    }
    keyToOriginals.get(key).push(sym);
  }
  const keys = [...keyToOriginals.keys()];
  if (!keys.length) {
    return out;
  }

  for (let i = 0; i < keys.length; i += CHUNK) {
    const chunk = keys.slice(i, i + CHUNK);
    const q = chunk.join(",");
    const url = `https://qt.gtimg.cn/q=${encodeURIComponent(q)}&_=${Date.now()}`;
    let text = "";
    try {
      const r = await fetch(url, {
        headers: { "User-Agent": "Mozilla/5.0 (compatible; stockreview/1.0)" },
      });
      if (!r.ok) {
        continue;
      }
      const buf = Buffer.from(await r.arrayBuffer());
      text = iconv.decode(buf, "gbk");
    } catch {
      continue;
    }
    const re = /v_([A-Za-z0-9._]+)="([^"]*)"/g;
    let m;
    while ((m = re.exec(text)) !== null) {
      const sourceKey = m[1];
      const parsed = parseTencentTildeRecord(m[2]);
      if (!parsed) {
        continue;
      }
      const originals = keyToOriginals.get(sourceKey) || [];
      for (const orig of originals) {
        out.set(orig, {
          name: parsed.name,
          marketTag: parsed.marketTag,
          displayCode: parsed.displayCode,
        });
      }
    }
  }
  return out;
}

module.exports = {
  toTencentQuoteKey,
  fetchTencentQuoteMetaForSymbols,
};
