/**
 * 长桥实时行情：经阿里云 FC 代理拉取（Vercel 不打包 longbridge SDK）。
 * 长桥密钥仅配置在 Vercel 环境变量，由服务端随请求转发给 FC。
 */
const { normalizeSymbol } = require("../db");
const { buildQuoteRecord } = require("./quote-common");

const LONGPORT_FETCH_TIMEOUT_MS = Math.max(
  3000,
  Math.min(25_000, Number(process.env.LONGPORT_QUOTE_TIMEOUT_MS || 18_000)),
);

const LONGPORT_PROXY_BASE =
  String(process.env.ALIYUN_QUOTE_PROXY_BASE_URL || "").trim().replace(/\/+$/, "") ||
  "https://market-et-proxy-chbtzurmsn.cn-hangzhou.fcapp.run";

function envValue(...names) {
  for (const name of names) {
    const v = String(process.env[name] || "").trim();
    if (v) {
      return v;
    }
  }
  return "";
}

function buildLongportProxyHeaders() {
  const appKey = envValue("LONGPORT_APP_KEY", "LONGBRIDGE_APP_KEY");
  const appSecret = envValue("LONGPORT_APP_SECRET", "LONGBRIDGE_APP_SECRET");
  const accessToken = envValue("LONGPORT_ACCESS_TOKEN", "LONGBRIDGE_ACCESS_TOKEN");
  if (!appKey || !appSecret || !accessToken) {
    return null;
  }
  const headers = {
    "X-Longport-App-Key": appKey,
    "X-Longport-App-Secret": appSecret,
    "X-Longport-Access-Token": accessToken,
  };
  const httpUrl = envValue("LONGPORT_HTTP_URL", "LONGBRIDGE_HTTP_URL");
  if (httpUrl) {
    headers["X-Longport-Http-Url"] = httpUrl;
  }
  const overnight = envValue("LONGPORT_ENABLE_OVERNIGHT", "LONGBRIDGE_ENABLE_OVERNIGHT");
  if (overnight) {
    headers["X-Longport-Enable-Overnight"] = overnight;
  }
  return headers;
}

function toLongportSymbol(rawSymbol) {
  const sym = normalizeSymbol(rawSymbol);
  if (!sym) {
    return "";
  }
  if (/^hk\d{5}$/.test(sym)) {
    const code = String(parseInt(sym.slice(2), 10));
    return `${code}.HK`;
  }
  if (/^sh\d{6}$/.test(sym)) {
    return `${sym.slice(2)}.SH`;
  }
  if (/^sz\d{6}$/.test(sym)) {
    return `${sym.slice(2)}.SZ`;
  }
  let ticker = sym;
  if (/^us[a-z0-9._-]+$/i.test(sym) && !/^us_/i.test(sym)) {
    ticker = sym.replace(/^us/i, "");
  } else if (/^gb_/i.test(sym)) {
    ticker = sym.slice(3);
  }
  ticker = String(ticker || "")
    .replace(/\.(OQ|N)$/i, "")
    .toUpperCase();
  if (!ticker) {
    return "";
  }
  return `${ticker}.US`;
}

function quoteRecordFromProxyRow(sym, row) {
  if (!row || typeof row !== "object") {
    return null;
  }
  const current = Number(row.current);
  const prevClose = Number(row.prevClose);
  const time = String(row.time || row.rawTime || "--");
  return buildQuoteRecord({
    symbol: normalizeSymbol(sym),
    name: row.name,
    current,
    prevClose,
    time,
    rawTime: row.rawTime || time,
    session: row.session || null,
    sessionLabel: row.sessionLabel || null,
    source: row.source || "longport",
    delayed: !!row.delayed,
  });
}

async function fetchLongportQuoteMap(symbols) {
  const symList = [...new Set((symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))];
  if (!symList.length) {
    return { ok: false, map: new Map(), delayed: false, error: "empty symbols" };
  }

  const proxyHeaders = buildLongportProxyHeaders();
  if (!proxyHeaders) {
    return { ok: false, map: new Map(), delayed: true, error: "longport credentials missing on server" };
  }

  const url = `${LONGPORT_PROXY_BASE}/api/quote/longport?symbols=${encodeURIComponent(symList.join(","))}`;
  const out = new Map();
  let delayed = false;
  let lastError = "";

  try {
    const response = await fetch(url, {
      headers: proxyHeaders,
      signal: AbortSignal.timeout(LONGPORT_FETCH_TIMEOUT_MS),
    });
    const rawText = await response.text();
    if (!response.ok) {
      lastError = `longport proxy http ${response.status}`;
      try {
        const errBody = rawText ? JSON.parse(rawText) : {};
        if (errBody?.error) {
          lastError = String(errBody.error);
        } else if (rawText && rawText.length < 500) {
          lastError = `${lastError}: ${rawText}`;
        }
      } catch {
        if (rawText && rawText.length < 500) {
          lastError = `${lastError}: ${rawText}`;
        }
      }
      return { ok: false, map: out, delayed: false, error: lastError };
    }
    const payload = rawText ? JSON.parse(rawText) : {};
    delayed = !!payload?.delayed;
    lastError = String(payload?.error || "");
    const quotes = payload?.quotes && typeof payload.quotes === "object" ? payload.quotes : {};
    for (const [sym, row] of Object.entries(quotes)) {
      const rec = quoteRecordFromProxyRow(sym, row);
      if (rec) {
        out.set(normalizeSymbol(rec.symbol), rec);
      }
    }
  } catch (error) {
    lastError = error?.message || "longport proxy failed";
    delayed = true;
  }

  if (out.size > 0 && out.size < symList.length) {
    delayed = true;
  }

  return {
    ok: out.size > 0,
    map: out,
    delayed,
    error: out.size ? "" : lastError,
    source: "longport",
  };
}

module.exports = {
  toLongportSymbol,
  fetchLongportQuoteMap,
};
