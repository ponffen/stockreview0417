console.log("[api/index.js] module-load start build=v6");

// 懒加载：不在模块顶层 require server.js，避免 server.js 顶层任何副作用
// 导致整个函数在冷启动阶段就 hang 300s。改为第一次业务请求时才加载。
let appPromise = null;
let loadError = null;
const DIRECT_SINA_SUGGEST_TIMEOUT_MS = 6500;
const ALIYUN_SINA_SUGGEST_TIMEOUT_MS = 4500;
const DIRECT_SINA_SUGGEST_CACHE_TTL_MS = 12 * 1000;
const DEFAULT_ALIYUN_SINA_SUGGEST_PROXY_BASE =
  "https://market-suggest-akylmuviow.cn-hangzhou.fcapp.run";
const sinaSuggestCache = new Map();

function getCacheHit(map, key, ttlMs) {
  const hit = map.get(String(key));
  if (!hit) {
    return null;
  }
  if (Date.now() - Number(hit.at || 0) > Number(ttlMs || 0)) {
    map.delete(String(key));
    return null;
  }
  return hit.value;
}

function setCacheValue(map, key, value) {
  map.set(String(key), { at: Date.now(), value });
}

async function fetchSinaSuggestFromUpstream(key) {
  const iconv = require("iconv-lite");
  const { parseSinaSuggestText, suggestLineToItem } = require("../src/sina-suggest");
  const { normalizeSymbol } = require("../src/db");
  const url = `https://suggest3.sinajs.cn/suggest/?key=${encodeURIComponent(
    key
  )}&type=111,41,31,101&name=suggest&num=50`;
  const response = await fetch(url, {
    headers: { "user-agent": "stockreview/1" },
    signal: AbortSignal.timeout(DIRECT_SINA_SUGGEST_TIMEOUT_MS),
  });
  if (!response.ok) {
    throw new Error(`sina suggest http ${response.status}`);
  }
  const buf = Buffer.from(await response.arrayBuffer());
  let text = iconv.decode(buf, "gbk");
  if (!/var suggest\s*=/.test(text) && /var suggest/.test(buf.toString("utf8"))) {
    text = buf.toString("utf8");
  }
  const lines = parseSinaSuggestText(text);
  const results = [];
  for (const line of lines) {
    const item = suggestLineToItem(line, normalizeSymbol);
    if (item) {
      results.push(item);
    }
  }
  return results;
}

async function fetchSinaSuggestFromAliyun(key) {
  const base = String(process.env.ALIYUN_SINA_SUGGEST_PROXY_BASE_URL || DEFAULT_ALIYUN_SINA_SUGGEST_PROXY_BASE)
    .trim()
    .replace(/\/+$/, "");
  if (!base) {
    return null;
  }
  const url = `${base}/api/sina/suggest?key=${encodeURIComponent(key)}`;
  const response = await fetch(url, {
    headers: { accept: "application/json" },
    signal: AbortSignal.timeout(ALIYUN_SINA_SUGGEST_TIMEOUT_MS),
  });
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`aliyun suggest http ${response.status}`);
  }
  const json = await response.json().catch(() => ({}));
  if (!json?.ok || !Array.isArray(json.results)) {
    throw new Error("aliyun suggest payload invalid");
  }
  return json.results;
}

function getServerlessApp() {
  if (appPromise) return appPromise;
  appPromise = (async () => {
    const t0 = Date.now();
    try {
      console.log("[api/index.js] lazy-require serverless-http...");
      const serverless = require("serverless-http");
      console.log("[api/index.js] serverless-http loaded in", Date.now() - t0, "ms");

      console.log("[api/index.js] lazy-require ../server (this may hang if server.js has a bad module-load side effect)...");
      const t1 = Date.now();
      const app = require("../server");
      console.log("[api/index.js] ../server loaded in", Date.now() - t1, "ms");

      return serverless(app);
    } catch (e) {
      loadError = e;
      console.error("[api/index.js] FATAL load error:", e?.stack || e?.message || e);
      throw e;
    }
  })();
  return appPromise;
}

/** 仅 path，不含 query；用于 Vercel 剥短 URL 后的匹配与还原判断 */
function urlPathOnly(u) {
  const p = String(u || "").split("?")[0];
  return p || "/";
}

/** 去掉尾部斜杠，避免 rewrite 后 path 不一致漏掉直连白名单 */
function apiPathKey(pathOnly) {
  const p = String(pathOnly || "/").replace(/\/+$/, "") || "/";
  return p;
}

function getSearchParam(req, key) {
  try {
    const u = new URL(String(req.url || "/"), "http://localhost");
    return u.searchParams.get(String(key || "")) || "";
  } catch {
    return "";
  }
}

function parsePositiveInt(input, fallback, min, max) {
  const n = Number(input);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  const v = Math.floor(n);
  return Math.min(max, Math.max(min, v));
}

function parseBooleanInput(input, fallback = false) {
  if (input == null || input === "") {
    return fallback;
  }
  const v = String(input).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(v)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(v)) {
    return false;
  }
  return fallback;
}

function sanitizeSymbolList(raw, normalizeSymbol) {
  const src = String(raw || "").trim();
  if (!src) {
    return [];
  }
  return [...new Set(src.split(",").map((s) => normalizeSymbol(String(s || ""))).filter(Boolean))];
}

async function readJsonBody(req) {
  const bodyStr = await new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => (data += chunk));
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
  if (!bodyStr) {
    return {};
  }
  try {
    return JSON.parse(bodyStr);
  } catch {
    return {};
  }
}

function extractBearerToken(req) {
  const auth = String(req.headers?.authorization || "").trim();
  if (!auth.toLowerCase().startsWith("bearer ")) {
    return "";
  }
  return auth.slice(7).trim();
}

// 最外层 handler：先处理"不依赖 server.js"的自证端点，再异步加载 Express app
module.exports = async function handler(req, res) {
  console.log(
    "[api/index.js] handler start url=%s method=%s x-matched-path=%s x-forwarded-uri=%s",
    req.url,
    req.method,
    req.headers["x-matched-path"] || "-",
    req.headers["x-forwarded-uri"] || req.headers["x-original-url"] || "-"
  );

  // 兜底：如果 Vercel 的 rewrite 把 req.url 改成了 "/" 或 "/api"，
  // 优先用 Vercel 透传的原始 path header 还原，避免 Express 错路由到 SPA fallback。
  try {
    const originalPath =
      req.headers["x-forwarded-uri"] ||
      req.headers["x-original-url"] ||
      req.headers["x-matched-path"];
    const pu = urlPathOnly(req.url);
    if (
      (pu === "/" || pu === "/api" || pu === "/api/" || pu === "/api/index") &&
      typeof originalPath === "string" &&
      originalPath.startsWith("/api/") &&
      urlPathOnly(originalPath) !== "/api"
    ) {
      console.log("[api/index.js] restoring req.url from %s to %s", req.url, originalPath);
      req.url = originalPath;
    }
  } catch (_) {}

  const pathOnly = urlPathOnly(req.url);
  const pathKey = apiPathKey(pathOnly);

  async function resolveMetricsBundleUserId(publicPath) {
    const { readUserIdFromRequest } = require("../src/auth-session");
    const { getUserCommunityRow } = require("../src/db");
    const viewerId = readUserIdFromRequest(req);
    if (!viewerId) {
      return { ok: false, status: 401, error: "请先登录" };
    }
    if (!publicPath) {
      return { ok: true, userId: viewerId };
    }
    const targetId = pathKey.split("/")[3];
    const tid = String(targetId || "").trim();
    if (viewerId === tid) {
      return { ok: true, userId: tid };
    }
    const row = await getUserCommunityRow(tid);
    if (!row) {
      return { ok: false, status: 404, error: "用户不存在" };
    }
    if (!Number(row.community_public)) {
      return { ok: false, status: 403, error: "未公开持仓" };
    }
    return { ok: true, userId: tid };
  }

  // home-bundle：直连 metrics，不 require server.js（避免冷启动 50s+ pending）
  if (
    req.method === "GET" &&
    (pathKey === "/api/metrics/home-bundle-diag" || pathKey === "/api/metrics/home-bundle")
  ) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const gate = await resolveMetricsBundleUserId(false);
      if (!gate.ok) {
        res.statusCode = gate.status;
        res.end(JSON.stringify({ ok: false, error: gate.error }));
        return;
      }
      const accountScope =
        String(getSearchParam(req, "account_id") || getSearchParam(req, "accountScope") || "all").trim() ||
        "all";
      const { probeMetricsHomeBundleDb, getMetricsHomeBundle } = require("../src/metrics-api-service");
      if (pathKey === "/api/metrics/home-bundle-diag") {
        const _diag = await probeMetricsHomeBundleDb(gate.userId, accountScope);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: { _diag, direct: true, build: "v8-home-bundle" } }));
        return;
      }
      const diagQ = String(getSearchParam(req, "diag") || "").trim().toLowerCase();
      const data = await getMetricsHomeBundle(gate.userId, accountScope, getSearchParam(req, "stages"), {
        diag: diagQ === "1" || diagQ === "true",
        diagOnly: diagQ === "only",
      });
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, data: { ...data, direct: true, build: "v8-home-bundle" } }));
      return;
    } catch (error) {
      console.error("[api/index.js] direct home-bundle error:", error);
      res.statusCode = 500;
      res.end(
        JSON.stringify({
          ok: false,
          error: error?.message || "home-bundle direct failed",
          build: "v8-home-bundle",
        }),
      );
      return;
    }
  }

  // analysis-bundle：与 home-bundle 相同直连策略（勿落入 Express 懒加载）
  const isPublicAnalysisBundle = /^\/api\/public\/[^/]+\/(?:metrics\/)?analysis-bundle$/.test(pathKey);
  if (req.method === "GET" && (pathKey === "/api/metrics/analysis-bundle" || isPublicAnalysisBundle)) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const gate = await resolveMetricsBundleUserId(isPublicAnalysisBundle);
      if (!gate.ok) {
        res.statusCode = gate.status;
        res.end(JSON.stringify({ ok: false, error: gate.error }));
        return;
      }
      const accountScope =
        String(getSearchParam(req, "account_id") || getSearchParam(req, "accountScope") || "all").trim() ||
        "all";
      const stage = String(getSearchParam(req, "stage") || "mtd").trim() || "mtd";
      const symbol = String(getSearchParam(req, "symbol") || "").trim();
      const customFrom = String(getSearchParam(req, "from") || "").slice(0, 10);
      const customTo = String(getSearchParam(req, "to") || "").slice(0, 10);
      const rangeOpts = {
        customFrom: customFrom || undefined,
        customTo: customTo || undefined,
      };
      const { getMetricsAnalysisBundle, getMetricsPublicAnalysisBundle } = require("../src/metrics-api-service");
      const data = isPublicAnalysisBundle
        ? await getMetricsPublicAnalysisBundle(gate.userId, accountScope, stage, symbol, rangeOpts)
        : await getMetricsAnalysisBundle(gate.userId, accountScope, stage, symbol, rangeOpts);
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, data: { ...data, direct: true, build: "v9-analysis-bundle-stock-rank" } }));
      return;
    } catch (error) {
      console.error("[api/index.js] direct analysis-bundle error:", error);
      res.statusCode = 500;
      res.end(
        JSON.stringify({
          ok: false,
          error: error?.message || "analysis-bundle direct failed",
          build: "v9-analysis-bundle-stock-rank",
        }),
      );
      return;
    }
  }

  // stock-record-bundle：与 home/analysis bundle 相同直连策略
  const isPublicStockRecordBundle = /^\/api\/public\/[^/]+\/metrics\/stock-record-bundle$/.test(pathKey);
  if (req.method === "GET" && (pathKey === "/api/metrics/stock-record-bundle" || isPublicStockRecordBundle)) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const gate = await resolveMetricsBundleUserId(isPublicStockRecordBundle);
      if (!gate.ok) {
        res.statusCode = gate.status;
        res.end(JSON.stringify({ ok: false, error: gate.error }));
        return;
      }
      const accountScope =
        String(getSearchParam(req, "account_id") || getSearchParam(req, "accountScope") || "all").trim() ||
        "all";
      const symbol = String(getSearchParam(req, "symbol") || "").trim();
      if (!symbol) {
        res.statusCode = 400;
        res.end(JSON.stringify({ ok: false, error: "missing symbol" }));
        return;
      }
      const {
        getMetricsStockRecordBundle,
        getMetricsPublicStockRecordBundle,
      } = require("../src/metrics-api-service");
      const chartOpts = (() => {
          const range = String(getSearchParam(req, "range") || "").trim().toLowerCase();
          if (["7", "30", "90", "mtd", "ytd", "all"].includes(range)) {
            return { chartRange: range };
          }
          const limit = Math.max(1, Math.min(200, parseInt(String(getSearchParam(req, "limit") || "30"), 10) || 30));
          const offset = Math.max(0, parseInt(String(getSearchParam(req, "offset") || "0"), 10) || 0);
          return { pointsLimit: limit, pointsOffset: offset };
        })();
      const data = isPublicStockRecordBundle
        ? await getMetricsPublicStockRecordBundle(gate.userId, accountScope, symbol, chartOpts)
        : await getMetricsStockRecordBundle(gate.userId, accountScope, symbol, chartOpts);
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, data: { ...data, direct: true, build: "v8-stock-record-bundle" } }));
      return;
    } catch (error) {
      console.error("[api/index.js] direct stock-record-bundle error:", error);
      res.statusCode = 500;
      res.end(
        JSON.stringify({
          ok: false,
          error: error?.message || "stock-record-bundle direct failed",
          build: "v8-stock-record-bundle",
        }),
      );
      return;
    }
  }

  const isPublicHomeBundle = /^\/api\/public\/[^/]+(?:\/home-bundle|\/metrics\/home-bundle)$/.test(pathKey);
  if (req.method === "GET" && isPublicHomeBundle) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const gate = await resolveMetricsBundleUserId(true);
      if (!gate.ok) {
        res.statusCode = gate.status;
        res.end(JSON.stringify({ ok: false, error: gate.error }));
        return;
      }
      const accountScope = String(getSearchParam(req, "accountScope") || "all").trim() || "all";
      const { getMetricsPublicHomeBundle } = require("../src/metrics-api-service");
      const accountId = String(getSearchParam(req, "account_id") || getSearchParam(req, "accountScope") || "all").trim() || "all";
      const data = await getMetricsPublicHomeBundle(gate.userId, accountId, getSearchParam(req, "stages"));
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, data: { ...data, direct: true, build: "v8-home-bundle" } }));
      return;
    } catch (error) {
      console.error("[api/index.js] direct public home-bundle error:", error);
      res.statusCode = 500;
      res.end(
        JSON.stringify({
          ok: false,
          error: error?.message || "public home-bundle direct failed",
          build: "v8-home-bundle",
        }),
      );
      return;
    }
  }

  const publicTradesMatch = pathKey.match(/^\/api\/public\/([^/]+)\/trades$/);
  if (req.method === "GET" && publicTradesMatch) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const { readUserIdFromRequest } = require("../src/auth-session");
      const { getPublicTradesPage, enrichPublicTradesWithTencent } = require("../src/community-service");
      const viewerId = readUserIdFromRequest(req);
      if (!viewerId) {
        res.statusCode = 401;
        res.end(JSON.stringify({ ok: false, error: "未登录" }));
        return;
      }
      const targetId = String(publicTradesMatch[1] || "").trim();
      const symbol = String(getSearchParam(req, "symbol") || "").trim();
      const accountId = String(getSearchParam(req, "account_id") || "").trim();
      const limit = getSearchParam(req, "limit");
      const offset = getSearchParam(req, "offset");
      const data = await getPublicTradesPage(viewerId, targetId, {
        symbol: symbol || undefined,
        accountId: accountId || undefined,
        limit,
        offset,
      });
      if (data.error === "unauthorized") {
        res.statusCode = 401;
        res.end(JSON.stringify({ ok: false, error: "未登录" }));
        return;
      }
      if (data.error === "hidden") {
        res.statusCode = 404;
        res.end(JSON.stringify({ ok: false, error: "用户未公开或不可见" }));
        return;
      }
      await enrichPublicTradesWithTencent(data);
      res.statusCode = 200;
      res.end(
        JSON.stringify({
          ok: true,
          data: data.data,
          pagination: data.pagination,
          direct: true,
          build: "v8-public-trades",
        }),
      );
      return;
    } catch (error) {
      console.error("[api/index.js] direct public trades error:", error);
      res.statusCode = 500;
      res.end(
        JSON.stringify({
          ok: false,
          error: error?.message || "public trades direct failed",
          build: "v8-public-trades",
        }),
      );
      return;
    }
  }

  // ---------------------------------------------------------
  // 极端防御：直接在 Vercel Handler 层拦截登录和用户信息接口，彻底绕过 Express
  // ---------------------------------------------------------
  const isMe = pathOnly === "/api/auth/me";
  const isLogin = pathOnly === "/api/auth/login";
  const isLogout = pathOnly === "/api/auth/logout";

  if (isMe || isLogin || isLogout) {
    try {
      console.log(`[api/index.js] direct-handle ${isMe ? 'me' : isLogin ? 'login' : 'logout'} start`);
      // Lazy require DB and Auth logic only when these routes are hit
      const { getUserPhone, getUserCommunityRow, verifyUserLogin } = require("../src/db");
      const { readUserIdFromRequest, setSessionCookie, clearSessionCookie } = require("../src/auth-session");
      const { maskPhone, displayNameForUser } = require("../src/community-service");

      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.setHeader("Cache-Control", "no-store");

      if (isLogout) {
        if (req.method !== "POST") {
          res.statusCode = 405;
          res.end(JSON.stringify({ ok: false, error: "Method Not Allowed" }));
          return;
        }
        clearSessionCookie(res);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true }));
        return;
      }

      if (isMe) {
        if (req.method !== "GET") {
          res.statusCode = 405;
          res.end(JSON.stringify({ ok: false, error: "Method Not Allowed" }));
          return;
        }
        const userId = readUserIdFromRequest(req);
        if (!userId) {
          res.statusCode = 401;
          res.end(JSON.stringify({ ok: false, error: "未登录" }));
          return;
        }
        const phone = await getUserPhone(userId);
        const row = await getUserCommunityRow(userId);
        res.statusCode = 200;
        res.end(JSON.stringify({
          ok: true,
          user: {
            id: userId,
            phone,
            phoneMasked: maskPhone(phone),
            nickname: row?.nickname != null && String(row.nickname).trim() ? String(row.nickname).trim() : null,
            communityPublic: row?.community_public != null ? !!Number(row.community_public) : true,
            displayName: row ? displayNameForUser(row) : maskPhone(phone),
          },
        }));
        return;
      }

      if (isLogin) {
        if (req.method !== "POST") {
          res.statusCode = 405;
          res.end(JSON.stringify({ ok: false, error: "Method Not Allowed" }));
          return;
        }

        // 读取 JSON body
        const bodyStr = await new Promise((resolve, reject) => {
          let data = '';
          req.on('data', chunk => data += chunk);
          req.on('end', () => resolve(data));
          req.on('error', reject);
        });

        let body = {};
        if (bodyStr) {
          try { body = JSON.parse(bodyStr); } catch (e) {}
        }

        const phone = body?.phone != null ? String(body.phone).trim() : "";
        const password = body?.password != null ? String(body.password) : "";

        const u = await verifyUserLogin(phone, password);
        if (!u) {
          res.statusCode = 401;
          res.end(JSON.stringify({ ok: false, error: "手机号或密码错误" }));
          return;
        }

        // 调用 auth-session.js 里的 setSessionCookie 来生成标准的 session token
        setSessionCookie(res, u.id);

        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, user: { phone: u.phone } }));
        return;
      }
    } catch (error) {
      console.error(`[api/index.js] direct-handle error:`, error);
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.end(JSON.stringify({
        ok: false,
        error: error?.message || "auth failed in direct handler",
      }));
      return;
    }
  }

  // --------- 直连：社区 API（与同上 auth，避免 Express 落到 SPA）---------
  const communityProfileMatch =
    pathOnly.match(/^\/api\/community\/users\/([^/]+)\/profile$/) || null;
  const communityFollowMatch = pathOnly.match(/^\/api\/community\/follow\/([^/]+)$/) || null;
  const isStateOrCommunityDirect =
    (req.method === "GET" &&
      (pathOnly === "/api/community/feed" ||
        pathOnly === "/api/community/following" ||
        pathOnly === "/api/community/leaderboard")) ||
    (req.method === "GET" && communityProfileMatch) ||
    (req.method === "PATCH" && pathOnly === "/api/me/community-profile") ||
    ((req.method === "POST" || req.method === "DELETE") && communityFollowMatch);

  const isCreateSymbolNameMapDirect =
    req.method === "POST" && pathOnly === "/api/admin/create-symbol-name-map";
  const isUpsertSymbolNameMapDirect =
    req.method === "POST" && pathOnly === "/api/admin/upsert-symbol-name-map";
  const isSymbolNameMapDirect =
    req.method === "GET" && pathOnly === "/api/symbol-name-map";
  const isSnapshotWatermarkDirect = req.method === "GET" && pathOnly === "/api/snapshot/watermark";
  const isSnapshotAccountDailyDirect = req.method === "GET" && pathOnly === "/api/snapshot/account-daily";
  const isSnapshotSymbolDailyDirect = req.method === "GET" && pathOnly === "/api/snapshot/symbol-daily";
  const isSnapshotSymbolCloseDirect = req.method === "GET" && pathOnly === "/api/snapshot/symbol-close";
  const isCronFreezeDirect = (req.method === "POST" || req.method === "GET") && pathOnly === "/api/cron/freeze-eod";
  const isCronDailyCloseDirect =
    (req.method === "POST" || req.method === "GET") && pathOnly === "/api/cron/sync-daily-close";
  const isSettingsGetDirect = req.method === "GET" && pathOnly === "/api/settings";
  const isSettingsPatchDirect = req.method === "PATCH" && pathOnly === "/api/settings";
  const tradesDeleteMatch = pathOnly.match(/^\/api\/trades\/([^/]+)$/) || null;
  const isTradesGetDirect = req.method === "GET" && pathOnly === "/api/trades";
  const isTradesPostDirect = req.method === "POST" && pathOnly === "/api/trades";
  const isTradesDeleteDirect = req.method === "DELETE" && !!tradesDeleteMatch;
  const isTradesImportDirect = req.method === "POST" && pathOnly === "/api/trades/import";
  const cashTransfersDeleteMatch = pathOnly.match(/^\/api\/cash-transfers\/([^/]+)$/) || null;
  const isCashTransfersGetDirect = req.method === "GET" && pathOnly === "/api/cash-transfers";
  const isCashTransfersPostDirect = req.method === "POST" && pathOnly === "/api/cash-transfers";
  const isCashTransfersDeleteDirect = req.method === "DELETE" && !!cashTransfersDeleteMatch;
  const isCashTransfersImportDirect = req.method === "POST" && pathOnly === "/api/cash-transfers/import";
  const isSinaSuggestDirect = req.method === "GET" && pathOnly === "/api/sina/suggest";

  if (isCreateSymbolNameMapDirect || isUpsertSymbolNameMapDirect || isSymbolNameMapDirect) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const { readUserIdFromRequest } = require("../src/auth-session");
      const userId = readUserIdFromRequest(req);
      if (!userId) {
        res.statusCode = 401;
        res.end(JSON.stringify({ ok: false, error: "请先登录" }));
        return;
      }
      const { createSymbolNameMapTableNow, getSymbolNameMap, normalizeSymbol } = require("../src/db");
      if (isCreateSymbolNameMapDirect) {
        const created = await createSymbolNameMapTableNow();
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, created }));
        return;
      }
      if (isUpsertSymbolNameMapDirect) {
        const { upsertSymbolNameMapBatch } = require("../src/db");
        const bodyStr = await new Promise((resolve, reject) => {
          let data = "";
          req.on("data", (chunk) => (data += chunk));
          req.on("end", () => resolve(data));
          req.on("error", reject);
        });
        let body = {};
        if (bodyStr) {
          try {
            body = JSON.parse(bodyStr);
          } catch (_) {}
        }
        const rows = Array.isArray(body?.rows) ? body.rows : [];
        const count = await upsertSymbolNameMapBatch(rows);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, count }));
        return;
      }
      const raw = req.query?.symbols != null ? String(req.query.symbols) : "";
      const symbols = [...new Set(raw.split(",").map((s) => normalizeSymbol(String(s || ""))).filter(Boolean))];
      if (!symbols.length) {
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: {} }));
        return;
      }
      const data = await getSymbolNameMap(symbols);
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, data }));
      return;
    } catch (error) {
      res.statusCode = 500;
      res.end(JSON.stringify({ ok: false, error: error?.message || "symbol name map direct failed" }));
      return;
    }
  }

  if (isSinaSuggestDirect) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    const key = String(getSearchParam(req, "key") || "").trim();
    if (!key || key.length > 64) {
      res.statusCode = 400;
      res.end(JSON.stringify({ ok: false, error: "invalid key" }));
      return;
    }
    const cacheKey = key.toLowerCase();
    const cached = getCacheHit(sinaSuggestCache, cacheKey, DIRECT_SINA_SUGGEST_CACHE_TTL_MS);
    if (Array.isArray(cached)) {
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, results: cached, source: "cache" }));
      return;
    }
    try {
      let results = null;
      try {
        results = await fetchSinaSuggestFromAliyun(key);
      } catch (error) {
        console.warn("[api/index.js] aliyun suggest failed:", error?.message || error);
      }
      if (!Array.isArray(results)) {
        results = await fetchSinaSuggestFromUpstream(key);
      }
      setCacheValue(sinaSuggestCache, cacheKey, results);
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, results }));
      return;
    } catch (error) {
      res.statusCode = 502;
      res.end(JSON.stringify({ ok: false, error: error?.message || "sina suggest failed" }));
      return;
    }
  }

  if (isTradesGetDirect || isTradesPostDirect || isTradesDeleteDirect) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const { readUserIdFromRequest } = require("../src/auth-session");
      const userId = readUserIdFromRequest(req);
      if (!userId) {
        res.statusCode = 401;
        res.end(JSON.stringify({ ok: false, error: "请先登录" }));
        return;
      }
      const {
        getTrades,
        getTradesForSymbol,
        getTradesPageForSymbol,
        getTradesPage,
        normalizeTrade,
        getTradeByIdForUser,
        upsertTrade,
        deleteTradeById,
        normalizeSymbol: dbNormalizeSymbol,
      } = require("../src/db");
      const { notifyLedgerMutation, hintDatesFromTradeMutation } = require("../src/metrics-invalidate");

      if (isTradesGetDirect) {
        const symbolRaw = getSearchParam(req, "symbol");
        if (symbolRaw != null && String(symbolRaw).trim() !== "") {
          const symbol = dbNormalizeSymbol(String(symbolRaw).trim());
          if (!symbol) {
            res.statusCode = 400;
            res.end(JSON.stringify({ ok: false, error: "invalid symbol" }));
            return;
          }
          const limit = Math.min(100, Math.max(1, parseInt(String(getSearchParam(req, "limit") || "10"), 10) || 10));
          const offset = Math.max(0, parseInt(String(getSearchParam(req, "offset") || "0"), 10) || 0);
          const accountIdRaw = String(getSearchParam(req, "accountId") || "all").trim();
          const accountId = accountIdRaw && accountIdRaw !== "all" ? accountIdRaw : null;
          const { data, pagination } = await getTradesPageForSymbol(userId, symbol, {
            limit,
            offset,
            accountId,
          });
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, data, pagination }));
          return;
        }
        const limitRaw = getSearchParam(req, "limit");
        if (limitRaw != null && String(limitRaw).trim() !== "") {
          const limit = Math.min(100, Math.max(1, parseInt(String(limitRaw), 10) || 10));
          const offset = Math.max(0, parseInt(String(getSearchParam(req, "offset") || "0"), 10) || 0);
          const accountIdRaw = String(getSearchParam(req, "accountId") || "all").trim();
          const accountId = accountIdRaw && accountIdRaw !== "all" ? accountIdRaw : null;
          const { data, pagination } = await getTradesPage(userId, { limit, offset, accountId });
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, data, pagination }));
          return;
        }
        const data = await getTrades(userId);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (isTradesPostDirect) {
        const body = await readJsonBody(req);
        const trade = normalizeTrade(body || {});
        if (!trade.symbol) {
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: "symbol is required" }));
          return;
        }
        const prior = trade.id ? await getTradeByIdForUser(trade.id, userId) : null;
        const saved = await upsertTrade(trade, userId);
        notifyLedgerMutation(userId, { hintDates: hintDatesFromTradeMutation(prior, saved) });
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: saved }));
        return;
      }

      if (isTradesDeleteDirect) {
        const tradeId = decodeURIComponent(String(tradesDeleteMatch?.[1] || "").trim());
        if (!tradeId) {
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: "invalid trade id" }));
          return;
        }
        const del = await deleteTradeById(tradeId, userId);
        if (del.deleted) {
          notifyLedgerMutation(userId, { hintDates: del.date ? [del.date] : [] });
        }
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, deleted: del.deleted }));
        return;
      }
    } catch (error) {
      console.error("[api/index.js] direct trades error:", error);
      res.statusCode = 500;
      res.end(JSON.stringify({ ok: false, error: error?.message || "direct trades failed" }));
      return;
    }
  }

  if (isCashTransfersGetDirect || isCashTransfersPostDirect || isCashTransfersDeleteDirect) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const { readUserIdFromRequest } = require("../src/auth-session");
      const userId = readUserIdFromRequest(req);
      if (!userId) {
        res.statusCode = 401;
        res.end(JSON.stringify({ ok: false, error: "请先登录" }));
        return;
      }
      const {
        getCashTransfers,
        getCashTransfersPage,
        getCashTransferByIdForUser,
        upsertCashTransfer,
        deleteCashTransferById,
      } = require("../src/db");
      const { notifyLedgerMutation, hintDatesFromCashMutation } = require("../src/metrics-invalidate");

      if (isCashTransfersGetDirect) {
        const limitRaw = getSearchParam(req, "limit");
        if (limitRaw != null && String(limitRaw).trim() !== "") {
          const limit = Math.min(100, Math.max(1, parseInt(String(limitRaw), 10) || 10));
          const offset = Math.max(0, parseInt(String(getSearchParam(req, "offset") || "0"), 10) || 0);
          const accountIdRaw = String(getSearchParam(req, "accountId") || "all").trim();
          const accountId = accountIdRaw && accountIdRaw !== "all" ? accountIdRaw : null;
          const { data, pagination } = await getCashTransfersPage(userId, { limit, offset, accountId });
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, data, pagination }));
          return;
        }
        const data = await getCashTransfers(userId);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (isCashTransfersPostDirect) {
        const body = await readJsonBody(req);
        const prior = body?.id ? await getCashTransferByIdForUser(body.id, userId) : null;
        const saved = await upsertCashTransfer(body || {}, userId);
        notifyLedgerMutation(userId, { hintDates: hintDatesFromCashMutation(prior, saved) });
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: saved }));
        return;
      }

      if (isCashTransfersDeleteDirect) {
        const cashId = decodeURIComponent(String(cashTransfersDeleteMatch?.[1] || "").trim());
        if (!cashId) {
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: "invalid cash transfer id" }));
          return;
        }
        const del = await deleteCashTransferById(cashId, userId);
        if (del.deleted) {
          notifyLedgerMutation(userId, { hintDates: del.date ? [del.date] : [] });
        }
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, deleted: del.deleted }));
        return;
      }
    } catch (error) {
      console.error("[api/index.js] direct cash transfers error:", error);
      res.statusCode = 500;
      res.end(JSON.stringify({ ok: false, error: error?.message || "direct cash transfers failed" }));
      return;
    }
  }

  if (
    isSettingsGetDirect ||
    isSettingsPatchDirect ||
    isTradesImportDirect ||
    isCashTransfersImportDirect
  ) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const { readUserIdFromRequest } = require("../src/auth-session");
      const userId = readUserIdFromRequest(req);
      if (!userId) {
        res.statusCode = 401;
        res.end(JSON.stringify({ ok: false, error: "请先登录" }));
        return;
      }
      const {
        DEFAULT_SETTINGS,
        getSettings,
        setSettings,
        normalizeTrade,
        importTrades,
        importCashTransfers,
      } = require("../src/db");
      const {
        notifyLedgerMutation,
        hintDatesFromImportRows,
      } = require("../src/metrics-invalidate");

      if (isSettingsGetDirect) {
        const data = await getSettings(userId);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (isSettingsPatchDirect) {
        const body = await readJsonBody(req);
        const patch = body && typeof body === "object" ? body : {};
        const sanitized = {};
        for (const key of Object.keys(DEFAULT_SETTINGS || {})) {
          if (Object.hasOwn(patch, key)) {
            sanitized[key] = patch[key];
          }
        }
        const data = await setSettings(sanitized, userId);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (isTradesImportDirect) {
        const body = await readJsonBody(req);
        const mode = body?.mode === "replace" ? "replace" : "append";
        const trades = Array.isArray(body?.trades) ? body.trades : [];
        const normalized = trades.map((item) => normalizeTrade(item));
        const data = await importTrades(normalized, mode, userId);
        notifyLedgerMutation(userId, {
          fullRebuild: mode === "replace",
          hintDates: hintDatesFromImportRows(normalized, "date"),
        });
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, count: data.length, data }));
        return;
      }

      if (isCashTransfersImportDirect) {
        const body = await readJsonBody(req);
        const mode = body?.mode === "replace" ? "replace" : "append";
        const rows = Array.isArray(body?.cashTransfers) ? body.cashTransfers : [];
        const data = await importCashTransfers(rows, mode, userId);
        notifyLedgerMutation(userId, {
          fullRebuild: mode === "replace",
          hintDates: hintDatesFromImportRows(rows, "date"),
        });
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, count: data.length, data }));
        return;
      }
    } catch (error) {
      console.error("[api/index.js] direct settings/import error:", error);
      res.statusCode = 500;
      res.end(JSON.stringify({ ok: false, error: error?.message || "direct settings/import failed" }));
      return;
    }
  }
  if (
    isSnapshotWatermarkDirect ||
    isSnapshotAccountDailyDirect ||
    isSnapshotSymbolDailyDirect ||
    isSnapshotSymbolCloseDirect ||
    isCronFreezeDirect ||
    isCronDailyCloseDirect
  ) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const { readUserIdFromRequest } = require("../src/auth-session");
      const {
        normalizeSymbol,
        getSnapshotWatermark,
        getAnalysisDailySnapshots,
        getSymbolDailyPnl,
        getTradeWindowForDailyClose,
        getSymbolDailyCloseRange,
      } = require("../src/db");
      const { runDailyFreeze } = require("../src/eod-freeze-service");
      const { runDailyCloseSync } = require("../src/daily-close-sync-service");

      if (isSnapshotWatermarkDirect) {
        const data = await getSnapshotWatermark();
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (isCronFreezeDirect) {
        const body = req.method === "POST" ? await readJsonBody(req) : {};
        const cronHeader = req.headers?.["x-vercel-cron"];
        const configuredSecret = String(process.env.CRON_SECRET || process.env.VERCEL_CRON_SECRET || "").trim();
        const tokenFromBearer = extractBearerToken(req);
        const tokenFromQuery = getSearchParam(req, "token");
        const tokenFromBody = String(body?.token || "").trim();
        const secretMatched =
          !!configuredSecret &&
          (tokenFromBearer === configuredSecret ||
            tokenFromQuery === configuredSecret ||
            tokenFromBody === configuredSecret);
        const sessionUserId = readUserIdFromRequest(req);
        if (!sessionUserId && cronHeader == null && !secretMatched) {
          res.statusCode = 401;
          res.end(JSON.stringify({ ok: false, error: "unauthorized cron request" }));
          return;
        }
        const frozenDate = getSearchParam(req, "frozenDate") || body?.frozenDate;
        const force = parseBooleanInput(getSearchParam(req, "force") || body?.force, false);
        const syncDailyClose = parseBooleanInput(getSearchParam(req, "syncDailyClose") || body?.syncDailyClose, false);
        const userIdFromQuery = getSearchParam(req, "userId");
        const userIds = Array.isArray(body?.userIds) && body.userIds.length
          ? body.userIds
          : userIdFromQuery ? [userIdFromQuery] : [];
        const fromCron = cronHeader != null;
        const data = await runDailyFreeze({
          frozenDate,
          force,
          syncDailyClose,
          userIds,
          fromCron,
          logger: console,
        });
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (isCronDailyCloseDirect) {
        const body = req.method === "POST" ? await readJsonBody(req) : {};
        const cronHeader = req.headers?.["x-vercel-cron"];
        const configuredSecret = String(process.env.CRON_SECRET || process.env.VERCEL_CRON_SECRET || "").trim();
        const tokenFromBearer = extractBearerToken(req);
        const tokenFromQuery = getSearchParam(req, "token");
        const tokenFromBody = String(body?.token || "").trim();
        const secretMatched =
          !!configuredSecret &&
          (tokenFromBearer === configuredSecret ||
            tokenFromQuery === configuredSecret ||
            tokenFromBody === configuredSecret);
        const sessionUserId = readUserIdFromRequest(req);
        if (!sessionUserId && cronHeader == null && !secretMatched) {
          res.statusCode = 401;
          res.end(JSON.stringify({ ok: false, error: "unauthorized cron request" }));
          return;
        }
        const asOfDate = getSearchParam(req, "asOfDate") || body?.asOfDate;
        const symbolsFromBody = Array.isArray(body?.symbols) ? body.symbols : [];
        const symbolsFromQuery = sanitizeSymbolList(getSearchParam(req, "symbols"), normalizeSymbol);
        const symbols = [...new Set([...symbolsFromBody, ...symbolsFromQuery])]
          .map((symbol) => normalizeSymbol(symbol))
          .filter(Boolean);
        const data = await runDailyCloseSync({ asOfDate, symbols, logger: console });
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      const userId = readUserIdFromRequest(req);
      if (!userId) {
        res.statusCode = 401;
        res.end(JSON.stringify({ ok: false, error: "请先登录" }));
        return;
      }

      if (isSnapshotAccountDailyDirect) {
        const accountId = getSearchParam(req, "accountId");
        const from = getSearchParam(req, "from") || "1970-01-01";
        const to = getSearchParam(req, "to") || "9999-12-31";
        const data = await getAnalysisDailySnapshots({ accountId, from, to }, userId);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (isSnapshotSymbolDailyDirect) {
        const accountId = getSearchParam(req, "accountId");
        const from = getSearchParam(req, "from") || "1970-01-01";
        const to = getSearchParam(req, "to") || "9999-12-31";
        const symbols = sanitizeSymbolList(getSearchParam(req, "symbols"), normalizeSymbol);
        let data = [];
        if (!symbols.length) {
          const symbol = getSearchParam(req, "symbol");
          data = await getSymbolDailyPnl({ accountId, from, to, symbol }, userId);
        } else {
          const chunks = await Promise.all(
            symbols.map((symbol) => getSymbolDailyPnl({ accountId, from, to, symbol }, userId))
          );
          data = chunks.flat();
          data.sort((a, b) => {
            if (a.date !== b.date) {
              return String(a.date).localeCompare(String(b.date));
            }
            return String(a.symbol).localeCompare(String(b.symbol));
          });
        }
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (isSnapshotSymbolCloseDirect) {
        const w = await getTradeWindowForDailyClose(userId);
        if (!w.symbols.length) {
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, data: {}, from: null, to: null, symbols: [] }));
          return;
        }
        const requested = sanitizeSymbolList(getSearchParam(req, "symbols"), normalizeSymbol);
        const wantedSet = requested.length ? new Set(requested) : null;
        const symbols = wantedSet ? w.symbols.filter((sym) => wantedSet.has(sym)) : w.symbols;
        if (!symbols.length) {
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, data: {}, from: null, to: null, symbols: [] }));
          return;
        }
        const days = parsePositiveInt(getSearchParam(req, "days"), 240, 30, 4000);
        const dateKeyDaysFromToday = (delta) => {
          const d = new Date();
          d.setDate(d.getDate() + Number(delta || 0));
          return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
        };
        const to = w.to || dateKeyDaysFromToday(0);
        const floorFrom = dateKeyDaysFromToday(-days);
        const from = w.from && w.from > floorFrom ? w.from : floorFrom;
        const data = {};
        const rowsBySymbol = await Promise.all(
          symbols.map(async (sym) => [sym, await getSymbolDailyCloseRange(sym, from, to)])
        );
        for (const [sym, rows] of rowsBySymbol) {
          data[sym] = rows;
        }
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data, from, to, symbols }));
        return;
      }
    } catch (error) {
      console.error("[api/index.js] direct snapshot/cron error:", error);
      res.statusCode = 500;
      res.end(JSON.stringify({ ok: false, error: error?.message || "snapshot/cron direct handler failed" }));
      return;
    }
  }

  if (isStateOrCommunityDirect) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const { readUserIdFromRequest } = require("../src/auth-session");
      const userId = readUserIdFromRequest(req);
      if (!userId) {
        res.statusCode = 401;
        res.end(JSON.stringify({ ok: false, error: "请先登录" }));
        return;
      }

      console.log("[api/index.js] direct-handle community path=%s method=%s", pathOnly, req.method);

      const {
        updateUserCommunityProfile,
        setCommunityFollow,
        removeCommunityFollow,
        isCommunityFollowing,
      } = require("../src/db");
      const {
        getLeaderboard,
        getFollowingCards,
        getFeedTrades,
        enrichFeedRowsWithTencent,
        enrichCardsTopPositionsWithTencent,
        enrichLeaderboardPayloadWithTencent,
        enrichLeaderboardPayloadWithViewer,
        getPublicProfileDetail,
        enrichPublicProfileDetailWithTencent,
        displayNameForUser,
      } = require("../src/community-service");

      const readRawBody = () =>
        new Promise((resolve, reject) => {
          let data = "";
          req.on("data", (chunk) => (data += chunk));
          req.on("end", () => resolve(data));
          req.on("error", reject);
        });

      if (req.method === "GET" && pathOnly === "/api/community/feed") {
        const rows = await getFeedTrades(userId);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: rows }));
        return;
      }

      if (req.method === "GET" && pathOnly === "/api/community/following") {
        const cards = await getFollowingCards(userId);
        await enrichCardsTopPositionsWithTencent(cards);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: cards }));
        return;
      }

      if (req.method === "GET" && pathOnly === "/api/community/leaderboard") {
        const data = await getLeaderboard();
        await enrichLeaderboardPayloadWithViewer(data, userId);
        await enrichLeaderboardPayloadWithTencent(data);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (req.method === "GET" && communityProfileMatch) {
        const targetId = String(communityProfileMatch[1] || "").trim();
        const detail = await getPublicProfileDetail(userId, targetId);
        if (detail.error === "unauthorized") {
          res.statusCode = 401;
          res.end(JSON.stringify({ ok: false, error: "未登录" }));
          return;
        }
        if (detail.error === "hidden") {
          res.statusCode = 404;
          res.end(JSON.stringify({ ok: false, error: "用户未公开或不可见" }));
          return;
        }
        await enrichPublicProfileDetailWithTencent(detail);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: detail }));
        return;
      }

      if (req.method === "PATCH" && pathOnly === "/api/me/community-profile") {
        let body = {};
        const raw = await readRawBody();
        if (raw) {
          try {
            body = JSON.parse(raw);
          } catch (_) {}
        }
        try {
          const u = await updateUserCommunityProfile(userId, {
            nickname: body.nickname,
            communityPublic: body.communityPublic,
          });
          res.statusCode = 200;
          res.end(
            JSON.stringify({
              ok: true,
              profile: {
                nickname: u.nickname != null && String(u.nickname).trim() ? String(u.nickname).trim() : null,
                communityPublic: !!Number(u.community_public),
                displayName: displayNameForUser(u),
              },
            })
          );
        } catch (error) {
          const msg = error?.message || "更新失败";
          if (msg.includes("nickname taken")) {
            res.statusCode = 409;
            res.end(JSON.stringify({ ok: false, error: "昵称已被占用" }));
            return;
          }
          if (msg.includes("too long")) {
            res.statusCode = 400;
            res.end(JSON.stringify({ ok: false, error: "昵称最长 20 个字符" }));
            return;
          }
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: msg }));
        }
        return;
      }

      if (communityFollowMatch && (req.method === "POST" || req.method === "DELETE")) {
        const targetId = String(communityFollowMatch[1] || "").trim();
        if (!targetId) {
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: "invalid target" }));
          return;
        }
        if (req.method === "POST") {
          await setCommunityFollow(userId, targetId);
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, following: await isCommunityFollowing(userId, targetId) }));
        } else {
          await removeCommunityFollow(userId, targetId);
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true }));
        }
        return;
      }
    } catch (error) {
      console.error("[api/index.js] direct state/community error:", error);
      if (!res.headersSent) {
        res.statusCode = 500;
        res.setHeader("Content-Type", "application/json; charset=utf-8");
        res.end(JSON.stringify({ ok: false, error: error?.message || "state/community handler failed" }));
      }
      return;
    }
  }

  // 不依赖 server.js 的诊断端点：证明 /api/(.*) 这条路由至少能到达函数
  if (req.url && urlPathOnly(req.url).startsWith("/api/diag/v5")) {
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    res.end(
      JSON.stringify({
        ok: true,
        build: "v6",
        where: "api/index.js (before server.js require)",
        node: process.version,
        env: process.env.VERCEL ? "vercel" : "local",
        url: req.url,
        ts: Date.now(),
      })
    );
    return;
  }

  // 其他请求：按需加载 server.js，带 20s 硬超时
  try {
    const loadTimeoutMs = 20000;
    const slaPromise = new Promise((_, reject) => {
      setTimeout(
        () => reject(new Error(`server.js require timeout after ${loadTimeoutMs}ms`)),
        loadTimeoutMs
      );
    });
    const serverlessApp = await Promise.race([getServerlessApp(), slaPromise]);
    return serverlessApp(req, res);
  } catch (e) {
    console.error("[api/index.js] handler error:", e?.stack || e?.message || e);
    if (!res.headersSent) {
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.end(
        JSON.stringify({
          ok: false,
          error: "api/index.js load/handler failed",
          message: e?.message || String(e),
          stackHead: (e?.stack || "").split("\n").slice(0, 5).join(" | "),
        })
      );
    }
  }
};