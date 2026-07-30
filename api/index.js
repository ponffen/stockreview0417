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

async function getSubscriptionExpiredPayload(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return null;
  }
  const { getUserValidUntil } = require("../src/db");
  const { isSubscriptionExpired } = require("../src/user-subscription");
  const validUntil = await getUserValidUntil(uid);
  if (!isSubscriptionExpired(validUntil)) {
    return null;
  }
  return { ok: false, status: 403, error: "免费使用已结束", code: "subscription_expired" };
}

function endJsonPayload(res, payload, statusCode = payload?.status || 500) {
  res.statusCode = statusCode;
  res.end(JSON.stringify(payload));
}

async function fetchSinaSuggestFromUpstream(key) {
  const iconv = require("iconv-lite");
  const { parseSinaSuggestText, suggestLineToItem, publicSearchResults } = require("../src/sina-suggest");
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
  return publicSearchResults(results);
}

async function fetchSinaSuggestFromAliyun(key) {
  const { publicSearchResults } = require("../src/sina-suggest");
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
  return publicSearchResults(json.results);
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

function cronSecretFromEnv() {
  return String(process.env.CRON_SECRET || process.env.VERCEL_CRON_SECRET || "").trim();
}

function isCronSecretMatched(req, body, configuredSecret) {
  if (!configuredSecret) {
    return false;
  }
  const tokenFromBearer = extractBearerToken(req);
  const tokenFromQuery = getSearchParam(req, "token");
  const tokenFromBody = String(body?.token || "").trim();
  const tokenFromHeader = String(req.headers?.["x-cron-secret"] || "").trim();
  return (
    tokenFromBearer === configuredSecret ||
    tokenFromQuery === configuredSecret ||
    tokenFromBody === configuredSecret ||
    tokenFromHeader === configuredSecret
  );
}

// 最外层 handler：先处理"不依赖 server.js"的自证端点，再异步加载 Express app
module.exports = async function handler(req, res, context) {
  const { runWithRequestContext } = require("../src/background-task");
  return runWithRequestContext(context, async () => {
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

  try {
    const { handleMcpDirectRoute } = require("../src/mcp/direct-routes");
    if (await handleMcpDirectRoute(req, res, pathKey)) {
      return;
    }
  } catch (error) {
    console.error("[api/index.js] mcp direct route error:", error);
    if (!res.headersSent) {
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.end(JSON.stringify({ ok: false, error: error?.message || "mcp route failed" }));
    }
    return;
  }

  async function resolveMetricsBundleUserId(publicPath) {
    const { readUserIdFromRequest } = require("../src/auth-session");
    const { getUserCommunityRow } = require("../src/db");
    const viewerId = readUserIdFromRequest(req);
    if (!viewerId) {
      if (!publicPath) {
        return { ok: false, status: 401, error: "请先登录" };
      }
    } else {
      const subExpired = await getSubscriptionExpiredPayload(viewerId);
      if (subExpired) {
        return subExpired;
      }
    }
    if (!publicPath) {
      return { ok: true, userId: viewerId };
    }
    const targetId = pathKey.split("/")[3];
    const tid = String(targetId || "").trim();
    if (viewerId && viewerId === tid) {
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

  if (req.method === "GET" && pathKey === "/api/guest/community/feed-preview") {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const { handleGuestFeedPreview } = require("../src/dynamics/dynamics-api");
      const result = await handleGuestFeedPreview();
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, data: result.data, pagination: result.pagination }));
    } catch (error) {
      res.statusCode = 500;
      res.end(JSON.stringify({ ok: false, error: error?.message || "guest feed preview failed" }));
    }
    return;
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
        res.end(JSON.stringify({ ok: false, error: gate.error, code: gate.code || null }));
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
        res.end(JSON.stringify({ ok: false, error: gate.error, code: gate.code || null }));
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
        res.end(JSON.stringify({ ok: false, error: gate.error, code: gate.code || null }));
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
          const range = String(getSearchParam(req, "range") || getSearchParam(req, "chartRange") || "30")
            .trim()
            .toLowerCase();
          if (["7", "30", "90", "mtd", "ytd", "all"].includes(range)) {
            return { chartRange: range };
          }
          return { chartRange: "30" };
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

  const isPublicCacheMeta = /^\/api\/public\/[^/]+\/cache-meta$/.test(pathKey);
  if (req.method === "GET" && isPublicCacheMeta) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const gate = await resolveMetricsBundleUserId(true);
      if (!gate.ok) {
        res.statusCode = gate.status;
        res.end(JSON.stringify({ ok: false, error: gate.error, code: gate.code || null }));
        return;
      }
      const { getCacheMeta } = require("../src/cache-epoch");
      const data = await getCacheMeta(gate.userId);
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, data }));
      return;
    } catch (error) {
      console.error("[api/index.js] direct public cache-meta error:", error);
      res.statusCode = 500;
      res.end(
        JSON.stringify({
          ok: false,
          error: error?.message || "public cache-meta direct failed",
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
        res.end(JSON.stringify({ ok: false, error: gate.error, code: gate.code || null }));
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

  function dynamicsReq(req) {
    return {
      query: {
        limit: getSearchParam(req, "limit"),
        cursor: getSearchParam(req, "cursor"),
        filter: getSearchParam(req, "filter"),
      },
    };
  }

  const dynamicsStockMatch = pathKey.match(/^\/api\/dynamics\/stock\/([^/]+)$/);
  const publicDynamicsStockMatch = pathKey.match(/^\/api\/public\/([^/]+)\/dynamics\/stock\/([^/]+)$/);
  const publicDynamicsMatch = pathKey.match(/^\/api\/public\/([^/]+)\/dynamics$/);
  const communityPostMatch = pathKey.match(/^\/api\/community\/posts\/([^/]+)$/);

  if (req.method === "GET" && (publicDynamicsMatch || publicDynamicsStockMatch)) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const { readUserIdFromRequest } = require("../src/auth-session");
      const viewerId = readUserIdFromRequest(req) || null;
      if (viewerId) {
        const subExpired = await getSubscriptionExpiredPayload(viewerId);
        if (subExpired) {
          endJsonPayload(res, subExpired, subExpired.status);
          return;
        }
      }
      const { handlePublicDynamics, handlePublicStockDynamics } = require("../src/dynamics/dynamics-api");
      if (publicDynamicsMatch) {
        const targetId = String(publicDynamicsMatch[1] || "").trim();
        const result = await handlePublicDynamics(dynamicsReq(req), viewerId, targetId);
        if (result.error === "hidden") {
          res.statusCode = 404;
          res.end(JSON.stringify({ ok: false, error: "用户未公开或不可见" }));
          return;
        }
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: result.data, pagination: result.pagination }));
        return;
      }
      const targetId = String(publicDynamicsStockMatch[1] || "").trim();
      const symbol = String(publicDynamicsStockMatch[2] || "").trim();
      const result = await handlePublicStockDynamics(dynamicsReq(req), viewerId, targetId, symbol);
      if (result.error === "hidden") {
        res.statusCode = 404;
        res.end(JSON.stringify({ ok: false, error: "用户未公开或不可见" }));
        return;
      }
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, data: result.data, pagination: result.pagination }));
      return;
    } catch (error) {
      res.statusCode = 500;
      res.end(JSON.stringify({ ok: false, error: error?.message || "public dynamics direct failed" }));
      return;
    }
  }

  if (req.method === "GET" && pathKey === "/api/dynamics/images/view") {
    try {
      const { handleViewDynamicsImage } = require("../src/dynamics/dynamics-api");
      const viewReq = {
        query: {
          path: getSearchParam(req, "path"),
          u: getSearchParam(req, "u"),
        },
      };
      await handleViewDynamicsImage(viewReq, res);
    } catch (error) {
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.end(JSON.stringify({ ok: false, error: error?.message || "image view failed" }));
    }
    return;
  }

  const isDynamicsDirect =
    (req.method === "GET" && (pathKey === "/api/dynamics" || !!dynamicsStockMatch || pathKey === "/api/community/feed")) ||
    (req.method === "POST" && (pathKey === "/api/dynamics/images" || pathKey === "/api/community/posts")) ||
    ((req.method === "PATCH" || req.method === "DELETE") && !!communityPostMatch);

  if (isDynamicsDirect) {
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
      const subExpired = await getSubscriptionExpiredPayload(userId);
      if (subExpired) {
        endJsonPayload(res, subExpired, subExpired.status);
        return;
      }
      const {
        handleCommunityFeed,
        handleSelfDynamics,
        handlePublicDynamics,
        handleSelfStockDynamics,
        handlePublicStockDynamics,
        handleUploadDynamicsImage,
        createCommunityPost,
        updateCommunityPost,
        deleteCommunityPost,
      } = require("../src/dynamics/dynamics-api");

      const readRawBody = () =>
        new Promise((resolve, reject) => {
          let data = "";
          req.on("data", (chunk) => (data += chunk));
          req.on("end", () => resolve(data));
          req.on("error", reject);
        });

      if (req.method === "GET" && pathKey === "/api/community/feed") {
        const result = await handleCommunityFeed(dynamicsReq(req), userId);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: result.data, pagination: result.pagination }));
        return;
      }

      if (req.method === "GET" && pathKey === "/api/dynamics") {
        const result = await handleSelfDynamics(dynamicsReq(req), userId);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: result.data, pagination: result.pagination }));
        return;
      }

      if (req.method === "GET" && dynamicsStockMatch) {
        const symbol = String(dynamicsStockMatch[1] || "").trim();
        const result = await handleSelfStockDynamics(dynamicsReq(req), userId, symbol);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: result.data, pagination: result.pagination }));
        return;
      }

      if (req.method === "POST" && pathKey === "/api/dynamics/images") {
        try {
          const data = await handleUploadDynamicsImage(req, userId);
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, data }));
        } catch (error) {
          const code = error?.code === "BLOB_NOT_CONFIGURED" ? 503 : 400;
          res.statusCode = code;
          res.end(JSON.stringify({ ok: false, error: error?.message || "upload failed" }));
        }
        return;
      }

      if (req.method === "POST" && pathKey === "/api/community/posts") {
        let body = {};
        const raw = await readRawBody();
        if (raw) {
          try {
            body = JSON.parse(raw);
          } catch (_) {}
        }
        const data = await createCommunityPost(userId, body);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (communityPostMatch) {
        const postId = String(communityPostMatch[1] || "").trim();
        if (req.method === "PATCH") {
          let body = {};
          const raw = await readRawBody();
          if (raw) {
            try {
              body = JSON.parse(raw);
            } catch (_) {}
          }
          const data = await updateCommunityPost(userId, postId, body);
          if (!data) {
            res.statusCode = 404;
            res.end(JSON.stringify({ ok: false, error: "帖子不存在" }));
            return;
          }
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, data }));
          return;
        }
        if (req.method === "DELETE") {
          const result = await deleteCommunityPost(userId, postId);
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, deleted: !!result.deleted }));
          return;
        }
      }
    } catch (error) {
      res.statusCode = 500;
      res.end(JSON.stringify({ ok: false, error: error?.message || "dynamics direct failed" }));
      return;
    }
  }

  const publicTradesMatch = pathKey.match(/^\/api\/public\/([^/]+)\/trades$/);
  if (req.method === "GET" && publicTradesMatch) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    try {
      const { readUserIdFromRequest } = require("../src/auth-session");
      const { getPublicTradesPage } = require("../src/community-service");
      const viewerId = readUserIdFromRequest(req);
      if (!viewerId) {
        res.statusCode = 401;
        res.end(JSON.stringify({ ok: false, error: "未登录" }));
        return;
      }
      const subExpired = await getSubscriptionExpiredPayload(viewerId);
      if (subExpired) {
        endJsonPayload(res, subExpired, subExpired.status);
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
  const isRegister = pathOnly === "/api/auth/register";

  if (isMe || isLogin || isLogout || isRegister) {
    try {
      console.log(
        `[api/index.js] direct-handle ${isMe ? "me" : isLogin ? "login" : isLogout ? "logout" : "register"} start`
      );
      // Lazy require DB and Auth logic only when these routes are hit
      const {
        verifyUserLogin,
        createRegisteredUser,
        getAuthSessionUserPayload,
      } = require("../src/db");
      const { isValidPhone, isValidPasswordDigits } = require("../src/password");
      const { REGISTER_INVITE_CODE } = require("../src/register-invite");
      const { readUserIdFromRequest, setSessionCookie, clearSessionCookie } = require("../src/auth-session");

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
        const user = await getAuthSessionUserPayload(userId);
        res.statusCode = 200;
        res.end(JSON.stringify({
          ok: true,
          user,
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

        const user = await getAuthSessionUserPayload(u.id);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, user }));
        return;
      }

      if (isRegister) {
        if (req.method !== "POST") {
          res.statusCode = 405;
          res.end(JSON.stringify({ ok: false, error: "Method Not Allowed" }));
          return;
        }

        const bodyStr = await new Promise((resolve, reject) => {
          let data = "";
          req.on("data", (chunk) => {
            data += chunk;
          });
          req.on("end", () => resolve(data));
          req.on("error", reject);
        });

        let body = {};
        if (bodyStr) {
          try {
            body = JSON.parse(bodyStr);
          } catch (_) {
            body = {};
          }
        }

        const phone = body?.phone != null ? String(body.phone).trim() : "";
        const password = body?.password != null ? String(body.password) : "";
        const inviteCode = body?.inviteCode != null ? String(body.inviteCode).trim() : "";

        if (!isValidPhone(phone)) {
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: "请输入 11 位手机号" }));
          return;
        }
        if (!isValidPasswordDigits(password)) {
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: "密码为不少于 6 位的数字" }));
          return;
        }
        if (inviteCode !== REGISTER_INVITE_CODE) {
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: "邀请码错误" }));
          return;
        }

        try {
          const u = await createRegisteredUser(phone, password);
          setSessionCookie(res, u.id);
          const user = await getAuthSessionUserPayload(u.id);
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, user }));
        } catch (error) {
          const msg = error?.message || "注册失败";
          if (msg.includes("already registered")) {
            res.statusCode = 409;
            res.end(JSON.stringify({ ok: false, error: "该手机号已注册" }));
            return;
          }
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: msg }));
        }
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
      (pathOnly === "/api/community/following" ||
        pathOnly === "/api/community/leaderboard")) ||
    (req.method === "GET" && communityProfileMatch) ||
    (req.method === "PATCH" && pathOnly === "/api/me/community-profile") ||
    ((req.method === "POST" || req.method === "DELETE") && communityFollowMatch);

  const isCreateSymbolNameMapDirect =
    req.method === "POST" && pathOnly === "/api/admin/create-symbol-name-map";
  const isSnapshotWatermarkDirect = req.method === "GET" && pathOnly === "/api/snapshot/watermark";
  const isSnapshotAccountDailyDirect = req.method === "GET" && pathOnly === "/api/snapshot/account-daily";
  const isSnapshotSymbolDailyDirect = req.method === "GET" && pathOnly === "/api/snapshot/symbol-daily";
  const isSnapshotSymbolCloseDirect = req.method === "GET" && pathOnly === "/api/snapshot/symbol-close";
  const isCronFreezeDirect = (req.method === "POST" || req.method === "GET") && pathOnly === "/api/cron/freeze-eod";
  const isCronDailyCloseDirect =
    (req.method === "POST" || req.method === "GET") && pathOnly === "/api/cron/sync-daily-close";
  const isSettingsGetDirect = req.method === "GET" && pathOnly === "/api/settings";
  const isCacheMetaDirect = req.method === "GET" && pathOnly === "/api/cache-meta";
  const isSettingsPatchDirect = req.method === "PATCH" && pathOnly === "/api/settings";
  const tradesDeleteMatch = pathOnly.match(/^\/api\/trades\/([^/]+)$/) || null;
  const isTradesGetDirect = req.method === "GET" && pathOnly === "/api/trades";
  const isTradesSearchHistoryDirect = req.method === "GET" && pathOnly === "/api/trades/search-history";
  const isTradesPostDirect = req.method === "POST" && pathOnly === "/api/trades";
  const isTradesDeleteDirect = req.method === "DELETE" && !!tradesDeleteMatch;
  const isTradesImportDirect = req.method === "POST" && pathOnly === "/api/trades/import";
  const cashTransfersDeleteMatch = pathOnly.match(/^\/api\/cash-transfers\/([^/]+)$/) || null;
  const isCashTransfersGetDirect = req.method === "GET" && pathOnly === "/api/cash-transfers";
  const isCashTransfersPostDirect = req.method === "POST" && pathOnly === "/api/cash-transfers";
  const isCashTransfersDeleteDirect = req.method === "DELETE" && !!cashTransfersDeleteMatch;
  const isCashTransfersImportDirect = req.method === "POST" && pathOnly === "/api/cash-transfers/import";
  const isStockSearchDirect = req.method === "GET" && pathOnly === "/api/search";

  if (isCreateSymbolNameMapDirect) {
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
      const subExpired = await getSubscriptionExpiredPayload(userId);
      if (subExpired) {
        endJsonPayload(res, subExpired, subExpired.status);
        return;
      }
      const { createSymbolNameMapTableNow } = require("../src/db");
      const created = await createSymbolNameMapTableNow();
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, created }));
      return;
    } catch (error) {
      res.statusCode = 500;
      res.end(JSON.stringify({ ok: false, error: error?.message || "symbol name map direct failed" }));
      return;
    }
  }

  if (isStockSearchDirect) {
    const { publicSearchResults } = require("../src/sina-suggest");
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    const query = String(getSearchParam(req, "query") || "").trim();
    if (!query || query.length > 64) {
      res.statusCode = 400;
      res.end(JSON.stringify({ ok: false, error: "invalid query" }));
      return;
    }
    const cacheKey = query.toLowerCase();
    const cached = getCacheHit(sinaSuggestCache, cacheKey, DIRECT_SINA_SUGGEST_CACHE_TTL_MS);
    if (Array.isArray(cached)) {
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, results: publicSearchResults(cached) }));
      return;
    }
    try {
      let results = null;
      try {
        results = await fetchSinaSuggestFromAliyun(query);
      } catch (error) {
        console.warn("[api/index.js] aliyun search failed:", error?.message || error);
      }
      if (!Array.isArray(results)) {
        results = await fetchSinaSuggestFromUpstream(query);
      }
      setCacheValue(sinaSuggestCache, cacheKey, results);
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, results }));
      return;
    } catch (error) {
      res.statusCode = 502;
      res.end(JSON.stringify({ ok: false, error: error?.message || "search failed" }));
      return;
    }
  }

  if (isTradesSearchHistoryDirect) {
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
      const subExpired = await getSubscriptionExpiredPayload(userId);
      if (subExpired) {
        endJsonPayload(res, subExpired, subExpired.status);
        return;
      }
      const { getTradeSearchHistoryForUser } = require("../src/trade-search-history");
      const items = await getTradeSearchHistoryForUser(userId);
      res.statusCode = 200;
      res.end(JSON.stringify({ ok: true, data: { items } }));
      return;
    } catch (error) {
      res.statusCode = 500;
      res.end(JSON.stringify({ ok: false, error: error?.message || "search history failed" }));
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
      const subExpired = await getSubscriptionExpiredPayload(userId);
      if (subExpired) {
        endJsonPayload(res, subExpired, subExpired.status);
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
      const {
        ensureSymbolNameMapOnNewTrade,
        enrichTradesWithSymbolNames,
      } = require("../src/symbol-name-resolve");

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
          await enrichTradesWithSymbolNames(data);
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
          await enrichTradesWithSymbolNames(data);
          res.statusCode = 200;
          res.end(JSON.stringify({ ok: true, data, pagination }));
          return;
        }
        const data = await getTrades(userId);
        await enrichTradesWithSymbolNames(data);
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
        if (!prior) {
          await ensureSymbolNameMapOnNewTrade(saved.symbol, saved.name);
        }
        await notifyLedgerMutation(userId, { hintDates: hintDatesFromTradeMutation(prior, saved) });
        const [enriched] = await enrichTradesWithSymbolNames([saved]);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: enriched }));
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
          await notifyLedgerMutation(userId, { hintDates: del.date ? [del.date] : [] });
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
      const subExpired = await getSubscriptionExpiredPayload(userId);
      if (subExpired) {
        endJsonPayload(res, subExpired, subExpired.status);
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
        await notifyLedgerMutation(userId, { hintDates: hintDatesFromCashMutation(prior, saved) });
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
          await notifyLedgerMutation(userId, { hintDates: del.date ? [del.date] : [] });
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
    isCacheMetaDirect ||
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
      const subExpired = await getSubscriptionExpiredPayload(userId);
      if (subExpired) {
        endJsonPayload(res, subExpired, subExpired.status);
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

      if (isCacheMetaDirect) {
        const { getCacheMeta } = require("../src/cache-epoch");
        const data = await getCacheMeta(userId);
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
        await notifyLedgerMutation(userId, {
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
        await notifyLedgerMutation(userId, {
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
        const configuredSecret = cronSecretFromEnv();
        const secretMatched = isCronSecretMatched(req, body, configuredSecret);
        const sessionUserId = readUserIdFromRequest(req);
        if (!sessionUserId && cronHeader == null && !secretMatched) {
          console.warn(
            "[api/index.js] cron auth failed freeze-eod secretConfigured=%s bearerLen=%s bodyTokenLen=%s headerSecretLen=%s",
            !!configuredSecret,
            extractBearerToken(req).length,
            String(body?.token || "").trim().length,
            String(req.headers?.["x-cron-secret"] || "").trim().length,
          );
          res.statusCode = 401;
          res.end(JSON.stringify({ ok: false, error: "unauthorized cron request" }));
          return;
        }
        const frozenDate = getSearchParam(req, "frozenDate") || body?.frozenDate;
        const force = parseBooleanInput(getSearchParam(req, "force") || body?.force, false);
        const syncDailyClose = parseBooleanInput(getSearchParam(req, "syncDailyClose") || body?.syncDailyClose, false);
        const rebuildFromDate = getSearchParam(req, "rebuildFromDate") || body?.rebuildFromDate || null;
        const fullRebuild = parseBooleanInput(getSearchParam(req, "fullRebuild") || body?.fullRebuild, false);
        const runAsync = parseBooleanInput(getSearchParam(req, "async") || body?.async, false);
        const userIdFromQuery = getSearchParam(req, "userId");
        const userIds = Array.isArray(body?.userIds) && body.userIds.length
          ? body.userIds
          : userIdFromQuery ? [userIdFromQuery] : [];
        const fromCron = cronHeader != null;

        if (runAsync) {
          const { runInBackground } = require("../src/background-task");
          const { runAndVerifyFreeze } = require("../src/metrics-rebuild-trigger");
          const freezeBody = {
            userIds,
            force,
            syncDailyClose,
            rebuildFromDate: rebuildFromDate || undefined,
            fullRebuild,
          };
          console.log(
            "[api/index.js] freeze-eod async accepted userIds=%s rebuildFromDate=%s force=%s fullRebuild=%s",
            userIds.join(",") || "-",
            rebuildFromDate || "-",
            force,
            fullRebuild,
          );
          runInBackground(() =>
            runAndVerifyFreeze(freezeBody).catch((e) => {
              console.warn(
                "[api/index.js] freeze-eod async failed userIds=%s %s",
                userIds.join(",") || "-",
                e?.message || e,
              );
            }),
          );
          res.statusCode = 202;
          res.end(JSON.stringify({ ok: true, accepted: true }));
          return;
        }

        console.log(
          "[api/index.js] freeze-eod start userIds=%s rebuildFromDate=%s force=%s fullRebuild=%s",
          userIds.join(",") || "-",
          rebuildFromDate || "-",
          force,
          fullRebuild,
        );
        const data = fromCron
          ? await (async () => {
              const { runScheduledEodPipeline } = require("../src/eod-freeze-service");
              return runScheduledEodPipeline({
                frozenDate,
                force,
                userIds,
                rebuildFromDate,
                fullRebuild,
                logger: console,
              });
            })()
          : await runDailyFreeze({
              frozenDate,
              force,
              syncDailyClose,
              userIds,
              fromCron,
              rebuildFromDate,
              fullRebuild,
              logger: console,
            });
        const { isFreezeUserFailure } = require("../src/metrics-rebuild-trigger");
        const lagRemaining = Array.isArray(data?.lagRemaining) ? data.lagRemaining : [];
        const failedUsers = (data.users || []).filter(isFreezeUserFailure);
        if (lagRemaining.length) {
          console.warn(
            "[api/index.js] freeze-eod lag remaining count=%s ids=%s catchUpRounds=%s",
            lagRemaining.length,
            lagRemaining.join(","),
            data.catchUpRounds ?? 0,
          );
          res.statusCode = 207;
          res.end(JSON.stringify({ ok: false, error: "freeze-eod partial", data }));
          return;
        }
        if (failedUsers.length) {
          console.warn(
            "[api/index.js] freeze-eod partial-fail users=%s",
            failedUsers.map((row) => `${row.userId || "?"}:${row.reason || "skipped"}`).join(","),
          );
          res.statusCode = 500;
          res.end(JSON.stringify({ ok: false, error: "freeze-eod failed", data }));
          return;
        }
        console.log(
          "[api/index.js] freeze-eod done elapsedMs=%s pipelineElapsedMs=%s userIds=%s dailyCloseRows=%s",
          data.elapsedMs,
          data.pipelineElapsedMs,
          userIds.join(",") || "-",
          data.dailyClose?.rowsWritten,
        );
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (isCronDailyCloseDirect) {
        const body = req.method === "POST" ? await readJsonBody(req) : {};
        const cronHeader = req.headers?.["x-vercel-cron"];
        const configuredSecret = cronSecretFromEnv();
        const secretMatched = isCronSecretMatched(req, body, configuredSecret);
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
      const subExpired = await getSubscriptionExpiredPayload(userId);
      if (subExpired) {
        endJsonPayload(res, subExpired, subExpired.status);
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
      const isGuestReadable =
        req.method === "GET" &&
        (pathOnly === "/api/community/leaderboard" || !!communityProfileMatch);
      if (!userId && !isGuestReadable) {
        res.statusCode = 401;
        res.end(JSON.stringify({ ok: false, error: "请先登录" }));
        return;
      }
      if (userId) {
        const subExpired = await getSubscriptionExpiredPayload(userId);
        if (subExpired) {
          endJsonPayload(res, subExpired, subExpired.status);
          return;
        }
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
        enrichLeaderboardPayloadWithSymbolNames,
        enrichLeaderboardPayloadWithViewer,
        enrichTopPositionsOnCards,
        getPublicProfileDetail,
        displayNameForUser,
      } = require("../src/community-service");

      const readRawBody = () =>
        new Promise((resolve, reject) => {
          let data = "";
          req.on("data", (chunk) => (data += chunk));
          req.on("end", () => resolve(data));
          req.on("error", reject);
        });

      if (req.method === "GET" && pathOnly === "/api/community/following") {
        const cards = await getFollowingCards(userId);
        await enrichTopPositionsOnCards(cards);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data: cards }));
        return;
      }

      if (req.method === "GET" && pathOnly === "/api/community/leaderboard") {
        const data = await getLeaderboard();
        await enrichLeaderboardPayloadWithViewer(data, userId);
        await enrichLeaderboardPayloadWithSymbolNames(data);
        res.statusCode = 200;
        res.end(JSON.stringify({ ok: true, data }));
        return;
      }

      if (req.method === "GET" && communityProfileMatch) {
        const targetId = String(communityProfileMatch[1] || "").trim();
        const detail = await getPublicProfileDetail(userId, targetId);
        if (detail.error === "hidden") {
          res.statusCode = 404;
          res.end(JSON.stringify({ ok: false, error: "用户未公开或不可见" }));
          return;
        }
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
  });
};