console.log("[api/index.js] module-load start build=v5");

// 懒加载：不在模块顶层 require server.js，避免 server.js 顶层任何副作用
// 导致整个函数在冷启动阶段就 hang 300s。改为第一次业务请求时才加载。
let appPromise = null;
let loadError = null;

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
    if (
      (req.url === "/" || req.url === "/api" || req.url === "/api/") &&
      typeof originalPath === "string" &&
      originalPath.startsWith("/api/") &&
      originalPath !== "/api" &&
      originalPath !== "/api/"
    ) {
      console.log("[api/index.js] restoring req.url from %s to %s", req.url, originalPath);
      req.url = originalPath;
    }
  } catch (_) {}

  // 不依赖 server.js 的诊断端点：证明 /api/(.*) 这条路由至少能到达函数
  if (req.url && req.url.startsWith("/api/diag/v5")) {
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    res.end(
      JSON.stringify({
        ok: true,
        build: "v5",
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
