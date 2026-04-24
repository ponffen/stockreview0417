/**
 * Lightweight health endpoint for Vercel.
 * Avoids importing the full Express app (and DB init path),
 * so /api/health can return fast even when business APIs are degraded.
 */
module.exports = function handler(_req, res) {
  res.statusCode = 200;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(
    JSON.stringify({
      ok: true,
      node: process.version,
      service: "stockreview0417",
      ts: Date.now(),
    })
  );
};
