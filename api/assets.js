/**
 * Lightweight static asset endpoint for Vercel.
 * Serves browser files from api/public without loading the full app.
 */
const fs = require("node:fs");
const path = require("node:path");

const ASSET_META = {
  "styles.css": "text/css; charset=utf-8",
  "app.js": "application/javascript; charset=utf-8",
  "site-state.json": "application/json; charset=utf-8",
};

function parseRequestedFile(reqUrl) {
  try {
    const u = new URL(reqUrl || "/", "http://localhost");
    return String(u.searchParams.get("file") || "").trim();
  } catch {
    return "";
  }
}

function resolveAssetPath(fileName) {
  const candidates = [
    path.join(__dirname, "public", fileName),
    path.join(__dirname, "..", fileName),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      return p;
    }
  }
  return "";
}

module.exports = function handler(req, res) {
  const fileName = parseRequestedFile(req.url);
  const contentType = ASSET_META[fileName];
  if (!contentType) {
    res.statusCode = 400;
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.end(JSON.stringify({ ok: false, error: "invalid file" }));
    return;
  }

  const assetPath = resolveAssetPath(fileName);
  if (!assetPath) {
    res.statusCode = 404;
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.end(JSON.stringify({ ok: false, error: "file not found" }));
    return;
  }

  try {
    const body = fs.readFileSync(assetPath);
    res.statusCode = 200;
    res.setHeader("Content-Type", contentType);
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
    res.setHeader("Pragma", "no-cache");
    res.end(body);
  } catch (error) {
    res.statusCode = 500;
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.end(JSON.stringify({ ok: false, error: error?.message || "read asset failed" }));
  }
};
