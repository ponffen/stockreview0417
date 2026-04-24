/**
 * Vercel-only lightweight handler for HTML/CSS/JS in api/public/.
 * Keeps /styles.css and /app.js from loading the full server (DB init would block ~15s+).
 */
const path = require("node:path");
const fs = require("node:fs");
const express = require("express");
const serverless = require("serverless-http");

function resolveWebStaticRoot() {
  const cands = [
    path.join(__dirname, "public"),
    path.join(__dirname, ".."),
    path.join(__dirname, "..", ".."),
  ];
  for (const dir of cands) {
    if (fs.existsSync(path.join(dir, "index.html")) && fs.existsSync(path.join(dir, "app.js"))) {
      return dir;
    }
  }
  return path.join(__dirname, "public");
}

const WEB_ROOT = resolveWebStaticRoot();
const app = express();
app.disable("x-powered-by");

app.use(
  express.static(WEB_ROOT, {
    setHeaders(res, filePath) {
      if (/\.(html|js|css|json|ico|svg)$/i.test(filePath)) {
        res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
        res.setHeader("Pragma", "no-cache");
      }
    },
  })
);

app.use((_req, res) => {
  res.status(404).type("text/plain").send("Not found");
});

module.exports = serverless(app);
