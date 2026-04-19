const path = require("node:path");
const fs = require("node:fs");
const express = require("express");
const cors = require("cors");

function ensureDataDir() {
  const dataDir = path.join(__dirname, "data");
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
}

ensureDataDir();

const {
  DEFAULT_SETTINGS,
  normalizeTrade,
  getTrades,
  upsertTrade,
  importTrades,
  deleteTradeById,
  getSettings,
  setSettings,
  getState,
} = require("./src/db");

const app = express();
const PORT = Number(process.env.PORT || 3030);

app.use(cors());
app.use(express.json({ limit: "5mb" }));

app.get("/api/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/api/state", (_req, res) => {
  res.json({ ok: true, data: getState() });
});

app.get("/api/trades", (_req, res) => {
  res.json({ ok: true, data: getTrades() });
});

app.post("/api/trades", (req, res) => {
  try {
    const trade = normalizeTrade(req.body || {});
    if (!trade.symbol) {
      res.status(400).json({ ok: false, error: "symbol is required" });
      return;
    }
    const saved = upsertTrade(trade);
    res.json({ ok: true, data: saved });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "save trade failed" });
  }
});

app.delete("/api/trades/:id", (req, res) => {
  const ok = deleteTradeById(req.params.id);
  res.json({ ok: true, deleted: ok });
});

app.post("/api/trades/import", (req, res) => {
  try {
    const payload = req.body || {};
    const mode = payload.mode === "replace" ? "replace" : "append";
    const trades = Array.isArray(payload.trades) ? payload.trades : [];
    const normalized = trades.map((item) => normalizeTrade(item));
    const data = importTrades(normalized, mode);
    res.json({ ok: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "import failed" });
  }
});

app.get("/api/settings", (_req, res) => {
  res.json({ ok: true, data: getSettings() });
});

app.patch("/api/settings", (req, res) => {
  try {
    const patch = req.body && typeof req.body === "object" ? req.body : {};
    const sanitized = {};
    for (const key of Object.keys(DEFAULT_SETTINGS)) {
      if (Object.hasOwn(patch, key)) {
        sanitized[key] = patch[key];
      }
    }
    const data = setSettings(sanitized);
    res.json({ ok: true, data });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || "update settings failed" });
  }
});

app.use("/api", (_req, res) => {
  res.status(404).json({ ok: false, error: "API route not found" });
});

app.use(express.static(path.join(__dirname)));

app.use((_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});
app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`Server listening on http://0.0.0.0:${PORT}`);
});
