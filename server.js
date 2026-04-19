const express = require("express");
const cors = require("cors");
const path = require("path");
const Database = require("better-sqlite3");

const PORT = Number(process.env.PORT || 3000);
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "data", "trades.db");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, node: process.version });
});

app.get("/api/trades/count", (_req, res) => {
  try {
    const db = new Database(DB_PATH, { readonly: true, fileMustExist: true });
    const row = db.prepare("SELECT COUNT(*) AS total FROM trades").get();
    db.close();
    res.json({ total: row?.total ?? 0, dbPath: DB_PATH });
  } catch (_error) {
    res.json({ total: 0, dbPath: DB_PATH });
  }
});

app.listen(PORT, () => {
  console.log(`Dev server running at http://127.0.0.1:${PORT}`);
  console.log(`DB path: ${DB_PATH}`);
});
