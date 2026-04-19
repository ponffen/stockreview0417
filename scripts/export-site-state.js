/**
 * Build a static snapshot of app state (same shape as GET /api/state) for GitHub Pages,
 * where Node/SQLite cannot run. Output: data/site-state.json
 */
const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");
const dataDir = path.join(root, "data");
const outPath = path.join(dataDir, "site-state.json");
const seedPath = path.join(__dirname, "seed-trades.sample.json");
const buildDbPath = path.join(dataDir, ".site-state-build.db");

if (!fs.existsSync(seedPath)) {
  throw new Error(`Missing seed file: ${seedPath}`);
}

if (fs.existsSync(buildDbPath)) {
  fs.unlinkSync(buildDbPath);
}

fs.mkdirSync(dataDir, { recursive: true });
process.env.DB_PATH = buildDbPath;

const {
  importTrades,
  getState,
  normalizeTrade,
  closeDatabase,
} = require("../src/db");

const seedRaw = fs.readFileSync(seedPath, "utf-8");
const seedRows = JSON.parse(seedRaw);
if (!Array.isArray(seedRows)) {
  throw new Error("Seed JSON must be an array of trades");
}

const trades = seedRows.map((row) => normalizeTrade(row));
importTrades(trades, "replace");

const state = getState();
fs.mkdirSync(dataDir, { recursive: true });
fs.writeFileSync(outPath, `${JSON.stringify(state, null, 2)}\n`, "utf-8");
closeDatabase();

if (fs.existsSync(buildDbPath)) {
  fs.unlinkSync(buildDbPath);
}

console.log(`Wrote ${outPath} (${state.trades?.length ?? 0} trades)`);
