/**
 * Build a static snapshot of app state (same shape as GET /api/state) for GitHub Pages,
 * where Node/SQLite cannot run. Output: data/site-state.json
 *
 * Usage:
 *   npm run build:site-state
 *     If data/app.db exists (e.g. after local import), export from it.
 *     Otherwise import scripts/seed-trades.sample.json into a temp DB and export.
 *   npm run build:site-state -- --db path/to.db
 *     Export from the given SQLite file.
 *   npm run build:site-state -- --seed
 *     Always use the sample seed (ignores data/app.db).
 */
const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");
const dataDir = path.join(root, "data");
const outPath = path.join(dataDir, "site-state.json");
const seedPath = path.join(__dirname, "seed-trades.sample.json");
const buildDbPath = path.join(dataDir, ".site-state-build.db");
const defaultAppDb = path.join(dataDir, "app.db");

function parseArgs(argv) {
  let explicitDb = "";
  let forceSeed = false;
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === "--db") {
      explicitDb = argv[i + 1] || "";
      i += 1;
    } else if (token === "--seed") {
      forceSeed = true;
    }
  }
  return { explicitDb, forceSeed };
}

function writeStateAndLog(state) {
  fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(outPath, `${JSON.stringify(state, null, 2)}\n`, "utf-8");
  console.log(`Wrote ${outPath} (${state.trades?.length ?? 0} trades)`);
}

const { explicitDb, forceSeed } = parseArgs(process.argv.slice(2));

let useExistingDb = "";
if (explicitDb) {
  useExistingDb = path.isAbsolute(explicitDb) ? explicitDb : path.join(process.cwd(), explicitDb);
} else if (!forceSeed && fs.existsSync(defaultAppDb)) {
  useExistingDb = defaultAppDb;
}

if (useExistingDb) {
  if (!fs.existsSync(useExistingDb)) {
    throw new Error(`Database not found: ${useExistingDb}`);
  }
  process.env.DB_PATH = useExistingDb;
  const { getState, closeDatabase } = require("../src/db");
  const state = getState();
  writeStateAndLog(state);
  closeDatabase();
  process.exit(0);
}

if (!fs.existsSync(seedPath)) {
  throw new Error(`Missing seed file: ${seedPath}`);
}

if (fs.existsSync(buildDbPath)) {
  fs.unlinkSync(buildDbPath);
}

fs.mkdirSync(dataDir, { recursive: true });
process.env.DB_PATH = buildDbPath;

const { importTrades, getState, normalizeTrade, closeDatabase } = require("../src/db");

const seedRaw = fs.readFileSync(seedPath, "utf-8");
const seedRows = JSON.parse(seedRaw);
if (!Array.isArray(seedRows)) {
  throw new Error("Seed JSON must be an array of trades");
}

const trades = seedRows.map((row) => normalizeTrade(row));
importTrades(trades, "replace");

const state = getState();
writeStateAndLog(state);
closeDatabase();

if (fs.existsSync(buildDbPath)) {
  fs.unlinkSync(buildDbPath);
}
