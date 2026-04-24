/**
 * Build a static snapshot of app state (same shape as GET /api/state) for GitHub Pages.
 * Output: data/site-state.json
 *
 * Requires DATABASE_URL (PostgreSQL). Exports the stockreview user selected by STOCKREVIEW_PHONE
 * (default seed phone) or the first user if configured.
 *
 * Usage:
 *   DATABASE_URL=postgres://... npm run build:site-state
 */
const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");
const dataDir = path.join(root, "data");
const outPath = path.join(dataDir, "site-state.json");

function writeStateAndLog(state) {
  fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(outPath, `${JSON.stringify(state, null, 2)}\n`, "utf-8");
  // eslint-disable-next-line no-console
  console.log(`Wrote ${outPath} (${state.trades?.length ?? 0} trades)`);
}

async function main() {
  if (!process.env.DATABASE_URL && !process.env.POSTGRES_URL) {
    // eslint-disable-next-line no-console
    console.error("Set DATABASE_URL or POSTGRES_URL (PostgreSQL connection string).");
    process.exit(1);
  }
  if (!process.env.DATABASE_URL && process.env.POSTGRES_URL) {
    process.env.DATABASE_URL = process.env.POSTGRES_URL;
  }
  const { getState, closeDatabase, getCliUserId } = require("../src/db");
  const state = await getState(await getCliUserId());
  writeStateAndLog(state);
  await closeDatabase();
}

main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(e);
  process.exit(1);
});
