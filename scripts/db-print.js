/**
 * Print SQLite contents (tables + row samples) to the terminal.
 * Usage: npm run db:print
 * Optional: DB_PATH=/path/to/app.db npm run db:print
 */
const path = require("node:path");
const Database = require("better-sqlite3");

const dbPath = process.env.DB_PATH || path.join(__dirname, "..", "data", "app.db");
const db = new Database(dbPath, { fileMustExist: true, readonly: true });

function tableCount(name) {
  try {
    const row = db.prepare(`SELECT COUNT(*) AS c FROM ${quoteIdent(name)}`).get();
    return row.c;
  } catch {
    return "?";
  }
}

function quoteIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

const tables = db
  .prepare(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
  )
  .all()
  .map((r) => r.name);

// eslint-disable-next-line no-console
console.log(`Database: ${dbPath}\n`);

for (const name of tables) {
  // eslint-disable-next-line no-console
  console.log(`── ${name} (${tableCount(name)} rows) ──`);
  try {
    const sample = db.prepare(`SELECT * FROM ${quoteIdent(name)} LIMIT 8`).all();
    if (sample.length === 0) {
      // eslint-disable-next-line no-console
      console.log("(empty)\n");
      continue;
    }
    console.table(sample);
    // eslint-disable-next-line no-console
    console.log("");
  } catch (e) {
    // eslint-disable-next-line no-console
    console.log(`(could not read: ${e.message})\n`);
  }
}

db.close();
