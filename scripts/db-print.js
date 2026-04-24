/**
 * Print PostgreSQL table row counts and small samples to the terminal.
 * Usage: DATABASE_URL=postgres://... npm run db:print
 */
const { Client } = require("pg");

async function main() {
  if (!process.env.DATABASE_URL) {
    // eslint-disable-next-line no-console
    console.error("DATABASE_URL is required.");
    process.exit(1);
  }
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_SSL === "0" || /localhost|127\.0\.0\.1/.test(process.env.DATABASE_URL) ? false : { rejectUnauthorized: false },
  });
  await client.connect();
  try {
    const { rows: tables } = await client.query(
      `SELECT tablename FROM pg_catalog.pg_tables
       WHERE schemaname = 'public' AND tablename NOT LIKE 'pg_%'
       ORDER BY tablename`
    );
    // eslint-disable-next-line no-console
    console.log(`Database: (postgresql)\n`);
    for (const { tablename: name } of tables) {
      const safe = String(name).replace(/[^a-z0-9_]/gi, "");
      if (safe !== name) {
        continue;
      }
      let count = "?";
      try {
        const r = await client.query(`SELECT COUNT(*)::bigint AS c FROM ${safe}`);
        count = r.rows[0].c;
      } catch (e) {
        count = `? (${e.message})`;
      }
      // eslint-disable-next-line no-console
      console.log(`── ${name} (${count} rows) ──`);
      try {
        const sample = await client.query(`SELECT * FROM ${safe} LIMIT 8`);
        if (!sample.rows.length) {
          // eslint-disable-next-line no-console
          console.log("(empty)\n");
          continue;
        }
        console.table(sample.rows);
        // eslint-disable-next-line no-console
        console.log("");
      } catch (e) {
        // eslint-disable-next-line no-console
        console.log(`(could not read: ${e.message})\n`);
      }
    }
  } finally {
    await client.end();
  }
}

main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(e);
  process.exit(1);
});
