#!/usr/bin/env node
/**
 * Full dump of local Postgres (from .env DATABASE_URL) and restore into Neon.
 *
 * Prerequisites:
 *   - pg_dump / pg_restore on PATH, or set PG_BIN to their directory (e.g. /Library/PostgreSQL/18/bin).
 *   - Neon pooled connection string in env NEON_DATABASE_URL, or a single line in file ./neon-database.url
 *
 * Optional:
 *   - FORCE_NEON_REPLACE=1  → DROP SCHEMA public CASCADE; CREATE SCHEMA public; on Neon before restore (wipes Neon public schema).
 *
 * Usage:
 *   NEON_DATABASE_URL='postgresql://...' FORCE_NEON_REPLACE=1 node scripts/sync-local-pg-to-neon.js
 */

require("dotenv").config();
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { spawnSync } = require("node:child_process");

function findPgBin(name) {
  const fromEnv = process.env.PG_BIN ? path.join(process.env.PG_BIN, name) : "";
  if (fromEnv && fs.existsSync(fromEnv)) {
    return fromEnv;
  }
  const candidates = [
    "/Library/PostgreSQL/18/bin",
    "/Library/PostgreSQL/17/bin",
    "/Library/PostgreSQL/16/bin",
    "/usr/local/pgsql/bin",
    "/opt/homebrew/opt/libpq/bin",
  ];
  for (const dir of candidates) {
    const p = path.join(dir, name);
    if (fs.existsSync(p)) {
      return p;
    }
  }
  return name;
}

function readNeonUrl() {
  if (process.env.NEON_DATABASE_URL && String(process.env.NEON_DATABASE_URL).trim()) {
    return String(process.env.NEON_DATABASE_URL).trim();
  }
  const file = path.join(process.cwd(), "neon-database.url");
  if (fs.existsSync(file)) {
    const t = fs.readFileSync(file, "utf8").trim().split(/\r?\n/)[0].trim();
    if (t) {
      return t;
    }
  }
  return "";
}

function ensureSslMode(url) {
  const u = String(url || "");
  if (!u) {
    return u;
  }
  if (/[?&]sslmode=/i.test(u)) {
    return u;
  }
  return u.includes("?") ? `${u}&sslmode=require` : `${u}?sslmode=require`;
}

function run(cmd, args, extraEnv = {}) {
  const r = spawnSync(cmd, args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, ...extraEnv },
  });
  if (r.status !== 0) {
    const err = (r.stderr || r.stdout || "").trim() || `exit ${r.status}`;
    throw new Error(`${cmd} ${args.join(" ")} failed: ${err}`);
  }
  return (r.stdout || "").trim();
}

const localUrl = String(process.env.DATABASE_URL || "").trim();
let neonUrl = readNeonUrl();
neonUrl = ensureSslMode(neonUrl);

if (!localUrl) {
  console.error("Missing DATABASE_URL in .env (local Postgres).");
  process.exit(1);
}
if (!neonUrl) {
  console.error(
    "Missing Neon URL. Set NEON_DATABASE_URL or create neon-database.url (single line, pooled URL from Vercel → Storage → Neon)."
  );
  process.exit(1);
}

const pgDump = findPgBin("pg_dump");
const pgRestore = findPgBin("pg_restore");
const psql = findPgBin("psql");

const dumpPath = path.join(os.tmpdir(), `stockreview-pgdump-${Date.now()}.dump`);

console.log("Using pg_dump:", pgDump);
console.log("Using pg_restore:", pgRestore);
console.log("Dump file:", dumpPath);

run(pgDump, ["--dbname", localUrl, "--format=custom", "--no-owner", "--file", dumpPath]);

if (process.env.FORCE_NEON_REPLACE === "1") {
  console.log("FORCE_NEON_REPLACE=1 → resetting public schema on Neon…");
  const sql = "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;";
  run(psql, ["--dbname", neonUrl, "-v", "ON_ERROR_STOP=1", "-c", sql]);
}

console.log("Restoring into Neon (this may take a while)…");
run(pgRestore, ["--dbname", neonUrl, "--no-owner", "--no-acl", "--verbose", dumpPath]);

try {
  fs.unlinkSync(dumpPath);
} catch {
  // ignore
}

console.log("Done. Verify in Neon console or hit /api/auth/me on Vercel.");
