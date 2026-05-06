const { pingDatabase } = require("../src/db");

function pickHost() {
  const raw =
    process.env.DATABASE_URL ||
    process.env.POSTGRES_URL ||
    process.env.POSTGRES_PRISMA_URL ||
    process.env.DATABASE_URL_UNPOOLED ||
    process.env.POSTGRES_URL_NON_POOLING ||
    "";
  try {
    const host = raw.split("@")[1]?.split("/")[0] || "";
    return {
      host,
      pooler: host.includes("-pooler"),
      envVarUsed: raw === process.env.DATABASE_URL
        ? "DATABASE_URL"
        : raw === process.env.POSTGRES_URL
        ? "POSTGRES_URL"
        : raw === process.env.POSTGRES_PRISMA_URL
        ? "POSTGRES_PRISMA_URL"
        : raw === process.env.DATABASE_URL_UNPOOLED
        ? "DATABASE_URL_UNPOOLED"
        : raw === process.env.POSTGRES_URL_NON_POOLING
        ? "POSTGRES_URL_NON_POOLING"
        : "none",
    };
  } catch (_) {
    return { host: "", pooler: false, envVarUsed: "unknown" };
  }
}

module.exports = async function handler(_req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  const startedAt = Date.now();
  const conn = pickHost();
  try {
    const row = await pingDatabase();
    res.statusCode = 200;
    res.end(
      JSON.stringify({
        ok: true,
        elapsedMs: Date.now() - startedAt,
        conn,
        database: row.db != null ? String(row.db) : null,
        schema: row.schema != null ? String(row.schema) : null,
        serverTime: row.server_time != null ? String(row.server_time) : null,
      })
    );
  } catch (error) {
    res.statusCode = 503;
    res.end(
      JSON.stringify({
        ok: false,
        elapsedMs: Date.now() - startedAt,
        conn,
        error: {
          message: error?.message || "database unreachable",
          name: error?.name || null,
          code: error?.code || null,
          errno: error?.errno || null,
          causeMessage: error?.cause?.message || null,
          stackHead: (error?.stack || "").split("\n").slice(0, 3).join(" | "),
        },
      })
    );
  }
};
