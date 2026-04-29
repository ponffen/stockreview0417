const { pingDatabase } = require("../src/db");

module.exports = async function handler(_req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  try {
    const row = await pingDatabase();
    res.statusCode = 200;
    res.end(
      JSON.stringify({
        ok: true,
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
        error: error?.message || "database unreachable",
      })
    );
  }
};
