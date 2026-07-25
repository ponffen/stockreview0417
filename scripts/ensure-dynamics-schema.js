#!/usr/bin/env node
/**
 * Idempotent DDL for dynamics: community_posts + trades.image_urls
 */
require("dotenv").config();
const { initPool, runSchemaDdl, closeDatabase } = require("../src/db");

async function main() {
  await initPool();
  await runSchemaDdl(`
    CREATE TABLE IF NOT EXISTS community_posts (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      content TEXT NOT NULL DEFAULT '',
      image_urls TEXT NOT NULL DEFAULT '[]',
      symbols TEXT NOT NULL DEFAULT '[]',
      post_type TEXT NOT NULL DEFAULT 'viewpoint',
      extra TEXT NOT NULL DEFAULT '{}',
      created_at BIGINT NOT NULL,
      updated_at BIGINT NOT NULL
    )
  `);
  await runSchemaDdl(`
    CREATE INDEX IF NOT EXISTS idx_community_posts_user_created
      ON community_posts (user_id, created_at DESC)
  `);
  await runSchemaDdl(`ALTER TABLE community_posts ADD COLUMN IF NOT EXISTS post_type TEXT NOT NULL DEFAULT 'viewpoint'`);
  await runSchemaDdl(`ALTER TABLE community_posts ADD COLUMN IF NOT EXISTS extra TEXT NOT NULL DEFAULT '{}'`);
  await runSchemaDdl(`
    CREATE INDEX IF NOT EXISTS idx_community_posts_user_type_created
      ON community_posts (user_id, post_type, created_at DESC)
  `);
  await runSchemaDdl(`ALTER TABLE trades ADD COLUMN IF NOT EXISTS image_urls TEXT NOT NULL DEFAULT '[]'`);
  console.log("[ensure-dynamics-schema] ok");
  await closeDatabase();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
