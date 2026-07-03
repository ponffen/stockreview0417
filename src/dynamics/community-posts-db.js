/**
 * community_posts CRUD.
 */

const { randomUUID } = require("crypto");
const { dbQuery, normalizeSymbol } = require("../db");
const { nowMs } = require("../db-pure");
const { ensureSymbolNameMapForSymbols } = require("../symbol-name-resolve");
const {
  parseImageUrlsField,
  serializeImageUrls,
  deleteBlobUrls,
  diffRemovedImageUrls,
  normalizeStoredImageUrls,
} = require("./blob-images");

const CONTENT_MAX = 2000;

function parseSymbolsField(raw) {
  if (raw == null || raw === "") {
    return [];
  }
  if (Array.isArray(raw)) {
    return [...new Set(raw.map((s) => normalizeSymbol(s)).filter(Boolean))].slice(0, 20);
  }
  try {
    const parsed = JSON.parse(String(raw));
    if (!Array.isArray(parsed)) {
      return [];
    }
    return [...new Set(parsed.map((s) => normalizeSymbol(s)).filter(Boolean))].slice(0, 20);
  } catch {
    return [];
  }
}

function serializeSymbols(symbols) {
  return JSON.stringify(parseSymbolsField(symbols));
}

function rowToPost(row) {
  return {
    id: row.id,
    userId: row.user_id,
    content: String(row.content || ""),
    imageUrls: parseImageUrlsField(row.image_urls),
    symbols: parseSymbolsField(row.symbols),
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
  };
}

function normalizePostInput(body) {
  const raw = body || {};
  const content = String(raw.content ?? "").trim();
  if (content.length > CONTENT_MAX) {
    throw new Error(`正文最长 ${CONTENT_MAX} 个字符`);
  }
  const imageUrls = normalizeStoredImageUrls(raw.imageUrls ?? raw.image_urls);
  if (imageUrls.length > 9) {
    throw new Error("最多 9 张图片");
  }
  const symbols = parseSymbolsField(raw.symbols);
  return { content, imageUrls, symbols };
}

async function getCommunityPostByIdForUser(postId, userId) {
  const uid = String(userId || "").trim();
  const pid = String(postId || "").trim();
  if (!uid || !pid) {
    return null;
  }
  const { rows } = await dbQuery(
    `SELECT id, user_id, content, image_urls, symbols, created_at, updated_at
     FROM community_posts WHERE user_id = $1 AND id = $2 LIMIT 1`,
    [uid, pid],
  );
  return rows.length ? rowToPost(rows[0]) : null;
}

async function createCommunityPost(userId, body) {
  const uid = String(userId || "").trim();
  if (!uid) {
    throw new Error("userId required");
  }
  const input = normalizePostInput(body);
  const now = nowMs();
  const id = randomUUID();
  await dbQuery(
    `INSERT INTO community_posts (id, user_id, content, image_urls, symbols, created_at, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [
      id,
      uid,
      input.content,
      serializeImageUrls(input.imageUrls),
      serializeSymbols(input.symbols),
      now,
      now,
    ],
  );
  await ensureSymbolNameMapForSymbols(input.symbols, { source: "tencent" });
  return getCommunityPostByIdForUser(id, uid);
}

async function updateCommunityPost(userId, postId, body) {
  const uid = String(userId || "").trim();
  const pid = String(postId || "").trim();
  const prior = await getCommunityPostByIdForUser(pid, uid);
  if (!prior) {
    return null;
  }
  const input = normalizePostInput(body);
  const removed = diffRemovedImageUrls(prior.imageUrls, input.imageUrls);
  const now = nowMs();
  await dbQuery(
    `UPDATE community_posts
     SET content = $3, image_urls = $4, symbols = $5, updated_at = $6
     WHERE user_id = $1 AND id = $2`,
    [
      uid,
      pid,
      input.content,
      serializeImageUrls(input.imageUrls),
      serializeSymbols(input.symbols),
      now,
    ],
  );
  if (removed.length) {
    await deleteBlobUrls(removed);
  }
  await ensureSymbolNameMapForSymbols(input.symbols, { source: "tencent" });
  return getCommunityPostByIdForUser(pid, uid);
}

async function deleteCommunityPost(userId, postId) {
  const uid = String(userId || "").trim();
  const pid = String(postId || "").trim();
  const prior = await getCommunityPostByIdForUser(pid, uid);
  if (!prior) {
    return { deleted: false };
  }
  const { rowCount } = await dbQuery(`DELETE FROM community_posts WHERE user_id = $1 AND id = $2`, [uid, pid]);
  if (rowCount > 0) {
    await deleteBlobUrls(prior.imageUrls);
  }
  return { deleted: rowCount > 0 };
}

module.exports = {
  CONTENT_MAX,
  parseSymbolsField,
  rowToPost,
  getCommunityPostByIdForUser,
  createCommunityPost,
  updateCommunityPost,
  deleteCommunityPost,
};
