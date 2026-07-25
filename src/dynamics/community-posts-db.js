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
const POST_TYPES = new Set(["viewpoint", "valuation"]);

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

function parseExtraField(raw) {
  if (raw == null || raw === "") {
    return {};
  }
  if (typeof raw === "object" && !Array.isArray(raw)) {
    return { ...raw };
  }
  try {
    const parsed = JSON.parse(String(raw));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }
    return { ...parsed };
  } catch {
    return {};
  }
}

function serializeExtra(extra) {
  return JSON.stringify(parseExtraField(extra));
}

function normalizePostType(raw) {
  const t = String(raw || "viewpoint").trim().toLowerCase();
  return POST_TYPES.has(t) ? t : "viewpoint";
}

function rowToPost(row) {
  const postType = normalizePostType(row.post_type);
  return {
    id: row.id,
    userId: row.user_id,
    content: String(row.content || ""),
    imageUrls: parseImageUrlsField(row.image_urls),
    symbols: parseSymbolsField(row.symbols),
    postType,
    extra: postType === "valuation" ? parseExtraField(row.extra) : {},
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
  const postType = normalizePostType(raw.postType ?? raw.post_type);

  if (postType === "valuation") {
    if (symbols.length !== 1) {
      throw new Error("个股估值必须且只能选择一只股票");
    }
    const extraIn = parseExtraField(raw.extra);
    const lowPrice = Number(extraIn.lowPrice ?? raw.lowPrice);
    const highPrice = Number(extraIn.highPrice ?? raw.highPrice);
    if (!Number.isFinite(lowPrice) || lowPrice <= 0) {
      throw new Error("请输入有效的低估价");
    }
    if (!Number.isFinite(highPrice) || highPrice <= 0) {
      throw new Error("请输入有效的高估价");
    }
    if (highPrice < lowPrice) {
      throw new Error("高估价不能低于低估价");
    }
    return {
      content,
      imageUrls,
      symbols,
      postType,
      extra: { lowPrice, highPrice },
    };
  }

  if (!content && !imageUrls.length && !symbols.length) {
    throw new Error("请输入内容、图片或关联股票");
  }
  return { content, imageUrls, symbols, postType: "viewpoint", extra: {} };
}

async function getCommunityPostByIdForUser(postId, userId) {
  const uid = String(userId || "").trim();
  const pid = String(postId || "").trim();
  if (!uid || !pid) {
    return null;
  }
  const { rows } = await dbQuery(
    `SELECT id, user_id, content, image_urls, symbols, post_type, extra, created_at, updated_at
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
    `INSERT INTO community_posts (id, user_id, content, image_urls, symbols, post_type, extra, created_at, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
    [
      id,
      uid,
      input.content,
      serializeImageUrls(input.imageUrls),
      serializeSymbols(input.symbols),
      input.postType,
      serializeExtra(input.extra),
      now,
      now,
    ],
  );
  await ensureSymbolNameMapForSymbols(input.symbols, { source: "tencent" });
  const { bumpDynamicsEpoch } = require("../cache-epoch");
  await bumpDynamicsEpoch(uid);
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
     SET content = $3, image_urls = $4, symbols = $5, post_type = $6, extra = $7, updated_at = $8
     WHERE user_id = $1 AND id = $2`,
    [
      uid,
      pid,
      input.content,
      serializeImageUrls(input.imageUrls),
      serializeSymbols(input.symbols),
      input.postType,
      serializeExtra(input.extra),
      now,
    ],
  );
  if (removed.length) {
    await deleteBlobUrls(removed);
  }
  await ensureSymbolNameMapForSymbols(input.symbols, { source: "tencent" });
  const { bumpDynamicsEpoch } = require("../cache-epoch");
  await bumpDynamicsEpoch(uid);
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
    const { bumpDynamicsEpoch } = require("../cache-epoch");
    await bumpDynamicsEpoch(uid);
  }
  return { deleted: rowCount > 0 };
}

/** 每标的取最新一条估值帖 extra（user 维度）。 */
async function getLatestValuationBySymbolForUser(userId) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return new Map();
  }
  const { rows } = await dbQuery(
    `SELECT DISTINCT ON (elem.sym)
       elem.sym AS symbol,
       p.extra
     FROM community_posts p
     CROSS JOIN LATERAL jsonb_array_elements_text(p.symbols::jsonb) AS elem(sym)
     WHERE p.user_id = $1 AND p.post_type = 'valuation'
     ORDER BY elem.sym, p.created_at DESC`,
    [uid],
  );
  const map = new Map();
  for (const row of rows) {
    const sym = normalizeSymbol(row.symbol);
    if (!sym) {
      continue;
    }
    map.set(sym, parseExtraField(row.extra));
  }
  return map;
}

module.exports = {
  CONTENT_MAX,
  POST_TYPES,
  parseSymbolsField,
  parseExtraField,
  rowToPost,
  getCommunityPostByIdForUser,
  createCommunityPost,
  updateCommunityPost,
  deleteCommunityPost,
  getLatestValuationBySymbolForUser,
};
