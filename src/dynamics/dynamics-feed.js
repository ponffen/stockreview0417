/**
 * Dynamics feed: trades + community_posts merged query.
 */

const { initPool, dbQuery, normalizeSymbol, getAccounts, getUserCommunityRow } = require("../db");
const { enrichDynamicsCards } = require("../symbol-name-resolve");
const { buildCardFromFeedRow } = require("./dynamics-card-build");
const { applyDynamicsRedaction, SCENES } = require("./dynamics-redact");
const { toClientImageUrls } = require("./blob-images");

const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 30;

/** 交易卡：trade_date 与 created_at 是否同一天（Asia/Shanghai 日历日） */
const TRADE_SAME_DAY_EXPR = `t.trade_date::date = (to_timestamp(t.created_at / 1000.0) AT TIME ZONE 'Asia/Shanghai')::date`;
/** 异天补录：按成交日末排序 */
const TRADE_DATE_END_MS = `((EXTRACT(EPOCH FROM (t.trade_date::date + TIME '23:59:59')) * 1000)::bigint + 999)`;
/** 交易卡有效排序键：同天用 created_at，异天用 trade_date 日末 */
const TRADE_SORT_KEY_EXPR = `CASE WHEN ${TRADE_SAME_DAY_EXPR} THEN t.created_at::bigint ELSE ${TRADE_DATE_END_MS} END`;

function encodeCursor(cursor) {
  if (!cursor) {
    return null;
  }
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
}

function decodeCursor(raw) {
  const s = String(raw || "").trim();
  if (!s) {
    return null;
  }
  try {
    const obj = JSON.parse(Buffer.from(s, "base64url").toString("utf8"));
    if (!obj || typeof obj !== "object") {
      return null;
    }
    return obj;
  } catch {
    return null;
  }
}

function resolveScene(input) {
  const scene = String(input || "").trim();
  if (Object.values(SCENES).includes(scene)) {
    return scene;
  }
  return SCENES.COMMUNITY;
}

function buildUserScopeSql(scene, params, { viewerId, targetUserId }) {
  const viewer = String(viewerId || "").trim();
  const target = String(targetUserId || "").trim();
  if (scene === SCENES.SELF || scene === SCENES.STOCK_SELF) {
    params.push(viewer);
    const uidIdx = params.length;
    return {
      tradeJoin: `INNER JOIN users u ON u.id = t.user_id`,
      postJoin: `INNER JOIN users u ON u.id = p.user_id`,
      tradeWhere: `t.user_id = $${uidIdx} AND t.type = 'trade'`,
      postWhere: `p.user_id = $${uidIdx}`,
    };
  }
  if (scene === SCENES.PUBLIC || scene === SCENES.STOCK_PUBLIC) {
    params.push(target);
    const uidIdx = params.length;
    return {
      tradeJoin: `INNER JOIN users u ON u.id = t.user_id AND COALESCE(u.community_public, 1) = 1`,
      postJoin: `INNER JOIN users u ON u.id = p.user_id AND COALESCE(u.community_public, 1) = 1`,
      tradeWhere: `t.user_id = $${uidIdx} AND t.type = 'trade'`,
      postWhere: `p.user_id = $${uidIdx}`,
    };
  }
  params.push(viewer);
  const viewerIdx = params.length;
  return {
    tradeJoin: `INNER JOIN users u ON u.id = t.user_id AND COALESCE(u.community_public, 1) = 1
      INNER JOIN community_follows f ON f.followee_id = t.user_id AND f.follower_id = $${viewerIdx}`,
    postJoin: `INNER JOIN users u ON u.id = p.user_id AND COALESCE(u.community_public, 1) = 1
      INNER JOIN community_follows f ON f.followee_id = p.user_id AND f.follower_id = $${viewerIdx}`,
    tradeWhere: `t.type = 'trade'`,
    postWhere: `TRUE`,
  };
}

function buildStockFilterSql(scene, symbol, params, { includeTrades = true, includePosts = true } = {}) {
  const sym = normalizeSymbol(symbol);
  if (!sym) {
    return { tradeExtra: "", postExtra: "", excludePureOpinion: false };
  }
  let tradeExtra = "";
  let postExtra = "";
  if (includeTrades) {
    params.push(sym);
    tradeExtra = ` AND t.symbol = $${params.length}`;
  }
  if (includePosts) {
    params.push(JSON.stringify([sym]));
    postExtra = ` AND p.symbols::jsonb @> $${params.length}::jsonb AND jsonb_array_length(p.symbols::jsonb) > 0`;
  }
  return {
    tradeExtra,
    postExtra,
    excludePureOpinion: true,
  };
}

function normalizeFeedFilter(raw) {
  const f = String(raw || "all").trim().toLowerCase();
  if (f === "trade" || f === "viewpoint" || f === "valuation") {
    return f;
  }
  return "all";
}

function resolveFeedKinds(filter) {
  const f = normalizeFeedFilter(filter);
  if (f === "trade") {
    return { includeTrades: true, includePosts: false, postTypeExtra: "" };
  }
  if (f === "viewpoint") {
    return {
      includeTrades: false,
      includePosts: true,
      postTypeExtra: ` AND COALESCE(p.post_type, 'viewpoint') = 'viewpoint'`,
    };
  }
  if (f === "valuation") {
    return { includeTrades: false, includePosts: true, postTypeExtra: ` AND p.post_type = 'valuation'` };
  }
  return { includeTrades: true, includePosts: true, postTypeExtra: "" };
}

async function listDynamicsFeed(options = {}) {
  await initPool();
  const viewerId = String(options.viewerId || "").trim();
  const targetUserId = String(options.targetUserId || viewerId || "").trim();
  const scene = resolveScene(options.scene);
  const symbol = options.symbol ? normalizeSymbol(options.symbol) : "";
  const limit = Math.min(MAX_LIMIT, Math.max(1, Number(options.limit) || DEFAULT_LIMIT));
  const offset = Math.max(0, Math.floor(Number(options.offset) || 0));
  const cursor = decodeCursor(options.cursor);
  const isSelf = viewerId && targetUserId && viewerId === targetUserId;
  const feedKinds = resolveFeedKinds(options.filter);

  if (scene === SCENES.PUBLIC || scene === SCENES.STOCK_PUBLIC) {
    if (!targetUserId) {
      return { data: [], pagination: { limit, hasMore: false, cursor: null } };
    }
    if (!isSelf) {
      const row = await getUserCommunityRow(targetUserId);
      if (!row || !Number(row.community_public)) {
        return { data: [], pagination: { limit, hasMore: false, cursor: null }, error: "hidden" };
      }
    }
  }

  const params = [];
  const scope = buildUserScopeSql(scene, params, { viewerId, targetUserId });

  const isStockScene = scene === SCENES.STOCK_SELF || scene === SCENES.STOCK_PUBLIC;
  const includePosts = feedKinds.includePosts && (!isStockScene || Boolean(symbol));
  const includeTrades = feedKinds.includeTrades;
  const stock = buildStockFilterSql(scene, symbol, params, { includeTrades, includePosts });

  let cursorClause = "";
  const cursorSortKey =
    cursor && cursor.sortKey != null
      ? Number(cursor.sortKey)
      : cursor && cursor.sortMs != null
        ? Number(cursor.sortMs)
        : null;
  if (cursorSortKey != null && cursor.createdAt != null && cursor.id) {
    params.push(cursorSortKey, Number(cursor.createdAt), String(cursor.id));
    const a = params.length - 2;
    const b = params.length - 1;
    const c = params.length;
    cursorClause = `WHERE (sort_key, created_at, id) < ($${a}, $${b}, $${c})`;
  }

  const tradeSelect = includeTrades
    ? `
    SELECT
      'trade'::text AS card_kind,
      t.id,
      t.user_id,
      ${TRADE_SORT_KEY_EXPR} AS sort_key,
      t.created_at,
      t.symbol,
      t.name,
      t.side,
      t.price,
      t.quantity,
      t.amount,
      t.trade_date,
      t.note,
      t.account_id,
      t.amount_share_ratio,
      t.image_urls,
      NULL::text AS content,
      '[]'::text AS symbols,
      NULL::text AS post_type,
      NULL::text AS extra,
      u.nickname,
      u.phone
    FROM trades t
    ${scope.tradeJoin}
    WHERE ${scope.tradeWhere}${stock.tradeExtra}
  `
    : `
    SELECT
      NULL::text AS card_kind,
      NULL::text AS id,
      NULL::text AS user_id,
      NULL::bigint AS sort_key,
      NULL::bigint AS created_at,
      NULL::text AS symbol,
      NULL::text AS name,
      NULL::text AS side,
      NULL::double precision AS price,
      NULL::double precision AS quantity,
      NULL::double precision AS amount,
      NULL::text AS trade_date,
      NULL::text AS note,
      NULL::text AS account_id,
      NULL::double precision AS amount_share_ratio,
      NULL::text AS image_urls,
      NULL::text AS content,
      NULL::text AS symbols,
      NULL::text AS post_type,
      NULL::text AS extra,
      NULL::text AS nickname,
      NULL::text AS phone
    WHERE FALSE
  `;

  const postSelect = includePosts
    ? `
    UNION ALL
    SELECT
      'post'::text AS card_kind,
      p.id,
      p.user_id,
      p.created_at::bigint AS sort_key,
      p.created_at,
      NULL::text AS symbol,
      NULL::text AS name,
      NULL::text AS side,
      NULL::double precision AS price,
      NULL::double precision AS quantity,
      NULL::double precision AS amount,
      NULL::text AS trade_date,
      NULL::text AS note,
      NULL::text AS account_id,
      NULL::double precision AS amount_share_ratio,
      p.image_urls,
      p.content,
      p.symbols,
      p.post_type,
      p.extra,
      u.nickname,
      u.phone
    FROM community_posts p
    ${scope.postJoin}
    WHERE ${scope.postWhere}${stock.postExtra}${feedKinds.postTypeExtra}
  `
    : "";

  let offsetClause = "";
  if (offset > 0 && !cursorClause) {
    params.push(offset);
    offsetClause = `OFFSET $${params.length}`;
  }
  params.push(limit + 1);
  const limitIdx = params.length;

  const sql = `
    SELECT * FROM (
      ${tradeSelect}
      ${postSelect}
    ) feed
    ${cursorClause}
    ORDER BY sort_key DESC, created_at DESC, id DESC
    ${offsetClause}
    LIMIT $${limitIdx}
  `;

  const { rows } = await dbQuery(sql, params);
  const hasMore = rows.length > limit;
  const slice = hasMore ? rows.slice(0, limit) : rows;

  const accounts =
    isSelf || scene === SCENES.SELF || scene === SCENES.STOCK_SELF
      ? await getAccounts(targetUserId)
      : [];
  const accountNameById = {};
  for (const acc of accounts) {
    accountNameById[acc.id] = acc.name || acc.id;
  }

  const ctx = { accountNameById };
  const cards = slice.map((row) => buildCardFromFeedRow(row, ctx));
  await enrichDynamicsCards(cards);
  const data = cards.map((card) => {
    const out = applyDynamicsRedaction(card, scene, { isSelf });
    if (Array.isArray(out.imageUrls) && out.imageUrls.length) {
      out.imageUrls = toClientImageUrls(out.imageUrls);
    }
    return out;
  });

  let nextCursor = null;
  if (hasMore && slice.length) {
    const last = slice[slice.length - 1];
    nextCursor = encodeCursor({
      sortKey: Number(last.sort_key),
      createdAt: Number(last.created_at),
      id: String(last.id),
      cardKind: String(last.card_kind),
    });
  }

  return {
    data,
    pagination: {
      limit,
      hasMore,
      cursor: nextCursor,
    },
  };
}

module.exports = {
  SCENES,
  listDynamicsFeed,
  encodeCursor,
  decodeCursor,
  normalizeFeedFilter,
};
