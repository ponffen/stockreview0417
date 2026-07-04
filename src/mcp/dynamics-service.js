/**
 * MCP read-only dynamics feed (portfolio / stock / community following).
 */

const { normalizeSymbol } = require("../db");
const { listDynamicsFeed } = require("../dynamics/dynamics-feed");
const { SCENES } = require("../dynamics/dynamics-scenes");
const { resolveDataAccess } = require("./target-access");
const { getPublicBaseUrl } = require("./config");

const MCP_DEFAULT_LIMIT = 20;
const MCP_MAX_LIMIT = 30;
const MCP_PAGE_SIZE = 30;
const MCP_MAX_TOTAL_ITEMS = 1000;

function parseDynamicsLimit(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    return MCP_DEFAULT_LIMIT;
  }
  return Math.min(MCP_MAX_LIMIT, Math.max(1, Math.floor(n)));
}

function parseDateKey(raw) {
  const s = String(raw || "").slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

function msToDateKey(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n <= 0) {
    return "";
  }
  const d = new Date(n);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function cardSortDateKey(card) {
  const ms = Number(card?.sortMs ?? card?.createdAt);
  return msToDateKey(ms);
}

function cardInDateRange(card, fromKey, toKey) {
  const dk = cardSortDateKey(card);
  if (!dk) {
    return true;
  }
  if (fromKey && dk < fromKey) {
    return false;
  }
  if (toKey && dk > toKey) {
    return false;
  }
  return true;
}

function shouldFetchAllMcpDynamics(input = {}) {
  const cursor = input.cursor != null ? String(input.cursor).trim() : "";
  if (cursor) {
    return false;
  }
  if (input.fetch_all === false || input.fetch_all === "false") {
    return false;
  }
  return true;
}

async function collectDynamicsFeedPages(feedOptions, input = {}) {
  const fromKey = parseDateKey(input.from);
  const toKey = parseDateKey(input.to);
  const fetchAll = shouldFetchAllMcpDynamics(input);
  const singleLimit = parseDynamicsLimit(input.limit);

  if (!fetchAll) {
    const result = await listDynamicsFeed({
      ...feedOptions,
      limit: singleLimit,
      cursor: input.cursor != null ? String(input.cursor) : "",
    });
    if (fromKey || toKey) {
      result.data = (result.data || []).filter((card) => cardInDateRange(card, fromKey, toKey));
    }
    return result;
  }

  const allCards = [];
  let cursor = "";
  let truncated = false;
  let resumeCursor = null;

  while (allCards.length < MCP_MAX_TOTAL_ITEMS) {
    const result = await listDynamicsFeed({
      ...feedOptions,
      limit: MCP_PAGE_SIZE,
      cursor,
    });
    if (result.error) {
      return result;
    }

    const pageCards = result.data || [];
    const filtered = fromKey || toKey
      ? pageCards.filter((card) => cardInDateRange(card, fromKey, toKey))
      : pageCards;
    allCards.push(...filtered);

    if (!result.pagination?.hasMore) {
      break;
    }

    const lastRaw = pageCards[pageCards.length - 1];
    if (fromKey && lastRaw) {
      const oldestKey = cardSortDateKey(lastRaw);
      if (oldestKey && oldestKey < fromKey) {
        break;
      }
    }

    cursor = result.pagination.cursor;
    if (!cursor) {
      break;
    }

    if (allCards.length >= MCP_MAX_TOTAL_ITEMS) {
      truncated = true;
      resumeCursor = cursor;
      break;
    }
  }

  return {
    data: allCards.slice(0, MCP_MAX_TOTAL_ITEMS),
    pagination: {
      limit: allCards.length,
      hasMore: truncated,
      cursor: truncated ? resumeCursor : null,
      fetchAll: true,
      truncated,
      totalReturned: allCards.length,
      maxTotal: MCP_MAX_TOTAL_ITEMS,
    },
  };
}

function resolveDynamicsScene(viewerId, dataUserId, symbol) {
  const viewer = String(viewerId || "").trim();
  const target = String(dataUserId || viewer).trim() || viewer;
  const sym = symbol ? normalizeSymbol(symbol) : "";
  const isSelf = viewer && target && viewer === target;
  if (isSelf) {
    if (sym) {
      return { scene: SCENES.STOCK_SELF, scope: "stock", symbol: sym };
    }
    return { scene: SCENES.SELF, scope: "portfolio", symbol: null };
  }
  if (sym) {
    return { scene: SCENES.STOCK_PUBLIC, scope: "stock", symbol: sym };
  }
  return { scene: SCENES.PUBLIC, scope: "portfolio", symbol: null };
}

function absoluteImageUrls(urls, baseUrl) {
  const base = String(baseUrl || "https://www.higcc.com").replace(/\/+$/, "");
  return (Array.isArray(urls) ? urls : [])
    .map((url) => {
      const u = String(url || "").trim();
      if (!u) {
        return "";
      }
      if (u.startsWith("http://") || u.startsWith("https://")) {
        return u;
      }
      if (u.startsWith("/")) {
        return `${base}${u}`;
      }
      return u;
    })
    .filter(Boolean);
}

function serializeMcpDynamicsCard(card, mode) {
  const c = card || {};
  const kind = c.cardKind === "post" ? "post" : "trade";
  const isPrivate = mode === "private";
  const sortAt = msToDateKey(c.sortMs ?? c.createdAt) || null;
  const out = {
    cardKind: kind,
    id: c.id,
    userId: c.userId,
    displayName: c.displayName,
    createdAt: c.createdAt,
    publishedAt: c.bottomTime,
    sortAt,
  };

  if (kind === "trade") {
    out.trade = {
      symbol: c.symbol,
      name: c.name,
      side: c.side,
      price: c.price,
      tradeDate: c.tradeDate,
      note: String(c.note || "").trim(),
      imageUrls: c.imageUrls || [],
    };
    if (isPrivate) {
      out.trade.quantity = c.quantity;
      out.trade.amount = c.amount;
      out.trade.accountId = c.accountId;
      out.trade.accountName = c.accountName;
    } else if (c.amountShareRatio != null && c.amountShareRatio !== "" && c.amountShareRatio !== "—") {
      out.trade.amountShareRatio = c.amountShareRatio;
    }
  } else {
    out.post = {
      content: String(c.content || ""),
      symbols: (Array.isArray(c.symbols) ? c.symbols : []).map((s) => ({
        symbol: s.symbol,
        name: s.name,
      })),
      imageUrls: c.imageUrls || [],
    };
  }

  return out;
}

function attachAbsoluteImageUrls(items, baseUrl) {
  return items.map((item) => {
    const next = { ...item };
    if (next.trade?.imageUrls) {
      next.trade = { ...next.trade, imageUrls: absoluteImageUrls(next.trade.imageUrls, baseUrl) };
    }
    if (next.post?.imageUrls) {
      next.post = { ...next.post, imageUrls: absoluteImageUrls(next.post.imageUrls, baseUrl) };
    }
    return next;
  });
}

function toolMeta(access, extra = {}) {
  return {
    viewerId: access.viewerId,
    targetId: access.targetId,
    mode: access.mode,
    ...extra,
  };
}

async function getDynamicsForMcp(viewerId, input = {}) {
  const access = await resolveDataAccess(viewerId, input.target_user_id);
  if (!access.ok) {
    return { ok: false, status: access.status || 403, error: access.error || "forbidden" };
  }

  const symbolInput = input.symbol ? normalizeSymbol(String(input.symbol)) : "";
  const { scene, scope, symbol } = resolveDynamicsScene(viewerId, access.dataUserId, symbolInput);

  const result = await collectDynamicsFeedPages(
    {
      viewerId: access.viewerId,
      targetUserId: access.dataUserId,
      scene,
      symbol: symbol || "",
    },
    input,
  );

  if (result.error === "hidden") {
    return { ok: false, status: 403, error: "用户未公开或不可见" };
  }

  const baseUrl = getPublicBaseUrl();
  const data = attachAbsoluteImageUrls(
    (result.data || []).map((card) => serializeMcpDynamicsCard(card, access.mode)),
    baseUrl,
  );

  return {
    ok: true,
    meta: toolMeta(access, {
      scope,
      symbol: symbol || null,
      from: parseDateKey(input.from) || null,
      to: parseDateKey(input.to) || null,
      fetchAll: shouldFetchAllMcpDynamics(input),
    }),
    data,
    pagination: result.pagination,
  };
}

async function getCommunityDynamicsFeedForMcp(viewerId, input = {}) {
  const viewer = String(viewerId || "").trim();
  if (!viewer) {
    return { ok: false, status: 401, error: "unauthorized" };
  }

  const result = await collectDynamicsFeedPages(
    {
      viewerId: viewer,
      scene: SCENES.COMMUNITY,
    },
    input,
  );

  const baseUrl = getPublicBaseUrl();
  const data = attachAbsoluteImageUrls(
    (result.data || []).map((card) => serializeMcpDynamicsCard(card, "public")),
    baseUrl,
  );

  return {
    ok: true,
    meta: {
      viewerId: viewer,
      scope: "community_following",
      mode: "public",
      from: parseDateKey(input.from) || null,
      to: parseDateKey(input.to) || null,
      fetchAll: shouldFetchAllMcpDynamics(input),
    },
    data,
    pagination: result.pagination,
  };
}

module.exports = {
  getDynamicsForMcp,
  getCommunityDynamicsFeedForMcp,
  shouldFetchAllMcpDynamics,
  MCP_MAX_TOTAL_ITEMS,
};
