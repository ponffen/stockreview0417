const { initPool, getUserCommunityRow } = require("../db");
const {
  displayNameForUser,
  buildUserCard,
  getLeaderboard,
  getFollowingCards,
  getFeedTrades,
  enrichLeaderboardPayloadWithViewer,
} = require("../community-service");

const MATCH_PRIORITY = {
  exact_nickname: 0,
  self: 1,
  display_name_exact: 2,
  fuzzy_nickname: 3,
  display_name_partial: 4,
};

function normalizeQuery(query) {
  return String(query || "").trim();
}

function queryLower(query) {
  return normalizeQuery(query).toLowerCase();
}

function displayNameMatches(displayName, query) {
  const dn = String(displayName || "").trim().toLowerCase();
  const q = queryLower(query);
  if (!dn || !q) {
    return { exact: false, partial: false };
  }
  return {
    exact: dn === q,
    partial: dn.includes(q) || q.includes(dn),
  };
}

function matchPriority(matchType) {
  return MATCH_PRIORITY[matchType] ?? 99;
}

function upsertCandidate(map, userId, matchType, row = null) {
  const id = String(userId || "").trim();
  if (!id) {
    return;
  }
  const prev = map.get(id);
  if (!prev || matchPriority(matchType) < matchPriority(prev.matchType)) {
    map.set(id, { matchType, row });
  }
}

function topPositionLabels(card) {
  const tops = Array.isArray(card?.topPositions) ? card.topPositions : [];
  return tops
    .map((p) => String(p?.name || p?.symbol || "").trim())
    .filter(Boolean)
    .slice(0, 3);
}

async function searchUsersByNicknameSql(query, sqlLimit = 20) {
  const q = normalizeQuery(query);
  if (!q) {
    return [];
  }
  const pool = await initPool();
  const { rows } = await pool.query(
    `SELECT id, nickname, phone, community_public
     FROM users
     WHERE nickname IS NOT NULL AND length(trim(nickname)) > 0
       AND nickname ILIKE $1
     ORDER BY CASE WHEN lower(trim(nickname)) = lower($2) THEN 0 ELSE 1 END, nickname ASC
     LIMIT $3`,
    [`%${q}%`, q, sqlLimit]
  );
  return rows;
}

async function collectCandidateIds(viewerId, query) {
  const map = new Map();
  const q = normalizeQuery(query);

  for (const row of await searchUsersByNicknameSql(q)) {
    const nick = String(row.nickname || "").trim();
    const exact = nick.toLowerCase() === q.toLowerCase();
    upsertCandidate(map, row.id, exact ? "exact_nickname" : "fuzzy_nickname", row);
  }

  const viewer = String(viewerId || "").trim();
  if (viewer) {
    const selfRow = await getUserCommunityRow(viewer);
    if (selfRow) {
      const dn = displayNameForUser(selfRow);
      const m = displayNameMatches(dn, q);
      if (m.exact || m.partial) {
        upsertCandidate(map, viewer, "self", selfRow);
      }
    }
  }

  let leaderboard = await getLeaderboard();
  if (viewer) {
    leaderboard = await enrichLeaderboardPayloadWithViewer(leaderboard, viewer);
  }
  for (const card of leaderboard?.entries || []) {
    const m = displayNameMatches(card.displayName, q);
    if (m.exact) {
      upsertCandidate(map, card.userId, "display_name_exact");
    } else if (m.partial) {
      upsertCandidate(map, card.userId, "display_name_partial");
    }
  }

  if (viewer) {
    for (const card of await getFollowingCards(viewer)) {
      const m = displayNameMatches(card.displayName, q);
      if (m.exact) {
        upsertCandidate(map, card.userId, "display_name_exact");
      } else if (m.partial) {
        upsertCandidate(map, card.userId, "display_name_partial");
      }
    }
    const seenFeed = new Set();
    for (const row of await getFeedTrades(viewer)) {
      const uid = String(row.userId || "").trim();
      if (!uid || seenFeed.has(uid)) {
        continue;
      }
      seenFeed.add(uid);
      const m = displayNameMatches(row.displayName, q);
      if (m.exact) {
        upsertCandidate(map, uid, "display_name_exact");
      } else if (m.partial) {
        upsertCandidate(map, uid, "display_name_partial");
      }
    }
  }

  return [...map.entries()]
    .map(([userId, meta]) => ({ userId, ...meta }))
    .sort((a, b) => matchPriority(a.matchType) - matchPriority(b.matchType));
}

async function buildAccessibleCandidate(viewerId, entry) {
  const tid = String(entry.userId || "").trim();
  const viewer = String(viewerId || "").trim();
  const isSelf = viewer && tid === viewer;
  const row = entry.row || (await getUserCommunityRow(tid));
  if (!row) {
    return null;
  }
  const displayName = displayNameForUser(row);
  const isPublic = !!Number(row.community_public);

  if (!isSelf && !isPublic) {
    return {
      kind: "blocked",
      display_name: displayName,
      match_type: entry.matchType,
      accessible: false,
      reason: "该用户未公开持仓，无法查看",
    };
  }

  const card = await buildUserCard(tid, viewer, { allowHidden: isSelf });
  if (!card) {
    return null;
  }

  return {
    kind: "accessible",
    user_id: tid,
    display_name: card.displayName || displayName,
    match_type: entry.matchType,
    accessible: true,
    is_self: isSelf,
    hints: {
      ytd_twr: card.ytdTwr ?? null,
      mtd_twr: card.mtdTwr ?? null,
      top_positions: topPositionLabels(card),
      following: !!card.following,
      mutual: !!card.mutual,
    },
  };
}

async function searchCommunityUsers(viewerId, input = {}) {
  const query = normalizeQuery(input.query);
  const limit = Math.min(10, Math.max(1, Number(input.limit) || 5));
  if (!viewerId) {
    const err = new Error("unauthorized");
    err.status = 401;
    throw err;
  }
  if (!query) {
    return {
      query,
      candidates: [],
      blocked: [],
      needs_confirmation: false,
      guidance:
        "请提供社区昵称或展示名关键词。查他人时须先确认候选人，再将 user_id 作为 target_user_id 调用其他工具。",
    };
  }

  const entries = await collectCandidateIds(viewerId, query);
  const accessible = [];
  const blocked = [];
  const seenAccessible = new Set();
  const seenBlocked = new Set();

  for (const entry of entries) {
    const built = await buildAccessibleCandidate(viewerId, entry);
    if (!built) {
      continue;
    }
    if (built.kind === "blocked") {
      const key = String(built.display_name || "").toLowerCase();
      if (!seenBlocked.has(key)) {
        seenBlocked.add(key);
        blocked.push({
          display_name: built.display_name,
          match_type: built.match_type,
          accessible: false,
          reason: built.reason,
        });
      }
      continue;
    }
    if (seenAccessible.has(built.user_id)) {
      continue;
    }
    seenAccessible.add(built.user_id);
    accessible.push({
      user_id: built.user_id,
      display_name: built.display_name,
      match_type: built.match_type,
      accessible: true,
      is_self: built.is_self,
      hints: built.hints,
    });
    if (accessible.length >= limit) {
      break;
    }
  }

  return {
    query,
    candidates: accessible,
    blocked,
    needs_confirmation: accessible.length > 0,
    guidance:
      accessible.length === 0
        ? "未找到可查看的公开用户。请检查昵称拼写，或确认对方已在麻雀公开持仓。"
        : accessible.length === 1
          ? "已向用户展示 1 位候选人。请让用户确认「是」后再将 user_id 作为 target_user_id 调用 get_holdings 等工具；未确认前不要读数。"
          : `已向用户展示 ${accessible.length} 位候选人。请让用户回复序号确认目标用户，再将对应 user_id 作为 target_user_id 继续查询。`,
  };
}

module.exports = {
  searchCommunityUsers,
};
