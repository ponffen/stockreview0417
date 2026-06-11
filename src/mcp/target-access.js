const { getUserCommunityRow } = require("../db");

async function resolveDataAccess(viewerId, targetUserId) {
  const viewer = String(viewerId || "").trim();
  const target = String(targetUserId || viewer).trim() || viewer;
  if (!viewer) {
    return { ok: false, status: 401, error: "unauthorized" };
  }
  if (target === viewer) {
    return { ok: true, mode: "private", dataUserId: viewer, viewerId: viewer, targetId: viewer };
  }
  const row = await getUserCommunityRow(target);
  if (!row) {
    return { ok: false, status: 404, error: "用户不存在" };
  }
  if (!Number(row.community_public)) {
    return { ok: false, status: 403, error: "未公开持仓" };
  }
  return { ok: true, mode: "public", dataUserId: target, viewerId: viewer, targetId: target };
}

module.exports = {
  resolveDataAccess,
};
