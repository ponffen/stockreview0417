/**
 * 写路径：标记 rebuilding 后异步调 /api/cron/freeze-eod（独立 Serverless 调用，不占保存请求 60s）。
 */
const { upsertUserMetricsMeta } = require("./db");
const {
  partitionHintDates,
  resolveFrozenThroughForUser,
  refreshLiveMetricsOnly,
  earliestFromHints,
} = require("./metrics-rebuild-service");

function isVercelRuntime() {
  return String(process.env.VERCEL || "").trim() === "1";
}

function resolveInternalApiOrigin() {
  const vercel = String(process.env.VERCEL_URL || "").trim();
  if (vercel) {
    return vercel.startsWith("http") ? vercel.replace(/\/$/, "") : `https://${vercel}`;
  }
  const site = String(process.env.SITE_URL || process.env.APP_URL || "").trim().replace(/\/$/, "");
  if (site) {
    return site;
  }
  const port = Number(process.env.PORT) || 3030;
  return `http://127.0.0.1:${port}`;
}

function cronSecret() {
  return String(process.env.CRON_SECRET || process.env.VERCEL_CRON_SECRET || "").trim();
}

async function runFreezeJobDirect(payload) {
  const { runDailyFreeze } = require("./eod-freeze-service");
  return runDailyFreeze({
    force: payload.force !== false,
    syncDailyClose: !!payload.syncDailyClose,
    userIds: payload.userIds || [],
    rebuildFromDate: payload.rebuildFromDate || null,
    fullRebuild: !!payload.fullRebuild,
    logger: console,
  });
}

function normalizeDispatchBody(payload) {
  return {
    userIds: payload.userIds || [],
    force: payload.force !== false,
    syncDailyClose: !!payload.syncDailyClose,
    rebuildFromDate: payload.rebuildFromDate || undefined,
    fullRebuild: !!payload.fullRebuild,
  };
}

async function dispatchFreezeEodJobAsync(payload) {
  const secret = cronSecret();
  const body = normalizeDispatchBody(payload);
  if (!body.userIds.length) {
    return;
  }

  if (!isVercelRuntime() || !secret) {
    await runFreezeJobDirect(body);
    return;
  }

  const url = `${resolveInternalApiOrigin()}/api/cron/freeze-eod`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${secret}`,
      },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      const detail = await response.text().catch(() => "");
      console.warn(
        "[metrics-rebuild-trigger] freeze-eod dispatch http",
        response.status,
        detail ? detail.slice(0, 200) : "",
      );
      await runFreezeJobDirect(body);
    }
  } catch (e) {
    console.warn("[metrics-rebuild-trigger] freeze-eod dispatch failed", e?.message || e);
    await runFreezeJobDirect(body);
  }
}

function dispatchFreezeEodJob(payload) {
  const { runInBackground } = require("./background-task");
  runInBackground(() => dispatchFreezeEodJobAsync(payload));
}

/**
 * 合并 hint 后打标并异步触发 freeze（不 await）。
 */
async function triggerLedgerMetricsFreeze(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { ok: false };
  }
  const fullRebuild = !!opts.fullRebuild;
  const hintDates = opts.hintDates || [];

  if (!fullRebuild) {
    const frozenThrough = await resolveFrozenThroughForUser(uid);
    const { freezeHints, liveOnlyHints } = partitionHintDates(hintDates, frozenThrough);
    if (!freezeHints.length) {
      if (liveOnlyHints.length) {
        await refreshLiveMetricsOnly(uid);
      }
      return { ok: true, skip: true, reason: "live-only-after-frozen-through" };
    }
    const rebuildFromDate = earliestFromHints(freezeHints, false);
    await upsertUserMetricsMeta(uid, {
      rebuilding: true,
      rebuildFrom: rebuildFromDate || null,
    });
    await dispatchFreezeEodJobAsync({
      userIds: [uid],
      force: true,
      rebuildFromDate: rebuildFromDate || undefined,
      fullRebuild: false,
    });
    return { ok: true, dispatched: true, rebuildFromDate };
  }

  await upsertUserMetricsMeta(uid, {
    rebuilding: true,
    rebuildFrom: null,
  });
  await dispatchFreezeEodJobAsync({
    userIds: [uid],
    force: true,
    fullRebuild: true,
  });
  return { ok: true, dispatched: true, fullRebuild: true };
}

module.exports = {
  dispatchFreezeEodJob,
  dispatchFreezeEodJobAsync,
  triggerLedgerMetricsFreeze,
  resolveInternalApiOrigin,
};
