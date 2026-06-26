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

function normalizeOrigin(value) {
  const raw = String(value || "").trim().replace(/\/$/, "");
  if (!raw) {
    return "";
  }
  return raw.startsWith("http") ? raw : `https://${raw}`;
}

function resolveInternalApiOrigin() {
  // Vercel 实例互调：优先部署域名，避免自定义域 CDN 剥掉 Authorization
  if (isVercelRuntime()) {
    const vercel = normalizeOrigin(process.env.VERCEL_URL);
    if (vercel) {
      return vercel;
    }
    const production = normalizeOrigin(process.env.VERCEL_PROJECT_PRODUCTION_URL);
    if (production) {
      return production;
    }
  }
  const site = normalizeOrigin(process.env.SITE_URL || process.env.APP_URL);
  if (site) {
    return site;
  }
  const production = normalizeOrigin(process.env.VERCEL_PROJECT_PRODUCTION_URL);
  if (production) {
    return production;
  }
  const vercel = normalizeOrigin(process.env.VERCEL_URL);
  if (vercel) {
    return vercel;
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

async function postFreezeEodDispatch(url, secret, body) {
  const query = new URLSearchParams();
  if (body.rebuildFromDate) {
    query.set("rebuildFromDate", String(body.rebuildFromDate).slice(0, 10));
  }
  if (body.force) {
    query.set("force", "true");
  }
  if (body.fullRebuild) {
    query.set("fullRebuild", "true");
  }
  const targetUrl = query.toString() ? `${url}?${query}` : url;
  return fetch(targetUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${secret}`,
      "x-cron-secret": secret,
    },
    body: JSON.stringify({ ...body, token: secret }),
  });
}

function freezeDispatchUsersFailed(payload) {
  const users = Array.isArray(payload?.data?.users) ? payload.data.users : [];
  return users.some((row) => row?.skipped);
}

async function readFreezeDispatchPayload(response) {
  const text = await response.text().catch(() => "");
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
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
  const userIdsLabel = body.userIds.join(",");
  const maxAttempts = 2;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await postFreezeEodDispatch(url, secret, body);
      const payload = await readFreezeDispatchPayload(response);
      const usersFailed = response.ok && freezeDispatchUsersFailed(payload);
      if (response.ok && !usersFailed) {
        console.log(
          "[metrics-rebuild-trigger] freeze-eod dispatch ok status=%s userIds=%s attempt=%s origin=%s rebuildFromDate=%s",
          response.status,
          userIdsLabel,
          attempt,
          resolveInternalApiOrigin(),
          body.rebuildFromDate || "-",
        );
        return;
      }
      const detail = payload ? JSON.stringify(payload).slice(0, 200) : "";
      console.warn(
        "[metrics-rebuild-trigger] freeze-eod dispatch failed status=%s userIds=%s attempt=%s origin=%s rebuildFromDate=%s %s",
        response.status,
        userIdsLabel,
        attempt,
        resolveInternalApiOrigin(),
        body.rebuildFromDate || "-",
        detail,
      );
    } catch (e) {
      console.warn(
        "[metrics-rebuild-trigger] freeze-eod dispatch failed userIds=%s attempt=%s origin=%s %s",
        userIdsLabel,
        attempt,
        resolveInternalApiOrigin(),
        e?.message || e,
      );
    }
    if (attempt < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  console.warn(
    "[metrics-rebuild-trigger] freeze-eod dispatch gave up userIds=%s; rebuilding stays true",
    userIdsLabel,
  );
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
  dispatchFreezeEodJobAsync,
  triggerLedgerMetricsFreeze,
  resolveInternalApiOrigin,
};
