/**
 * 写路径：成交响应前同步打标 rebuilding；Vercel 上 HTTP 调 async freeze-eod（独立实例），本地同进程。
 */
const { upsertUserMetricsMeta, getUserMetricsMeta } = require("./db");
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

function isFreezeUserFailure(row) {
  return row?.skipped && row?.reason !== "already-up-to-date";
}

async function runAndVerifyFreeze(body) {
  const userIdsLabel = body.userIds.join(",");
  const t0 = Date.now();
  const data = await runFreezeJobDirect(body);
  const failed = (data?.users || []).filter(isFreezeUserFailure);
  if (failed.length) {
    const reasons = failed.map((row) => `${row.userId || "?"}:${row.reason || "skipped"}`).join(",");
    throw new Error(`freeze failed: ${reasons}`);
  }
  for (const uid of body.userIds) {
    const meta = await getUserMetricsMeta(uid, { light: true });
    if (meta.rebuilding) {
      console.warn("[metrics-rebuild-trigger] rebuilding still true after freeze, force clear", uid);
      await upsertUserMetricsMeta(uid, {
        rebuilding: false,
        rebuildFrom: null,
        dataVersion: (meta.dataVersion || 0) + 1,
      });
    }
  }
  console.log(
    "[metrics-rebuild-trigger] freeze ok userIds=%s rebuildFromDate=%s wallMs=%s serviceMs=%s",
    userIdsLabel,
    body.rebuildFromDate || "-",
    Date.now() - t0,
    data?.elapsedMs ?? "-",
  );
  return data;
}

const DISPATCH_ACCEPT_TIMEOUT_MS = 15_000;

async function postFreezeEodDispatch(url, secret, body) {
  const dispatchBody = { ...body, async: true, token: secret };
  return fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-cron-secret": secret,
    },
    body: JSON.stringify(dispatchBody),
    signal: AbortSignal.timeout(DISPATCH_ACCEPT_TIMEOUT_MS),
  });
}

async function dispatchFreezeEodViaHttp(body) {
  const secret = cronSecret();
  if (!secret) {
    return false;
  }
  const url = `${resolveInternalApiOrigin()}/api/cron/freeze-eod`;
  const userIdsLabel = body.userIds.join(",");
  const maxAttempts = 2;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await postFreezeEodDispatch(url, secret, body);
      const json = await response.json().catch(() => ({}));
      if (response.status === 202 && json?.ok) {
        console.log(
          "[metrics-rebuild-trigger] freeze-eod dispatch accepted userIds=%s attempt=%s origin=%s",
          userIdsLabel,
          attempt,
          resolveInternalApiOrigin(),
        );
        return true;
      }
      console.warn(
        "[metrics-rebuild-trigger] freeze-eod dispatch rejected status=%s userIds=%s attempt=%s origin=%s %s",
        response.status,
        userIdsLabel,
        attempt,
        resolveInternalApiOrigin(),
        json?.error ? String(json.error).slice(0, 200) : "",
      );
    } catch (e) {
      console.warn(
        "[metrics-rebuild-trigger] freeze-eod dispatch error userIds=%s attempt=%s origin=%s %s",
        userIdsLabel,
        attempt,
        resolveInternalApiOrigin(),
        e?.message || e,
      );
    }
    if (attempt < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, 400));
    }
  }
  return false;
}

async function dispatchFreezeEodJobAsync(payload) {
  const body = normalizeDispatchBody(payload);
  if (!body.userIds.length) {
    return;
  }

  if (isVercelRuntime()) {
    const accepted = await dispatchFreezeEodViaHttp(body);
    if (accepted) {
      return;
    }
    console.warn(
      "[metrics-rebuild-trigger] freeze-eod HTTP dispatch failed, falling back in-process userIds=%s",
      body.userIds.join(","),
    );
  }

  try {
    await runAndVerifyFreeze(body);
  } catch (e) {
    console.warn(
      "[metrics-rebuild-trigger] freeze failed userIds=%s rebuildFromDate=%s %s",
      body.userIds.join(","),
      body.rebuildFromDate || "-",
      e?.message || e,
    );
    console.warn(
      "[metrics-rebuild-trigger] freeze gave up userIds=%s; rebuilding stays true",
      body.userIds.join(","),
    );
  }
}

/**
 * 同步阶段：判断是否需要 freeze，需要则立即写 rebuilding=true。
 * @returns {Promise<{ payload?: object, skip?: boolean, reason?: string }>}
 */
async function prepareLedgerMetricsFreeze(userId, opts = {}) {
  const uid = String(userId || "").trim();
  if (!uid) {
    return { skip: true, reason: "missing-user" };
  }
  const fullRebuild = !!opts.fullRebuild;
  const hintDates = opts.hintDates || [];

  if (!fullRebuild) {
    if (!hintDates.length) {
      return { skip: true, reason: "no-hint-dates" };
    }
    const frozenThrough = await resolveFrozenThroughForUser(uid);
    const { freezeHints, liveOnlyHints } = partitionHintDates(hintDates, frozenThrough);
    if (!freezeHints.length) {
      if (liveOnlyHints.length) {
        await refreshLiveMetricsOnly(uid);
      }
      return { skip: true, reason: "live-only-after-frozen-through" };
    }
    const rebuildFromDate = earliestFromHints(freezeHints, false);
    await upsertUserMetricsMeta(uid, {
      rebuilding: true,
      rebuildFrom: rebuildFromDate || null,
    });
    return {
      payload: {
        userIds: [uid],
        force: true,
        rebuildFromDate: rebuildFromDate || undefined,
        fullRebuild: false,
      },
      rebuildFromDate,
    };
  }

  await upsertUserMetricsMeta(uid, {
    rebuilding: true,
    rebuildFrom: null,
  });
  return {
    payload: {
      userIds: [uid],
      force: true,
      fullRebuild: true,
    },
    fullRebuild: true,
  };
}

/** debounce flush 仍走此入口（内部 await freeze）。 */
async function triggerLedgerMetricsFreeze(userId, opts = {}) {
  const prepared = await prepareLedgerMetricsFreeze(userId, opts);
  if (!prepared.payload) {
    return { ok: true, skip: true, reason: prepared.reason || "no-op" };
  }
  await dispatchFreezeEodJobAsync(prepared.payload);
  return {
    ok: true,
    dispatched: true,
    rebuildFromDate: prepared.rebuildFromDate,
    fullRebuild: prepared.fullRebuild,
  };
}

module.exports = {
  prepareLedgerMetricsFreeze,
  dispatchFreezeEodJobAsync,
  triggerLedgerMetricsFreeze,
  runAndVerifyFreeze,
  isFreezeUserFailure,
  resolveInternalApiOrigin,
};
