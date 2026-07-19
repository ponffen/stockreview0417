/**
 * Browser page cache: IndexedDB for bundle snapshots + localStorage for cache-meta.
 * Exposes window.PageCache for app.js.
 */
(function initPageCache(global) {
  const DB_NAME = "stockreview_page_cache";
  const DB_VERSION = 1;
  const STORE = "entries";
  const LS_META_KEY = "stockreview_cache_meta";
  const LIVE_HEAD_TTL_MS = 60 * 1000;

  const PAGE_DOMAIN_DEPS = {
    home: ["ledger", "metrics", "quote"],
    analysis: ["ledger", "metrics", "quote"],
    stockRecord: ["ledger", "metrics", "quote"],
    dynamicsPortfolio: ["dynamics", "ledger"],
    communityFeed: ["dynamics", "ledger", "quote", "follow"],
    communityLeaderboard: ["follow", "metrics", "ledger", "quote"],
  };

  let dbPromise = null;
  let serverMetaPromise = null;
  let serverMetaCache = null;
  const publicMetaByTarget = new Map();
  const publicMetaPromises = new Map();
  const LS_PUBLIC_META_PREFIX = "stockreview_cache_meta_public:";

  function domainsForPart(part) {
    if (part === "seriesFrozen") {
      return ["metrics"];
    }
    if (part === "liveHead") {
      return ["ledger", "quote"];
    }
    return ["ledger", "metrics", "quote", "dynamics", "follow"];
  }

  function readLocalMeta() {
    try {
      const raw = global.localStorage.getItem(LS_META_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  }

  function writeLocalMeta(meta) {
    try {
      if (!meta) {
        global.localStorage.removeItem(LS_META_KEY);
        return;
      }
      global.localStorage.setItem(LS_META_KEY, JSON.stringify(meta));
    } catch {
      // quota or private mode
    }
  }

  function epochValue(meta, domain) {
    if (!meta) {
      return 0;
    }
    if (domain === "ledger") {
      return Number(meta.ledgerEpoch) || 0;
    }
    if (domain === "metrics") {
      return Number(meta.metricsEpoch) || 0;
    }
    if (domain === "dynamics") {
      return Number(meta.dynamicsEpoch) || 0;
    }
    if (domain === "follow") {
      return Number(meta.followEpoch) || 0;
    }
    if (domain === "quote") {
      return Number(meta.quoteEpoch) || 0;
    }
    return 0;
  }

  function domainsStale(savedEpochs, currentMeta, domains) {
    if (!savedEpochs || !currentMeta) {
      return true;
    }
    if (currentMeta.rebuilding) {
      return true;
    }
    for (const d of domains || []) {
      if (epochValue(savedEpochs, d) !== epochValue(currentMeta, d)) {
        return true;
      }
    }
    return false;
  }

  function openDb() {
    if (!global.indexedDB) {
      return Promise.resolve(null);
    }
    if (dbPromise) {
      return dbPromise;
    }
    dbPromise = new Promise((resolve, reject) => {
      const req = global.indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(STORE)) {
          db.createObjectStore(STORE, { keyPath: "cacheKey" });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    return dbPromise;
  }

  async function readEntry(cacheKey) {
    const db = await openDb();
    if (!db) {
      return null;
    }
    return new Promise((resolve) => {
      const tx = db.transaction(STORE, "readonly");
      const store = tx.objectStore(STORE);
      const req = store.get(String(cacheKey));
      req.onsuccess = () => resolve(req.result || null);
      req.onerror = () => resolve(null);
    });
  }

  async function writeEntry(entry) {
    const db = await openDb();
    if (!db || !entry?.cacheKey) {
      return;
    }
    return new Promise((resolve) => {
      const tx = db.transaction(STORE, "readwrite");
      tx.objectStore(STORE).put(entry);
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
    });
  }

  function readLocalPublicMeta(targetId) {
    const tid = String(targetId || "").trim();
    if (!tid) {
      return null;
    }
    try {
      const raw = global.localStorage.getItem(`${LS_PUBLIC_META_PREFIX}${tid}`);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  }

  function writeLocalPublicMeta(targetId, meta) {
    const tid = String(targetId || "").trim();
    if (!tid) {
      return;
    }
    try {
      if (!meta) {
        global.localStorage.removeItem(`${LS_PUBLIC_META_PREFIX}${tid}`);
        return;
      }
      global.localStorage.setItem(`${LS_PUBLIC_META_PREFIX}${tid}`, JSON.stringify(meta));
    } catch {
      // quota or private mode
    }
  }

  async function fetchPublicCacheMeta(apiFetch, baseUrl, targetId) {
    const tid = String(targetId || "").trim();
    if (!apiFetch || !baseUrl || !tid) {
      return readLocalPublicMeta(tid);
    }
    try {
      const res = await apiFetch(`${baseUrl}/public/${encodeURIComponent(tid)}/cache-meta`, {
        cache: "no-store",
        timeoutMs: 8000,
      });
      const j = await res.json().catch(() => ({}));
      if (!res.ok || !j?.ok || !j.data) {
        return readLocalPublicMeta(tid);
      }
      publicMetaByTarget.set(tid, j.data);
      writeLocalPublicMeta(tid, j.data);
      return j.data;
    } catch {
      return readLocalPublicMeta(tid);
    }
  }

  async function ensurePublicCacheMeta(apiFetch, baseUrl, targetId, opts = {}) {
    const tid = String(targetId || "").trim();
    if (!tid) {
      return null;
    }
    if (opts.force) {
      publicMetaPromises.delete(tid);
    }
    if (publicMetaByTarget.has(tid) && !opts.force) {
      return publicMetaByTarget.get(tid);
    }
    const pending = publicMetaPromises.get(tid);
    if (pending && !opts.force) {
      return pending;
    }
    const promise = fetchPublicCacheMeta(apiFetch, baseUrl, tid).then((meta) => {
      const resolved = meta || readLocalPublicMeta(tid);
      publicMetaByTarget.set(tid, resolved);
      return resolved;
    });
    publicMetaPromises.set(tid, promise);
    return promise;
  }

  function invalidatePublicCacheMeta(targetId) {
    const tid = String(targetId || "").trim();
    if (!tid) {
      publicMetaByTarget.clear();
      publicMetaPromises.clear();
      return;
    }
    publicMetaByTarget.delete(tid);
    publicMetaPromises.delete(tid);
  }

  async function clearAll() {
    writeLocalMeta(null);
    serverMetaCache = null;
    serverMetaPromise = null;
    publicMetaByTarget.clear();
    publicMetaPromises.clear();
    try {
      const keysToRemove = [];
      for (let i = 0; i < global.localStorage.length; i += 1) {
        const k = global.localStorage.key(i);
        if (k && k.startsWith(LS_PUBLIC_META_PREFIX)) {
          keysToRemove.push(k);
        }
      }
      for (const k of keysToRemove) {
        global.localStorage.removeItem(k);
      }
    } catch {
      // ignore
    }
    const db = await openDb();
    if (!db) {
      return;
    }
    return new Promise((resolve) => {
      const tx = db.transaction(STORE, "readwrite");
      tx.objectStore(STORE).clear();
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
    });
  }

  function splitHomeBundle(bundle) {
    if (!bundle || typeof bundle !== "object") {
      return { seriesFrozen: null, liveHead: null };
    }
    const stages = bundle.returns?.stages || {};
    const frozenStages = { ...stages };
    const today = frozenStages.today;
    delete frozenStages.today;
    return {
      seriesFrozen: {
        meta: {
          accountId: bundle.meta?.accountId,
          frozenThrough: bundle.meta?.frozenThrough,
          dataVersion: bundle.meta?.dataVersion,
          rebuilding: bundle.meta?.rebuilding,
          bookCurrency: bundle.meta?.bookCurrency,
          algoMode: bundle.meta?.algoMode,
          ruleVersion: bundle.meta?.ruleVersion,
        },
        returns: { stages: frozenStages },
      },
      liveHead: {
        meta: { ...bundle.meta },
        returns: { stages: today != null ? { today } : {} },
        assets: bundle.assets,
        holdings: bundle.holdings,
      },
    };
  }

  function mergeHomeBundle(seriesFrozen, liveHead) {
    if (!seriesFrozen && !liveHead) {
      return null;
    }
    const frozenStages = seriesFrozen?.returns?.stages || {};
    const liveStages = liveHead?.returns?.stages || {};
    return {
      meta: { ...(seriesFrozen?.meta || {}), ...(liveHead?.meta || {}) },
      returns: { stages: { ...frozenStages, ...liveStages } },
      assets: liveHead?.assets || null,
      holdings: liveHead?.holdings || { rows: [] },
    };
  }

  function liveDateFromBundle(bundle) {
    return String(bundle?.meta?.liveDate || "").slice(0, 10);
  }

  function stripTodayFromSeriesPoints(points, liveDate) {
    const list = Array.isArray(points) ? points : [];
    if (!liveDate) {
      return list;
    }
    return list.filter((p) => String(p?.date || "").slice(0, 10) !== liveDate);
  }

  function todayPointsFromSeries(points, liveDate) {
    const list = Array.isArray(points) ? points : [];
    if (!liveDate) {
      return [];
    }
    return list.filter((p) => String(p?.date || "").slice(0, 10) === liveDate);
  }

  function splitAnalysisBundle(bundle) {
    if (!bundle || typeof bundle !== "object") {
      return { seriesFrozen: null, liveHead: null };
    }
    const liveDate = liveDateFromBundle(bundle);
    const series = bundle.series || {};
    const frozenSeries = {};
    const liveSeries = {};
    for (const [key, pts] of Object.entries(series)) {
      frozenSeries[key] = stripTodayFromSeriesPoints(pts, liveDate);
      const todayPts = todayPointsFromSeries(pts, liveDate);
      if (todayPts.length) {
        liveSeries[key] = todayPts;
      }
    }
    const benchPts = bundle.benchmark?.points;
    const frozenBench = stripTodayFromSeriesPoints(benchPts, liveDate);
    const liveBench = todayPointsFromSeries(benchPts, liveDate);
    return {
      seriesFrozen: {
        meta: {
          accountId: bundle.meta?.accountId,
          frozenThrough: bundle.meta?.frozenThrough,
          dataVersion: bundle.meta?.dataVersion,
          rebuilding: bundle.meta?.rebuilding,
          stage: bundle.meta?.stage,
          from: bundle.meta?.from,
          to: bundle.meta?.to,
        },
        series: frozenSeries,
        benchmark: bundle.benchmark
          ? { ...bundle.benchmark, points: frozenBench }
          : bundle.benchmark,
      },
      liveHead: {
        meta: { ...bundle.meta },
        returns: bundle.returns,
        assets: bundle.assets,
        series: liveSeries,
        benchmark: liveBench.length ? { ...bundle.benchmark, points: liveBench } : null,
        stockRank: bundle.stockRank,
      },
    };
  }

  function mergeSeriesMaps(frozenMap, liveMap) {
    const out = { ...(frozenMap || {}) };
    for (const [key, livePts] of Object.entries(liveMap || {})) {
      const frozenPts = Array.isArray(out[key]) ? [...out[key]] : [];
      const merged = [...frozenPts];
      for (const p of livePts || []) {
        const d = String(p?.date || "").slice(0, 10);
        const idx = merged.findIndex((x) => String(x?.date || "").slice(0, 10) === d);
        if (idx >= 0) {
          merged[idx] = p;
        } else {
          merged.push(p);
        }
      }
      merged.sort((a, b) => String(a.date).localeCompare(String(b.date)));
      out[key] = merged;
    }
    return out;
  }

  function mergeAnalysisBundle(seriesFrozen, liveHead) {
    if (!seriesFrozen && !liveHead) {
      return null;
    }
    const frozenSeries = seriesFrozen?.series || {};
    const liveSeries = liveHead?.series || {};
    const mergedSeries = mergeSeriesMaps(frozenSeries, liveSeries);
    let benchmark = seriesFrozen?.benchmark || liveHead?.benchmark || null;
    if (benchmark && (seriesFrozen?.benchmark?.points || liveHead?.benchmark?.points)) {
      const frozenBench = seriesFrozen?.benchmark?.points || [];
      const liveBench = liveHead?.benchmark?.points || [];
      benchmark = {
        ...benchmark,
        points: mergeSeriesMaps({ b: frozenBench }, { b: liveBench }).b || [],
      };
    }
    return {
      meta: { ...(seriesFrozen?.meta || {}), ...(liveHead?.meta || {}) },
      returns: liveHead?.returns || seriesFrozen?.returns,
      assets: liveHead?.assets || seriesFrozen?.assets,
      series: mergedSeries,
      benchmark,
      stockRank: liveHead?.stockRank || seriesFrozen?.stockRank,
    };
  }

  function splitStockRecordBundle(bundle) {
    if (!bundle || typeof bundle !== "object") {
      return { seriesFrozen: null, liveHead: null };
    }
    const liveDate = liveDateFromBundle(bundle);
    const points = Array.isArray(bundle.charts?.points) ? bundle.charts.points : [];
    return {
      seriesFrozen: {
        meta: {
          symbol: bundle.meta?.symbol,
          frozenThrough: bundle.meta?.frozenThrough,
          dataVersion: bundle.meta?.dataVersion,
          stage: bundle.meta?.stage,
        },
        charts: {
          ...bundle.charts,
          points: stripTodayFromSeriesPoints(points, liveDate),
        },
      },
      liveHead: {
        meta: { ...bundle.meta },
        headline: bundle.headline,
        charts: {
          range: bundle.charts?.range,
          defaults: bundle.charts?.defaults,
          points: todayPointsFromSeries(points, liveDate),
        },
      },
    };
  }

  function mergeStockRecordBundle(seriesFrozen, liveHead) {
    if (!seriesFrozen && !liveHead) {
      return null;
    }
    const frozenPts = seriesFrozen?.charts?.points || [];
    const livePts = liveHead?.charts?.points || [];
    const mergedPts = mergeSeriesMaps({ p: frozenPts }, { p: livePts }).p || [];
    return {
      meta: { ...(seriesFrozen?.meta || {}), ...(liveHead?.meta || {}) },
      headline: liveHead?.headline || seriesFrozen?.headline,
      charts: {
        ...(seriesFrozen?.charts || {}),
        ...(liveHead?.charts || {}),
        points: mergedPts,
      },
    };
  }

  function isPartStale(entry, part, currentMeta) {
    if (!entry?.epochs) {
      return true;
    }
    if (domainsStale(entry.epochs, currentMeta, domainsForPart(part))) {
      return true;
    }
    if (part === "liveHead" && entry.savedAt) {
      const age = Date.now() - Number(entry.savedAt);
      if (age > LIVE_HEAD_TTL_MS) {
        return true;
      }
    }
    return false;
  }

  function isPageStale(entry, pageKind, currentMeta) {
    if (!entry) {
      return true;
    }
    const deps = PAGE_DOMAIN_DEPS[pageKind] || PAGE_DOMAIN_DEPS.home;
    if (domainsStale(entry.epochs, currentMeta, deps)) {
      return true;
    }
    return isPartStale(entry, "liveHead", currentMeta);
  }

  async function fetchServerCacheMeta(apiFetch, baseUrl) {
    if (!apiFetch || !baseUrl) {
      return null;
    }
    try {
      const res = await apiFetch(`${baseUrl}/cache-meta`, { cache: "no-store", timeoutMs: 8000 });
      const j = await res.json().catch(() => ({}));
      if (!res.ok || !j?.ok || !j.data) {
        return readLocalMeta();
      }
      serverMetaCache = j.data;
      writeLocalMeta(j.data);
      return j.data;
    } catch {
      return readLocalMeta();
    }
  }

  async function ensureCacheMeta(apiFetch, baseUrl, opts = {}) {
    if (opts.force) {
      serverMetaPromise = null;
    }
    if (serverMetaCache && !opts.force) {
      return serverMetaCache;
    }
    if (serverMetaPromise && !opts.force) {
      return serverMetaPromise;
    }
    serverMetaPromise = fetchServerCacheMeta(apiFetch, baseUrl).then((meta) => {
      serverMetaCache = meta || readLocalMeta();
      return serverMetaCache;
    });
    return serverMetaPromise;
  }

  function bumpLocalEpoch(domain, delta = 1) {
    const meta = { ...(serverMetaCache || readLocalMeta() || {}) };
    const key =
      domain === "ledger"
        ? "ledgerEpoch"
        : domain === "metrics"
          ? "metricsEpoch"
          : domain === "dynamics"
            ? "dynamicsEpoch"
            : domain === "follow"
              ? "followEpoch"
              : domain === "quote"
                ? "quoteEpoch"
                : null;
    if (!key) {
      return;
    }
    meta[key] = (Number(meta[key]) || 0) + delta;
    serverMetaCache = meta;
    writeLocalMeta(meta);
  }

  async function saveBundleParts(cacheKey, splitFn, mergeFn, bundle, epochs) {
    if (!cacheKey || !bundle) {
      return;
    }
    const parts = splitFn(bundle);
    await writeEntry({
      cacheKey: String(cacheKey),
      seriesFrozen: parts.seriesFrozen,
      liveHead: parts.liveHead,
      epochs: { ...epochs },
      savedAt: Date.now(),
    });
  }

  async function loadMergedBundle(cacheKey, mergeFn, pageKind, currentMeta) {
    const entry = await readEntry(cacheKey);
    if (!entry) {
      return { bundle: null, entry: null, needFetch: true, needLiveOnly: false };
    }
    const seriesStale = isPartStale(entry, "seriesFrozen", currentMeta);
    const liveStale = isPartStale(entry, "liveHead", currentMeta);
    if (seriesStale && liveStale) {
      return { bundle: null, entry, needFetch: true, needLiveOnly: false };
    }
    const bundle = mergeFn(entry.seriesFrozen, entry.liveHead);
    if (!bundle) {
      return { bundle: null, entry, needFetch: true, needLiveOnly: false };
    }
    if (currentMeta?.rebuilding) {
      return { bundle: null, entry, needFetch: true, needLiveOnly: false };
    }
    return {
      bundle,
      entry,
      needFetch: seriesStale || liveStale,
      needLiveOnly: !seriesStale && liveStale,
    };
  }

  global.PageCache = {
    PAGE_DOMAIN_DEPS,
    LIVE_HEAD_TTL_MS,
    readLocalMeta,
    writeLocalMeta,
    ensureCacheMeta,
    ensurePublicCacheMeta,
    readLocalPublicMeta,
    invalidatePublicCacheMeta,
    invalidateServerMeta: () => {
      serverMetaCache = null;
      serverMetaPromise = null;
    },
    bumpLocalEpoch,
    clearAll,
    readEntry,
    writeEntry,
    splitHomeBundle,
    mergeHomeBundle,
    splitAnalysisBundle,
    mergeAnalysisBundle,
    splitStockRecordBundle,
    mergeStockRecordBundle,
    saveBundleParts,
    loadMergedBundle,
    isPartStale,
    isPageStale,
    homeBundleCacheKey: (userId, accountId, stages) =>
      `home:${String(userId || "")}:${String(accountId || "all")}:${String(stages || "")}`,
    analysisBundleCacheKey: (userId, accountId, querySig) =>
      `analysis:${String(userId || "")}:${String(accountId || "all")}:${String(querySig || "")}`,
    stockRecordBundleCacheKey: (userId, symbol, accountId, range) =>
      `stock:${String(userId || "")}:${normalizeSymbolKey(symbol)}:${String(accountId || "all")}:${String(range || "30")}`,
    dynamicsListCacheKey: (userId, listKey) =>
      `dyn:${String(userId || "")}:${String(listKey || "")}`,
  };

  function normalizeSymbolKey(sym) {
    return String(sym || "")
      .trim()
      .toUpperCase();
  }
})(typeof window !== "undefined" ? window : globalThis);
