/**
 * Vercel Blob helpers for dynamics images.
 */

const { put, del, get } = require("@vercel/blob");
const { randomUUID } = require("crypto");

const MAX_IMAGE_BYTES = 5 * 1024 * 1024;
const ALLOWED_MIME = new Set(["image/jpeg", "image/png", "image/webp"]);
const EXT_BY_MIME = {
  "image/jpeg": "jpg",
  "image/png": "png",
  "image/webp": "webp",
};

function getBlobToken() {
  return String(process.env.BLOB_READ_WRITE_TOKEN || "").trim();
}

function getBlobStoreAccess() {
  const raw = String(process.env.BLOB_STORE_ACCESS || "private").trim().toLowerCase();
  return raw === "public" ? "public" : "private";
}

function isPrivateBlobStore() {
  return getBlobStoreAccess() === "private";
}

function getStoreIdFromToken() {
  const token = getBlobToken();
  if (!token) {
    return "";
  }
  const parts = token.split("_");
  return parts[3] || "";
}

function assertBlobConfigured() {
  if (!getBlobToken()) {
    const err = new Error("BLOB_READ_WRITE_TOKEN is not configured");
    err.code = "BLOB_NOT_CONFIGURED";
    throw err;
  }
}

function parseImageUrlsField(raw) {
  if (raw == null || raw === "") {
    return [];
  }
  if (Array.isArray(raw)) {
    return raw.map((u) => String(u || "").trim()).filter(Boolean).slice(0, 9);
  }
  try {
    const parsed = JSON.parse(String(raw));
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.map((u) => String(u || "").trim()).filter(Boolean).slice(0, 9);
  } catch {
    return [];
  }
}

function serializeImageUrls(urls) {
  const list = normalizeStoredImageUrls(urls);
  return JSON.stringify(list);
}

function isBlobUrl(url) {
  const u = String(url || "").trim();
  return u.includes("blob.vercel-storage.com");
}

function pathnameToBlobUrl(pathname) {
  const path = String(pathname || "").trim().replace(/^\/+/, "");
  if (!path) {
    return "";
  }
  const storeId = getStoreIdFromToken();
  if (!storeId) {
    return "";
  }
  return `https://${storeId}.${getBlobStoreAccess()}.blob.vercel-storage.com/${path}`;
}

function extractPathnameFromBlobUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    return u.pathname.replace(/^\/+/, "");
  } catch {
    return "";
  }
}

function parseClientImageViewUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) {
    return null;
  }
  try {
    const parsed = raw.startsWith("/") ? new URL(raw, "http://local") : new URL(raw);
    const path = parsed.pathname.replace(/\/+$/, "");
    if (!path.endsWith("/api/dynamics/images/view")) {
      return null;
    }
    const blobPath = parsed.searchParams.get("path");
    if (blobPath) {
      return { pathname: blobPath.replace(/^\/+/, "") };
    }
    const encoded = parsed.searchParams.get("u");
    if (encoded) {
      const blobUrl = Buffer.from(encoded, "base64url").toString("utf8");
      return { blobUrl };
    }
  } catch {
    return null;
  }
  return null;
}

function normalizeStoredImageUrl(url) {
  const u = String(url || "").trim();
  if (!u) {
    return "";
  }
  if (isBlobUrl(u)) {
    return u;
  }
  const view = parseClientImageViewUrl(u);
  if (!view) {
    return u;
  }
  if (view.blobUrl && isBlobUrl(view.blobUrl)) {
    return view.blobUrl;
  }
  if (view.pathname) {
    return pathnameToBlobUrl(view.pathname);
  }
  return u;
}

function normalizeStoredImageUrls(urls) {
  return parseImageUrlsField(urls).map(normalizeStoredImageUrl).filter(Boolean).slice(0, 9);
}

function toClientImageUrl(storedUrl) {
  const url = normalizeStoredImageUrl(storedUrl);
  if (!url) {
    return "";
  }
  if (!isPrivateBlobStore() || !isBlobUrl(url)) {
    return url;
  }
  const pathname = extractPathnameFromBlobUrl(url);
  if (!pathname) {
    return url;
  }
  return `/api/dynamics/images/view?path=${encodeURIComponent(pathname)}`;
}

function toClientImageUrls(urls) {
  return parseImageUrlsField(urls).map(toClientImageUrl).filter(Boolean);
}

async function uploadDynamicsImage(userId, buffer, contentType) {
  assertBlobConfigured();
  const mime = String(contentType || "").split(";")[0].trim().toLowerCase();
  if (!ALLOWED_MIME.has(mime)) {
    throw new Error("仅支持 jpg / png / webp 图片");
  }
  const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
  if (!buf.length || buf.length > MAX_IMAGE_BYTES) {
    throw new Error("图片大小不能超过 5MB");
  }
  const uid = String(userId || "").trim() || "anon";
  const ext = EXT_BY_MIME[mime] || "jpg";
  const pathname = `dynamics/${uid}/${randomUUID()}.${ext}`;
  const access = getBlobStoreAccess();
  const blob = await put(pathname, buf, {
    access,
    token: getBlobToken(),
    contentType: mime,
    addRandomSuffix: false,
  });
  return {
    url: toClientImageUrl(blob.url),
    storageUrl: blob.url,
  };
}

async function readBlobStreamToBuffer(stream) {
  if (!stream) {
    return null;
  }
  if (typeof stream.getReader === "function") {
    const reader = stream.getReader();
    const chunks = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (value) {
        chunks.push(Buffer.from(value));
      }
    }
    return Buffer.concat(chunks);
  }
  const { Readable } = require("stream");
  const nodeStream = Readable.fromWeb(stream);
  const chunks = [];
  for await (const chunk of nodeStream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

async function streamDynamicsImage(req, res) {
  assertBlobConfigured();
  const query = req.query || {};
  const pathParam = query.path != null ? String(query.path).trim().replace(/^\/+/, "") : "";
  const encoded = query.u != null ? String(query.u).trim() : "";
  const sendJson = (status, payload) => {
    if (typeof res.status === "function") {
      res.status(status).json(payload);
      return;
    }
    res.statusCode = status;
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.end(JSON.stringify(payload));
  };
  let getArg = "";
  if (pathParam) {
    if (!pathParam.startsWith("dynamics/")) {
      sendJson(403, { ok: false, error: "forbidden" });
      return;
    }
    getArg = pathParam;
  } else if (encoded) {
    try {
      getArg = Buffer.from(encoded, "base64url").toString("utf8");
    } catch {
      getArg = "";
    }
  }
  if (!getArg) {
    sendJson(400, { ok: false, error: "invalid image path" });
    return;
  }
  let result;
  try {
    result = await get(getArg, {
      access: getBlobStoreAccess(),
      token: getBlobToken(),
    });
  } catch (error) {
    sendJson(500, { ok: false, error: error?.message || "image fetch failed" });
    return;
  }
  if (!result || !result.stream) {
    sendJson(404, { ok: false, error: "not found" });
    return;
  }
  const contentType = result.blob?.contentType || "application/octet-stream";
  let body;
  try {
    body = await readBlobStreamToBuffer(result.stream);
  } catch (error) {
    sendJson(500, { ok: false, error: error?.message || "image stream failed" });
    return;
  }
  if (!body || !body.length) {
    sendJson(404, { ok: false, error: "not found" });
    return;
  }
  if (typeof res.status === "function") {
    res.set("Cache-Control", "public, max-age=31536000, immutable");
    if (result.blob?.etag) {
      res.set("ETag", result.blob.etag);
    }
    res.type(contentType).send(body);
    return;
  }
  res.statusCode = 200;
  res.setHeader("Content-Type", contentType);
  res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
  if (result.blob?.etag) {
    res.setHeader("ETag", result.blob.etag);
  }
  res.end(body);
}

async function deleteBlobUrls(urls) {
  const token = getBlobToken();
  if (!token) {
    return;
  }
  const list = normalizeStoredImageUrls(urls);
  await Promise.all(
    list.map(async (url) => {
      if (!isBlobUrl(url)) {
        return;
      }
      try {
        await del(url, { token });
      } catch {
        // ignore missing blobs
      }
    }),
  );
}

function diffRemovedImageUrls(before, after) {
  const prev = new Set(normalizeStoredImageUrls(before));
  const next = new Set(normalizeStoredImageUrls(after));
  return [...prev].filter((u) => !next.has(u));
}

module.exports = {
  MAX_IMAGE_BYTES,
  ALLOWED_MIME,
  parseImageUrlsField,
  serializeImageUrls,
  normalizeStoredImageUrl,
  normalizeStoredImageUrls,
  toClientImageUrl,
  toClientImageUrls,
  uploadDynamicsImage,
  streamDynamicsImage,
  deleteBlobUrls,
  diffRemovedImageUrls,
  isBlobConfigured: () => Boolean(getBlobToken()),
  getBlobStoreAccess,
};
