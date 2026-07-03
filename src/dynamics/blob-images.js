/**
 * Vercel Blob helpers for dynamics images.
 */

const { put, del } = require("@vercel/blob");
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
  const list = parseImageUrlsField(urls);
  return JSON.stringify(list);
}

function isBlobUrl(url) {
  const u = String(url || "").trim();
  return u.includes("blob.vercel-storage.com") || u.includes(".public.blob.vercel-storage.com");
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
  const blob = await put(pathname, buf, {
    access: "public",
    token: getBlobToken(),
    contentType: mime,
    addRandomSuffix: false,
  });
  return blob.url;
}

async function deleteBlobUrls(urls) {
  const token = getBlobToken();
  if (!token) {
    return;
  }
  const list = parseImageUrlsField(urls);
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
  const prev = new Set(parseImageUrlsField(before));
  const next = new Set(parseImageUrlsField(after));
  return [...prev].filter((u) => !next.has(u));
}

module.exports = {
  MAX_IMAGE_BYTES,
  ALLOWED_MIME,
  parseImageUrlsField,
  serializeImageUrls,
  uploadDynamicsImage,
  deleteBlobUrls,
  diffRemovedImageUrls,
  isBlobConfigured: () => Boolean(getBlobToken()),
};
