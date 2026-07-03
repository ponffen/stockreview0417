/**
 * Shared HTTP handlers for dynamics APIs (Express + Vercel).
 */

const { listDynamicsFeed, SCENES } = require("./dynamics-feed");
const {
  createCommunityPost,
  updateCommunityPost,
  deleteCommunityPost,
} = require("./community-posts-db");
const { uploadDynamicsImage, isBlobConfigured } = require("./blob-images");
const Busboy = require("busboy");

function parseLimit(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    return 10;
  }
  return Math.min(30, Math.max(1, Math.floor(n)));
}

async function handleListDynamicsFeed(req, userId, scene, options = {}) {
  const limit = parseLimit(req.query?.limit);
  const cursor = req.query?.cursor != null ? String(req.query.cursor) : "";
  return listDynamicsFeed({
    viewerId: userId,
    targetUserId: options.targetUserId || userId,
    scene,
    symbol: options.symbol || "",
    limit,
    cursor,
  });
}

async function handleCommunityFeed(req, userId) {
  return handleListDynamicsFeed(req, userId, SCENES.COMMUNITY);
}

async function handleSelfDynamics(req, userId) {
  return handleListDynamicsFeed(req, userId, SCENES.SELF);
}

async function handlePublicDynamics(req, userId, targetId) {
  return handleListDynamicsFeed(req, userId, SCENES.PUBLIC, { targetUserId: targetId });
}

async function handleSelfStockDynamics(req, userId, symbol) {
  return handleListDynamicsFeed(req, userId, SCENES.STOCK_SELF, { symbol });
}

async function handlePublicStockDynamics(req, userId, targetId, symbol) {
  return handleListDynamicsFeed(req, userId, SCENES.STOCK_PUBLIC, {
    targetUserId: targetId,
    symbol,
  });
}

function parseDynamicsImageUpload(req) {
  return new Promise((resolve, reject) => {
    const busboy = Busboy({
      headers: req.headers,
      limits: { files: 1, fileSize: 5 * 1024 * 1024 },
    });
    let fileBuffer = null;
    let fileMime = "";
    let fileSeen = false;
    busboy.on("file", (_name, stream, info) => {
      fileSeen = true;
      fileMime = info.mimeType || "";
      const chunks = [];
      stream.on("data", (chunk) => chunks.push(chunk));
      stream.on("limit", () => reject(new Error("图片大小不能超过 5MB")));
      stream.on("end", () => {
        fileBuffer = Buffer.concat(chunks);
      });
    });
    busboy.on("error", reject);
    busboy.on("finish", () => {
      if (!fileSeen || !fileBuffer || !fileBuffer.length) {
        reject(new Error("请选择图片文件"));
        return;
      }
      resolve({ buffer: fileBuffer, mimeType: fileMime });
    });
    if (req.pipe) {
      req.pipe(busboy);
      return;
    }
    if (req.on) {
      req.on("error", reject);
      req.pipe(busboy);
      return;
    }
    reject(new Error("invalid request stream"));
  });
}

async function handleUploadDynamicsImage(req, userId) {
  if (!isBlobConfigured()) {
    const err = new Error("图片服务未配置");
    err.code = "BLOB_NOT_CONFIGURED";
    throw err;
  }
  const { buffer, mimeType } = await parseDynamicsImageUpload(req);
  const url = await uploadDynamicsImage(userId, buffer, mimeType);
  return { url };
}

module.exports = {
  SCENES,
  handleCommunityFeed,
  handleSelfDynamics,
  handlePublicDynamics,
  handleSelfStockDynamics,
  handlePublicStockDynamics,
  handleUploadDynamicsImage,
  createCommunityPost,
  updateCommunityPost,
  deleteCommunityPost,
};
