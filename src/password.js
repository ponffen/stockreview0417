const crypto = require("node:crypto");

const SCRYPT_PARAMS = { N: 16384, r: 8, p: 1, maxmem: 64 * 1024 * 1024 };

function hashPassword(plain) {
  const salt = crypto.randomBytes(16);
  const hash = crypto.scryptSync(String(plain), salt, 64, SCRYPT_PARAMS);
  return `scrypt$${salt.toString("base64")}$${hash.toString("base64")}`;
}

function verifyPassword(plain, stored) {
  if (!stored || typeof stored !== "string") {
    return false;
  }
  const m = /^scrypt\$([^$]+)\$(.+)$/.exec(stored);
  if (!m) {
    return false;
  }
  let salt;
  let expected;
  try {
    salt = Buffer.from(m[1], "base64");
    expected = Buffer.from(m[2], "base64");
  } catch {
    return false;
  }
  if (expected.length !== 64) {
    return false;
  }
  const hash = crypto.scryptSync(String(plain), salt, 64, SCRYPT_PARAMS);
  if (hash.length !== expected.length) {
    return false;
  }
  return crypto.timingSafeEqual(hash, expected);
}

/** 大陆手机号 11 位，1 开头 */
function isValidPhone(phone) {
  return /^1\d{10}$/.test(String(phone || "").trim());
}

/** 不少于 6 位数字 */
function isValidPasswordDigits(plain) {
  return /^\d{6,}$/.test(String(plain || ""));
}

module.exports = {
  hashPassword,
  verifyPassword,
  isValidPhone,
  isValidPasswordDigits,
};
