/**
 * 成交金额占全账户总资产比例（单一计算入口，调用方只认输出 ratio）。
 */

const { normalizeSymbol } = require("./db-pure");

const RATIO_DECIMALS = 6;

function inferMarket(symbol) {
  const s = String(symbol || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
  if (!s) {
    return "其他";
  }
  if (s.startsWith("sh") || s.startsWith("sz")) {
    return "A股";
  }
  if (s.startsWith("hk") || s.startsWith("rt_hk")) {
    return "港股";
  }
  if (s.startsWith("gb_")) {
    return "美股";
  }
  if (/^\d{5}$/.test(s)) {
    return "港股";
  }
  if (/^\d{6}$/.test(s)) {
    return "A股";
  }
  if (/^[a-z][a-z0-9.\-]{0,14}$/.test(s)) {
    return "美股";
  }
  return "其他";
}

function tradeAmountCny({ amount, symbol, fxUsdCny, fxHkdCny }) {
  const m = inferMarket(normalizeSymbol(symbol));
  const amt = Math.abs(Number(amount) || 0);
  const fxUsd = Number(fxUsdCny) > 0 ? Number(fxUsdCny) : 0;
  const fxHkd = Number(fxHkdCny) > 0 ? Number(fxHkdCny) : 0;
  if (m === "A股" || m === "其他") {
    return amt;
  }
  if (m === "美股") {
    return amt * fxUsd;
  }
  if (m === "港股") {
    return amt * fxHkd;
  }
  return amt;
}

/**
 * @param {{ amount: number, symbol: string, totalAssetsCny: number, fxUsdCny?: number|null, fxHkdCny?: number|null }} input
 * @returns {number|null} 比例小数（如 0.0146），保留 6 位；无法计算时 null
 */
function computeTradeAmountShareRatio(input = {}) {
  const totalAssetsCny = Number(input.totalAssetsCny);
  if (!Number.isFinite(totalAssetsCny) || totalAssetsCny <= 0) {
    return null;
  }
  const amountCny = tradeAmountCny({
    amount: input.amount,
    symbol: input.symbol,
    fxUsdCny: input.fxUsdCny,
    fxHkdCny: input.fxHkdCny,
  });
  if (!Number.isFinite(amountCny) || amountCny <= 0) {
    return null;
  }
  const raw = amountCny / totalAssetsCny;
  if (!Number.isFinite(raw)) {
    return null;
  }
  const factor = 10 ** RATIO_DECIMALS;
  return Math.round(raw * factor) / factor;
}

module.exports = {
  RATIO_DECIMALS,
  inferMarket,
  tradeAmountCny,
  computeTradeAmountShareRatio,
};
