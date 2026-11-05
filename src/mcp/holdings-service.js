/**
 * MCP get_holdings：公开行字段与 Web 公开 bundle 一致；补充仓位带列说明。
 */

const {
  HOLDING_ROW_PUBLIC_KEYS,
  redactHoldingRow,
} = require("../metrics/public-home-bundle-redact");

const POSITION_BAND_HOLDING_KEYS = [
  "positionMin",
  "positionMinChange",
  "positionMax",
  "positionMaxChange",
  "positionPercentile",
];

const MCP_HOLDINGS_POSITION_BAND_HINTS = {
  positionMin: "最低仓位期望（占当前总资产 %）；最新仓位带帖",
  positionMinChange: "当前仓位相对最低期望的偏离（%）",
  positionMax: "最高仓位期望（%）",
  positionMaxChange: "最高期望相对当前仓位的偏离（%）",
  positionPercentile: "仓位分位：(现仓-低)/(高-低)，可小于 0% 或大于 100%",
  scopeNote: "仅 account_id=all（总账户）时有值；子账户三列均为 —",
};

function ensurePositionBandKeys(row) {
  const out = row && typeof row === "object" ? { ...row } : {};
  for (const key of POSITION_BAND_HOLDING_KEYS) {
    if (!(key in out)) {
      out[key] = "";
    }
  }
  return out;
}

function formatHoldingsForMcp(holdings, mode) {
  const rows = Array.isArray(holdings?.rows) ? holdings.rows : [];
  const isPublic = mode === "public";
  const mapped = isPublic
    ? rows.map((row) => redactHoldingRow(row))
    : rows.map((row) => ensurePositionBandKeys(row));

  return {
    rows: mapped,
    publicFieldKeys: isPublic ? [...HOLDING_ROW_PUBLIC_KEYS] : null,
    positionBandFields: POSITION_BAND_HOLDING_KEYS,
    positionBandHints: MCP_HOLDINGS_POSITION_BAND_HINTS,
  };
}

module.exports = {
  formatHoldingsForMcp,
  POSITION_BAND_HOLDING_KEYS,
  MCP_HOLDINGS_POSITION_BAND_HINTS,
};
