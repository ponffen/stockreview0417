/**
 * 账户级 KPI「展示态」：由 analysis_daily_snapshot 冻结日行映射生成，
 * 供首页 / 分析 tab 共用（仅格式化与字段选择，不含业务重算）。
 */

function fmtMoney(n, bookCurrency) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  const ccy = String(bookCurrency || "CNY").toUpperCase();
  const abs = Math.abs(v).toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const sign = v < 0 ? "-" : "";
  if (ccy === "CNY") {
    return `${sign}¥${abs}`;
  }
  if (ccy === "USD") {
    return `${sign}US$${abs}`;
  }
  if (ccy === "HKD") {
    return `${sign}HK$${abs}`;
  }
  return `${sign}${abs} ${ccy}`;
}

function fmtPercentRatio(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return `${(v * 100).toFixed(2)}%`;
}

/** 收益率展示（比率 r → 百分数字符串）：正值带 + 号；累计 TWR>1 时亦统一 ×100（如 2.30 → +230.00%）。 */
function fmtSignedPercentRatio(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  const pct = v * 100;
  const sign = pct > 0 ? "+" : "";
  return `${sign}${pct.toFixed(2)}%`;
}

function fmtPlainAmount(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  return v.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtPlainSignedAmount(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  const sign = v > 0 ? "+" : v < 0 ? "-" : "";
  const abs = Math.abs(v).toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return `${sign}${abs}`;
}

/** 人民币口径盈亏 → 记账币数值，再格式化为绿框同款无货币符号带正负号字符串 */
function fmtPlainSignedAmountInBook(profitCny, bookCurrency, fxUsdCny, fxHkdCny) {
  return fmtPlainSignedAmount(cnyScalarToBookAmount(profitCny, bookCurrency, fxUsdCny, fxHkdCny));
}

/** 将人民币口径的标量按冻结日快照中的 USD/CNY、HKD/CNY 汇率换到展示账本（与「全部=CNY」单账户=记账币一致）。 */
function cnyScalarToBookAmount(cny, bookCurrency, fxUsdCny, fxHkdCny) {
  const x = Number(cny);
  const c = String(bookCurrency || "CNY").toUpperCase().slice(0, 3) || "CNY";
  if (!Number.isFinite(x)) {
    return 0;
  }
  if (c === "CNY") {
    return x;
  }
  const u = Number(fxUsdCny);
  const h = Number(fxHkdCny);
  if (c === "USD" && Number.isFinite(u) && u > 0) {
    return x / u;
  }
  if (c === "HKD" && Number.isFinite(h) && h > 0) {
    return x / h;
  }
  return x;
}

module.exports = {
  cnyScalarToBookAmount,
  fmtMoney,
  fmtPlainAmount,
  fmtPlainSignedAmount,
  fmtPlainSignedAmountInBook,
  fmtPercentRatio,
  fmtSignedPercentRatio,
};
