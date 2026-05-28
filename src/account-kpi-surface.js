/**
 * 账户级 KPI「展示态」：由定时任务写入的 account_home_summary 行生成，
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
  const pct = Math.abs(v) <= 1.00001 ? v * 100 : v;
  return `${pct.toFixed(2)}%`;
}

/** 收益率展示（总览 headline）：正值带 + 号，如 +0.65% */
function fmtSignedPercentRatio(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) {
    return "—";
  }
  const pct = Math.abs(v) <= 1.00001 ? v * 100 : v;
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

function rowNum(row, camel, snake, fallback = 0) {
  const v = row[camel] != null ? row[camel] : row[snake];
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
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

/**
 * @param {object} row account_home_summary 一行（pg snake_case 或已 camel 化）
 * @param {string} bookCurrency CNY | USD | HKD
 * @param {string} algoMode twr | mwr — 决定 stages 内展示用收益率口径
 */
function buildAccountKpiSurfacePayload(row, bookCurrency = "CNY", algoMode = "twr") {
  if (!row || typeof row !== "object") {
    return null;
  }
  const book = String(bookCurrency || "CNY").toUpperCase().slice(0, 3) || "CNY";
  const mwr = String(algoMode || "twr").toLowerCase() === "mwr";
  const frozen = String(row.frozen_through || row.frozenThrough || "").slice(0, 10) || "";
  const fxUsd = rowNum(row, "eodFxUsdCny", "eod_fx_usd_cny", 0);
  const fxHkd = rowNum(row, "eodFxHkdCny", "eod_fx_hkd_cny", 0);
  const taCny = rowNum(
    row,
    "eodTotalAssetsCny",
    "eod_total_assets_cny",
    rowNum(row, "lastMarketValueCny", "last_market_value_cny", 0),
  );
  const mvCny = rowNum(row, "eodMarketValueCny", "eod_market_value_cny", 0);
  const cashCny = rowNum(row, "eodCashCny", "eod_cash_cny", 0);
  const ratioDb = rowNum(row, "eodCashRatio", "eod_cash_ratio", 0);
  const principalCny = rowNum(row, "eodPrincipalCny", "eod_principal_cny", 0);
  const ta = cnyScalarToBookAmount(taCny, book, fxUsd, fxHkd);
  const mv = cnyScalarToBookAmount(mvCny, book, fxUsd, fxHkd);
  const cash = cnyScalarToBookAmount(cashCny, book, fxUsd, fxHkd);
  const principal = cnyScalarToBookAmount(principalCny, book, fxUsd, fxHkd);
  const ratioStr = Number.isFinite(taCny) && taCny > 0 && Number.isFinite(cashCny) ? fmtPercentRatio(cashCny / taCny) : fmtPercentRatio(ratioDb);

  const stage = (profitCamel, profitSnake, rtCamel, rtSnake, rmCamel, rmSnake) => {
    const p = rowNum(row, profitCamel, profitSnake, 0);
    const r = mwr ? rowNum(row, rmCamel, rmSnake, 0) : rowNum(row, rtCamel, rtSnake, 0);
    return {
      profitCny: p,
      rate: r,
      profitDisplay: fmtMoney(p, "CNY"),
      rateDisplay: fmtSignedPercentRatio(r),
    };
  };

  return {
    frozenThrough: frozen,
    bookCurrency: book,
    algoMode: mwr ? "mwr" : "twr",
    display: {
      totalAssets: fmtMoney(ta, book),
      marketValue: fmtMoney(mv, book),
      cash: fmtMoney(cash, book),
      cashRatio: ratioStr,
      principal: fmtMoney(principal, book),
    },
    stages: {
      month: stage("monthProfitCny", "month_profit_cny", "monthRateTwr", "month_rate_twr", "monthRateMwr", "month_rate_mwr"),
      ytd: stage("ytdProfitCny", "ytd_profit_cny", "ytdRateTwr", "ytd_rate_twr", "ytdRateMwr", "ytd_rate_mwr"),
      total: stage("totalProfitCny", "total_profit_cny", "totalRateTwr", "total_rate_twr", "totalRateMwr", "total_rate_mwr"),
    },
    summaryLine: frozen
      ? `截至 ${frozen} 日终（${book} 口径展示）：总资产 ${fmtMoney(ta, book)} · 市值 ${fmtMoney(
          mv,
          book,
        )} · 现金 ${fmtMoney(cash, book)} · 现金占比 ${ratioStr} · 本金 ${fmtMoney(principal, book)}`
      : "",
  };
}

module.exports = {
  buildAccountKpiSurfacePayload,
  cnyScalarToBookAmount,
  fmtMoney,
  fmtPlainAmount,
  fmtPlainSignedAmount,
  fmtPercentRatio,
  fmtSignedPercentRatio,
};
