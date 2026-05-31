/**
 * 单账户 scope：analysis_daily_snapshot / home 映射字段已是记账币（历史 *_cny 命名）。
 * scope=all：金额为人民币汇总。展示层勿对单账户再做 CNY→记账币换汇。
 */
const {
  cnyScalarToBookAmount,
  fmtPlainAmount,
  fmtPlainSignedAmount,
  fmtPlainSignedAmountInBook,
  fmtMoney,
} = require("../account-kpi-surface");

function isAggregateScope(scope) {
  return String(scope || "all").trim() === "all";
}

/** 盘中 live 由 market-realtime-pnl 以人民币汇总；单账户非 CNY 账本需一次 CNY→记账币。 */
function liveCnyToBookAmount(cny, bookCurrency, fxUsdCny, fxHkdCny) {
  const book = String(bookCurrency || "CNY").toUpperCase().slice(0, 3) || "CNY";
  if (book === "CNY") {
    return Number(cny) || 0;
  }
  return cnyScalarToBookAmount(cny, book, fxUsdCny, fxHkdCny);
}

function formatSignedProfitForScope(profitScalar, scope, bookCurrency, fxUsdCny, fxHkdCny) {
  if (isAggregateScope(scope)) {
    return fmtPlainSignedAmountInBook(profitScalar, bookCurrency, fxUsdCny, fxHkdCny);
  }
  return fmtPlainSignedAmount(profitScalar);
}

function formatPlainAssetForScope(amount, scope, bookCurrency, fxUsdCny, fxHkdCny) {
  const n = Number(amount);
  if (!Number.isFinite(n)) {
    return "—";
  }
  if (isAggregateScope(scope)) {
    return fmtPlainAmount(cnyScalarToBookAmount(n, bookCurrency, fxUsdCny, fxHkdCny));
  }
  return fmtPlainAmount(n);
}

function formatMoneyAssetForScope(amount, scope, bookCurrency, fxUsdCny, fxHkdCny) {
  const n = Number(amount);
  if (!Number.isFinite(n)) {
    return "—";
  }
  if (isAggregateScope(scope)) {
    return fmtMoney(cnyScalarToBookAmount(n, bookCurrency, fxUsdCny, fxHkdCny), bookCurrency);
  }
  return fmtMoney(n, bookCurrency);
}

function frozenHomeScalars(homeAcc) {
  const acc = homeAcc || {};
  const eodMv = acc.eod_market_value_cny ?? acc.eodMarketValueCny;
  const marketValue =
    eodMv != null && Number.isFinite(Number(eodMv)) ? Number(eodMv) : 0;
  return {
    totalAssets: Number(acc.eod_total_assets_cny) || 0,
    marketValue,
    cash: Number(acc.eod_cash_cny) || 0,
    principal: Number(acc.eod_principal_cny) || 0,
    cashRatioPct: Number(acc.eod_cash_ratio) || 0,
  };
}

function liveScalarOrFrozen(liveValue, frozenValue, toBook) {
  const raw = Number(liveValue);
  if (!Number.isFinite(raw)) {
    return frozenValue;
  }
  return toBook ? toBook(raw) : raw;
}

/**
 * 账户维总资产/市值/现金/本金（展示用记账币数值）。
 * 冻结：单账户=账本币；all=CNY。交易日 live：all 与 CNY 账本直接用 CNY live，其余一次 CNY→账本。
 */
function resolveAccountAssetScalars(ctx) {
  const { scope, live, home } = ctx;
  const frozen = frozenHomeScalars(home?.account);
  const book = String(ctx.bookCurrency || "CNY").toUpperCase().slice(0, 3) || "CNY";
  const fxU = Number(ctx.fxUsdCny) || 0;
  const fxH = Number(ctx.fxHkdCny) || 0;

  if (!live?.tradingDay) {
    return frozen;
  }

  const toBook = isAggregateScope(scope) ? null : (cny) => liveCnyToBookAmount(cny, book, fxU, fxH);
  const ta = liveScalarOrFrozen(live.totalAssetsCny, frozen.totalAssets, toBook) || frozen.totalAssets;
  const mv = liveScalarOrFrozen(live.liveMarketValueCny, frozen.marketValue, toBook);
  const cash = liveScalarOrFrozen(live.cashCny, frozen.cash, toBook) || frozen.cash;
  const principal = liveScalarOrFrozen(live.principalCny, frozen.principal, toBook) || frozen.principal;
  if (isAggregateScope(scope)) {
    return {
      totalAssets: ta,
      marketValue: mv,
      cash,
      principal,
      cashRatioPct: ta > 0 ? (cash / ta) * 100 : frozen.cashRatioPct,
    };
  }

  return {
    totalAssets: ta,
    marketValue: mv,
    cash,
    principal,
    cashRatioPct: ta > 0 ? (cash / ta) * 100 : frozen.cashRatioPct,
  };
}

/** 盘中今日盈亏、银证净额：live 为 CNY 时换到记账币；冻结阶段收益已是记账币。 */
function liveProfitScalarToBook(profitCny, scope, bookCurrency, fxUsdCny, fxHkdCny) {
  if (isAggregateScope(scope)) {
    return Number(profitCny) || 0;
  }
  return liveCnyToBookAmount(profitCny, bookCurrency, fxUsdCny, fxHkdCny);
}

function enrichCtxFx(ctx) {
  const { fxU, fxH, book } = ctx.fxFromCtx ? ctx.fxFromCtx() : { fxU: 0, fxH: 0, book: "CNY" };
  return {
    ...ctx,
    bookCurrency: book,
    fxUsdCny: fxU,
    fxHkdCny: fxH,
  };
}

module.exports = {
  isAggregateScope,
  liveCnyToBookAmount,
  formatSignedProfitForScope,
  formatPlainAssetForScope,
  formatMoneyAssetForScope,
  frozenHomeScalars,
  resolveAccountAssetScalars,
  liveProfitScalarToBook,
  enrichCtxFx,
};
