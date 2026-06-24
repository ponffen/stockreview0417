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

/** 盘中 live 与冻结快照同口径：全部账户=人民币，单账户=记账币（历史字段名仍带 Cny 后缀）。 */
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
 * 冻结与盘中 live 均为 scope 计价币，不再做 CNY→记账币换汇。
 */
function resolveAccountAssetScalars(ctx) {
  const { scope, live, home } = ctx;
  const frozen = frozenHomeScalars(home?.account);

  if (!live?.tradingDay) {
    return frozen;
  }

  const ta = liveScalarOrFrozen(live.totalAssetsCny, frozen.totalAssets, null) || frozen.totalAssets;
  const mv = liveScalarOrFrozen(live.liveMarketValueCny, frozen.marketValue, null);
  const cash = liveScalarOrFrozen(live.cashCny, frozen.cash, null) || frozen.cash;
  const principal = liveScalarOrFrozen(live.principalCny, frozen.principal, null) || frozen.principal;
  return {
    totalAssets: ta,
    marketValue: mv,
    cash,
    principal,
    cashRatioPct: ta > 0 ? (cash / ta) * 100 : frozen.cashRatioPct,
  };
}

/** 盘中今日盈亏、银证净额：与 scope 计价币一致，直接取用。 */
function liveProfitScalarToBook(profitScalar, scope, bookCurrency, fxUsdCny, fxHkdCny) {
  return Number(profitScalar) || 0;
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
