/**
 * 与 home-bundle 对齐的盘中 live 指标：必须带冻结 pack（homeAccount）才能正确并入现金。
 */
const { fetchHomeBundleFrozenPack } = require("../db");
const { getComputeLiveMetrics } = require("../market-realtime-pnl");

function accountsFromHomeBundlePack(pack) {
  if (Array.isArray(pack?.accounts) && pack.accounts.length > 0) {
    return pack.accounts;
  }
  if (Array.isArray(pack?.settings?.accounts) && pack.settings.accounts.length > 0) {
    return pack.settings.accounts;
  }
  return [];
}

function isScopeMetricsCleared(scope, um, accountMetaList) {
  if (um?.isCleared === true) {
    return true;
  }
  const sc = String(scope || "all").trim() || "all";
  if (sc === "all") {
    return false;
  }
  const meta = (accountMetaList || []).find((m) => String(m.accountId) === sc);
  return meta?.isCleared === true;
}

async function getLiveMetricsWithFrozenPack(userId, accountScope = "all", opts = {}) {
  const uid = String(userId || "").trim();
  const scope = String(accountScope || "all").trim() || "all";
  if (!uid) {
    throw new Error("missing userId");
  }
  if (opts.live) {
    return opts.live;
  }

  const pack = opts.pack ?? (await fetchHomeBundleFrozenPack(uid, scope));
  if (!pack) {
    return getComputeLiveMetrics(uid, scope, opts);
  }

  const {
    home,
    um,
    accountMetaList,
    lastEodRows,
    frozenSymbolEodRows,
    trades,
    cashTransfers,
  } = pack;
  const accounts = accountsFromHomeBundlePack(pack);
  const scopeCleared = isScopeMetricsCleared(scope, um, accountMetaList);

  return getComputeLiveMetrics(uid, scope, {
    ...opts,
    preloaded: {
      trades,
      cashTransfers,
      accounts,
      homeAccount: home.account,
      scopeCleared,
      lastEodRows,
      frozenSymbolEodRows: frozenSymbolEodRows || [],
      ...(opts.preloaded || {}),
    },
  });
}

module.exports = {
  getLiveMetricsWithFrozenPack,
};
