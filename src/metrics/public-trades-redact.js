/**
 * Public trades list rows: drop absolute quantity/amount; keep account + amountShareRatio.
 */

function redactPublicTradeRow(trade, accountNameById = {}) {
  const t = trade || {};
  const accountId = String(t.accountId || "default").trim() || "default";
  const accountName =
    String(t.accountName || accountNameById[accountId] || accountId).trim() || accountId;
  const ratioRaw = t.amountShareRatio;
  const amountShareRatio =
    ratioRaw == null || ratioRaw === "" ? null : Number(ratioRaw);
  return {
    id: t.id,
    accountId,
    accountName,
    type: t.type,
    symbol: t.symbol,
    name: t.name,
    side: t.side,
    price: t.price,
    date: t.date,
    note: t.note || "",
    amountShareRatio: Number.isFinite(amountShareRatio) ? amountShareRatio : null,
  };
}

module.exports = {
  redactPublicTradeRow,
};
