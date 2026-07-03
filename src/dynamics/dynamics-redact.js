/**
 * Dynamics card field visibility by scene.
 */

const SCENES = {
  COMMUNITY: "community",
  SELF: "self",
  PUBLIC: "public",
  STOCK_SELF: "stock_self",
  STOCK_PUBLIC: "stock_public",
};

function applyDynamicsRedaction(card, scene, { isSelf = false } = {}) {
  const out = { ...card };
  const s = String(scene || "").trim() || SCENES.COMMUNITY;
  const showHeader = s === SCENES.COMMUNITY || s === SCENES.SELF || s === SCENES.PUBLIC;
  const isPrivateTrade = s === SCENES.SELF || s === SCENES.STOCK_SELF;
  const isPublicTrade = !isPrivateTrade;

  out.showHeader = showHeader;
  if (!showHeader) {
    delete out.displayName;
    delete out.userId;
  }

  if (out.cardKind === "trade") {
    if (isPublicTrade) {
      delete out.quantity;
      delete out.amount;
      delete out.accountName;
      delete out.accountId;
    } else {
      delete out.amountShareRatio;
    }
  }

  if (s === SCENES.STOCK_SELF || s === SCENES.STOCK_PUBLIC) {
    delete out.displayName;
    delete out.userId;
    out.showHeader = false;
  }

  const allowActions = isSelf && (s === SCENES.SELF || s === SCENES.STOCK_SELF);
  if (allowActions) {
    out.actions = { canEdit: true, canDelete: true };
  } else {
    delete out.actions;
  }

  return out;
}

module.exports = {
  SCENES,
  applyDynamicsRedaction,
};
