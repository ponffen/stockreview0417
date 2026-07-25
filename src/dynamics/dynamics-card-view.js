/**
 * Dynamics card view config: one template + slot/field visibility by scene.
 */

const { SCENES } = require("./dynamics-scenes");

const METRIC_DEFS = {
  price: { label: "交易价格", field: "price" },
  quantity: { label: "交易数量", field: "quantity" },
  amount: { label: "交易金额", field: "amount" },
  amountShareRatio: { label: "金额", field: "amountShareRatio", help: true },
  tradeDate: { label: "交易日期", field: "tradeDate" },
};

function defaultSlots(cardKind, postType) {
  if (cardKind === "post") {
    return {
      header: true,
      stock: true,
      metrics: postType === "valuation",
      body: true,
      images: true,
      footer: true,
    };
  }
  return {
    header: true,
    stock: true,
    metrics: true,
    body: true,
    images: true,
    footer: true,
  };
}

function headerLinksForScene(scene) {
  if (scene === SCENES.COMMUNITY) {
    return ["stockAnalysis", "portfolioAnalysis"];
  }
  if (scene === SCENES.PUBLIC || scene === SCENES.SELF) {
    return ["stockAnalysis"];
  }
  return [];
}

function metricKeysForScene(scene, cardKind) {
  if (cardKind !== "trade") {
    return [];
  }
  const isPrivateTrade = scene === SCENES.SELF || scene === SCENES.STOCK_SELF;
  if (isPrivateTrade) {
    return ["price", "quantity", "amount", "tradeDate"];
  }
  return ["price", "amountShareRatio", "tradeDate"];
}

function buildMetricsPresentation(model, metricKeys) {
  const cols = [];
  for (const key of metricKeys) {
    const def = METRIC_DEFS[key];
    if (!def) {
      continue;
    }
    const raw = model[def.field];
    if (key === "amountShareRatio") {
      if (raw == null || raw === "" || raw === "—") {
        continue;
      }
    }
    cols.push({
      label: def.label,
      value: raw == null || raw === "" ? "—" : String(raw),
      help: Boolean(def.help),
    });
  }
  return cols;
}

function applyCardView(model, scene, { isSelf = false } = {}) {
  const src = model || {};
  const s = String(scene || "").trim() || SCENES.COMMUNITY;
  const cardKind = src.cardKind === "post" ? "post" : "trade";
  const postType = cardKind === "post" ? String(src.postType || "viewpoint").trim() || "viewpoint" : "";
  const slots = defaultSlots(cardKind, postType);
  const isPrivateTrade = s === SCENES.SELF || s === SCENES.STOCK_SELF;

  if (s === SCENES.STOCK_SELF || s === SCENES.STOCK_PUBLIC) {
    slots.header = false;
  }

  const metricKeys = slots.metrics && cardKind === "trade" ? metricKeysForScene(s, cardKind) : [];
  const metrics = buildMetricsPresentation(src, metricKeys);

  const view = {
    slots,
    headerLinks: slots.header ? headerLinksForScene(s) : [],
    footerAccount: isPrivateTrade && cardKind === "trade",
    stockMode: cardKind === "post" ? "tags" : "trade-row",
  };

  const out = {
    ...src,
    cardKind,
    view,
  };

  if (cardKind === "trade" && metrics.length) {
    out.metrics = metrics;
  }

  if (!slots.header) {
    delete out.displayName;
    delete out.userId;
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
  METRIC_DEFS,
  applyCardView,
};
