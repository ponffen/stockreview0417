/**
 * Dynamics card field visibility by scene (delegates to card view layer).
 */

const { SCENES } = require("./dynamics-scenes");
const { applyCardView } = require("./dynamics-card-view");

function applyDynamicsRedaction(card, scene, options = {}) {
  return applyCardView(card, scene, options);
}

module.exports = {
  SCENES,
  applyDynamicsRedaction,
};
