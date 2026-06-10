const { addCalendarDays } = require("./db-pure");
const { toDateKey: beijingDateKey } = require("../scripts/lib/market-fetch");

const LEGACY_USER_VALID_UNTIL = "2099-12-31";
/** 注册日起 10 个自然日（含当天）→ 最后一天 = 注册日 + 9 */
const NEW_USER_TRIAL_LAST_DAY_OFFSET = 9;

function normalizeValidUntilDate(value) {
  const raw = String(value || "").trim().slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : LEGACY_USER_VALID_UNTIL;
}

function computeNewUserValidUntil(fromDate = new Date()) {
  const registerDay = beijingDateKey(fromDate);
  return addCalendarDays(registerDay, NEW_USER_TRIAL_LAST_DAY_OFFSET);
}

function isSubscriptionExpired(validUntil, todayKey = beijingDateKey()) {
  const until = normalizeValidUntilDate(validUntil);
  const today = normalizeValidUntilDate(todayKey);
  return today > until;
}

module.exports = {
  LEGACY_USER_VALID_UNTIL,
  NEW_USER_TRIAL_LAST_DAY_OFFSET,
  normalizeValidUntilDate,
  computeNewUserValidUntil,
  isSubscriptionExpired,
  beijingDateKey,
};
