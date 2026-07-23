/**
 * 收益三轨（native / book / cny）日收益与阶段字段映射。
 */
const { fxToCnyOnDate } = require("../return-calcs");

const FX_FALLBACK = { USD: 7.2, HKD: 0.92 };

const STAGE_KEYS = ["mtd", "ytd", "inception", "last_7d", "last_30d", "last_90d"];

function nativeToCny(amountNative, ccy, dateKey, fxUsdMap, fxHkdMap) {
  const v = Number(amountNative) || 0;
  const c = String(ccy || "CNY").toUpperCase();
  if (c === "CNY") {
    return v;
  }
  return v * fxToCnyOnDate(fxUsdMap, fxHkdMap, c, dateKey, FX_FALLBACK);
}

function cnyToBook(amountCny, book, dateKey, fxUsdMap, fxHkdMap) {
  const v = Number(amountCny) || 0;
  const b = String(book || "CNY").toUpperCase();
  if (b === "CNY") {
    return v;
  }
  const fx = fxToCnyOnDate(fxUsdMap, fxHkdMap, b, dateKey, FX_FALLBACK);
  return fx > 0 ? v / fx : v;
}

function nativeToBook(amountNative, ccy, book, dateKey, fxUsdMap, fxHkdMap) {
  return cnyToBook(nativeToCny(amountNative, ccy, dateKey, fxUsdMap, fxHkdMap), book, dateKey, fxUsdMap, fxHkdMap);
}

function dailyRateTwr(profit, startMv, flow) {
  const p = Number(profit) || 0;
  const base = Number(startMv) || 0;
  const f = Number(flow) || 0;
  const denom = base + Math.max(f, 0);
  return denom > 0 ? p / denom : 0;
}

function computeSymbolDailyProfitTracks({
  qty,
  qBod,
  closeD,
  prevPx,
  dayFlow,
  ccy,
  book,
  dk,
  prevD,
  fxUsdMap,
  fxHkdMap,
}) {
  const mvEndNat = Number(qty) * Number(closeD);
  const mvStartNat = Number(qBod) * Number(prevPx);
  const flowNat = Number(dayFlow) || 0;
  const nativeProfit = mvEndNat - mvStartNat - flowNat;

  const mvEndCny = nativeToCny(mvEndNat, ccy, dk, fxUsdMap, fxHkdMap);
  const mvStartCny = nativeToCny(mvStartNat, ccy, prevD, fxUsdMap, fxHkdMap);
  const flowCny = nativeToCny(flowNat, ccy, dk, fxUsdMap, fxHkdMap);
  const cnyProfit = mvEndCny - mvStartCny - flowCny;

  const mvEndBook = nativeToBook(mvEndNat, ccy, book, dk, fxUsdMap, fxHkdMap);
  const mvStartBook = nativeToBook(mvStartNat, ccy, book, prevD, fxUsdMap, fxHkdMap);
  const flowBook = nativeToBook(flowNat, ccy, book, dk, fxUsdMap, fxHkdMap);
  const bookProfit = mvEndBook - mvStartBook - flowBook;

  return {
    native: {
      profit: nativeProfit,
      rateTwr: dailyRateTwr(nativeProfit, mvStartNat, flowNat),
    },
    book: {
      profit: bookProfit,
      rateTwr: dailyRateTwr(bookProfit, mvStartBook, flowBook),
    },
    cny: {
      profit: cnyProfit,
      rateTwr: dailyRateTwr(cnyProfit, mvStartCny, flowCny),
    },
  };
}

function computeTodayProfitTracks({
  endQty,
  startQty,
  current,
  prevClose,
  dayFlowNative,
  ccy,
  book,
  liveDate,
  frozenDate,
  fxLive,
  fxFrozen,
}) {
  const qEnd = Number(endQty) || 0;
  const qStart = Number(startQty) || 0;
  const pxEnd = Number(current) || 0;
  const pxStart = Number(prevClose) || 0;
  const flowNat = Number(dayFlowNative) || 0;
  const dk = String(liveDate || "").slice(0, 10);
  const prevD = String(frozenDate || "").slice(0, 10);
  const fxUsdMapLive = { [dk]: Number(fxLive?.USD) || FX_FALLBACK.USD };
  const fxHkdMapLive = { [dk]: Number(fxLive?.HKD) || FX_FALLBACK.HKD };
  const fxUsdMapFrozen = { [prevD]: Number(fxFrozen?.USD) || FX_FALLBACK.USD };
  const fxHkdMapFrozen = { [prevD]: Number(fxFrozen?.HKD) || FX_FALLBACK.HKD };
  const fxUsdMap = { ...fxUsdMapFrozen, ...fxUsdMapLive };
  const fxHkdMap = { ...fxHkdMapFrozen, ...fxHkdMapLive };
  return computeSymbolDailyProfitTracks({
    qty: qEnd,
    qBod: qStart,
    closeD: pxEnd,
    prevPx: pxStart,
    dayFlow: flowNat,
    ccy,
    book,
    dk,
    prevD,
    fxUsdMap,
    fxHkdMap,
  });
}

function stageFieldBase(key) {
  const parts = String(key || "")
    .split("_")
    .map((p) => (p ? p[0].toUpperCase() + p.slice(1) : ""));
  return `stage${parts.join("")}`;
}

function rowStageValue(row, stageKey, field, track) {
  if (!row) {
    return 0;
  }
  const base = stageFieldBase(stageKey);
  const suffix = track === "native" ? "" : `_${track}`;
  const camel = `${base}${field}${suffix}`;
  if (row[camel] != null && Number.isFinite(Number(row[camel]))) {
    return Number(row[camel]);
  }
  if (track === "native") {
    const legacy = `${base}${field}`;
    return Number(row[legacy]) || 0;
  }
  if (track === "book" && field === "Profit") {
    return Number(row[`${base}${field}Book`]) || 0;
  }
  if (track === "cny" && field === "Profit") {
    return Number(row[`${base}${field}Cny`]) || 0;
  }
  if (track === "book" && field === "RateTwr") {
    return Number(row[`${base}${field}Book`]) || 0;
  }
  if (track === "cny" && field === "RateTwr") {
    return Number(row[`${base}${field}Cny`]) || 0;
  }
  return 0;
}

function applyStageSnapshotsToRow(row, snaps) {
  for (const stageKey of STAGE_KEYS) {
    const base = stageFieldBase(stageKey);
    row[`${base}Profit`] = snaps.native[`${base}Profit`];
    row[`${base}RateTwr`] = snaps.native[`${base}RateTwr`];
    row[`${base}ProfitBook`] = snaps.book[`${base}Profit`];
    row[`${base}RateTwrBook`] = snaps.book[`${base}RateTwr`];
    row[`${base}ProfitCny`] = snaps.cny[`${base}Profit`];
    row[`${base}RateTwrCny`] = snaps.cny[`${base}RateTwr`];
  }
}

function resolveFrozenStageProfits(frozenRow, { freshMonth, freshYear, sessionAsOf }, track) {
  const { monthStartKeyShanghai, yearStartKeyShanghai } = require("./stages");
  const monthStart = monthStartKeyShanghai(sessionAsOf);
  const yearStart = yearStartKeyShanghai(sessionAsOf);
  const rowDate = frozenRow ? String(frozenRow.date || "").slice(0, 10) : "";
  let monthFrozen = 0;
  if (!freshMonth && frozenRow && rowDate >= monthStart) {
    monthFrozen = rowStageValue(frozenRow, "mtd", "Profit", track);
  }
  let yearFrozen = 0;
  if (!freshYear && frozenRow && rowDate >= yearStart) {
    yearFrozen = rowStageValue(frozenRow, "ytd", "Profit", track);
  }
  const totalFrozen = frozenRow ? rowStageValue(frozenRow, "inception", "Profit", track) : 0;
  return { monthFrozen, yearFrozen, totalFrozen };
}

function profitTrackForDisplay(stockAmountDisplay) {
  return stockAmountDisplay === "cny" ? "cny" : "native";
}

function todayProfitFromTracks(tracks, track) {
  return Number(tracks?.[track]?.profit) || 0;
}

module.exports = {
  FX_FALLBACK,
  STAGE_KEYS,
  nativeToCny,
  cnyToBook,
  nativeToBook,
  dailyRateTwr,
  computeSymbolDailyProfitTracks,
  computeTodayProfitTracks,
  applyStageSnapshotsToRow,
  resolveFrozenStageProfits,
  rowStageValue,
  profitTrackForDisplay,
  todayProfitFromTracks,
};
