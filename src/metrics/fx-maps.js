/**
 * 外汇日汇率：冻结/重算/盘中冻结日均读 symbol_daily_close（fx_usdcny / fx_hkdcny）。
 * 仅盘中 live 当日汇率可走实时行情；此处不调用新浪/腾讯接口。
 */
const { addCalendarDays } = require("../db-pure");

const FX_SYMBOL_USD = "fx_usdcny";
const FX_SYMBOL_HKD = "fx_hkdcny";

function getSymbolDailyCloseRange() {
  return require("../db").getSymbolDailyCloseRange;
}

function rowsToFxMap(rows) {
  const out = {};
  for (const row of rows || []) {
    const dk = String(row.date || "").slice(0, 10);
    const close = Number(row.close);
    if (dk && Number.isFinite(close) && close > 0) {
      out[dk] = close;
    }
  }
  return out;
}

/** 已知 K 线日期（升序）。 */
function sortedPositiveFxKeys(fxMap) {
  return Object.keys(fxMap || {})
    .filter((k) => Number(fxMap[k]) > 0)
    .sort();
}

/** 目标日缺 bar 时：用不晚于该日的最近有效收盘价；仍无则保持缺失。 */
function fxRateOnOrBefore(fxMap, dateKey) {
  const dk = String(dateKey || "").slice(0, 10);
  const direct = Number(fxMap?.[dk]);
  if (Number.isFinite(direct) && direct > 0) {
    return direct;
  }
  let pick = 0;
  for (const k of sortedPositiveFxKeys(fxMap)) {
    if (k <= dk) {
      pick = Number(fxMap[k]);
    } else {
      break;
    }
  }
  return pick;
}

/** 先向前用最近有效日填充，再向后延续；不使用 map 全表最早日兜底。 */
function backwardThenForwardFillFxMap(fxMap, dateKeys) {
  const sorted = [...new Set((dateKeys || []).map((d) => String(d || "").slice(0, 10)).filter(Boolean))].sort();
  if (!sorted.length) {
    return fxMap;
  }

  const knownKeys = sortedPositiveFxKeys(fxMap);
  let last = fxRateOnOrBefore(fxMap, sorted[0]);

  for (const dk of sorted) {
    const v = Number(fxMap[dk]);
    if (Number.isFinite(v) && v > 0) {
      last = v;
      fxMap[dk] = v;
    } else {
      const prior = fxRateOnOrBefore(fxMap, dk);
      if (prior > 0) {
        fxMap[dk] = prior;
        last = prior;
      } else if (last > 0) {
        fxMap[dk] = last;
      }
    }
  }

  for (const dk of sorted) {
    if (!(Number(fxMap[dk]) > 0) && knownKeys.length) {
      const prior = fxRateOnOrBefore(fxMap, dk);
      if (prior > 0) {
        fxMap[dk] = prior;
      }
    }
  }
  return fxMap;
}

async function loadFxCloseMapsFromDb(fromDate, toDate) {
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  const fetchRange = getSymbolDailyCloseRange();
  const [usdRows, hkdRows] = await Promise.all([
    fetchRange(FX_SYMBOL_USD, from, to),
    fetchRange(FX_SYMBOL_HKD, from, to),
  ]);
  return {
    fxUsdMap: rowsToFxMap(usdRows),
    fxHkdMap: rowsToFxMap(hkdRows),
  };
}

function warnMissingFxDates(fxUsdMap, fxHkdMap, dateKeys, logger = console) {
  for (const dk of dateKeys || []) {
    if (!(Number(fxUsdMap[dk]) > 0)) {
      logger.warn?.("[buildFxMaps] missing USDCNY in symbol_daily_close for", dk);
    }
    if (!(Number(fxHkdMap[dk]) > 0)) {
      logger.warn?.("[buildFxMaps] missing HKDCNY in symbol_daily_close for", dk);
    }
  }
}

/** 冻结路径：按交易日序列从 symbol_daily_close 构建 USD/HKD→CNY 汇率表。 */
async function buildFxMaps(allDates, logger = console) {
  const sorted = [...new Set((allDates || []).map((d) => String(d || "").slice(0, 10)).filter(Boolean))].sort();
  if (!sorted.length) {
    return { fxUsdMap: {}, fxHkdMap: {} };
  }
  const minD = sorted[0];
  const maxD = sorted[sorted.length - 1];
  const { fxUsdMap, fxHkdMap } = await loadFxCloseMapsFromDb(minD, maxD);
  backwardThenForwardFillFxMap(fxUsdMap, sorted);
  backwardThenForwardFillFxMap(fxHkdMap, sorted);
  warnMissingFxDates(fxUsdMap, fxHkdMap, sorted, logger);
  return { fxUsdMap, fxHkdMap };
}

/**
 * 单日汇率（冻结日 / 非 live 场景）：读 symbol_daily_close，必要时向前找最近有效日。
 */
async function loadFxRatesOnDate(dateKey, logger = console) {
  const dk = String(dateKey || "").slice(0, 10);
  if (!dk) {
    return { USD: 0, HKD: 0 };
  }
  const from = addCalendarDays(dk, -120);
  const { fxUsdMap, fxHkdMap } = await loadFxCloseMapsFromDb(from, dk);
  const usd = fxRateOnOrBefore(fxUsdMap, dk);
  const hkd = fxRateOnOrBefore(fxHkdMap, dk);
  if (!(usd > 0)) {
    logger.warn?.("[loadFxRatesOnDate] missing USDCNY in symbol_daily_close for", dk);
  }
  if (!(hkd > 0)) {
    logger.warn?.("[loadFxRatesOnDate] missing HKDCNY in symbol_daily_close for", dk);
  }
  return { USD: usd, HKD: hkd };
}

/**
 * 统一汇率解析：盘中优先 live 实时 > 快照/入参 > symbol_daily_close。
 */
async function resolveFxRatesCny(opts = {}, logger = console) {
  const dk = String(opts.dateKey || "").slice(0, 10);
  let usd = Number(opts.snapshotUsd) || 0;
  let hkd = Number(opts.snapshotHkd) || 0;
  if (opts.preferLiveSpot) {
    const liveUsd = Number(opts.liveSpot?.USD ?? opts.liveUsd) || 0;
    const liveHkd = Number(opts.liveSpot?.HKD ?? opts.liveHkd) || 0;
    if (liveUsd > 0) {
      usd = liveUsd;
    }
    if (liveHkd > 0) {
      hkd = liveHkd;
    }
  }
  if (usd > 0 && hkd > 0) {
    return { USD: usd, HKD: hkd, fxUsdCny: usd, fxHkdCny: hkd };
  }
  const db = dk ? await loadFxRatesOnDate(dk, logger) : { USD: 0, HKD: 0 };
  if (!(usd > 0)) {
    usd = Number(db.USD) || 0;
  }
  if (!(hkd > 0)) {
    hkd = Number(db.HKD) || 0;
  }
  return { USD: usd, HKD: hkd, fxUsdCny: usd, fxHkdCny: hkd };
}

/** 返回 dateKey→{USD,HKD}，供前端历史汇率缓存。 */
async function loadFxCloseSeriesByDate(fromDate, toDate) {
  const from = String(fromDate || "").slice(0, 10);
  const to = String(toDate || "").slice(0, 10);
  const { fxUsdMap, fxHkdMap } = await loadFxCloseMapsFromDb(from, to);
  const dates = [...new Set([...Object.keys(fxUsdMap), ...Object.keys(fxHkdMap)])].sort();
  backwardThenForwardFillFxMap(fxUsdMap, dates);
  backwardThenForwardFillFxMap(fxHkdMap, dates);
  const out = {};
  for (const dk of dates) {
    const usd = Number(fxUsdMap[dk]) || 0;
    const hkd = Number(fxHkdMap[dk]) || 0;
    if (usd > 0 || hkd > 0) {
      out[dk] = { USD: usd, HKD: hkd };
    }
  }
  return out;
}

module.exports = {
  FX_SYMBOL_USD,
  FX_SYMBOL_HKD,
  rowsToFxMap,
  fxRateOnOrBefore,
  backwardThenForwardFillFxMap,
  loadFxCloseMapsFromDb,
  buildFxMaps,
  loadFxRatesOnDate,
  resolveFxRatesCny,
  loadFxCloseSeriesByDate,
};
