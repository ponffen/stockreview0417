/**
 * One-off import for phone 18320260702 (user_id resolved at runtime).
 * Usage:
 *   node scripts/import-broker-trades-18320260702.js --dry-run
 *   node scripts/import-broker-trades-18320260702.js --import
 */
require("dotenv").config();
const fs = require("node:fs");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const {
  normalizeTrade,
  normalizeSymbol,
  importTrades,
  importCashTransfers,
  findUserByPhone,
  initPool,
  upsertSymbolNameMapBatch,
} = require("../src/db");
const { fetchSinaForexDayKSeries } = require("./lib/market-fetch");
const { previousSessionDate } = require("../src/metrics/freeze-calendar");

const PHONE = "18320260702";
const ACCOUNT_ID = "default";
const FX_FALLBACK = { USD: 7.2, HKD: 0.92 };

/** Manual overrides where symbol_name_map is ambiguous or missing */
const MANUAL_NAME_MAP = {
  苹果: "aapl",
  英伟达: "nvda",
  谷歌C: "goog",
  拼多多: "pdd",
  特斯拉: "tsla",
  "伯克希尔-哈撒韦B": "brk.b",
  "伯克希尔-哈撒书B": "brk.b",
  Strategy: "mstr",
  微软: "msft",
  台积电: "tsm",
  CoreWeave: "crwv",
  "Core Weave": "crwv",
  Coreleave: "crwv",
  "Credo Technology": "crdo",
  Coherent: "cohr",
  Marvell: "mrvl",
  美光科技: "mu",
  应用材料公司: "amat",
  "小米集团-W": "hk01810",
  腾讯控股: "hk00700",
  泡泡玛特: "hk09992",
  XL二南方海力士: "hk07709",
  "华夏沪深三百（港股）": "hk03188",
  "携程集团-S": "hk09961",
  紫金矿业: "sh601899",
  紫金矿业H: "hk02899",
  中国神华H: "hk01088",
  "黄金ETF-iShares": "hk02840",
  ASM太平洋: "hk00522",
  半导体板块指数ETF: "hk03167",
  中钨高新: "sz000657",
  云铝股份: "sz000807",
  黄金ETF: "sh518880",
  黃金ETF: "sh518880",
  黄金ETF华安: "sh518880",
  贵州茅台: "sh600519",
  沪深300ETF华夏: "sh510330",
  卫星产业ETF: "sh561660",
  芯片ETF: "sh512760",
  半导体设备ETF: "sh562590",
  世纪华通: "sz002602",
  中证500ETF: "sh510500",
  兆易创新: "sh603986",
  洛阳钼业: "sh603993",
  亨通光电: "sh600487",
  五洲新春: "sh603667",
  四方股份: "sh601126",
  标普500ETF博时: "sh513500",
  江丰电子: "sz300666",
  绿的谐波: "sh688017",
  绿的谱波: "sh688017",
  长光华新: "sh688048",
  长光华芯: "sh688048",
  中国巨石: "sh600176",
  中芯国际: "sh688981",
  东材科技: "sh601208",
  胜宏科技: "sz300476",
  雅克科技: "sz002409",
  鼎龙股份: "sz300054",
  中际旭创: "sz300308",
  宁德时代: "sz300750",
  寒武纪: "sh688256",
  海光信息: "sh688041",
  澜起科技: "sh688008",
  华虹半导体公司: "hk01347",
  华虹宏力: "sh688347",
  中瓷电子: "sz003031",
  长电科技: "sh600584",
  大普微: "sz301536",
  智谱: "hk02513",
  SpaceX: "spcx",
};

function round2(n) {
  return Math.round(Number(n) * 100) / 100;
}

function parseNum(raw) {
  return Number(String(raw || "").replace(/,/g, "").trim()) || 0;
}

function inferCurrency(symbol) {
  const s = String(symbol || "").toLowerCase();
  if (s.startsWith("hk") || s.startsWith("rt_hk")) return "HKD";
  if (s.startsWith("sh") || s.startsWith("sz")) return "CNY";
  if (s.startsWith("fx_")) return "CNY";
  return "USD";
}

function getFxRateForDate(currency, dateKey, fxByDate) {
  if (currency === "CNY") return 1;
  const dk = String(dateKey || "").slice(0, 10);
  const series = currency === "HKD" ? fxByDate.HKD : fxByDate.USD;
  if (series?.[dk] > 0) return series[dk];
  const keys = Object.keys(series || {}).sort();
  for (let i = keys.length - 1; i >= 0; i -= 1) {
    if (keys[i] <= dk && series[keys[i]] > 0) return series[keys[i]];
  }
  return FX_FALLBACK[currency] || 1;
}

function parseTsv(filePath) {
  const text = fs.readFileSync(filePath, "utf8");
  const lines = text.split(/\r?\n/).filter((l) => l.trim());
  const header = lines[0].split("\t");
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = lines[i].split("\t");
    if (cols.length < 6) continue;
    const row = {};
    header.forEach((h, idx) => {
      row[h.trim()] = (cols[idx] || "").trim();
    });
    rows.push(row);
  }
  return rows;
}

async function loadNameMap(pool) {
  const map = { ...MANUAL_NAME_MAP };
  const { rows } = await pool.query("SELECT symbol, name_cn FROM symbol_name_map");
  for (const r of rows) {
    const name = String(r.name_cn || "").trim();
    if (name && !map[name]) map[name] = r.symbol;
  }
  return map;
}

function resolveSymbol(name, nameMap) {
  const raw = String(name || "").trim();
  if (!raw) return "";
  if (nameMap[raw]) return normalizeSymbol(nameMap[raw]);
  const stripped = raw.replace(/[-－]?[WSBHSW]+$/i, "").replace(/（港股）$/, "").trim();
  if (nameMap[stripped]) return normalizeSymbol(nameMap[stripped]);
  for (const [k, v] of Object.entries(nameMap)) {
    if (k.includes(raw) || raw.includes(k)) return normalizeSymbol(v);
  }
  return "";
}

function rowToTrade(row, idx, nameMap) {
  const date = String(row["日期"] || "").slice(0, 10);
  const stockName = String(row["股票名称"] || "").trim();
  const sideRaw = String(row["买入卖出"] || "").trim();
  const side = sideRaw === "卖出" ? "sell" : "buy";
  const quantity = parseNum(row["交易股数"]);
  const price = parseNum(row["交易价格"]);
  const amount = parseNum(row["交易金额"]) || round2(price * quantity);
  const symbol = resolveSymbol(stockName, nameMap);
  if (!symbol) {
    return { error: `未映射股票: ${stockName}`, stockName };
  }
  return normalizeTrade({
    id: randomUUID(),
    accountId: ACCOUNT_ID,
    type: "trade",
    symbol,
    name: stockName,
    side,
    price,
    quantity,
    amount,
    date,
    note: "基于微博持仓明细推导出的交易记录。",
    createdAt: Date.parse(`${date}T12:00:00+08:00`) + idx,
  });
}

async function buildPayload(rows, nameMap, fxByDate) {
  const trades = [];
  const unmapped = [];
  for (let i = 0; i < rows.length; i += 1) {
    const t = rowToTrade(rows[i], i, nameMap);
    if (t.error) {
      unmapped.push(t.stockName);
      continue;
    }
    trades.push(t);
  }
  if (unmapped.length) {
    throw new Error(`未映射股票名称(${unmapped.length}): ${[...new Set(unmapped)].join("、")}`);
  }

  const daily = {};
  for (const t of trades) {
    const dk = t.date;
    if (!daily[dk]) daily[dk] = { buyCny: 0, sellCny: 0 };
    const ccy = inferCurrency(t.symbol);
    const cny = round2(t.amount * getFxRateForDate(ccy, dk, fxByDate));
    if (t.side === "buy") daily[dk].buyCny = round2(daily[dk].buyCny + cny);
    else daily[dk].sellCny = round2(daily[dk].sellCny + cny);
  }

  const cashTransfers = [];
  const dates = Object.keys(daily).sort();
  const firstTradeDate = dates[0] || "";
  let cashIdx = 0;
  for (const date of dates) {
    const slot = daily[date];
    if (slot.buyCny > 0) {
      const inDate = date === firstTradeDate ? previousSessionDate(firstTradeDate) || date : date;
      const inNote =
        date === firstTradeDate
          ? "期初本金（基于交易记录按当日汇总算出来的出入金。）"
          : "基于交易记录按当日汇总算出来的出入金。";
      cashTransfers.push({
        id: randomUUID(),
        accountId: ACCOUNT_ID,
        date: inDate,
        direction: "in",
        amount: round2(slot.buyCny),
        note: inNote,
        createdAt: Date.parse(`${inDate}T08:00:00+08:00`) + cashIdx++,
      });
    }
    if (slot.sellCny > 0) {
      cashTransfers.push({
        id: randomUUID(),
        accountId: ACCOUNT_ID,
        date,
        direction: "out",
        amount: round2(slot.sellCny),
        note: `基于交易记录按当日汇总算出来的出入金。`,
        createdAt: Date.parse(`${date}T18:00:00+08:00`) + cashIdx++,
      });
    }
  }

  return { trades, cashTransfers };
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const doImport = process.argv.includes("--import");
  if (!dryRun && !doImport) {
    console.log("Usage: node scripts/import-broker-trades-18320260702.js --dry-run|--import");
    process.exit(1);
  }

  const filePath = path.join(__dirname, "../data/import-18320260702.tsv");
  if (!fs.existsSync(filePath)) {
    throw new Error(`Missing data file: ${filePath}`);
  }

  const user = await findUserByPhone(PHONE);
  if (!user?.id) throw new Error(`User not found for phone ${PHONE}`);

  const pool = await initPool();
  const nameMap = await loadNameMap(pool);
  const rows = parseTsv(filePath);
  console.log(`Parsed ${rows.length} rows from TSV`);

  console.log("Fetching FX rates (USD/HKD)...");
  let usdSeries = {};
  let hkdSeries = {};
  try {
    [usdSeries, hkdSeries] = await Promise.all([
      fetchSinaForexDayKSeries("USD"),
      fetchSinaForexDayKSeries("HKD"),
    ]);
    console.log(`FX loaded: USD ${Object.keys(usdSeries).length} days, HKD ${Object.keys(hkdSeries).length} days`);
  } catch (e) {
    console.warn(`FX fetch failed (${e.message}); using fallback USD=${FX_FALLBACK.USD}, HKD=${FX_FALLBACK.HKD}`);
  }
  const fxByDate = { USD: usdSeries, HKD: hkdSeries };

  const { trades, cashTransfers } = await buildPayload(rows, nameMap, fxByDate);

  const symBatch = [...new Set(trades.map((t) => ({ symbol: t.symbol, nameCn: t.name })).map(JSON.stringify))].map(
    (s) => JSON.parse(s),
  );

  console.log(`Trades: ${trades.length}, Cash transfers: ${cashTransfers.length}`);
  console.log("Sample trades:", trades.slice(0, 3));
  console.log("Sample cash:", cashTransfers.slice(0, 3));
  console.log(
    "Date range:",
    trades[0]?.date,
    "->",
    trades[trades.length - 1]?.date,
  );

  if (dryRun) {
    console.log("[dry-run] No DB writes.");
    return;
  }

  await importTrades(trades, "replace", user.id);
  await importCashTransfers(cashTransfers, "replace", user.id);
  await upsertSymbolNameMapBatch(
    symBatch.map((x) => ({ symbol: x.symbol, nameCn: x.nameCn, source: "import" })),
  );
  console.log(`Imported for user ${user.id} (${PHONE})`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
