#!/usr/bin/env node
/** One-shot patch: add 股票占比 + 现金占比 rows to overview KPI grid. */
const fs = require("fs");
const path = require("path");

const appPath = path.join(__dirname, "..", "app.js");
let s = fs.readFileSync(appPath, "utf8");

if (!s.includes("overviewMetricsUi")) {
  console.error("app.js missing overviewMetricsUi — restore from origin/main first");
  process.exit(1);
}

const oldHelpers = `/** 总览「现金占比」：现金 / 总资产 × 100%（与当前展示的 totalAssets、overviewCash 同口径）。 */
function formatOverviewCashRatioFromTotals(totalAssetsBook, cashBook) {
  const ta = Number(totalAssetsBook);
  const c = Number(cashBook);
  if (!Number.isFinite(ta) || ta <= 0 || !Number.isFinite(c)) {
    return "0.00%";
  }
  return \`\${formatNumber((c / ta) * 100, 2)}%\`;
}

/**
 * 总览区 2×3 栅格：总资产、总市值 | 现金、现金占比 | 本金、（空）。
 * @param {{ label: string, value: string }[]} entries 共 5 条
 */
function buildOverviewKpiGridInnerHtml(entries) {
  const cells = entries
    .map(
      (item) => \`
      <article class="kpi-item">
        <p class="kpi-label">\${escapeHtml(item.label)}</p>
        <p class="kpi-value">\${escapeHtml(String(item.value))}</p>
      </article>
    \`,
    )
    .join("");
  return \`\${cells}<article class="kpi-item kpi-item--empty" aria-hidden="true"></article>\`;
}`;

const newHelpers = `/** 总览「现金占比」：现金 / 总资产 × 100%（与当前展示的 totalAssets、overviewCash 同口径）。 */
function formatOverviewCashRatioFromTotals(totalAssetsBook, cashBook) {
  const ta = Number(totalAssetsBook);
  const c = Number(cashBook);
  if (!Number.isFinite(ta) || ta <= 0 || !Number.isFinite(c)) {
    return "0.00%";
  }
  return \`\${formatNumber((c / ta) * 100, 2)}%\`;
}

/** 总览「股票占比」：持仓总市值 / 总资产 × 100%（全部股票仓位合计）。 */
function formatOverviewStockRatioFromTotals(totalAssetsBook, marketValueBook) {
  const ta = Number(totalAssetsBook);
  const mv = Number(marketValueBook);
  if (!Number.isFinite(ta) || ta <= 0 || !Number.isFinite(mv)) {
    return "0.00%";
  }
  return \`\${formatNumber((mv / ta) * 100, 2)}%\`;
}

/** 总览 KPI 六项：总资产、总市值、现金、股票占比、现金占比、本金。 */
function buildOverviewKpiEntries({
  totalAssets,
  marketValue,
  cash,
  stockRatio,
  cashRatio,
  principal,
}) {
  return [
    { label: "总资产", value: totalAssets },
    { label: "总市值", value: marketValue },
    { label: "现金", value: cash },
    { label: "股票占比", value: stockRatio },
    { label: "现金占比", value: cashRatio },
    { label: "本金", value: principal },
  ];
}

/**
 * 总览区 2 列栅格；条目数为奇数时末尾补空单元格。
 * @param {{ label: string, value: string }[]} entries
 */
function buildOverviewKpiGridInnerHtml(entries) {
  const cells = entries
    .map(
      (item) => \`
      <article class="kpi-item">
        <p class="kpi-label">\${escapeHtml(item.label)}</p>
        <p class="kpi-value">\${escapeHtml(String(item.value))}</p>
      </article>
    \`,
    )
    .join("");
  if (entries.length % 2 === 1) {
    return \`\${cells}<article class="kpi-item kpi-item--empty" aria-hidden="true"></article>\`;
  }
  return cells;
}`;

if (!s.includes(oldHelpers)) {
  if (s.includes("buildOverviewKpiEntries")) {
    console.log("helpers already patched");
  } else {
    console.error("helper block not found");
    process.exit(1);
  }
} else {
  s = s.replace(oldHelpers, newHelpers);
}

const replacements = [
  [
    `overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml([
      { label: "总资产", value: dash },
      { label: "总市值", value: dash },
      { label: "现金", value: dash },
      { label: "现金占比", value: dash },
      { label: "本金", value: dash },
    ]);`,
    `overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml(
      buildOverviewKpiEntries({
        totalAssets: dash,
        marketValue: dash,
        cash: dash,
        stockRatio: dash,
        cashRatio: dash,
        principal: dash,
      }),
    );`,
  ],
  [
    `overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml([
    { label: "总资产", value: dash },
    { label: "总市值", value: dash },
    { label: "现金", value: dash },
    { label: "现金占比", value: dash },
    { label: "本金", value: dash },
  ]);`,
    `overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml(
    buildOverviewKpiEntries({
      totalAssets: dash,
      marketValue: dash,
      cash: dash,
      stockRatio: dash,
      cashRatio: dash,
      principal: dash,
    }),
  );`,
  ],
  [
    `  const ratioStr = formatOverviewCashRatioFromTotals(totalAssets, cash);

  overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml([
    { label: "总资产", value: formatOverviewPlainMoney(totalAssets, bookCcy) },
    { label: "总市值", value: formatOverviewPlainMoney(mv, bookCcy) },
    { label: "现金", value: formatOverviewPlainMoney(cash, bookCcy) },
    { label: "现金占比", value: ratioStr },
    { label: "本金", value: formatOverviewPlainMoney(principal, bookCcy) },
  ]);`,
    `  const cashRatioStr = formatOverviewCashRatioFromTotals(totalAssets, cash);
  const stockRatioStr = formatOverviewStockRatioFromTotals(totalAssets, mv);

  overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml(
    buildOverviewKpiEntries({
      totalAssets: formatOverviewPlainMoney(totalAssets, bookCcy),
      marketValue: formatOverviewPlainMoney(mv, bookCcy),
      cash: formatOverviewPlainMoney(cash, bookCcy),
      stockRatio: stockRatioStr,
      cashRatio: cashRatioStr,
      principal: formatOverviewPlainMoney(principal, bookCcy),
    }),
  );`,
  ],
  [
    `    overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml([
      { label: "总资产", value: String(surfDisp.totalAssets) },
      { label: "总市值", value: String(surfDisp.marketValue) },
      { label: "现金", value: String(surfDisp.cash) },
      { label: "现金占比", value: String(surfDisp.cashRatio) },
      { label: "本金", value: String(surfDisp.principal) },
    ]);`,
    `    overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml(
      buildOverviewKpiEntries({
        totalAssets: String(surfDisp.totalAssets),
        marketValue: String(surfDisp.marketValue),
        cash: String(surfDisp.cash),
        stockRatio: String(surfDisp.stockRatio || "–"),
        cashRatio: String(surfDisp.cashRatio),
        principal: String(surfDisp.principal),
      }),
    );`,
  ],
  [
    `    const ratioStr = formatOverviewCashRatioFromTotals(portfolio.totalAssets, portfolio.overviewCash);
    overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml([
      { label: "总资产", value: formatOverviewPlainMoney(portfolio.totalAssets, bookCcy) },
      { label: "总市值", value: formatOverviewPlainMoney(portfolio.totalMarketValue, bookCcy) },
      { label: "现金", value: formatOverviewPlainMoney(portfolio.overviewCash, bookCcy) },
      { label: "现金占比", value: ratioStr },
      { label: "本金", value: formatOverviewPlainMoney(portfolio.overviewPrincipal, bookCcy) },
    ]);`,
    `    const cashRatioStr = formatOverviewCashRatioFromTotals(portfolio.totalAssets, portfolio.overviewCash);
    const stockRatioStr = formatOverviewStockRatioFromTotals(
      portfolio.totalAssets,
      portfolio.totalMarketValue,
    );
    overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml(
      buildOverviewKpiEntries({
        totalAssets: formatOverviewPlainMoney(portfolio.totalAssets, bookCcy),
        marketValue: formatOverviewPlainMoney(portfolio.totalMarketValue, bookCcy),
        cash: formatOverviewPlainMoney(portfolio.overviewCash, bookCcy),
        stockRatio: stockRatioStr,
        cashRatio: cashRatioStr,
        principal: formatOverviewPlainMoney(portfolio.overviewPrincipal, bookCcy),
      }),
    );`,
  ],
  [
    `    overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml([
      { label: "总资产", value: String(assets.totalAssetsDisplay || "–") },
      { label: "总市值", value: String(assets.marketValueDisplay || "–") },
      { label: "现金", value: String(assets.cashDisplay || "–") },
      { label: "现金占比", value: String(assets.cashRatioDisplay || "–") },
      { label: "本金", value: String(assets.principalDisplay || "–") },
    ]);`,
    `    overviewGrid.innerHTML = buildOverviewKpiGridInnerHtml(
      buildOverviewKpiEntries({
        totalAssets: String(assets.totalAssetsDisplay || "–"),
        marketValue: String(assets.marketValueDisplay || "–"),
        cash: String(assets.cashDisplay || "–"),
        stockRatio: String(assets.stockRatioDisplay || "–"),
        cashRatio: String(assets.cashRatioDisplay || "–"),
        principal: String(assets.principalDisplay || "–"),
      }),
    );`,
  ],
];

for (const [from, to] of replacements) {
  if (s.includes(from)) {
    s = s.replace(from, to);
  }
}

if (s.includes('{ label: "现金占比", value: dash }')) {
  console.warn("some dash blocks may remain");
}

fs.writeFileSync(appPath, s);
console.log("patched", appPath);
