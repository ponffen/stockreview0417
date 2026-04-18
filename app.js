const STORAGE_KEY = "earning-clone-state-v2";
const QUOTE_REFRESH_MS = 60_000;
const KLINE_DATALEN = 420;
const CHART_FALLBACK_DAYS = 90;
const DEFAULT_BENCHMARK_PRICE = {
  sh000001: 0,
  sz399001: 0,
  rt_hkHSI: 0,
  gb_inx: 0,
};

const demoTrades = [
  {
    id: crypto.randomUUID(),
    type: "trade",
    symbol: "sz300750",
    name: "宁德时代",
    side: "buy",
    price: 443.27,
    quantity: 100,
    amount: 44327,
    date: "2026-04-17",
    note: "",
    createdAt: Date.now() - 2,
  },
  {
    id: crypto.randomUUID(),
    type: "trade",
    symbol: "sh601899",
    name: "紫金矿业",
    side: "buy",
    price: 34.68,
    quantity: 300,
    amount: 10404,
    date: "2026-04-17",
    note: "",
    createdAt: Date.now() - 1,
  },
];

const state = {
  route: "earning",
  previousRoute: "earning",
  useDemoData: true,
  algoMode: "cost",
  benchmark: "none",
  stageRange: "month",
  rangeDays: 7,
  capitalAmount: 0,
  trades: [],
  quoteMap: {},
  klineMap: {},
  quoteTime: "--",
  marketLoading: false,
  editingTradeId: null,
  activeRecordId: null,
  activeRecordSymbol: null,
};

const routeButtons = [...document.querySelectorAll(".bottom-tab-btn")];
const routePanes = [...document.querySelectorAll(".route-pane")];
const overviewGrid = document.getElementById("overviewGrid");
const quoteTime = document.getElementById("quoteTime");
const todayProfitMain = document.getElementById("todayProfitMain");
const monthProfitMain = document.getElementById("monthProfitMain");
const stageRangeSelect = document.getElementById("stageRangeSelect");
const stockTableBody = document.getElementById("stockTableBody");
const recordList = document.getElementById("recordList");
const analysisSummary = document.getElementById("analysisSummary");
const analysisChart = document.getElementById("analysisChart");
const demoToggleBtn = document.getElementById("demoToggleBtn");
const quickTradeBtn = document.getElementById("quickTradeBtn");
const recordTradeBtn = document.getElementById("recordTradeBtn");
const setCapitalBtn = document.getElementById("setCapitalBtn");
const algoModeSelect = document.getElementById("algoMode");
const benchmarkSelect = document.getElementById("benchmark");
const rangeChips = [...document.querySelectorAll(".range-chip")];
const tradeTableBody = document.getElementById("tradeTableBody");
const tradeDialog = document.getElementById("tradeDialog");
const tradeForm = document.getElementById("tradeForm");
const closeTradeDialogBtn = document.getElementById("closeTradeDialogBtn");
const tradeTypeInput = document.getElementById("tradeType");
const tradePriceInput = document.getElementById("tradePrice");
const tradeQuantityInput = document.getElementById("tradeQuantity");
const tradeSideInput = document.getElementById("tradeSide");
const tradeAmountInput = document.getElementById("tradeAmount");
const tradeDialogTitle = document.getElementById("tradeDialogTitle");
const tradeSubmitBtn = document.getElementById("tradeSubmitBtn");
const capitalDialog = document.getElementById("capitalDialog");
const capitalForm = document.getElementById("capitalForm");
const closeCapitalDialogBtn = document.getElementById("closeCapitalDialogBtn");
const capitalAmountInput = document.getElementById("capitalAmount");
const closeStockRecordDialogBtn = document.getElementById("closeStockRecordDialogBtn");
const stockRecordTitle = document.getElementById("stockRecordTitle");
const stockRecordTime = document.getElementById("stockRecordTime");
const stockRecordPrice = document.getElementById("stockRecordPrice");
const stockRecordChange = document.getElementById("stockRecordChange");
const stockRecordChart = document.getElementById("stockRecordChart");
const stockRecordMarket = document.getElementById("stockRecordMarket");
const stockRecordRegret = document.getElementById("stockRecordRegret");
const stockRecordListBody = document.getElementById("stockRecordListBody");
const recordActionPopover = document.getElementById("recordActionPopover");
const tradeSymbolInput = document.getElementById("tradeSymbol");
const tradeNameInput = document.getElementById("tradeName");
const tradeDateInput = document.getElementById("tradeDate");
const tradeNoteInput = document.getElementById("tradeNote");

initialize();

function initialize() {
  hydrateState();
  bindEvents();
  renderAll();
  refreshMarketData();
  window.setInterval(refreshMarketData, QUOTE_REFRESH_MS);
}

function hydrateState() {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      state.route = parsed.route ?? state.route;
      if (state.route === "records") {
        state.route = "trade";
      }
      if (state.route === "introduction") {
        state.route = "account";
      }
      state.useDemoData = parsed.useDemoData ?? state.useDemoData;
      state.algoMode = parsed.algoMode ?? state.algoMode;
      state.benchmark = parsed.benchmark ?? state.benchmark;
      state.stageRange = parsed.stageRange ?? state.stageRange;
      state.rangeDays = parsed.rangeDays ?? state.rangeDays;
      state.capitalAmount = Number(parsed.capitalAmount ?? 0);
      state.trades = Array.isArray(parsed.trades) ? parsed.trades.map(normalizeTrade) : [];
    } catch (error) {
      console.error("读取本地数据失败，已使用默认配置", error);
    }
  }
  if (state.useDemoData && state.trades.length === 0) {
    state.trades = demoTrades.map((item) => ({ ...item }));
  }
  if (state.trades.length === 0) {
    state.useDemoData = true;
    state.trades = demoTrades.map((item) => ({ ...item }));
  }
}

function persistState() {
  const payload = {
    route: state.route,
    useDemoData: state.useDemoData,
    algoMode: state.algoMode,
    benchmark: state.benchmark,
    stageRange: state.stageRange,
    rangeDays: state.rangeDays,
    capitalAmount: state.capitalAmount,
    trades: state.trades,
  };
  localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
}

function bindEvents() {
  routeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (state.route !== "stock-record") {
        state.previousRoute = state.route;
      }
      state.route = button.dataset.route;
      persistState();
      renderAll();
    });
  });

  if (demoToggleBtn) {
    demoToggleBtn.addEventListener("click", () => {
      state.useDemoData = !state.useDemoData;
      if (state.useDemoData) {
        state.trades = demoTrades.map((item) => ({ ...item }));
      }
      persistState();
      renderAll();
      refreshMarketData();
    });
  }

  algoModeSelect.addEventListener("change", () => {
    state.algoMode = algoModeSelect.value;
    persistState();
    renderOverviewAndStockTable();
    renderAnalysis();
  });

  benchmarkSelect.addEventListener("change", () => {
    state.benchmark = benchmarkSelect.value;
    persistState();
    renderAnalysis();
    refreshMarketData();
  });

  stageRangeSelect?.addEventListener("change", () => {
    state.stageRange = stageRangeSelect.value;
    persistState();
    renderOverviewAndStockTable();
  });

  rangeChips.forEach((chip) => {
    chip.addEventListener("click", () => {
      state.rangeDays = Number(chip.dataset.range);
      persistState();
      renderAnalysis();
      renderControls();
    });
  });

  [quickTradeBtn, recordTradeBtn].filter(Boolean).forEach((button) => {
    button.addEventListener("click", () => {
      clearEditState();
      tradeForm.reset();
      tradeTypeInput.value = "trade";
      applyTradeTypePreset();
      tradeDateInput.value = toDateKey(new Date());
      tradeDialog.showModal();
    });
  });

  closeTradeDialogBtn.addEventListener("click", () => {
    clearEditState();
    tradeDialog.close();
  });
  tradeTypeInput.addEventListener("change", applyTradeTypePreset);

  tradeForm.addEventListener("submit", (event) => {
    event.preventDefault();
    const formData = new FormData(tradeForm);
    const type = String(formData.get("type"));
    const symbol = normalizeSymbol(String(formData.get("symbol") || ""));
    const side = normalizedSide(type, String(formData.get("side")));
    const price = Number(formData.get("price") || 0);
    const quantity = Number(formData.get("quantity") || 0);
    const explicitAmount = Number(formData.get("amount"));
    const defaultAmount = Math.abs(price * quantity);
    const amount =
      Number.isFinite(explicitAmount) && explicitAmount !== 0
        ? Math.abs(explicitAmount)
        : Math.abs(defaultAmount);

    if (!symbol) {
      return;
    }

    const trade = normalizeTrade({
      id: state.editingTradeId || crypto.randomUUID(),
      type,
      symbol,
      name: String(formData.get("name") || symbol).trim(),
      side,
      price,
      quantity,
      amount,
      date: String(formData.get("date") || toDateKey(new Date())),
      note: String(formData.get("note") || "").trim(),
      createdAt: Date.now(),
    });

    state.useDemoData = false;
    if (state.editingTradeId) {
      state.trades = state.trades.filter((item) => item.id !== state.editingTradeId);
    }
    state.trades.push(trade);
    state.trades.sort(sortTradeAsc);
    persistState();
    clearEditState();
    tradeDialog.close();
    renderAll();
    refreshMarketData();
  });

  if (setCapitalBtn) {
    setCapitalBtn.addEventListener("click", () => {
      capitalAmountInput.value = state.capitalAmount ? String(state.capitalAmount) : "";
      capitalDialog.showModal();
    });
  }
  closeCapitalDialogBtn?.addEventListener("click", () => capitalDialog.close());
  if (capitalForm) {
    capitalForm.addEventListener("submit", (event) => {
      event.preventDefault();
      const formData = new FormData(capitalForm);
      state.capitalAmount = Math.max(0, Number(formData.get("capitalAmount") || 0));
      persistState();
      capitalDialog.close();
      renderOverviewAndStockTable();
    });
  }

  tradeTableBody?.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-record-id]");
    if (!row) {
      return;
    }
    const id = row.dataset.recordId;
    if (!id) {
      return;
    }
    showRecordActionPopover(row, id);
  });

  document.addEventListener("click", (event) => {
    if (!recordActionPopover) {
      return;
    }
    if (recordActionPopover.contains(event.target)) {
      return;
    }
    if (event.target.closest("tr[data-record-id]")) {
      return;
    }
    hideRecordActionPopover();
  });

  recordActionPopover?.addEventListener("click", (event) => {
    const actionBtn = event.target.closest("button[data-action]");
    if (!actionBtn) {
      return;
    }
    const action = actionBtn.dataset.action;
    const tradeId = recordActionPopover.dataset.tradeId;
    hideRecordActionPopover();
    if (!tradeId) {
      return;
    }
    if (action === "edit") {
      openEditTradeDialog(tradeId);
      return;
    }
    if (action === "delete") {
      removeTradeById(tradeId);
    }
  });

  stockTableBody?.addEventListener("click", (event) => {
    const link = event.target.closest("[data-stock-record]");
    if (!link) {
      return;
    }
    const symbol = link.dataset.stockRecord;
    openStockRecordDialog(symbol);
  });

  closeStockRecordDialogBtn?.addEventListener("click", () => {
    state.route = state.previousRoute || "earning";
    persistState();
    renderRoute();
  });
}

function applyTradeTypePreset() {
  const type = tradeTypeInput.value;
  if (type === "dividend") {
    tradeSideInput.value = "sell";
    tradePriceInput.value = "0";
    tradeQuantityInput.value = "0";
    tradeAmountInput.placeholder = "填写分红金额";
  } else if (type === "bonus" || type === "split") {
    tradeSideInput.value = "buy";
    tradePriceInput.value = "0";
    tradeAmountInput.value = "0";
    tradeAmountInput.placeholder = "默认为0";
  } else if (type === "merge") {
    tradeSideInput.value = "sell";
    tradePriceInput.value = "0";
    tradeAmountInput.value = "0";
    tradeAmountInput.placeholder = "默认为0";
  } else {
    tradeAmountInput.placeholder = "不填则默认价格*数量";
  }
}

function renderAll() {
  renderControls();
  renderRoute();
  renderOverviewAndStockTable();
  renderTradeTable();
  renderAnalysis();
  if (state.route === "stock-record" && state.activeRecordSymbol) {
    renderStockRecordPage(state.activeRecordSymbol);
  }
}

function renderControls() {
  if (demoToggleBtn) {
    demoToggleBtn.textContent = state.useDemoData ? "演示中" : "演示";
  }
  algoModeSelect.value = state.algoMode;
  benchmarkSelect.value = state.benchmark;
  if (quoteTime) {
    quoteTime.textContent = `行情更新时间：${state.quoteTime}`;
  }
  if (stageRangeSelect) {
    stageRangeSelect.value = state.stageRange;
  }
  rangeChips.forEach((chip) => {
    chip.classList.toggle("active", Number(chip.dataset.range) === state.rangeDays);
  });
}

function renderRoute() {
  if (!routePanes.some((pane) => pane.id === `route-${state.route}`)) {
    state.route = "earning";
  }
  routeButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.route === state.route);
  });
  routePanes.forEach((pane) => {
    pane.classList.toggle("active", pane.id === `route-${state.route}`);
  });
  const bottomTabs = document.querySelector(".bottom-tabs");
  if (bottomTabs) {
    bottomTabs.style.display = state.route === "stock-record" ? "none" : "grid";
  }
  if (state.route === "stock-record" && state.activeRecordSymbol) {
    renderStockRecordPage(state.activeRecordSymbol);
  }
}

function renderOverviewAndStockTable() {
  const portfolio = computePortfolio();
  const history = buildPortfolioHistory(portfolio.positions);
  const stagePerf = computeStagePerformance(history, state.stageRange, state.algoMode, portfolio);
  const cards = [
    { label: "总市值", value: formatPlainMoney(portfolio.totalMarketValue) },
    { label: "本金", value: formatPlainMoney(portfolio.principal) },
    { label: "总资产", value: formatPlainMoney(portfolio.totalAssets) },
    { label: "现金", value: formatPlainMoney(portfolio.cash) },
  ];
  todayProfitMain.innerHTML = metricValueWithRate(portfolio.todayProfit, portfolio.todayRate);
  todayProfitMain.className = `profit-main ${portfolio.todayProfit >= 0 ? "up" : "down"}`;
  monthProfitMain.innerHTML = metricValueWithRate(stagePerf.profit, stagePerf.rate);
  monthProfitMain.className = `profit-main ${stagePerf.profit >= 0 ? "up" : "down"}`;

  overviewGrid.innerHTML = cards
    .map(
      (item) => `
      <article class="kpi-item">
        <p class="kpi-label">${item.label}</p>
        <p class="kpi-value">${item.value}</p>
      </article>
    `
    )
    .join("");

  if (!portfolio.positions.length) {
    stockTableBody.innerHTML = `
      <tr>
        <td colspan="14"><p class="empty">暂无持仓，点击“记一笔”开始记录。</p></td>
      </tr>
    `;
    return;
  }

  stockTableBody.innerHTML = portfolio.positions
    .map((row) => {
      const stockCode = row.symbol.replace(/^(sh|sz|hk|gb_)/i, "").toUpperCase();
      const tag = row.market === "A股" ? "CN" : row.market === "港股" ? "HK" : row.market === "美股" ? "US" : "OT";
      const dayClass = row.todayProfit >= 0 ? "up" : "down";
      const changeClass = row.dayChangeRate >= 0 ? "up" : "down";
      const totalClass = row.totalProfit >= 0 ? "up" : "down";
      return `
        <tr>
          <td class="stock-name">
            <strong>${escapeHtml(row.name)}</strong>
            <span><i class="market-tag">${tag}</i> ${stockCode}</span>
          </td>
          <td class="${dayClass}">${formatSignedMoney(row.todayProfit, 2)}</td>
          <td>
            <div class="cell-main">${formatNumber(row.currentPrice, 3)}</div>
            <div class="cell-sub ${changeClass}">${formatPercent(row.dayChangeRate)}</div>
          </td>
          <td>
            <div class="cell-main">${formatPlainMoney(row.marketValue)}</div>
            <div class="cell-sub">${formatNumber(row.quantity, 0)}</div>
          </td>
          <td>${formatPercent(row.weight)}</td>
          <td>${formatNumber(row.cost, 3)}</td>
          <td class="${row.monthProfit >= 0 ? "up" : "down"}">${formatSignedMoney(row.monthProfit, 2)}</td>
          <td>${formatPercent(row.monthWeight)}</td>
          <td class="${row.yearProfit >= 0 ? "up" : "down"}">${formatSignedMoney(row.yearProfit, 2)}</td>
          <td>${formatPercent(row.yearWeight)}</td>
          <td class="${totalClass}">${formatSignedMoney(row.totalProfit, 2)}</td>
          <td class="${totalClass}">${formatPercent(row.totalRate)}</td>
          <td class="${row.regretRate >= 0 ? "up" : "down"}">${formatPercent(row.regretRate)}</td>
          <td><a href="javascript:void(0)" class="record-link" data-stock-record="${row.symbol}">记录</a></td>
        </tr>
      `;
    })
    .join("");
}

function computeStagePerformance(history, stageRange, algoMode, portfolio) {
  if (!history.length) {
    return { profit: 0, rate: 0 };
  }
  if (stageRange === "day") {
    return {
      profit: portfolio.todayProfit,
      rate: portfolio.todayRate,
    };
  }

  const startKey = getStageStartKey(stageRange, history[0].date);
  const points = history.filter((point) => point.date >= startKey);
  if (!points.length) {
    return { profit: 0, rate: 0 };
  }
  const startClose = points[0].value - points[0].flow;
  const stageFlow = points.reduce((sum, point) => sum + point.flow, 0);
  const profit = points[points.length - 1].value - startClose - stageFlow;
  const rate = computeModeSeries(points, algoMode).at(-1)?.rate ?? 0;
  return { profit, rate };
}

function getStageStartKey(stageRange, firstDate) {
  const today = new Date();
  const start = new Date(today);
  if (stageRange === "week") {
    start.setDate(today.getDate() - 6);
  } else if (stageRange === "month") {
    start.setDate(1);
  } else if (stageRange === "quarter") {
    start.setDate(today.getDate() - 89);
  } else if (stageRange === "ytd") {
    start.setMonth(0, 1);
  } else if (stageRange === "total" && firstDate) {
    return firstDate;
  }
  return toDateKey(start);
}

function computePositionStageProfit(position, stageRange) {
  const firstTradeDate = state.trades.length
    ? [...state.trades].sort(sortTradeAsc)[0].date
    : toDateKey(new Date());
  const startKey = getStageStartKey(stageRange, firstTradeDate);
  const symbolTrades = state.trades
    .filter((trade) => trade.symbol === position.symbol)
    .sort(sortTradeAsc);
  if (!symbolTrades.length) {
    return 0;
  }

  let startQuantity = 0;
  let stageFlow = 0;
  symbolTrades.forEach((trade) => {
    const deltaQty = trade.side === "buy" ? trade.quantity : -trade.quantity;
    if (trade.date < startKey) {
      startQuantity += deltaQty;
    } else {
      stageFlow += signedAmount(trade);
    }
  });

  const startClose = getSymbolCloseBeforeDate(position.symbol, startKey, position.prevClose);
  const startMarketValue = startQuantity * startClose;
  return position.marketValue - startMarketValue - stageFlow;
}

function getSymbolCloseBeforeDate(symbol, dateKey, fallbackPrice) {
  const kline = state.klineMap[symbol] || [];
  for (let i = kline.length - 1; i >= 0; i -= 1) {
    const item = kline[i];
    if (item.day < dateKey && Number.isFinite(Number(item.close))) {
      return Number(item.close);
    }
  }
  return validNumber(
    fallbackPrice,
    state.quoteMap[symbol]?.prevClose,
    state.quoteMap[symbol]?.current,
    0
  );
}

function renderTradeTable() {
  if (!tradeTableBody) {
    return;
  }
  if (!state.trades.length) {
    tradeTableBody.innerHTML = `
      <tr>
        <td colspan="6"><p class="empty">暂无交易记录，点击上方“记一笔”新增。</p></td>
      </tr>
    `;
    return;
  }
  const sorted = [...state.trades].sort(sortTradeDesc);
  tradeTableBody.innerHTML = sorted
    .map((trade) => {
      return `
        <tr class="trade-row" data-record-id="${trade.id}">
          <td>${trade.date.replace(/-/g, "/")}</td>
          <td>${escapeHtml(trade.name)}</td>
          <td class="type-cell">${trade.side === "buy" ? "买入" : "卖出"}</td>
          <td class="num">${formatNumber(trade.price, 2)}</td>
          <td class="num">${formatNumber(trade.quantity, 0)}</td>
          <td class="num ${trade.side === "buy" ? "down" : "up"}">${
            trade.side === "buy" ? "-" : "+"
          }${formatNumber(trade.amount, 2)}</td>
        </tr>
      `;
    })
    .join("");
}

function showRecordActionPopover(row, tradeId) {
  if (!recordActionPopover) {
    return;
  }
  const rect = row.getBoundingClientRect();
  recordActionPopover.dataset.tradeId = tradeId;
  recordActionPopover.style.top = `${window.scrollY + rect.bottom - 2}px`;
  recordActionPopover.style.left = `${window.scrollX + rect.right - 118}px`;
  recordActionPopover.classList.add("show");
}

function hideRecordActionPopover() {
  if (!recordActionPopover) {
    return;
  }
  recordActionPopover.classList.remove("show");
  recordActionPopover.dataset.tradeId = "";
}

function openEditTradeDialog(tradeId) {
  const trade = state.trades.find((item) => item.id === tradeId);
  if (!trade) {
    return;
  }
  state.editingTradeId = tradeId;
  if (tradeDialogTitle) {
    tradeDialogTitle.textContent = "修改交易";
  }
  if (tradeSubmitBtn) {
    tradeSubmitBtn.textContent = "保存修改";
  }
  tradeTypeInput.value = trade.type;
  tradeSymbolInput.value = trade.symbol;
  tradeNameInput.value = trade.name;
  tradeSideInput.value = trade.side;
  tradePriceInput.value = trade.price;
  tradeQuantityInput.value = trade.quantity;
  tradeAmountInput.value = trade.amount;
  tradeDateInput.value = trade.date;
  tradeNoteInput.value = trade.note || "";
  applyTradeTypePreset();
  tradeDialog.showModal();
}

function clearEditState() {
  state.editingTradeId = null;
  if (tradeDialogTitle) {
    tradeDialogTitle.textContent = "新建交易";
  }
  if (tradeSubmitBtn) {
    tradeSubmitBtn.textContent = "保存交易";
  }
}

function removeTradeById(tradeId) {
  state.trades = state.trades.filter((item) => item.id !== tradeId);
  if (state.trades.length === 0) {
    state.useDemoData = true;
    state.trades = demoTrades.map((item) => ({ ...item }));
  }
  persistState();
  renderAll();
  refreshMarketData();
}

function renderAnalysis() {
  const portfolio = computePortfolio();
  const history = buildPortfolioHistory(portfolio.positions);
  const selected = history.slice(-Math.min(Math.max(state.rangeDays, 2), history.length));
  const mySeries = computeModeSeries(selected, state.algoMode);
  const benchSeries = buildBenchmarkSeries(selected);
  drawLineChart(mySeries, benchSeries);

  const lastMy = mySeries.at(-1)?.rate ?? 0;
  const lastBench = benchSeries.at(-1)?.rate ?? 0;
  const excess = lastMy - lastBench;
  analysisSummary.textContent =
    state.benchmark === "none"
      ? `我的收益 ${formatPercent(lastMy)}`
      : `我的 ${formatPercent(lastMy)} / 基准 ${formatPercent(lastBench)} / 对比 ${formatPercent(excess)}`;
}

async function openStockRecordDialog(symbol) {
  state.activeRecordSymbol = symbol;
  state.previousRoute = state.route;
  state.route = "stock-record";
  renderAll();
  window.scrollTo(0, 0);
  persistState();

  await ensureSymbolData(symbol);
  renderStockRecordPage(symbol);
  // wait for layout settle on mobile after route switch
  window.setTimeout(() => renderStockRecordPage(symbol), 40);
}

function renderStockRecordPage(symbol) {
  const position = computePortfolio().positions.find((item) => item.symbol === symbol);
  if (!position) {
    return;
  }
  const symbolTrades = state.trades
    .filter((item) => item.symbol === symbol)
    .sort(sortTradeDesc);
  const quote = state.quoteMap[symbol] || {};
  const current = validNumber(quote.current, position.currentPrice);
  const prev = validNumber(quote.prevClose, position.prevClose, current);
  const change = prev > 0 ? (current - prev) / prev : 0;

  stockRecordTitle.textContent = `${position.name}(${symbol.toUpperCase()})`;
  stockRecordTime.textContent = quote.time || state.quoteTime || "--";
  stockRecordPrice.textContent = formatNumber(current, 3);
  stockRecordPrice.className = `stock-record-price ${change >= 0 ? "up" : "down"}`;
  stockRecordChange.textContent = `${formatSignedMoney(current - prev, 2)} ${formatPercent(change)}`;
  stockRecordChange.className = `stock-record-change ${change >= 0 ? "up" : "down"}`;
  stockRecordMarket.textContent = marketLabel(position.market);
  stockRecordRegret.textContent = `后悔率 ${formatPercent(position.regretRate)}`;
  stockRecordRegret.className = `${position.regretRate >= 0 ? "up" : "down"}`;

  stockRecordListBody.innerHTML = symbolTrades
    .map(
      (trade) => `
      <tr>
        <td>${trade.date.replace(/-/g, "/")}</td>
        <td>${trade.side === "buy" ? "买入" : "卖出"}</td>
        <td>${formatNumber(trade.price, 2)}</td>
        <td>${formatNumber(trade.quantity, 0)}</td>
        <td class="${trade.side === "buy" ? "down" : "up"}">${trade.side === "buy" ? "-" : "+"}${formatNumber(
          trade.amount,
          2
        )}</td>
      </tr>
    `
    )
    .join("");

  drawStockRecordChart(symbol, symbolTrades);
}

async function ensureSymbolData(symbol) {
  try {
    const quoteMap = await fetchRealtimeQuotes([symbol]);
    if (quoteMap[symbol]) {
      state.quoteMap[symbol] = quoteMap[symbol];
      state.quoteTime = quoteMap[symbol].time || state.quoteTime;
    }
  } catch (error) {
    console.error("加载个股实时行情失败", error);
  }
  if (!state.quoteMap[symbol] || !Number.isFinite(state.quoteMap[symbol].current)) {
    const latest = await fetchLatestQuoteFromMinuteK(symbol);
    if (latest) {
      state.quoteMap[symbol] = latest;
      state.quoteTime = latest.time || state.quoteTime;
    }
  }

  if (!supportsKline(symbol)) {
    return;
  }
  if (state.klineMap[symbol] && state.klineMap[symbol].length) {
    return;
  }
  try {
    const list = await fetchKlineData(symbol);
    if (list.length) {
      state.klineMap[symbol] = list;
    } else {
      state.klineMap[symbol] = buildFallbackKlineFromTrades(symbol);
    }
  } catch (error) {
    console.error("加载个股K线失败", error);
    if (!state.klineMap[symbol] || !state.klineMap[symbol].length) {
      state.klineMap[symbol] = buildFallbackKlineFromTrades(symbol);
    }
  }
}

function ensureSymbolPrefixForQuote(symbol) {
  if (/^sh600750$/i.test(symbol)) {
    return "sz300750";
  }
  return symbol;
}

function buildFallbackKlineFromTrades(symbol) {
  const symbolTrades = state.trades
    .filter((item) => item.symbol === symbol)
    .sort(sortTradeAsc);
  if (!symbolTrades.length) {
    return [];
  }
  const start = new Date(symbolTrades[0].date);
  const end = new Date();
  const closeSeed = validNumber(symbolTrades[symbolTrades.length - 1].price, 1);
  const rows = [];
  let cursor = new Date(start);
  let prev = closeSeed;
  while (cursor <= end && rows.length < CHART_FALLBACK_DAYS) {
    const day = toDateKey(cursor);
    const trade = symbolTrades.find((item) => item.date === day);
    const close = validNumber(trade?.price, prev);
    rows.push({
      day,
      open: close,
      high: close,
      low: close,
      close,
      volume: 0,
    });
    prev = close;
    cursor.setDate(cursor.getDate() + 1);
  }
  return rows;
}

function marketLabel(market) {
  if (market === "A股") {
    return "CN 沪深默认";
  }
  if (market === "港股") {
    return "HK 港股默认";
  }
  if (market === "美股") {
    return "US 美股默认";
  }
  return "OT 其他市场";
}

function drawStockRecordChart(symbol, symbolTrades) {
  const canvas = stockRecordChart;
  if (!canvas) {
    return;
  }
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);

  const kline = state.klineMap[symbol] || [];
  const points = kline.slice(-60);
  if (!points.length) {
    drawFallbackStockRecordChart(symbolTrades, ctx, width, height);
    return;
  }
  const closes = points.map((item) => Number(item.close));
  const maxClose = Math.max(...closes);
  const minClose = Math.min(...closes);
  const qtyByDay = {};
  let qty = 0;
  const sortedTrades = [...symbolTrades].sort(sortTradeAsc);
  sortedTrades.forEach((trade) => {
    qty += trade.side === "buy" ? trade.quantity : -trade.quantity;
    qtyByDay[trade.date] = qty;
  });
  let rollingQty = 0;
  const qtySeries = points.map((item) => {
    if (qtyByDay[item.day] != null) {
      rollingQty = qtyByDay[item.day];
    }
    return rollingQty;
  });
  const maxQty = Math.max(1, ...qtySeries.map((v) => Math.abs(v)));
  const mapX = (idx) => 20 + (idx / Math.max(points.length - 1, 1)) * (width - 40);
  const mapYPrice = (value) =>
    20 + ((maxClose - value) / Math.max(maxClose - minClose, 0.0001)) * (height - 60);
  const mapYQty = (value) => height - 20 - (value / maxQty) * (height - 60);

  ctx.strokeStyle = "#e8edf5";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const y = 20 + ((height - 40) / 4) * i;
    ctx.beginPath();
    ctx.moveTo(10, y);
    ctx.lineTo(width - 10, y);
    ctx.stroke();
  }

  ctx.fillStyle = "rgba(64, 145, 224, 0.20)";
  ctx.beginPath();
  points.forEach((item, index) => {
    const x = mapX(index);
    const y = mapYPrice(Number(item.close));
    if (index === 0) {
      ctx.moveTo(x, height - 20);
      ctx.lineTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.lineTo(mapX(points.length - 1), height - 20);
  ctx.closePath();
  ctx.fill();

  ctx.strokeStyle = "#4091e0";
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((item, index) => {
    const x = mapX(index);
    const y = mapYPrice(Number(item.close));
    if (index === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();

  ctx.strokeStyle = "#ff4d4f";
  ctx.lineWidth = 2;
  ctx.beginPath();
  qtySeries.forEach((value, index) => {
    const x = mapX(index);
    const y = mapYQty(value);
    if (index === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();

  const pointByDate = Object.fromEntries(points.map((item, idx) => [item.day, idx]));
  sortedTrades.forEach((trade) => {
    const idx = pointByDate[trade.date];
    if (idx == null) {
      return;
    }
    const x = mapX(idx);
    const y = mapYPrice(validNumber(trade.price, points[idx].close));
    ctx.fillStyle = trade.side === "buy" ? "#3b7bf6" : "#ffffff";
    ctx.strokeStyle = "#3b7bf6";
    ctx.beginPath();
    ctx.arc(x, y, 6, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
  });
}

function drawFallbackStockRecordChart(symbolTrades, ctx, width, height) {
  const sortedTrades = [...symbolTrades].sort(sortTradeAsc);
  if (!sortedTrades.length) {
    return;
  }
  const count = sortedTrades.length;
  const prices = sortedTrades.map((t) => validNumber(t.price, 0));
  const maxP = Math.max(...prices, 1);
  const minP = Math.min(...prices, 0);
  let q = 0;
  const qty = sortedTrades.map((t) => {
    q += t.side === "buy" ? t.quantity : -t.quantity;
    return q;
  });
  const maxQ = Math.max(...qty.map((v) => Math.abs(v)), 1);
  const mapX = (idx) => 20 + (idx / Math.max(count - 1, 1)) * (width - 40);
  const mapYPrice = (value) => 20 + ((maxP - value) / Math.max(maxP - minP, 0.001)) * (height - 60);
  const mapYQty = (value) => height - 20 - (value / maxQ) * (height - 60);

  ctx.strokeStyle = "#e8edf5";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const y = 20 + ((height - 40) / 4) * i;
    ctx.beginPath();
    ctx.moveTo(10, y);
    ctx.lineTo(width - 10, y);
    ctx.stroke();
  }

  ctx.strokeStyle = "#4091e0";
  ctx.lineWidth = 2;
  ctx.beginPath();
  sortedTrades.forEach((trade, index) => {
    const x = mapX(index);
    const y = mapYPrice(validNumber(trade.price, 0));
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();

  ctx.strokeStyle = "#ff4d4f";
  ctx.lineWidth = 2;
  ctx.beginPath();
  qty.forEach((value, index) => {
    const x = mapX(index);
    const y = mapYQty(value);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();

  sortedTrades.forEach((trade, index) => {
    const x = mapX(index);
    const y = mapYPrice(validNumber(trade.price, 0));
    ctx.fillStyle = trade.side === "buy" ? "#3b7bf6" : "#ffffff";
    ctx.strokeStyle = "#3b7bf6";
    ctx.beginPath();
    ctx.arc(x, y, 6, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
  });
}

function computePortfolio() {
  const grouped = new Map();
  const sortedTrades = [...state.trades].sort(sortTradeAsc);

  for (const trade of sortedTrades) {
    if (!grouped.has(trade.symbol)) {
      grouped.set(trade.symbol, {
        symbol: trade.symbol,
        name: trade.name || trade.symbol,
        market: inferMarket(trade.symbol),
        quantity: 0,
        sigmaAmount: 0,
        lastTradePrice: trade.price,
        lastTradeSide: trade.side,
        lastTradeDate: trade.date,
      });
    }
    const item = grouped.get(trade.symbol);
    item.name = trade.name || item.name;
    item.market = inferMarket(trade.symbol);
    item.quantity += trade.side === "buy" ? trade.quantity : -trade.quantity;
    item.sigmaAmount += signedAmount(trade);
    if (trade.price > 0) {
      item.lastTradePrice = trade.price;
    }
    item.lastTradeSide = trade.side;
    item.lastTradeDate = trade.date;
  }

  const positions = [...grouped.values()].map((item) => {
    const quote = state.quoteMap[item.symbol] || {};
    const currentPrice = validNumber(quote.current, item.lastTradePrice);
    const prevClose = validNumber(quote.prevClose, currentPrice);
    const marketValue = item.quantity * currentPrice;
    const yesterdayValue = item.quantity * prevClose;
    const cost = item.quantity !== 0 ? item.sigmaAmount / item.quantity : 0;
    const totalProfit = marketValue - item.sigmaAmount;
    const profitRate =
      Math.abs(item.sigmaAmount) > 0 ? totalProfit / Math.abs(item.sigmaAmount) : 0;
    const todayFlowForSymbol = state.trades
      .filter((trade) => trade.symbol === item.symbol && trade.date === toDateKey(new Date()))
      .reduce((sum, trade) => sum + signedAmount(trade), 0);
    const todayProfit = marketValue - yesterdayValue - todayFlowForSymbol;
    const dayChangeRate = prevClose > 0 ? (currentPrice - prevClose) / prevClose : 0;
    const regretRate =
      item.lastTradePrice > 0 ? (currentPrice - item.lastTradePrice) / item.lastTradePrice : 0;
    return {
      ...item,
      currentPrice,
      prevClose,
      marketValue,
      yesterdayValue,
      cost,
      totalProfit,
      profitRate,
      todayProfit,
      dayChangeRate,
      regretRate,
      totalRate: profitRate,
    };
  });

  positions.forEach((item) => {
    item.monthProfit = computePositionStageProfit(item, "month");
    item.yearProfit = computePositionStageProfit(item, "ytd");
  });
  const monthTotalProfit = positions.reduce((sum, item) => sum + item.monthProfit, 0);
  const yearTotalProfit = positions.reduce((sum, item) => sum + item.yearProfit, 0);
  positions.forEach((item) => {
    item.monthWeight = monthTotalProfit !== 0 ? item.monthProfit / monthTotalProfit : 0;
    item.yearWeight = yearTotalProfit !== 0 ? item.yearProfit / yearTotalProfit : 0;
  });

  const sigmaAmountAll = state.trades.reduce((sum, trade) => sum + signedAmount(trade), 0);
  const principal = Math.max(state.capitalAmount, sigmaAmountAll, 0);
  const totalMarketValue = positions.reduce((sum, item) => sum + item.marketValue, 0);
  const yesterdayMarketValue = positions.reduce((sum, item) => sum + item.yesterdayValue, 0);
  const cash = principal - sigmaAmountAll;
  const totalAssets = totalMarketValue + cash;
  const todayFlow = state.trades
    .filter((trade) => trade.date === toDateKey(new Date()))
    .reduce((sum, trade) => sum + signedAmount(trade), 0);
  const todayProfit = totalMarketValue - yesterdayMarketValue - todayFlow;
  const todayRateDen = yesterdayMarketValue + Math.max(todayFlow, 0);
  const todayRate = todayRateDen !== 0 ? todayProfit / todayRateDen : 0;
  const totalProfit = totalMarketValue - sigmaAmountAll;

  positions.forEach((item) => {
    item.weight = totalAssets !== 0 ? item.marketValue / totalAssets : 0;
  });
  positions.sort((a, b) => Math.abs(b.marketValue) - Math.abs(a.marketValue));

  return {
    positions,
    sigmaAmountAll,
    principal,
    totalMarketValue,
    yesterdayMarketValue,
    cash,
    totalAssets,
    todayFlow,
    todayProfit,
    todayRate,
    totalProfit,
  };
}

function buildPortfolioHistory(positions) {
  const end = new Date();
  const start = new Date(end);
  start.setDate(end.getDate() - 370);

  const dateKeys = [];
  const cursor = new Date(start);
  while (cursor <= end) {
    dateKeys.push(toDateKey(cursor));
    cursor.setDate(cursor.getDate() + 1);
  }

  const symbolSet = new Set(positions.map((item) => item.symbol));
  const klineMap = {};
  const lastPriceMap = {};

  symbolSet.forEach((symbol) => {
    const list = state.klineMap[symbol] || [];
    klineMap[symbol] = Object.fromEntries(list.map((item) => [item.day, Number(item.close)]));
    const fallbackTrade = state.trades.find((item) => item.symbol === symbol);
    lastPriceMap[symbol] = validNumber(state.quoteMap[symbol]?.prevClose, fallbackTrade?.price, 0);
  });

  const tradesByDate = {};
  for (const trade of state.trades) {
    if (!tradesByDate[trade.date]) {
      tradesByDate[trade.date] = [];
    }
    tradesByDate[trade.date].push(trade);
  }
  Object.values(tradesByDate).forEach((list) => list.sort((a, b) => a.createdAt - b.createdAt));

  const holdings = {};
  const points = [];
  const todayKey = toDateKey(new Date());

  for (const dateKey of dateKeys) {
    const dailyTrades = tradesByDate[dateKey] || [];
    for (const trade of dailyTrades) {
      if (holdings[trade.symbol] == null) {
        holdings[trade.symbol] = 0;
      }
      holdings[trade.symbol] += trade.side === "buy" ? trade.quantity : -trade.quantity;
    }

    let value = 0;
    let flow = 0;
    for (const trade of dailyTrades) {
      flow += signedAmount(trade);
    }
    for (const symbol of symbolSet) {
      const dayClose = klineMap[symbol][dateKey];
      if (Number.isFinite(dayClose) && dayClose > 0) {
        lastPriceMap[symbol] = dayClose;
      } else if (dateKey === todayKey && validNumber(state.quoteMap[symbol]?.current, 0) > 0) {
        lastPriceMap[symbol] = Number(state.quoteMap[symbol].current);
      }
      value += (holdings[symbol] || 0) * (lastPriceMap[symbol] || 0);
    }
    points.push({ date: dateKey, value, flow });
  }
  return points;
}

function computeModeSeries(historyPoints, mode) {
  if (!historyPoints.length) {
    return [{ date: toDateKey(new Date()), rate: 0 }];
  }
  if (mode === "time") {
    return computeTimeWeightedSeries(historyPoints);
  }
  if (mode === "money") {
    return computeMoneyWeightedSeries(historyPoints);
  }
  return computeCostSeries(historyPoints);
}

function computeCostSeries(points) {
  const result = [];
  const startClose = points[0].value - points[0].flow;
  let sumFlow = 0;
  points.forEach((point) => {
    sumFlow += point.flow;
    const profit = point.value - startClose - sumFlow;
    const denominator = startClose + sumFlow;
    const rate = denominator !== 0 ? profit / denominator : 0;
    result.push({ date: point.date, rate });
  });
  return result;
}

function computeMoneyWeightedSeries(points) {
  const result = [];
  const startClose = points[0].value - points[0].flow;
  const flows = [];
  points.forEach((point, index) => {
    flows.push(point.flow);
    const totalPeriods = index + 1;
    let weightedFlow = 0;
    let sumFlow = 0;
    flows.forEach((flow, flowIdx) => {
      const weight = (totalPeriods - flowIdx) / totalPeriods;
      weightedFlow += flow * weight;
      sumFlow += flow;
    });
    const profit = point.value - startClose - sumFlow;
    const denominator = startClose + weightedFlow;
    const rate = denominator !== 0 ? profit / denominator : 0;
    result.push({ date: point.date, rate });
  });
  return result;
}

function computeTimeWeightedSeries(points) {
  const result = [];
  let compounded = 1;
  let prevValue = points[0].value - points[0].flow;
  points.forEach((point) => {
    const denominator = prevValue + Math.max(point.flow, 0);
    const dailyRate = denominator !== 0 ? (point.value - prevValue - point.flow) / denominator : 0;
    compounded *= 1 + dailyRate;
    result.push({ date: point.date, rate: compounded - 1 });
    prevValue = point.value;
  });
  return result;
}

function buildBenchmarkSeries(selectedPoints) {
  if (state.benchmark === "none") {
    return selectedPoints.map((point) => ({ date: point.date, rate: 0 }));
  }

  const symbol = state.benchmark;
  const kline = state.klineMap[symbol] || [];
  if (kline.length) {
    const byDate = Object.fromEntries(kline.map((item) => [item.day, Number(item.close)]));
    let lastPrice = validNumber(kline[0]?.close, DEFAULT_BENCHMARK_PRICE[symbol], 1);
    let base = 0;
    return selectedPoints.map((point, idx) => {
      if (Number.isFinite(byDate[point.date])) {
        lastPrice = Number(byDate[point.date]);
      } else if (idx === selectedPoints.length - 1 && validNumber(state.quoteMap[symbol]?.current, 0) > 0) {
        lastPrice = Number(state.quoteMap[symbol].current);
      }
      if (idx === 0) {
        base = lastPrice || 1;
      }
      const rate = base ? (lastPrice - base) / base : 0;
      return { date: point.date, rate };
    });
  }

  const quote = state.quoteMap[symbol];
  const fallbackRate =
    quote && validNumber(quote.prevClose, 0) > 0
      ? (validNumber(quote.current, quote.prevClose) - quote.prevClose) / quote.prevClose
      : 0;
  const len = selectedPoints.length;
  return selectedPoints.map((point, index) => ({
    date: point.date,
    rate: len > 1 ? (fallbackRate * index) / (len - 1) : fallbackRate,
  }));
}

function drawLineChart(mySeries, benchmarkSeries) {
  const ctx = analysisChart.getContext("2d");
  const width = analysisChart.width;
  const height = analysisChart.height;
  ctx.clearRect(0, 0, width, height);

  ctx.strokeStyle = "#e6ebf2";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const y = (height / 4) * i;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(width, y);
    ctx.stroke();
  }

  const mergedRates = [
    ...mySeries.map((point) => Math.abs(point.rate)),
    ...benchmarkSeries.map((point) => Math.abs(point.rate)),
    0.02,
  ];
  const maxAbs = Math.max(...mergedRates);
  const count = Math.max(mySeries.length, 2);
  const mapX = (idx) => (idx / (count - 1)) * (width - 40) + 20;
  const mapY = (rate) => height / 2 - (rate / maxAbs) * (height * 0.35);

  drawSeries(ctx, mySeries, mapX, mapY, "#f24957");
  if (state.benchmark !== "none") {
    drawSeries(ctx, benchmarkSeries, mapX, mapY, "#2f80f6");
  }
}

function drawSeries(ctx, series, mapX, mapY, color) {
  if (!series.length) {
    return;
  }
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  series.forEach((point, index) => {
    const x = mapX(index);
    const y = mapY(point.rate);
    if (index === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();
}

async function refreshMarketData() {
  if (state.marketLoading) {
    return;
  }
  state.marketLoading = true;

  try {
    const symbols = collectSymbolsForMarket();
    if (!symbols.length) {
      state.marketLoading = false;
      return;
    }

    let quoteMap = {};
    try {
      quoteMap = await fetchRealtimeQuotes(symbols);
    } catch (error) {
      // hq.sinajs.cn may be blocked by anti-hotlink on third-party domains.
      quoteMap = {};
    }
    if (Object.keys(quoteMap).length) {
      state.quoteMap = { ...state.quoteMap, ...quoteMap };
      const times = Object.values(quoteMap)
        .map((item) => item.time)
        .filter(Boolean);
      state.quoteTime = times[0] || state.quoteTime;
    }

    const klineSymbols = symbols.filter(supportsKline);
    await Promise.all(
      klineSymbols.map(async (symbol) => {
        const needDaily = !state.klineMap[symbol] || !state.klineMap[symbol].length;
        if (needDaily) {
          const list = await fetchKlineData(symbol);
          if (list.length) {
            state.klineMap[symbol] = list;
          }
        }
        // Fallback "realtime": use minute-kline last point when realtime endpoint is blocked.
        if (!state.quoteMap[symbol] || !Number.isFinite(state.quoteMap[symbol].current)) {
          const latest = await fetchLatestQuoteFromMinuteK(symbol);
          if (latest) {
            state.quoteMap[symbol] = latest;
          }
        }
      })
    );
  } catch (error) {
    console.error("行情拉取失败，保留本地数据展示", error);
  } finally {
    state.marketLoading = false;
    renderAll();
  }
}

async function fetchLatestQuoteFromMinuteK(symbol) {
  try {
    const list = await fetchMinuteKData(symbol, 5, 2);
    if (!list.length) {
      return null;
    }
    const last = list[list.length - 1];
    const prev = list.length > 1 ? list[list.length - 2] : last;
    return {
      name: symbol,
      current: Number(last.close),
      prevClose: Number(prev.close || last.open || last.close),
      time: String(last.day || "--"),
    };
  } catch (error) {
    return null;
  }
}

async function fetchRealtimeQuotes(symbols) {
  const uniqSymbols = [...new Set(symbols)];
  const url = `https://hq.sinajs.cn/rn=${Date.now()}&list=${uniqSymbols.join(",")}`;
  await loadScript(url, "gbk");
  const parsed = {};

  uniqSymbols.forEach((symbol) => {
    const raw = window[`hq_str_${symbol}`];
    const record = parseQuoteRecord(symbol, raw);
    if (record) {
      parsed[symbol] = record;
    }
  });
  return parsed;
}

async function fetchKlineData(symbol) {
  const variableName = `__kline_${symbol.replace(/[^a-zA-Z0-9_]/g, "_")}_${Date.now()}`;
  const query = `https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20${variableName}=/CN_MarketDataService.getKLineData?symbol=${encodeURIComponent(
    symbol
  )}&scale=240&ma=no&datalen=${KLINE_DATALEN}`;
  await loadScript(query, "utf-8");
  const data = window[variableName];
  delete window[variableName];
  if (!Array.isArray(data)) {
    return [];
  }
  return data
    .map((item) => ({
      day: item.day,
      open: Number(item.open),
      high: Number(item.high),
      low: Number(item.low),
      close: Number(item.close),
      volume: Number(item.volume),
    }))
    .filter((item) => item.day && Number.isFinite(item.close));
}

async function fetchMinuteKData(symbol, scale = 5, datalen = 2) {
  const variableName = `__minute_${symbol.replace(/[^a-zA-Z0-9_]/g, "_")}_${Date.now()}`;
  const query = `https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20${variableName}=/CN_MarketDataService.getKLineData?symbol=${encodeURIComponent(
    symbol
  )}&scale=${scale}&ma=no&datalen=${datalen}`;
  await loadScript(query, "utf-8");
  const data = window[variableName];
  delete window[variableName];
  if (!Array.isArray(data)) {
    return [];
  }
  return data
    .map((item) => ({
      day: item.day,
      open: Number(item.open),
      high: Number(item.high),
      low: Number(item.low),
      close: Number(item.close),
      volume: Number(item.volume),
    }))
    .filter((item) => item.day && Number.isFinite(item.close));
}

function parseQuoteRecord(symbol, rawText) {
  if (!rawText || typeof rawText !== "string") {
    return null;
  }
  const parts = rawText.split(",");
  if (!parts.length) {
    return null;
  }

  if (symbol.startsWith("gb_")) {
    const current = Number(parts[1]);
    const prevClose = Number(parts[8]);
    return {
      name: parts[0] || symbol,
      current: Number.isFinite(current) ? current : 0,
      prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
      time: parts[3] || "--",
    };
  }

  if (symbol.startsWith("hk") || symbol.startsWith("rt_hk")) {
    const current = Number(parts[6] || parts[2]);
    const prevClose = Number(parts[3] || parts[2]);
    return {
      name: parts[1] || parts[0] || symbol,
      current: Number.isFinite(current) ? current : 0,
      prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
      time: parts[17] || "--",
    };
  }

  const current = Number(parts[3]);
  const prevClose = Number(parts[2]);
  const dateText = parts[30] || "";
  const timeText = parts[31] || "";
  return {
    name: parts[0] || symbol,
    current: Number.isFinite(current) ? current : 0,
    prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : current,
    time: `${dateText} ${timeText}`.trim() || "--",
  };
}

function loadScript(src, charset = "utf-8") {
  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = src;
    script.charset = charset;
    script.async = true;
    const timer = window.setTimeout(() => {
      script.remove();
      reject(new Error(`加载超时: ${src}`));
    }, 12_000);

    script.onload = () => {
      window.clearTimeout(timer);
      script.remove();
      resolve();
    };
    script.onerror = () => {
      window.clearTimeout(timer);
      script.remove();
      reject(new Error(`加载失败: ${src}`));
    };
    document.head.appendChild(script);
  });
}

function collectSymbolsForMarket() {
  const fromTrades = state.trades.map((item) => ensureSymbolPrefixForQuote(item.symbol));
  if (state.benchmark !== "none") {
    fromTrades.push(state.benchmark);
  }
  if (!fromTrades.length) {
    fromTrades.push("sz300750", "sh601899", "sh000001", "sz399001");
  }
  return [...new Set(fromTrades)];
}

function supportsKline(symbol) {
  return /^(sh|sz)\d{6}$/i.test(symbol);
}

function normalizeTrade(input) {
  const trade = { ...input };
  trade.symbol = normalizeSymbol(trade.symbol || "");
  trade.type = trade.type || "trade";
  trade.side = normalizedSide(trade.type, trade.side || "buy");
  trade.price = Number(trade.price || 0);
  trade.quantity = Number(trade.quantity || 0);
  if (trade.type === "dividend") {
    trade.price = 0;
    trade.quantity = 0;
  }
  if (trade.type === "bonus" || trade.type === "split" || trade.type === "merge") {
    trade.price = 0;
  }
  const defaultAmount = Math.abs(trade.price * trade.quantity);
  trade.amount = Math.abs(Number.isFinite(Number(trade.amount)) ? Number(trade.amount) : defaultAmount);
  trade.date = trade.date || toDateKey(new Date());
  trade.note = trade.note || "";
  trade.name = trade.name || trade.symbol;
  trade.createdAt = Number(trade.createdAt || Date.now());
  return trade;
}

function normalizedSide(type, side) {
  if (type === "dividend" || type === "merge") {
    return "sell";
  }
  if (type === "bonus" || type === "split") {
    return "buy";
  }
  return side === "sell" ? "sell" : "buy";
}

function signedAmount(trade) {
  return trade.side === "buy" ? trade.amount : -trade.amount;
}

function normalizeSymbol(rawSymbol) {
  const value = String(rawSymbol || "")
    .trim()
    .replace(/\s+/g, "")
    .toLowerCase();
  if (!value) {
    return "";
  }
  if (value.startsWith("sh") || value.startsWith("sz") || value.startsWith("hk")) {
    return value;
  }
  if (value.startsWith("rt_hk") || value.startsWith("gb_")) {
    return value;
  }
  if (/^\d{6}$/.test(value)) {
    if (["3"].includes(value[0])) {
      return `sz${value}`;
    }
    if (["5", "6", "9"].includes(value[0])) {
      return `sh${value}`;
    }
    return `sz${value}`;
  }
  if (/^\d{5}$/.test(value)) {
    return `hk${value}`;
  }
  return value;
}

function inferMarket(symbol) {
  if (symbol.startsWith("sh") || symbol.startsWith("sz")) {
    return "A股";
  }
  if (symbol.startsWith("hk") || symbol.startsWith("rt_hk")) {
    return "港股";
  }
  if (symbol.startsWith("gb_")) {
    return "美股";
  }
  return "其他";
}

function typeLabel(type) {
  if (type === "dividend") return "分红";
  if (type === "bonus") return "送股";
  if (type === "split") return "拆股";
  if (type === "merge") return "合股";
  return "买入卖出";
}

function toDateKey(date) {
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function sortTradeAsc(a, b) {
  const ad = new Date(a.date).getTime();
  const bd = new Date(b.date).getTime();
  if (ad !== bd) {
    return ad - bd;
  }
  return Number(a.createdAt) - Number(b.createdAt);
}

function sortTradeDesc(a, b) {
  return -sortTradeAsc(a, b);
}

function validNumber(...values) {
  for (const value of values) {
    if (Number.isFinite(Number(value))) {
      return Number(value);
    }
  }
  return 0;
}

function formatCurrency(value) {
  const sign = value < 0 ? "-" : "";
  const abs = Math.abs(value);
  return `${sign}¥${abs.toLocaleString("zh-CN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function formatPlainMoney(value) {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  return safe.toFixed(2);
}

function formatSignedMoney(value, fraction = 2) {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  const abs = Math.abs(safe).toFixed(fraction);
  const sign = safe > 0 ? "+" : safe < 0 ? "-" : "";
  return `${sign}${abs}`;
}

function formatNumber(value, fraction = 2) {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  return safe.toLocaleString("zh-CN", {
    minimumFractionDigits: fraction,
    maximumFractionDigits: fraction,
  });
}

function formatPercent(value) {
  const safe = Number.isFinite(Number(value)) ? Number(value) : 0;
  const num = (safe * 100).toFixed(2);
  return `${safe >= 0 ? "+" : ""}${num}%`;
}

function metricValueWithRate(amount, rate) {
  const amountText = formatSignedMoney(amount, 2);
  const rateText = formatPercent(rate);
  return `${amountText}<span class="profit-rate-inline">${rateText}</span>`;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
