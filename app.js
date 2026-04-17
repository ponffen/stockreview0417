const STORAGE_KEY = "earning-clone-state-v2";
const QUOTE_REFRESH_MS = 60_000;
const KLINE_DATALEN = 420;
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
    symbol: "sh600519",
    name: "贵州茅台",
    side: "buy",
    price: 1480,
    quantity: 100,
    amount: 148000,
    date: "2026-01-10",
    note: "长线配置",
    createdAt: Date.now() - 5,
  },
  {
    id: crypto.randomUUID(),
    type: "trade",
    symbol: "sz000858",
    name: "五粮液",
    side: "buy",
    price: 129.5,
    quantity: 500,
    amount: 64750,
    date: "2026-02-14",
    note: "",
    createdAt: Date.now() - 4,
  },
  {
    id: crypto.randomUUID(),
    type: "trade",
    symbol: "hk00700",
    name: "腾讯控股",
    side: "buy",
    price: 310,
    quantity: 200,
    amount: 62000,
    date: "2026-03-02",
    note: "",
    createdAt: Date.now() - 3,
  },
  {
    id: crypto.randomUUID(),
    type: "trade",
    symbol: "hk00700",
    name: "腾讯控股",
    side: "sell",
    price: 336,
    quantity: 60,
    amount: 20160,
    date: "2026-04-03",
    note: "减仓",
    createdAt: Date.now() - 2,
  },
];

const state = {
  route: "earning",
  useDemoData: true,
  algoMode: "cost",
  benchmark: "none",
  rangeDays: 7,
  capitalAmount: 0,
  trades: [],
  quoteMap: {},
  klineMap: {},
  quoteTime: "--",
  marketLoading: false,
};

const routeButtons = [...document.querySelectorAll(".route-btn")];
const routePanes = [...document.querySelectorAll(".route-pane")];
const overviewGrid = document.getElementById("overviewGrid");
const quoteTime = document.getElementById("quoteTime");
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
const tradeDialog = document.getElementById("tradeDialog");
const tradeForm = document.getElementById("tradeForm");
const closeTradeDialogBtn = document.getElementById("closeTradeDialogBtn");
const tradeTypeInput = document.getElementById("tradeType");
const tradePriceInput = document.getElementById("tradePrice");
const tradeQuantityInput = document.getElementById("tradeQuantity");
const tradeSideInput = document.getElementById("tradeSide");
const tradeAmountInput = document.getElementById("tradeAmount");
const capitalDialog = document.getElementById("capitalDialog");
const capitalForm = document.getElementById("capitalForm");
const closeCapitalDialogBtn = document.getElementById("closeCapitalDialogBtn");
const capitalAmountInput = document.getElementById("capitalAmount");

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
      state.useDemoData = parsed.useDemoData ?? state.useDemoData;
      state.algoMode = parsed.algoMode ?? state.algoMode;
      state.benchmark = parsed.benchmark ?? state.benchmark;
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
}

function persistState() {
  const payload = {
    route: state.route,
    useDemoData: state.useDemoData,
    algoMode: state.algoMode,
    benchmark: state.benchmark,
    rangeDays: state.rangeDays,
    capitalAmount: state.capitalAmount,
    trades: state.trades,
  };
  localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
}

function bindEvents() {
  routeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      state.route = button.dataset.route;
      persistState();
      renderRoute();
    });
  });

  demoToggleBtn.addEventListener("click", () => {
    state.useDemoData = !state.useDemoData;
    state.trades = state.useDemoData ? demoTrades.map((item) => ({ ...item })) : [];
    persistState();
    renderAll();
    refreshMarketData();
  });

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

  rangeChips.forEach((chip) => {
    chip.addEventListener("click", () => {
      state.rangeDays = Number(chip.dataset.range);
      persistState();
      renderAnalysis();
      renderControls();
    });
  });

  [quickTradeBtn, recordTradeBtn].forEach((button) => {
    button.addEventListener("click", () => {
      tradeForm.reset();
      tradeTypeInput.value = "trade";
      applyTradeTypePreset();
      document.getElementById("tradeDate").value = toDateKey(new Date());
      tradeDialog.showModal();
    });
  });

  closeTradeDialogBtn.addEventListener("click", () => tradeDialog.close());
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
      id: crypto.randomUUID(),
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
    state.trades.push(trade);
    state.trades.sort(sortTradeAsc);
    persistState();
    tradeDialog.close();
    renderAll();
    refreshMarketData();
  });

  setCapitalBtn.addEventListener("click", () => {
    capitalAmountInput.value = state.capitalAmount ? String(state.capitalAmount) : "";
    capitalDialog.showModal();
  });
  closeCapitalDialogBtn.addEventListener("click", () => capitalDialog.close());
  capitalForm.addEventListener("submit", (event) => {
    event.preventDefault();
    const formData = new FormData(capitalForm);
    state.capitalAmount = Math.max(0, Number(formData.get("capitalAmount") || 0));
    persistState();
    capitalDialog.close();
    renderOverviewAndStockTable();
  });

  recordList.addEventListener("click", (event) => {
    const button = event.target.closest("[data-remove-id]");
    if (!button) {
      return;
    }
    state.trades = state.trades.filter((item) => item.id !== button.dataset.removeId);
    persistState();
    renderAll();
    refreshMarketData();
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
  renderRecords();
  renderAnalysis();
}

function renderControls() {
  demoToggleBtn.textContent = state.useDemoData ? "开始使用" : "查看演示数据";
  algoModeSelect.value = state.algoMode;
  benchmarkSelect.value = state.benchmark;
  quoteTime.textContent = `行情更新时间：${state.quoteTime}`;
  rangeChips.forEach((chip) => {
    chip.classList.toggle("active", Number(chip.dataset.range) === state.rangeDays);
  });
}

function renderRoute() {
  routeButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.route === state.route);
  });
  routePanes.forEach((pane) => {
    pane.classList.toggle("active", pane.id === `route-${state.route}`);
  });
}

function renderOverviewAndStockTable() {
  const portfolio = computePortfolio();
  const history = buildPortfolioHistory(portfolio.positions);
  const totalRate = computeModeSeries(history, state.algoMode).at(-1)?.rate ?? 0;
  const cards = [
    { label: "总资产", value: formatCurrency(portfolio.totalAssets) },
    { label: "总市值", value: formatCurrency(portfolio.totalMarketValue) },
    { label: "现金", value: formatCurrency(portfolio.cash) },
    { label: "本金", value: formatCurrency(portfolio.principal) },
    {
      label: "总收益",
      value: `${formatCurrency(portfolio.totalProfit)} (${formatPercent(totalRate)})`,
      className: portfolio.totalProfit >= 0 ? "up" : "down",
    },
    {
      label: "今日收益",
      value: `${formatCurrency(portfolio.todayProfit)} (${formatPercent(portfolio.todayRate)})`,
      className: portfolio.todayProfit >= 0 ? "up" : "down",
    },
  ];

  overviewGrid.innerHTML = cards
    .map(
      (item) => `
      <article class="kpi-item">
        <p class="kpi-label">${item.label}</p>
        <p class="kpi-value ${item.className || ""}">${item.value}</p>
      </article>
    `
    )
    .join("");

  if (!portfolio.positions.length) {
    stockTableBody.innerHTML = `
      <tr>
        <td colspan="14"><p class="empty">暂无持仓，点击“新建交易”开始记录。</p></td>
      </tr>
    `;
    return;
  }

  stockTableBody.innerHTML = portfolio.positions
    .map((row) => {
      const profitClass = row.totalProfit >= 0 ? "up" : "down";
      return `
        <tr>
          <td class="stock-name"><strong>${escapeHtml(row.name)}</strong><span>${row.symbol}</span></td>
          <td>${row.market}</td>
          <td>${formatNumber(row.quantity, 0)}</td>
          <td>${formatNumber(row.currentPrice, 3)}</td>
          <td>${formatNumber(row.prevClose, 3)}</td>
          <td>${formatCurrency(row.marketValue)}</td>
          <td>${formatCurrency(row.sigmaAmount)}</td>
          <td>${formatNumber(row.cost, 3)}</td>
          <td class="${profitClass}">${formatCurrency(row.totalProfit)}</td>
          <td class="${profitClass}">${formatPercent(row.profitRate)}</td>
          <td>${formatPercent(row.weight)}</td>
          <td class="${row.regretRate >= 0 ? "up" : "down"}">${formatPercent(row.regretRate)}</td>
          <td>${formatNumber(row.lastTradePrice, 3)}</td>
          <td>${row.lastTradeSide === "buy" ? "B" : "S"}</td>
        </tr>
      `;
    })
    .join("");
}

function renderRecords() {
  if (!state.trades.length) {
    recordList.innerHTML = '<p class="empty">暂无交易记录，点击右上角“新建交易”。</p>';
    return;
  }
  const sorted = [...state.trades].sort(sortTradeDesc);
  recordList.innerHTML = sorted
    .map((trade) => {
      const amountText = `${trade.side === "buy" ? "+" : "-"}${formatCurrency(trade.amount)}`;
      return `
        <article class="record-item">
          <div class="record-main">
            <h3>${escapeHtml(trade.name)} <span class="caption">${trade.symbol}</span></h3>
            <p>${trade.date} · ${typeLabel(trade.type)} · ${trade.note || "无备注"}</p>
          </div>
          <div class="record-side">
            <p class="${trade.side === "buy" ? "up" : "down"}">${trade.side === "buy" ? "买入(B)" : "卖出(S)"}</p>
            <p>价格 ${formatNumber(trade.price, 3)} × 数量 ${formatNumber(trade.quantity, 0)}</p>
            <p>${amountText}</p>
            <button class="delete-btn" data-remove-id="${trade.id}">删除</button>
          </div>
        </article>
      `;
    })
    .join("");
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
      regretRate,
    };
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

    const quoteMap = await fetchRealtimeQuotes(symbols);
    if (Object.keys(quoteMap).length) {
      state.quoteMap = { ...state.quoteMap, ...quoteMap };
      const times = Object.values(quoteMap)
        .map((item) => item.time)
        .filter(Boolean);
      state.quoteTime = times[0] || state.quoteTime;
    }

    const klineSymbols = symbols.filter(supportsKline).filter((symbol) => !state.klineMap[symbol]);
    await Promise.all(
      klineSymbols.map(async (symbol) => {
        const list = await fetchKlineData(symbol);
        if (list.length) {
          state.klineMap[symbol] = list;
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
  const fromTrades = state.trades.map((item) => item.symbol);
  if (state.benchmark !== "none") {
    fromTrades.push(state.benchmark);
  }
  if (!fromTrades.length) {
    fromTrades.push("sh600519", "sz000858", "hk00700", "sh000001", "sz399001");
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

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
