const STORAGE_KEY = "earning-clone-state-v1";

const demoTrades = [
  {
    id: crypto.randomUUID(),
    symbol: "600519",
    name: "贵州茅台",
    side: "buy",
    price: 1480,
    quantity: 100,
    date: "2026-01-10",
    note: "长线配置",
  },
  {
    id: crypto.randomUUID(),
    symbol: "000858",
    name: "五粮液",
    side: "buy",
    price: 129.5,
    quantity: 500,
    date: "2026-02-14",
    note: "",
  },
  {
    id: crypto.randomUUID(),
    symbol: "00700",
    name: "腾讯控股",
    side: "buy",
    price: 310,
    quantity: 200,
    date: "2026-03-02",
    note: "",
  },
  {
    id: crypto.randomUUID(),
    symbol: "00700",
    name: "腾讯控股",
    side: "sell",
    price: 336,
    quantity: 60,
    date: "2026-04-03",
    note: "减仓",
  },
];

const state = {
  route: "earning",
  useDemoData: true,
  algoMode: "cost",
  benchmark: "none",
  rangeDays: 7,
  trades: [],
};

const routeButtons = [...document.querySelectorAll(".tab-btn")];
const routeViews = [...document.querySelectorAll(".route-view")];
const overviewGrid = document.getElementById("overviewGrid");
const positionList = document.getElementById("positionList");
const recordList = document.getElementById("recordList");
const tradeDialog = document.getElementById("tradeDialog");
const tradeForm = document.getElementById("tradeForm");
const quickTradeBtn = document.getElementById("quickTradeBtn");
const recordTradeBtn = document.getElementById("recordTradeBtn");
const closeDialogBtn = document.getElementById("closeDialogBtn");
const demoToggleBtn = document.getElementById("demoToggleBtn");
const algoModeSelect = document.getElementById("algoMode");
const benchmarkSelect = document.getElementById("benchmark");
const rangeChips = [...document.querySelectorAll(".chip")];
const analysisSummary = document.getElementById("analysisSummary");
const analysisChart = document.getElementById("analysisChart");

initialize();

function initialize() {
  hydrateState();
  bindEvents();
  render();
}

function hydrateState() {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      Object.assign(state, parsed);
    } catch (error) {
      console.error("读取本地数据失败，已使用默认数据", error);
    }
  }

  if (!Array.isArray(state.trades)) {
    state.trades = [];
  }

  if (state.useDemoData && state.trades.length === 0) {
    state.trades = demoTrades;
  }
}

function persistState() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

function bindEvents() {
  routeButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      state.route = btn.dataset.route;
      renderRoute();
      persistState();
    });
  });

  [quickTradeBtn, recordTradeBtn].forEach((btn) =>
    btn.addEventListener("click", () => {
      tradeForm.reset();
      const today = new Date().toISOString().slice(0, 10);
      document.getElementById("tradeDate").value = today;
      tradeDialog.showModal();
    })
  );

  closeDialogBtn.addEventListener("click", () => tradeDialog.close());

  tradeForm.addEventListener("submit", (event) => {
    event.preventDefault();
    const formData = new FormData(tradeForm);
    const trade = {
      id: crypto.randomUUID(),
      symbol: String(formData.get("symbol")).trim().toUpperCase(),
      name: String(formData.get("name")).trim(),
      side: String(formData.get("side")),
      price: Number(formData.get("price")),
      quantity: Number(formData.get("quantity")),
      date: String(formData.get("date")),
      note: String(formData.get("note") ?? "").trim(),
    };

    state.useDemoData = false;
    state.trades.push(trade);
    state.trades.sort((a, b) => new Date(a.date) - new Date(b.date));
    persistState();
    render();
    tradeDialog.close();
  });

  demoToggleBtn.addEventListener("click", () => {
    state.useDemoData = !state.useDemoData;
    state.trades = state.useDemoData ? [...demoTrades] : [];
    persistState();
    render();
  });

  algoModeSelect.addEventListener("change", () => {
    state.algoMode = algoModeSelect.value;
    persistState();
    renderAnalysis();
  });

  benchmarkSelect.addEventListener("change", () => {
    state.benchmark = benchmarkSelect.value;
    persistState();
    renderAnalysis();
  });

  rangeChips.forEach((chip) =>
    chip.addEventListener("click", () => {
      state.rangeDays = Number(chip.dataset.range);
      rangeChips.forEach((item) => item.classList.remove("active"));
      chip.classList.add("active");
      persistState();
      renderAnalysis();
    })
  );

  recordList.addEventListener("click", (event) => {
    const target = event.target.closest("button[data-remove-id]");
    if (!target) {
      return;
    }
    const id = target.dataset.removeId;
    state.trades = state.trades.filter((item) => item.id !== id);
    persistState();
    render();
  });
}

function render() {
  demoToggleBtn.textContent = state.useDemoData ? "开始使用" : "查看演示数据";
  algoModeSelect.value = state.algoMode;
  benchmarkSelect.value = state.benchmark;
  rangeChips.forEach((chip) => {
    chip.classList.toggle("active", Number(chip.dataset.range) === state.rangeDays);
  });

  renderRoute();
  renderOverviewAndPositions();
  renderRecords();
  renderAnalysis();
}

function renderRoute() {
  routeButtons.forEach((button) =>
    button.classList.toggle("active", button.dataset.route === state.route)
  );
  routeViews.forEach((view) =>
    view.classList.toggle("active", view.id === `route-${state.route}`)
  );
}

function computePortfolio() {
  const grouped = new Map();
  for (const trade of state.trades) {
    const key = trade.symbol;
    if (!grouped.has(key)) {
      grouped.set(key, {
        symbol: trade.symbol,
        name: trade.name,
        buyAmount: 0,
        sellAmount: 0,
        position: 0,
        latestPrice: trade.price,
        lastTradeDate: trade.date,
      });
    }
    const item = grouped.get(key);
    const amount = trade.price * trade.quantity;
    if (trade.side === "buy") {
      item.buyAmount += amount;
      item.position += trade.quantity;
    } else {
      item.sellAmount += amount;
      item.position -= trade.quantity;
    }
    item.latestPrice = trade.price;
    item.lastTradeDate = trade.date;
  }

  const positions = [...grouped.values()].map((item) => {
    const cost = item.position > 0 ? (item.buyAmount - item.sellAmount) / item.position : 0;
    const currentValue = item.position * item.latestPrice;
    const invested = item.buyAmount - item.sellAmount;
    const pnl = currentValue - invested;
    const pnlRate = invested > 0 ? pnl / invested : 0;
    const regretRate = cost > 0 ? (item.latestPrice - cost) / cost : 0;

    return {
      ...item,
      cost,
      currentValue,
      invested,
      pnl,
      pnlRate,
      regretRate,
    };
  });

  const totalValue = positions.reduce((sum, p) => sum + p.currentValue, 0);
  const totalInvested = positions.reduce((sum, p) => sum + p.invested, 0);
  const totalPnl = totalValue - totalInvested;

  return {
    positions,
    totalValue,
    totalInvested,
    totalPnl,
    totalRate: totalInvested > 0 ? totalPnl / totalInvested : 0,
    totalAssets: totalValue + Math.max(0, totalInvested * 0.2),
  };
}

function renderOverviewAndPositions() {
  const portfolio = computePortfolio();
  const overview = [
    { label: "总资产", value: formatCurrency(portfolio.totalAssets) },
    { label: "总市值", value: formatCurrency(portfolio.totalValue) },
    { label: "本金", value: formatCurrency(portfolio.totalInvested) },
    {
      label: "总收益",
      value: `${formatCurrency(portfolio.totalPnl)} (${formatPercent(portfolio.totalRate)})`,
      positive: portfolio.totalPnl >= 0,
    },
    {
      label: "今日收益",
      value: formatCurrency(portfolio.totalPnl * 0.08),
      positive: portfolio.totalPnl >= 0,
    },
    { label: "币种", value: "人民币" },
  ];

  overviewGrid.innerHTML = overview
    .map(
      (item) => `
      <article class="kpi">
        <span class="kpi-label">${item.label}</span>
        <p class="kpi-value ${item.positive === false ? "fall" : item.positive ? "rise" : ""}">${item.value}</p>
      </article>
    `
    )
    .join("");

  if (portfolio.positions.length === 0) {
    positionList.innerHTML = `
      <p class="empty">
        您没有添加交易记录，可以看看我们的演示数据，来了解预览产品功能。
      </p>
    `;
    return;
  }

  positionList.innerHTML = portfolio.positions
    .map((position) => {
      const rateClass = position.pnl >= 0 ? "rise" : "fall";
      return `
        <article class="position-item">
          <div>
            <h3>${position.name}</h3>
            <p class="muted">${position.symbol} · 持有股数 ${position.position}</p>
          </div>
          <div class="position-values">
            <p class="${rateClass}">${formatCurrency(position.pnl)}</p>
            <p class="${rateClass}">${formatPercent(position.pnlRate)}</p>
            <p class="muted">后悔率 ${formatPercent(position.regretRate)}</p>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderRecords() {
  if (state.trades.length === 0) {
    recordList.innerHTML = '<p class="empty">暂无交易记录，点击右上角“新建交易”。</p>';
    return;
  }

  const sorted = [...state.trades].sort((a, b) => new Date(b.date) - new Date(a.date));
  recordList.innerHTML = sorted
    .map(
      (trade) => `
      <article class="record-item">
        <div>
          <h3>${trade.name} <span class="muted">${trade.symbol}</span></h3>
          <p class="muted">${trade.date} · ${trade.note || "无备注"}</p>
        </div>
        <div class="record-meta">
          <p class="${trade.side === "buy" ? "rise" : "fall"}">${trade.side === "buy" ? "买入" : "卖出"}</p>
          <p>￥${trade.price.toFixed(3)} × ${trade.quantity}</p>
          <button class="danger-btn" data-remove-id="${trade.id}">删除</button>
        </div>
      </article>
    `
    )
    .join("");
}

function renderAnalysis() {
  const points = buildSyntheticCurve(state.rangeDays);
  drawLineChart(points);
  const latest = points[points.length - 1] ?? { myRate: 0, benchmarkRate: 0 };
  const excess = latest.myRate - latest.benchmarkRate;
  analysisSummary.textContent =
    state.benchmark === "none"
      ? `我的收益 ${formatPercent(latest.myRate)}`
      : `我的 ${formatPercent(latest.myRate)} / 基准 ${formatPercent(
          latest.benchmarkRate
        )} / 对比 ${formatPercent(excess)}`;
}

function buildSyntheticCurve(days) {
  const base = computePortfolio().totalRate || 0.02;
  const arr = [];
  let benchmarkAcc = 0;
  let myAcc = 0;

  for (let i = 1; i <= days; i++) {
    const wave = Math.sin(i / 4) * 0.002;
    const trend = base / days;
    const algoAdjust = state.algoMode === "money" ? 0.0002 : state.algoMode === "time" ? -0.0001 : 0;
    myAcc += trend + wave + algoAdjust;
    benchmarkAcc += trend * 0.75 + Math.cos(i / 6) * 0.0012;
    arr.push({ x: i, myRate: myAcc, benchmarkRate: benchmarkAcc });
  }
  return arr;
}

function drawLineChart(points) {
  const ctx = analysisChart.getContext("2d");
  const width = analysisChart.width;
  const height = analysisChart.height;
  ctx.clearRect(0, 0, width, height);

  ctx.strokeStyle = "#dbe2ea";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = (height / 4) * i;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(width, y);
    ctx.stroke();
  }

  const maxAbs = Math.max(
    0.02,
    ...points.map((p) => Math.max(Math.abs(p.myRate), Math.abs(p.benchmarkRate)))
  );
  const mapX = (x) => (x / (points.length || 1)) * (width - 40) + 20;
  const mapY = (v) => height / 2 - (v / maxAbs) * (height * 0.36);

  drawSeries({
    ctx,
    points,
    mapX,
    mapY,
    valueKey: "myRate",
    color: "#eb3b5a",
  });
  if (state.benchmark !== "none") {
    drawSeries({
      ctx,
      points,
      mapX,
      mapY,
      valueKey: "benchmarkRate",
      color: "#2f9cf4",
    });
  }
}

function drawSeries({ ctx, points, mapX, mapY, valueKey, color }) {
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((point, index) => {
    const x = mapX(index + 1);
    const y = mapY(point[valueKey]);
    if (index === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();
}

function formatCurrency(value) {
  const sign = value < 0 ? "-" : "";
  const abs = Math.abs(value);
  return `${sign}¥${abs.toLocaleString("zh-CN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function formatPercent(value) {
  const val = (value * 100).toFixed(2);
  return `${value >= 0 ? "+" : ""}${val}%`;
}
