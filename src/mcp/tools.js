const {
  getTradesPage,
  getTradesPageForSymbol,
  getCashTransfersPage,
  normalizeSymbol,
} = require("../db");
const { enrichTradesWithSymbolNames } = require("../symbol-name-resolve");
const {
  getMetricsHomeBundle,
  getMetricsAnalysisBundle,
  getMetricsPublicHomeBundle,
  getMetricsPublicAnalysisBundle,
} = require("../metrics-api-service");
const { getPublicTradesPage } = require("../community-service");
const { resolveDataAccess } = require("./target-access");
const { searchCommunityUsers } = require("./community-search");
const { getDynamicsForMcp, getCommunityDynamicsFeedForMcp, MCP_MAX_TOTAL_ITEMS } = require("./dynamics-service");
const { assertMcpUserActive, McpSubscriptionExpiredError } = require("./subscription-gate");
const { assertMcpScope, WRITE_LEDGER_SCOPE } = require("./scope");
const { upsertTradesViaMcp, upsertCashTransfersViaMcp } = require("./ledger-write");
const {
  MCP_UPSERT_TRADES_INPUT_SCHEMA,
  MCP_UPSERT_CASH_TRANSFERS_INPUT_SCHEMA,
  MCP_LEDGER_WRITE_BATCH_MAX,
} = require("./ledger-schema");

const OTHER_USER_TARGET_RULE =
  "查他人时：若用户只给昵称/称呼，必须先调用 search_community_users，展示候选人并请用户确认后，再将确认的 user_id 作为 target_user_id；禁止把昵称当作 target_user_id。";

const TOOL_DEFS = [
  {
    name: "search_community_users",
    description:
      "按昵称或展示名搜索社区用户，返回可查看的公开组合候选人（含 user_id、展示名、收益与重仓提示）。查他人持仓/成交/分析/动态前必须先调用本工具，经用户确认后再用返回的 user_id 作为 target_user_id。",
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string", description: "昵称或展示名关键词，如「西坡GCC」" },
        limit: { type: "number", description: "最多返回候选人数，默认 5，最大 10" },
      },
      required: ["query"],
    },
  },
  {
    name: "get_portfolio_summary",
    description: `组合摘要：总资产、现金占比、阶段收益（today/mtd/ytd/inception）。默认当前授权用户；可查他人公开组合时传 target_user_id。${OTHER_USER_TARGET_RULE}`,
    inputSchema: {
      type: "object",
      properties: {
        target_user_id: { type: "string", description: "可选。他人用户 ID；仅公开账户可用。" },
        account_id: { type: "string", description: "账户筛选，默认 all" },
        stages: { type: "string", description: "阶段列表，逗号分隔，默认 today,mtd,ytd,inception" },
      },
    },
  },
  {
    name: "get_holdings",
    description: `当前持仓表：市值、数量、权重、收益等。查本人持仓（如 TOP3 重仓）请直接调用本工具，无需 search_community_users。${OTHER_USER_TARGET_RULE}`,
    inputSchema: {
      type: "object",
      properties: {
        target_user_id: { type: "string" },
        account_id: { type: "string" },
        stages: { type: "string" },
      },
    },
  },
  {
    name: "get_analysis",
    description: `分析区间 bundle：收益走势序列、资产结构、个股排名等。stage 支持 mtd/ytd/last_7d/last_30d/last_90d/custom 等。${OTHER_USER_TARGET_RULE}`,
    inputSchema: {
      type: "object",
      properties: {
        target_user_id: { type: "string" },
        account_id: { type: "string" },
        stage: { type: "string", description: "默认 ytd" },
        from: { type: "string", description: "custom 区间起始 YYYY-MM-DD" },
        to: { type: "string", description: "custom 区间结束 YYYY-MM-DD" },
        benchmark_symbol: { type: "string", description: "可选基准代码" },
      },
    },
  },
  {
    name: "get_trades",
    description: `成交记录分页（新→旧），可按 symbol 筛选。用于逐笔复盘。${OTHER_USER_TARGET_RULE}`,
    inputSchema: {
      type: "object",
      properties: {
        target_user_id: { type: "string" },
        account_id: { type: "string" },
        symbol: { type: "string" },
        limit: { type: "number", description: "默认 50，最大 100" },
        offset: { type: "number", description: "默认 0" },
      },
    },
  },
  {
    name: "get_cash_transfers",
    description: "银证转账记录（仅本人账户；他人公开页不提供）。",
    inputSchema: {
      type: "object",
      properties: {
        account_id: { type: "string" },
        limit: { type: "number" },
        offset: { type: "number" },
      },
    },
  },
  {
    name: "get_stock_rank",
    description: `分析区间内个股排名（收益、交易笔数、持仓天数等）。${OTHER_USER_TARGET_RULE}`,
    inputSchema: {
      type: "object",
      properties: {
        target_user_id: { type: "string" },
        account_id: { type: "string" },
        stage: { type: "string" },
        from: { type: "string" },
        to: { type: "string" },
        benchmark_symbol: { type: "string" },
      },
    },
  },
  {
    name: "get_dynamics",
    description:
      `组合或个股动态时间线（交易备注 + 观点帖混排，含图片）。不传 symbol=整个人组合动态；传 symbol=该股动态（不含无标的纯观点帖）。查本人直接调用；查他人公开页需 target_user_id。默认自动翻页返回全部历史（最多 ${MCP_MAX_TOTAL_ITEMS} 条）；传 cursor 则只取一页；fetch_all=false 可强制单页。分析某段时间请配合 from/to（YYYY-MM-DD）。${OTHER_USER_TARGET_RULE}`,
    inputSchema: {
      type: "object",
      properties: {
        target_user_id: { type: "string", description: "可选。他人用户 ID；仅公开账户可用。" },
        symbol: { type: "string", description: "可选。个股代码，如 SH600519、AAPL" },
        from: { type: "string", description: "可选。起始日期 YYYY-MM-DD（含）" },
        to: { type: "string", description: "可选。结束日期 YYYY-MM-DD（含）" },
        fetch_all: {
          type: "boolean",
          description: "默认 true（无 cursor 时自动翻页取全量）；false 则只返回一页",
        },
        limit: { type: "number", description: "单页条数（fetch_all=false 时生效），默认 20，最大 30" },
        cursor: { type: "string", description: "分页游标；传入后仅取该页，不自动翻页" },
      },
    },
  },
  {
    name: "get_community_dynamics_feed",
    description:
      `社区关注流动态：仅包含当前用户已关注且已公开的用户动态（交易卡自动出现 + 观点帖），按时间混排。不能代替某一个人的 get_dynamics。默认自动翻页返回全部（最多 ${MCP_MAX_TOTAL_ITEMS} 条）；传 cursor 则只取一页。`,
    inputSchema: {
      type: "object",
      properties: {
        from: { type: "string", description: "可选。起始日期 YYYY-MM-DD（含）" },
        to: { type: "string", description: "可选。结束日期 YYYY-MM-DD（含）" },
        fetch_all: {
          type: "boolean",
          description: "默认 true（无 cursor 时自动翻页取全量）；false 则只返回一页",
        },
        limit: { type: "number", description: "单页条数（fetch_all=false 时生效），默认 20，最大 30" },
        cursor: { type: "string", description: "分页游标；传入后仅取该页，不自动翻页" },
      },
    },
  },
  {
    name: "upsert_trades",
    description:
      `新增或更新本人交易记录（单笔 trade 或批量 trades，最多 ${MCP_LEDGER_WRITE_BATCH_MAX} 条）。需要 scope write:ledger。` +
      "普通买卖(type=trade 或省略)必填：date、symbol、side、price、quantity、amount，以及 account_id 或 account_name 二选一（无默认账户）。" +
      "date 必须为实际成交日（YYYY-MM-DD），不要用录入当天代替成交日；录入前可先 get_trades 核对，避免重复新增。" +
      "分红/送股/拆股/并股字段要求见 inputSchema。校验失败返回 errors、format_spec 与 available_accounts。",
    inputSchema: MCP_UPSERT_TRADES_INPUT_SCHEMA,
  },
  {
    name: "upsert_cash_transfers",
    description:
      `新增或更新本人银证转账/资金记录（单笔 cash_transfer 或批量 cash_transfers，最多 ${MCP_LEDGER_WRITE_BATCH_MAX} 条）。需要 scope write:ledger。` +
      "必填：date、direction、amount，以及 account_id 或 account_name 二选一（无默认账户）。校验失败返回 available_accounts。",
    inputSchema: MCP_UPSERT_CASH_TRANSFERS_INPUT_SCHEMA,
  },
];

function toolMeta(access, extra = {}) {
  return {
    viewerId: access.viewerId,
    targetId: access.targetId,
    mode: access.mode,
    ...extra,
  };
}

async function callMcpTool(viewerId, name, args = {}, opts = {}) {
  const tool = String(name || "").trim();
  const input = args && typeof args === "object" ? args : {};
  const tokenScope = opts.scope || "";

  const subGate = await assertMcpUserActive(viewerId);
  if (!subGate.ok) {
    if (subGate.code === "subscription_expired") {
      throw new McpSubscriptionExpiredError();
    }
    const err = new Error(subGate.error || "forbidden");
    err.status = subGate.status || 403;
    throw err;
  }

  if (tool === "upsert_trades" || tool === "upsert_cash_transfers") {
    assertMcpScope(tokenScope, WRITE_LEDGER_SCOPE);
    if (input.target_user_id != null && String(input.target_user_id).trim()) {
      const err = new Error("写操作不支持 target_user_id");
      err.status = 403;
      throw err;
    }
    if (tool === "upsert_trades") {
      return upsertTradesViaMcp(viewerId, input);
    }
    return upsertCashTransfersViaMcp(viewerId, input);
  }

  if (tool === "search_community_users") {
    return searchCommunityUsers(viewerId, input);
  }

  if (tool === "get_community_dynamics_feed") {
    const result = await getCommunityDynamicsFeedForMcp(viewerId, input);
    if (!result.ok) {
      const err = new Error(result.error || "forbidden");
      err.status = result.status || 403;
      throw err;
    }
    return result;
  }

  if (tool === "get_dynamics") {
    const result = await getDynamicsForMcp(viewerId, input);
    if (!result.ok) {
      const err = new Error(result.error || "forbidden");
      err.status = result.status || 403;
      throw err;
    }
    return result;
  }

  const access = await resolveDataAccess(viewerId, input.target_user_id);
  if (!access.ok) {
    const err = new Error(access.error || "forbidden");
    err.status = access.status || 403;
    throw err;
  }

  const accountId = String(input.account_id || "all").trim() || "all";

  if (tool === "get_cash_transfers") {
    if (access.mode === "public") {
      return {
        meta: toolMeta(access),
        error: "他人公开组合不提供银证转账记录",
        data: [],
      };
    }
    const limit = Math.min(100, Math.max(1, Number(input.limit) || 50));
    const offset = Math.max(0, Number(input.offset) || 0);
    const page = await getCashTransfersPage(access.dataUserId, {
      limit,
      offset,
      accountId: accountId === "all" ? null : accountId,
    });
    return { meta: toolMeta(access), ...page };
  }

  if (tool === "get_trades") {
    const limit = Math.min(100, Math.max(1, Number(input.limit) || 50));
    const offset = Math.max(0, Number(input.offset) || 0);
    const symbol = input.symbol ? normalizeSymbol(String(input.symbol)) : "";
    if (access.mode === "public") {
      const page = await getPublicTradesPage(access.viewerId, access.dataUserId, {
        limit,
        offset,
        account_id: accountId,
        symbol: symbol || undefined,
      });
      if (page.error) {
        const err = new Error(page.error);
        err.status = page.error === "hidden" ? 403 : 404;
        throw err;
      }
      return { meta: toolMeta(access), data: page.data, pagination: page.pagination };
    }
    const pageOpts = {
      limit,
      offset,
      accountId: accountId === "all" ? null : accountId,
    };
    const page = symbol
      ? await getTradesPageForSymbol(access.dataUserId, symbol, pageOpts)
      : await getTradesPage(access.dataUserId, pageOpts);
    await enrichTradesWithSymbolNames(page.data);
    return { meta: toolMeta(access), data: page.data, pagination: page.pagination };
  }

  if (tool === "get_portfolio_summary" || tool === "get_holdings") {
    const stages = String(input.stages || "today,mtd,ytd,inception").trim() || "today,mtd,ytd,inception";
    const bundle =
      access.mode === "public"
        ? await getMetricsPublicHomeBundle(access.dataUserId, accountId, stages)
        : await getMetricsHomeBundle(access.dataUserId, accountId, stages);
    if (tool === "get_holdings") {
      return {
        meta: toolMeta(access, { stages }),
        holdings: bundle?.holdings || { rows: [] },
        metaBundle: bundle?.meta || null,
      };
    }
    return {
      meta: toolMeta(access, { stages }),
      returns: bundle?.returns || null,
      assets: bundle?.assets || null,
      metaBundle: bundle?.meta || null,
    };
  }

  if (tool === "get_analysis" || tool === "get_stock_rank") {
    const stage = String(input.stage || "ytd").trim() || "ytd";
    const bench = String(input.benchmark_symbol || "").trim();
    const opts = {
      customFrom: input.from ? String(input.from).slice(0, 10) : undefined,
      customTo: input.to ? String(input.to).slice(0, 10) : undefined,
    };
    const bundle =
      access.mode === "public"
        ? await getMetricsPublicAnalysisBundle(access.dataUserId, accountId, stage, bench, opts)
        : await getMetricsAnalysisBundle(access.dataUserId, accountId, stage, bench, opts);
    if (tool === "get_stock_rank") {
      return {
        meta: toolMeta(access, { stage }),
        stockRank: bundle?.stockRank || null,
        metaBundle: bundle?.meta || null,
      };
    }
    return {
      meta: toolMeta(access, { stage }),
      returns: bundle?.returns || null,
      assets: bundle?.assets || null,
      series: bundle?.series || null,
      stockRank: bundle?.stockRank || null,
      metaBundle: bundle?.meta || null,
    };
  }

  const err = new Error(`unknown tool: ${tool}`);
  err.status = 400;
  throw err;
}

module.exports = {
  TOOL_DEFS,
  callMcpTool,
};
