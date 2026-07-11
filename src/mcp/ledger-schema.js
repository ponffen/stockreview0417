const MCP_LEDGER_WRITE_BATCH_MAX = Math.min(
  200,
  Math.max(1, Number(process.env.MCP_LEDGER_WRITE_BATCH_MAX) || 50),
);

const TRADE_DATE_SCHEMA = {
  type: "string",
  description: "交易日期，格式 YYYY-MM-DD",
  examples: ["2026-06-24"],
};

const TRADE_SYMBOL_SCHEMA = {
  type: "string",
  description:
    "股票代码。支持 A 股 6 位(600519)、带前缀(sh600519)、港股(00700/hk00700)、美股(tsm/us_tsm)、内置中文别名",
  examples: ["sh600519", "600519", "hk00700", "tsm"],
};

const TRADE_ACCOUNT_ID_SCHEMA = {
  type: "string",
  description: "本人账户 id。与 account_name 二选一，必填其一，无默认值",
  examples: ["default"],
};

const TRADE_ACCOUNT_NAME_SCHEMA = {
  type: "string",
  description: "本人账户名称。用户说账户名而非 id 时用此字段；与 account_id 二选一",
  examples: ["默认账户", "华泰"],
};

const TRADE_ACCOUNT_ANY_OF = [{ required: ["account_id"] }, { required: ["account_name"] }];

const TRADE_OPTIONAL_FIELDS = {
  note: { type: "string", maxLength: 500, description: "备注，可选" },
  id: { type: "string", description: "可选 UUID；传入则更新已有记录" },
};

const MCP_TRADE_ROW_SCHEMA = {
  oneOf: [
    {
      title: "普通买卖（type 省略视为 trade）",
      type: "object",
      properties: {
        date: TRADE_DATE_SCHEMA,
        symbol: TRADE_SYMBOL_SCHEMA,
        side: {
          type: "string",
          enum: ["buy", "sell"],
          description: "买卖方向：buy=买入，sell=卖出（可接受中文 买入/卖出）",
        },
        price: { type: "number", minimum: 0, description: "成交价，≥0，最多 3 位小数" },
        quantity: { type: "number", minimum: 0, description: "成交数量，≥0，最多 4 位小数" },
        amount: {
          type: "number",
          exclusiveMinimum: 0,
          description: "发生金额，>0，最多 2 位小数（必填，服务端不会自动 price×quantity）",
        },
        account_id: TRADE_ACCOUNT_ID_SCHEMA,
        account_name: TRADE_ACCOUNT_NAME_SCHEMA,
        ...TRADE_OPTIONAL_FIELDS,
      },
      required: ["date", "symbol", "side", "price", "quantity", "amount"],
      allOf: TRADE_ACCOUNT_ANY_OF,
      additionalProperties: true,
    },
    {
      title: "普通买卖 type=trade",
      type: "object",
      properties: {
        type: { type: "string", enum: ["trade"] },
        date: TRADE_DATE_SCHEMA,
        symbol: TRADE_SYMBOL_SCHEMA,
        side: { type: "string", enum: ["buy", "sell"] },
        price: { type: "number", minimum: 0 },
        quantity: { type: "number", minimum: 0 },
        amount: { type: "number", exclusiveMinimum: 0 },
        account_id: TRADE_ACCOUNT_ID_SCHEMA,
        account_name: TRADE_ACCOUNT_NAME_SCHEMA,
        ...TRADE_OPTIONAL_FIELDS,
      },
      required: ["type", "date", "symbol", "side", "price", "quantity", "amount"],
      allOf: TRADE_ACCOUNT_ANY_OF,
      additionalProperties: true,
    },
    {
      title: "分红 dividend",
      type: "object",
      properties: {
        type: { type: "string", enum: ["dividend"] },
        date: TRADE_DATE_SCHEMA,
        symbol: TRADE_SYMBOL_SCHEMA,
        amount: { type: "number", exclusiveMinimum: 0, description: "分红金额，>0，最多 2 位小数" },
        account_id: TRADE_ACCOUNT_ID_SCHEMA,
        account_name: TRADE_ACCOUNT_NAME_SCHEMA,
        ...TRADE_OPTIONAL_FIELDS,
      },
      required: ["type", "date", "symbol", "amount"],
      allOf: TRADE_ACCOUNT_ANY_OF,
      additionalProperties: true,
    },
    {
      title: "送股/拆股 bonus|split",
      type: "object",
      properties: {
        type: { type: "string", enum: ["bonus", "split"] },
        date: TRADE_DATE_SCHEMA,
        symbol: TRADE_SYMBOL_SCHEMA,
        quantity: { type: "number", exclusiveMinimum: 0, description: "送股/拆股数量，>0，最多 4 位小数" },
        amount: { type: "number", minimum: 0, description: "可选，默认 0，最多 2 位小数" },
        account_id: TRADE_ACCOUNT_ID_SCHEMA,
        account_name: TRADE_ACCOUNT_NAME_SCHEMA,
        ...TRADE_OPTIONAL_FIELDS,
      },
      required: ["type", "date", "symbol", "quantity"],
      allOf: TRADE_ACCOUNT_ANY_OF,
      additionalProperties: true,
    },
    {
      title: "并股 merge",
      type: "object",
      properties: {
        type: { type: "string", enum: ["merge"] },
        date: TRADE_DATE_SCHEMA,
        symbol: TRADE_SYMBOL_SCHEMA,
        quantity: { type: "number", exclusiveMinimum: 0, description: "并股后数量，>0，最多 4 位小数" },
        amount: { type: "number", minimum: 0, description: "可选，默认 0，最多 2 位小数" },
        account_id: TRADE_ACCOUNT_ID_SCHEMA,
        account_name: TRADE_ACCOUNT_NAME_SCHEMA,
        ...TRADE_OPTIONAL_FIELDS,
      },
      required: ["type", "date", "symbol", "quantity"],
      allOf: TRADE_ACCOUNT_ANY_OF,
      additionalProperties: true,
    },
  ],
};

const MCP_CASH_TRANSFER_ROW_SCHEMA = {
  type: "object",
  properties: {
    date: TRADE_DATE_SCHEMA,
    direction: {
      type: "string",
      enum: ["in", "out"],
      description: "资金方向：in=转入，out=转出（可接受中文 转入/转出）",
    },
    amount: { type: "number", exclusiveMinimum: 0, description: "金额，>0，最多 2 位小数" },
    account_id: TRADE_ACCOUNT_ID_SCHEMA,
    account_name: TRADE_ACCOUNT_NAME_SCHEMA,
    note: { type: "string", maxLength: 500 },
    id: { type: "string", description: "可选 UUID；传入则更新已有记录" },
  },
  required: ["date", "direction", "amount"],
  allOf: TRADE_ACCOUNT_ANY_OF,
  additionalProperties: true,
};

const MCP_UPSERT_TRADES_INPUT_SCHEMA = {
  type: "object",
  properties: {
    trade: {
      ...MCP_TRADE_ROW_SCHEMA,
      description: "单笔交易记录",
    },
    trades: {
      type: "array",
      items: MCP_TRADE_ROW_SCHEMA,
      maxItems: MCP_LEDGER_WRITE_BATCH_MAX,
      description: `批量交易，最多 ${MCP_LEDGER_WRITE_BATCH_MAX} 条`,
    },
    strict: {
      type: "boolean",
      description: "默认 true：任一校验失败则整批不写入",
      default: true,
    },
  },
};

const MCP_UPSERT_CASH_TRANSFERS_INPUT_SCHEMA = {
  type: "object",
  properties: {
    cash_transfer: {
      ...MCP_CASH_TRANSFER_ROW_SCHEMA,
      description: "单笔银证转账记录",
    },
    cash_transfers: {
      type: "array",
      items: MCP_CASH_TRANSFER_ROW_SCHEMA,
      maxItems: MCP_LEDGER_WRITE_BATCH_MAX,
      description: `批量资金记录，最多 ${MCP_LEDGER_WRITE_BATCH_MAX} 条`,
    },
    strict: {
      type: "boolean",
      description: "默认 true：任一校验失败则整批不写入",
      default: true,
    },
  },
};

const MCP_LEDGER_FORMAT_SPEC = {
  trade: {
    date: "YYYY-MM-DD，例如 2026-06-24",
    symbol: {
      input:
        "可传 A 股 6 位(600519)、带前缀(sh600519)、港股(00700/hk00700)、美股(tsm/us_tsm)、内置中文别名(台积电→tsm)",
      stored: "normalize 后的 canonical 形式，如 sh600519、hk00700、tsm",
    },
    type: "trade | dividend | bonus | split | merge；省略视为 trade",
  },
  trade_type_trade: {
    required: ["date", "symbol", "side", "price", "quantity", "amount", "account_id|account_name"],
    side: "buy | sell（可接受 买入/卖出）",
    price: "number，≥0，最多 3 位小数",
    quantity: "number，≥0，最多 4 位小数",
    amount: "number，>0，最多 2 位小数（必填，不会自动 price×quantity）",
    account_id: "必填其一：account_id 或 account_name；无默认值",
    account_name: "用户说账户名时用此字段；须精确匹配本人账户名称",
  },
  trade_type_dividend: {
    required: ["date", "symbol", "amount", "account_id|account_name"],
    amount: "number，>0，最多 2 位小数",
  },
  trade_type_bonus_split: {
    required: ["date", "symbol", "quantity", "account_id|account_name"],
    quantity: "number，>0，最多 4 位小数",
    amount: "可选，默认 0",
  },
  trade_type_merge: {
    required: ["date", "symbol", "quantity", "account_id|account_name"],
    quantity: "number，>0，最多 4 位小数",
    amount: "可选，默认 0",
  },
  trade_common_optional: {
    note: "可选，最长 500 字符",
    id: "可选 UUID，有则更新",
    forbidden: ["amount_share_ratio", "image_urls"],
  },
  cash_transfer: {
    date: "YYYY-MM-DD，例如 2026-06-24",
    direction: "必填：in | out（可接受 转入/转出）",
    amount: "number，>0，最多 2 位小数",
    account_id: "必填其一：account_id 或 account_name；无默认值",
    account_name: "用户说账户名时用此字段",
    note: "可选，最长 500 字符",
    id: "可选 UUID，有则更新",
  },
  batch: {
    max_items: MCP_LEDGER_WRITE_BATCH_MAX,
    strict: "默认 true：任一校验失败则整批不写入",
  },
  available_accounts:
    "校验失败时返回本人账户列表 [{ id, name, currency }]，供 AI 向用户确认账户",
};

module.exports = {
  MCP_LEDGER_WRITE_BATCH_MAX,
  MCP_TRADE_ROW_SCHEMA,
  MCP_CASH_TRANSFER_ROW_SCHEMA,
  MCP_UPSERT_TRADES_INPUT_SCHEMA,
  MCP_UPSERT_CASH_TRANSFERS_INPUT_SCHEMA,
  MCP_LEDGER_FORMAT_SPEC,
};
