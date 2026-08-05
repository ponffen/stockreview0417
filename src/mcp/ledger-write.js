const {
  normalizeSymbol,
  isUsTickerSymbol,
  normalizeTrade,
  normalizeCashTransfer,
  upsertTrade,
  upsertCashTransfer,
  getTradeByIdForUser,
  findLikelyDuplicateTrade,
  getCashTransferByIdForUser,
  getAccounts,
} = require("../db");
const { parseType } = require("../db-pure");
const { enrichTradesWithSymbolNames, ensureSymbolNameMapOnNewTrade } = require("../symbol-name-resolve");
const {
  hintDatesFromImportRows,
  notifyLedgerMutation,
} = require("../metrics-invalidate");
const {
  MCP_LEDGER_WRITE_BATCH_MAX,
  MCP_LEDGER_FORMAT_SPEC,
} = require("./ledger-schema");

function formatAvailableAccounts(accounts) {
  return (accounts || []).map((account) => ({
    id: String(account.id || ""),
    name: String(account.name || ""),
    currency: String(account.currency || ""),
  }));
}

function validationPayload(extra = {}, availableAccounts = null) {
  const payload = { format_spec: MCP_LEDGER_FORMAT_SPEC, ...extra };
  if (Array.isArray(availableAccounts) && availableAccounts.length) {
    payload.available_accounts = formatAvailableAccounts(availableAccounts);
  }
  return payload;
}

function resolveAccountByName(nameRaw, accounts, index, errors) {
  const name = String(nameRaw || "").trim();
  const matches = (accounts || []).filter((account) => String(account.name || "").trim() === name);
  if (!matches.length) {
    pushError(
      errors,
      index,
      "account_name",
      "unknown_account",
      "account_name 不存在",
      name,
      "本人已有账户名称",
    );
    return null;
  }
  if (matches.length > 1) {
    pushError(
      errors,
      index,
      "account_name",
      "ambiguous_account",
      "account_name 匹配到多个账户，请改用 account_id",
      name,
      matches.map((account) => account.id).join(", "),
    );
    return null;
  }
  return String(matches[0].id);
}

function resolveAccountInput(raw, accounts, index, errors) {
  const idRaw = raw.account_id ?? raw.accountId;
  const nameRaw = raw.account_name ?? raw.accountName;
  const legacyRaw = raw.account;
  const hasId = isPresent(idRaw);
  const hasName = isPresent(nameRaw);
  const hasLegacy = isPresent(legacyRaw);

  if (!hasId && !hasName && !hasLegacy) {
    pushError(
      errors,
      index,
      "account_id",
      "required",
      "account_id 或 account_name 为必填",
      null,
      "account_id 或 account_name 二选一",
    );
    return null;
  }

  if (hasId) {
    const accountId = String(idRaw).trim();
    if ((accounts || []).some((account) => String(account.id) === accountId)) {
      return accountId;
    }
    pushError(
      errors,
      index,
      "account_id",
      "unknown_account",
      "account_id 不存在",
      accountId,
      "本人已有账户 id",
    );
    return null;
  }

  if (hasName) {
    return resolveAccountByName(nameRaw, accounts, index, errors);
  }

  const legacy = String(legacyRaw).trim();
  if ((accounts || []).some((account) => String(account.id) === legacy)) {
    return legacy;
  }
  return resolveAccountByName(legacy, accounts, index, errors);
}

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

class McpValidationError extends Error {
  constructor(payload) {
    super(payload?.error || "validation_error");
    this.name = "McpValidationError";
    this.status = 400;
    this.payload = payload;
  }
}

function isValidDateKey(value) {
  const s = String(value || "").slice(0, 10);
  if (!DATE_RE.test(s)) {
    return false;
  }
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) {
    return false;
  }
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  return (
    !Number.isNaN(d.getTime()) &&
    d.getFullYear() === Number(m[1]) &&
    d.getMonth() === Number(m[2]) - 1 &&
    d.getDate() === Number(m[3])
  );
}

function countDecimalPlaces(value) {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return NaN;
    }
    const s = String(value);
    const dot = s.indexOf(".");
    return dot === -1 ? 0 : s.length - dot - 1;
  }
  const s = String(value ?? "").trim();
  if (!s) {
    return 0;
  }
  const dot = s.indexOf(".");
  return dot === -1 ? 0 : s.length - dot - 1;
}

function isPresent(value) {
  return value !== undefined && value !== null && String(value).trim() !== "";
}

function tradeDedupFingerprint(trade) {
  const t = trade || {};
  return [
    String(t.accountId || "default"),
    String(t.symbol || "").toLowerCase(),
    String(t.side || ""),
    String(t.type || "trade"),
    Number(t.price).toFixed(3),
    Number(t.quantity).toFixed(4),
    Number(t.amount).toFixed(2),
  ].join("|");
}

function stripClientTimestampsForNewRow(raw) {
  const row = { ...raw };
  delete row.timestamp;
  delete row.createdAt;
  delete row.created_at;
  return row;
}

function pushError(errors, index, field, code, message, received, expected) {
  errors.push({
    index,
    field,
    code,
    message,
    received: received === undefined ? null : received,
    expected,
  });
}

function isValidStockSymbol(symbol) {
  const s = String(symbol || "").trim().toLowerCase();
  if (!s) {
    return false;
  }
  if (s.startsWith("fx_") || /^wh(usd|hkd)cny$/.test(s) || s === "usdcny" || s === "hkdcny") {
    return false;
  }
  if (s.startsWith("sh") || s.startsWith("sz") || s.startsWith("hk") || s.startsWith("rt_hk")) {
    return true;
  }
  return isUsTickerSymbol(s);
}

function parseSideInput(raw) {
  const value = String(raw || "").trim().toLowerCase();
  if (value === "buy" || value === "b" || value.includes("买")) {
    return "buy";
  }
  if (value === "sell" || value === "s" || value.includes("卖")) {
    return "sell";
  }
  return "";
}

function parseDirectionInput(raw) {
  const value = String(raw || "").trim().toLowerCase();
  if (value === "out" || value === "transfer_out" || value.includes("转出")) {
    return "out";
  }
  if (value === "in" || value === "transfer_in" || value.includes("转入")) {
    return "in";
  }
  return "";
}

function checkForbiddenFields(raw, forbiddenKeys, index, errors) {
  if (!raw || typeof raw !== "object") {
    return;
  }
  for (const key of forbiddenKeys) {
    if (!Object.hasOwn(raw, key)) {
      continue;
    }
    const val = raw[key];
    if (key === "image_urls" || key === "imageUrls") {
      const list = Array.isArray(val) ? val : [];
      if (list.length > 0) {
        pushError(
          errors,
          index,
          key,
          "forbidden_field",
          "MCP 写入暂不支持 image_urls",
          val,
          "请勿传入 image_urls",
        );
      }
      continue;
    }
    pushError(
      errors,
      index,
      key,
      "forbidden_field",
      `${key} 由服务端计算，不可传入`,
      val,
      "请勿传入该字段",
    );
  }
}

function resolveBatchRows(input, singleKey, pluralKey, availableAccounts = null) {
  const hasSingle = input[singleKey] != null && typeof input[singleKey] === "object";
  const hasPlural = Array.isArray(input[pluralKey]);
  if (hasSingle && hasPlural) {
    throw new McpValidationError(
      validationPayload(
        {
          ok: false,
          code: "validation_error",
          error: `请只传 ${singleKey} 或 ${pluralKey} 之一`,
          errors: [
            {
              index: -1,
              field: singleKey,
              code: "invalid_format",
              message: `请只传 ${singleKey} 或 ${pluralKey} 之一`,
              received: { [singleKey]: true, [pluralKey]: true },
              expected: `二选一：${singleKey} 或 ${pluralKey}[]`,
            },
          ],
        },
        availableAccounts,
      ),
    );
  }
  if (hasSingle) {
    return [input[singleKey]];
  }
  if (hasPlural) {
    return input[pluralKey];
  }
  throw new McpValidationError(
    validationPayload(
      {
        ok: false,
        code: "validation_error",
        error: `缺少 ${singleKey} 或 ${pluralKey}`,
        errors: [
          {
            index: -1,
            field: pluralKey,
            code: "required",
            message: `请提供 ${singleKey} 或 ${pluralKey} 数组`,
            received: null,
            expected: `${singleKey} 对象或 ${pluralKey}[]`,
          },
        ],
      },
      availableAccounts,
    ),
  );
}

function validateNumberField(errors, index, field, raw, { min = null, maxDecimals, required, positive }) {
  if (!isPresent(raw)) {
    if (required) {
      pushError(errors, index, field, "required", `${field} 为必填`, raw ?? null, "number");
    }
    return null;
  }
  const num = Number(raw);
  if (!Number.isFinite(num)) {
    pushError(errors, index, field, "invalid_number", `${field} 必须是有效数字`, raw, "number");
    return null;
  }
  if (min != null && num < min) {
    pushError(errors, index, field, "invalid_number", `${field} 不能小于 ${min}`, raw, `number ≥ ${min}`);
    return null;
  }
  if (positive && num <= 0) {
    pushError(errors, index, field, "invalid_number", `${field} 必须大于 0`, raw, "number > 0");
    return null;
  }
  const places = countDecimalPlaces(raw);
  if (maxDecimals != null && places > maxDecimals) {
    pushError(
      errors,
      index,
      field,
      "invalid_precision",
      `${field} 最多 ${maxDecimals} 位小数`,
      raw,
      `最多 ${maxDecimals} 位小数`,
    );
    return null;
  }
  return num;
}

async function validateTradeRow(raw, index, accounts, userId, errors) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    pushError(errors, index, "trade", "invalid_format", "每条记录必须是对象", raw, "object");
    return null;
  }

  checkForbiddenFields(raw, ["amount_share_ratio", "amountShareRatio", "image_urls", "imageUrls"], index, errors);

  const type = parseType(raw.type || raw.tradeType || raw["类型"]);
  const symbolInput = raw.symbol ?? raw.code ?? raw.stockCode ?? raw["证券代码"] ?? raw["代码"];
  const normalizedSymbol = normalizeSymbol(symbolInput);
  if (!isPresent(symbolInput)) {
    pushError(
      errors,
      index,
      "symbol",
      "required",
      "symbol 为必填",
      symbolInput ?? null,
      MCP_LEDGER_FORMAT_SPEC.trade.symbol.input,
    );
  } else if (!isValidStockSymbol(normalizedSymbol)) {
    pushError(
      errors,
      index,
      "symbol",
      "invalid_symbol",
      "无法识别为有效股票代码",
      symbolInput,
      MCP_LEDGER_FORMAT_SPEC.trade.symbol.input,
    );
  }

  const dateRaw = raw.date ?? raw.trade_date ?? raw.tradeDate ?? raw["成交日期"];
  if (!isPresent(dateRaw)) {
    pushError(errors, index, "date", "required", "date 为必填", dateRaw ?? null, MCP_LEDGER_FORMAT_SPEC.trade.date);
  } else if (!isValidDateKey(dateRaw)) {
    pushError(errors, index, "date", "invalid_format", "date 必须是 YYYY-MM-DD", dateRaw, MCP_LEDGER_FORMAT_SPEC.trade.date);
  }

  const accountId = resolveAccountInput(raw, accounts, index, errors);

  let side = parseSideInput(raw.side ?? raw.direction ?? raw["方向"] ?? raw["买卖"]);
  let price = null;
  let quantity = null;
  let amount = null;

  if (type === "trade") {
    if (!side) {
      pushError(errors, index, "side", "required", "type=trade 时 side 为必填", raw.side ?? null, "buy | sell");
    }
    price = validateNumberField(errors, index, "price", raw.price ?? raw["价格"] ?? raw["成交价"], {
      min: 0,
      maxDecimals: 3,
      required: true,
    });
    quantity = validateNumberField(errors, index, "quantity", raw.quantity ?? raw.qty ?? raw["数量"], {
      min: 0,
      maxDecimals: 4,
      required: true,
    });
    amount = validateNumberField(errors, index, "amount", raw.amount ?? raw["发生金额"] ?? raw["成交金额"], {
      min: 0,
      maxDecimals: 2,
      required: true,
      positive: true,
    });
  } else if (type === "dividend") {
    side = "sell";
    price = 0;
    quantity = 0;
    amount = validateNumberField(errors, index, "amount", raw.amount ?? raw["发生金额"], {
      min: 0,
      maxDecimals: 2,
      required: true,
      positive: true,
    });
  } else if (type === "bonus" || type === "split") {
    side = "buy";
    price = 0;
    quantity = validateNumberField(errors, index, "quantity", raw.quantity ?? raw.qty ?? raw["数量"], {
      min: 0,
      maxDecimals: 4,
      required: true,
      positive: true,
    });
    amount = validateNumberField(errors, index, "amount", raw.amount ?? 0, {
      min: 0,
      maxDecimals: 2,
      required: false,
    });
    if (amount == null) {
      amount = 0;
    }
  } else if (type === "merge") {
    side = "sell";
    price = 0;
    quantity = validateNumberField(errors, index, "quantity", raw.quantity ?? raw.qty ?? raw["数量"], {
      min: 0,
      maxDecimals: 4,
      required: true,
      positive: true,
    });
    amount = validateNumberField(errors, index, "amount", raw.amount ?? 0, {
      min: 0,
      maxDecimals: 2,
      required: false,
    });
    if (amount == null) {
      amount = 0;
    }
  }

  const note = raw.note ?? raw.remark ?? raw["备注"];
  if (note != null && String(note).length > 500) {
    pushError(errors, index, "note", "invalid_format", "note 最长 500 字符", String(note).slice(0, 80), "≤500 字符");
  }

  const idRaw = raw.id ?? raw.tradeId;
  let prior = null;
  if (isPresent(idRaw)) {
    prior = await getTradeByIdForUser(String(idRaw), userId);
    if (!prior) {
      pushError(errors, index, "id", "not_found", "交易记录不存在或不属于当前用户", idRaw, "本人已有 trade id");
    }
  }

  if (errors.some((e) => e.index === index)) {
    return null;
  }
  if (!accountId) {
    return null;
  }

  const rowForNormalize = isPresent(idRaw) ? raw : stripClientTimestampsForNewRow(raw);
  const trade = normalizeTrade({
    ...rowForNormalize,
    id: isPresent(idRaw) ? String(idRaw) : undefined,
    accountId,
    account_id: accountId,
    type,
    symbol: normalizedSymbol,
    side: side || "buy",
    price: price ?? 0,
    quantity: quantity ?? 0,
    amount: amount ?? 0,
    date: String(dateRaw).slice(0, 10),
    note,
  });

  if (!isPresent(idRaw)) {
    const dup = await findLikelyDuplicateTrade(userId, trade);
    if (dup) {
      pushError(
        errors,
        index,
        "trade",
        "duplicate_trade",
        `疑似重复成交：${dup.date} 已有相同记录（id=${dup.id}）。若需修改请传 id 更新，勿重复新增`,
        { date: trade.date, symbol: trade.symbol, side: trade.side },
        `已有 id=${dup.id}`,
      );
      return null;
    }
  }

  return { trade, prior, symbolInput: isPresent(symbolInput) ? String(symbolInput).trim() : "" };
}

async function validateCashTransferRow(raw, index, accounts, userId, errors) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    pushError(errors, index, "cash_transfer", "invalid_format", "每条记录必须是对象", raw, "object");
    return null;
  }

  const dateRaw = raw.date ?? raw.transfer_date ?? raw.transferDate;
  if (!isPresent(dateRaw)) {
    pushError(errors, index, "date", "required", "date 为必填", dateRaw ?? null, MCP_LEDGER_FORMAT_SPEC.cash_transfer.date);
  } else if (!isValidDateKey(dateRaw)) {
    pushError(errors, index, "date", "invalid_format", "date 必须是 YYYY-MM-DD", dateRaw, MCP_LEDGER_FORMAT_SPEC.cash_transfer.date);
  }

  const direction = parseDirectionInput(raw.direction ?? raw["方向"]);
  if (!direction) {
    pushError(
      errors,
      index,
      "direction",
      "required",
      "direction 为必填",
      raw.direction ?? null,
      MCP_LEDGER_FORMAT_SPEC.cash_transfer.direction,
    );
  }

  const amount = validateNumberField(errors, index, "amount", raw.amount, {
    min: 0,
    maxDecimals: 2,
    required: true,
    positive: true,
  });

  const accountId = resolveAccountInput(raw, accounts, index, errors);

  const note = raw.note ?? raw.remark ?? raw["备注"];
  if (note != null && String(note).length > 500) {
    pushError(errors, index, "note", "invalid_format", "note 最长 500 字符", String(note).slice(0, 80), "≤500 字符");
  }

  const idRaw = raw.id;
  let prior = null;
  if (isPresent(idRaw)) {
    prior = await getCashTransferByIdForUser(String(idRaw), userId);
    if (!prior) {
      pushError(errors, index, "id", "not_found", "资金记录不存在或不属于当前用户", idRaw, "本人已有 cash_transfer id");
    }
  }

  if (errors.some((e) => e.index === index)) {
    return null;
  }
  if (!accountId) {
    return null;
  }

  const row = normalizeCashTransfer({
    ...raw,
    id: isPresent(idRaw) ? String(idRaw) : undefined,
    accountId,
    account_id: accountId,
    direction,
    amount: amount ?? 0,
    date: String(dateRaw).slice(0, 10),
    note,
  });

  return { row, prior };
}

function buildValidationFailure(errors, total, availableAccounts) {
  const invalidIndexes = new Set(errors.map((e) => e.index));
  return validationPayload(
    {
      ok: false,
      code: "validation_error",
      error: `批量校验失败：${total} 条记录中有 ${invalidIndexes.size} 条不符合格式`,
      summary: {
        total,
        invalid: invalidIndexes.size,
        valid: Math.max(0, total - invalidIndexes.size),
      },
      errors,
    },
    availableAccounts,
  );
}

async function loadAccountsForUser(userId) {
  return getAccounts(userId);
}

async function upsertTradesViaMcp(userId, input) {
  const accounts = await loadAccountsForUser(userId);

  if (input.strict === false) {
    throw new McpValidationError(
      validationPayload(
        {
          ok: false,
          code: "validation_error",
          error: "暂不支持 strict=false 部分成功模式",
          errors: [
            {
              index: -1,
              field: "strict",
              code: "invalid_format",
              message: "仅支持 strict=true（默认）",
              received: false,
              expected: "true 或省略",
            },
          ],
        },
        accounts,
      ),
    );
  }

  const rows = resolveBatchRows(input, "trade", "trades", accounts);
  if (rows.length > MCP_LEDGER_WRITE_BATCH_MAX) {
    throw new McpValidationError(
      validationPayload(
        {
          ok: false,
          code: "validation_error",
          error: `批量条数超过上限 ${MCP_LEDGER_WRITE_BATCH_MAX}`,
          errors: [
            {
              index: -1,
              field: "trades",
              code: "batch_too_large",
              message: `单次最多 ${MCP_LEDGER_WRITE_BATCH_MAX} 条`,
              received: rows.length,
              expected: `≤ ${MCP_LEDGER_WRITE_BATCH_MAX}`,
            },
          ],
        },
        accounts,
      ),
    );
  }

  const errors = [];
  const normalized = [];

  for (let index = 0; index < rows.length; index += 1) {
    const item = await validateTradeRow(rows[index], index, accounts, userId, errors);
    if (item) {
      normalized.push(item);
    }
  }

  if (errors.length) {
    throw new McpValidationError(buildValidationFailure(errors, rows.length, accounts));
  }

  const batchFingerprints = new Map();
  for (let index = 0; index < normalized.length; index += 1) {
    const item = normalized[index];
    if (item.prior) {
      continue;
    }
    const fp = tradeDedupFingerprint(item.trade);
    const prevIdx = batchFingerprints.get(fp);
    if (prevIdx != null) {
      errors.push({
        index,
        field: "trade",
        code: "duplicate_trade",
        message: `本批次第 ${prevIdx + 1} 条与第 ${index + 1} 条疑似重复，请只保留一条`,
        received: { date: item.trade.date, symbol: item.trade.symbol, side: item.trade.side },
        expected: "单条成交",
      });
    } else {
      batchFingerprints.set(fp, index);
    }
  }
  if (errors.length) {
    throw new McpValidationError(buildValidationFailure(errors, rows.length, accounts));
  }

  const saved = [];
  let created = 0;
  let updated = 0;

  for (const item of normalized) {
    const savedRow = await upsertTrade(item.trade, userId);
    if (!item.prior) {
      created += 1;
      await ensureSymbolNameMapOnNewTrade(savedRow.symbol, savedRow.name);
    } else {
      updated += 1;
    }
    saved.push(savedRow);
  }

  await enrichTradesWithSymbolNames(saved);
  const data = saved.map((row, i) => ({
    ...row,
    symbol_input: normalized[i]?.symbolInput || row.symbol,
  }));

  await notifyLedgerMutation(userId, { hintDates: hintDatesFromImportRows(data, "date") });

  return {
    ok: true,
    action: "batch_upsert",
    count: data.length,
    created,
    updated,
    data,
    meta: { viewerId: userId, rebuilding: true },
  };
}

async function upsertCashTransfersViaMcp(userId, input) {
  const accounts = await loadAccountsForUser(userId);

  if (input.strict === false) {
    throw new McpValidationError(
      validationPayload(
        {
          ok: false,
          code: "validation_error",
          error: "暂不支持 strict=false 部分成功模式",
          errors: [
            {
              index: -1,
              field: "strict",
              code: "invalid_format",
              message: "仅支持 strict=true（默认）",
              received: false,
              expected: "true 或省略",
            },
          ],
        },
        accounts,
      ),
    );
  }

  const rows = resolveBatchRows(input, "cash_transfer", "cash_transfers", accounts);
  if (rows.length > MCP_LEDGER_WRITE_BATCH_MAX) {
    throw new McpValidationError(
      validationPayload(
        {
          ok: false,
          code: "validation_error",
          error: `批量条数超过上限 ${MCP_LEDGER_WRITE_BATCH_MAX}`,
          errors: [
            {
              index: -1,
              field: "cash_transfers",
              code: "batch_too_large",
              message: `单次最多 ${MCP_LEDGER_WRITE_BATCH_MAX} 条`,
              received: rows.length,
              expected: `≤ ${MCP_LEDGER_WRITE_BATCH_MAX}`,
            },
          ],
        },
        accounts,
      ),
    );
  }

  const errors = [];
  const normalized = [];

  for (let index = 0; index < rows.length; index += 1) {
    const item = await validateCashTransferRow(rows[index], index, accounts, userId, errors);
    if (item) {
      normalized.push(item);
    }
  }

  if (errors.length) {
    throw new McpValidationError(buildValidationFailure(errors, rows.length, accounts));
  }

  const saved = [];
  let created = 0;
  let updated = 0;

  for (const item of normalized) {
    const savedRow = await upsertCashTransfer(item.row, userId);
    if (!item.prior) {
      created += 1;
    } else {
      updated += 1;
    }
    saved.push(savedRow);
  }

  await notifyLedgerMutation(userId, { hintDates: hintDatesFromImportRows(saved, "date") });

  return {
    ok: true,
    action: "batch_upsert",
    count: saved.length,
    created,
    updated,
    data: saved,
    meta: { viewerId: userId, rebuilding: true },
  };
}

module.exports = {
  MCP_LEDGER_FORMAT_SPEC,
  MCP_LEDGER_WRITE_BATCH_MAX,
  McpValidationError,
  upsertTradesViaMcp,
  upsertCashTransfersViaMcp,
  formatAvailableAccounts,
  resolveAccountInput,
};
