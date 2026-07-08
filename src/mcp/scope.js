const { WRITE_LEDGER_SCOPE } = require("./config");

function parseScopes(scopeStr) {
  return String(scopeStr || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
}

function hasMcpScope(scopeStr, required) {
  const need = String(required || "").trim();
  if (!need) {
    return true;
  }
  return parseScopes(scopeStr).includes(need);
}

function assertMcpScope(scopeStr, required) {
  if (hasMcpScope(scopeStr, required)) {
    return;
  }
  const err = new Error(`需要 OAuth scope: ${required}`);
  err.status = 403;
  err.code = "insufficient_scope";
  throw err;
}

module.exports = {
  parseScopes,
  hasMcpScope,
  assertMcpScope,
  WRITE_LEDGER_SCOPE,
};
