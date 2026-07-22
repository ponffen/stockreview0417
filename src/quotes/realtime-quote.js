/**
 * 实时行情编排：长桥优先 → 腾讯备灾。
 */
const { normalizeSymbol } = require("../db");
const {
  fetchTencentQuoteMap,
  fetchTencentForexMap,
  fetchTencentQuotePayloadMap,
  parseTencentForexFromPayload,
  toTencentQuoteKey,
} = require("./tencent-quote");
const { fetchLongportQuoteMap } = require("./longport-quote");
const { resolveQuoteSource } = require("./quote-common");

async function fetchQuoteMap(symbols, opts = {}) {
  const symList = [...new Set((symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))];
  const out = new Map();
  const sources = [];
  let delayed = false;

  if (!symList.length) {
    return { ok: false, map: out, delayed: false, quoteSource: "", missing: [] };
  }

  const budgetMs = opts.budgetMs;
  const lp = await fetchLongportQuoteMap(symList);
  if (lp.map?.size) {
    sources.push("longport");
    for (const [sym, rec] of lp.map.entries()) {
      out.set(sym, rec);
    }
    if (lp.delayed) {
      delayed = true;
    }
  }

  const missing = symList.filter((sym) => !out.has(sym));
  if (missing.length) {
    const tc = await fetchTencentQuoteMap(missing, budgetMs);
    if (tc.map?.size) {
      sources.push("tencent");
      for (const [sym, rec] of tc.map.entries()) {
        if (!out.has(sym)) {
          out.set(sym, { ...rec, delayed: tc.delayed || rec.delayed });
        }
      }
    }
    if (tc.delayed) {
      delayed = true;
    }
  }

  const stillMissing = symList.filter((sym) => !out.has(sym));
  return {
    ok: out.size > 0,
    map: out,
    delayed,
    quoteSource: resolveQuoteSource(sources),
    quoteError: lp.error || "",
    missing: stillMissing,
    error: lp.error || "",
  };
}

module.exports = {
  fetchQuoteMap,
  fetchTencentForexMap,
  fetchTencentQuotePayloadMap,
  fetchTencentQuoteMap,
  fetchLongportQuoteMap,
  toTencentQuoteKey,
  parseTencentForexFromPayload,
};
