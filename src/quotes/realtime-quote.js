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
const { recordQuoteStep, sampleList } = require("./quote-trace-log");

async function fetchQuoteMap(symbols, opts = {}) {
  const symList = [...new Set((symbols || []).map((s) => normalizeSymbol(s)).filter(Boolean))];
  const out = new Map();
  const sources = [];
  let delayed = false;
  const quoteTrace = opts.quoteTrace || null;
  const t0 = Date.now();

  if (!symList.length) {
    return { ok: false, map: out, delayed: false, quoteSource: "", missing: [] };
  }

  const budgetMs = opts.budgetMs;
  recordQuoteStep(quoteTrace, "quote.orchestrator.start", {
    symbolCount: symList.length,
    budgetMs: budgetMs || null,
  });

  const lp = await fetchLongportQuoteMap(symList, quoteTrace);
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
  let tc = { ok: false, map: new Map(), delayed: false, source: "" };
  if (missing.length) {
    tc = await fetchTencentQuoteMap(missing, { budgetMs, quoteTrace });
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
  recordQuoteStep(quoteTrace, "quote.orchestrator.done", {
    ok: out.size > 0,
    quoteSource: resolveQuoteSource(sources),
    longportGot: lp.map?.size || 0,
    tencentGot: tc.map?.size || 0,
    stillMissingCount: stillMissing.length,
    stillMissingSample: sampleList(stillMissing),
    quoteErrorFrom: lp.error ? "longport" : "",
    quoteError: lp.error || "",
    ms: Date.now() - t0,
  });

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
