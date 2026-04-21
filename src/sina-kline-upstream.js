const iconv = require("iconv-lite");

const SINA_CHROME_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const SINA_REFERER = "https://finance.sina.com.cn/";
const SINA_KLINE_PATH = "/quotes_service/api/json_v2.php/CN_MarketData.getKLineData";

/**
 * 从新浪拉日 K JSON。优先 HTTPS，回退 HTTP；charset=gbk。
 */
async function fetchSinaKlineJsonFromUpstream(paramsObj) {
  const q = new URLSearchParams(paramsObj);
  const qs = q.toString();
  const bases = ["https://money.finance.sina.com.cn", "http://money.finance.sina.com.cn"];
  let lastErr = "";
  for (const base of bases) {
    const url = `${base}${SINA_KLINE_PATH}?${qs}`;
    try {
      const r = await fetch(url, {
        headers: { "User-Agent": SINA_CHROME_UA, Referer: SINA_REFERER },
        signal: AbortSignal.timeout(25_000),
      });
      if (!r.ok) {
        lastErr = `sina ${r.status}`;
        continue;
      }
      const buf = Buffer.from(await r.arrayBuffer());
      const text = iconv.decode(buf, "gbk");
      let data;
      try {
        data = JSON.parse(text);
      } catch {
        try {
          data = JSON.parse(buf.toString("utf8"));
        } catch {
          lastErr = "invalid json";
          continue;
        }
      }
      return { ok: true, data, error: "" };
    } catch (e) {
      lastErr = e.message || "fetch failed";
    }
  }
  return { ok: false, data: null, error: lastErr || "sina failed" };
}

module.exports = {
  fetchSinaKlineJsonFromUpstream,
  SINA_CHROME_UA,
  SINA_REFERER,
};
