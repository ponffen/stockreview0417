const TENCENT_TIMEOUT_MS = Math.max(3000, Math.min(20_000, Number(process.env.TENCENT_QUOTE_TIMEOUT_MS || 8000)));

async function fetchTencentQuoteText(q) {
  const keys = String(q || "").trim();
  if (!keys || keys.length > 2048 || !/^[a-zA-Z0-9._,\-]+$/.test(keys)) {
    throw new Error("invalid q");
  }
  const url = `https://qt.gtimg.cn/q=${encodeURIComponent(keys)}`;
  const response = await fetch(url, {
    headers: {
      Referer: "https://finance.qq.com/",
      "User-Agent": "stockreview/1",
    },
    signal: AbortSignal.timeout(TENCENT_TIMEOUT_MS),
  });
  if (!response.ok) {
    throw new Error(`tencent http ${response.status}`);
  }
  const buf = Buffer.from(await response.arrayBuffer());
  let text = "";
  try {
    text = new TextDecoder("gbk").decode(buf);
  } catch {
    text = buf.toString("utf8");
  }
  return text;
}

module.exports = {
  fetchTencentQuoteText,
};
