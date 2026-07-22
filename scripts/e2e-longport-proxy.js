#!/usr/bin/env node
/**
 * E2E smoke test for Aliyun FC LongPort proxy.
 * Usage:
 *   LONGPORT_APP_KEY=... LONGPORT_APP_SECRET=... LONGPORT_ACCESS_TOKEN=... \
 *     node scripts/e2e-longport-proxy.js
 */
const PROXY_BASE =
  String(process.env.ALIYUN_QUOTE_PROXY_BASE_URL || "").trim().replace(/\/+$/, "") ||
  "https://market-et-proxy-chbtzurmsn.cn-hangzhou.fcapp.run";

const SYMBOLS = (process.env.LONGPORT_TEST_SYMBOLS || "hk00700,usNVDA,usGOOG").split(",");

function env(name) {
  return String(process.env[name] || "").trim();
}

async function main() {
  const appKey = env("LONGPORT_APP_KEY") || env("LONGBRIDGE_APP_KEY");
  const appSecret = env("LONGPORT_APP_SECRET") || env("LONGBRIDGE_APP_SECRET");
  const accessToken = env("LONGPORT_ACCESS_TOKEN") || env("LONGBRIDGE_ACCESS_TOKEN");
  const httpUrl = env("LONGPORT_HTTP_URL") || env("LONGBRIDGE_HTTP_URL");

  console.log("== FC health ==");
  const health = await fetch(`${PROXY_BASE}/api/health`).then((r) => r.json());
  console.log(JSON.stringify(health, null, 2));
  if (!health.ok || health.longportTransport !== "websocket") {
    process.exitCode = 1;
    return;
  }

  if (!appKey || !appSecret || !accessToken) {
    console.log("\n(skip quote test: LONGPORT_* env not set)");
    return;
  }

  const headers = {
    "X-Longport-App-Key": appKey,
    "X-Longport-App-Secret": appSecret,
    "X-Longport-Access-Token": accessToken,
  };
  if (httpUrl) {
    headers["X-Longport-Http-Url"] = httpUrl;
  }
  const overnight = env("LONGPORT_ENABLE_OVERNIGHT") || env("LONGBRIDGE_ENABLE_OVERNIGHT");
  if (overnight) {
    headers["X-Longport-Enable-Overnight"] = overnight;
  }

  const url = `${PROXY_BASE}/api/quote/longport?symbols=${encodeURIComponent(SYMBOLS.join(","))}`;
  console.log("\n== LongPort quote ==");
  console.log("GET", url);
  const res = await fetch(url, { headers });
  const text = await res.text();
  let body = {};
  try {
    body = text ? JSON.parse(text) : {};
  } catch {
    body = { raw: text };
  }
  console.log("HTTP", res.status);
  console.log(JSON.stringify(body, null, 2));

  if (!res.ok || !body.ok || !Object.keys(body.quotes || {}).length) {
    console.error("\nFAIL: longport proxy did not return quotes");
    process.exitCode = 1;
    return;
  }
  console.log("\nOK:", Object.keys(body.quotes).length, "quotes");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
