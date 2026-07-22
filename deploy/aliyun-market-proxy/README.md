# Aliyun FC Market Proxy

国内行情代理（Function Compute）。**长桥密钥只配在 Vercel**，由服务端请求 FC 时通过 HTTP Header 转发，FC 本身不存密钥。

## 路由

| 路径 | 说明 |
|------|------|
| `GET /api/quote/longport?symbols=hk00700,usAAPL` | 长桥实时行情（JSON） |
| `GET /api/quote/tencent?q=hk_hk00700,us_aapl` | 腾讯 qt 行情 |
| `GET /api/health` | 健康检查 |

长桥请求需携带 Header（由 Vercel `longport-quote.js` 自动添加）：

- `X-Longport-App-Key`
- `X-Longport-App-Secret`
- `X-Longport-Access-Token`
- `X-Longport-Http-Url`（可选）
- `X-Longport-Enable-Overnight`（可选）

## Vercel 环境变量（唯一配置点）

```bash
LONGPORT_APP_KEY=...
LONGPORT_APP_SECRET=...
LONGPORT_ACCESS_TOKEN=...
LONGPORT_HTTP_URL=https://openapi.longbridge.cn
LONGPORT_ENABLE_OVERNIGHT=true
ALIYUN_QUOTE_PROXY_BASE_URL=https://market-et-proxy-chbtzurmsn.cn-hangzhou.fcapp.run
```

## 部署（仅需阿里云 AK，无需在 FC 配长桥密钥）

```bash
s config add -a aliyun-fc --AccessKeyID "<AK>" --AccessKeySecret "<SK>"
cd deploy/aliyun-market-proxy && npm ci && cd ../..
npm run deploy:aliyun-market-proxy
```

当前生产 URL：`https://market-et-proxy-chbtzurmsn.cn-hangzhou.fcapp.run`

## 实现说明

- 长桥行情走 **WebSocket + legacy HTTP 签名**（`/v1/socket/token` → `wss://openapi-quote.longbridge.cn`），**不依赖** `longbridge` 原生 `.node` 绑定，避免 FC `nodejs20` 运行时 glibc 版本不兼容（`GLIBC_2.29`）。
- 健康检查 `GET /api/health` 返回 `longportTransport: "websocket"`。

## 资源

- 内存 1536 MB / 超时 30s / nodejs20
