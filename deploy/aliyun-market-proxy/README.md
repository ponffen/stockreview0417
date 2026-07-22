# Aliyun FC Market Proxy

国内行情代理（Function Compute），供 Vercel / 本地 `stockreview` 通过 HTTP 调用。

## 路由

| 路径 | 说明 |
|------|------|
| `GET /api/quote/longport?symbols=hk00700,usAAPL` | 长桥实时行情（JSON） |
| `GET /api/quote/tencent?q=hk_hk00700,us_aapl` | 腾讯 qt 行情（纯文本，与旧代理兼容） |
| `GET /api/health` | 健康检查 |

## 环境变量（在 FC 函数上配置，不要放在 Vercel）

| 变量 | 必填 | 说明 |
|------|------|------|
| `LONGPORT_APP_KEY` | 是 | 长桥 App Key |
| `LONGPORT_APP_SECRET` | 是 | 长桥 App Secret |
| `LONGPORT_ACCESS_TOKEN` | 是 | 长桥 Access Token |
| `LONGPORT_HTTP_URL` | 否 | 默认 `https://openapi.longbridge.cn` |
| `LONGPORT_ENABLE_OVERNIGHT` | 否 | `true` 开启美股夜盘 |
| `LONGPORT_QUOTE_TIMEOUT_MS` | 否 | 单次 quote 超时（默认 15000） |

兼容旧名：`LONGBRIDGE_*` 同上。

## 部署（Serverless Devs）

```bash
# 1. 安装 FC 依赖（含 longbridge 原生包，仅部署在国内 FC）
cd deploy/aliyun-market-proxy && npm ci && cd ../..

# 2. 配置阿里云凭证（一次性）
s config add -a aliyun-fc --AccessKeyID "<AK>" --AccessKeySecret "<SK>"

# 3. 部署前在 shell 导出长桥密钥（或部署后在 FC 控制台补环境变量）
export LONGPORT_APP_KEY="..."
export LONGPORT_APP_SECRET="..."
export LONGPORT_ACCESS_TOKEN="..."
export LONGPORT_HTTP_URL="https://openapi.longbridge.cn"
export LONGPORT_ENABLE_OVERNIGHT="true"

# 4. 部署
npm run deploy:aliyun-market-proxy
```

部署成功后终端会打印 HTTP 触发器 URL，形如：

`https://<id>.cn-hangzhou.fcapp.run`

## 接入 Vercel / 本地

在 Vercel 与本地 `.env` 设置（**不要**再放长桥密钥）：

```bash
ALIYUN_QUOTE_PROXY_BASE_URL=https://<你的fc域名>.cn-hangzhou.fcapp.run
```

应用内服务端会请求：

- `${ALIYUN_QUOTE_PROXY_BASE_URL}/api/quote/longport?symbols=...`
- `${ALIYUN_QUOTE_PROXY_BASE_URL}/api/quote/tencent?q=...`（腾讯备源，已有逻辑）

`vercel.json` 已将 `/api/quote/longport` 与 `/api/quote/tencent` rewrite 到同一 FC 基址（可通过环境变量覆盖默认域名）。

## 资源建议

- **内存**：1536 MB（longbridge 原生 SDK）
- **超时**：30 s
- **运行时**：nodejs20

已在 `s.aliyun-market-proxy.yaml` 中预设。

## 验证

```bash
curl -s "https://<fc>/api/health"
curl -s "https://<fc>/api/quote/longport?symbols=hk00700,usAAPL"
```
