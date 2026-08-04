# Aliyun FC EOD Cron Trigger

北京时间 **08:00（周二～六）** 调用 `https://www.higcc.com/api/cron/freeze-eod`，执行完整 EOD 流水线（日 K → 日冻结）。

Vercel 内置 Cron 不保证准点，已由本 FC 定时器接管；`vercel.json` 中不再配置 `crons`。

## 前置条件

1. **Vercel 生产环境** 设置 `CRON_SECRET`（与 FC 相同）
2. 阿里云账号已配置 Serverless Devs：`s config add -a aliyun-fc --AccessKeyID "<AK>" --AccessKeySecret "<SK>"`

生成密钥示例：

```bash
openssl rand -hex 24
```

在 [Vercel → stockreview0417 → Settings → Environment Variables](https://vercel.com/jinpengfen-projects/stockreview0417/settings/environment-variables) 添加 `CRON_SECRET`（Production）。

## 部署

```bash
export CRON_SECRET='<与 Vercel 相同>'
npm run deploy:aliyun-eod-cron
```

## 手动触发（联调）

部署后 FC 会暴露 HTTP 地址，例如：

```bash
curl -X POST "https://<fc-url>/run" -H "x-cron-secret: $CRON_SECRET"
```

健康检查：

```bash
curl "https://<fc-url>/health"
```

## 资源

- 区域：`cn-hangzhou`（与行情代理同区）
- 超时：300s（等待 Vercel EOD 跑完）
- 内存：256 MB
