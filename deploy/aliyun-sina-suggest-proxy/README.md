# Aliyun FC Sina Suggest Proxy

This function exposes a single route used by Vercel rewrite:

- `GET /api/sina/suggest?key=<keyword>`

It fetches Sina suggest upstream, decodes GBK content, and returns JSON:

```json
{ "ok": true, "results": [...] }
```

## Deploy (Serverless Devs)

```bash
s config add -a aliyun-fc --AccessKeyID "<AK>" --AccessKeySecret "<SK>"
s deploy -t s.aliyun-sina-suggest.yaml -a aliyun-fc
```

Current production endpoint after deployment:

- `https://market-suggest-akylmuviow.cn-hangzhou.fcapp.run/api/sina/suggest`
