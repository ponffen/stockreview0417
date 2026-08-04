#!/usr/bin/env bash
# 配置 Serverless Devs 并部署 EOD 定时触发 FC。
# 需要环境变量：
#   ALIYUN_ACCESS_KEY_ID
#   ALIYUN_ACCESS_KEY_SECRET
#   CRON_SECRET（与 Vercel Production 相同）
set -euo pipefail
cd "$(dirname "$0")/.."

for v in ALIYUN_ACCESS_KEY_ID ALIYUN_ACCESS_KEY_SECRET CRON_SECRET; do
  if [[ -z "${!v:-}" ]]; then
    echo "Missing env: $v" >&2
    exit 1
  fi
done

npx @serverless-devs/s config add -a aliyun-fc \
  --AccessKeyID "$ALIYUN_ACCESS_KEY_ID" \
  --AccessKeySecret "$ALIYUN_ACCESS_KEY_SECRET" \
  -f

echo "[setup] deploying stockreview-eod-cron-trigger..."
npx @serverless-devs/s deploy -t s.aliyun-eod-cron.yaml -a aliyun-fc --use-local

echo "[setup] done. Ensure Vercel Production has CRON_SECRET set to the same value."
