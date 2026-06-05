# AGENTS.md

## Cursor Cloud specific instructions

### Overview

stockreview0417 is a Chinese stock portfolio review web app (持仓收益). It is a full-stack Node.js/Express application with a vanilla JS SPA frontend. The server runs on port 3030 by default.

### Prerequisites

- **Node.js 22.x** and **npm >= 10** (required by `engines` in `package.json`)
- **PostgreSQL (Neon)**: The app requires `DATABASE_URL` env var pointing to a Neon Postgres instance. Copy `.env.example` to `.env` if not already present — the example file contains working Neon credentials.

### Key commands

| Task | Command |
|------|---------|
| Install deps | `npm ci` (preferred; falls back to `npm install` if lockfile is missing) |
| Dev server | `npm run dev` (or `npm start`) — listens on port 3030 |
| Build (Vercel asset copy) | `npm run build` (runs `node scripts/copy-web-to-api-public.js`) |
| Syntax check server | `node --check server.js` |
| Syntax check frontend | `node --check app.js` |
| Tests | `npm test` (currently echoes "No tests configured") |
| Backfill daily close prices | `npm run backfill:daily-close` — syncs `symbol_daily_close` table from upstream APIs for all held symbols |
| Backfill daily P&L + snapshots | `npm run backfill:daily` — rebuilds `symbol_daily_pnl` and `analysis_daily_snapshot` tables (fetches FX + kline from Sina, may take ~2 min) |

### Gotchas

- There is no ESLint or TypeScript — the project is pure vanilla JS (CommonJS on server, browser globals on client).
- The `.env.example` contains real Neon Postgres credentials. If `DATABASE_URL` is not set, DB-dependent routes will fail with 503.
- `app.js` is the **frontend** SPA bundle (~286 KB, browser-only code using `window`). `server.js` is the **backend** Express entry point. Do not confuse them.
- `node --check app.js` validates syntax only; the file uses browser APIs (`window`, `fetch` without `node:` import) so it cannot be `require()`'d in Node.
- The `npm run build` script copies frontend assets into `api/public/` for Vercel deployment; it gracefully skips missing source files.
- No lint or formatting tools are configured in the project.
- Both backfill scripts (`backfill:daily-close`, `backfill:daily`) require network access to Sina Finance APIs and a valid `DATABASE_URL`. They connect to the DB, run DDL migrations, then fetch/write data. `backfill:daily` takes ~2 minutes due to fetching kline data for ~60 symbols with 120 ms throttle between each.
- The update script uses `npm ci` (deterministic, faster) when `package-lock.json` is present; falls back to `npm install` otherwise.
- **首屏数据**：SPA 启动优先 `GET /api/home/bootstrap`（设置 + 账户列表 + `dataVersion`/`rebuilding`/`frozenThrough`；成交/银证用 `/api/trades`、`/api/cash-transfers` 按需加载）。收益/资产/持仓表走 metrics API：`/api/metrics/returns`、`/api/metrics/assets`、`/api/holdings`；分析图 ` /api/series/daily-*`、`/api/analysis/stock-rank`、`/api/series/benchmark`；社区公开页 `GET /api/public/:targetId/...` 同路径。全量重算：`DATABASE_URL=... node scripts/rebuild-all-metrics.js`。Cron 运行记录：`cron_job_run` 表 + `GET /api/ops/cron-runs`（需 env `ADMIN_USER_IDS` 或 `ADMIN_PHONES`）。
- **首屏数据（旧）**：`GET /api/daily-returns` 无 `full=1` 时默认只返回最近约 800 个自然日内的行。`scripts/export-site-state.js` 仍用完整 `getState()`（含全量日收益）。
- **Vercel + Neon**：`VERCEL=1` 时 `src/db.js` 的 `q()` 每次查询独立连接；可用 `VERCEL_DB_SLOT_MAX`（默认 6）限制单实例并发连接，减轻 pooler 排队。`HOME_SUMMARY_REBUILD_DEBOUNCE_MS`（默认 12000）合并短时间内的 home-summary 重算，避免与首屏请求抢连接。
- **指标 v3**：EOD / cron 走 `metrics/freeze-v3`（`analysis_daily_snapshot` 全历史 + `symbol_daily_pnl` 仅冻结日一行）。`home-bundle` 读 v3 快照，不再写 `account_home_summary` / `symbol_home_summary`。
- **Bundle API（v2 终版）**：`GET /api/metrics/home-bundle`（`meta` + `returns.stages` + `assets` + `holdings.rows`）；`GET /api/metrics/analysis-bundle`（`meta` + 扁平 `returns` + `assets` + `series.{stageProfit,stageRate,totalAssets,marketValue,cash,cashRatio}` + `stockRank`）。对外均为已格式化字符串，无 `*Display`、无 `*Num`；读库→算→直出 JSON（`bundle-payload` 仅加 `metricsArchitecture`）。Query 支持 `account_id`（兼容 `accountScope`）。持仓表排序由前端解析字符串（规则同原 CNY/涨跌幅逻辑）。分析页不拉 `/api/trades`、`/api/cash-transfers`、`/api/surface/account-kpis`。
- **社区关注/排行卡片**：`buildUserCard` 内部 `getMetricsPublicHomeBundle`（`stages=today,mtd,ytd,inception`）；四格收益用 `returns.stages.*.rate` 展示字符串，TOP3 用 `holdings.rows` 前 3；排行按 `ytd.rate` 排序；准入：已公开且有交易或资金记录。
- **社区他人主页**：`GET /api/community/users/:id/profile` 仅身份五字段；**收益 Tab** 首屏 profile + `GET /api/public/:id/home-bundle`（`redactPublicHomeBundle`）；**交易 Tab** `GET /api/public/:id/trades`（分页 `limit`/`offset`，可选 `symbol`/`account_id`；响应 `{ ok, data[], pagination }`，行内仅 `amountShareRatio` 无 `quantity`/`amount`）；**分析 Tab** `GET /api/public/:id/analysis-bundle`；**个股记录** `GET /api/public/:id/metrics/stock-record-bundle`（`getMetricsPublicStockRecordBundle` → `redactPublicStockRecordBundle`，图表 `shares`/`marketValueNative`/`totalProfit` 指数归一化）。分析 Tab UI：复用 `#route-analysis` + `paintAnalysisFromMetricsApi(publicTargetId)`。**交易 Tab UI**：复用 `#route-trade-records`（mount 进 `[data-profile-panel="trade"]`），隐藏返回头/账户下拉；`renderTradeTable` 公开模式 6 列（`trade-table--ledger-6` + `trade-col-amt` 展示 `amountShareRatio`）。交易记录列宽/对齐：`trade-col-*` 语义类 + `--ledger-trade-col-*`（私人 7 列与公开 6 列共用）。个股记录 UI：复用 `#route-stock-record`，公开模式隐藏新增/账户下拉，成交分页同 `/trades?symbol=`。金额占比 help 文案：**占当前总资产比例**。Vercel public 路由在 `api/index.js` 直连。他人收益 Tab：**按列索引**克隆 `#route-earning` th + `paintStockTableFromMetricsRows`（`PUBLIC_EARNING_VISIBLE_COL_INDICES`）。
