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
| Install deps | `npm install` |
| Dev server | `npm run dev` (or `npm start`) — listens on port 3030 |
| Build (Vercel asset copy) | `npm run build` (runs `node scripts/copy-web-to-api-public.js`) |
| Syntax check server | `node --check server.js` |
| Syntax check frontend | `node --check app.js` |
| Tests | `npm test` (currently echoes "No tests configured") |

### Gotchas

- There is no ESLint or TypeScript — the project is pure vanilla JS (CommonJS on server, browser globals on client).
- The `.env.example` contains real Neon Postgres credentials. If `DATABASE_URL` is not set, DB-dependent routes will fail with 503.
- `app.js` is the **frontend** SPA bundle (~286 KB, browser-only code using `window`). `server.js` is the **backend** Express entry point. Do not confuse them.
- `node --check app.js` validates syntax only; the file uses browser APIs (`window`, `fetch` without `node:` import) so it cannot be `require()`'d in Node.
- The `npm run build` script copies frontend assets into `api/public/` for Vercel deployment; it gracefully skips missing source files.
- No lint or formatting tools are configured in the project.
