# stockreview0417

## Development (Cloud Agent)

- Required runtime: Node.js 22, npm 10+
- Install dependencies: `npm install`
- Start dev server: `npm run dev` (listens on port 3030 by default)

Dev server exposes static frontend plus APIs such as `GET /api/health`, `GET /api/state`, `GET /api/trades`, and trade/settings endpoints documented below.

## Local run

```bash
npm install
npm run dev
```

Open: http://127.0.0.1:3030

## Data storage

- Trades and app settings are persisted in SQLite: `data/app.db`
- Frontend prefers `/api/*` for read/write; if the backend is unavailable, it falls back to `localStorage`.

### Daily close cache (日 K 收盘价本地表)

Table `symbol_daily_close` stores `(symbol, date, close)` so the UI can hydrate K-line from SQLite instead of calling live APIs when offline or blocked.

1. Backfill once (uses Eastmoney long history plus Sina as merge): `npm run backfill:daily-close`
2. Or with the server running: `POST /api/daily-close/backfill` (optional body `{ "symbols": ["sz300750"] }`)
3. On refresh, the app calls `GET /api/daily-close/for-trades` before fetching missing K-lines.

## GitHub Pages (static hosting)

GitHub Pages cannot run the Node server or SQLite. The deploy workflow runs `npm run build:site-state`, which writes `data/site-state.json` (same shape as `GET /api/state`) from `scripts/seed-trades.sample.json`. The browser loads that file when the API is unavailable so the public site shows seeded trades instead of only demo data.

To change what visitors see on Pages:

- If you only have sample data: edit `scripts/seed-trades.sample.json`, run `npm run build:site-state`, commit `data/site-state.json`.
- If you already maintain a local SQLite DB at `data/app.db` (for example after `npm run import:trades`), run `npm run build:site-state` and it will export from that file automatically. Use `npm run build:site-state -- --seed` to force the sample seed instead, or `--db <path>` to export from another database file.

## Batch import trades

Prepare a JSON file as either:

```json
[
  {
    "symbol": "sz300750",
    "name": "宁德时代",
    "type": "trade",
    "side": "buy",
    "price": 443.27,
    "quantity": 100,
    "amount": 44327,
    "date": "2026-04-17"
  }
]
```

or:

```json
{
  "trades": [ ... ]
}
```

Import:

```bash
npm run import:trades -- --file <your-json-file> --mode append
```

Use `--mode replace` to overwrite existing trades.
