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

## GitHub Pages (static hosting)

GitHub Pages cannot run the Node server or SQLite. The deploy workflow runs `npm run build:site-state`, which writes `data/site-state.json` (same shape as `GET /api/state`) from `scripts/seed-trades.sample.json`. The browser loads that file when the API is unavailable so the public site shows seeded trades instead of only demo data.

To change what visitors see on Pages, edit `scripts/seed-trades.sample.json` (or point the export script at another JSON source), run `npm run build:site-state`, and commit the updated `data/site-state.json`.

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
