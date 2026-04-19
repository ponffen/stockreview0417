# stockreview0417

## Local run

```bash
npm install
npm run dev
```

Open: http://127.0.0.1:3030

## Data storage

- Trades and app settings are persisted in SQLite: `data/app.db`
- Frontend prefers `/api/*` for read/write; if backend is unavailable, it falls back to `localStorage`.

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
