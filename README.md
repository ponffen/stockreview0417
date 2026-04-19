# stockreview0417

## Development (Cloud Agent)

- Required runtime: Node.js 22, npm 10+
- Install dependencies: `npm install`
- Import seed trades into SQLite: `npm run import:trades`
- Start dev server: `npm run dev`

Dev server exposes:

- `GET /` static frontend
- `GET /api/health` runtime health check
- `GET /api/trades/count` current trade row count in SQLite
