const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");

const dataDir = path.join(__dirname, "..", "data");
const dbPath = path.join(dataDir, "trades.db");

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new Database(dbPath);

db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    price REAL NOT NULL,
    quantity REAL NOT NULL,
    amount REAL NOT NULL,
    trade_date TEXT NOT NULL,
    note TEXT DEFAULT '',
    created_at TEXT NOT NULL
  );
`);

const hasRows = db.prepare("SELECT COUNT(1) AS count FROM trades").get().count > 0;

if (!hasRows) {
  const now = new Date().toISOString();
  const insert = db.prepare(`
    INSERT INTO trades (id, symbol, name, side, price, quantity, amount, trade_date, note, created_at)
    VALUES (@id, @symbol, @name, @side, @price, @quantity, @amount, @trade_date, @note, @created_at)
  `);

  const sampleTrades = [
    {
      id: "sample-1",
      symbol: "sz300750",
      name: "宁德时代",
      side: "buy",
      price: 443.27,
      quantity: 100,
      amount: 44327,
      trade_date: "2026-04-17",
      note: "seed",
      created_at: now,
    },
    {
      id: "sample-2",
      symbol: "sh601899",
      name: "紫金矿业",
      side: "buy",
      price: 34.68,
      quantity: 300,
      amount: 10404,
      trade_date: "2026-04-17",
      note: "seed",
      created_at: now,
    },
  ];

  const tx = db.transaction((rows) => {
    for (const row of rows) {
      insert.run(row);
    }
  });
  tx(sampleTrades);
  console.log(`Imported ${sampleTrades.length} trades into ${dbPath}`);
} else {
  console.log(`Trades table already has data in ${dbPath}; skipping seed import`);
}

const total = db.prepare("SELECT COUNT(1) AS count FROM trades").get().count;
console.log(`Total trades in database: ${total}`);

db.close();
