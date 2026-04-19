const fs = require("node:fs");
const path = require("node:path");
const { normalizeTrade, importTrades } = require("../src/db");

function printUsage() {
  // eslint-disable-next-line no-console
  console.log("Usage: npm run import:trades -- --file <json-path> [--mode append|replace]");
}

function parseArgs(argv) {
  const args = { file: "", mode: "append" };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === "--file") {
      args.file = argv[i + 1] || "";
      i += 1;
    } else if (token === "--mode") {
      args.mode = argv[i + 1] || "append";
      i += 1;
    }
  }
  return args;
}

function loadTrades(filePath) {
  const absPath = path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
  const raw = fs.readFileSync(absPath, "utf-8");
  const parsed = JSON.parse(raw);
  if (Array.isArray(parsed)) {
    return parsed;
  }
  if (parsed && Array.isArray(parsed.trades)) {
    return parsed.trades;
  }
  throw new Error("JSON must be an array or an object containing trades array");
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.file) {
    printUsage();
    process.exit(1);
  }
  const mode = args.mode === "replace" ? "replace" : "append";
  const inputTrades = loadTrades(args.file).map((item) => normalizeTrade(item));
  const allTrades = importTrades(inputTrades, mode);
  // eslint-disable-next-line no-console
  console.log(`Imported ${inputTrades.length} trades (${mode}). Total trades in DB: ${allTrades.length}`);
}

main();
