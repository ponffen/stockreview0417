/**
 * Copies browser static assets into api/public/ so Vercel can bundle them
 * reliably via vercel.json includeFiles (glob under api/public/**).
 */
const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");
const destDir = path.join(root, "api", "public");

const copies = [
  ["index.html", "index.html"],
  ["app.js", "app.js"],
  ["styles.css", "styles.css"],
  ["data/site-state.json", "site-state.json"],
];

fs.mkdirSync(destDir, { recursive: true });

for (const [srcRel, destName] of copies) {
  const src = path.join(root, srcRel);
  const dest = path.join(destDir, destName);
  if (!fs.existsSync(src)) {
    // eslint-disable-next-line no-console
    console.warn(`[vercel-build] skip missing source: ${srcRel}`);
    continue;
  }
  fs.copyFileSync(src, dest);
}

// eslint-disable-next-line no-console
console.log(`[vercel-build] copied web assets to ${path.relative(root, destDir)}`);
