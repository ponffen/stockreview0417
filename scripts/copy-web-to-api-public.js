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
  ["quote-smoke-test.html", "quote-smoke-test.html"],
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
  if (srcRel === "index.html") {
    const cssPath = path.join(root, "styles.css");
    const jsPath = path.join(root, "app.js");
    const cssV = fs.existsSync(cssPath) ? String(fs.statSync(cssPath).mtimeMs | 0) : Date.now();
    const jsV = fs.existsSync(jsPath) ? String(fs.statSync(jsPath).mtimeMs | 0) : Date.now();
    let html = fs.readFileSync(src, "utf8");
    html = html.replace(/href="\.\/styles\.css(?:\?[^"]*)?"/, `href="./styles.css?v=${cssV}"`);
    html = html.replace(/src="\.\/app\.js(?:\?[^"]*)?"/, `src="./app.js?v=${jsV}"`);
    fs.writeFileSync(dest, html);
    continue;
  }
  fs.copyFileSync(src, dest);
}

// eslint-disable-next-line no-console
console.log(`[vercel-build] copied web assets to ${path.relative(root, destDir)}`);
