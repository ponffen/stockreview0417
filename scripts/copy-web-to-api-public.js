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
  ["page-cache.js", "page-cache.js"],
  ["text-format.js", "text-format.js"],
  ["text-format-editor.js", "text-format-editor.js"],
  ["styles.css", "styles.css"],
  ["quote-smoke-test.html", "quote-smoke-test.html"],
  ["public/favicon.ico", "favicon.ico"],
  ["public/icon.png", "icon.png"],
  ["public/icon.png", "public/icon.png"],
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
    const pageCachePath = path.join(root, "page-cache.js");
    const textFormatPath = path.join(root, "text-format.js");
    const textFormatEditorPath = path.join(root, "text-format-editor.js");
    const cssV = fs.existsSync(cssPath) ? String(fs.statSync(cssPath).mtimeMs | 0) : Date.now();
    const jsV = fs.existsSync(jsPath) ? String(fs.statSync(jsPath).mtimeMs | 0) : Date.now();
    const pageCacheV = fs.existsSync(pageCachePath)
      ? String(fs.statSync(pageCachePath).mtimeMs | 0)
      : Date.now();
    const textFormatV = fs.existsSync(textFormatPath)
      ? String(fs.statSync(textFormatPath).mtimeMs | 0)
      : Date.now();
    const textFormatEditorV = fs.existsSync(textFormatEditorPath)
      ? String(fs.statSync(textFormatEditorPath).mtimeMs | 0)
      : Date.now();
    let html = fs.readFileSync(src, "utf8");
    html = html.replace(/href="\.\/styles\.css(?:\?[^"]*)?"/, `href="./styles.css?v=${cssV}"`);
    html = html.replace(
      /src="\.\/page-cache\.js(?:\?[^"]*)?"/,
      `src="./page-cache.js?v=${pageCacheV}"`,
    );
    html = html.replace(/src="\.\/text-format\.js(?:\?[^"]*)?"/, `src="./text-format.js?v=${textFormatV}"`);
    html = html.replace(
      /src="\.\/text-format-editor\.js(?:\?[^"]*)?"/,
      `src="./text-format-editor.js?v=${textFormatEditorV}"`,
    );
    html = html.replace(/src="\.\/app\.js(?:\?[^"]*)?"/, `src="./app.js?v=${jsV}"`);
    fs.writeFileSync(dest, html);
    continue;
  }
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.copyFileSync(src, dest);
}

// eslint-disable-next-line no-console
console.log(`[vercel-build] copied web assets to ${path.relative(root, destDir)}`);
