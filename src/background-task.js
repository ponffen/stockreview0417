/**
 * Vercel：用 waitUntil 延长请求生命周期，保证 res.end 后仍能跑完 dispatch/freeze。
 * 本地：普通 fire-and-forget。
 */
function runInBackground(task) {
  const run = () => Promise.resolve().then(task);
  if (String(process.env.VERCEL || "").trim() === "1") {
    try {
      const { waitUntil } = require("@vercel/functions");
      waitUntil(
        run().catch((e) => {
          console.warn("[background-task] failed", e?.message || e);
        }),
      );
      return;
    } catch (e) {
      console.warn("[background-task] waitUntil unavailable", e?.message || e);
    }
  }
  void run().catch((e) => {
    console.warn("[background-task] failed", e?.message || e);
  });
}

module.exports = {
  runInBackground,
};
