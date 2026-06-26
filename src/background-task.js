/**
 * Vercel：优先用 handler 注入的 context.waitUntil，其次 @vercel/functions。
 * 本地：普通 fire-and-forget。
 */
const { AsyncLocalStorage } = require("async_hooks");

const requestContext = new AsyncLocalStorage();

function runWithRequestContext(context, fn) {
  const waitUntil =
    context && typeof context.waitUntil === "function" ? context.waitUntil.bind(context) : null;
  return requestContext.run({ waitUntil }, fn);
}

function resolveWaitUntil() {
  const store = requestContext.getStore();
  if (store?.waitUntil) {
    return store.waitUntil;
  }
  if (String(process.env.VERCEL || "").trim() === "1") {
    try {
      const { waitUntil } = require("@vercel/functions");
      if (typeof waitUntil === "function") {
        return waitUntil;
      }
    } catch (e) {
      console.warn("[background-task] waitUntil unavailable", e?.message || e);
    }
  }
  return null;
}

function runInBackground(task) {
  const run = () => Promise.resolve().then(task);
  const onError = (e) => {
    console.warn("[background-task] failed", e?.message || e);
  };
  const waitUntil = resolveWaitUntil();
  if (waitUntil) {
    waitUntil(run().catch(onError));
    return;
  }
  void run().catch(onError);
}

module.exports = {
  runInBackground,
  runWithRequestContext,
};
