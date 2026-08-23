/** 批量指标脚本用户范围：默认 metrics_enabled=1；--all-users 扫全员。 */
function metricsUserScopeFromArgv(argv = process.argv) {
  return argv.includes("--all-users");
}

async function listBatchMetricsUserIds(options = {}) {
  const { resolveBatchMetricsUserIds } = require("../../src/db");
  const allUsers = options.allUsers === true || metricsUserScopeFromArgv(options.argv);
  return resolveBatchMetricsUserIds({ allUsers });
}

module.exports = {
  metricsUserScopeFromArgv,
  listBatchMetricsUserIds,
};
