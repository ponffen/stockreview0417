const {
  handleWellKnownProtectedResource,
  handleWellKnownAuthServer,
  handleOAuthAuthorize,
  handleOAuthToken,
} = require("./oauth-handlers");
const { handleMcpRequest } = require("./protocol");
const { handleConnectionStatus, handleConnectionRevoke } = require("./connection-api");

function normalizePathKey(pathOnly) {
  const p = String(pathOnly || "/").replace(/\/+$/, "") || "/";
  return p;
}

function matchMcpDirectRoute(pathOnly) {
  const key = normalizePathKey(pathOnly);
  if (key === "/.well-known/oauth-protected-resource") {
    return "well-known-protected";
  }
  if (key === "/.well-known/oauth-authorization-server") {
    return "well-known-auth-server";
  }
  if (key === "/oauth/authorize") {
    return "oauth-authorize";
  }
  if (key === "/oauth/token") {
    return "oauth-token";
  }
  if (key === "/mcp") {
    return "mcp";
  }
  if (key === "/api/mcp/connection-status") {
    return "connection-status";
  }
  if (key === "/api/mcp/connection") {
    return "connection-revoke";
  }
  return null;
}

async function handleMcpDirectRoute(req, res, pathOnly) {
  const route = matchMcpDirectRoute(pathOnly);
  if (!route) {
    return false;
  }
  if (route === "well-known-protected") {
    handleWellKnownProtectedResource(req, res);
    return true;
  }
  if (route === "well-known-auth-server") {
    handleWellKnownAuthServer(req, res);
    return true;
  }
  if (route === "oauth-authorize") {
    await handleOAuthAuthorize(req, res);
    return true;
  }
  if (route === "oauth-token") {
    await handleOAuthToken(req, res);
    return true;
  }
  if (route === "mcp") {
    await handleMcpRequest(req, res);
    return true;
  }
  if (route === "connection-status") {
    await handleConnectionStatus(req, res);
    return true;
  }
  if (route === "connection-revoke") {
    await handleConnectionRevoke(req, res);
    return true;
  }
  return false;
}

module.exports = {
  matchMcpDirectRoute,
  handleMcpDirectRoute,
};
