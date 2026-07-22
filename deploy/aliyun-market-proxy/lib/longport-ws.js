const WebSocket = require("ws");
const { legacyGet } = require("./legacy-http");
const { encodeRequest, encodeResponse, decodePacket, PACKET_RESPONSE } = require("./longbridge-codec");
const { encodeAuthRequest, encodeMultiSecurityRequest, parseSecurityQuoteResponse, parseControlError } = require("./longbridge-proto");
const {
  decimalToNum,
  formatTimestampBeijing,
  buildQuoteRecord,
  isUsTickerSymbol,
} = require("./quote-common");

const CONTROL_AUTH = 2;
const CONTROL_HEARTBEAT = 1;
const CMD_QUERY_SECURITY_QUOTE = 11;

const SESSION_LABELS = {
  pre: "盘前",
  post: "盘后",
  overnight: "夜盘",
};

let nextRequestId = 1;

function quoteWsUrl(httpUrl) {
  const raw = String(httpUrl || "https://openapi.longbridge.cn").trim();
  const u = new URL(raw);
  const host = u.hostname.includes("longbridge.cn")
    ? "openapi-quote.longbridge.cn"
    : "openapi-quote.longbridge.com";
  return `wss://${host}/v2/?version=1&codec=1&platform=9`;
}

function overnightEnabled(creds) {
  const raw = String(creds?.enableOvernight || process.env.LONGPORT_ENABLE_OVERNIGHT || "").trim();
  return raw === "1" || /^true$/i.test(raw);
}

function sessionCandidate(session, sessionLabel, block) {
  if (!block) {
    return null;
  }
  const current = decimalToNum(block.last_done);
  const prevClose = decimalToNum(block.prev_close);
  const ts = Number(block.timestamp) || 0;
  if (!Number.isFinite(current) || current <= 0 || !ts) {
    return null;
  }
  return {
    session,
    sessionLabel,
    current,
    prevClose,
    ts,
    time: formatTimestampBeijing(ts * 1000),
  };
}

function resolveUsActiveSession(quote) {
  const regular = sessionCandidate("regular", null, {
    last_done: quote.last_done,
    prev_close: quote.prev_close,
    timestamp: quote.timestamp,
  });
  const candidates = [
    sessionCandidate("overnight", SESSION_LABELS.overnight, quote.over_night_quote),
    sessionCandidate("post", SESSION_LABELS.post, quote.post_market_quote),
    sessionCandidate("pre", SESSION_LABELS.pre, quote.pre_market_quote),
    regular,
  ].filter(Boolean);
  if (!candidates.length) {
    return null;
  }
  candidates.sort((a, b) => b.ts - a.ts);
  return candidates[0];
}

function parseQuoteRow(lpSymbol, row, internalSym) {
  const sym = internalSym || lpSymbol;
  let session = "regular";
  let sessionLabel = null;
  let current = decimalToNum(row.last_done);
  let prevClose = decimalToNum(row.prev_close);
  let time = formatTimestampBeijing((Number(row.timestamp) || 0) * 1000);

  if (isUsTickerSymbol(sym)) {
    const active = resolveUsActiveSession(row);
    if (active) {
      session = active.session;
      sessionLabel = active.sessionLabel;
      current = active.current;
      prevClose = active.prevClose;
      time = active.time;
    }
  }

  return buildQuoteRecord({
    symbol: sym,
    current,
    prevClose,
    time,
    rawTime: time,
    session,
    sessionLabel,
    source: "longport",
  });
}

function waitForPacket(ws, matcher, timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("longport websocket timeout"));
    }, timeoutMs);
    const onMessage = (data) => {
      try {
        const packet = decodePacket(data);
        if (packet.isRequest && packet.cmdCode === CONTROL_HEARTBEAT) {
          ws.send(
            encodeResponse(CONTROL_HEARTBEAT, packet.requestId, packet.body, 0),
          );
          return;
        }
        if (matcher(packet)) {
          cleanup();
          resolve(packet);
        }
      } catch {
        /* ignore malformed frames */
      }
    };
    const onError = (err) => {
      cleanup();
      reject(err);
    };
    const cleanup = () => {
      clearTimeout(timer);
      ws.off("message", onMessage);
      ws.off("error", onError);
    };
    ws.on("message", onMessage);
    ws.on("error", onError);
  });
}

async function connectAndAuth(creds, timeoutMs) {
  const httpUrl = String(creds?.httpUrl || "https://openapi.longbridge.cn").trim();
  const otpPayload = await legacyGet(httpUrl, "/v1/socket/token", creds, timeoutMs);
  const otp = String(otpPayload?.otp || otpPayload?.Otp || "").trim();
  if (!otp) {
    throw new Error("longport socket otp missing");
  }

  const ws = await new Promise((resolve, reject) => {
    const socket = new WebSocket(quoteWsUrl(httpUrl), {
      handshakeTimeout: timeoutMs,
    });
    const timer = setTimeout(() => {
      socket.terminate();
      reject(new Error("longport websocket connect timeout"));
    }, timeoutMs);
    socket.once("open", () => {
      clearTimeout(timer);
      resolve(socket);
    });
    socket.once("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });

  const authReqId = nextRequestId++;
  const authBody = overnightEnabled(creds)
    ? encodeAuthRequest(otp, { need_over_night_quote: "true" })
    : encodeAuthRequest(otp);
  ws.send(
    encodeRequest(
      CONTROL_AUTH,
      authReqId,
      authBody,
      Math.min(timeoutMs, 15000),
    ),
  );
  const authResp = await waitForPacket(
    ws,
    (packet) =>
      packet.type === PACKET_RESPONSE && packet.cmdCode === CONTROL_AUTH && packet.requestId === authReqId,
    timeoutMs,
  );
  if (authResp.status !== 0) {
    ws.close();
    const detail = parseControlError(authResp.body);
    const statusNames = [
      "ok",
      "server_timeout",
      "client_timeout",
      "bad_request",
      "bad_response",
      "unauthenticated",
      "permission_denied",
      "server_internal",
      "client_internal",
    ];
    const statusLabel = statusNames[authResp.status] || `status_${authResp.status}`;
    throw new Error(
      detail
        ? `longport auth failed (${statusLabel}): ${detail}`
        : `longport auth failed (${statusLabel})`,
    );
  }
  return ws;
}

async function fetchQuotesOverWs(lpSymbols, lpToInternal, creds, timeoutMs) {
  const ws = await connectAndAuth(creds, timeoutMs);
  try {
    const reqId = nextRequestId++;
    ws.send(
      encodeRequest(
        CMD_QUERY_SECURITY_QUOTE,
        reqId,
        encodeMultiSecurityRequest(lpSymbols),
        Math.min(timeoutMs, 15000),
      ),
    );
    const resp = await waitForPacket(
      ws,
      (packet) =>
        packet.type === PACKET_RESPONSE &&
        packet.cmdCode === CMD_QUERY_SECURITY_QUOTE &&
        packet.requestId === reqId,
      timeoutMs,
    );
    if (resp.status !== 0) {
      const detail = parseControlError(resp.body);
      throw new Error(detail ? `longport quote failed: ${detail}` : `longport quote failed status ${resp.status}`);
    }
    const rows = parseSecurityQuoteResponse(resp.body);
    const quotes = {};
    for (const row of rows) {
      const lpSym = String(row.symbol || "");
      const internal = lpToInternal.get(lpSym);
      const rec = parseQuoteRow(lpSym, row, internal);
      if (rec) {
        quotes[rec.symbol] = rec;
      }
    }
    return quotes;
  } finally {
    try {
      ws.close();
    } catch {
      /* ignore */
    }
  }
}

module.exports = {
  fetchQuotesOverWs,
};
