/**
 * LongPort WebSocket binary codec (v1 wire format on /v2 endpoint).
 * Reference: BrownSweet/longbridge-php LongbridgeCodec.php
 */
const zlib = require("zlib");

const PACKET_REQUEST = 1;
const PACKET_RESPONSE = 2;
const PACKET_PUSH = 3;

function decompressBody(body, gzip) {
  if (!gzip || !body?.length) {
    return body;
  }
  try {
    return zlib.gunzipSync(body);
  } catch (err) {
    throw new Error(`longport gzip decompress failed: ${err?.message || err}`);
  }
}

function packHeader(type, verify = false, gzip = false, reserve = 0) {
  const byte = (type & 0x0f) | ((verify ? 1 : 0) << 4) | ((gzip ? 1 : 0) << 5) | ((reserve & 0x03) << 6);
  return Buffer.from([byte]);
}

function packUint24(value) {
  const v = Number(value) || 0;
  return Buffer.from([(v >> 16) & 0xff, (v >> 8) & 0xff, v & 0xff]);
}

function unpackUint24(buf, offset = 0) {
  return ((buf[offset] & 0xff) << 16) | ((buf[offset + 1] & 0xff) << 8) | (buf[offset + 2] & 0xff);
}

function encodeRequest(cmdCode, requestId, body, timeoutMs = 10000) {
  const payload = Buffer.isBuffer(body) ? body : Buffer.from(body || "");
  const header = packHeader(PACKET_REQUEST);
  const out = Buffer.alloc(header.length + 1 + 4 + 2 + 3 + payload.length);
  let o = 0;
  header.copy(out, o);
  o += header.length;
  out[o++] = cmdCode & 0xff;
  out.writeUInt32BE(requestId >>> 0, o);
  o += 4;
  out.writeUInt16BE(timeoutMs & 0xffff, o);
  o += 2;
  packUint24(payload.length).copy(out, o);
  o += 3;
  payload.copy(out, o);
  return out;
}

function encodeResponse(cmdCode, requestId, body = Buffer.alloc(0), status = 0) {
  const payload = Buffer.isBuffer(body) ? body : Buffer.from(body || "");
  const header = packHeader(PACKET_RESPONSE);
  const out = Buffer.alloc(header.length + 1 + 4 + 1 + 3 + payload.length);
  let o = 0;
  header.copy(out, o);
  o += header.length;
  out[o++] = cmdCode & 0xff;
  out.writeUInt32BE(requestId >>> 0, o);
  o += 4;
  out[o++] = status & 0xff;
  packUint24(payload.length).copy(out, o);
  o += 3;
  payload.copy(out, o);
  return out;
}

function decodePacket(buf) {
  const data = Buffer.isBuffer(buf) ? buf : Buffer.from(buf || []);
  if (data.length < 2) {
    throw new Error("invalid packet");
  }
  const header = data[0];
  const type = header & 0x0f;
  const gzip = !!(header & 0x20);
  const cmdCode = data[1];

  if (type === PACKET_RESPONSE) {
    if (data.length < 10) {
      throw new Error("invalid response packet");
    }
    const requestId = data.readUInt32BE(2);
    const status = data[6];
    const bodyLen = unpackUint24(data, 7);
    const rawBody = data.subarray(10, 10 + bodyLen);
    const body = decompressBody(rawBody, gzip);
    return { type, cmdCode, requestId, status, body, gzip };
  }
  if (type === PACKET_REQUEST) {
    if (data.length < 11) {
      throw new Error("invalid request packet");
    }
    const requestId = data.readUInt32BE(2);
    const bodyLen = unpackUint24(data, 8);
    const rawBody = data.subarray(11, 11 + bodyLen);
    const body = decompressBody(rawBody, gzip);
    return { type, cmdCode, requestId, status: 0, body, gzip, isRequest: true };
  }
  if (type === PACKET_PUSH) {
    if (data.length < 5) {
      throw new Error("invalid push packet");
    }
    const bodyLen = unpackUint24(data, 2);
    const rawBody = data.subarray(5, 5 + bodyLen);
    const body = decompressBody(rawBody, gzip);
    return { type, cmdCode, requestId: 0, status: 0, body, gzip, isPush: true };
  }
  throw new Error(`unsupported packet type ${type}`);
}

module.exports = {
  PACKET_REQUEST,
  PACKET_RESPONSE,
  PACKET_PUSH,
  encodeRequest,
  encodeResponse,
  decodePacket,
  decompressBody,
};
