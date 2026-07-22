const PACKET_REQUEST = 1;
const PACKET_RESPONSE = 2;

function unpackUint24(buf, offset = 0) {
  return ((buf[offset] & 0xff) << 16) | ((buf[offset + 1] & 0xff) << 8) | (buf[offset + 2] & 0xff);
}

function encodeRequest(cmdCode, requestId, body, timeoutMs = 10000, metadata = Buffer.alloc(0)) {
  const payload = Buffer.isBuffer(body) ? body : Buffer.from(body || "");
  const meta = Buffer.isBuffer(metadata) ? metadata : Buffer.from(metadata || "");
  const out = Buffer.alloc(13 + meta.length + payload.length);
  let o = 0;
  out[o++] = PACKET_REQUEST & 0x0f;
  out[o++] = cmdCode & 0xff;
  out.writeUInt32BE(requestId >>> 0, o);
  o += 4;
  out.writeUInt16BE(timeoutMs & 0xffff, o);
  o += 2;
  out.writeUInt16BE(meta.length & 0xffff, o);
  o += 2;
  out[o++] = (payload.length >> 16) & 0xff;
  out[o++] = (payload.length >> 8) & 0xff;
  out[o++] = payload.length & 0xff;
  meta.copy(out, o);
  o += meta.length;
  payload.copy(out, o);
  return out;
}

function decodePacket(buf) {
  const data = Buffer.isBuffer(buf) ? buf : Buffer.from(buf || []);
  if (data.length < 2) {
    throw new Error("invalid packet");
  }
  const type = data[0] & 0x0f;
  const cmdCode = data[1];
  if (type === PACKET_RESPONSE) {
    if (data.length < 12) {
      throw new Error("invalid response packet");
    }
    const requestId = data.readUInt32BE(2);
    const status = data[6];
    const metadataLen = data.readUInt16BE(7);
    const bodyLen = unpackUint24(data, 9);
    const bodyStart = 12 + metadataLen;
    const body = data.subarray(bodyStart, bodyStart + bodyLen);
    return { type, cmdCode, requestId, status, body, metadataLen };
  }
  if (type === PACKET_REQUEST) {
    if (data.length < 13) {
      throw new Error("invalid request packet");
    }
    const requestId = data.readUInt32BE(2);
    const metadataLen = data.readUInt16BE(8);
    const bodyLen = unpackUint24(data, 10);
    const bodyStart = 13 + metadataLen;
    const body = data.subarray(bodyStart, bodyStart + bodyLen);
    return { type, cmdCode, requestId, status: 0, body, isRequest: true, metadataLen };
  }
  throw new Error(`unsupported packet type ${type}`);
}

function encodeResponse(cmdCode, requestId, body = Buffer.alloc(0), status = 0, metadata = Buffer.alloc(0)) {
  const payload = Buffer.isBuffer(body) ? body : Buffer.from(body || "");
  const meta = Buffer.isBuffer(metadata) ? metadata : Buffer.from(metadata || "");
  const out = Buffer.alloc(12 + meta.length + payload.length);
  let o = 0;
  out[o++] = PACKET_RESPONSE & 0x0f;
  out[o++] = cmdCode & 0xff;
  out.writeUInt32BE(requestId >>> 0, o);
  o += 4;
  out[o++] = status & 0xff;
  out.writeUInt16BE(meta.length & 0xffff, o);
  o += 2;
  out[o++] = (payload.length >> 16) & 0xff;
  out[o++] = (payload.length >> 8) & 0xff;
  out[o++] = payload.length & 0xff;
  meta.copy(out, o);
  o += meta.length;
  payload.copy(out, o);
  return out;
}

module.exports = {
  PACKET_REQUEST,
  PACKET_RESPONSE,
  encodeRequest,
  encodeResponse,
  decodePacket,
};
