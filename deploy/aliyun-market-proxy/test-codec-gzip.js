#!/usr/bin/env node
const zlib = require("zlib");
const { encodeResponse, decodePacket, PACKET_RESPONSE } = require("./lib/longbridge-codec");
const { encodeMultiSecurityRequest, parseSecurityQuoteResponse } = require("./lib/longbridge-proto");

const body = encodeMultiSecurityRequest(["700.HK", "AAPL.US"]);
const gzBody = zlib.gzipSync(body);
const packet = encodeResponse(11, 42, gzBody, 0);
// Simulate server gzip flag in header (type=2 response, gzip=1)
packet[0] = (PACKET_RESPONSE & 0x0f) | 0x20;

const decoded = decodePacket(packet);
if (decoded.gzip !== true) {
  throw new Error("expected gzip flag");
}
const rows = parseSecurityQuoteResponse(decoded.body);
if (rows.length !== 0) {
  // request body is not a quote response; we only verify gzip round-trip length
}
if (decoded.body.length !== body.length) {
  throw new Error("gzip decompress size mismatch");
}
console.log("OK gzip codec");
