function writeStringField(fieldNo, value) {
  const text = Buffer.from(String(value || ""), "utf8");
  const tag = Buffer.from([(fieldNo << 3) | 2]);
  const len = Buffer.from([text.length]);
  return Buffer.concat([tag, len, text]);
}

function writeVarintField(fieldNo, value) {
  const num = BigInt(value || 0);
  const tag = Buffer.from([(fieldNo << 3) | 0]);
  const chunks = [];
  let n = num;
  do {
    let b = Number(n & 0x7fn);
    n >>= 7n;
    if (n > 0n) {
      b |= 0x80;
    }
    chunks.push(b);
  } while (n > 0n);
  return Buffer.concat([tag, Buffer.from(chunks)]);
}

function encodeAuthRequest(token) {
  return writeStringField(1, token);
}

function encodeMultiSecurityRequest(symbols) {
  const parts = [];
  for (const sym of symbols || []) {
    parts.push(writeStringField(1, sym));
  }
  return Buffer.concat(parts);
}

function readVarint(buf, offset) {
  let result = 0n;
  let shift = 0n;
  let i = offset;
  while (i < buf.length) {
    const b = BigInt(buf[i]);
    result |= (b & 0x7fn) << shift;
    i += 1;
    if ((b & 0x80n) === 0n) {
      break;
    }
    shift += 7n;
  }
  return { value: result, offset: i };
}

function parseMessage(buf) {
  const fields = new Map();
  let i = 0;
  while (i < buf.length) {
    const tagInfo = readVarint(buf, i);
    i = tagInfo.offset;
    const tag = Number(tagInfo.value);
    const fieldNo = tag >> 3;
    const wire = tag & 0x07;
    if (wire === 0) {
      const val = readVarint(buf, i);
      i = val.offset;
      if (!fields.has(fieldNo)) {
        fields.set(fieldNo, []);
      }
      fields.get(fieldNo).push({ kind: "varint", value: val.value });
    } else if (wire === 2) {
      const lenInfo = readVarint(buf, i);
      i = lenInfo.offset;
      const len = Number(lenInfo.value);
      const slice = buf.subarray(i, i + len);
      i += len;
      if (!fields.has(fieldNo)) {
        fields.set(fieldNo, []);
      }
      fields.get(fieldNo).push({ kind: "bytes", value: slice });
    } else {
      break;
    }
  }
  return fields;
}

function fieldString(fields, no) {
  const list = fields.get(no) || [];
  const item = list[0];
  if (!item || item.kind !== "bytes") {
    return "";
  }
  return item.value.toString("utf8");
}

function fieldVarint(fields, no) {
  const list = fields.get(no) || [];
  const item = list[0];
  if (!item || item.kind !== "varint") {
    return 0;
  }
  return Number(item.value);
}

function fieldMessage(fields, no) {
  const list = fields.get(no) || [];
  const item = list[0];
  if (!item || item.kind !== "bytes") {
    return null;
  }
  return parseMessage(item.value);
}

function fieldMessages(fields, no) {
  const out = [];
  for (const item of fields.get(no) || []) {
    if (item.kind === "bytes") {
      out.push(parseMessage(item.value));
    }
  }
  return out;
}

function parseSecurityQuoteResponse(buf) {
  const root = parseMessage(buf);
  const rows = fieldMessages(root, 1);
  return rows.map((fields) => {
    const pre = fieldMessage(fields, 11);
    const post = fieldMessage(fields, 12);
    const overnight = fieldMessage(fields, 13);
    return {
      symbol: fieldString(fields, 1),
      last_done: fieldString(fields, 2),
      prev_close: fieldString(fields, 3),
      timestamp: fieldVarint(fields, 7),
      pre_market_quote: pre
        ? {
            last_done: fieldString(pre, 1),
            timestamp: fieldVarint(pre, 2),
            prev_close: fieldString(pre, 7),
          }
        : null,
      post_market_quote: post
        ? {
            last_done: fieldString(post, 1),
            timestamp: fieldVarint(post, 2),
            prev_close: fieldString(post, 7),
          }
        : null,
      over_night_quote: overnight
        ? {
            last_done: fieldString(overnight, 1),
            timestamp: fieldVarint(overnight, 2),
            prev_close: fieldString(overnight, 7),
          }
        : null,
    };
  });
}

module.exports = {
  encodeAuthRequest,
  encodeMultiSecurityRequest,
  parseSecurityQuoteResponse,
};
