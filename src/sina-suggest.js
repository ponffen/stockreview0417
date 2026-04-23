/**
 * 新浪 suggest 接口：解析 var suggest="..."; 结果。
 * 第 2 个字段(下标1)：市场类型 111/101=A股(沪/深)及同类市场，31=港，41=美。
 * 第 3 个字段(下标2)：代码
 * 第 7 个字段(下标6)：中文名称（无则回退下标4）
 */
function suggestLineToItem(line, normalizeSymbol) {
  const parts = String(line || "").split(",");
  if (parts.length < 3) {
    return null;
  }
  const typeCode = Number.parseInt(String(parts[1] || ""), 10);
  if (!Number.isFinite(typeCode)) {
    return null;
  }
  const rawCode = String(parts[2] || "").trim();
  if (!rawCode) {
    return null;
  }
  const name = String(
    (parts.length > 6 && String(parts[6] || "").trim()) || (parts[4] || parts[3] || "").trim() || rawCode
  );
  let symbol = "";
  if (typeCode === 31) {
    const digits = rawCode.replace(/\D/g, "");
    if (digits) {
      symbol = `hk${digits.length <= 5 ? digits.padStart(5, "0") : digits.slice(-5).padStart(5, "0")}`;
    }
  } else if (typeCode === 41) {
    const al = rawCode.toUpperCase().replace(/[^A-Z0-9._]/g, "");
    if (al) {
      const tick = al.replace(/^GB_/i, "");
      symbol = `gb_${tick.toLowerCase()}`;
    }
  } else {
    // 101、111 及 ETF/其他数字代码
    symbol = rawCode;
  }
  if (normalizeSymbol) {
    symbol = normalizeSymbol(symbol);
  } else {
    symbol = String(symbol).trim();
  }
  if (!symbol) {
    return null;
  }
  const market =
    typeCode === 31 ? "港" : typeCode === 41 ? "美" : typeCode === 101 || typeCode === 111 ? "沪/深" : "其他";
  return { typeCode, code: rawCode, name, symbol, market };
}

function parseSinaSuggestText(text) {
  const m = /var suggest="([^"]*)"/.exec(String(text || ""));
  if (!m) {
    return [];
  }
  const raw = m[1].trim();
  if (!raw) {
    return [];
  }
  const lines = raw.split(/;/).map((s) => s.trim()).filter(Boolean);
  return lines;
}

module.exports = {
  suggestLineToItem,
  parseSinaSuggestText,
};
