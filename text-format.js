/**
 * 动态/成交备注轻量富文本：加粗、大中小、红黑灰、换行。
 * 存储为纯文本标记，如 [b][c:red][s:lg]文本[/s][/c][/b]
 */
(function initDynamicsTextFormat(root, factory) {
  if (typeof module !== "undefined" && module.exports) {
    module.exports = factory();
    return;
  }
  root.DynamicsTextFormat = factory();
})(typeof globalThis !== "undefined" ? globalThis : this, function dynamicsTextFormatFactory() {
  const TAG_RE = /\[(b|s:(?:lg|md|sm)|c:(?:red|gray)|\/b|\/s|\/c)\]/g;

  const DEFAULT_STYLE = Object.freeze({ bold: false, size: "md", color: "black" });

  function normalizeNewlines(raw) {
    return String(raw ?? "")
      .replace(/\r\n/g, "\n")
      .replace(/\r/g, "\n");
  }

  function cloneStyle(style) {
    return {
      bold: !!style.bold,
      size: style.size || "md",
      color: style.color || "black",
    };
  }

  function stylesEqual(a, b) {
    return a.bold === b.bold && a.size === b.size && a.color === b.color;
  }

  function applyOpenTag(style, tag) {
    const next = cloneStyle(style);
    if (tag === "b") {
      next.bold = true;
      return next;
    }
    if (tag.startsWith("s:")) {
      const size = tag.slice(2);
      if (size === "lg" || size === "md" || size === "sm") {
        next.size = size;
      }
      return next;
    }
    if (tag.startsWith("c:")) {
      const color = tag.slice(2);
      if (color === "red" || color === "gray") {
        next.color = color;
      }
      return next;
    }
    return next;
  }

  function applyCloseTag(style, tag) {
    const next = cloneStyle(style);
    if (tag === "/b") {
      next.bold = false;
      return next;
    }
    if (tag === "/s") {
      next.size = "md";
      return next;
    }
    if (tag === "/c") {
      next.color = "black";
      return next;
    }
    return next;
  }

  function parseToSegments(raw) {
    const text = normalizeNewlines(raw);
    const segments = [];
    const stack = cloneStyle(DEFAULT_STYLE);
    let last = 0;
    let match;
    TAG_RE.lastIndex = 0;

    function pushText(chunk) {
      if (!chunk) {
        return;
      }
      segments.push({
        text: chunk,
        bold: stack.bold,
        size: stack.size,
        color: stack.color,
      });
    }

    while ((match = TAG_RE.exec(text)) !== null) {
      pushText(text.slice(last, match.index));
      const tag = match[1];
      if (tag === "b" || tag.startsWith("s:") || tag.startsWith("c:")) {
        Object.assign(stack, applyOpenTag(stack, tag));
      } else {
        Object.assign(stack, applyCloseTag(stack, tag));
      }
      last = match.index + match[0].length;
    }
    pushText(text.slice(last));
    return segments;
  }

  function visibleLength(raw) {
    return parseToSegments(raw).reduce((sum, seg) => sum + seg.text.length, 0);
  }

  function hasFormatting(raw) {
    TAG_RE.lastIndex = 0;
    return TAG_RE.test(String(raw ?? ""));
  }

  function openTagsForStyle(style) {
    const tags = [];
    if (style.bold) {
      tags.push("[b]");
    }
    if (style.size && style.size !== "md") {
      tags.push(`[s:${style.size}]`);
    }
    if (style.color && style.color !== "black") {
      tags.push(`[c:${style.color}]`);
    }
    return tags;
  }

  function closeTagsForStyle(style) {
    const tags = [];
    if (style.color && style.color !== "black") {
      tags.push("[/c]");
    }
    if (style.size && style.size !== "md") {
      tags.push("[/s]");
    }
    if (style.bold) {
      tags.push("[/b]");
    }
    return tags;
  }

  function serializeSegments(segments) {
    let out = "";
    let active = cloneStyle(DEFAULT_STYLE);
    for (const seg of segments || []) {
      const next = {
        bold: !!seg.bold,
        size: seg.size || "md",
        color: seg.color || "black",
      };
      if (!stylesEqual(active, next)) {
        out += closeTagsForStyle(active).join("");
        out += openTagsForStyle(next).join("");
        active = next;
      }
      out += seg.text;
    }
    out += closeTagsForStyle(active).join("");
    return out;
  }

  function sanitizeFormattedText(raw, maxLen) {
    const limit = Math.max(0, Number(maxLen) || 0);
    const segments = parseToSegments(raw);
    if (!limit) {
      return serializeSegments(segments).trim();
    }
    let used = 0;
    const kept = [];
    for (const seg of segments) {
      const text = String(seg.text || "");
      if (!text) {
        continue;
      }
      const remain = limit - used;
      if (remain <= 0) {
        break;
      }
      const slice = text.length > remain ? text.slice(0, remain) : text;
      used += slice.length;
      kept.push({ ...seg, text: slice });
    }
    return serializeSegments(kept).trim();
  }

  function stripFormatting(raw) {
    return parseToSegments(raw)
      .map((seg) => seg.text)
      .join("");
  }

  function escapeHtml(text) {
    return String(text ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function classesForStyle(style) {
    const cls = ["dyn-fmt"];
    if (style.bold) {
      cls.push("dyn-fmt--b");
    }
    if (style.size === "lg") {
      cls.push("dyn-fmt--lg");
    } else if (style.size === "sm") {
      cls.push("dyn-fmt--sm");
    }
    if (style.color === "red") {
      cls.push("dyn-fmt--red");
    } else if (style.color === "gray") {
      cls.push("dyn-fmt--gray");
    }
    return cls.length > 1 ? cls.join(" ") : "";
  }

  function linkifyEscapedHtml(escaped) {
    return escaped.replace(/(https?:\/\/[^\s<]+)/g, (url) => {
      return `<a href="${url}" target="_blank" rel="noopener noreferrer">${url}</a>`;
    });
  }

  function renderSegmentHtml(seg) {
    const parts = String(seg.text || "").split("\n");
    const chunks = parts.map((part, idx) => {
      const escaped = linkifyEscapedHtml(escapeHtml(part));
      const withBreak = idx < parts.length - 1 ? `${escaped}<br />` : escaped;
      const cls = classesForStyle(seg);
      if (!cls) {
        return withBreak;
      }
      return `<span class="${cls}">${withBreak}</span>`;
    });
    return chunks.join("");
  }

  function renderFormattedTextToHtml(raw) {
    const segments = parseToSegments(raw);
    if (!segments.length) {
      return "";
    }
    return segments.map((seg) => renderSegmentHtml(seg)).join("");
  }

  return {
    TAG_RE,
    DEFAULT_STYLE,
    normalizeNewlines,
    parseToSegments,
    visibleLength,
    hasFormatting,
    serializeSegments,
    sanitizeFormattedText,
    stripFormatting,
    renderFormattedTextToHtml,
    escapeHtml,
  };
});
