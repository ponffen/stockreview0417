/**
 * contenteditable 富文本编辑器：与 DynamicsTextFormat 标记互转。
 */
(function initDynamicsTextFormatEditor(root, factory) {
  const fmt = root.DynamicsTextFormat;
  if (!fmt) {
    throw new Error("DynamicsTextFormat must load before text-format-editor.js");
  }
  root.DynamicsTextFormatEditor = factory(fmt);
})(typeof globalThis !== "undefined" ? globalThis : this, function createEditorApi(fmt) {
  function isBlockElement(el) {
    const tag = String(el?.tagName || "").toUpperCase();
    return tag === "DIV" || tag === "P";
  }

  function styleFromElement(el) {
    if (!el || el.nodeType !== Node.ELEMENT_NODE) {
      return null;
    }
    const bold =
      el.dataset.dynBold === "1" || el.tagName === "B" || el.tagName === "STRONG";
    const size = el.dataset.dynSize || "md";
    const color = el.dataset.dynColor || "black";
    if (!bold && size === "md" && color === "black") {
      return null;
    }
    return { bold, size, color };
  }

  function applyStyleToElement(el, style) {
    if (!style) {
      return;
    }
    if (style.bold) {
      el.dataset.dynBold = "1";
    }
    if (style.size && style.size !== "md") {
      el.dataset.dynSize = style.size;
    }
    if (style.color && style.color !== "black") {
      el.dataset.dynColor = style.color;
    }
  }

  function appendStyledText(parent, text, style) {
    if (!text) {
      return;
    }
    if (!style || (!style.bold && style.size === "md" && style.color === "black")) {
      parent.appendChild(document.createTextNode(text));
      return;
    }
    const span = document.createElement("span");
    applyStyleToElement(span, style);
    span.textContent = text;
    parent.appendChild(span);
  }

  function appendNewline(parent) {
    parent.appendChild(document.createElement("br"));
  }

  function markupToFragment(markup) {
    const frag = document.createDocumentFragment();
    const segments = fmt.parseToSegments(markup);
    if (!segments.length) {
      return frag;
    }
    for (const seg of segments) {
      const parts = String(seg.text || "").split("\n");
      parts.forEach((part, idx) => {
        if (idx > 0) {
          appendNewline(frag);
        }
        appendStyledText(frag, part, seg);
      });
    }
    return frag;
  }

  function setEditorContent(surface, markup) {
    if (!surface) {
      return;
    }
    surface.innerHTML = "";
    const text = fmt.normalizeNewlines(markup);
    if (!text) {
      return;
    }
    if (!fmt.hasFormatting(text)) {
      const lines = text.split("\n");
      lines.forEach((line, idx) => {
        if (idx > 0) {
          appendNewline(surface);
        }
        if (line) {
          surface.appendChild(document.createTextNode(line));
        }
      });
      return;
    }
    surface.appendChild(markupToFragment(text));
  }

  function domToSegments(surface) {
    const segments = [];
    const baseStyle = { bold: false, size: "md", color: "black" };

    function mergeStyle(inherited, local) {
      if (!local) {
        return inherited;
      }
      return {
        bold: !!(inherited.bold || local.bold),
        size: local.size !== "md" ? local.size : inherited.size,
        color: local.color !== "black" ? local.color : inherited.color,
      };
    }

    function walk(node, inherited) {
      if (node.nodeType === Node.TEXT_NODE) {
        const text = node.nodeValue || "";
        if (text) {
          segments.push({ text, ...inherited });
        }
        return;
      }
      if (node.nodeType !== Node.ELEMENT_NODE) {
        return;
      }
      const el = node;
      if (el.tagName === "BR") {
        segments.push({ text: "\n", ...inherited });
        return;
      }
      const nextInherited = mergeStyle(inherited, styleFromElement(el));
      el.childNodes.forEach((child) => walk(child, nextInherited));
    }

    const children = [...surface.childNodes];
    children.forEach((child, idx) => {
      if (idx > 0 && isBlockElement(child) && isBlockElement(children[idx - 1])) {
        segments.push({ text: "\n", ...baseStyle });
      }
      walk(child, baseStyle);
    });
    return segments;
  }

  function getEditorMarkup(surface, maxLen) {
    if (!surface) {
      return "";
    }
    const segments = domToSegments(surface);
    return fmt.sanitizeFormattedText(fmt.serializeSegments(segments), maxLen);
  }

  function autoResizeSurface(surface, minHeightPx) {
    if (!surface) {
      return;
    }
    surface.style.height = "auto";
    const min = Number(minHeightPx) || 48;
    surface.style.height = `${Math.max(min, surface.scrollHeight)}px`;
  }

  function selectionInSurface(surface) {
    const sel = window.getSelection();
    if (!sel || sel.rangeCount === 0) {
      return null;
    }
    const range = sel.getRangeAt(0);
    if (!surface.contains(range.commonAncestorContainer)) {
      return null;
    }
    return { sel, range };
  }

  function wrapRange(range, style) {
    if (range.collapsed) {
      return false;
    }
    const span = document.createElement("span");
    applyStyleToElement(span, style);
    try {
      const contents = range.extractContents();
      span.appendChild(contents);
      range.insertNode(span);
      range.selectNodeContents(span);
      return true;
    } catch {
      return false;
    }
  }

  function applyFormat(surface, format) {
    const picked = selectionInSurface(surface);
    if (!picked) {
      return false;
    }
    const { sel, range } = picked;
    const style = {};
    if (format.bold != null) {
      style.bold = !!format.bold;
    }
    if (format.size) {
      style.size = format.size;
    }
    if (format.color) {
      style.color = format.color;
    }
    const ok = wrapRange(range, style);
    if (ok) {
      sel.removeAllRanges();
      sel.addRange(range);
    }
    return ok;
  }

  function toolbarButtonHtml({ action, label, title, pressed, variant }) {
    const classes = ["dyn-fmt-tool"];
    if (variant) {
      classes.push(`dyn-fmt-tool--${variant}`);
    }
    if (pressed) {
      classes.push("is-active");
    }
    return `<button type="button" class="${classes.join(" ")}" data-dyn-fmt-action="${action}" title="${title}" aria-label="${title}">${label}</button>`;
  }

  function renderToolbarHtml() {
    return `<div class="dyn-fmt-toolbar" role="toolbar" aria-label="文字格式">
      ${toolbarButtonHtml({ action: "bold", label: "B", title: "加粗" })}
      <span class="dyn-fmt-toolbar__sep" aria-hidden="true"></span>
      ${toolbarButtonHtml({ action: "size-lg", label: "大", title: "大号" })}
      ${toolbarButtonHtml({ action: "size-md", label: "中", title: "中号（默认）" })}
      ${toolbarButtonHtml({ action: "size-sm", label: "小", title: "小号" })}
      <span class="dyn-fmt-toolbar__sep" aria-hidden="true"></span>
      ${toolbarButtonHtml({ action: "color-red", label: "红", title: "红色", variant: "red" })}
      ${toolbarButtonHtml({ action: "color-black", label: "黑", title: "黑色（默认）", variant: "black" })}
      ${toolbarButtonHtml({ action: "color-gray", label: "灰", title: "灰色", variant: "gray" })}
    </div>`;
  }

  function bindToolbar(toolbar, surface, onChange) {
    if (!toolbar || !surface) {
      return;
    }
    toolbar.addEventListener("mousedown", (event) => {
      event.preventDefault();
    });
    toolbar.addEventListener("click", (event) => {
      const btn = event.target.closest("[data-dyn-fmt-action]");
      if (!btn || !toolbar.contains(btn)) {
        return;
      }
      const action = btn.getAttribute("data-dyn-fmt-action");
      surface.focus();
      if (action === "bold") {
        applyFormat(surface, { bold: true });
      } else if (action === "size-lg") {
        applyFormat(surface, { size: "lg" });
      } else if (action === "size-md") {
        applyFormat(surface, { size: "md" });
      } else if (action === "size-sm") {
        applyFormat(surface, { size: "sm" });
      } else if (action === "color-red") {
        applyFormat(surface, { color: "red" });
      } else if (action === "color-black") {
        applyFormat(surface, { color: "black" });
      } else if (action === "color-gray") {
        applyFormat(surface, { color: "gray" });
      }
      onChange?.();
    });
  }

  function mountFormatEditor({ surface, toolbar, maxLength, minHeightPx, onChange }) {
    if (!surface) {
      return null;
    }
    if (toolbar && !toolbar.innerHTML.trim()) {
      toolbar.innerHTML = renderToolbarHtml();
    }
    bindToolbar(toolbar, surface, onChange);

    surface.addEventListener("input", () => {
      const markup = getEditorMarkup(surface, maxLength);
      if (fmt.visibleLength(markup) > maxLength) {
        setEditorContent(surface, markup);
      }
      autoResizeSurface(surface, minHeightPx);
      onChange?.();
    });

    surface.addEventListener("paste", (event) => {
      event.preventDefault();
      const text = event.clipboardData?.getData("text/plain") || "";
      if (!text) {
        return;
      }
      const remain = maxLength - fmt.visibleLength(getEditorMarkup(surface, maxLength));
      if (remain <= 0) {
        return;
      }
      const slice = text.length > remain ? text.slice(0, remain) : text;
      document.execCommand("insertText", false, slice);
      onChange?.();
    });

    autoResizeSurface(surface, minHeightPx);
    return {
      getMarkup() {
        return getEditorMarkup(surface, maxLength);
      },
      setMarkup(markup) {
        setEditorContent(surface, markup);
        autoResizeSurface(surface, minHeightPx);
        onChange?.();
      },
      focus() {
        surface.focus();
      },
      resize() {
        autoResizeSurface(surface, minHeightPx);
      },
    };
  }

  return {
    mountFormatEditor,
    setEditorContent,
    getEditorMarkup,
    autoResizeSurface,
    renderToolbarHtml,
  };
});
