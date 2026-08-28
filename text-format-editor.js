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
  const VISUAL_CLASSES = ["dyn-fmt--b", "dyn-fmt--lg", "dyn-fmt--sm", "dyn-fmt--red", "dyn-fmt--gray"];

  function defaultStyle() {
    return { bold: false, size: "md", color: "black" };
  }

  function normalizeEditorStyle(style) {
    const base = defaultStyle();
    const next = { ...base, ...(style || {}) };
    next.bold = !!next.bold;
    next.size = next.size === "lg" || next.size === "sm" ? next.size : "md";
    next.color = next.color === "red" || next.color === "gray" ? next.color : "black";
    return next;
  }

  function visualClassesForStyle(style) {
    const s = normalizeEditorStyle(style);
    const cls = [];
    if (s.bold) {
      cls.push("dyn-fmt--b");
    }
    if (s.size === "lg") {
      cls.push("dyn-fmt--lg");
    } else if (s.size === "sm") {
      cls.push("dyn-fmt--sm");
    }
    if (s.color === "red") {
      cls.push("dyn-fmt--red");
    } else if (s.color === "gray") {
      cls.push("dyn-fmt--gray");
    }
    return cls;
  }

  function isBlockElement(el) {
    const tag = String(el?.tagName || "").toUpperCase();
    return tag === "DIV" || tag === "P";
  }

  function styleFromElement(el) {
    if (!el || el.nodeType !== Node.ELEMENT_NODE) {
      return null;
    }
    const bold =
      el.classList.contains("dyn-fmt--b") ||
      el.dataset.dynBold === "1" ||
      el.tagName === "B" ||
      el.tagName === "STRONG";
    let size = "md";
    if (el.classList.contains("dyn-fmt--lg") || el.dataset.dynSize === "lg") {
      size = "lg";
    } else if (el.classList.contains("dyn-fmt--sm") || el.dataset.dynSize === "sm") {
      size = "sm";
    }
    let color = "black";
    if (el.classList.contains("dyn-fmt--red") || el.dataset.dynColor === "red") {
      color = "red";
    } else if (el.classList.contains("dyn-fmt--gray") || el.dataset.dynColor === "gray") {
      color = "gray";
    }
    if (!bold && size === "md" && color === "black") {
      return null;
    }
    return { bold, size, color };
  }

  function syncElementStyle(el, style) {
    if (!el || el.nodeType !== Node.ELEMENT_NODE) {
      return;
    }
    const s = normalizeEditorStyle(style);
    el.classList.remove(...VISUAL_CLASSES);
    delete el.dataset.dynBold;
    delete el.dataset.dynSize;
    delete el.dataset.dynColor;
    if (!s.bold && s.size === "md" && s.color === "black") {
      return;
    }
    if (s.bold) {
      el.dataset.dynBold = "1";
    }
    if (s.size !== "md") {
      el.dataset.dynSize = s.size;
    }
    if (s.color !== "black") {
      el.dataset.dynColor = s.color;
    }
    for (const cls of visualClassesForStyle(s)) {
      el.classList.add(cls);
    }
  }

  function mergeStyles(base, patch) {
    const prev = normalizeEditorStyle(base);
    const next = { ...prev };
    if (patch.bold != null) {
      next.bold = !!patch.bold;
    }
    if (patch.size != null) {
      next.size = patch.size;
    }
    if (patch.color != null) {
      next.color = patch.color;
    }
    return normalizeEditorStyle(next);
  }

  function appendStyledText(parent, text, style) {
    if (!text) {
      return;
    }
    const s = normalizeEditorStyle(style);
    if (!s.bold && s.size === "md" && s.color === "black") {
      parent.appendChild(document.createTextNode(text));
      return;
    }
    const span = document.createElement("span");
    syncElementStyle(span, s);
    span.textContent = text;
    parent.appendChild(span);
  }

  function appendNewline(parent) {
    parent.appendChild(document.createElement("br"));
  }

  function isBlockPlaceholder(el) {
    if (!isBlockElement(el)) {
      return false;
    }
    const children = [...el.childNodes].filter((node) => {
      return !(node.nodeType === Node.TEXT_NODE && !String(node.nodeValue || "").length);
    });
    if (!children.length) {
      return true;
    }
    return children.length === 1 && children[0].nodeType === Node.ELEMENT_NODE && children[0].tagName === "BR";
  }

  /** 块内已有尾部 <br> 时，下一块不应再插入分隔换行（否则会多出一行空行）。 */
  function blockEndsWithBreak(el) {
    if (!isBlockElement(el) || isBlockPlaceholder(el)) {
      return false;
    }
    const children = [...el.childNodes].filter((node) => {
      return !(node.nodeType === Node.TEXT_NODE && !String(node.nodeValue || "").trim());
    });
    if (!children.length) {
      return false;
    }
    const last = children[children.length - 1];
    return last.nodeType === Node.ELEMENT_NODE && last.tagName === "BR";
  }

  function isContentBlock(el) {
    return isBlockElement(el) && !isBlockPlaceholder(el);
  }

  function fragmentToSegments(root) {
    const segments = [];
    const baseStyle = defaultStyle();
    let pendingBlankLine = false;

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

    function pushNewline(inherited) {
      segments.push({ text: "\n", ...normalizeEditorStyle(inherited) });
    }

    function pushBlockSep(inherited, prevEl) {
      if (prevEl && blockEndsWithBreak(prevEl)) {
        return;
      }
      pushNewline(inherited);
    }

    function pushText(text, inherited) {
      const value = String(text || "");
      if (!value) {
        return;
      }
      if (pendingBlankLine) {
        pushNewline(inherited);
        pendingBlankLine = false;
      }
      segments.push({ text: value, ...normalizeEditorStyle(inherited) });
    }

    function walkNodes(nodes, inherited) {
      const list = [...nodes];
      list.forEach((node, idx) => {
        if (node.nodeType === Node.TEXT_NODE) {
          pushText(node.nodeValue || "", inherited);
          return;
        }
        if (node.nodeType !== Node.ELEMENT_NODE) {
          return;
        }
        const el = node;
        if (el.tagName === "BR") {
          pushNewline(inherited);
          return;
        }
        if (isBlockElement(el)) {
          if (isBlockPlaceholder(el)) {
            const prev = idx > 0 ? list[idx - 1] : null;
            const next = list[idx + 1] || null;
            const sandwiched =
              isContentBlock(prev) && isContentBlock(next);
            if (sandwiched) {
              if (idx > 0) {
                pushNewline(inherited);
              }
            } else {
              if (idx > 0) {
                pushNewline(inherited);
              }
              pendingBlankLine = true;
            }
            return;
          }
          if (idx > 0) {
            const prev = list[idx - 1];
            if (pendingBlankLine) {
              pushNewline(inherited);
              pendingBlankLine = false;
            } else if (isBlockPlaceholder(prev)) {
              // 夹在两块之间的空 div 已在 placeholder 处理里写入单个 \n
            } else {
              pushBlockSep(inherited, prev);
            }
          }
          walkNodes([...el.childNodes], inherited);
          return;
        }
        const nextInherited = mergeStyle(inherited, styleFromElement(el));
        if (pendingBlankLine) {
          pushNewline(inherited);
          pendingBlankLine = false;
        }
        walkNodes([...el.childNodes], nextInherited);
      });
    }

    const nodes =
      root instanceof DocumentFragment
        ? [...root.childNodes]
        : root && root.nodeType === Node.ELEMENT_NODE
          ? [...root.childNodes]
          : root
            ? [root]
            : [];
    walkNodes(nodes, baseStyle);
    return segments;
  }

  function segmentsToFragment(segments) {
    const frag = document.createDocumentFragment();
    for (const seg of segments || []) {
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

  function markupToFragment(markup) {
    return segmentsToFragment(fmt.parseToSegments(markup));
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
    return fragmentToSegments(surface);
  }

  function getEditorMarkup(surface, maxLen) {
    if (!surface) {
      return "";
    }
    const segments = domToSegments(surface);
    return fmt.sanitizeFormattedText(fmt.serializeSegments(segments), maxLen);
  }

  function autoResizeSurface(surface, minHeightPx, maxHeightPx) {
    if (!surface) {
      return;
    }
    surface.style.height = "auto";
    surface.style.maxHeight = "none";
    surface.style.overflowY = "hidden";
    const min = Number(minHeightPx) || 48;
    let next = Math.max(min, surface.scrollHeight);
    const max = Number(maxHeightPx);
    if (Number.isFinite(max) && max > 0 && next > max) {
      next = max;
      surface.style.overflowY = "auto";
    } else {
      surface.style.overflowY = "hidden";
    }
    surface.style.height = `${next}px`;
    surface.style.maxHeight = "";
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

  function applyFormat(surface, format) {
    const picked = selectionInSurface(surface);
    if (!picked) {
      return false;
    }
    const { sel, range } = picked;
    if (range.collapsed) {
      return false;
    }
    const extracted = range.extractContents();
    const segments = fragmentToSegments(extracted);
    if (!segments.length) {
      return false;
    }
    const nextSegments = segments.map((seg) => mergeStyles(seg, format));
    const frag = segmentsToFragment(nextSegments);
    const first = frag.firstChild;
    const last = frag.lastChild;
    range.insertNode(frag);
    if (first && last) {
      const nextRange = document.createRange();
      nextRange.setStartBefore(first);
      nextRange.setEndAfter(last);
      sel.removeAllRanges();
      sel.addRange(nextRange);
    }
    return true;
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
    let savedRange = null;

    toolbar.addEventListener("mousedown", (event) => {
      event.preventDefault();
      const picked = selectionInSurface(surface);
      savedRange = picked ? picked.range.cloneRange() : null;
    });

    toolbar.addEventListener("click", (event) => {
      const btn = event.target.closest("[data-dyn-fmt-action]");
      if (!btn || !toolbar.contains(btn)) {
        return;
      }
      const action = btn.getAttribute("data-dyn-fmt-action");
      const sel = window.getSelection();
      if (savedRange && sel) {
        sel.removeAllRanges();
        sel.addRange(savedRange);
      }
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
      savedRange = null;
      onChange?.();
    });
  }

  function mountFormatEditor({ surface, toolbar, maxLength, minHeightPx, maxHeightPx, onChange }) {
    if (!surface) {
      return null;
    }
    const resize = () => autoResizeSurface(surface, minHeightPx, maxHeightPx);
    if (toolbar && !toolbar.innerHTML.trim()) {
      toolbar.innerHTML = renderToolbarHtml();
    }
    bindToolbar(toolbar, surface, onChange);

    surface.addEventListener("input", () => {
      const markup = getEditorMarkup(surface, maxLength);
      if (fmt.visibleLength(markup) > maxLength) {
        setEditorContent(surface, markup);
      }
      resize();
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

    resize();
    return {
      getMarkup() {
        return getEditorMarkup(surface, maxLength);
      },
      setMarkup(markup) {
        setEditorContent(surface, markup);
        resize();
        onChange?.();
      },
      focus() {
        surface.focus();
      },
      resize,
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
