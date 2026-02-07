let apiKey = '';
let textHistory = [];
let isSending = false;

const byId = (id) => document.getElementById(id);

function escapeHtml(text) {
  if (!text) return '';
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function isTextApi(api) {
  return api === 'chat' || api === 'responses';
}

function getApiPath(api) {
  if (api === 'chat') return '/v1/chat/completions';
  if (api === 'responses') return '/v1/responses';
  if (api === 'images') return '/v1/images/generations';
  return '/v1/images/generations/nsfw';
}

function applyApiMode() {
  const api = byId('api-select')?.value || 'chat';
  const textMode = isTextApi(api);

  const streamGroup = byId('stream-group');
  const imageParams = byId('image-params');
  const modelGroup = byId('model-group');
  const modelInput = byId('model-input');

  if (streamGroup) streamGroup.classList.toggle('hidden', !textMode);
  if (imageParams) imageParams.classList.toggle('hidden', textMode);

  if (modelGroup) {
    const label = modelGroup.querySelector('label');
    if (label) {
      label.textContent = textMode ? '模型' : '模型（可选）';
    }
  }

  if (modelInput && !modelInput.value.trim()) {
    modelInput.value = 'grok-4';
  }
}

function ensureEmptyState() {
  const container = byId('dialog-messages');
  if (!container) return;
  if (container.children.length > 0) return;

  const empty = document.createElement('div');
  empty.className = 'dialog-empty';
  empty.id = 'dialog-empty';
  empty.textContent = '开始一段对话吧。';
  container.appendChild(empty);
}

function clearEmptyState() {
  const empty = byId('dialog-empty');
  if (empty) empty.remove();
}

function scrollMessagesToBottom() {
  const container = byId('dialog-messages');
  if (!container) return;
  container.scrollTop = container.scrollHeight;
}

function createMessageBubble(role) {
  const container = byId('dialog-messages');
  if (!container) return null;
  clearEmptyState();

  const bubble = document.createElement('div');
  bubble.className = `dialog-bubble ${role}`;

  const roleEl = document.createElement('div');
  roleEl.className = 'dialog-role';
  roleEl.textContent = role === 'user' ? '用户' : '助手';

  const textEl = document.createElement('div');
  textEl.className = 'dialog-text';

  const imagesEl = document.createElement('div');
  imagesEl.className = 'dialog-images hidden';

  bubble.appendChild(roleEl);
  bubble.appendChild(textEl);
  bubble.appendChild(imagesEl);
  container.appendChild(bubble);

  scrollMessagesToBottom();

  return { bubble, textEl, imagesEl };
}

function renderInlineMarkdown(text) {
  let html = text;

  html = html.replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, (_all, label, url) => {
    return `<a href="${url}" target="_blank" rel="noopener noreferrer">${label}</a>`;
  });

  html = html.replace(/`([^`]+)`/g, '<code>$1</code>');
  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/\*([^*\n]+)\*/g, '<em>$1</em>');
  html = html.replace(/~~([^~\n]+)~~/g, '<del>$1</del>');

  return html;
}

function renderMarkdown(text) {
  const raw = String(text || '').replace(/\r\n/g, '\n');
  if (!raw.trim()) {
    return '';
  }

  const codeBlocks = [];
  let source = raw.replace(/```([a-zA-Z0-9_-]+)?\n([\s\S]*?)```/g, (_all, language, code) => {
    const idx = codeBlocks.length;
    const lang = (language || '').trim();
    const langBadge = lang
      ? `<span class="dialog-code-lang">${escapeHtml(lang)}</span>`
      : '';
    codeBlocks.push(`<pre class="dialog-code">${langBadge}<code>${escapeHtml(code)}</code></pre>`);
    return `@@CODE_BLOCK_${idx}@@`;
  });

  source = escapeHtml(source);

  const lines = source.split('\n');
  const html = [];
  let paragraph = [];
  let inUl = false;
  let inOl = false;

  const flushParagraph = () => {
    if (paragraph.length === 0) return;
    html.push(`<p>${renderInlineMarkdown(paragraph.join('<br>'))}</p>`);
    paragraph = [];
  };

  const closeLists = () => {
    if (inUl) {
      html.push('</ul>');
      inUl = false;
    }
    if (inOl) {
      html.push('</ol>');
      inOl = false;
    }
  };

  lines.forEach((line) => {
    const codeMatch = line.match(/^@@CODE_BLOCK_(\d+)@@$/);
    if (codeMatch) {
      flushParagraph();
      closeLists();
      html.push(`@@CODE_BLOCK_${codeMatch[1]}@@`);
      return;
    }

    if (!line.trim()) {
      flushParagraph();
      closeLists();
      return;
    }

    const headingMatch = line.match(/^(#{1,6})\s+(.*)$/);
    if (headingMatch) {
      flushParagraph();
      closeLists();
      const level = headingMatch[1].length;
      html.push(`<h${level}>${renderInlineMarkdown(headingMatch[2])}</h${level}>`);
      return;
    }

    const quoteMatch = line.match(/^>\s?(.*)$/);
    if (quoteMatch) {
      flushParagraph();
      closeLists();
      html.push(`<blockquote>${renderInlineMarkdown(quoteMatch[1])}</blockquote>`);
      return;
    }

    const ulMatch = line.match(/^[-*]\s+(.*)$/);
    if (ulMatch) {
      flushParagraph();
      if (inOl) {
        html.push('</ol>');
        inOl = false;
      }
      if (!inUl) {
        html.push('<ul>');
        inUl = true;
      }
      html.push(`<li>${renderInlineMarkdown(ulMatch[1])}</li>`);
      return;
    }

    const olMatch = line.match(/^\d+\.\s+(.*)$/);
    if (olMatch) {
      flushParagraph();
      if (inUl) {
        html.push('</ul>');
        inUl = false;
      }
      if (!inOl) {
        html.push('<ol>');
        inOl = true;
      }
      html.push(`<li>${renderInlineMarkdown(olMatch[1])}</li>`);
      return;
    }

    paragraph.push(line);
  });

  flushParagraph();
  closeLists();

  let rendered = html.join('\n');
  rendered = rendered.replace(/@@CODE_BLOCK_(\d+)@@/g, (_all, idx) => {
    const i = Number(idx);
    return codeBlocks[i] || '';
  });

  return rendered;
}

function setBubbleContent(target, text, images) {
  if (!target) return;
  const cleanText = String(text || '');
  const imageList = Array.isArray(images) ? images : [];

  const html = renderMarkdown(cleanText);
  target.textEl.innerHTML = html;
  target.textEl.style.display = html ? 'block' : 'none';

  target.imagesEl.innerHTML = '';
  imageList.forEach((src, idx) => {
    if (!src) return;
    const img = document.createElement('img');
    img.src = src;
    img.alt = `generated-image-${idx + 1}`;
    img.loading = 'lazy';
    target.imagesEl.appendChild(img);
  });
  target.imagesEl.classList.toggle('hidden', imageList.length === 0);

  scrollMessagesToBottom();
}

function appendMessage(role, text = '', images = []) {
  const target = createMessageBubble(role);
  setBubbleContent(target, text, images);
  return target;
}

function extractMarkdownImages(content) {
  const images = [];
  if (!content) return { text: '', images };

  let text = String(content);

  text = text.replace(/!\[[^\]]*\]\(([^)\s]+)\)/g, (_, url) => {
    images.push(url);
    return '';
  });

  text = text.replace(/data:image\/[a-zA-Z0-9.+-]+;base64,[A-Za-z0-9+/=]+/g, (uri) => {
    images.push(uri);
    return '';
  });

  const plain = text.trim();
  const uniqueImages = [...new Set(images.filter(Boolean))];
  return { text: plain, images: uniqueImages };
}

function normalizeImageSource(item) {
  if (!item) return null;
  if (item.url && typeof item.url === 'string') {
    return item.url;
  }
  if (item.b64_json && typeof item.b64_json === 'string') {
    return `data:image/png;base64,${item.b64_json}`;
  }
  return null;
}

function extractResponseText(json) {
  if (!json || typeof json !== 'object') return '';

  if (typeof json.output_text === 'string') {
    return json.output_text;
  }

  const chatText = json?.choices?.[0]?.message?.content;
  if (typeof chatText === 'string') {
    return chatText;
  }

  const output = Array.isArray(json.output) ? json.output : [];
  for (const item of output) {
    const content = Array.isArray(item.content) ? item.content : [];
    for (const block of content) {
      if (block.type === 'output_text' && typeof block.text === 'string') {
        return block.text;
      }
    }
  }

  return '';
}

async function parseErrorResponse(res) {
  let message = `HTTP ${res.status}`;
  try {
    const payload = await res.json();
    const detail = payload?.error?.message || payload?.detail || payload?.message;
    if (detail) message = detail;
  } catch (_) {
    // ignore
  }
  return message;
}

async function readSseStream(response, api, onDelta) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let dataLines = [];

  const dispatchData = (data) => {
    if (!data || data === '[DONE]') {
      return;
    }
    let json;
    try {
      json = JSON.parse(data);
    } catch (_) {
      return;
    }

    if (api === 'chat') {
      const delta = json?.choices?.[0]?.delta?.content;
      if (typeof delta === 'string' && delta.length > 0) {
        onDelta(delta);
      }
      return;
    }

    if (api === 'responses') {
      if (json.type === 'response.output_text.delta') {
        const delta = json.delta || '';
        if (delta) onDelta(delta);
      }
      if (json.type === 'response.output_text.done' && typeof json.text === 'string') {
        onDelta('', json.text);
      }
    }
  };

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, idx).replace(/\r$/, '');
      buffer = buffer.slice(idx + 1);

      if (line.length === 0) {
        if (dataLines.length > 0) {
          dispatchData(dataLines.join('\n'));
          dataLines = [];
        }
        continue;
      }

      if (line.startsWith('data:')) {
        dataLines.push(line.slice(5).trimStart());
      }
    }
  }

  if (dataLines.length > 0) {
    dispatchData(dataLines.join('\n'));
  }
}

async function sendText(api, prompt) {
  const model = byId('model-input')?.value?.trim() || '';
  if (!model) {
    showToast('文本接口需要填写模型', 'error');
    return;
  }

  const streamEnabled = !!byId('stream-toggle')?.checked;
  textHistory.push({ role: 'user', content: prompt });

  const payload = {
    model,
    messages: textHistory,
    stream: streamEnabled
  };

  const assistantBubble = appendMessage('assistant', streamEnabled ? '...' : '', []);

  const res = await fetch(getApiPath(api), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...buildAuthHeaders(apiKey)
    },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const err = await parseErrorResponse(res);
    setBubbleContent(assistantBubble, `请求失败：${err}`, []);
    throw new Error(err);
  }

  let finalText = '';
  if (streamEnabled) {
    await readSseStream(res, api, (delta, overrideText = null) => {
      if (typeof overrideText === 'string') {
        finalText = overrideText;
      } else {
        finalText += delta;
      }
      setBubbleContent(assistantBubble, finalText || '...', []);
    });
  } else {
    const json = await res.json();
    finalText = extractResponseText(json);
  }

  const parsed = extractMarkdownImages(finalText);
  setBubbleContent(assistantBubble, parsed.text || '(空响应)', parsed.images);
  textHistory.push({ role: 'assistant', content: finalText });
}

async function sendImage(api, prompt) {
  const model = byId('model-input')?.value?.trim() || '';
  const n = Math.max(1, Math.min(4, Number(byId('image-count')?.value || 1)));
  const size = byId('image-size')?.value || '1024x1024';
  const responseFormat = byId('response-format')?.value || 'url';

  const payload = {
    prompt,
    n,
    size,
    response_format: responseFormat,
    stream: false
  };

  if (api === 'images' && model) {
    payload.model = model;
  }

  const assistantBubble = appendMessage('assistant', '生成中...', []);

  const res = await fetch(getApiPath(api), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...buildAuthHeaders(apiKey)
    },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const err = await parseErrorResponse(res);
    setBubbleContent(assistantBubble, `请求失败：${err}`, []);
    throw new Error(err);
  }

  const json = await res.json();
  const list = Array.isArray(json.data) ? json.data : [];
  const images = list.map(normalizeImageSource).filter(Boolean);

  if (images.length === 0) {
    setBubbleContent(assistantBubble, '生成完成，但未返回图片。', []);
  } else {
    setBubbleContent(assistantBubble, `生成完成，共 ${images.length} 张。`, images);
  }
}

async function sendMessage() {
  if (isSending) return;

  const input = byId('user-input');
  const api = byId('api-select')?.value || 'chat';
  const prompt = input?.value?.trim() || '';

  if (!prompt) {
    showToast('请输入内容', 'error');
    return;
  }

  appendMessage('user', prompt, []);

  isSending = true;
  const sendBtn = byId('send-btn');
  if (sendBtn) sendBtn.disabled = true;

  try {
    if (isTextApi(api)) {
      await sendText(api, prompt);
    } else {
      await sendImage(api, prompt);
    }
    if (input) input.value = '';
  } catch (err) {
    showToast(String(err?.message || err || '请求失败'), 'error');
  } finally {
    isSending = false;
    if (sendBtn) sendBtn.disabled = false;
  }
}

function clearDialog() {
  textHistory = [];
  const container = byId('dialog-messages');
  if (container) container.innerHTML = '';
  ensureEmptyState();
}

async function init() {
  apiKey = await ensureApiKey();
  if (apiKey === null) return;

  ensureEmptyState();
  applyApiMode();

  byId('api-select')?.addEventListener('change', () => {
    applyApiMode();
  });

  byId('send-btn')?.addEventListener('click', sendMessage);
  byId('clear-btn')?.addEventListener('click', clearDialog);
  byId('user-input')?.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      sendMessage();
    }
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
