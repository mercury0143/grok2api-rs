let apiKey = '';
let currentConfig = {};

const ENDPOINTS = [
  {
    key: 'enable_chat_completions',
    name: 'Chat Completions',
    method: 'POST',
    path: '/v1/chat/completions',
    desc: 'OpenAI Chat Completions 兼容接口'
  },
  {
    key: 'enable_responses',
    name: 'Responses API',
    method: 'POST',
    path: '/v1/responses',
    desc: 'OpenAI Responses API 兼容接口'
  },
  {
    key: 'enable_images',
    name: 'Images Generations',
    method: 'POST',
    path: '/v1/images/generations',
    desc: '图片生成接口'
  },
  {
    key: 'enable_images_nsfw',
    name: 'Images NSFW',
    method: 'POST',
    path: '/v1/images/generations/nsfw',
    desc: 'NSFW 专用图片生成接口（会自动尝试开启 Token 的 NSFW 开关）'
  },
  {
    key: 'enable_models',
    name: 'Models',
    method: 'GET',
    path: '/v1/models',
    desc: '模型列表接口'
  },
  {
    key: 'enable_files',
    name: 'Files',
    method: 'GET',
    path: '/v1/files/image/*, /v1/files/video/*',
    desc: '缓存文件访问接口'
  }
];

const byId = (id) => document.getElementById(id);

async function fetchConfig() {
  const key = await ensureApiKey();
  if (!key) return null;
  apiKey = key;
  const res = await fetch('/api/v1/admin/config', {
    headers: { Authorization: apiKey }
  });
  if (!res.ok) {
    throw new Error('加载配置失败');
  }
  return res.json();
}

async function updateConfigPatch(patch) {
  const res = await fetch('/api/v1/admin/config', {
    method: 'POST',
    headers: {
      Authorization: apiKey,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(patch)
  });
  if (!res.ok) {
    throw new Error('更新失败');
  }
  const data = await res.json();
  return data;
}

function getEnabledValue(key) {
  const section = currentConfig.downstream || {};
  if (typeof section[key] === 'boolean') return section[key];
  return true;
}

function renderList() {
  const container = byId('endpoint-list');
  if (!container) return;
  container.innerHTML = '';

  ENDPOINTS.forEach((item) => {
    const enabled = getEnabledValue(item.key);
    const card = document.createElement('div');
    card.className = 'flex items-center justify-between gap-4 border border-[var(--border)] rounded-xl px-4 py-3 bg-white/70';
    card.innerHTML = `
      <div class="space-y-1">
        <div class="flex items-center gap-2">
          <span class="font-medium">${item.name}</span>
          <span class="text-[10px] px-2 py-0.5 rounded-full ${enabled ? 'bg-emerald-50 text-emerald-600' : 'bg-zinc-100 text-zinc-500'}">
            ${enabled ? '已启用' : '已关闭'}
          </span>
        </div>
        <div class="text-xs text-[var(--accents-4)]">${item.method} ${item.path}</div>
        <div class="text-xs text-[var(--accents-4)]">${item.desc}</div>
      </div>
      <label class="relative inline-flex items-center cursor-pointer">
        <input type="checkbox" class="sr-only peer" ${enabled ? 'checked' : ''} data-key="${item.key}">
        <div class="w-11 h-6 bg-zinc-200 rounded-full peer peer-checked:bg-emerald-500 transition-colors"></div>
        <div class="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-5"></div>
      </label>
    `;
    container.appendChild(card);
  });

  container.querySelectorAll('input[type="checkbox"]').forEach((input) => {
    input.addEventListener('change', async (e) => {
      const key = e.target.dataset.key;
      const value = !!e.target.checked;
      const patch = { downstream: { [key]: value } };
      try {
        await updateConfigPatch(patch);
        if (!currentConfig.downstream) currentConfig.downstream = {};
        currentConfig.downstream[key] = value;
        renderList();
        showToast('已更新');
      } catch (err) {
        e.target.checked = !value;
        showToast('更新失败', 'error');
      }
    });
  });
}

async function init() {
  try {
    const cfg = await fetchConfig();
    if (!cfg) return;
    currentConfig = cfg;
    renderList();
  } catch (e) {
    const container = byId('endpoint-list');
    if (container) container.innerHTML = '<div class="text-center py-12 text-[var(--accents-4)]">加载失败</div>';
  }
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
