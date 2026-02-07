// storage.js - 存储管理页面逻辑

let currentConfig = null;

// 页面加载时获取配置
document.addEventListener('DOMContentLoaded', async () => {
  await loadConfig();
});

// 加载当前配置
async function loadConfig() {
  try {
    const apiKey = await ensureApiKey();
    if (!apiKey) {
      return;
    }

    const response = await fetch('/api/v1/admin/storage/config', {
      headers: {
        'Authorization': apiKey
      }
    });

    if (!response.ok) {
      throw new Error('加载配置失败');
    }

    currentConfig = await response.json();
    displayConfig(currentConfig);
  } catch (error) {
    console.error('加载配置错误:', error);
    showToast('加载配置失败：' + error.message, 'error');
  }
}

// 显示配置到表单
function displayConfig(config) {
  const type = config.type || 'local';

  // 设置存储类型单选框
  document.querySelector(`input[name="storage-type"][value="${type}"]`).checked = true;
  switchStorageType(type);

  if (type === 'local') {
    document.getElementById('local-base-dir').value = config.base_dir || 'data/tmp';
  } else if (type === 's3') {
    document.getElementById('s3-endpoint').value = config.endpoint || '';
    document.getElementById('s3-region').value = config.region || 'us-east-1';
    document.getElementById('s3-access-key').value = config.access_key || '';
    // secret_key 不显示（安全考虑）
    document.getElementById('s3-bucket').value = config.bucket || 'grok-media';
    document.getElementById('s3-custom-domain').value = config.custom_domain || '';
    document.getElementById('s3-path-prefix').value = config.path_prefix || 'grok/';
    document.getElementById('s3-upload-timeout').value = config.upload_timeout || 300;
    document.getElementById('s3-use-direct-url').checked = config.use_direct_url || false;
  }
}

// 切换存储类型
function switchStorageType(type) {
  const localConfig = document.getElementById('local-config');
  const s3Config = document.getElementById('s3-config');

  if (type === 'local') {
    localConfig.style.display = 'block';
    s3Config.style.display = 'none';
  } else {
    localConfig.style.display = 'none';
    s3Config.style.display = 'block';
  }
}

// 获取当前表单配置
function getCurrentFormConfig() {
  const type = document.querySelector('input[name="storage-type"]:checked').value;

  if (type === 'local') {
    return {
      type: 'local',
      base_dir: document.getElementById('local-base-dir').value.trim()
    };
  } else {
    const secretKey = document.getElementById('s3-secret-key').value.trim();
    // 如果用户没有输入 secret_key，发送 "***" 让后端保留原值
    const finalSecretKey = secretKey || '***';

    return {
      type: 's3',
      endpoint: document.getElementById('s3-endpoint').value.trim(),
      region: document.getElementById('s3-region').value.trim(),
      access_key: document.getElementById('s3-access-key').value.trim(),
      secret_key: finalSecretKey,
      bucket: document.getElementById('s3-bucket').value.trim(),
      custom_domain: document.getElementById('s3-custom-domain').value.trim() || null,
      path_prefix: document.getElementById('s3-path-prefix').value.trim(),
      upload_timeout: parseInt(document.getElementById('s3-upload-timeout').value) || 300,
      use_direct_url: document.getElementById('s3-use-direct-url').checked
    };
  }
}

// 测试连接
async function testConnection() {
  const btn = document.getElementById('test-btn');
  const originalText = btn.innerHTML;

  try {
    btn.disabled = true;
    btn.innerHTML = `
      <svg class="animate-spin" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M21 12a9 9 0 1 1-6.219-8.56"></path>
      </svg>
      测试中...
    `;

    const config = getCurrentFormConfig();

    // 验证必填字段
    if (config.type === 's3') {
      if (!config.access_key || !config.secret_key || !config.bucket) {
        throw new Error('请填写 Access Key、Secret Key 和 Bucket 名称');
      }
    }

    const apiKey = await ensureApiKey();
    if (!apiKey) {
      return;
    }

    const response = await fetch('/api/v1/admin/storage/test', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': apiKey
      },
      body: JSON.stringify(config)
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.message || '测试失败');
    }

    const result = await response.json();
    showToast(result.message || '连接测试成功！', 'success');
  } catch (error) {
    console.error('测试连接错误:', error);
    showToast('测试失败：' + error.message, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = originalText;
  }
}

// 保存配置
async function saveConfig() {
  const btn = document.getElementById('save-btn');
  const originalText = btn.innerHTML;

  try {
    btn.disabled = true;
    btn.innerHTML = `
      <svg class="animate-spin" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M21 12a9 9 0 1 1-6.219-8.56"></path>
      </svg>
      保存中...
    `;

    const config = getCurrentFormConfig();

    // 验证必填字段
    if (config.type === 's3') {
      if (!config.access_key) {
        throw new Error('请填写 Access Key');
      }
      // 如果是新配置（没有 currentConfig）或者 currentConfig 中没有 secret_key，则必须输入
      if (config.secret_key === '***' && (!currentConfig || !currentConfig.secret_key || currentConfig.secret_key === '')) {
        throw new Error('请填写 Secret Key');
      }
      if (!config.bucket) {
        throw new Error('请填写 Bucket 名称');
      }
    } else if (config.type === 'local') {
      if (!config.base_dir) {
        throw new Error('请填写存储目录');
      }
    }

    const apiKey = await ensureApiKey();
    if (!apiKey) {
      return;
    }

    const response = await fetch('/api/v1/admin/storage/config', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': apiKey
      },
      body: JSON.stringify(config)
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.message || '保存失败');
    }

    const result = await response.json();
    showToast(result.message || '配置已保存！', 'success');

    // 重新加载配置
    await loadConfig();
  } catch (error) {
    console.error('保存配置错误:', error);
    showToast('保存失败：' + error.message, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = originalText;
  }
}
