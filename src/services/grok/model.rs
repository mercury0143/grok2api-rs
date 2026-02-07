use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Basic,
    Super,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Cost {
    Low,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    pub model_id: String,
    pub grok_model: String,
    pub model_mode: String,
    pub tier: Tier,
    pub cost: Cost,
    pub display_name: String,
    pub description: String,
    pub is_video: bool,
    pub is_image: bool,
}

impl ModelInfo {
    pub fn new(model_id: &str, grok_model: &str, mode: &str, display: &str) -> Self {
        Self {
            model_id: model_id.to_string(),
            grok_model: grok_model.to_string(),
            model_mode: mode.to_string(),
            tier: Tier::Basic,
            cost: Cost::Low,
            display_name: display.to_string(),
            description: String::new(),
            is_video: false,
            is_image: false,
        }
    }
}

pub struct ModelService;

impl ModelService {
    pub fn list() -> Vec<ModelInfo> {
        let mut models = vec![
            ModelInfo::new("grok-3", "grok-3", "MODEL_MODE_AUTO", "Grok 3"),
            ModelInfo::new("grok-3-fast", "grok-3", "MODEL_MODE_FAST", "Grok 3 Fast"),
            ModelInfo::new("grok-4", "grok-4", "MODEL_MODE_AUTO", "Grok 4"),
            ModelInfo::new(
                "grok-4-mini",
                "grok-4-mini-thinking-tahoe",
                "MODEL_MODE_GROK_4_MINI_THINKING",
                "Grok 4 Mini",
            ),
            ModelInfo::new("grok-4-fast", "grok-4", "MODEL_MODE_FAST", "Grok 4 Fast"),
            {
                let mut m =
                    ModelInfo::new("grok-4-heavy", "grok-4", "MODEL_MODE_HEAVY", "Grok 4 Heavy");
                m.tier = Tier::Super;
                m.cost = Cost::High;
                m
            },
            ModelInfo::new(
                "grok-4.1",
                "grok-4-1-thinking-1129",
                "MODEL_MODE_AUTO",
                "Grok 4.1",
            ),
            {
                let mut m = ModelInfo::new(
                    "grok-4.1-thinking",
                    "grok-4-1-thinking-1129",
                    "MODEL_MODE_GROK_4_1_THINKING",
                    "Grok 4.1 Thinking",
                );
                m.cost = Cost::High;
                m
            },
            {
                let mut m = ModelInfo::new(
                    "grok-imagine-1.0",
                    "grok-3",
                    "MODEL_MODE_FAST",
                    "Grok Image",
                );
                m.cost = Cost::High;
                m.is_image = true;
                m.description = "Image generation model".to_string();
                m
            },
            {
                let mut m = ModelInfo::new(
                    "grok-imagine-1.0-video",
                    "grok-3",
                    "MODEL_MODE_FAST",
                    "Grok Video",
                );
                m.cost = Cost::High;
                m.is_video = true;
                m.description = "Video generation model".to_string();
                m
            },
            {
                let mut m = ModelInfo::new(
                    "grok-video-3",
                    "grok-3",
                    "MODEL_MODE_FAST",
                    "Grok Video 3",
                );
                m.cost = Cost::High;
                m.is_video = true;
                m.description = "3-second video generation model".to_string();
                m
            },
            {
                let mut m = ModelInfo::new(
                    "grok-video-3-pro",
                    "grok-3",
                    "MODEL_MODE_FAST",
                    "Grok Video 3 Pro",
                );
                m.cost = Cost::High;
                m.is_video = true;
                m.description = "6-second video generation model".to_string();
                m
            },
        ];
        models
    }

    pub fn get(model_id: &str) -> Option<ModelInfo> {
        Self::list().into_iter().find(|m| m.model_id == model_id)
    }

    pub fn valid(model_id: &str) -> bool {
        Self::get(model_id).is_some()
    }

    pub fn pool_for_model(model_id: &str) -> String {
        if let Some(m) = Self::get(model_id) {
            if m.tier == Tier::Super {
                return "ssoSuper".to_string();
            }
        }
        "ssoBasic".to_string()
    }
}
/*
  文本对话模型
  ┌───────────────────┬───────────────────┬────────────────────┐
  │     model_id      │       说明        │        备注        │
  ├───────────────────┼───────────────────┼────────────────────┤
  │ grok-3            │ Grok 3            │ 基础模型           │
  ├───────────────────┼───────────────────┼────────────────────┤
  │ grok-3-fast       │ Grok 3 Fast       │ 快速模式           │
  ├───────────────────┼───────────────────┼────────────────────┤
  │ grok-4            │ Grok 4            │ 默认模型           │
  ├───────────────────┼───────────────────┼────────────────────┤
  │ grok-4-mini       │ Grok 4 Mini       │ 轻量思考模型       │
  ├───────────────────┼───────────────────┼────────────────────┤
  │ grok-4-fast       │ Grok 4 Fast       │ 快速模式           │
  ├───────────────────┼───────────────────┼────────────────────┤
  │ grok-4-heavy      │ Grok 4 Heavy      │ Super 级别，高消耗 │
  ├───────────────────┼───────────────────┼────────────────────┤
  │ grok-4.1          │ Grok 4.1          │ 新版模型           │
  ├───────────────────┼───────────────────┼────────────────────┤
  │ grok-4.1-thinking │ Grok 4.1 Thinking │ 深度思考，高消耗   │
  └───────────────────┴───────────────────┴────────────────────┘
  图片生成模型
  ┌──────────────────┬──────────┐
  │     model_id     │   说明   │
  ├──────────────────┼──────────┤
  │ grok-imagine-1.0 │ 图片生成 │
  └──────────────────┴──────────┘
  视频生成模型
  ┌────────────────────────┬──────────────┐
  │        model_id        │     说明     │
  ├────────────────────────┼──────────────┤
  │ grok-imagine-1.0-video │ 视频生成     │
  ├────────────────────────┼──────────────┤
  │ grok-video-3           │ 3 秒视频生成 │
  ├────────────────────────┼──────────────┤
  │ grok-video-3-pro       │ 6 秒视频生成 │
  └────────────────────────┴──────────────┘
  其中 grok-4-heavy 和 grok-4.1-thinking 需要 Super 级别的 token（ssoSuper 池），其余使用基础 token（ssoBasic 池）。对话页面默认填入的是 grok-4。



  ┌──────────────┬──────────────────────────────────────────────────────────────────────────┐
  │     字段     │                                   含义                                   │
  ├──────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ model_id     │ 对外暴露的模型 ID，用户请求时传入的名称，如 grok-4                       │
  ├──────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ grok_model   │ 实际调用 Grok 上游 API 时使用的模型名，如 grok-4、grok-4-1-thinking-1129 │
  ├──────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ model_mode   │ Grok 上游的运行模式，控制推理行为                                        │
  ├──────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ tier         │ 令牌池级别：Basic（普通 token）或 Super（高级 token）                    │
  ├──────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ cost         │ 消耗级别：Low（低消耗）或 High（高消耗）                                 │
  ├──────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ display_name │ 显示名称，如 "Grok 4 Heavy"                                              │
  ├──────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ description  │ 模型描述，文本模型为空，图片/视频模型有说明                              │
  ├──────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ is_video     │ 是否为视频生成模型                                                       │
  ├──────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ is_image     │ 是否为图片生成模型                                                       │
  └──────────────┴──────────────────────────────────────────────────────────────────────────┘
  model_mode 的作用

  这是发给 Grok 上游的关键参数，决定模型的推理方式：
  ┌─────────────────────────────────┬──────────────────────────────────┐
  │              mode               │               含义               │
  ├─────────────────────────────────┼──────────────────────────────────┤
  │ MODEL_MODE_AUTO                 │ 自动模式（标准推理）             │
  ├─────────────────────────────────┼──────────────────────────────────┤
  │ MODEL_MODE_FAST                 │ 快速模式（低延迟，质量略低）     │
  ├─────────────────────────────────┼──────────────────────────────────┤
  │ MODEL_MODE_HEAVY                │ 重度模式（更深度推理，消耗更高） │
  ├─────────────────────────────────┼──────────────────────────────────┤
  │ MODEL_MODE_GROK_4_MINI_THINKING │ Grok 4 Mini 专用思考模式         │
  ├─────────────────────────────────┼──────────────────────────────────┤
  │ MODEL_MODE_GROK_4_1_THINKING    │ Grok 4.1 深度思考模式            │
  └─────────────────────────────────┴──────────────────────────────────┘
  tier 的作用

  决定从哪个 token 池取 token（pool_for_model 方法）：
  - Basic → 从 ssoBasic 池取 token
  - Super → 从 ssoSuper 池取 token（目前只有 grok-4-heavy）

  简单来说，model_id 是给用户看的，grok_model + model_mode 是给 Grok 上游 API 用的，tier 决定用哪个 token 池。
 */