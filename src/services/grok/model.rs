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
