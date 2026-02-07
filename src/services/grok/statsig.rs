use base64::Engine;
use rand::{Rng, distributions::Alphanumeric};

use crate::core::config::get_config;

pub struct StatsigService;

impl StatsigService {
    fn rand_str(len: usize, alnum: bool) -> String {
        if alnum {
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(len)
                .map(char::from)
                .collect::<String>()
                .to_lowercase()
        } else {
            let mut rng = rand::thread_rng();
            (0..len)
                .map(|_| (b'a' + (rng.r#gen::<u8>() % 26)) as char)
                .collect()
        }
    }

    pub async fn gen_id() -> String {
        let dynamic: bool = get_config("grok.dynamic_statsig", true).await;
        if !dynamic {
            return "ZTpUeXBlRXJyb3I6IENhbm5vdCByZWFkIHByb3BlcnRpZXMgb2YgdW5kZWZpbmVkIChyZWFkaW5nICdjaGlsZE5vZGVzJyk=".to_string();
        }
        let msg = if rand::random::<bool>() {
            let rand = Self::rand_str(5, true);
            format!("e:TypeError: Cannot read properties of null (reading 'children[\"{rand}\"]')")
        } else {
            let rand = Self::rand_str(10, false);
            format!("e:TypeError: Cannot read properties of undefined (reading '{rand}')")
        };
        base64::engine::general_purpose::STANDARD.encode(msg.as_bytes())
    }
}
