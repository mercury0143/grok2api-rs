use std::collections::HashMap;
use std::sync::Arc;

use once_cell::sync::Lazy;
use serde_json::{Value as JsonValue, json};
use tokio::sync::{Mutex, RwLock, mpsc};
use uuid::Uuid;

#[derive(Debug)]
pub struct BatchTask {
    pub id: String,
    pub total: usize,
    pub processed: usize,
    pub ok: usize,
    pub fail: usize,
    pub status: String,
    pub warning: Option<String>,
    pub result: Option<JsonValue>,
    pub error: Option<String>,
    pub created_at: f64,
    pub cancelled: bool,
    final_event: Option<JsonValue>,
    queues: Vec<mpsc::Sender<JsonValue>>,
}

impl BatchTask {
    pub fn new(total: usize) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            total,
            processed: 0,
            ok: 0,
            fail: 0,
            status: "running".to_string(),
            warning: None,
            result: None,
            error: None,
            created_at: chrono::Utc::now().timestamp_millis() as f64 / 1000.0,
            cancelled: false,
            final_event: None,
            queues: Vec::new(),
        }
    }

    pub fn snapshot(&self) -> JsonValue {
        json!({
            "task_id": self.id,
            "status": self.status,
            "total": self.total,
            "processed": self.processed,
            "ok": self.ok,
            "fail": self.fail,
            "warning": self.warning,
        })
    }

    pub fn attach(&mut self) -> mpsc::Receiver<JsonValue> {
        let (tx, rx) = mpsc::channel(200);
        self.queues.push(tx);
        rx
    }

    fn publish(&mut self, event: JsonValue) {
        self.queues.retain(|tx| tx.try_send(event.clone()).is_ok());
    }

    pub fn record(
        &mut self,
        ok: bool,
        item: Option<JsonValue>,
        detail: Option<JsonValue>,
        error: Option<String>,
    ) {
        self.processed += 1;
        if ok {
            self.ok += 1;
        } else {
            self.fail += 1;
        }
        let mut event = json!({
            "type": "progress",
            "task_id": self.id,
            "total": self.total,
            "processed": self.processed,
            "ok": self.ok,
            "fail": self.fail,
        });
        if let Some(item) = item {
            event["item"] = item;
        }
        if let Some(detail) = detail {
            event["detail"] = detail;
        }
        if let Some(error) = error {
            event["error"] = JsonValue::String(error);
        }
        self.publish(event);
    }

    pub fn finish(&mut self, result: JsonValue, warning: Option<String>) {
        self.status = "done".to_string();
        self.result = Some(result.clone());
        self.warning = warning.clone();
        let event = json!({
            "type": "done",
            "task_id": self.id,
            "total": self.total,
            "processed": self.processed,
            "ok": self.ok,
            "fail": self.fail,
            "warning": warning,
            "result": result,
        });
        self.final_event = Some(event.clone());
        self.publish(event);
    }

    pub fn fail_task(&mut self, error: String) {
        self.status = "error".to_string();
        self.error = Some(error.clone());
        let event = json!({
            "type": "error",
            "task_id": self.id,
            "total": self.total,
            "processed": self.processed,
            "ok": self.ok,
            "fail": self.fail,
            "error": error,
        });
        self.final_event = Some(event.clone());
        self.publish(event);
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    pub fn finish_cancelled(&mut self) {
        self.status = "cancelled".to_string();
        let event = json!({
            "type": "cancelled",
            "task_id": self.id,
            "total": self.total,
            "processed": self.processed,
            "ok": self.ok,
            "fail": self.fail,
        });
        self.final_event = Some(event.clone());
        self.publish(event);
    }

    pub fn final_event(&self) -> Option<JsonValue> {
        self.final_event.clone()
    }
}

static TASKS: Lazy<RwLock<HashMap<String, Arc<Mutex<BatchTask>>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub async fn create_task(total: usize) -> Arc<Mutex<BatchTask>> {
    let task = Arc::new(Mutex::new(BatchTask::new(total)));
    let id = task.lock().await.id.clone();
    TASKS.write().await.insert(id, task.clone());
    task
}

pub async fn get_task(task_id: &str) -> Option<Arc<Mutex<BatchTask>>> {
    TASKS.read().await.get(task_id).cloned()
}

pub async fn delete_task(task_id: &str) {
    TASKS.write().await.remove(task_id);
}

pub async fn expire_task(task_id: String, delay: u64) {
    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
    delete_task(&task_id).await;
}
