use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

pub type SessionMap = Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<String>>>>;

/// Create a new session, returning its id and the receiver side.
pub async fn create_session(sessions: &SessionMap) -> (Uuid, mpsc::UnboundedReceiver<String>) {
    let id = Uuid::new_v4();
    let (tx, rx) = mpsc::unbounded_channel();
    sessions.lock().await.insert(id, tx);
    (id, rx)
}

/// Send a message string to a session. Returns false if the session is gone.
pub async fn send_to_session(sessions: &SessionMap, id: &Uuid, msg: String) -> bool {
    let map = sessions.lock().await;
    if let Some(tx) = map.get(id) {
        tx.send(msg).is_ok()
    } else {
        false
    }
}

/// Check whether a session exists.
pub async fn session_exists(sessions: &SessionMap, id: &Uuid) -> bool {
    sessions.lock().await.contains_key(id)
}

/// Remove a session (called when the SSE connection drops).
pub async fn remove_session(sessions: &SessionMap, id: &Uuid) {
    sessions.lock().await.remove(id);
}
