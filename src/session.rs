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

#[cfg(test)]
mod tests {
    use super::*;

    fn new_map() -> SessionMap {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[tokio::test]
    async fn create_session_returns_unique_ids() {
        let sessions = new_map();
        let (id1, _rx1) = create_session(&sessions).await;
        let (id2, _rx2) = create_session(&sessions).await;
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn create_session_inserts_into_map() {
        let sessions = new_map();
        let (id, _rx) = create_session(&sessions).await;
        assert!(session_exists(&sessions, &id).await);
    }

    #[tokio::test]
    async fn send_to_existing_session_delivers_message() {
        let sessions = new_map();
        let (id, mut rx) = create_session(&sessions).await;
        let sent = send_to_session(&sessions, &id, "hello".to_string()).await;
        assert!(sent);
        let msg = rx.recv().await;
        assert_eq!(msg, Some("hello".to_string()));
    }

    #[tokio::test]
    async fn send_to_non_existent_session_returns_false() {
        let sessions = new_map();
        let fake_id = Uuid::new_v4();
        let sent = send_to_session(&sessions, &fake_id, "nope".to_string()).await;
        assert!(!sent);
    }

    #[tokio::test]
    async fn session_exists_for_non_existent_returns_false() {
        let sessions = new_map();
        assert!(!session_exists(&sessions, &Uuid::new_v4()).await);
    }

    #[tokio::test]
    async fn remove_session_deletes_entry() {
        let sessions = new_map();
        let (id, _rx) = create_session(&sessions).await;
        remove_session(&sessions, &id).await;
        assert!(!session_exists(&sessions, &id).await);
    }

    #[tokio::test]
    async fn remove_non_existent_session_does_not_panic() {
        let sessions = new_map();
        remove_session(&sessions, &Uuid::new_v4()).await;
    }

    #[tokio::test]
    async fn send_to_removed_session_returns_false() {
        let sessions = new_map();
        let (id, _rx) = create_session(&sessions).await;
        remove_session(&sessions, &id).await;
        let sent = send_to_session(&sessions, &id, "lost".to_string()).await;
        assert!(!sent);
    }

    #[tokio::test]
    async fn send_to_dropped_receiver_returns_false() {
        let sessions = new_map();
        let (id, rx) = create_session(&sessions).await;
        drop(rx); // close receiver side
        let sent = send_to_session(&sessions, &id, "orphan".to_string()).await;
        assert!(!sent);
    }

    #[tokio::test]
    async fn multiple_sessions_independent() {
        let sessions = new_map();
        let (id_a, mut rx_a) = create_session(&sessions).await;
        let (id_b, mut rx_b) = create_session(&sessions).await;

        send_to_session(&sessions, &id_a, "to-a".to_string()).await;
        send_to_session(&sessions, &id_b, "to-b".to_string()).await;

        assert_eq!(rx_a.recv().await, Some("to-a".to_string()));
        assert_eq!(rx_b.recv().await, Some("to-b".to_string()));
    }
}
