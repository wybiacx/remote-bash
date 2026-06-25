use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    Mutex,
};
use uuid::Uuid;

const SESSION_BUFFER_SIZE: usize = 64;

pub type SessionMap = Arc<Mutex<HashMap<Uuid, mpsc::Sender<String>>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendResult {
    Sent,
    Full,
    Closed,
    Missing,
}

/// Create a new session, returning its id and the receiver side.
pub async fn create_session(sessions: &SessionMap) -> (Uuid, mpsc::Receiver<String>) {
    let id = Uuid::new_v4();
    let (tx, rx) = mpsc::channel(SESSION_BUFFER_SIZE);
    sessions.lock().await.insert(id, tx);
    (id, rx)
}

/// Try to send a message string to a session without waiting for queue space.
/// Returns whether the message was sent, the queue was full, or the session is gone.
pub async fn send_to_session(sessions: &SessionMap, id: &Uuid, msg: String) -> SendResult {
    let tx = {
        let map = sessions.lock().await;
        map.get(id).cloned()
    };

    if let Some(tx) = tx {
        match tx.try_send(msg) {
            Ok(()) => SendResult::Sent,
            Err(TrySendError::Full(_)) => SendResult::Full,
            Err(TrySendError::Closed(_)) => {
                remove_session(sessions, id).await;
                SendResult::Closed
            }
        }
    } else {
        SendResult::Missing
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
        assert_eq!(sent, SendResult::Sent);
        let msg = rx.recv().await;
        assert_eq!(msg, Some("hello".to_string()));
    }

    #[tokio::test]
    async fn send_to_non_existent_session_returns_false() {
        let sessions = new_map();
        let fake_id = Uuid::new_v4();
        let sent = send_to_session(&sessions, &fake_id, "nope".to_string()).await;
        assert_eq!(sent, SendResult::Missing);
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
        assert_eq!(sent, SendResult::Missing);
    }

    #[tokio::test]
    async fn send_to_dropped_receiver_returns_false() {
        let sessions = new_map();
        let (id, rx) = create_session(&sessions).await;
        drop(rx); // close receiver side
        let sent = send_to_session(&sessions, &id, "orphan".to_string()).await;
        assert_eq!(sent, SendResult::Closed);
        assert!(!session_exists(&sessions, &id).await);
    }

    #[tokio::test]
    async fn send_to_full_session_returns_false_and_keeps_session() {
        let sessions = new_map();
        let (id, _rx) = create_session(&sessions).await;

        for i in 0..SESSION_BUFFER_SIZE {
            let sent = send_to_session(&sessions, &id, format!("msg-{i}")).await;
            assert_eq!(sent, SendResult::Sent);
        }

        let sent = send_to_session(&sessions, &id, "overflow".to_string()).await;
        assert_eq!(sent, SendResult::Full);
        assert!(session_exists(&sessions, &id).await);
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
