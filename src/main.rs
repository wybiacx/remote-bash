mod executor;
mod handler;
mod protocol;
mod session;
mod tls;

use axum::{routing::get, routing::post, Router};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

pub use session::SessionMap;

#[derive(Clone)]
pub struct AppState {
    pub sessions: SessionMap,
    pub token: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("remote_bash=debug")
        .with_ansi(true)
        .init();

    let token = std::env::var("MCP_TOKEN").unwrap_or_else(|_| {
        tracing::error!("MCP_TOKEN not set, refusing to start");
        std::process::exit(1);
    });
    tracing::info!("MCP_TOKEN is set");

    let sessions: SessionMap = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let port = std::env::var("MCP_PORT").unwrap_or_else(|_| "9020".into());
    let use_tls = std::env::var("MCP_TLS")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let app = Router::new()
        .route("/sse", get(handler::sse_handler))
        .route("/messages", post(handler::message_handler))
        .layer(CorsLayer::permissive())
        .with_state(AppState { sessions, token });

    let addr = format!("0.0.0.0:{}", port);

    if use_tls {
        match tls::setup_tls().await {
            Ok(tls_setup) => {
                tracing::info!("MCP server listening on https://{}", addr);
                tracing::info!("cert SHA-256 fingerprint: {}", tls_setup.cert_sha256);
                tracing::info!(
                    "use cert_sha256 in client config for certificate pinning to prevent MITM"
                );

                axum_server::bind_rustls(
                    addr.parse::<std::net::SocketAddr>().unwrap(),
                    tls_setup.rustls_config,
                )
                .serve(app.into_make_service())
                .await
                .unwrap();
            }
            Err(e) => {
                tracing::error!("TLS setup failed: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        tracing::info!("MCP server listening on http://{}", addr);
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
}
