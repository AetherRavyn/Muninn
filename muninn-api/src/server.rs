use std::sync::Arc;

use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

use muninn_core::audit::AuditLog;
use muninn_core::config::MuninnConfig;
use muninn_core::metrics::MetricsCollector;
use muninn_core::rate_limiter::RateLimiter;
use muninn_core::traits::EmbeddingProvider;
use muninn_core::error::Result;

use muninn_storage::ShardStore;
use muninn_embedding::MockEmbeddingProvider;

use crate::handlers;
use crate::middleware::{auth_middleware, rate_limit_middleware};

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub config: MuninnConfig,
    pub shard_store: Arc<ShardStore>,
    pub embedding_provider: Arc<dyn EmbeddingProvider>,
    pub audit_log: Arc<AuditLog>,
    pub metrics: Arc<MetricsCollector>,
    pub rate_limiter: Arc<RateLimiter>,
}

/// Build and run the API server
pub async fn run_server(config: MuninnConfig) -> Result<()> {
    // Initialize tracing
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.observability.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();

    info!("Starting Muninn v{}", env!("CARGO_PKG_VERSION"));

    // Initialize storage
    let shard_config = muninn_storage::shard::ShardConfig {
        shard_id: 0,
        data_dir: std::path::PathBuf::from(&config.storage.data_dir),
        wal_dir: std::path::PathBuf::from(&config.storage.wal_dir),
        tantivy_dir: std::path::PathBuf::from(&config.storage.data_dir).join("tantivy"),
        embedding_dimension: config.embedding.dimension,
        max_wal_size: config.storage.max_wal_size_bytes,
    };

    let shard_store = Arc::new(ShardStore::open(shard_config)?);

    // Initialize embedding provider
    let embedding_provider: Arc<dyn EmbeddingProvider> = Arc::new(MockEmbeddingProvider::new(config.embedding.dimension));

    let audit_log = Arc::new(AuditLog::stdout(10000));
    let metrics = Arc::new(MetricsCollector::new());
    let rate_limiter = Arc::new(RateLimiter::new(config.security.source_rate_limits.clone()));

    let state = AppState {
        config: config.clone(),
        shard_store,
        embedding_provider,
        audit_log,
        metrics,
        rate_limiter,
    };

    // Build REST API routes
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api_routes = Router::new()
        .route("/v1/memory/write", post(handlers::write_memory))
        .route("/v1/memory/retrieve", post(handlers::retrieve_memory))
        .route("/v1/memory/{id}", get(handlers::get_memory))
        .route("/v1/memory/{id}/supersede", post(handlers::supersede_memory))
        .route("/v1/memory/{id}/lineage", get(handlers::get_lineage))
        .route("/v1/tenants/{tenant_id}/purge", axum::routing::delete(handlers::purge_tenant))
        .route("/healthz", get(handlers::health_check))
        .route("/readyz", get(handlers::ready_check))
        .route("/metrics", get(handlers::metrics))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    auth_middleware,
                ))
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    rate_limit_middleware,
                )),
        );

    let app = Router::new()
        .nest("/api", api_routes)
        .layer(cors)
        .with_state(state);

    let addr = format!("0.0.0.0:{}", config.server.rest_port);
    info!("REST API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Run the API server with external state (for use by the main server binary)
pub async fn run_server_with_state(
    config: MuninnConfig,
    shard_store: Arc<ShardStore>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> Result<()> {
    let audit_log = Arc::new(AuditLog::stdout(10000));
    let metrics = Arc::new(MetricsCollector::new());
    let rate_limiter = Arc::new(RateLimiter::new(config.security.source_rate_limits.clone()));

    let state = AppState {
        config: config.clone(),
        shard_store,
        embedding_provider,
        audit_log,
        metrics,
        rate_limiter,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api_routes = Router::new()
        .route("/v1/memory/write", post(handlers::write_memory))
        .route("/v1/memory/retrieve", post(handlers::retrieve_memory))
        .route("/v1/memory/{id}", get(handlers::get_memory))
        .route("/v1/memory/{id}/supersede", post(handlers::supersede_memory))
        .route("/v1/memory/{id}/lineage", get(handlers::get_lineage))
        .route("/v1/tenants/{tenant_id}/purge", axum::routing::delete(handlers::purge_tenant))
        .route("/healthz", get(handlers::health_check))
        .route("/readyz", get(handlers::ready_check))
        .route("/metrics", get(handlers::metrics))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    auth_middleware,
                ))
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    rate_limit_middleware,
                )),
        );

    let app = Router::new()
        .nest("/api", api_routes)
        .layer(cors)
        .with_state(state);

    let addr = format!("0.0.0.0:{}", config.server.rest_port);
    info!("REST API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
// commit 63 1788294954623656891
// commit 111 1788294955354763073
// commit 183 1788294956474678160
// commit 207 1788294956854605853
// commit 327 1788294958721235510
