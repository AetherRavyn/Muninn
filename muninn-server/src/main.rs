use std::sync::Arc;

use muninn_core::config::MuninnConfig;
use muninn_core::traits::EmbeddingProvider;
use muninn_embedding::MockEmbeddingProvider;
use muninn_storage::ShardStore;

#[tokio::main]
async fn main() {
    // Load configuration
    let config = match MuninnConfig::load() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Failed to load configuration: {}", e);
            std::process::exit(1);
        }
    };

    // Initialize tracing
    muninn_core::tracing_setup::init_tracing(&config.observability);

    tracing::info!("Starting Muninn v{}", env!("CARGO_PKG_VERSION"));

    // Initialize storage
    let shard_config = muninn_storage::shard::ShardConfig {
        shard_id: 0,
        data_dir: std::path::PathBuf::from(&config.storage.data_dir),
        wal_dir: std::path::PathBuf::from(&config.storage.wal_dir),
        tantivy_dir: std::path::PathBuf::from(&config.storage.data_dir).join("tantivy"),
        embedding_dimension: config.embedding.dimension,
        max_wal_size: config.storage.max_wal_size_bytes,
    };

    let shard_store = Arc::new(match ShardStore::open(shard_config) {
        Ok(store) => store,
        Err(e) => {
            tracing::error!("Failed to open storage: {}", e);
            std::process::exit(1);
        }
    });

    // Initialize embedding provider
    let embedding_provider: Arc<dyn EmbeddingProvider> =
        Arc::new(MockEmbeddingProvider::new(config.embedding.dimension));

    // Start REST API server
    let rest_config = config.clone();
    let rest_shard = shard_store.clone();
    let rest_embedding = embedding_provider.clone();
    let rest_handle = tokio::spawn(async move {
        if let Err(e) = muninn_api::server::run_server_with_state(
            rest_config,
            rest_shard,
            rest_embedding,
        )
        .await
        {
            tracing::error!("REST server error: {}", e);
        }
    });

    // Start gRPC server
    let grpc_addr = format!("0.0.0.0:{}", config.server.grpc_port)
        .parse()
        .expect("Invalid gRPC address");
    let grpc_shard = shard_store.clone();
    let grpc_embedding = embedding_provider.clone();
    let grpc_handle = tokio::spawn(async move {
        if let Err(e) = muninn_grpc::server::run_grpc_server(
            grpc_addr,
            grpc_shard,
            grpc_embedding,
        )
        .await
        {
            tracing::error!("gRPC server error: {}", e);
        }
    });

    tracing::info!(
        "Muninn started — REST on :{}, gRPC on :{}",
        config.server.rest_port,
        config.server.grpc_port
    );

    // Wait for both servers
    tokio::select! {
        _ = rest_handle => {},
        _ = grpc_handle => {},
    }
}
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
// commit 91 1788294955050615621
