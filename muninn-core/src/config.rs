use serde::{Deserialize, Serialize};

use crate::model::SourceRateLimit;
use crate::retrieval::RetrievalWeights;

/// Top-level configuration for Muninn
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MuninnConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub retrieval: RetrievalConfig,
    pub security: SecurityConfig,
    pub consolidation: ConsolidationConfig,
    pub embedding: EmbeddingConfig,
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub grpc_port: u16,
    pub rest_port: u16,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub jwt_secret: Option<String>,
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub wal_dir: String,
    pub snapshot_dir: String,
    pub shard_count: usize,
    pub max_wal_size_bytes: u64,
    pub checkpoint_interval_secs: u64,
    pub encryption_key: Option<String>,
    pub replica_endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalConfig {
    pub default_weights: RetrievalWeights,
    pub max_results: usize,
    pub min_score: f32,
    pub index_algorithm: IndexAlgorithm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexAlgorithm {
    Hnsw,
    Flat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub require_tls: bool,
    pub api_key_header: String,
    pub tenant_isolation_strict: bool,
    pub source_rate_limits: SourceRateLimit,
    pub max_shared_memory_influence_per_hour: f32,
    pub audit_log_enabled: bool,
    pub audit_log_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsolidationConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub batch_size: usize,
    pub max_concurrent_jobs: usize,
    pub quarantine_trust_tier: bool,
    pub anomaly_detection_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingConfig {
    pub provider: String,
    pub model: String,
    pub api_key: Option<String>,
    pub api_base_url: Option<String>,
    pub dimension: usize,
    pub batch_size: usize,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub log_level: String,
    pub tracing_endpoint: Option<String>,
    pub metrics_port: u16,
    pub health_port: u16,
}

impl Default for MuninnConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                grpc_port: 50051,
                rest_port: 3000,
                tls_cert_path: None,
                tls_key_path: None,
                jwt_secret: None,
                request_timeout_ms: 30000,
            },
            storage: StorageConfig {
                data_dir: "./data".to_string(),
                wal_dir: "./data/wal".to_string(),
                snapshot_dir: "./data/snapshots".to_string(),
                shard_count: 4,
                max_wal_size_bytes: 1024 * 1024 * 100, // 100MB
                checkpoint_interval_secs: 300,
                encryption_key: None,
                replica_endpoint: None,
            },
            retrieval: RetrievalConfig {
                default_weights: RetrievalWeights::default(),
                max_results: 10,
                min_score: 0.1,
                index_algorithm: IndexAlgorithm::Hnsw,
            },
            security: SecurityConfig {
                require_tls: true,
                api_key_header: "X-Api-Key".to_string(),
                tenant_isolation_strict: true,
                source_rate_limits: SourceRateLimit {
                    max_writes_per_minute: 100,
                    max_bytes_per_minute: 1024 * 1024,
                    max_influence_score_per_hour: 50.0,
                },
                max_shared_memory_influence_per_hour: 100.0,
                audit_log_enabled: true,
                audit_log_path: None,
            },
            consolidation: ConsolidationConfig {
                enabled: true,
                interval_secs: 60,
                batch_size: 100,
                max_concurrent_jobs: 2,
                quarantine_trust_tier: true,
                anomaly_detection_enabled: true,
            },
            embedding: EmbeddingConfig {
                provider: "openai".to_string(),
                model: "text-embedding-3-small".to_string(),
                api_key: None,
                api_base_url: None,
                dimension: 1536,
                batch_size: 32,
                timeout_ms: 10000,
            },
            observability: ObservabilityConfig {
                log_level: "info".to_string(),
                tracing_endpoint: None,
                metrics_port: 9090,
                health_port: 8080,
            },
        }
    }
}

impl MuninnConfig {
    /// Load config from file, then env overrides
    pub fn load() -> Result<Self, String> {
        let mut config = if std::path::Path::new("muninn.toml").exists() {
            let content = std::fs::read_to_string("muninn.toml")
                .map_err(|e| format!("Failed to read config file: {}", e))?;
            toml::from_str(&content)
                .map_err(|e| format!("Failed to parse config file: {}", e))?
        } else {
            Self::default()
        };

        // Environment variable overrides
        if let Ok(val) = std::env::var("MUNINN_GRPC_PORT") {
            config.server.grpc_port = val.parse().unwrap_or(config.server.grpc_port);
        }
        if let Ok(val) = std::env::var("MUNINN_REST_PORT") {
            config.server.rest_port = val.parse().unwrap_or(config.server.rest_port);
        }
        if let Ok(val) = std::env::var("MUNINN_DATA_DIR") {
            config.storage.data_dir = val;
        }
        if let Ok(val) = std::env::var("MUNINN_EMBEDDING_API_KEY") {
            config.embedding.api_key = Some(val);
        }
        if let Ok(val) = std::env::var("MUNINN_LOG_LEVEL") {
            config.observability.log_level = val;
        }

        Ok(config)
    }
}
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
// commit 123 1788294955538525015
// commit 147 1788294955925169998
// commit 195 1788294956662906043
// commit 243 1788294957411298081
// commit 363 1788294959279627525
