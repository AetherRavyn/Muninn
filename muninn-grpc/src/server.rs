use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::info;

use muninn_core::model::*;
use muninn_core::retrieval::*;
use muninn_core::trust::TrustTier;
use muninn_core::visibility::Visibility;
use muninn_core::traits::EmbeddingProvider;
use muninn_storage::ShardStore;

/// Simple line-based gRPC-like protocol for internal communication.
/// In production, this would be replaced with tonic + protobuf.
pub struct GrpcMemoryService {
    shard_store: Arc<ShardStore>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
}

/// Request types for the internal API
#[derive(serde::Deserialize)]
pub struct WriteRequest {
    pub tenant_id: String,
    pub agent_id: String,
    pub content: String,
    pub importance: Option<f32>,
    pub visibility: Option<String>,
    pub trust_tier: Option<String>,
    pub tier: Option<String>,
}

#[derive(serde::Serialize)]
pub struct WriteResponse {
    pub record_id: String,
    pub wal_offset: u64,
    pub timestamp: String,
}

#[derive(serde::Deserialize)]
pub struct RetrieveRequest {
    pub tenant_id: String,
    pub agent_id: String,
    pub query: String,
    pub max_results: Option<usize>,
    pub min_score: Option<f32>,
    pub tiers: Option<Vec<String>>,
    pub keyword_query: Option<String>,
}

#[derive(serde::Serialize)]
pub struct RetrieveResponse {
    pub results: Vec<ScoredResult>,
    pub total_candidates: usize,
    pub query_time_us: u64,
}

#[derive(serde::Serialize)]
pub struct ScoredResult {
    pub id: String,
    pub content: String,
    pub tier: String,
    pub trust_tier: String,
    pub importance: f32,
    pub score: f32,
    pub rank: usize,
}

#[derive(serde::Deserialize)]
pub struct LineageRequest {
    pub record_id: String,
}

#[derive(serde::Serialize)]
pub struct LineageResponse {
    pub root_id: String,
    pub total_downstream_facts: usize,
    pub affects_shared_memory: bool,
    pub affects_other_agents: bool,
}

#[derive(serde::Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub records_count: u64,
}

/// Request commands for the line-based protocol
#[derive(serde::Deserialize)]
#[serde(tag = "command")]
enum Command {
    #[serde(rename = "write")]
    Write(WriteRequest),
    #[serde(rename = "retrieve")]
    Retrieve(RetrieveRequest),
    #[serde(rename = "lineage")]
    Lineage(LineageRequest),
    #[serde(rename = "health")]
    Health,
}

#[derive(serde::Serialize)]
#[serde(tag = "type")]
enum Response {
    #[serde(rename = "write")]
    Write(WriteResponse),
    #[serde(rename = "retrieve")]
    Retrieve(RetrieveResponse),
    #[serde(rename = "lineage")]
    Lineage(LineageResponse),
    #[serde(rename = "health")]
    Health(HealthResponse),
    #[serde(rename = "error")]
    Error { message: String },
}

impl GrpcMemoryService {
    pub fn new(shard_store: Arc<ShardStore>, embedding_provider: Arc<dyn EmbeddingProvider>) -> Self {
        Self {
            shard_store,
            embedding_provider,
        }
    }

    async fn handle_command(&self, cmd: Command) -> Response {
        match cmd {
            Command::Write(req) => {
                let embedding = match self.embedding_provider.embed(&req.content).await {
                    Ok(e) => e,
                    Err(e) => return Response::Error { message: format!("Embedding failed: {}", e) },
                };

                let trust_tier = match req.trust_tier.as_deref() {
                    Some("verified") => TrustTier::Verified,
                    Some("untrusted") => TrustTier::Untrusted,
                    _ => TrustTier::Standard,
                };

                let visibility = match req.visibility.as_deref() {
                    Some("shared") => Visibility::Shared,
                    _ => Visibility::Private,
                };

                let tier = match req.tier.as_deref() {
                    Some("semantic") => MemoryTier::Semantic,
                    Some("procedural") => MemoryTier::Procedural,
                    Some("shared") => MemoryTier::Shared,
                    _ => MemoryTier::Episodic,
                };

                let mut record = MemoryRecord::new_episodic(
                    TenantId(req.tenant_id),
                    AgentId(req.agent_id),
                    req.content,
                    embedding,
                    self.embedding_provider.model_version().to_string(),
                    req.importance.unwrap_or(0.5),
                    visibility,
                    trust_tier,
                );
                record.tier = tier;

                match self.shard_store.write(record) {
                    Ok(ack) => Response::Write(WriteResponse {
                        record_id: ack.record_id.to_string(),
                        wal_offset: ack.wal_offset,
                        timestamp: ack.timestamp.to_rfc3339(),
                    }),
                    Err(e) => Response::Error { message: format!("Write failed: {}", e) },
                }
            }
            Command::Retrieve(req) => {
                let start = Instant::now();

                let query_embedding = match self.embedding_provider.embed(&req.query).await {
                    Ok(e) => e,
                    Err(e) => return Response::Error { message: format!("Embedding failed: {}", e) },
                };

                let tiers = req.tiers.unwrap_or_default().iter().filter_map(|t| match t.as_str() {
                    "episodic" => Some(MemoryTier::Episodic),
                    "semantic" => Some(MemoryTier::Semantic),
                    "procedural" => Some(MemoryTier::Procedural),
                    "shared" => Some(MemoryTier::Shared),
                    _ => None,
                }).collect();

                let query = RetrievalQuery {
                    tenant_id: TenantId(req.tenant_id),
                    agent_id: AgentId(req.agent_id),
                    embedding: query_embedding,
                    tiers,
                    max_results: req.max_results.unwrap_or(10),
                    min_score: req.min_score.unwrap_or(0.1),
                    visibility_filter: None,
                    trust_tier_minimum: None,
                    time_range: None,
                    keyword_query: req.keyword_query,
                };

                match self.shard_store.retrieve(&query) {
                    Ok(results) => {
                        let query_time_us = start.elapsed().as_micros() as u64;
                        let total = results.len();
                        let scored: Vec<ScoredResult> = results.into_iter().map(|sr| ScoredResult {
                            id: sr.record.id.to_string(),
                            content: sr.record.content,
                            tier: sr.record.tier.to_string(),
                            trust_tier: sr.record.trust_tier.to_string(),
                            importance: sr.record.importance,
                            score: sr.score.total_score,
                            rank: sr.rank,
                        }).collect();

                        Response::Retrieve(RetrieveResponse {
                            results: scored,
                            total_candidates: total,
                            query_time_us,
                        })
                    }
                    Err(e) => Response::Error { message: format!("Retrieve failed: {}", e) },
                }
            }
            Command::Lineage(req) => {
                let id = match uuid::Uuid::parse_str(&req.record_id) {
                    Ok(id) => id,
                    Err(e) => return Response::Error { message: format!("Invalid UUID: {}", e) },
                };

                match self.shard_store.trace_lineage(id) {
                    Ok(graph) => Response::Lineage(LineageResponse {
                        root_id: graph.root_id.to_string(),
                        total_downstream_facts: graph.total_downstream_facts,
                        affects_shared_memory: graph.affects_shared_memory,
                        affects_other_agents: graph.affects_other_agents,
                    }),
                    Err(e) => Response::Error { message: format!("Lineage trace failed: {}", e) },
                }
            }
            Command::Health => {
                match self.shard_store.health() {
                    Ok(health) => Response::Health(HealthResponse {
                        status: if health.is_healthy { "ok".to_string() } else { "degraded".to_string() },
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        records_count: health.records_count,
                    }),
                    Err(e) => Response::Error { message: format!("Health check failed: {}", e) },
                }
            }
        }
    }
}

/// Start the internal gRPC-like server
pub async fn run_grpc_server(
    addr: std::net::SocketAddr,
    shard_store: Arc<ShardStore>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    let service = Arc::new(GrpcMemoryService::new(shard_store, embedding_provider));

    info!("Internal API server listening on {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let service = service.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break, // Connection closed
                    Ok(_) => {
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }

                        let response = match serde_json::from_str::<Command>(line) {
                            Ok(cmd) => service.handle_command(cmd).await,
                            Err(e) => Response::Error { message: format!("Invalid command: {}", e) },
                        };

                        let mut response_json = serde_json::to_string(&response).unwrap_or_default();
                        response_json.push('\n');

                        if writer.write_all(response_json.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }
}
