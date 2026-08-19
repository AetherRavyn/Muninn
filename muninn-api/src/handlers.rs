use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::server::AppState;
use muninn_core::model::*;
use muninn_core::retrieval::RetrievalQuery;
use muninn_core::trust::TrustTier;
use muninn_core::visibility::Visibility;

/// API request/response types

#[derive(Deserialize)]
pub struct WriteRequest {
    pub tenant_id: String,
    pub agent_id: String,
    pub content: String,
    pub importance: Option<f32>,
    pub visibility: Option<String>,
    pub trust_tier: Option<String>,
    pub tier: Option<String>,
}

#[derive(Serialize)]
pub struct WriteResponse {
    pub record_id: Uuid,
    pub wal_offset: u64,
    pub timestamp: String,
}

#[derive(Deserialize)]
pub struct RetrieveRequest {
    pub tenant_id: String,
    pub agent_id: String,
    pub query: String,
    pub max_results: Option<usize>,
    pub min_score: Option<f32>,
    pub tiers: Option<Vec<String>>,
    pub keyword_query: Option<String>,
}

#[derive(Serialize)]
pub struct RetrieveResponse {
    pub results: Vec<ScoredResult>,
    pub total_candidates: usize,
    pub query_time_ms: u64,
}

#[derive(Serialize)]
pub struct ScoredResult {
    pub id: Uuid,
    pub content: String,
    pub tier: String,
    pub trust_tier: String,
    pub importance: f32,
    pub score: f32,
    pub score_breakdown: ScoreBreakdownResponse,
    pub rank: usize,
}

#[derive(Serialize)]
pub struct ScoreBreakdownResponse {
    pub cosine_similarity: f32,
    pub recency_score: f32,
    pub importance_score: f32,
    pub keyword_score: f32,
    pub trust_multiplier: f32,
    pub total_score: f32,
}

#[derive(Deserialize)]
pub struct SupersedeRequest {
    pub superseded_by: Uuid,
}

#[derive(Serialize)]
pub struct PurgeResponse {
    pub records_purged: usize,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
}

#[derive(Serialize)]
pub struct LineageResponse {
    pub root_id: Uuid,
    pub total_downstream_facts: usize,
    pub affects_shared_memory: bool,
    pub affects_other_agents: bool,
    pub nodes: Vec<LineageNodeResponse>,
}

#[derive(Serialize)]
pub struct LineageNodeResponse {
    pub id: Uuid,
    pub tier: String,
    pub trust_tier: String,
    pub content_preview: String,
    pub agent_id: String,
    pub is_superseded: bool,
}

/// POST /v1/memory/write
pub async fn write_memory(
    State(state): State<AppState>,
    Json(request): Json<WriteRequest>,
) -> Result<Json<WriteResponse>, StatusCode> {
    let embedding = state
        .embedding_provider
        .embed(&request.content)
        .await
        .map_err(|e| {
            tracing::error!("Embedding failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let trust_tier = match request.trust_tier.as_deref() {
        Some("verified") => TrustTier::Verified,
        Some("untrusted") => TrustTier::Untrusted,
        _ => TrustTier::Standard,
    };

    let visibility = match request.visibility.as_deref() {
        Some("shared") => Visibility::Shared,
        _ => Visibility::Private,
    };

    let tier = match request.tier.as_deref() {
        Some("semantic") => MemoryTier::Semantic,
        Some("procedural") => MemoryTier::Procedural,
        Some("shared") => MemoryTier::Shared,
        _ => MemoryTier::Episodic,
    };

    let mut record = MemoryRecord::new_episodic(
        TenantId(request.tenant_id.clone()),
        AgentId(request.agent_id.clone()),
        request.content,
        embedding,
        state.embedding_provider.model_version().to_string(),
        request.importance.unwrap_or(0.5),
        visibility,
        trust_tier,
    );
    record.tier = tier;

    let ack = state
        .shard_store
        .write(record)
        .map_err(|e| {
            tracing::error!("Write failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(WriteResponse {
        record_id: ack.record_id,
        wal_offset: ack.wal_offset,
        timestamp: ack.timestamp.to_rfc3339(),
    }))
}

/// POST /v1/memory/retrieve
pub async fn retrieve_memory(
    State(state): State<AppState>,
    Json(request): Json<RetrieveRequest>,
) -> Result<Json<RetrieveResponse>, StatusCode> {
    let start = std::time::Instant::now();

    let query_embedding = state
        .embedding_provider
        .embed(&request.query)
        .await
        .map_err(|e| {
            tracing::error!("Embedding failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let tiers = request
        .tiers
        .unwrap_or_default()
        .iter()
        .filter_map(|t| match t.as_str() {
            "episodic" => Some(MemoryTier::Episodic),
            "semantic" => Some(MemoryTier::Semantic),
            "procedural" => Some(MemoryTier::Procedural),
            "shared" => Some(MemoryTier::Shared),
            _ => None,
        })
        .collect();

    let query = RetrievalQuery {
        tenant_id: TenantId(request.tenant_id),
        agent_id: AgentId(request.agent_id),
        embedding: query_embedding,
        tiers,
        max_results: request.max_results.unwrap_or(10),
        min_score: request.min_score.unwrap_or(0.1),
        visibility_filter: None,
        trust_tier_minimum: None,
        time_range: None,
        keyword_query: request.keyword_query,
    };

    let results = state
        .shard_store
        .retrieve(&query)
        .map_err(|e| {
            tracing::error!("Retrieve failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let query_time = start.elapsed().as_millis() as u64;

    let scored_results: Vec<ScoredResult> = results
        .into_iter()
        .map(|sr| ScoredResult {
            id: sr.record.id,
            content: sr.record.content,
            tier: sr.record.tier.to_string(),
            trust_tier: sr.record.trust_tier.to_string(),
            importance: sr.record.importance,
            score: sr.score.total_score,
            score_breakdown: ScoreBreakdownResponse {
                cosine_similarity: sr.score.cosine_similarity,
                recency_score: sr.score.recency_score,
                importance_score: sr.score.importance_score,
                keyword_score: sr.score.keyword_score,
                trust_multiplier: sr.score.trust_multiplier,
                total_score: sr.score.total_score,
            },
            rank: sr.rank,
        })
        .collect();

    let total = scored_results.len();

    Ok(Json(RetrieveResponse {
        results: scored_results,
        total_candidates: total,
        query_time_ms: query_time,
    }))
}

/// GET /v1/memory/:id
pub async fn get_memory(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<MemoryRecord>, StatusCode> {
    state
        .shard_store
        .get(id)
        .map_err(|e| {
            tracing::error!("Get failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}

/// POST /v1/memory/:id/supersede
pub async fn supersede_memory(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<SupersedeRequest>,
) -> Result<StatusCode, StatusCode> {
    state
        .shard_store
        .supersede(id, request.superseded_by)
        .map_err(|e| {
            tracing::error!("Supersede failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// GET /v1/memory/:id/lineage
pub async fn get_lineage(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<LineageResponse>, StatusCode> {
    let graph = state
        .shard_store
        .trace_lineage(id)
        .map_err(|e| {
            tracing::error!("Lineage trace failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let nodes: Vec<LineageNodeResponse> = graph
        .nodes
        .into_iter()
        .map(|n| LineageNodeResponse {
            id: n.id,
            tier: n.tier.to_string(),
            trust_tier: n.trust_tier.to_string(),
            content_preview: n.content_preview,
            agent_id: n.agent_id,
            is_superseded: n.is_superseded,
        })
        .collect();

    Ok(Json(LineageResponse {
        root_id: graph.root_id,
        total_downstream_facts: graph.total_downstream_facts,
        affects_shared_memory: graph.affects_shared_memory,
        affects_other_agents: graph.affects_other_agents,
        nodes,
    }))
}

/// GET /metrics
pub async fn metrics(State(state): State<AppState>) -> String {
    state.metrics.export_prometheus()
}

/// GET /healthz
pub async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: 0, // TODO: track uptime
    })
}

/// DELETE /v1/tenants/:tenant_id/purge
pub async fn purge_tenant(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<Json<PurgeResponse>, StatusCode> {
    let purged = state
        .shard_store
        .purge_tenant(&TenantId(tenant_id.clone()))
        .map_err(|e| {
            tracing::error!("Tenant purge failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Log to audit trail
    state.audit_log.log(muninn_core::audit::AuditEvent::TenantPurge {
        tenant_id,
        initiated_by: "api".to_string(),
        records_purged: purged,
    });

    Ok(Json(PurgeResponse {
        records_purged: purged,
    }))
}

/// GET /readyz
pub async fn ready_check(State(state): State<AppState>) -> Result<Json<HealthResponse>, StatusCode> {
    let health = state
        .shard_store
        .health()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    if health.is_healthy {
        Ok(Json(HealthResponse {
            status: "ready".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: 0,
        }))
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
// commit 15 1788294953908283004
// commit 39 1788294954259386443
// commit 87 1788294954990837644
// commit 135 1788294955737112037
// commit 159 1788294956106989889
// commit 231 1788294957223052129
