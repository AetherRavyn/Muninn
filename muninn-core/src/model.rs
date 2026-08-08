use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::trust::TrustTier;
use crate::vector_clock::VectorClock;
use crate::visibility::Visibility;

/// Unique identifiers
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TenantId(pub String);

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct AgentId(pub String);

impl std::fmt::Display for AgentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Memory tiers
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MemoryTier {
    Working,
    Episodic,
    Semantic,
    Procedural,
    Shared,
}

impl std::fmt::Display for MemoryTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryTier::Working => write!(f, "working"),
            MemoryTier::Episodic => write!(f, "episodic"),
            MemoryTier::Semantic => write!(f, "semantic"),
            MemoryTier::Procedural => write!(f, "procedural"),
            MemoryTier::Shared => write!(f, "shared"),
        }
    }
}

/// Retention classes
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RetentionClass {
    Ephemeral,
    Standard,
    LegalHold,
}

impl RetentionClass {
    /// Returns true if this record should never be auto-archived or deleted
    pub fn is_immutable(&self) -> bool {
        matches!(self, RetentionClass::LegalHold)
    }
}

/// The core memory record stored in every tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub agent_id: AgentId,
    pub tier: MemoryTier,
    pub schema_version: u16,
    pub content: String,
    pub embedding: Vec<f32>,
    pub embedding_model_version: String,
    pub importance: f32,
    pub retention_class: RetentionClass,
    pub trust_tier: TrustTier,
    pub created_at: DateTime<Utc>,
    pub last_accessed: DateTime<Utc>,
    pub access_count: u32,
    pub visibility: Visibility,
    pub source_ids: Vec<Uuid>,
    pub superseded_by: Option<Uuid>,
    pub vector_clock: VectorClock,
}

impl MemoryRecord {
    /// Create a new episodic memory record
    pub fn new_episodic(
        tenant_id: TenantId,
        agent_id: AgentId,
        content: String,
        embedding: Vec<f32>,
        embedding_model_version: String,
        importance: f32,
        visibility: Visibility,
        trust_tier: TrustTier,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            tenant_id,
            agent_id,
            tier: MemoryTier::Episodic,
            schema_version: CURRENT_SCHEMA_VERSION,
            content,
            embedding,
            embedding_model_version,
            importance,
            retention_class: RetentionClass::Standard,
            trust_tier,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            visibility,
            source_ids: Vec::new(),
            superseded_by: None,
            vector_clock: VectorClock::new(),
        }
    }

    /// Create a new semantic fact
    pub fn new_semantic(
        tenant_id: TenantId,
        agent_id: AgentId,
        subject: String,
        predicate: String,
        object: String,
        confidence: f32,
        source_episode_ids: Vec<Uuid>,
        embedding: Vec<f32>,
        embedding_model_version: String,
        trust_tier: TrustTier,
    ) -> Self {
        let content = format!("{} {} {}", subject, predicate, object);
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            tenant_id,
            agent_id,
            tier: MemoryTier::Semantic,
            schema_version: CURRENT_SCHEMA_VERSION,
            content,
            embedding,
            embedding_model_version,
            importance: confidence,
            retention_class: RetentionClass::Standard,
            trust_tier,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            visibility: Visibility::Private,
            source_ids: source_episode_ids,
            superseded_by: None,
            vector_clock: VectorClock::new(),
        }
    }

    /// Mark this record as superseded by another
    pub fn supersede(&mut self, superseded_by: Uuid) {
        self.superseded_by = Some(superseded_by);
    }

    /// Touch the record on access (updates last_accessed and access_count)
    pub fn touch(&mut self) {
        self.last_accessed = Utc::now();
        self.access_count += 1;
    }

    /// Calculate composite score for retrieval ranking
    pub fn composite_score(
        &self,
        cosine_similarity: f32,
        recency_weight: f32,
        importance_weight: f32,
        decay_rate: f32,
        now: DateTime<Utc>,
    ) -> f32 {
        let recency = (-decay_rate * (now - self.last_accessed).num_milliseconds() as f32 / 60000.0).exp();
        let importance_score = self.importance;
        0.5 * cosine_similarity + recency_weight * recency + importance_weight * importance_score
    }
}

/// Current schema version — bump on breaking changes
pub const CURRENT_SCHEMA_VERSION: u16 = 1;

/// Write acknowledgment returned after durable write
#[derive(Debug, Clone)]
pub struct WriteAck {
    pub record_id: Uuid,
    pub wal_offset: u64,
    pub timestamp: DateTime<Utc>,
    pub vector_clock: VectorClock,
}

/// Lineage graph for a record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageGraph {
    pub root_id: Uuid,
    pub nodes: Vec<LineageNode>,
    pub edges: Vec<LineageEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageNode {
    pub id: Uuid,
    pub tier: MemoryTier,
    pub trust_tier: TrustTier,
    pub content_preview: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    pub from: Uuid,
    pub to: Uuid,
    pub relationship: String,
}

/// Decay budget for background decay passes
#[derive(Debug, Clone)]
pub struct DecayBudget {
    pub max_records_to_decay: usize,
    pub max_duration: std::time::Duration,
    pub tenant_id: TenantId,
    pub agent_id: Option<AgentId>,
}

/// Report from a decay pass
#[derive(Debug, Clone)]
pub struct DecayReport {
    pub records_decayed: usize,
    pub records_archived: usize,
    pub records_deleted: usize,
    pub duration: std::time::Duration,
}

/// Snapshot handle for backup/restore
#[derive(Debug, Clone)]
pub struct SnapshotHandle {
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
    pub path: String,
    pub size_bytes: u64,
    pub checksum: String,
}

/// Semantic fact (for knowledge graph operations)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticFact {
    pub id: Uuid,
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub confidence: f32,
    pub source_episode_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub superseded_by: Option<Uuid>,
}

/// Rate limit configuration per source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRateLimit {
    pub max_writes_per_minute: u32,
    pub max_bytes_per_minute: u64,
    pub max_influence_score_per_hour: f32,
}

/// Consolidation job status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsolidationStatus {
    Pending,
    Running { started_at: DateTime<Utc> },
    Completed { report: ConsolidationReport },
    Failed { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsolidationReport {
    pub episodes_processed: usize,
    pub facts_created: usize,
    pub facts_superseded: usize,
    pub facts_quarantined: usize,
    pub duration_ms: u64,
}

/// Health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub shards_healthy: usize,
    pub shards_total: usize,
    pub wal_bytes_written: u64,
    pub embedding_provider_status: String,
    pub consolidation_queue_depth: usize,
    pub uptime_seconds: u64,
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
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
// commit 27 1788294954085201755
// commit 75 1788294954806530607
