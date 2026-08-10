use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Result;
use crate::lineage::FullLineageGraph;
use crate::model::*;
use crate::retrieval::{RecallIntrospection, RetrievalQuery, ScoredRecord};

/// Core memory store trait — all state lives in shard storage.
/// The memory service frontend is stateless and scales horizontally.
#[async_trait]
pub trait MemoryStore: Send + Sync {
    /// Write a memory record. Ack only after fsync to WAL.
    async fn write(&self, record: MemoryRecord) -> Result<WriteAck>;

    /// Retrieve records matching a query, ranked by hybrid scoring.
    /// Returns full score breakdown for explainability.
    async fn retrieve(&self, query: &RetrievalQuery) -> Result<Vec<ScoredRecord>>;

    /// Run a decay pass within the given budget.
    async fn decay_pass(&self, budget: DecayBudget) -> Result<DecayReport>;

    /// Create a snapshot for backup
    async fn snapshot(&self, dest: &SnapshotTarget) -> Result<SnapshotHandle>;

    /// Restore from a snapshot
    async fn restore(&self, src: &SnapshotHandle) -> Result<()>;

    /// Trace the full lineage of a record (all downstream derivations)
    async fn trace_lineage(&self, record_id: Uuid) -> Result<FullLineageGraph>;

    /// Get a record by ID
    async fn get(&self, id: Uuid) -> Result<Option<MemoryRecord>>;

    /// Mark a record as superseded
    async fn supersede(&self, id: Uuid, superseded_by: Uuid) -> Result<()>;

    /// Bulk write for consolidation
    async fn write_batch(&self, records: Vec<MemoryRecord>) -> Result<Vec<WriteAck>>;

    /// Get health status of this shard
    async fn health(&self) -> Result<ShardHealth>;

    /// Introspect a past recall event
    async fn introspect_recall(
        &self,
        recall_id: Uuid,
    ) -> Result<RecallIntrospection>;
}

/// Snapshot target configuration
#[derive(Debug, Clone)]
pub struct SnapshotTarget {
    pub path: String,
    pub compress: bool,
}

/// Shard health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardHealth {
    pub shard_id: usize,
    pub records_count: u64,
    pub wal_size_bytes: u64,
    pub disk_size_bytes: u64,
    pub last_checkpoint: Option<chrono::DateTime<chrono::Utc>>,
    pub replica_lag_bytes: u64,
    pub is_healthy: bool,
}

/// Consolidator trait — distills episodic memories into semantic facts.
/// Runs as a background task, never blocks foreground recall.
#[async_trait]
pub trait Consolidator: Send + Sync {
    /// Consolidate a batch of episodic memories into semantic facts.
    /// Quarantines untrusted content (§7.2).
    async fn consolidate(&self, episodes: Vec<MemoryRecord>) -> Result<ConsolidationOutput>;
}

/// Output from consolidation
#[derive(Debug, Clone)]
pub struct ConsolidationOutput {
    pub facts_created: Vec<MemoryRecord>,
    pub facts_superseded: Vec<Uuid>,
    pub episodes_quarantined: Vec<Uuid>,
    pub anomalies_detected: Vec<AnomalyFlag>,
}

/// Anomaly flag from consolidation
#[derive(Debug, Clone)]
pub struct AnomalyFlag {
    pub record_id: Uuid,
    pub anomaly_type: AnomalyType,
    pub description: String,
    pub severity: AnomalySeverity,
}

#[derive(Debug, Clone)]
pub enum AnomalyType {
    /// New fact sharply contradicts high-confidence existing knowledge
    ContradictionDetected,
    /// Single source suddenly dominating graph influence
    InfluenceSpike,
    /// Unusually high write rate from one source
    WriteRateAnomaly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnomalySeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Embedding provider trait — never hardcode a vendor (§14 guardrail)
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Generate an embedding for the given text
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;

    /// Get the model version identifier
    fn model_version(&self) -> &str;

    /// Get the embedding dimension
    fn dimension(&self) -> usize;

    /// Health check
    async fn health_check(&self) -> bool;
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
// commit 5 1788294953761183725
// commit 77 1788294954836787695
// commit 101 1788294955200983834
