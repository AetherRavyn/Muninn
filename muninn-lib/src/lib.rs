//! Muninn Offline Library
//!
//! Use Muninn as a library without running any API server.
//! Just create a `MuninnMemory` instance and call methods directly.
//!
//! # Example
//!
//! ```no_run
//! use muninn_lib::MuninnMemory;
//!
//! #[tokio::main]
//! async fn main() {
//!     let memory = MuninnMemory::new("./my_data").await.unwrap();
//!
//!     // Write a memory
//!     let id = memory.write(
//!         "agent-1",
//!         "The deadline is March 15",
//!         0.8,
//!     ).await.unwrap();
//!
//!     // Retrieve memories
//!     let results = memory.retrieve(
//!         "agent-1",
//!         "What is the deadline?",
//!         5,
//!     ).await.unwrap();
//!
//!     for result in &results {
//!         println!("{} (score: {:.2})", result.content, result.score);
//!     }
//! }
//! ```

use std::path::Path;
use std::sync::Arc;

use muninn_core::model::*;
use muninn_core::retrieval::*;
use muninn_core::trust::TrustTier;
use muninn_core::visibility::Visibility;
use muninn_core::vector_clock::VectorClock;
use muninn_core::traits::EmbeddingProvider;
use muninn_storage::ShardStore;
use muninn_embedding::MockEmbeddingProvider;

/// A scored memory result
#[derive(Debug, Clone)]
pub struct MemoryResult {
    pub id: uuid::Uuid,
    pub content: String,
    pub tier: String,
    pub importance: f32,
    pub score: f32,
    pub score_breakdown: ScoreBreakdown,
}

/// Lineage information for a record
#[derive(Debug, Clone)]
pub struct LineageInfo {
    pub root_id: uuid::Uuid,
    pub total_downstream: usize,
    pub affects_shared: bool,
    pub affects_other_agents: bool,
}

/// Offline Muninn memory system.
/// No API server needed - just call methods directly.
pub struct MuninnMemory {
    shard: Arc<ShardStore>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
    default_tenant: String,
}

impl MuninnMemory {
    /// Create a new Muninn memory instance.
    ///
    /// # Arguments
    /// * `data_dir` - Directory to store data
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// let memory = muninn_lib::MuninnMemory::new("./my_data").await.unwrap();
    /// # });
    /// ```
    pub async fn new(data_dir: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_config(data_dir, "default", 128).await
    }

    /// Create with custom configuration.
    ///
    /// # Arguments
    /// * `data_dir` - Directory to store data
    /// * `tenant_id` - Default tenant ID
    /// * `embedding_dim` - Embedding dimension
    pub async fn with_config(
        data_dir: &str,
        tenant_id: &str,
        embedding_dim: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = Path::new(data_dir);
        std::fs::create_dir_all(path)?;

        let config = muninn_storage::shard::ShardConfig {
            shard_id: 0,
            data_dir: path.to_path_buf(),
            wal_dir: path.join("wal"),
            tantivy_dir: path.join("tantivy"),
            embedding_dimension: embedding_dim,
            max_wal_size: 1024 * 1024 * 100, // 100MB
        };

        let shard = Arc::new(ShardStore::open(config)?);
        let embedding_provider: Arc<dyn EmbeddingProvider> =
            Arc::new(MockEmbeddingProvider::new(embedding_dim));

        Ok(Self {
            shard,
            embedding_provider,
            default_tenant: tenant_id.to_string(),
        })
    }

    /// Write a memory.
    ///
    /// # Arguments
    /// * `agent_id` - Agent writing the memory
    /// * `content` - Memory content
    /// * `importance` - Importance score (0.0 to 1.0)
    ///
    /// # Returns
    /// UUID of the written record
    pub async fn write(
        &self,
        agent_id: &str,
        content: &str,
        importance: f32,
    ) -> Result<uuid::Uuid, Box<dyn std::error::Error>> {
        self.write_with_options(
            &self.default_tenant,
            agent_id,
            content,
            importance,
            Visibility::Private,
            TrustTier::Standard,
            MemoryTier::Episodic,
        ).await
    }

    /// Write with full options.
    pub async fn write_with_options(
        &self,
        tenant_id: &str,
        agent_id: &str,
        content: &str,
        importance: f32,
        visibility: Visibility,
        trust_tier: TrustTier,
        tier: MemoryTier,
    ) -> Result<uuid::Uuid, Box<dyn std::error::Error>> {
        let embedding = self.embedding_provider.embed(content).await?;

        let mut record = MemoryRecord::new_episodic(
            TenantId(tenant_id.to_string()),
            AgentId(agent_id.to_string()),
            content.to_string(),
            embedding,
            self.embedding_provider.model_version().to_string(),
            importance,
            visibility,
            trust_tier,
        );
        record.tier = tier;

        let ack = self.shard.write(record)?;
        Ok(ack.record_id)
    }

    /// Write to shared memory (visible to all agents in tenant).
    pub async fn write_shared(
        &self,
        agent_id: &str,
        content: &str,
        importance: f32,
    ) -> Result<uuid::Uuid, Box<dyn std::error::Error>> {
        self.write_with_options(
            &self.default_tenant,
            agent_id,
            content,
            importance,
            Visibility::Shared,
            TrustTier::Standard,
            MemoryTier::Shared,
        ).await
    }

    /// Retrieve memories matching a query.
    ///
    /// # Arguments
    /// * `agent_id` - Agent requesting memories
    /// * `query` - Search query
    /// * `max_results` - Maximum results to return
    ///
    /// # Returns
    /// Vector of scored memory results
    pub async fn retrieve(
        &self,
        agent_id: &str,
        query: &str,
        max_results: usize,
    ) -> Result<Vec<MemoryResult>, Box<dyn std::error::Error>> {
        self.retrieve_with_options(
            &self.default_tenant,
            agent_id,
            query,
            max_results,
            0.0,
            vec![],
        ).await
    }

    /// Retrieve with full options.
    pub async fn retrieve_with_options(
        &self,
        tenant_id: &str,
        agent_id: &str,
        query: &str,
        max_results: usize,
        min_score: f32,
        tiers: Vec<MemoryTier>,
    ) -> Result<Vec<MemoryResult>, Box<dyn std::error::Error>> {
        let embedding = self.embedding_provider.embed(query).await?;

        let retrieval_query = RetrievalQuery {
            tenant_id: TenantId(tenant_id.to_string()),
            agent_id: AgentId(agent_id.to_string()),
            embedding,
            tiers,
            max_results,
            min_score,
            visibility_filter: None,
            trust_tier_minimum: None,
            time_range: None,
            keyword_query: None,
        };

        let results = self.shard.retrieve(&retrieval_query)?;

        Ok(results.into_iter().map(|sr| MemoryResult {
            id: sr.record.id,
            content: sr.record.content,
            tier: sr.record.tier.to_string(),
            importance: sr.record.importance,
            score: sr.score.total_score,
            score_breakdown: sr.score,
        }).collect())
    }

    /// Get a specific memory by ID.
    pub async fn get(
        &self,
        id: uuid::Uuid,
    ) -> Result<Option<MemoryRecord>, Box<dyn std::error::Error>> {
        Ok(self.shard.get(id)?)
    }

    /// Mark a memory as superseded by another.
    pub async fn supersede(
        &self,
        old_id: uuid::Uuid,
        new_id: uuid::Uuid,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.shard.supersede(old_id, new_id)?;
        Ok(())
    }

    /// Trace lineage of a memory.
    pub async fn trace_lineage(
        &self,
        id: uuid::Uuid,
    ) -> Result<LineageInfo, Box<dyn std::error::Error>> {
        let graph = self.shard.trace_lineage(id)?;
        Ok(LineageInfo {
            root_id: graph.root_id,
            total_downstream: graph.total_downstream_facts,
            affects_shared: graph.affects_shared_memory,
            affects_other_agents: graph.affects_other_agents,
        })
    }

    /// Get record count for a tenant.
    pub fn count(&self, tenant_id: &str) -> usize {
        self.shard.count_tenant_records(&TenantId(tenant_id.to_string()))
    }

    /// Purge all data for a tenant.
    pub fn purge_tenant(&self, tenant_id: &str) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(self.shard.purge_tenant(&TenantId(tenant_id.to_string()))?)
    }

    /// Shutdown gracefully (flush all pending writes).
    pub fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.shard.shutdown()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_write_retrieve() {
        let dir = tempfile::tempdir().unwrap();
        let memory = MuninnMemory::new(dir.path().to_str().unwrap()).await.unwrap();

        // Write
        let id = memory.write("agent-1", "The deadline is March 15", 0.8).await.unwrap();
        assert!(!id.is_nil());

        // Retrieve
        let results = memory.retrieve("agent-1", "What is the deadline?", 5).await.unwrap();
        assert!(!results.is_empty());
        assert!(results[0].content.contains("March 15"));
    }

    #[tokio::test]
    async fn test_shared_memory() {
        let dir = tempfile::tempdir().unwrap();
        let memory = MuninnMemory::new(dir.path().to_str().unwrap()).await.unwrap();

        // Write to shared memory
        memory.write_shared("agent-1", "Important shared fact", 0.9).await.unwrap();

        // Any agent should be able to read
        let results = memory.retrieve("agent-2", "important fact", 5).await.unwrap();
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_tenant_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let memory = MuninnMemory::with_config(
            dir.path().to_str().unwrap(),
            "tenant-a",
            128,
        ).await.unwrap();

        // Write to tenant-a
        memory.write("agent-1", "Secret for tenant A", 0.8).await.unwrap();

        // Different tenant should not see it
        let results = memory.retrieve_with_options(
            "tenant-b",
            "agent-2",
            "secret",
            10,
            0.0,
            vec![],
        ).await.unwrap();

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_supersede() {
        let dir = tempfile::tempdir().unwrap();
        let memory = MuninnMemory::new(dir.path().to_str().unwrap()).await.unwrap();

        let old_id = memory.write("agent-1", "Old fact", 0.5).await.unwrap();
        let new_id = memory.write("agent-1", "New fact", 0.8).await.unwrap();

        memory.supersede(old_id, new_id).await.unwrap();

        let old = memory.get(old_id).await.unwrap().unwrap();
        assert!(old.superseded_by.is_some());
    }

    #[tokio::test]
    async fn test_lineage() {
        let dir = tempfile::tempdir().unwrap();
        let memory = MuninnMemory::new(dir.path().to_str().unwrap()).await.unwrap();

        let source_id = memory.write("agent-1", "Source", 0.5).await.unwrap();

        // Create derived record with source
        let embedding = memory.embedding_provider.embed("Derived").await.unwrap();
        let mut derived = MemoryRecord::new_episodic(
            TenantId("default".to_string()),
            AgentId("agent-1".to_string()),
            "Derived fact".to_string(),
            embedding,
            "test".to_string(),
            0.7,
            Visibility::Private,
            TrustTier::Standard,
        );
        derived.source_ids = vec![source_id];
        memory.shard.write(derived).unwrap();

        let lineage = memory.trace_lineage(source_id).await.unwrap();
        assert!(lineage.total_downstream >= 1);
    }

    #[tokio::test]
    async fn test_purge() {
        let dir = tempfile::tempdir().unwrap();
        let memory = MuninnMemory::with_config(
            dir.path().to_str().unwrap(),
            "tenant-x",
            128,
        ).await.unwrap();

        memory.write("agent-1", "Fact 1", 0.5).await.unwrap();
        memory.write("agent-1", "Fact 2", 0.5).await.unwrap();

        assert_eq!(memory.count("tenant-x"), 2);

        memory.purge_tenant("tenant-x").unwrap();

        assert_eq!(memory.count("tenant-x"), 0);
    }
}
