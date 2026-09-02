use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use parking_lot::RwLock;
use tracing::info;
use uuid::Uuid;

use muninn_core::error::{Error, Result};
use muninn_core::lineage::{FullLineageGraph, LineageEdge, LineageNode};
use muninn_core::model::*;
use muninn_core::retrieval::{RecallIntrospection, RetrievalQuery, ScoredRecord};
use muninn_core::traits::*;

use crate::hnsw_index::HnswIndex;
use crate::tantivy_index::TantivyIndex;
use crate::wal::{Wal, WalEntry};

/// Configuration for a shard
#[derive(Debug, Clone)]
pub struct ShardConfig {
    pub shard_id: usize,
    pub data_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub tantivy_dir: PathBuf,
    pub embedding_dimension: usize,
    pub max_wal_size: u64,
}

/// A single shard of the memory store.
/// Contains: WAL, in-memory record map, HNSW vector index, Tantivy full-text index.
pub struct ShardStore {
    config: ShardConfig,
    wal: Arc<Wal>,
    records: Arc<RwLock<HashMap<Uuid, MemoryRecord>>>,
    vector_index: Arc<HnswIndex>,
    text_index: Arc<TantivyIndex>,
    lineage_tracker: Arc<RwLock<muninn_core::lineage::LineageTracker>>,
    write_count: AtomicCounter,
    read_count: AtomicCounter,
}

struct AtomicCounter {
    count: std::sync::atomic::AtomicU64,
}

impl AtomicCounter {
    fn new() -> Self {
        Self {
            count: std::sync::atomic::AtomicU64::new(0),
        }
    }
    fn increment(&self) -> u64 {
        self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
    #[allow(dead_code)]
    fn get(&self) -> u64 {
        self.count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl ShardStore {
    /// Open or create a shard
    pub fn open(config: ShardConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;
        std::fs::create_dir_all(&config.wal_dir)?;
        std::fs::create_dir_all(&config.tantivy_dir)?;

        let wal = Arc::new(Wal::open(&config.wal_dir, config.max_wal_size)?);
        let vector_index = Arc::new(HnswIndex::new(config.embedding_dimension));
        let text_index = Arc::new(TantivyIndex::open(&config.tantivy_dir)?);

        let mut shard = Self {
            config,
            wal,
            records: Arc::new(RwLock::new(HashMap::new())),
            vector_index,
            text_index,
            lineage_tracker: Arc::new(RwLock::new(muninn_core::lineage::LineageTracker::new())),
            write_count: AtomicCounter::new(),
            read_count: AtomicCounter::new(),
        };

        // Replay WAL for crash recovery
        shard.replay_wal()?;

        Ok(shard)
    }

    /// Replay WAL from last checkpoint for crash recovery
    fn replay_wal(&mut self) -> Result<()> {
        let checkpoint_offset = self.wal.last_checkpoint_offset()?;
        info!(
            "Shard {}: replaying WAL from offset {}",
            self.config.shard_id, checkpoint_offset
        );

        let records_clone = self.records.clone();
        let vector_index = self.vector_index.clone();
        let text_index = self.text_index.clone();
        let lineage_tracker = self.lineage_tracker.clone();
        let shard_id = self.config.shard_id;

        let count = self.wal.replay(checkpoint_offset, |entry, _offset| {
            match entry {
                WalEntry::Write(record) => {
                    // Index in all structures
                    Self::apply_record(
                        &records_clone,
                        &vector_index,
                        &text_index,
                        &lineage_tracker,
                        &record,
                    )?;
                }
                WalEntry::BatchWrite(records) => {
                    for record in &records {
                        Self::apply_record(
                            &records_clone,
                            &vector_index,
                            &text_index,
                            &lineage_tracker,
                            record,
                        )?;
                    }
                }
                WalEntry::Supersede { id, superseded_by } => {
                    let mut records = records_clone.write();
                    if let Some(record) = records.get_mut(&id) {
                        record.supersede(superseded_by);
                    }
                }
            }
            Ok(())
        })?;

        info!("Shard {}: replayed {} WAL entries", shard_id, count);
        self.wal.checkpoint(self.wal.current_offset())?;
        Ok(())
    }

    /// Apply a record to all in-memory indices
    fn apply_record(
        records: &RwLock<HashMap<Uuid, MemoryRecord>>,
        vector_index: &HnswIndex,
        text_index: &TantivyIndex,
        lineage_tracker: &RwLock<muninn_core::lineage::LineageTracker>,
        record: &MemoryRecord,
    ) -> Result<()> {
        // Store in record map
        records.write().insert(record.id, record.clone());

        // Index vector
        vector_index.insert(
            record.id,
            record.embedding.clone(),
            &record.tenant_id.0,
            &record.agent_id.0,
        )?;

        // Index text (buffered for batch commit)
        text_index.buffer_document(
            record.id,
            &record.content,
            &record.tenant_id.0,
            &record.agent_id.0,
            &record.tier.to_string(),
            &record.trust_tier.to_string(),
        );

        // Track lineage
        let mut tracker = lineage_tracker.write();
        for source_id in &record.source_ids {
            tracker.add_derivation(record.id, *source_id);
        }

        Ok(())
    }

    /// Get a record by ID
    pub fn get(&self, id: Uuid) -> Result<Option<MemoryRecord>> {
        self.read_count.increment();
        Ok(self.records.read().get(&id).cloned())
    }

    /// Write a record — appends to WAL, fsyncs, then indexes
    pub fn write(&self, record: MemoryRecord) -> Result<WriteAck> {
        // Enforce capacity limits per tier
        self.enforce_capacity(&record)?;

        // Append to WAL (with fsync)
        let wal_offset = self.wal.append(WalEntry::Write(record.clone()))?;

        // Apply to in-memory indices
        Self::apply_record(
            &self.records,
            &self.vector_index,
            &self.text_index,
            &self.lineage_tracker,
            &record,
        )?;

        self.write_count.increment();

        Ok(WriteAck {
            record_id: record.id,
            wal_offset,
            timestamp: Utc::now(),
            vector_clock: record.vector_clock.clone(),
        })
    }

    /// Bulk write — single WAL entry for the batch
    pub fn write_batch(&self, records: Vec<MemoryRecord>) -> Result<Vec<WriteAck>> {
        let mut acks = Vec::with_capacity(records.len());

        // Single WAL entry for the batch
        let wal_offset = self.wal.append(WalEntry::BatchWrite(records.clone()))?;

        for (_i, record) in records.into_iter().enumerate() {
            self.enforce_capacity(&record)?;

            Self::apply_record(
                &self.records,
                &self.vector_index,
                &self.text_index,
                &self.lineage_tracker,
                &record,
            )?;

            acks.push(WriteAck {
                record_id: record.id,
                wal_offset,
                timestamp: Utc::now(),
                vector_clock: record.vector_clock.clone(),
            });

            self.write_count.increment();
        }

        Ok(acks)
    }

    /// Enforce per-tier capacity limits — evict lowest-scoring when full
    fn enforce_capacity(&self, new_record: &MemoryRecord) -> Result<()> {
        let records = self.records.read();
        let tier_count = records
            .values()
            .filter(|r| r.tier == new_record.tier && r.tenant_id == new_record.tenant_id && r.agent_id == new_record.agent_id)
            .count();

        let max_per_tier = 1_000_000; // Configurable per tier

        if tier_count >= max_per_tier {
            drop(records);
            // Evict lowest-scoring entry from this tier
            self.evict_lowest(new_record.tier, &new_record.tenant_id, &new_record.agent_id)?;
        }

        Ok(())
    }

    /// Evict the lowest-scoring record from a tier
    fn evict_lowest(
        &self,
        tier: MemoryTier,
        tenant_id: &TenantId,
        agent_id: &AgentId,
    ) -> Result<()> {
        let now = Utc::now();
        let mut records = self.records.write();

        // Find the record with the lowest composite score (non-legal-hold)
        let worst_id = records
            .values()
            .filter(|r| {
                r.tier == tier
                    && r.tenant_id == *tenant_id
                    && r.agent_id == *agent_id
                    && !r.retention_class.is_immutable()
            })
            .min_by(|a, b| {
                let score_a = a.composite_score(0.0, 0.2, 0.2, 0.01, now);
                let score_b = b.composite_score(0.0, 0.2, 0.2, 0.01, now);
                score_a.partial_cmp(&score_b).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|r| r.id);

        if let Some(id) = worst_id {
            records.remove(&id);
            self.vector_index.remove(id)?;
            self.text_index.remove_document(id)?;
            info!("Evicted record {} from tier {}", id, tier);
        }

        Ok(())
    }

    /// Retrieve records matching a query
    pub fn retrieve(&self, query: &RetrievalQuery) -> Result<Vec<ScoredRecord>> {
        self.read_count.increment();

        let mut results = Vec::new();
        let now = Utc::now();

        // Step 1: Vector similarity search
        let vector_results = self.vector_index.search(
            &query.embedding,
            query.max_results * 3, // Over-fetch for filtering
            &query.tenant_id.0,
            Some(&query.agent_id.0),
        )?;

        // Step 2: BM25 keyword search (if provided)
        let keyword_results = if let Some(ref keyword_query) = query.keyword_query {
            self.text_index.search(keyword_query, &query.tenant_id.0, query.max_results * 3)?
        } else {
            Vec::new()
        };

        // Merge results — union of vector and keyword matches
        let mut candidate_ids: Vec<Uuid> = vector_results.iter().map(|r| r.id).collect();
        for (id, _) in &keyword_results {
            if !candidate_ids.contains(id) {
                candidate_ids.push(*id);
            }
        }

        // Step 3: Filter and score candidates
        let records = self.records.read();
        let vector_scores: HashMap<Uuid, f32> = vector_results.into_iter().map(|r| (r.id, r.score)).collect();
        let keyword_scores: HashMap<Uuid, f32> = keyword_results.into_iter().collect();

        let mut _total_candidates = 0;
        let mut _filtered_by_visibility = 0;
        let mut _filtered_by_trust = 0;
        let mut _filtered_by_tier = 0;

        for id in &candidate_ids {
            _total_candidates += 1;

            let record = match records.get(id) {
                Some(r) => r,
                None => continue,
            };

            // Tier filter
            if !query.tiers.is_empty() && !query.tiers.contains(&record.tier) {
                _filtered_by_tier += 1;
                continue;
            }

            // Visibility filter
            if !record.visibility.allows_access(&record.agent_id, &query.agent_id) {
                _filtered_by_visibility += 1;
                continue;
            }

            // Trust tier filter
            if let Some(min_trust) = &query.trust_tier_minimum {
                if &record.trust_tier < min_trust {
                    _filtered_by_trust += 1;
                    continue;
                }
            }

            // Time range filter
            if let Some((start, end)) = &query.time_range {
                if record.created_at < *start || record.created_at > *end {
                    continue;
                }
            }

            // Calculate score
            let cosine_sim = vector_scores.get(id).copied().unwrap_or(0.0);
            let bm25 = keyword_scores.get(id).copied().unwrap_or(0.0);

            let scored = ScoredRecord::calculate(
                record.clone(),
                cosine_sim,
                bm25,
                &muninn_core::retrieval::RetrievalWeights::default(), // TODO: per-agent weights
                now,
            );

            if scored.score.total_score >= query.min_score {
                results.push(scored);
            }
        }

        // Sort by score descending
        results.sort_by(|a, b| {
            b.score
                .total_score
                .partial_cmp(&a.score.total_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Assign ranks and truncate
        for (i, result) in results.iter_mut().enumerate() {
            result.rank = i + 1;
        }
        results.truncate(query.max_results);

        Ok(results)
    }

    /// Mark a record as superseded
    pub fn supersede(&self, id: Uuid, superseded_by: Uuid) -> Result<()> {
        self.wal.append(WalEntry::Supersede { id, superseded_by })?;

        let mut records = self.records.write();
        if let Some(record) = records.get_mut(&id) {
            record.supersede(superseded_by);
        }

        Ok(())
    }

    /// Trace lineage of a record
    pub fn trace_lineage(&self, record_id: Uuid) -> Result<FullLineageGraph> {
        let tracker = self.lineage_tracker.read();
        let records = self.records.read();

        let downstream_ids = tracker.trace_downstream(&record_id);

        let root_record = records.get(&record_id).ok_or_else(|| {
            Error::NotFound(format!("Record {} not found", record_id))
        })?;

        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut affects_shared = false;
        let mut affects_other_agents = false;

        // Add root node
        nodes.push(LineageNode {
            id: record_id,
            tier: root_record.tier,
            trust_tier: root_record.trust_tier,
            content_preview: root_record.content.chars().take(100).collect(),
            created_at: root_record.created_at,
            agent_id: root_record.agent_id.0.clone(),
            is_superseded: root_record.superseded_by.is_some(),
        });

        // Add downstream nodes
        for &downstream_id in &downstream_ids {
            if let Some(record) = records.get(&downstream_id) {
                if matches!(record.tier, MemoryTier::Shared | MemoryTier::Semantic) {
                    affects_shared = true;
                }
                if record.agent_id != root_record.agent_id {
                    affects_other_agents = true;
                }

                nodes.push(LineageNode {
                    id: downstream_id,
                    tier: record.tier,
                    trust_tier: record.trust_tier,
                    content_preview: record.content.chars().take(100).collect(),
                    created_at: record.created_at,
                    agent_id: record.agent_id.0.clone(),
                    is_superseded: record.superseded_by.is_some(),
                });

                // Add edges from sources
                let sources = tracker.get_sources(&downstream_id);
                for source_id in sources {
                    edges.push(LineageEdge {
                        from: source_id,
                        to: downstream_id,
                        relationship: muninn_core::lineage::LineageRelationship::EpisodeToFact,
                    });
                }
            }
        }

        Ok(FullLineageGraph {
            root_id: record_id,
            nodes,
            edges,
            total_downstream_facts: downstream_ids.len(),
            affects_shared_memory: affects_shared,
            affects_other_agents,
        })
    }

    /// Run a decay pass
    pub fn decay_pass(&self, budget: DecayBudget) -> Result<DecayReport> {
        let now = Utc::now();
        let start = std::time::Instant::now();
        let mut records_decayed = 0;
        let mut records_archived = 0;
        let mut records_deleted = 0;

        let mut records = self.records.write();
        let mut to_remove = Vec::new();

        for (id, record) in records.iter() {
            if records_decayed >= budget.max_records_to_decay {
                break;
            }

            // Skip legal-hold records
            if record.retention_class.is_immutable() {
                continue;
            }

            // Check if agent matches (if specified)
            if let Some(ref agent_id) = budget.agent_id {
                if record.agent_id != *agent_id {
                    continue;
                }
            }

            // Calculate decay score
            let decay_score = record.composite_score(0.0, 0.2, 0.2, 0.01, now);

            if record.retention_class == RetentionClass::Ephemeral && decay_score < 0.1 {
                to_remove.push(*id);
                records_deleted += 1;
                records_decayed += 1;
            } else if decay_score < 0.05 && record.access_count == 0 {
                // Archive records that haven't been accessed and have very low scores
                to_remove.push(*id);
                records_archived += 1;
                records_decayed += 1;
            }
        }

        // Remove decayed records
        for id in &to_remove {
            records.remove(id);
            self.vector_index.remove(*id)?;
            self.text_index.remove_document(*id)?;
        }

        Ok(DecayReport {
            records_decayed,
            records_archived,
            records_deleted,
            duration: start.elapsed(),
        })
    }

    /// Get health status
    pub fn health(&self) -> Result<ShardHealth> {
        let records = self.records.read();
        Ok(ShardHealth {
            shard_id: self.config.shard_id,
            records_count: records.len() as u64,
            wal_size_bytes: std::fs::metadata(self.config.wal_dir.join("wal_000000.log"))
                .map(|m| m.len())
                .unwrap_or(0),
            disk_size_bytes: 0, // TODO: calculate
            last_checkpoint: Some(Utc::now()),
            replica_lag_bytes: 0,
            is_healthy: true,
        })
    }

    /// Introspect a recall event
    pub fn introspect_recall(&self, _recall_id: Uuid) -> Result<RecallIntrospection> {
        // TODO: Store recall history for introspection
        Err(Error::NotFound("Recall history not yet implemented".to_string()))
    }

    /// Purge all data for a tenant.
    /// Removes from: in-memory records, vector index, text index.
    /// Returns the count of records purged.
    pub fn purge_tenant(&self, tenant_id: &TenantId) -> Result<usize> {
        let mut records = self.records.write();
        let mut purged = 0;
        let mut to_remove = Vec::new();

        // Find all records for this tenant
        for (id, record) in records.iter() {
            if record.tenant_id == *tenant_id {
                to_remove.push(*id);
            }
        }

        // Remove from all indices
        for id in &to_remove {
            if let Some(_record) = records.remove(id) {
                self.vector_index.remove(*id)?;
                self.text_index.remove_document(*id)?;
                
                // Remove from lineage tracker
                let mut tracker = self.lineage_tracker.write();
                // Remove forward edges from this record
                tracker.forward_edges.remove(id);
                // Remove backward edges to this record
                for (_, sources) in tracker.backward_edges.iter_mut() {
                    sources.retain(|s| s != id);
                }
                
                purged += 1;
            }
        }

        info!("Purged {} records for tenant {}", purged, tenant_id);
        Ok(purged)
    }

    /// Get count of records for a tenant
    pub fn count_tenant_records(&self, tenant_id: &TenantId) -> usize {
        self.records
            .read()
            .values()
            .filter(|r| r.tenant_id == *tenant_id)
            .count()
    }

    /// Flush all pending index operations to disk
    pub fn flush(&self) -> Result<()> {
        self.text_index.flush()?;
        Ok(())
    }

    /// Graceful shutdown — flush everything
    pub fn shutdown(&self) -> Result<()> {
        self.flush()?;
        info!("Shard {} shut down gracefully", self.config.shard_id);
        Ok(())
    }
}
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
// commit 11 1788294953848039799
// commit 35 1788294954201627176
// commit 59 1788294954561437070
// commit 83 1788294954929912688
// commit 227 1788294957162517381
// commit 323 1788294958655005814
// commit 419 1788294960140351209
