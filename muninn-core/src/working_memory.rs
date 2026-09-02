use std::collections::VecDeque;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::error::Result;
use crate::model::MemoryRecord;

/// Working memory — current task/conversation state.
/// In-RAM only, bounded by an explicit token/entry budget.
/// Forced prioritization under a budget is itself part of what makes retrieval meaningful.
pub struct WorkingMemory {
    entries: RwLock<VecDeque<MemoryRecord>>,
    max_entries: usize,
    max_tokens: usize,
    current_tokens: RwLock<usize>,
}

impl WorkingMemory {
    /// Create a new working memory with the given capacity
    pub fn new(max_entries: usize, max_tokens: usize) -> Self {
        Self {
            entries: RwLock::new(VecDeque::with_capacity(max_entries)),
            max_entries,
            max_tokens,
            current_tokens: RwLock::new(0),
        }
    }

    /// Add a record to working memory.
    /// If at capacity, evicts the lowest-priority entry.
    pub fn push(&self, record: MemoryRecord) -> Result<()> {
        let record_tokens = estimate_tokens(&record.content);

        let mut entries = self.entries.write();
        let mut tokens = self.current_tokens.write();

        // If adding this would exceed token budget, evict until we have room
        while *tokens + record_tokens > self.max_tokens && !entries.is_empty() {
            if let Some(evicted) = self.evict_lowest(&mut entries) {
                *tokens -= estimate_tokens(&evicted.content);
            } else {
                break;
            }
        }

        // If still at entry capacity, evict oldest
        while entries.len() >= self.max_entries {
            if let Some(evicted) = entries.pop_front() {
                *tokens -= estimate_tokens(&evicted.content);
            } else {
                break;
            }
        }

        *tokens += record_tokens;
        entries.push_back(record);
        Ok(())
    }

    /// Get all entries in working memory (most recent last)
    pub fn entries(&self) -> Vec<MemoryRecord> {
        self.entries.read().iter().cloned().collect()
    }

    /// Get a specific entry by ID
    pub fn get(&self, id: Uuid) -> Option<MemoryRecord> {
        self.entries.read().iter().find(|r| r.id == id).cloned()
    }

    /// Remove a specific entry
    pub fn remove(&self, id: Uuid) -> Option<MemoryRecord> {
        let mut entries = self.entries.write();
        if let Some(pos) = entries.iter().position(|r| r.id == id) {
            let removed = entries.remove(pos).unwrap();
            let mut tokens = self.current_tokens.write();
            *tokens -= estimate_tokens(&removed.content);
            Some(removed)
        } else {
            None
        }
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.entries.write().clear();
        *self.current_tokens.write() = 0;
    }

    /// Get current token usage
    pub fn current_tokens(&self) -> usize {
        *self.current_tokens.read()
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Evict the lowest-priority entry (by importance * recency)
    fn evict_lowest(&self, entries: &mut VecDeque<MemoryRecord>) -> Option<MemoryRecord> {
        let now = chrono::Utc::now();
        let mut worst_idx = 0;
        let mut worst_score = f32::MAX;

        for (i, entry) in entries.iter().enumerate() {
            let recency = (-0.01 * (now - entry.last_accessed).num_milliseconds() as f32 / 60000.0).exp();
            let score = entry.importance * recency;

            if score < worst_score {
                worst_score = score;
                worst_idx = i;
            }
        }

        entries.remove(worst_idx)
    }
}

/// Rough token estimate: ~4 chars per token (English text)
fn estimate_tokens(text: &str) -> usize {
    (text.len() + 3) / 4
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;
    use crate::trust::TrustTier;
    use crate::visibility::Visibility;
    use crate::vector_clock::VectorClock;
    use chrono::Utc;

    fn test_record(content: &str, importance: f32) -> MemoryRecord {
        let now = Utc::now();
        MemoryRecord {
            id: Uuid::new_v4(),
            tenant_id: TenantId("t1".to_string()),
            agent_id: AgentId("a1".to_string()),
            tier: MemoryTier::Working,
            schema_version: 1,
            content: content.to_string(),
            embedding: vec![],
            embedding_model_version: "test".to_string(),
            importance,
            retention_class: RetentionClass::Standard,
            trust_tier: TrustTier::Standard,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            visibility: Visibility::Private,
            source_ids: vec![],
            superseded_by: None,
            vector_clock: VectorClock::new(),
        }
    }

    #[test]
    fn test_push_and_get() {
        let wm = WorkingMemory::new(10, 10000);
        let record = test_record("Hello world", 0.5);
        let id = record.id;

        wm.push(record).unwrap();
        assert_eq!(wm.len(), 1);
        assert!(wm.get(id).is_some());
    }

    #[test]
    fn test_capacity_eviction() {
        let wm = WorkingMemory::new(3, 10000);
        for i in 0..5 {
            wm.push(test_record(&format!("Record {}", i), 0.5)).unwrap();
        }
        assert_eq!(wm.len(), 3);
    }

    #[test]
    fn test_token_budget() {
        // Each record is ~4 tokens, budget is 20 tokens (5 records max)
        let wm = WorkingMemory::new(100, 20);
        for i in 0..10 {
            wm.push(test_record(&format!("Record {}", i), 0.5)).unwrap();
        }
        assert!(wm.current_tokens() <= 20);
    }

    #[test]
    fn test_remove() {
        let wm = WorkingMemory::new(10, 10000);
        let record = test_record("Hello", 0.5);
        let id = record.id;

        wm.push(record).unwrap();
        assert!(wm.remove(id).is_some());
        assert_eq!(wm.len(), 0);
    }

    #[test]
    fn test_clear() {
        let wm = WorkingMemory::new(10, 10000);
        wm.push(test_record("A", 0.5)).unwrap();
        wm.push(test_record("B", 0.5)).unwrap();
        wm.clear();
        assert_eq!(wm.len(), 0);
        assert_eq!(wm.current_tokens(), 0);
    }
}
