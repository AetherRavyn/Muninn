use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::{Read, Write, BufWriter, BufReader};
use std::time::Instant;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use uuid::Uuid;
use tracing::info;

use muninn_core::error::{Error, Result};
use muninn_core::model::MemoryRecord;

/// Snapshot metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
    pub record_count: u64,
    pub size_bytes: u64,
    pub checksum: String,
    pub wal_offset: u64,
    pub description: String,
}

/// Snapshot manager for DR backup and restore.
/// Creates point-in-time snapshots of shard state.
pub struct SnapshotManager {
    snapshot_dir: PathBuf,
    snapshots: RwLock<Vec<SnapshotMeta>>,
}

impl SnapshotManager {
    /// Create a new snapshot manager
    pub fn new(snapshot_dir: PathBuf) -> Self {
        fs::create_dir_all(&snapshot_dir).ok();

        // Load existing snapshots
        let snapshots = Self::load_snapshot_index(&snapshot_dir);

        Self {
            snapshot_dir,
            snapshots: RwLock::new(snapshots),
        }
    }

    /// Create a snapshot of the current state
    pub fn create_snapshot(
        &self,
        records: &std::collections::HashMap<Uuid, MemoryRecord>,
        wal_offset: u64,
        description: &str,
    ) -> Result<SnapshotMeta> {
        let start = Instant::now();
        let snapshot_id = Uuid::new_v4();
        let snapshot_path = self.snapshot_dir.join(format!("snapshot_{}.dat", snapshot_id));

        info!("Creating snapshot {} with {} records", snapshot_id, records.len());

        // Serialize records
        let mut file = File::create(&snapshot_path)
            .map_err(|e| Error::Storage(format!("Failed to create snapshot file: {}", e)))?;

        let mut writer = BufWriter::new(&mut file);
        let mut hasher = Sha256::new();
        let mut total_bytes = 0u64;

        // Write header
        let record_count = records.len() as u64;
        writer.write_all(&record_count.to_le_bytes())
            .map_err(|e| Error::Storage(format!("Write failed: {}", e)))?;

        // Write each record
        for record in records.values() {
            let serialized = bincode::serialize(record)
                .map_err(|e| Error::Storage(format!("Serialization failed: {}", e)))?;

            let len = serialized.len() as u32;
            writer.write_all(&len.to_le_bytes())
                .map_err(|e| Error::Storage(format!("Write failed: {}", e)))?;
            writer.write_all(&serialized)
                .map_err(|e| Error::Storage(format!("Write failed: {}", e)))?;

            hasher.update(&len.to_le_bytes());
            hasher.update(&serialized);
            total_bytes += 4 + serialized.len() as u64;
        }

        writer.flush()
            .map_err(|e| Error::Storage(format!("Flush failed: {}", e)))?;

        let checksum = format!("{:x}", hasher.finalize());

        let meta = SnapshotMeta {
            id: snapshot_id,
            created_at: Utc::now(),
            record_count,
            size_bytes: total_bytes,
            checksum: checksum.clone(),
            wal_offset,
            description: description.to_string(),
        };

        // Save metadata
        let meta_path = self.snapshot_dir.join(format!("snapshot_{}.json", snapshot_id));
        let meta_json = serde_json::to_string_pretty(&meta)
            .map_err(|e| Error::Serialization(format!("Meta serialization failed: {}", e)))?;
        fs::write(&meta_path, meta_json)
            .map_err(|e| Error::Storage(format!("Failed to write meta: {}", e)))?;

        // Update index
        self.snapshots.write().push(meta.clone());
        self.save_snapshot_index()?;

        let duration = start.elapsed();
        info!(
            "Snapshot {} created in {:.2?} ({} records, {} bytes, checksum: {})",
            snapshot_id, duration, record_count, total_bytes, &checksum[..16]
        );

        Ok(meta)
    }

    /// Restore from a snapshot
    pub fn restore_snapshot(&self, snapshot_id: Uuid) -> Result<Vec<MemoryRecord>> {
        let start = Instant::now();
        let snapshot_path = self.snapshot_dir.join(format!("snapshot_{}.dat", snapshot_id));

        if !snapshot_path.exists() {
            return Err(Error::NotFound(format!("Snapshot {} not found", snapshot_id)));
        }

        info!("Restoring snapshot {}", snapshot_id);

        // Read and verify
        let file = File::open(&snapshot_path)
            .map_err(|e| Error::Storage(format!("Failed to open snapshot: {}", e)))?;
        let mut reader = BufReader::new(file);

        // Read header
        let mut count_buf = [0u8; 8];
        reader.read_exact(&mut count_buf)
            .map_err(|e| Error::Storage(format!("Read failed: {}", e)))?;
        let record_count = u64::from_le_bytes(count_buf);

        let mut records = Vec::with_capacity(record_count as usize);
        let mut hasher = Sha256::new();

        for _ in 0..record_count {
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)
                .map_err(|e| Error::Storage(format!("Read failed: {}", e)))?;
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)
                .map_err(|e| Error::Storage(format!("Read failed: {}", e)))?;

            hasher.update(&len_buf);
            hasher.update(&data);

            let record: MemoryRecord = bincode::deserialize(&data)
                .map_err(|e| Error::Storage(format!("Deserialization failed: {}", e)))?;

            records.push(record);
        }

        let checksum = format!("{:x}", hasher.finalize());

        // Verify checksum
        let meta = self.get_snapshot_meta(snapshot_id)?;
        if checksum != meta.checksum {
            return Err(Error::Storage(format!(
                "Checksum mismatch: expected {}, got {}",
                meta.checksum, checksum
            )));
        }

        let duration = start.elapsed();
        info!(
            "Snapshot {} restored in {:.2?} ({} records)",
            snapshot_id, duration, records.len()
        );

        Ok(records)
    }

    /// List all available snapshots
    pub fn list_snapshots(&self) -> Vec<SnapshotMeta> {
        self.snapshots.read().clone()
    }

    /// Get metadata for a specific snapshot
    pub fn get_snapshot_meta(&self, snapshot_id: Uuid) -> Result<SnapshotMeta> {
        self.snapshots
            .read()
            .iter()
            .find(|s| s.id == snapshot_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("Snapshot {} not found", snapshot_id)))
    }

    /// Delete a snapshot
    pub fn delete_snapshot(&self, snapshot_id: Uuid) -> Result<()> {
        let data_path = self.snapshot_dir.join(format!("snapshot_{}.dat", snapshot_id));
        let meta_path = self.snapshot_dir.join(format!("snapshot_{}.json", snapshot_id));

        if data_path.exists() {
            fs::remove_file(&data_path)
                .map_err(|e| Error::Storage(format!("Failed to delete snapshot data: {}", e)))?;
        }
        if meta_path.exists() {
            fs::remove_file(&meta_path)
                .map_err(|e| Error::Storage(format!("Failed to delete snapshot meta: {}", e)))?;
        }

        self.snapshots.write().retain(|s| s.id != snapshot_id);
        self.save_snapshot_index()?;

        info!("Deleted snapshot {}", snapshot_id);
        Ok(())
    }

    /// Get the latest snapshot
    pub fn latest_snapshot(&self) -> Option<SnapshotMeta> {
        self.snapshots
            .read()
            .iter()
            .max_by_key(|s| s.created_at)
            .cloned()
    }

    /// Prune old snapshots, keeping the most recent N
    pub fn prune_snapshots(&self, keep_count: usize) -> Result<usize> {
        let mut snapshots = self.snapshots.read().clone();
        snapshots.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        let mut deleted = 0;
        for snapshot in snapshots.iter().skip(keep_count) {
            self.delete_snapshot(snapshot.id)?;
            deleted += 1;
        }

        Ok(deleted)
    }

    /// Save snapshot index to disk
    fn save_snapshot_index(&self) -> Result<()> {
        let index_path = self.snapshot_dir.join("snapshots.json");
        let snapshots = self.snapshots.read().clone();
        let json = serde_json::to_string_pretty(&snapshots)
            .map_err(|e| Error::Serialization(format!("Index serialization failed: {}", e)))?;
        fs::write(&index_path, json)
            .map_err(|e| Error::Storage(format!("Failed to write index: {}", e)))?;
        Ok(())
    }

    /// Load snapshot index from disk
    fn load_snapshot_index(snapshot_dir: &Path) -> Vec<SnapshotMeta> {
        let index_path = snapshot_dir.join("snapshots.json");
        if index_path.exists() {
            if let Ok(content) = fs::read_to_string(&index_path) {
                if let Ok(snapshots) = serde_json::from_str(&content) {
                    return snapshots;
                }
            }
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muninn_core::model::*;
    use muninn_core::trust::TrustTier;
    use muninn_core::visibility::Visibility;
    use muninn_core::vector_clock::VectorClock;

    fn test_record(id: &str) -> MemoryRecord {
        let now = Utc::now();
        MemoryRecord {
            id: Uuid::parse_str(id).unwrap_or_else(|_| Uuid::new_v4()),
            tenant_id: TenantId("t1".to_string()),
            agent_id: AgentId("a1".to_string()),
            tier: MemoryTier::Episodic,
            schema_version: 1,
            content: format!("Test record {}", id),
            embedding: vec![0.1, 0.2, 0.3],
            embedding_model_version: "test".to_string(),
            importance: 0.5,
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
    fn test_snapshot_create_and_restore() {
        let dir = tempfile::tempdir().unwrap();
        let manager = SnapshotManager::new(dir.path().to_path_buf());

        // Create test records
        let mut records = std::collections::HashMap::new();
        for i in 0..10 {
            let record = test_record(&format!("record-{}", i));
            records.insert(record.id, record);
        }

        // Create snapshot
        let meta = manager.create_snapshot(&records, 100, "Test snapshot").unwrap();
        assert_eq!(meta.record_count, 10);

        // Restore snapshot
        let restored = manager.restore_snapshot(meta.id).unwrap();
        assert_eq!(restored.len(), 10);
    }

    #[test]
    fn test_snapshot_checksum_verification() {
        let dir = tempfile::tempdir().unwrap();
        let manager = SnapshotManager::new(dir.path().to_path_buf());

        let mut records = std::collections::HashMap::new();
        let record = test_record("test-1");
        records.insert(record.id, record);

        let meta = manager.create_snapshot(&records, 0, "Checksum test").unwrap();

        // Corrupt the snapshot file
        let snapshot_path = dir.path().join(format!("snapshot_{}.dat", meta.id));
        let mut content = fs::read(&snapshot_path).unwrap();
        content[100] ^= 0xFF; // Flip a byte
        fs::write(&snapshot_path, content).unwrap();

        // Restore should fail with checksum mismatch
        let result = manager.restore_snapshot(meta.id);
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_list_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let manager = SnapshotManager::new(dir.path().to_path_buf());

        let records = std::collections::HashMap::new();

        // Create multiple snapshots
        let meta1 = manager.create_snapshot(&records, 0, "First").unwrap();
        let meta2 = manager.create_snapshot(&records, 0, "Second").unwrap();

        assert_eq!(manager.list_snapshots().len(), 2);

        // Delete one
        manager.delete_snapshot(meta1.id).unwrap();
        assert_eq!(manager.list_snapshots().len(), 1);

        // Verify remaining
        let remaining = manager.list_snapshots();
        assert_eq!(remaining[0].id, meta2.id);
    }

    #[test]
    fn test_snapshot_prune() {
        let dir = tempfile::tempdir().unwrap();
        let manager = SnapshotManager::new(dir.path().to_path_buf());

        let records = std::collections::HashMap::new();

        // Create 5 snapshots
        for i in 0..5 {
            manager.create_snapshot(&records, i, &format!("Snapshot {}", i)).unwrap();
        }

        assert_eq!(manager.list_snapshots().len(), 5);

        // Keep only 2
        let deleted = manager.prune_snapshots(2).unwrap();
        assert_eq!(deleted, 3);
        assert_eq!(manager.list_snapshots().len(), 2);
    }
}
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
// commit 12 1788294953862995898
// commit 36 1788294954216012863
// commit 84 1788294954944851347
// commit 156 1788294956063069524
// commit 180 1788294956427455028
// commit 228 1788294957178815891
