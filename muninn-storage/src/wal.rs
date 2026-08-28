use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, Seek, SeekFrom, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use sha2::{Sha256, Digest};
use tracing::{info, warn};

use muninn_core::error::{Error, Result};
use muninn_core::model::MemoryRecord;

/// WAL entry types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WalEntry {
    Write(MemoryRecord),
    Supersede { id: uuid::Uuid, superseded_by: uuid::Uuid },
    BatchWrite(Vec<MemoryRecord>),
}

/// WAL record on disk — entry + checksum for corruption detection
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WalRecord {
    entry: WalEntry,
    checksum: [u8; 32],
    offset: u64,
}

/// Write-Ahead Log — fsynced before ack, replayed on crash recovery.
///
/// Design:
/// - Append-only log file with checksummed entries
/// - Entries fsynced to disk before write acknowledgment
/// - Checkpointing creates snapshots of the applied state
/// - On replay, entries after the last checkpoint are applied in order
pub struct Wal {
    writer: RwLock<BufWriter<File>>,
    current_offset: AtomicU64,
    wal_dir: PathBuf,
    segment_index: AtomicU64,
    max_segment_size: u64,
}

impl Wal {
    /// Open or create a WAL at the given directory
    pub fn open(wal_dir: &Path, max_segment_size: u64) -> Result<Self> {
        std::fs::create_dir_all(wal_dir)?;

        let segment_index = Self::find_latest_segment(wal_dir)?;
        let segment_path = Self::segment_path(wal_dir, segment_index);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)?;

        let current_offset = file.metadata()?.len();

        info!(
            "WAL opened at {:?}, segment {}, offset {}",
            segment_path, segment_index, current_offset
        );

        Ok(Self {
            writer: RwLock::new(BufWriter::new(file)),
            current_offset: AtomicU64::new(current_offset),
            wal_dir: wal_dir.to_path_buf(),
            segment_index: AtomicU64::new(segment_index),
            max_segment_size,
        })
    }

    /// Append a single entry to the WAL. Returns the offset.
    /// This fsyncs the entry to disk before returning.
    pub fn append(&self, entry: WalEntry) -> Result<u64> {
        let mut writer = self.writer.write();

        let record = WalRecord {
            entry,
            checksum: [0; 32],
            offset: self.current_offset.load(Ordering::Relaxed),
        };

        // Serialize the entry
        let entry_bytes = bincode::serialize(&record.entry)
            .map_err(|e| Error::Wal(format!("Serialization failed: {}", e)))?;

        // Calculate checksum over entry bytes
        let mut hasher = Sha256::new();
        hasher.update(&entry_bytes);
        let checksum: [u8; 32] = hasher.finalize().into();

        let record = WalRecord {
            checksum,
            ..record
        };

        // Write record: [length:u32][entry_bytes][checksum:32bytes]
        let record_bytes = bincode::serialize(&record)
            .map_err(|e| Error::Wal(format!("Record serialization failed: {}", e)))?;

        let length = record_bytes.len() as u32;

        // Write length
        writer.write_all(&length.to_le_bytes())?;
        // Write record
        writer.write_all(&record_bytes)?;
        // Fsync — this is the durability guarantee
        writer.flush()?;
        let file = writer.get_ref();
        file.sync_data()?;

        let offset = self.current_offset.fetch_add(
            4 + record_bytes.len() as u64,
            Ordering::SeqCst,
        );

        // Rotate segment if needed
        let new_offset = self.current_offset.load(Ordering::Relaxed);
        if new_offset >= self.max_segment_size {
            drop(writer);
            self.rotate_segment()?;
        }

        Ok(offset)
    }

    /// Replay the WAL from a given offset, calling the callback for each entry.
    /// Used for crash recovery.
    pub fn replay<F>(&self, from_offset: u64, mut callback: F) -> Result<u64>
    where
        F: FnMut(WalEntry, u64) -> Result<()>,
    {
        let mut total_entries = 0u64;
        let mut current_offset = from_offset;

        // Read all segments
        let segments = self.list_segments()?;

        for segment_path in &segments {
            let file = File::open(segment_path)?;
            let file_size = file.metadata()?.len();

            let mut reader = std::io::BufReader::new(file);
            reader.seek(SeekFrom::Start(current_offset.min(file_size)))?;

            loop {
                // Read length
                let mut length_buf = [0u8; 4];
                match reader.read_exact(&mut length_buf) {
                    Ok(_) => {}
                    Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(Error::Wal(format!("Read failed: {}", e))),
                }

                let length = u32::from_le_bytes(length_buf) as usize;

                // Read record
                let mut record_buf = vec![0u8; length];
                reader.read_exact(&mut record_buf)?;

                let record: WalRecord = bincode::deserialize(&record_buf)
                    .map_err(|e| Error::Wal(format!("Deserialization failed: {}", e)))?;

                // Verify checksum
                let entry_bytes = bincode::serialize(&record.entry)
                    .map_err(|e| Error::Wal(format!("Re-serialization failed: {}", e)))?;
                let mut hasher = Sha256::new();
                hasher.update(&entry_bytes);
                let computed_checksum: [u8; 32] = hasher.finalize().into();

                if computed_checksum != record.checksum {
                    warn!(
                        "WAL checksum mismatch at offset {}, possible corruption",
                        record.offset
                    );
                    // Continue — partial replay is better than no replay
                    break;
                }

                callback(record.entry, record.offset)?;
                total_entries += 1;
            }

            // Reset offset for next segment
            current_offset = 0;
        }

        Ok(total_entries)
    }

    /// Get the current write offset
    pub fn current_offset(&self) -> u64 {
        self.current_offset.load(Ordering::Relaxed)
    }

    /// Create a checkpoint marker (does not compact, just marks a recovery point)
    pub fn checkpoint(&self, applied_offset: u64) -> Result<()> {
        let checkpoint_path = self.wal_dir.join("checkpoint");
        std::fs::write(checkpoint_path, applied_offset.to_le_bytes())?;
        info!("WAL checkpoint at offset {}", applied_offset);
        Ok(())
    }

    /// Read the last checkpoint offset
    pub fn last_checkpoint_offset(&self) -> Result<u64> {
        let checkpoint_path = self.wal_dir.join("checkpoint");
        if !checkpoint_path.exists() {
            return Ok(0);
        }
        let bytes = std::fs::read(checkpoint_path)?;
        if bytes.len() != 8 {
            return Ok(0);
        }
        let offset = u64::from_le_bytes(bytes.try_into().unwrap());
        Ok(offset)
    }

    /// Rotate to a new WAL segment
    fn rotate_segment(&self) -> Result<()> {
        let mut writer = self.writer.write();
        writer.flush()?;

        let new_index = self.segment_index.fetch_add(1, Ordering::SeqCst) + 1;
        let segment_path = Self::segment_path(&self.wal_dir, new_index);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)?;

        *writer = BufWriter::new(file);
        self.current_offset.store(0, Ordering::Relaxed);

        info!("WAL rotated to segment {}", new_index);
        Ok(())
    }

    /// Find the latest segment index
    fn find_latest_segment(wal_dir: &Path) -> Result<u64> {
        let mut max_index = 0u64;
        if let Ok(entries) = std::fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(idx) = name_str.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                    if let Ok(index) = idx.parse::<u64>() {
                        max_index = max_index.max(index);
                    }
                }
            }
        }
        Ok(max_index)
    }

    /// List all WAL segments in order
    fn list_segments(&self) -> Result<Vec<PathBuf>> {
        let mut segments: Vec<(u64, PathBuf)> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(idx) = name_str.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                    if let Ok(index) = idx.parse::<u64>() {
                        segments.push((index, entry.path()));
                    }
                }
            }
        }
        segments.sort_by_key(|(idx, _)| *idx);
        Ok(segments.into_iter().map(|(_, path)| path).collect())
    }

    /// Segment file path
    fn segment_path(wal_dir: &Path, index: u64) -> PathBuf {
        wal_dir.join(format!("wal_{:06}.log", index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use muninn_core::model::*;
    use muninn_core::trust::TrustTier;
    use muninn_core::visibility::Visibility;
    use muninn_core::vector_clock::VectorClock;
    use uuid::Uuid;

    fn test_record() -> MemoryRecord {
        let now = Utc::now();
        MemoryRecord {
            id: Uuid::new_v4(),
            tenant_id: TenantId("tenant1".to_string()),
            agent_id: AgentId("agent1".to_string()),
            tier: MemoryTier::Episodic,
            schema_version: 1,
            content: "test memory".to_string(),
            embedding: vec![0.1, 0.2, 0.3],
            embedding_model_version: "test-v1".to_string(),
            importance: 0.8,
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
    fn test_wal_append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(dir.path(), 1024 * 1024).unwrap();

        let record = test_record();
        let offset1 = wal.append(WalEntry::Write(record.clone())).unwrap();
        assert_eq!(offset1, 0);

        let record2 = test_record();
        let offset2 = wal.append(WalEntry::Write(record2.clone())).unwrap();
        assert!(offset2 > offset1);

        // Replay
        let mut entries = Vec::new();
        wal.replay(0, |entry, off| {
            entries.push((entry, off));
            Ok(())
        })
        .unwrap();

        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_wal_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(dir.path(), 1024 * 1024).unwrap();

        wal.checkpoint(1024).unwrap();
        let offset = wal.last_checkpoint_offset().unwrap();
        assert_eq!(offset, 1024);
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
// commit 130 1788294955650507257
// commit 226 1788294957148410288
// commit 274 1788294957911545756
// commit 346 1788294959028894183
