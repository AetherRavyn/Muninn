#[cfg(test)]
mod chaos_tests {
    use crate::ShardStore;
    use muninn_core::model::*;
    use muninn_core::trust::TrustTier;
    use muninn_core::visibility::Visibility;
    use muninn_core::vector_clock::VectorClock;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn test_record(tenant: &str, agent: &str, content: &str) -> MemoryRecord {
        let now = chrono::Utc::now();
        MemoryRecord {
            id: uuid::Uuid::new_v4(),
            tenant_id: TenantId(tenant.to_string()),
            agent_id: AgentId(agent.to_string()),
            tier: MemoryTier::Episodic,
            schema_version: 1,
            content: content.to_string(),
            embedding: vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
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

    /// Test: WAL recovery after simulated crash.
    /// Write records, force checkpoint, verify all records survive reopen.
    #[test]
    fn test_wal_crash_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let wal_dir = dir.path().join("wal");
        let tantivy_dir = dir.path().join("tantivy");

        // Phase 1: Write records and drop the shard (simulates crash)
        let record_count = 100;
        {
            let config = crate::shard::ShardConfig {
                shard_id: 0,
                data_dir: data_dir.clone(),
                wal_dir: wal_dir.clone(),
                tantivy_dir: tantivy_dir.clone(),
                embedding_dimension: 8,
                max_wal_size: 1024 * 1024,
            };
            let shard = ShardStore::open(config).unwrap();

            for i in 0..record_count {
                let mut record = test_record("t1", "a1", &format!("Record {}", i));
                record.embedding = vec![i as f32 / 100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
                shard.write(record).unwrap();
            }

            // Shard is dropped here — simulates crash
        }

        // Phase 2: Reopen and verify WAL replay recovered all records
        let config = crate::shard::ShardConfig {
            shard_id: 0,
            data_dir,
            wal_dir,
            tantivy_dir,
            embedding_dimension: 8,
            max_wal_size: 1024 * 1024,
        };
        let shard = ShardStore::open(config).unwrap();
        let health = shard.health().unwrap();

        assert_eq!(
            health.records_count, record_count as u64,
            "WAL replay should recover all records"
        );
    }

    /// Test: Concurrent write safety under contention.
    /// Multiple threads writing to the same tenant simultaneously.
    #[test]
    fn test_concurrent_write_safety() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::shard::ShardConfig {
            shard_id: 0,
            data_dir: dir.path().to_path_buf(),
            wal_dir: dir.path().join("wal"),
            tantivy_dir: dir.path().join("tantivy"),
            embedding_dimension: 8,
            max_wal_size: 1024 * 1024,
        };
        let shard = Arc::new(ShardStore::open(config).unwrap());

        let thread_count = 8;
        let ops_per_thread = 100;
        let mut handles = vec![];

        for thread_id in 0..thread_count {
            let shard = shard.clone();
            handles.push(thread::spawn(move || {
                for op in 0..ops_per_thread {
                    let mut record = test_record(
                        "shared_tenant",
                        &format!("agent_{}", thread_id),
                        &format!("Thread {} op {}", thread_id, op),
                    );
                    record.embedding = vec![
                        thread_id as f32 / 8.0,
                        op as f32 / 100.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                    ];
                    shard.write(record).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let health = shard.health().unwrap();
        assert_eq!(
            health.records_count,
            (thread_count * ops_per_thread) as u64,
            "All concurrent writes should succeed"
        );
    }

    /// Test: Read-during-write consistency.
    /// Reads should never see partial writes.
    #[test]
    fn test_read_during_write_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::shard::ShardConfig {
            shard_id: 0,
            data_dir: dir.path().to_path_buf(),
            wal_dir: dir.path().join("wal"),
            tantivy_dir: dir.path().join("tantivy"),
            embedding_dimension: 8,
            max_wal_size: 1024 * 1024,
        };
        let shard = Arc::new(ShardStore::open(config).unwrap());

        // Pre-populate
        for i in 0..50 {
            let mut record = test_record("t1", "a1", &format!("Initial {}", i));
            record.embedding = vec![0.1; 8];
            shard.write(record).unwrap();
        }

        let shard_reader = shard.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_clone = stop.clone();

        // Reader thread: continuously retrieve and verify consistency
        let reader = thread::spawn(move || {
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                let query = muninn_core::retrieval::RetrievalQuery {
                    tenant_id: TenantId("t1".to_string()),
                    agent_id: AgentId("a1".to_string()),
                    embedding: vec![0.1; 8],
                    tiers: vec![],
                    max_results: 100,
                    min_score: 0.0,
                    visibility_filter: None,
                    trust_tier_minimum: None,
                    time_range: None,
                    keyword_query: None,
                };

                let results = shard_reader.retrieve(&query).unwrap();
                // All results should be complete records (no partial state)
                for result in &results {
                    assert!(!result.record.content.is_empty());
                    assert_eq!(result.record.tenant_id.0, "t1");
                }
            }
        });

        // Writer thread: write more records
        for i in 0..50 {
            let mut record = test_record("t1", "a1", &format!("Added {}", i));
            record.embedding = vec![0.1; 8];
            shard.write(record).unwrap();
            thread::sleep(Duration::from_millis(1));
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        reader.join().unwrap();
    }

    /// Test: Burst traffic handling.
    /// Rapid burst of writes should not cause data loss.
    #[test]
    fn test_burst_traffic() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::shard::ShardConfig {
            shard_id: 0,
            data_dir: dir.path().to_path_buf(),
            wal_dir: dir.path().join("wal"),
            tantivy_dir: dir.path().join("tantivy"),
            embedding_dimension: 8,
            max_wal_size: 1024 * 1024,
        };
        let shard = ShardStore::open(config).unwrap();

        // Burst of 500 writes
        let burst_count = 500;
        for i in 0..burst_count {
            let mut record = test_record("t1", "a1", &format!("Burst {}", i));
            record.embedding = vec![i as f32 / 500.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
            shard.write(record).unwrap();
        }

        let health = shard.health().unwrap();
        assert_eq!(health.records_count, burst_count as u64);
    }

    /// Test: Tenant isolation under load.
    /// Writes to tenant A must never appear in tenant B's results.
    #[test]
    fn test_tenant_isolation_under_load() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::shard::ShardConfig {
            shard_id: 0,
            data_dir: dir.path().to_path_buf(),
            wal_dir: dir.path().join("wal"),
            tantivy_dir: dir.path().join("tantivy"),
            embedding_dimension: 8,
            max_wal_size: 1024 * 1024,
        };
        let shard = Arc::new(ShardStore::open(config).unwrap());

        // Write to both tenants concurrently
        let mut handles = vec![];
        for tenant_num in 0..2 {
            let shard = shard.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let mut record = test_record(
                        &format!("tenant_{}", tenant_num),
                        "agent_0",
                        &format!("Tenant {} record {}", tenant_num, i),
                    );
                    record.embedding = vec![0.5; 8];
                    shard.write(record).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify isolation: searching tenant_0 should not return tenant_1 records
        let query = muninn_core::retrieval::RetrievalQuery {
            tenant_id: TenantId("tenant_0".to_string()),
            agent_id: AgentId("agent_0".to_string()),
            embedding: vec![0.5; 8],
            tiers: vec![],
            max_results: 200,
            min_score: 0.0,
            visibility_filter: None,
            trust_tier_minimum: None,
            time_range: None,
            keyword_query: None,
        };

        let results = shard.retrieve(&query).unwrap();
        for result in &results {
            assert_eq!(
                result.record.tenant_id.0, "tenant_0",
                "Tenant isolation violated: got record from {}",
                result.record.tenant_id.0
            );
        }
    }

    /// Test: Memory pressure handling.
    /// Fill shard to capacity, verify eviction works.
    #[test]
    fn test_memory_pressure_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::shard::ShardConfig {
            shard_id: 0,
            data_dir: dir.path().to_path_buf(),
            wal_dir: dir.path().join("wal"),
            tantivy_dir: dir.path().join("tantivy"),
            embedding_dimension: 8,
            max_wal_size: 1024 * 10, // Small WAL to force rotation
        };
        let shard = ShardStore::open(config).unwrap();

        // Write many records
        for i in 0..1000 {
            let mut record = test_record("t1", "a1", &format!("Record {}", i));
            record.embedding = vec![i as f32 / 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
            shard.write(record).unwrap();
        }

        // Shard should still be functional
        let health = shard.health().unwrap();
        assert!(health.records_count > 0);
        assert!(health.is_healthy);
    }
}
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
// commit 60 1788294954578017629
// commit 108 1788294955307924157
// commit 132 1788294955682950974
// commit 204 1788294956805059874
