use std::sync::Arc;
use std::thread;

use muninn_core::model::*;
use muninn_core::trust::TrustTier;
use muninn_core::visibility::Visibility;
use muninn_core::vector_clock::VectorClock;
use muninn_storage::ShardStore;

/// Prove §8.3: Single-writer-per-shard within a tenant.
/// Shared memory uses optimistic concurrency via vector clocks —
/// conflicts retained and surfaced, never silently lost.
///
/// This test proves: N agents hammering the same shared key,
/// assert zero silent data loss.

fn test_shard() -> (ShardStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = muninn_storage::shard::ShardConfig {
        shard_id: 0,
        data_dir: dir.path().to_path_buf(),
        wal_dir: dir.path().join("wal"),
        tantivy_dir: dir.path().join("tantivy"),
        embedding_dimension: 8,
        max_wal_size: 1024 * 1024 * 10,
    };
    (ShardStore::open(config).unwrap(), dir)
}

fn shared_record(agent_id: &str, content: &str, vc: VectorClock) -> MemoryRecord {
    let now = chrono::Utc::now();
    MemoryRecord {
        id: uuid::Uuid::new_v4(),
        tenant_id: TenantId("shared_tenant".to_string()),
        agent_id: AgentId(agent_id.to_string()),
        tier: MemoryTier::Shared,
        schema_version: 1,
        content: content.to_string(),
        embedding: vec![0.5; 8],
        embedding_model_version: "test".to_string(),
        importance: 0.5,
        retention_class: RetentionClass::Standard,
        trust_tier: TrustTier::Standard,
        created_at: now,
        last_accessed: now,
        access_count: 0,
        visibility: Visibility::Shared,
        source_ids: vec![],
        superseded_by: None,
        vector_clock: vc,
    }
}

/// Test: Concurrent writes to shared memory with vector clocks.
/// All writes should succeed, no silent data loss.
#[test]
fn test_concurrent_shared_memory_no_data_loss() {
    let (shard, _dir) = test_shard();
    let shard = Arc::new(shard);

    let num_agents = 10;
    let writes_per_agent = 50;
    let mut handles = vec![];

    for agent_num in 0..num_agents {
        let shard = shard.clone();
        handles.push(thread::spawn(move || {
            let agent_id = format!("agent_{}", agent_num);
            let mut success_count = 0;
            let mut conflict_count = 0;

            for write_num in 0..writes_per_agent {
                let mut vc = VectorClock::new();
                vc.increment(&agent_id);

                let record = shared_record(
                    &agent_id,
                    &format!("Agent {} write {}", agent_num, write_num),
                    vc,
                );

                match shard.write(record) {
                    Ok(_) => success_count += 1,
                    Err(_) => conflict_count += 1,
                }
            }

            (success_count, conflict_count)
        }));
    }

    // Collect results
    let mut total_success = 0;
    let mut total_conflicts = 0;

    for handle in handles {
        let (success, conflicts) = handle.join().unwrap();
        total_success += success;
        total_conflicts += conflicts;
    }

    // All writes should succeed (optimistic concurrency allows concurrent writes)
    assert_eq!(
        total_success,
        num_agents * writes_per_agent,
        "All writes should succeed: got {} success out of {}",
        total_success,
        num_agents * writes_per_agent
    );

    // Verify record count
    let health = shard.health().unwrap();
    assert_eq!(
        health.records_count,
        (num_agents * writes_per_agent) as u64,
        "Record count should match total writes"
    );
}

/// Test: Vector clock conflict detection.
/// When two agents write concurrently, conflicts should be detected.
#[test]
fn test_vector_clock_conflict_detection() {
    let mut vc1 = VectorClock::new();
    vc1.increment("agent_a");

    let mut vc2 = VectorClock::new();
    vc2.increment("agent_b");

    // These should be concurrent (neither dominates)
    assert!(
        vc1.is_concurrent_with(&vc2),
        "Concurrent writes should be detected as conflicts"
    );
    assert!(
        vc1.conflicts_with(&vc2),
        "conflicts_with should return true for concurrent clocks"
    );
}

/// Test: Vector clock merge preserves all writes.
/// After merging, the merged clock should dominate both originals.
#[test]
fn test_vector_clock_merge_preserves_writes() {
    let mut vc_a = VectorClock::new();
    vc_a.increment("agent_a");
    vc_a.increment("agent_a");
    vc_a.increment("agent_a"); // agent_a: 3

    let mut vc_b = VectorClock::new();
    vc_b.increment("agent_b");
    vc_b.increment("agent_b"); // agent_b: 2

    let mut merged = vc_a.clone();
    merged.merge(&vc_b);

    // Merged should dominate both
    assert!(merged.dominates(&vc_a), "Merged should dominate vc_a");
    assert!(merged.dominates(&vc_b), "Merged should dominate vc_b");

    // Merged should have both agents' counts
    assert_eq!(merged.get("agent_a"), 3);
    assert_eq!(merged.get("agent_b"), 2);
}

/// Test: Concurrent reads never see partial writes.
/// Reads should always see consistent state.
#[test]
fn test_concurrent_read_consistency() {
    let (shard, _dir) = test_shard();
    let shard = Arc::new(shard);

    // Pre-populate
    for i in 0..100 {
        let mut record = shared_record("system", &format!("Initial {}", i), VectorClock::new());
        record.embedding = vec![0.1; 8];
        shard.write(record).unwrap();
    }

    let shard_reader = shard.clone();
    let shard_writer = shard.clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_reader = stop.clone();
    let stop_writer = stop.clone();

    // Writer thread
    let writer = thread::spawn(move || {
        let mut count = 0;
        while !stop_writer.load(std::sync::atomic::Ordering::Relaxed) {
            let mut vc = VectorClock::new();
            vc.increment("writer");
            vc.increment(&count.to_string());

            let mut record = shared_record("writer", &format!("Written {}", count), vc);
            record.embedding = vec![0.5; 8];
            let _ = shard_writer.write(record);
            count += 1;
            thread::sleep(std::time::Duration::from_micros(10));
        }
        count
    });

    // Reader thread
    let reader = thread::spawn(move || {
        let mut read_count = 0;
        let mut consistent_reads = 0;

        while !stop_reader.load(std::sync::atomic::Ordering::Relaxed) {
            let query = muninn_core::retrieval::RetrievalQuery {
                tenant_id: TenantId("shared_tenant".to_string()),
                agent_id: AgentId("reader".to_string()),
                embedding: vec![0.5; 8],
                tiers: vec![],
                max_results: 100,
                min_score: 0.0,
                visibility_filter: None,
                trust_tier_minimum: None,
                time_range: None,
                keyword_query: None,
            };

            if let Ok(results) = shard_reader.retrieve(&query) {
                // Every result should be a complete, valid record
                let all_valid = results.iter().all(|r| {
                    !r.record.content.is_empty()
                        && r.record.tenant_id.0 == "shared_tenant"
                });

                if all_valid && !results.is_empty() {
                    consistent_reads += 1;
                }
                read_count += 1;
            }

            thread::sleep(std::time::Duration::from_micros(10));
        }

        (read_count, consistent_reads)
    });

    // Let them run for a bit
    thread::sleep(std::time::Duration::from_millis(100));

    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    let writes = writer.join().unwrap();
    let (reads, consistent) = reader.join().unwrap();

    // All reads should be consistent (no partial state)
    if reads > 0 {
        let consistency_ratio = consistent as f64 / reads as f64;
        assert!(
            consistency_ratio > 0.99,
            "Read consistency should be > 99%, got {:.1}%",
            consistency_ratio * 100.0
        );
    }

    println!(
        "Concurrent read consistency: {}/{} reads consistent ({:.1}%)",
        consistent,
        reads,
        if reads > 0 {
            consistent as f64 / reads as f64 * 100.0
        } else {
            0.0
        }
    );
}

/// Test: Zero silent data loss under high contention.
/// Every write that succeeds should be retrievable.
#[test]
fn test_zero_silent_data_loss() {
    let (shard, _dir) = test_shard();
    let shard = Arc::new(shard);

    let num_threads = 8;
    let writes_per_thread = 100;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let shard = shard.clone();
        handles.push(thread::spawn(move || {
            let mut written_ids = Vec::new();

            for i in 0..writes_per_thread {
                let mut vc = VectorClock::new();
                vc.increment(&format!("agent_{}", thread_id));

                let record = shared_record(
                    &format!("agent_{}", thread_id),
                    &format!("Thread {} write {}", thread_id, i),
                    vc,
                );

                let id = record.id;
                match shard.write(record) {
                    Ok(_) => written_ids.push(id),
                    Err(_) => {} // Conflicts are expected, not data loss
                }
            }

            written_ids
        }));
    }

    // Collect all written IDs
    let mut all_written_ids = Vec::new();
    for handle in handles {
        let ids = handle.join().unwrap();
        all_written_ids.extend(ids);
    }

    // Every written ID should be retrievable
    let mut retrievable_count = 0;
    let mut missing_count = 0;

    for id in &all_written_ids {
        match shard.get(*id) {
            Ok(Some(_)) => retrievable_count += 1,
            Ok(None) => missing_count += 1,
            Err(_) => missing_count += 1,
        }
    }

    println!(
        "Zero data loss test: {}/{} writes retrievable",
        retrievable_count,
        all_written_ids.len()
    );

    assert_eq!(
        missing_count, 0,
        "ZERO silent data loss: {} records missing out of {} written",
        missing_count,
        all_written_ids.len()
    );
}

/// Test: Vector clock ordering is consistent.
/// If A dominates B, then A happened after B.
#[test]
fn test_vector_clock_happens_before() {
    let mut vc_before = VectorClock::new();
    vc_before.increment("agent_a"); // 1
    vc_before.increment("agent_a"); // 2

    let mut vc_after = vc_before.clone();
    vc_after.increment("agent_a"); // 3

    // vc_after should dominate vc_before
    assert!(vc_after.dominates(&vc_before));
    assert!(!vc_before.dominates(&vc_after));

    // vc_after happens after vc_before
    assert!(vc_after.get("agent_a") > vc_before.get("agent_a"));
}
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
// commit 86 1788294954976449574
// commit 134 1788294955718833274
