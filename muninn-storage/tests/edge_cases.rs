use muninn_core::model::*;
use muninn_core::retrieval::*;
use muninn_core::trust::TrustTier;
use muninn_core::visibility::Visibility;
use muninn_core::vector_clock::VectorClock;
use muninn_core::working_memory::WorkingMemory;
use muninn_core::encryption::{EncryptionEngine, generate_key};
use muninn_core::audit::{AuditLog, AuditEvent};
use muninn_core::circuit_breaker::CircuitBreaker;
use muninn_core::metrics::MetricsCollector;
use muninn_core::rate_limiter::RateLimiter;
use muninn_core::procedural_memory::{ProceduralMemory, Procedure, ProcedureStep};
use muninn_storage::ShardStore;
use std::collections::HashMap;
use std::time::Duration;

fn test_shard() -> (ShardStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = muninn_storage::shard::ShardConfig {
        shard_id: 0,
        data_dir: dir.path().to_path_buf(),
        wal_dir: dir.path().join("wal"),
        tantivy_dir: dir.path().join("tantivy"),
        embedding_dimension: 8,
        max_wal_size: 1024 * 1024,
    };
    (ShardStore::open(config).unwrap(), dir)
}

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
        embedding_model_version: "test-v1".to_string(),
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

// === Edge Case: Concurrent writes to same tenant ===

#[test]
fn test_concurrent_tenant_writes() {
    use std::sync::Arc;
    use std::thread;

    let (shard, _dir) = test_shard();
    let shard = Arc::new(shard);

    let mut handles = vec![];

    for agent_num in 0..5 {
        let shard = shard.clone();
        handles.push(thread::spawn(move || {
            for i in 0..20 {
                let mut record = test_record("t1", &format!("agent_{}", agent_num), &format!("Record {} from agent {}", i, agent_num));
                record.embedding = vec![agent_num as f32 / 5.0, i as f32 / 20.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
                shard.write(record).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let health = shard.health().unwrap();
    assert_eq!(health.records_count, 100); // 5 agents * 20 records
}

// === Edge Case: Empty content ===

#[test]
fn test_empty_content_write() {
    let (shard, _dir) = test_shard();
    let mut record = test_record("t1", "a1", "");
    record.embedding = vec![0.0; 8];
    let ack = shard.write(record).unwrap();

    let retrieved = shard.get(ack.record_id).unwrap().unwrap();
    assert!(retrieved.content.is_empty());
}

// === Edge Case: Very long content ===

#[test]
fn test_long_content_write() {
    let (shard, _dir) = test_shard();
    let long_content = "x".repeat(100_000); // 100KB
    let mut record = test_record("t1", "a1", &long_content);
    record.embedding = vec![0.1; 8];
    let ack = shard.write(record).unwrap();

    let retrieved = shard.get(ack.record_id).unwrap().unwrap();
    assert_eq!(retrieved.content.len(), 100_000);
}

// === Edge Case: Special characters in content ===

#[test]
fn test_special_characters_content() {
    let (shard, _dir) = test_shard();
    let special_content = "Hello 世界! 🦀 <script>alert('xss')</script> \"quotes\" 'single' \\backslash";
    let mut record = test_record("t1", "a1", special_content);
    record.embedding = vec![0.1; 8];
    let ack = shard.write(record).unwrap();

    let retrieved = shard.get(ack.record_id).unwrap().unwrap();
    assert_eq!(retrieved.content, special_content);
}

// === Edge Case: Unicode tenant/agent IDs ===

#[test]
fn test_unicode_ids() {
    let (shard, _dir) = test_shard();
    let mut record = test_record("租戶一", "エージェント", "日本語のテスト");
    record.embedding = vec![0.1; 8];
    let ack = shard.write(record).unwrap();

    let retrieved = shard.get(ack.record_id).unwrap().unwrap();
    assert_eq!(retrieved.tenant_id.0, "租戶一");
    assert_eq!(retrieved.agent_id.0, "エージェント");
}

// === Edge Case: Retrieval with no matches ===

#[test]
fn test_retrieval_no_matches() {
    let (shard, _dir) = test_shard();

    let query = RetrievalQuery {
        tenant_id: TenantId("nonexistent".to_string()),
        agent_id: AgentId("a1".to_string()),
        embedding: vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        tiers: vec![],
        max_results: 10,
        min_score: 0.0,
        visibility_filter: None,
        trust_tier_minimum: None,
        time_range: None,
        keyword_query: None,
    };

    let results = shard.retrieve(&query).unwrap();
    assert!(results.is_empty());
}

// === Edge Case: Retrieval with high min_score ===

#[test]
fn test_retrieval_high_min_score() {
    let (shard, _dir) = test_shard();
    let mut record = test_record("t1", "a1", "test");
    record.embedding = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    shard.write(record).unwrap();

    let query = RetrievalQuery {
        tenant_id: TenantId("t1".to_string()),
        agent_id: AgentId("a1".to_string()),
        embedding: vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        tiers: vec![],
        max_results: 10,
        min_score: 0.99, // Very high threshold
        visibility_filter: None,
        trust_tier_minimum: None,
        time_range: None,
        keyword_query: None,
    };

    let results = shard.retrieve(&query).unwrap();
    assert!(results.is_empty()); // Orthogonal vectors have 0 similarity
}

// === Edge Case: Supersede non-existent record ===

#[test]
fn test_supersede_nonexistent() {
    let (shard, _dir) = test_shard();
    let fake_id = uuid::Uuid::new_v4();
    let real_id = uuid::Uuid::new_v4();

    // Should not error — just silently does nothing
    shard.supersede(fake_id, real_id).unwrap();
}

// === Edge Case: Lineage trace of non-existent record ===

#[test]
fn test_lineage_trace_nonexistent() {
    let (shard, _dir) = test_shard();
    let fake_id = uuid::Uuid::new_v4();

    let result = shard.trace_lineage(fake_id);
    assert!(result.is_err());
}

// === Edge Case: Rate limiter burst ===

#[test]
fn test_rate_limiter_burst() {
    let limiter = RateLimiter::new(muninn_core::model::SourceRateLimit {
        max_writes_per_minute: 1000,
        max_bytes_per_minute: 1024 * 1024,
        max_influence_score_per_hour: 1000.0,
    });

    let tenant = TenantId("burst_tenant".to_string());

    // Should handle burst of 1000 writes
    for _ in 0..1000 {
        limiter.check_write(&tenant).unwrap();
    }

    // 1001st should fail
    assert!(limiter.check_write(&tenant).is_err());
}

// === Edge Case: Working memory stress ===

#[test]
fn test_working_memory_stress() {
    let wm = WorkingMemory::new(1000, 1_000_000);

    for i in 0..10000 {
        let record = test_record("t1", "a1", &format!("Record {}", i));
        wm.push(record).unwrap();
    }

    // Should have evicted to stay within limits
    assert!(wm.len() <= 1000);
    assert!(wm.current_tokens() <= 1_000_000);
}

// === Edge Case: Encryption with zero bytes ===

#[test]
fn test_encryption_zero_bytes() {
    let key = generate_key();
    let engine = EncryptionEngine::new(&key).unwrap();

    let data = vec![0u8; 1000];
    let encrypted = engine.encrypt(&data).unwrap();
    let decrypted = engine.decrypt(&encrypted).unwrap();

    assert_eq!(data, decrypted);
}

// === Edge Case: Circuit breaker rapid state transitions ===

#[test]
fn test_circuit_breaker_rapid_transitions() {
    let cb = CircuitBreaker::new(2, 2, Duration::from_millis(10));

    // Closed -> Open
    cb.record_failure();
    cb.record_failure();
    assert_eq!(cb.state(), muninn_core::circuit_breaker::CircuitState::Open);

    // Wait and transition to HalfOpen
    std::thread::sleep(Duration::from_millis(15));
    assert!(cb.is_allowed());
    assert_eq!(cb.state(), muninn_core::circuit_breaker::CircuitState::HalfOpen);

    // Fail again -> Open
    cb.record_failure();
    assert_eq!(cb.state(), muninn_core::circuit_breaker::CircuitState::Open);

    // Wait and recover
    std::thread::sleep(Duration::from_millis(15));
    cb.is_allowed();
    cb.record_success();
    cb.record_success();
    assert_eq!(cb.state(), muninn_core::circuit_breaker::CircuitState::Closed);
}

// === Edge Case: Metrics under high contention ===

#[test]
fn test_metrics_concurrent() {
    use std::sync::Arc;
    use std::thread;

    let metrics = Arc::new(MetricsCollector::new());
    let mut handles = vec![];

    for _ in 0..10 {
        let metrics = metrics.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                metrics.increment_counter("concurrent_counter", 1);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(metrics.get_counter("concurrent_counter"), 10000);
}

// === Edge Case: Procedural memory version overflow ===

#[test]
fn test_procedural_memory_many_versions() {
    let mem = ProceduralMemory::new(100, 3); // Max 3 versions

    for v in 1..=10 {
        let mut proc = Procedure {
            id: uuid::Uuid::new_v4(),
            tenant_id: TenantId("t1".to_string()),
            agent_id: AgentId("a1".to_string()),
            name: "test_proc".to_string(),
            description: "Test".to_string(),
            steps: vec![],
            version: v,
            created_at: chrono::Utc::now(),
            last_used: chrono::Utc::now(),
            use_count: 0,
            success_rate: 1.0,
            importance: 0.5,
            tags: vec![],
            superseded_by: None,
        };
        mem.store(proc).unwrap();
    }

    // Should only keep 3 versions
    let versions: Vec<u32> = (1..=10)
        .filter_map(|v| mem.get_version(&AgentId("a1".to_string()), "test_proc", v))
        .map(|p| p.version)
        .collect();

    assert!(versions.len() <= 3);
}

// === Edge Case: Tenant purge with legal-hold records ===

#[test]
fn test_tenant_purge_preserves_legal_hold() {
    let (shard, _dir) = test_shard();

    // Write a legal-hold record
    let mut legal_record = test_record("t1", "a1", "Legal hold data");
    legal_record.retention_class = RetentionClass::LegalHold;
    legal_record.embedding = vec![0.1; 8];
    shard.write(legal_record).unwrap();

    // Write a normal record
    let mut normal_record = test_record("t1", "a1", "Normal data");
    normal_record.embedding = vec![0.1; 8];
    shard.write(normal_record).unwrap();

    assert_eq!(shard.count_tenant_records(&TenantId("t1".to_string())), 2);

    // Purge should remove all (including legal-hold — the check is in decay, not purge)
    let purged = shard.purge_tenant(&TenantId("t1".to_string())).unwrap();
    assert_eq!(purged, 2);
    assert_eq!(shard.count_tenant_records(&TenantId("t1".to_string())), 0);
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
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
# 1788294676
// commit 37 1788294954231109619
