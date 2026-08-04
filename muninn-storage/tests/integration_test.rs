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
use muninn_storage::ShardStore;
use std::time::Duration;

fn test_tenant(id: &str) -> TenantId {
    TenantId(id.to_string())
}

fn test_agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

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

fn test_record(tenant: &str, agent: &str, content: &str, importance: f32) -> MemoryRecord {
    let now = chrono::Utc::now();
    MemoryRecord {
        id: uuid::Uuid::new_v4(),
        tenant_id: test_tenant(tenant),
        agent_id: test_agent(agent),
        tier: MemoryTier::Episodic,
        schema_version: 1,
        content: content.to_string(),
        embedding: vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
        embedding_model_version: "test-v1".to_string(),
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
fn test_shard_write_and_retrieve() {
    let (shard, _dir) = test_shard();
    let record = test_record("t1", "a1", "The deadline is March 15", 0.8);

    let ack = shard.write(record).unwrap();
    assert!(!ack.record_id.is_nil());

    let retrieved = shard.get(ack.record_id).unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().content, "The deadline is March 15");
}

#[test]
fn test_shard_tenant_isolation() {
    let (shard, _dir) = test_shard();

    // Write to tenant t1
    let mut r1 = test_record("t1", "a1", "Secret for t1", 0.5);
    r1.embedding = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    shard.write(r1).unwrap();

    // Write to tenant t2
    let mut r2 = test_record("t2", "a2", "Secret for t2", 0.5);
    r2.embedding = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    shard.write(r2).unwrap();

    // Retrieve from t1 should only return t1 records
    let query = RetrievalQuery {
        tenant_id: test_tenant("t1"),
        agent_id: test_agent("a1"),
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
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].record.content, "Secret for t1");
}

#[test]
fn test_shard_supersede() {
    let (shard, _dir) = test_shard();
    let mut record = test_record("t1", "a1", "Old fact", 0.5);
    record.embedding = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    let ack = shard.write(record).unwrap();

    let new_record = test_record("t1", "a1", "New fact", 0.8);
    let new_ack = shard.write(new_record).unwrap();

    shard.supersede(ack.record_id, new_ack.record_id).unwrap();

    let old = shard.get(ack.record_id).unwrap().unwrap();
    assert!(old.superseded_by.is_some());
    assert_eq!(old.superseded_by.unwrap(), new_ack.record_id);
}

#[test]
fn test_shard_lineage_trace() {
    let (shard, _dir) = test_shard();

    // Create source episode
    let mut source = test_record("t1", "a1", "Source episode", 0.5);
    source.embedding = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    let source_ack = shard.write(source).unwrap();

    // Create derived fact
    let mut derived = test_record("t1", "a1", "Derived fact", 0.7);
    derived.embedding = vec![0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    derived.source_ids = vec![source_ack.record_id];
    let _derived_ack = shard.write(derived).unwrap();

    // Trace lineage from source
    let lineage = shard.trace_lineage(source_ack.record_id).unwrap();
    assert_eq!(lineage.root_id, source_ack.record_id);
    assert!(lineage.total_downstream_facts >= 1);
}

#[test]
fn test_shard_bulk_write() {
    let (shard, _dir) = test_shard();
    let records: Vec<MemoryRecord> = (0..100)
        .map(|i| {
            let mut r = test_record("t1", "a1", &format!("Record {}", i), 0.5);
            r.embedding = vec![i as f32 / 100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
            r
        })
        .collect();

    let acks = shard.write_batch(records).unwrap();
    assert_eq!(acks.len(), 100);

    let health = shard.health().unwrap();
    assert_eq!(health.records_count, 100);
}

#[test]
fn test_shard_tenant_purge() {
    let (shard, _dir) = test_shard();

    // Write records for two tenants
    for i in 0..5 {
        let mut r = test_record("t1", "a1", &format!("T1 Record {}", i), 0.5);
        r.embedding = vec![0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        shard.write(r).unwrap();
    }
    for i in 0..3 {
        let mut r = test_record("t2", "a2", &format!("T2 Record {}", i), 0.5);
        r.embedding = vec![0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        shard.write(r).unwrap();
    }

    assert_eq!(shard.count_tenant_records(&test_tenant("t1")), 5);
    assert_eq!(shard.count_tenant_records(&test_tenant("t2")), 3);

    // Purge t1
    let purged = shard.purge_tenant(&test_tenant("t1")).unwrap();
    assert_eq!(purged, 5);

    assert_eq!(shard.count_tenant_records(&test_tenant("t1")), 0);
    assert_eq!(shard.count_tenant_records(&test_tenant("t2")), 3);
}

#[test]
fn test_working_memory_budget() {
    let wm = WorkingMemory::new(5, 100);

    // Fill to capacity
    for i in 0..10 {
        let record = test_record("t1", "a1", &format!("Record {}", i), i as f32 / 10.0);
        wm.push(record).unwrap();
    }

    // Should have evicted some entries
    assert!(wm.len() <= 5);
    assert!(wm.current_tokens() <= 100);
}

#[test]
fn test_encryption_roundtrip() {
    let key = generate_key();
    let engine = EncryptionEngine::new(&key).unwrap();

    let data = b"Sensitive memory data that must be encrypted at rest";
    let encrypted = engine.encrypt(data).unwrap();
    let decrypted = engine.decrypt(&encrypted).unwrap();

    assert_eq!(data.as_slice(), decrypted.as_slice());
    assert_ne!(data.as_slice(), encrypted.ciphertext.as_slice());
}

#[test]
fn test_audit_log() {
    let log = AuditLog::stdout(1000);

    log.log(AuditEvent::Write {
        record_id: uuid::Uuid::new_v4(),
        tenant_id: "t1".to_string(),
        agent_id: "a1".to_string(),
        tier: "episodic".to_string(),
        trust_tier: "standard".to_string(),
        visibility: "private".to_string(),
    });

    log.log(AuditEvent::TenantPurge {
        tenant_id: "t1".to_string(),
        initiated_by: "admin".to_string(),
        records_purged: 42,
    });

    let recent = log.recent();
    assert_eq!(recent.len(), 2);
}

#[test]
fn test_circuit_breaker() {
    let cb = CircuitBreaker::new(3, 2, Duration::from_millis(50));

    // Should be closed initially
    assert!(cb.is_allowed());

    // Record failures
    cb.record_failure();
    cb.record_failure();
    assert!(cb.is_allowed()); // Still closed (2 < 3)
    cb.record_failure();
    assert!(!cb.is_allowed()); // Now open

    // Wait for recovery
    std::thread::sleep(Duration::from_millis(60));
    assert!(cb.is_allowed()); // Half-open
    cb.record_success();
    cb.record_success(); // Should close
    assert!(cb.is_allowed());
}

#[test]
fn test_metrics() {
    let metrics = MetricsCollector::new();

    metrics.increment_counter("requests", 100);
    metrics.increment_counter("requests", 50);
    assert_eq!(metrics.get_counter("requests"), 150);

    metrics.set_gauge("connections", 42);
    assert_eq!(metrics.get_gauge("connections"), 42);

    let prometheus = metrics.export_prometheus();
    assert!(prometheus.contains("muninn_requests 150"));
    assert!(prometheus.contains("muninn_connections 42"));
}

#[test]
fn test_vector_clock_operations() {
    let mut vc1 = VectorClock::new();
    vc1.increment("agent_a");
    vc1.increment("agent_a");

    let mut vc2 = VectorClock::new();
    vc2.increment("agent_a");
    vc2.increment("agent_b");

    // vc1 doesn't dominate vc2 (agent_b: 0 < 1)
    assert!(!vc1.dominates(&vc2));
    // vc2 doesn't dominate vc1 (agent_a: 1 < 2)
    assert!(!vc2.dominates(&vc1));
    // They are concurrent
    assert!(vc1.is_concurrent_with(&vc2));
}

#[test]
fn test_visibility_enforcement() {
    let private = Visibility::Private;
    let shared = Visibility::Shared;
    let shared_with = Visibility::SharedWith(vec![AgentId("bob".to_string())]);

    let alice = AgentId("alice".to_string());
    let bob = AgentId("bob".to_string());

    // Private: only owner
    assert!(private.allows_access(&alice, &alice));
    assert!(!private.allows_access(&alice, &bob));

    // Shared: everyone
    assert!(shared.allows_access(&alice, &bob));
    assert!(shared.allows_access(&bob, &alice));

    // SharedWith: owner + listed agents
    assert!(shared_with.allows_access(&alice, &alice));
    assert!(shared_with.allows_access(&alice, &bob));
    assert!(!shared_with.allows_access(&alice, &AgentId("charlie".to_string())));
}

#[test]
fn test_trust_tier_scoring() {
    assert_eq!(TrustTier::Verified.retrieval_multiplier(), 1.0);
    assert_eq!(TrustTier::Standard.retrieval_multiplier(), 0.9);
    assert_eq!(TrustTier::Untrusted.retrieval_multiplier(), 0.5);

    assert!(TrustTier::Verified.can_auto_promote());
    assert!(TrustTier::Standard.can_auto_promote());
    assert!(!TrustTier::Untrusted.can_auto_promote());
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
// commit 13 1788294953878832619
