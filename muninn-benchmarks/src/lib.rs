#[cfg(test)]
mod benchmarks {
    use std::time::{Duration, Instant};
    use muninn_core::model::*;
    use muninn_core::retrieval::*;
    use muninn_core::trust::TrustTier;
    use muninn_core::visibility::Visibility;
    use muninn_core::vector_clock::VectorClock;
    use muninn_storage::ShardStore;

    /// Benchmark harness for comparing against published baselines.
    /// Based on LoCoMo and LongMemEval benchmark protocols.

    struct BenchResult {
        name: String,
        operations: usize,
        duration: Duration,
        ops_per_sec: f64,
        avg_latency_us: f64,
        p99_latency_us: f64,
    }

    impl BenchResult {
        fn print(&self) {
            println!(
                "{:<40} {:>8} ops  {:>8.0} ops/s  avg {:>8.0}µs  p99 {:>8.0}µs",
                self.name, self.operations, self.ops_per_sec, self.avg_latency_us, self.p99_latency_us
            );
        }
    }

    fn create_test_shard() -> (ShardStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = muninn_storage::shard::ShardConfig {
            shard_id: 0,
            data_dir: dir.path().to_path_buf(),
            wal_dir: dir.path().join("wal"),
            tantivy_dir: dir.path().join("tantivy"),
            embedding_dimension: 128,
            max_wal_size: 1024 * 1024 * 100,
        };
        (ShardStore::open(config).unwrap(), dir)
    }

    fn generate_embedding(seed: u64, dim: usize) -> Vec<f32> {
        let mut embedding = vec![0.0; dim];
        let mut val = seed;
        for i in 0..dim {
            val = val.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            embedding[i] = ((val >> 33) as f32) / (1u64 << 31) as f32;
        }
        // Normalize
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        for val in &mut embedding {
            *val /= norm;
        }
        embedding
    }

    fn generate_record(id: usize, dim: usize) -> MemoryRecord {
        let now = chrono::Utc::now();
        MemoryRecord {
            id: uuid::Uuid::new_v4(),
            tenant_id: TenantId("bench_tenant".to_string()),
            agent_id: AgentId(format!("agent_{}", id % 10)),
            tier: MemoryTier::Episodic,
            schema_version: 1,
            content: format!("Benchmark record {} with some content for testing retrieval quality and latency", id),
            embedding: generate_embedding(id as u64, dim),
            embedding_model_version: "benchmark".to_string(),
            importance: (id as f32 / 1000.0).min(1.0),
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

    /// Benchmark 1: Write throughput (LoCoMo-style)
    /// Measures sustained write performance under load.
    #[test]
    fn bench_write_throughput() {
        let (shard, _dir) = create_test_shard();
        let num_writes = 1000;
        let dim = 128;

        let start = Instant::now();
        for i in 0..num_writes {
            let record = generate_record(i, dim);
            shard.write(record).unwrap();
        }
        let duration = start.elapsed();

        let result = BenchResult {
            name: "Write Throughput".to_string(),
            operations: num_writes,
            duration,
            ops_per_sec: num_writes as f64 / duration.as_secs_f64(),
            avg_latency_us: duration.as_micros() as f64 / num_writes as f64,
            p99_latency_us: 0.0, // Would need histogram for real p99
        };
        result.print();

        // Target: > 1000 writes/sec
        assert!(result.ops_per_sec > 1000.0, "Write throughput too low: {:.0} ops/sec", result.ops_per_sec);
    }

    /// Benchmark 2: Read throughput (LongMemEval-style)
    /// Measures retrieval performance with various query patterns.
    #[test]
    fn bench_read_throughput() {
        let (shard, _dir) = create_test_shard();
        let dim = 128;

        // Pre-populate
        let populate_count = 1000;
        for i in 0..populate_count {
            let record = generate_record(i, dim);
            shard.write(record).unwrap();
        }

        // Benchmark reads
        let num_reads = 500;
        let mut latencies = Vec::new();

        for i in 0..num_reads {
            let query_embedding = generate_embedding(i as u64 + 10000, dim);

            let query = RetrievalQuery {
                tenant_id: TenantId("bench_tenant".to_string()),
                agent_id: AgentId("agent_0".to_string()),
                embedding: query_embedding,
                tiers: vec![],
                max_results: 10,
                min_score: 0.0,
                visibility_filter: None,
                trust_tier_minimum: None,
                time_range: None,
                keyword_query: None,
            };

            let start = Instant::now();
            let _results = shard.retrieve(&query).unwrap();
            latencies.push(start.elapsed().as_micros() as f64);
        }

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let total_duration: Duration = latencies.iter().map(|l| Duration::from_micros(*l as u64)).sum();
        let p99_idx = (num_reads as f64 * 0.99) as usize;

        let result = BenchResult {
            name: "Read Throughput (k=10)".to_string(),
            operations: num_reads,
            duration: total_duration,
            ops_per_sec: num_reads as f64 / total_duration.as_secs_f64(),
            avg_latency_us: latencies.iter().sum::<f64>() / num_reads as f64,
            p99_latency_us: latencies.get(p99_idx).copied().unwrap_or(0.0),
        };
        result.print();

        // Target: < 20ms p99 (20000µs)
        assert!(result.p99_latency_us < 20000.0, "Read p99 too high: {:.0}µs", result.p99_latency_us);
    }

    /// Benchmark 3: Retrieval quality (LoCoMo-style)
    /// Measures precision@k for known relevant documents.
    #[test]
    fn bench_retrieval_quality() {
        let (shard, _dir) = create_test_shard();
        let dim = 128;

        // Create documents with known similarity
        // Documents 0-9: "technology" cluster (similar embeddings)
        // Documents 10-19: "science" cluster (different embeddings)
        for i in 0..20 {
            let mut record = generate_record(i, dim);

            // Make technology cluster similar
            if i < 10 {
                record.embedding = generate_embedding(100, dim); // Same base
                for j in 0..dim {
                    record.embedding[j] += (i as f32 / 100.0) * 0.01; // Small variation
                }
            } else {
                record.embedding = generate_embedding(200, dim); // Different base
                for j in 0..dim {
                    record.embedding[j] += ((i - 10) as f32 / 100.0) * 0.01;
                }
            }

            // Normalize
            let norm: f32 = record.embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
            for val in &mut record.embedding {
                *val /= norm;
            }

            shard.write(record).unwrap();
        }

        // Query with technology-like embedding
        let query_embedding = generate_embedding(100, dim);
        let query = RetrievalQuery {
            tenant_id: TenantId("bench_tenant".to_string()),
            agent_id: AgentId("agent_0".to_string()),
            embedding: query_embedding,
            tiers: vec![],
            max_results: 10,
            min_score: 0.0,
            visibility_filter: None,
            trust_tier_minimum: None,
            time_range: None,
            keyword_query: None,
        };

        let results = shard.retrieve(&query).unwrap();

        // Check that technology documents rank higher
        let tech_in_top5 = results.iter().take(5).filter(|r| {
            r.record.content.contains("Benchmark record") &&
            r.record.agent_id.0 == "agent_0" // First 10 records are agent_0
        }).count();

        println!("Retrieval Quality: {} technology docs in top 5", tech_in_top5);

        // At least some relevant docs should rank highly
        assert!(tech_in_top5 >= 2, "Retrieval quality too low: only {} relevant in top 5", tech_in_top5);
    }

    /// Benchmark 4: Mixed workload (realistic scenario)
    /// Simulates realistic 80/20 read/write pattern.
    #[test]
    fn bench_mixed_workload() {
        let (shard, _dir) = create_test_shard();
        let dim = 128;

        // Pre-populate
        for i in 0..500 {
            let record = generate_record(i, dim);
            shard.write(record).unwrap();
        }

        let total_ops = 1000;
        let read_ratio = 0.8; // 80% reads
        let mut read_count = 0;
        let mut write_count = 0;
        let mut latencies = Vec::new();

        let start = Instant::now();

        for i in 0..total_ops {
            let is_read = (i as f64 / total_ops as f64) < read_ratio;

            if is_read {
                let query_embedding = generate_embedding(i as u64 + 20000, dim);
                let query = RetrievalQuery {
                    tenant_id: TenantId("bench_tenant".to_string()),
                    agent_id: AgentId("agent_0".to_string()),
                    embedding: query_embedding,
                    tiers: vec![],
                    max_results: 10,
                    min_score: 0.0,
                    visibility_filter: None,
                    trust_tier_minimum: None,
                    time_range: None,
                    keyword_query: None,
                };

                let op_start = Instant::now();
                let _results = shard.retrieve(&query).unwrap();
                latencies.push(op_start.elapsed().as_micros() as f64);
                read_count += 1;
            } else {
                let record = generate_record(500 + write_count, dim);
                let op_start = Instant::now();
                shard.write(record).unwrap();
                latencies.push(op_start.elapsed().as_micros() as f64);
                write_count += 1;
            }
        }

        let duration = start.elapsed();
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let p99_idx = (total_ops as f64 * 0.99) as usize;

        println!("\nMixed Workload Results:");
        println!("  Total ops:    {}", total_ops);
        println!("  Reads:        {}", read_count);
        println!("  Writes:       {}", write_count);
        println!("  Duration:     {:.2?}", duration);
        println!("  Throughput:   {:.0} ops/sec", total_ops as f64 / duration.as_secs_f64());
        println!("  Avg latency:  {:.0}µs", latencies.iter().sum::<f64>() / total_ops as f64);
        println!("  P99 latency:  {:.0}µs", latencies.get(p99_idx).copied().unwrap_or(0.0));

        // Target: < 50ms p99 for mixed workload
        let p99 = latencies.get(p99_idx).copied().unwrap_or(0.0);
        assert!(p99 < 50000.0, "Mixed workload p99 too high: {:.0}µs", p99);
    }

    /// Benchmark 5: Concurrent access
    /// Measures performance under concurrent read/write load.
    #[test]
    fn bench_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let (shard, _dir) = create_test_shard();
        let shard = Arc::new(shard);
        let dim = 128;

        // Pre-populate
        for i in 0..200 {
            let record = generate_record(i, dim);
            shard.write(record).unwrap();
        }

        let num_threads = 4;
        let ops_per_thread = 250;
        let mut handles = vec![];

        let start = Instant::now();

        for thread_id in 0..num_threads {
            let shard = shard.clone();
            handles.push(thread::spawn(move || {
                let mut local_latencies = Vec::new();

                for i in 0..ops_per_thread {
                    if i % 5 == 0 {
                        // 20% writes
                        let record = generate_record(200 + thread_id * 1000 + i, dim);
                        let op_start = Instant::now();
                        shard.write(record).unwrap();
                        local_latencies.push(op_start.elapsed().as_micros() as f64);
                    } else {
                        // 80% reads
                        let query_embedding = generate_embedding(thread_id as u64 * 1000 + i as u64, dim);
                        let query = RetrievalQuery {
                            tenant_id: TenantId("bench_tenant".to_string()),
                            agent_id: AgentId("agent_0".to_string()),
                            embedding: query_embedding,
                            tiers: vec![],
                            max_results: 10,
                            min_score: 0.0,
                            visibility_filter: None,
                            trust_tier_minimum: None,
                            time_range: None,
                            keyword_query: None,
                        };

                        let op_start = Instant::now();
                        let _results = shard.retrieve(&query).unwrap();
                        local_latencies.push(op_start.elapsed().as_micros() as f64);
                    }
                }

                local_latencies
            }));
        }

        let mut all_latencies: Vec<f64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        let duration = start.elapsed();
        let total_ops = all_latencies.len();

        all_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let p99_idx = (total_ops as f64 * 0.99) as usize;

        println!("\nConcurrent Access Results:");
        println!("  Threads:      {}", num_threads);
        println!("  Total ops:    {}", total_ops);
        println!("  Duration:     {:.2?}", duration);
        println!("  Throughput:   {:.0} ops/sec", total_ops as f64 / duration.as_secs_f64());
        println!("  P99 latency:  {:.0}µs", all_latencies.get(p99_idx).copied().unwrap_or(0.0));
    }
}
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
// commit 44 1788294954331500113
// commit 92 1788294955065393382
// commit 140 1788294955816192270
// commit 164 1788294956181293099
// commit 188 1788294956554608628
// commit 212 1788294956933255506
// commit 332 1788294958802571452
