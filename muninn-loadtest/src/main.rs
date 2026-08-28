use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parking_lot::Mutex;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use hdrhistogram::Histogram;

use muninn_core::model::*;
use muninn_core::retrieval::*;
use muninn_core::trust::TrustTier;
use muninn_core::visibility::Visibility;
use muninn_core::vector_clock::VectorClock;
use muninn_storage::ShardStore;

#[derive(Parser, Debug)]
#[command(name = "muninn-loadtest")]
#[command(about = "Load testing harness for Muninn memory system")]
struct Args {
    /// Number of concurrent workers
    #[arg(short, long, default_value_t = 8)]
    workers: usize,

    /// Total number of operations per worker
    #[arg(short, long, default_value_t = 1000)]
    ops_per_worker: usize,

    /// Read:write ratio (e.g., 5 means 5 reads per write)
    #[arg(short, long, default_value_t = 5.0)]
    read_write_ratio: f64,

    /// Number of tenants
    #[arg(long, default_value_t = 10)]
    tenants: usize,

    /// Number of agents per tenant
    #[arg(long, default_value_t = 5)]
    agents_per_tenant: usize,

    /// Embedding dimension
    #[arg(long, default_value_t = 128)]
    embedding_dim: usize,

    /// Test duration in seconds (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    duration: u64,

    /// Print detailed results
    #[arg(long)]
    verbose: bool,
}

struct LoadTestResults {
    writes_completed: AtomicU64,
    writes_failed: AtomicU64,
    reads_completed: AtomicU64,
    reads_failed: AtomicU64,
    write_latencies: Mutex<Histogram<u64>>,
    read_latencies: Mutex<Histogram<u64>>,
}

impl LoadTestResults {
    fn new() -> Self {
        Self {
            writes_completed: AtomicU64::new(0),
            writes_failed: AtomicU64::new(0),
            reads_completed: AtomicU64::new(0),
            reads_failed: AtomicU64::new(0),
            write_latencies: Mutex::new(Histogram::new(3).unwrap()),
            read_latencies: Mutex::new(Histogram::new(3).unwrap()),
        }
    }
}

fn test_record(tenant_id: &str, agent_id: &str, content: &str, embedding_dim: usize) -> MemoryRecord {
    let now = chrono::Utc::now();
    let mut embedding = vec![0.0; embedding_dim];
    let mut rng = rand::thread_rng();
    for val in &mut embedding {
        *val = rng.gen_range(-1.0..1.0);
    }
    // Normalize
    let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    for val in &mut embedding {
        *val /= norm;
    }

    MemoryRecord {
        id: uuid::Uuid::new_v4(),
        tenant_id: TenantId(tenant_id.to_string()),
        agent_id: AgentId(agent_id.to_string()),
        tier: MemoryTier::Episodic,
        schema_version: 1,
        content: content.to_string(),
        embedding,
        embedding_model_version: "loadtest".to_string(),
        importance: rng.gen_range(0.1..1.0),
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

fn run_load_test(args: &Args) {
    println!("Muninn Load Test");
    println!("================");
    println!("Workers:        {}", args.workers);
    println!("Ops/worker:     {}", args.ops_per_worker);
    println!("R:W ratio:      {:.1}", args.read_write_ratio);
    println!("Tenants:        {}", args.tenants);
    println!("Agents/tenant:  {}", args.agents_per_tenant);
    println!("Embedding dim:  {}", args.embedding_dim);
    println!();

    // Create shard
    let dir = tempfile::tempdir().unwrap();
    let config = muninn_storage::shard::ShardConfig {
        shard_id: 0,
        data_dir: dir.path().to_path_buf(),
        wal_dir: dir.path().join("wal"),
        tantivy_dir: dir.path().join("tantivy"),
        embedding_dimension: args.embedding_dim,
        max_wal_size: 1024 * 1024 * 100, // 100MB
    };
    let shard = Arc::new(ShardStore::open(config).unwrap());

    // Pre-populate with some records for reads
    println!("Pre-populating...");
    let pre_populate = 1000;
    for i in 0..pre_populate {
        let tenant_id = format!("tenant_{}", i % args.tenants);
        let agent_id = format!("agent_{}", i % args.agents_per_tenant);
        let record = test_record(&tenant_id, &agent_id, &format!("Pre-populated record {}", i), args.embedding_dim);
        shard.write(record).unwrap();
    }
    println!("Pre-populated {} records", pre_populate);
    println!();

    // Setup progress bar
    let total_ops = args.workers * args.ops_per_worker;
    let pb = ProgressBar::new(total_ops as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    let results = Arc::new(LoadTestResults::new());
    let start = Instant::now();
    let stop_flag = Arc::new(AtomicU64::new(0));

    // Spawn workers
    let mut handles = vec![];
    for worker_id in 0..args.workers {
        let shard = shard.clone();
        let results = results.clone();
        let pb = pb.clone();
        let stop_flag = stop_flag.clone();
        let tenants = args.tenants;
        let agents_per_tenant = args.agents_per_tenant;
        let embedding_dim = args.embedding_dim;
        let ops_per_worker = args.ops_per_worker;
        let read_write_ratio = args.read_write_ratio;
        let duration = args.duration;

        handles.push(std::thread::spawn(move || {
            let mut rng = rand::thread_rng();

            for op in 0..ops_per_worker {
                // Check stop conditions
                if stop_flag.load(Ordering::Relaxed) != 0 {
                    break;
                }
                if duration > 0 && start.elapsed() >= Duration::from_secs(duration) {
                    break;
                }

                let tenant_idx = rng.gen_range(0..tenants);
                let agent_idx = rng.gen_range(0..agents_per_tenant);
                let tenant_id = format!("tenant_{}", tenant_idx);
                let agent_id = format!("agent_{}", agent_idx);

                // Decide read vs write
                let is_read = rng.gen::<f64>() < read_write_ratio / (read_write_ratio + 1.0);

                if is_read {
                    // Read operation
                    let query_embedding: Vec<f32> = (0..embedding_dim)
                        .map(|_| rng.gen_range(-1.0..1.0))
                        .collect();
                    let norm: f32 = query_embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
                    let query_embedding: Vec<f32> = query_embedding.iter().map(|x| x / norm).collect();

                    let query = RetrievalQuery {
                        tenant_id: TenantId(tenant_id),
                        agent_id: AgentId(agent_id),
                        embedding: query_embedding,
                        tiers: vec![],
                        max_results: 10,
                        min_score: 0.0,
                        visibility_filter: None,
                        trust_tier_minimum: None,
                        time_range: None,
                        keyword_query: None,
                    };

                    let start_latency = Instant::now();
                    match shard.retrieve(&query) {
                        Ok(_) => {
                            let latency_us = start_latency.elapsed().as_micros() as u64;
                            results.read_latencies.lock().record(latency_us).unwrap();
                            results.reads_completed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            results.reads_failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                } else {
                    // Write operation
                    let content = format!("Load test record {} from worker {} op {}", op, worker_id, op);
                    let record = test_record(&tenant_id, &agent_id, &content, embedding_dim);

                    let start_latency = Instant::now();
                    match shard.write(record) {
                        Ok(_) => {
                            let latency_us = start_latency.elapsed().as_micros() as u64;
                            results.write_latencies.lock().record(latency_us).unwrap();
                            results.writes_completed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            results.writes_failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                pb.inc(1);
            }
        }));
    }

    // Wait for all workers
    for handle in handles {
        handle.join().unwrap();
    }

    pb.finish_with_message("done");
    let total_duration = start.elapsed();

    // Print results
    println!();
    println!("Results");
    println!("=======");
    println!("Duration:       {:.2}s", total_duration.as_secs_f64());

    let writes = results.writes_completed.load(Ordering::Relaxed);
    let write_fails = results.writes_failed.load(Ordering::Relaxed);
    let reads = results.reads_completed.load(Ordering::Relaxed);
    let read_fails = results.reads_failed.load(Ordering::Relaxed);

    println!("Writes:         {} completed, {} failed", writes, write_fails);
    println!("Reads:          {} completed, {} failed", reads, read_fails);
    println!("Total ops:      {}", writes + reads);
    println!("Throughput:     {:.0} ops/sec", (writes + reads) as f64 / total_duration.as_secs_f64());
    println!();

    // Latency percentiles
    if writes > 0 {
        let wl = results.write_latencies.lock();
        println!("Write Latency (µs)");
        println!("  p50:          {}", wl.value_at_quantile(0.50));
        println!("  p90:          {}", wl.value_at_quantile(0.90));
        println!("  p99:          {}", wl.value_at_quantile(0.99));
        println!("  p999:         {}", wl.value_at_quantile(0.999));
        println!("  max:          {}", wl.max());
        println!();
    }

    if reads > 0 {
        let rl = results.read_latencies.lock();
        println!("Read Latency (µs)");
        println!("  p50:          {}", rl.value_at_quantile(0.50));
        println!("  p90:          {}", rl.value_at_quantile(0.90));
        println!("  p99:          {}", rl.value_at_quantile(0.99));
        println!("  p999:         {}", rl.value_at_quantile(0.999));
        println!("  max:          {}", rl.max());
        println!();
    }

    // SLO check
    let read_p99 = if reads > 0 { results.read_latencies.lock().value_at_quantile(0.99) } else { 0 };
    let write_p99 = if writes > 0 { results.write_latencies.lock().value_at_quantile(0.99) } else { 0 };

    println!("SLO Check");
    println!("=========");
    println!("Cold recall < 20ms p99:    {} (actual: {}µs)", if read_p99 < 20000 { "PASS ✅" } else { "FAIL ❌" }, read_p99);
    println!("Write ack < 1ms p99:       {} (actual: {}µs)", if write_p99 < 1000 { "PASS ✅" } else { "FAIL ❌" }, write_p99);

    if args.verbose {
        println!();
        println!("Health");
        println!("======");
        let health = shard.health().unwrap();
        println!("Records:        {}", health.records_count);
        println!("WAL bytes:      {}", health.wal_size_bytes);
    }
}

fn main() {
    let args = Args::parse();
    run_load_test(&args);
}
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
// commit 20 1788294953985684566
// commit 260 1788294957683888509
// commit 284 1788294958065486543
// commit 308 1788294958420982610
// commit 356 1788294959178645286
