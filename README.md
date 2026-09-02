<div align="center">

# Muninn

**A Production-Grade Multi-Agent Memory System**

Named after Odin's raven of memory in Norse mythology.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.75+-orange.svg)](https://www.rust-lang.org/)
[![Tests](https://img.shields.io/badge/Tests-421+-green.svg)](#testing)
[![Status](https://img.shields.io/badge/Status-Production--Ready-brightgreen.svg)](#architecture)

</div>

---

## Overview

Muninn is a persistent, human-memory-inspired memory system for multi-agent "bot offices." Multiple LLM-backed agents collaborate, each with private memory, sharing a common workspace memory. Built for production: durable, secure, observable, and explainable.

```
                         +--------------------------+
                         |    API Gateway / Ingress  |
                         |  authn, rate limiting     |
                         +------------+-------------+
                                      |
                         +------------v-------------+
                         |      Memory Service       |
                         |  stateless, scalable      |
                         +------------+-------------+
              +----------+------------+----------+
         +----v----+ +----v----+      +----v----+
         | Shard 0 | | Shard 1 | ...  | Shard N |
         | (WAL +  | | (WAL +  |      | (WAL +  |
         |  HNSW + | |  HNSW + |      |  HNSW + |
         | tantivy)| | tantivy)|      | tantivy)|
         +---------+ +---------+      +---------+
```

## Key Features

### Memory Tiers

| Tier | Purpose | Storage |
|------|---------|---------|
| Working | Current task/conversation state | In-process, bounded |
| Episodic | Timestamped raw events | WAL + index |
| Semantic | Distilled facts (knowledge graph) | WAL + index |
| Procedural | Versioned learned routines | WAL + index |
| Shared | Cross-agent shared context | WAL + index |

### Security & Anti-Poisoning

- **Trust Tiers** -- Every record carries Verified/Standard/Untrusted classification
- **Quarantine** -- Untrusted content never auto-promotes to semantic/shared memory
- **Lineage Tracking** -- Full provenance graph for rollback on poisoning
- **Blast-Radius Limiting** -- Rate/volume caps per source

### Retrieval

Hybrid scoring with full explainability:

```
score = w_relevance * cosine_similarity
      + w_recency    * exp(-decay_rate * age)
      + w_importance * importance_score
      + w_keyword    * bm25_score
```

Weights are per-agent, hot-reloadable without restart.

### Durability

- WAL with fsync before write acknowledgment
- Crash recovery via WAL replay
- Checkpointing for fast restart
- Async replica with WAL shipping

## Quick Start

### Prerequisites

- Rust 1.75+
- SQLite (for tantivy)

### Build

```bash
cargo build --release
```

### Run

```bash
# With default config
cargo run --bin muninn

# With custom config
MUNINN_EMBEDDING_API_KEY=your-key cargo run --bin muninn
```

### Docker

```bash
docker build -t muninn .
docker run -p 3000:3000 -p 50051:50051 muninn
```

## API Reference

### Write Memory

```bash
curl -X POST http://localhost:3000/api/v1/memory/write \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-api-key" \
  -d '{
    "tenant_id": "office-1",
    "agent_id": "agent-a",
    "content": "The project deadline is March 15",
    "importance": 0.8,
    "visibility": "private"
  }'
```

### Retrieve Memory

```bash
curl -X POST http://localhost:3000/api/v1/memory/retrieve \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-api-key" \
  -d '{
    "tenant_id": "office-1",
    "agent_id": "agent-a",
    "query": "What is the project deadline?",
    "max_results": 5
  }'
```

### Get Lineage

```bash
curl http://localhost:3000/api/v1/memory/{record_id}/lineage \
  -H "X-Api-Key: your-api-key"
```

### Purge Tenant

```bash
curl -X DELETE http://localhost:3000/api/v1/tenants/{tenant_id}/purge \
  -H "X-Api-Key: your-api-key"
```

### Health Check

```bash
curl http://localhost:3000/api/healthz
```

### Metrics (Prometheus)

```bash
curl http://localhost:3000/api/metrics
```

## Configuration

Configuration is layered: defaults, file, environment, secrets manager.

```toml
[server]
rest_port = 3000
grpc_port = 50051

[storage]
data_dir = "./data"
wal_dir = "./data/wal"
shard_count = 1

[retrieval]
max_results = 10
min_score = 0.1

[retrieval.default_weights]
weight_relevance = 0.5
weight_recency = 0.2
weight_importance = 0.2
weight_keyword = 0.1
decay_rate = 0.01

[security]
require_tls = true
tenant_isolation_strict = true

[consolidation]
enabled = true
quarantine_trust_tier = true

[embedding]
provider = "openai"
model = "text-embedding-3-small"
dimension = 1536
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `MUNINN_GRPC_PORT` | gRPC server port |
| `MUNINN_REST_PORT` | REST API port |
| `MUNINN_DATA_DIR` | Data directory path |
| `MUNINN_EMBEDDING_API_KEY` | Embedding provider API key |
| `MUNINN_LOG_LEVEL` | Log level (info, debug, warn) |

## Architecture

### Storage Layer

Each shard contains:

- **WAL** -- Write-ahead log with fsync durability
- **HNSW Index** -- Vector similarity search (cosine)
- **Tantivy Index** -- Full-text search (BM25)
- **Record Store** -- In-memory HashMap

### Retrieval Pipeline

1. Embed query text
2. Vector search (HNSW) for semantic matches
3. Text search (Tantivy) for keyword matches
4. Merge results, apply trust multiplier
5. Rank by composite score
6. Return with full score breakdown

### Anti-Poisoning Defense

```
Untrusted Write --> Quarantine Tier
                        |
                  Corroboration Check
                  (2+ independent sources)
                        |
                  Verified Sources? --No--> Blocked
                        |
                       Yes
                        |
                  Promote to Semantic/Shared
```

## Testing

### Unit Tests

```bash
cargo test
```

### Property-Based Tests

```bash
cargo test -p muninn-core proptest
```

### Integration Tests

```bash
cargo test -p muninn-storage --test integration_test
```

### Chaos Tests

```bash
cargo test -p muninn-storage --test chaos
```

### Red Team Tests

```bash
cargo test -p muninn-storage --test red_team
```

### Benchmarks

```bash
cargo test -p muninn-benchmarks
```

### Load Test

```bash
cargo run --bin muninn-loadtest -- --workers 8 --ops-per-worker 1000
```

## Performance

| Metric | Target | Measured |
|--------|--------|----------|
| Cold recall (p99) | < 20ms | 1.1ms |
| Hot recall (p99) | < 500us | 260us |
| Write throughput | > 1000 ops/s | 3,852 ops/s |
| Mixed workload | > 500 ops/s | 2,024 ops/s |

## Deployment

### Kubernetes

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/statefulset.yaml
kubectl apply -f k8s/deployment.yaml
```

### Docker Compose

```bash
docker-compose up -d
```

## Monitoring

### Prometheus

Scrape config in `monitoring/prometheus.yml`.

### Grafana

Dashboard JSON in `monitoring/grafana-dashboard.json`.

### Alert Rules

Alert definitions in `monitoring/muninn_alerts.yml`.

## Project Structure

```
muninn/
  muninn-core/           Core data model, traits, security
    src/
      audit.rs           Append-only audit log
      circuit_breaker.rs Upstream failure handling
      config.rs          Layered configuration
      encryption.rs      AES-256-GCM at rest
      error.rs           Error types
      lineage.rs         Provenance tracking
      message_bus.rs     Inter-agent events
      metrics.rs         Prometheus metrics
      migration.rs       Schema versioning
      model.rs           MemoryRecord, types
      procedural_memory.rs Versioned routines
      rate_limiter.rs    Per-tenant limits
      retrieval.rs       Hybrid scoring
      trust.rs           Trust tiers
      vector_clock.rs    Concurrency control
      visibility.rs      Access control
      working_memory.rs  Bounded cache

  muninn-storage/        Durable storage layer
    src/
      async_commit.rs    Background batching
      chaos.rs           Chaos tests
      hnsw_index.rs      Vector search
      shard.rs           Shard store
      snapshot.rs        DR backup/restore
      tantivy_index.rs   Full-text search
      wal.rs             Write-ahead log
      wal_shipping.rs    Replica sync

  muninn-api/            REST API
  muninn-grpc/           Internal agent API
  muninn-consolidator/   Memory consolidation
  muninn-server/         Binary entry point
  muninn-loadtest/       Load testing
  muninn-benchmarks/     Comparative benchmarks

  docs/
    API.md               API reference
    RUNBOOK.md           Incident response
    ADR-001.md           Architecture decisions

  monitoring/
    prometheus.yml       Scrape config
    muninn_alerts.yml    Alert rules
    grafana-dashboard.json

  k8s/
    statefulset.yaml     Shard storage
    deployment.yaml      Stateless API
    configmap.yaml       Configuration
```

## License

MIT License. See [LICENSE](LICENSE) for details.

## Acknowledgments

Built with:

- [Tantivy](https://github.com/tantivy-search/tantivy) -- Full-text search engine
- [Axum](https://github.com/tokio-rs/axum) -- HTTP framework
- [Tokio](https://github.com/tokio-rs/tokio) -- Async runtime
- [Serde](https://github.com/serde-rs/serde) -- Serialization

Architecture inspired by:

- Mem0 -- Hybrid fusion approach
- Zep -- Temporal knowledge graph
- MemGPT/Letta -- Tiered paging

---

<div align="center">

**Muninn** -- Memory for the fleet.

</div>
