# Muninn — Production-Grade Multi-Agent Memory System

> *"Memory for the fleet."* Named after Odin's raven of memory in Norse mythology.

Muninn is a persistent, human-memory-inspired memory system for multi-agent "bot offices": multiple LLM-backed agents collaborating, each with private memory, sharing a common workspace memory.

## Architecture

```
┌──────────────────────────┐
│   API Gateway / Ingress   │  authn, rate limiting, TLS
└─────────────┬────────────┘
              │
┌─────────────▼────────────┐
│      Memory Service       │  stateless frontend, horizontally scalable
└─────────────┬────────────┘
     ┌────────┼────────┐
┌────▼────┐ ┌────▼────┐ ┌────▼────┐
│ Shard 0 │ │ Shard 1 │ │ Shard N │
│ (WAL +  │ │ (WAL +  │ │ (WAL +  │
│  HNSW + │ │  HNSW + │ │  HNSW + │
│ tantivy)│ │ tantivy)│ │ tantivy)│
└─────────┘ └─────────┘ └─────────┘
```

## Key Features

### Memory Tiers
- **Working memory**: In-process, bounded by token/entry budget
- **Episodic memory**: Timestamped raw events
- **Semantic memory**: Distilled facts as knowledge graph
- **Procedural memory**: Versioned, reusable learned routines
- **Shared office memory**: Cross-agent shared context

### Security & Anti-Poisoning
- **Trust tiers**: Verified / Standard / Untrusted — gates promotion
- **Quarantine**: Untrusted content never auto-promotes to semantic/shared memory
- **Lineage tracking**: Full provenance graph for rollback on poisoning
- **Blast-radius limiting**: Rate/volume caps per source
- **Tenant isolation**: Enforced at storage layer, not just API

### Retrieval
Hybrid scoring with full explainability:
```
score = w_relevance * cosine_similarity
      + w_recency    * exp(-decay_rate * age)
      + w_importance * importance_score
      + w_keyword    * bm25_score
```

### Durability
- WAL with fsync before write acknowledgment
- Crash recovery via WAL replay
- Checkpointing for fast restart
- Async replica with WAL shipping

## Quick Start

```bash
# Build
cargo build --release

# Run with default config
cargo run --bin muninn

# Run with custom config
MUNINN_EMBEDDING_API_KEY=your-key cargo run --bin muninn
```

## API

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
curl http://localhost:3000/api/v1/memory/{id}/lineage \
  -H "X-Api-Key: your-api-key"
```

## Configuration

Configuration is layered: defaults → file → environment → secrets manager.

See `muninn.toml` for all options.

Environment variable overrides:
- `MUNINN_GRPC_PORT`
- `MUNINN_REST_PORT`
- `MUNINN_DATA_DIR`
- `MUNINN_EMBEDDING_API_KEY`
- `MUNINN_LOG_LEVEL`

## Deployment

### Docker
```bash
docker build -t muninn .
docker run -p 3000:3000 -p 50051:50051 muninn
```

### Kubernetes
```bash
kubectl apply -f k8s/
```

## Testing

```bash
# Unit tests
cargo test

# With output
cargo test -- --nocapture

# Specific test
cargo test vector_clock::tests
```

## License

MIT
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
// commit 48 1788294954389795775
// commit 360 1788294959236006611
// commit 384 1788294959599320588
