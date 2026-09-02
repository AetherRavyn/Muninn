# ADR-001: Core Architecture Decisions

## Status
Accepted

## Context
Muninn is a production-grade multi-agent memory system that must be:
- Durable through crashes
- Secure against cross-tenant/cross-agent leakage
- Resistant to memory poisoning (OWASP ASI06)
- Explainable in what it retrieves and why
- Performant at production scale

## Decisions

### 1. Storage: WAL + HNSW + Tantivy

**Decision**: Use Write-Ahead Log (WAL) for durability, HNSW for vector similarity search, and Tantivy for full-text search.

**Rationale**:
- WAL provides crash recovery with fsync guarantee
- HNSW offers O(log n) approximate nearest neighbor search
- Tantivy provides BM25 scoring for keyword queries
- Hybrid scoring combines all signals

**Consequences**:
- Write path: WAL append → in-memory index update → ack
- Read path: Vector search + text search → merge → score → rank

### 2. Sharding: Tenant + Agent Hash

**Decision**: Shard by `tenant_id` hash, then `agent_id` hash within tenant.

**Rationale**:
- Keeps an agent's data on one shard (locality)
- Spreads load horizontally for multi-tenant
- Avoids cross-shard queries for single-agent operations

**Consequences**:
- Single-shard queries are fast (no coordination)
- Cross-tenant queries require fan-out
- Rebalancing requires data movement

### 3. Shared Memory: Eventually Consistent

**Decision**: Shared office memory is eventually consistent with last-writer-wins and vector clock conflict detection.

**Rationale**:
- Strong consistency would require consensus (Raft) on every write
- Most shared memory operations tolerate eventual consistency
- Only narrow workflows (task claiming) need linearizability
- Vector clocks detect conflicts without preventing them

**Consequences**:
- Conflicting writes are both retained (never silently dropped)
- Readers may see stale data briefly
- Conflict resolution is application-specific

### 4. Anti-Poisoning: Trust Tiers + Quarantine

**Decision**: Every record carries a trust tier (Verified/Standard/Untrusted). Untrusted content is quarantined until corroborated.

**Rationale**:
- OWASP ASI06 classifies memory poisoning as critical
- Published attacks succeed 80-99% against undefended stores
- Trust tiers gate promotion to semantic/shared memory
- Lineage tracking enables rollback

**Consequences**:
- Untrusted content cannot auto-promote
- Corroboration requires independent Verified/Standard sources
- Poisoned source can be identified and invalidated transitively

### 5. Retrieval: Hybrid Scoring with Explainability

**Decision**: Combine cosine similarity, recency, importance, and BM25 with trust multiplier. Return full score breakdown.

**Rationale**:
- No single signal is sufficient for good retrieval
- Trust multiplier penalizes untrusted content
- Explainability is required for debugging and audits
- Weights are per-agent, hot-reloadable

**Consequences**:
- Retrieval is more complex but more accurate
- Score breakdown enables introspection
- Adaptive tuning can optimize weights from outcomes

### 6. WAL: Fsync Before Ack

**Decision**: WAL entry must be fsynced to disk before write acknowledgment.

**Rationale**:
- Guarantees crash durability
- Never ack a write that could be lost
- Checkpointing reduces replay time on restart

**Consequences**:
- Write latency includes disk I/O
- Batched commits reduce per-write overhead
- Replica can lag behind primary

### 7. Encryption: AES-256-GCM at Rest

**Decision**: Encrypt shard files with AES-256-GCM. Keys from secrets manager.

**Rationale**:
- Protects data if storage is compromised
- AES-256-GCM provides authenticated encryption
- Secrets manager avoids key exposure in config

**Consequences**:
- Slight performance overhead for encryption/decryption
- Key rotation requires re-encryption
- Backup/restore must handle encryption keys

## Alternatives Considered

### PostgreSQL + pgvector
- **Pros**: ACID, mature ecosystem, pgvector for vectors
- **Cons**: Heavier dependency, less control over WAL, harder to shard by agent

### Redis + RediSearch
- **Pros**: Fast in-memory, good for hot data
- **Cons**: Not durable by default, limited vector search, memory costs

### Custom B-tree + Inverted Index
- **Pros**: Full control, optimized for access patterns
- **Cons**: More development effort, less battle-tested

## References
- Mem0: Hybrid fusion approach
- Zep: Temporal knowledge graph
- MemGPT/Letta: Tiered paging
- OWASP ASI06: Memory poisoning classification
