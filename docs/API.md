# Muninn API Documentation

## Overview

Muninn provides two API interfaces:
- **REST API** (port 3000) — External client access
- **Internal API** (port 50051) — Agent-to-agent communication

## Authentication

All API requests require an API key in the `X-Api-Key` header.

```bash
curl -H "X-Api-Key: your-api-key" http://localhost:3000/api/...
```

## REST API Endpoints

### Write Memory

```http
POST /api/v1/memory/write
Content-Type: application/json
X-Api-Key: your-api-key

{
  "tenant_id": "office-1",
  "agent_id": "agent-a",
  "content": "The project deadline is March 15",
  "importance": 0.8,
  "visibility": "private",
  "trust_tier": "standard",
  "tier": "episodic"
}
```

**Response:**
```json
{
  "record_id": "550e8400-e29b-41d4-a716-446655440000",
  "wal_offset": 12345,
  "timestamp": "2026-09-01T12:00:00Z"
}
```

### Retrieve Memory

```http
POST /api/v1/memory/retrieve
Content-Type: application/json
X-Api-Key: your-api-key

{
  "tenant_id": "office-1",
  "agent_id": "agent-a",
  "query": "What is the project deadline?",
  "max_results": 5,
  "min_score": 0.1,
  "tiers": ["episodic", "semantic"],
  "keyword_query": "deadline"
}
```

**Response:**
```json
{
  "results": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "content": "The project deadline is March 15",
      "tier": "episodic",
      "trust_tier": "standard",
      "importance": 0.8,
      "score": 0.95,
      "score_breakdown": {
        "cosine_similarity": 0.92,
        "recency_score": 0.98,
        "importance_score": 0.8,
        "keyword_score": 0.85,
        "trust_multiplier": 0.9,
        "total_score": 0.95
      },
      "rank": 1
    }
  ],
  "total_candidates": 1,
  "query_time_ms": 2
}
```

### Get Record

```http
GET /api/v1/memory/{record_id}
X-Api-Key: your-api-key
```

### Supersede Record

```http
POST /api/v1/memory/{record_id}/supersede
Content-Type: application/json
X-Api-Key: your-api-key

{
  "superseded_by": "new-record-id"
}
```

### Get Lineage

```http
GET /api/v1/memory/{record_id}/lineage
X-Api-Key: your-api-key
```

**Response:**
```json
{
  "root_id": "550e8400-e29b-41d4-a716-446655440000",
  "total_downstream_facts": 5,
  "affects_shared_memory": true,
  "affects_other_agents": false,
  "nodes": [...],
  "edges": [...]
}
```

### Purge Tenant

```http
DELETE /api/v1/tenants/{tenant_id}/purge
X-Api-Key: your-api-key
```

**Response:**
```json
{
  "records_purged": 1234
}
```

### Health Check

```http
GET /api/healthz
```

### Ready Check

```http
GET /api/readyz
```

### Metrics (Prometheus)

```http
GET /api/metrics
```

## Internal API (Port 50051)

The internal API uses a line-based JSON protocol for agent-to-agent communication.

### Protocol Format

Each request is a single JSON line followed by newline:
```json
{"command": "write", "tenant_id": "t1", "agent_id": "a1", "content": "..."}\n
```

Each response is a single JSON line:
```json
{"type": "write", "record_id": "...", "wal_offset": 123, "timestamp": "..."}\n

### Commands

#### Write
```json
{"command": "write", "tenant_id": "...", "agent_id": "...", "content": "...", "importance": 0.5}
```

#### Retrieve
```json
{"command": "retrieve", "tenant_id": "...", "agent_id": "...", "query": "...", "max_results": 10}
```

#### Lineage
```json
{"command": "lineage", "record_id": "..."}
```

#### Health
```json
{"command": "health"}
```

## Rate Limiting

- **Write rate**: 100 requests/minute per tenant
- **Influence cap**: 50 points/hour per tenant on shared memory

Exceeding limits returns `429 Too Many Requests`.

## Error Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad request (invalid JSON, missing fields) |
| 401 | Unauthorized (missing or invalid API key) |
| 404 | Not found |
| 429 | Rate limited |
| 500 | Internal server error |

## Data Model

### Trust Tiers

| Tier | Description | Auto-promote | Retrieval multiplier |
|------|-------------|--------------|---------------------|
| `verified` | Authenticated, known-good action | Yes | 1.0 |
| `standard` | Normal agent traffic | Yes | 0.9 |
| `untrusted` | External input, potential attack | No (quarantine) | 0.5 |

### Visibility

| Level | Description |
|-------|-------------|
| `private` | Only owning agent can read |
| `shared` | All agents in tenant can read |
| `shared_with` | Specific agents can read |

### Memory Tiers

| Tier | Description |
|------|-------------|
| `episodic` | Timestamped raw events |
| `semantic` | Distilled facts (knowledge graph) |
| `procedural` | Versioned learned routines |
| `shared` | Cross-agent shared context |

## Idempotency

All write operations are idempotent. Retrying the same request will not create duplicate records.

## Schema Versioning

All records carry a `schema_version` field. Migrations are explicit functions that transform records between versions. See `migration.rs` for details.
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
// commit 69 1788294954714939061
// commit 93 1788294955079978177
// commit 141 1788294955832260524
// commit 165 1788294956196324211
// commit 261 1788294957703225842
