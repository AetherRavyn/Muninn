# Muninn Operations Runbooks

## Table of Contents
1. [Shard Primary Failure](#shard-primary-failure)
2. [Replica Lag](#replica-lag)
3. [Consolidation Backlog](#consolidation-backlog)
4. [Embedding Provider Outage](#embedding-provider-outage)
5. [Restore from Backup](#restore-from-backup)
6. [Suspected Memory Poisoning](#suspected-memory-poisoning)
7. [High Latency Alert](#high-latency-alert)
8. [Disk Space Warning](#disk-space-warning)

---

## Shard Primary Failure

**Symptoms**: Read/write errors, health check failing, metrics showing 0 throughput for a shard.

**Blast radius**: All agents assigned to this shard lose memory access.

### Diagnosis
```bash
# Check shard health
curl http://muninn-shard-0:8080/healthz

# Check WAL integrity
ls -la /data/wal/
md5sum /data/wal/wal_*.log

# Check disk space
df -h /data
```

### Resolution
1. **If WAL is intact**: Promote the standby replica
   ```bash
   # On the replica node
   muninn promote --data-dir /data --wal-dir /data/wal
   ```

2. **If WAL is corrupted**: Restore from latest snapshot
   ```bash
   # See "Restore from Backup" section
   ```

3. **Update DNS/service discovery** to point to the new primary

4. **Monitor** replica lag on the new primary

**RTO target**: ≤ 15 minutes

---

## Replica Lag

**Symptoms**: Alert firing for `wal_replica_lag_bytes > threshold`

### Diagnosis
```bash
# Check primary WAL position
curl http://muninn-primary:8080/metrics | grep wal_offset

# Check replica WAL position
curl http://muninn-replica:8080/metrics | grep wal_offset

# Calculate lag
lag=$((primary_offset - replica_offset))
echo "Lag: $lag bytes"
```

### Resolution
1. **If lag is growing**: Check network connectivity between primary and replica
2. **If lag is stable**: May be normal during high write load — monitor
3. **If lag exceeds RPO (5 min of writes)**: Alert on-call, consider manual sync

**RPO target**: ≤ 5 minutes of writes

---

## Consolidation Backlog

**Symptoms**: Alert for `consolidation_queue_depth > 1000`

### Diagnosis
```bash
# Check consolidation metrics
curl http://muninn:8080/metrics | grep consolidation

# Check if consolidation is enabled
curl http://muninn:8080/readyz
```

### Resolution
1. **Check embedding provider**: If down, consolidation can't proceed
2. **Increase batch size**: Update config
   ```toml
   [consolidation]
   batch_size = 200  # Default 100
   max_concurrent_jobs = 4  # Default 2
   ```
3. **Restart consolidation worker** if stuck

---

## Embedding Provider Outage

**Symptoms**: Write failures, embedding errors in logs

### Diagnosis
```bash
# Test embedding provider connectivity
curl -X POST https://api.openai.com/v1/embeddings \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"input": "test", "model": "text-embedding-3-small"}'
```

### Resolution
1. **Circuit breaker should activate automatically** — writes will be queued for async embedding backfill
2. **If provider is down for extended period**:
   - Switch to backup provider (if configured)
   - Or temporarily reduce write rate
3. **When provider recovers**: Backfill will complete automatically

**Graceful degradation**: Writes are queued, not rejected

---

## Restore from Backup

**Prerequisites**: Access to S3-compatible storage with WAL snapshots

### Steps
```bash
# 1. List available backups
aws s3 ls s3://muninn-backups/snapshots/ --recursive

# 2. Download latest snapshot
aws s3 cp s3://muninn-backups/snapshots/latest.tar.gz /tmp/

# 3. Stop the shard
systemctl stop muninn-shard

# 4. Backup current data (just in case)
mv /data /data.bak.$(date +%s)

# 5. Extract snapshot
mkdir -p /data
tar xzf /tmp/latest.tar.gz -C /data

# 6. Replay any WAL entries after snapshot
muninn replay-wal --data-dir /data --wal-dir /data/wal

# 7. Start the shard
systemctl start muninn-shard

# 8. Verify health
curl http://localhost:8080/healthz
```

**RTO target**: ≤ 15 minutes
**Test quarterly** via restore drill

---

## Suspected Memory Poisoning

**Symptoms**: Anomalous agent behavior, contradictory facts in semantic memory, alerts from anomaly detection

### ⚠️ CRITICAL — Do Not Delay

### Immediate Containment (First 15 Minutes)
1. **Identify the suspected source**
   ```bash
   # Check audit log for suspicious writes
   grep -i "untrusted" /var/log/muninn/audit.log | tail -50
   ```

2. **Quarantine the source** — revoke API key or isolate agent
   ```bash
   # Revoke agent's API key
   curl -X DELETE http://muninn:3000/api/v1/agents/{agent_id}/key
   ```

3. **Stop the agent** to prevent further writes

### Investigation (Next 30 Minutes)
4. **Trace lineage** of suspicious records
   ```bash
   curl http://muninn:3000/api/v1/memory/{suspicious_record_id}/lineage
   ```

5. **Identify all downstream facts** derived from the poisoned source

6. **Check shared memory** — did poisoned content reach semantic/shared memory?

### Remediation
7. **Invalidate all downstream facts**
   ```bash
   # Use the lineage graph to supersede all affected records
   for fact_id in $(curl -s http://muninn:3000/api/v1/memory/{id}/lineage | jq -r '.nodes[].id'); do
     curl -X POST http://muninn:3000/api/v1/memory/$fact_id/supersede \
       -d '{"superseded_by": null}'  # Mark as invalid
   done
   ```

8. **Document the incident**
   - Which agents were affected
   - What facts were exposed
   - Timeline of the attack

9. **Review and harden**
   - Check if quarantine was bypassed
   - Review trust tier assignments
   - Consider additional rate limits for the source

### Post-Incident
10. **Run red-team test** to verify defenses hold
11. **Update runbook** with new attack patterns if discovered

---

## High Latency Alert

**Symptoms**: Alert for `read_latency_p99 > 20ms` or `write_latency_p99 > 1ms`

### Diagnosis
```bash
# Check current latency metrics
curl http://muninn:8080/metrics | grep latency

# Check disk I/O
iostat -x 1 5

# Check CPU usage
top -bn1 | head -20

# Check memory pressure
free -h
```

### Resolution
1. **If disk I/O is high**: Check for WAL rotation or snapshot in progress
2. **If CPU is high**: Check consolidation worker load
3. **If memory is high**: Check vector index size, consider pruning

---

## Disk Space Warning

**Symptoms**: Alert for `disk_usage > 80%`

### Diagnosis
```bash
# Check disk usage by directory
du -sh /data/*
du -sh /data/wal/*

# Check for large WAL segments
ls -lhS /data/wal/
```

### Resolution
1. **Rotate WAL** if segments are too large
   ```bash
   curl -X POST http://muninn:8080/api/v1/admin/wal/rotate
   ```

2. **Run decay pass** to evict old records
   ```bash
   curl -X POST http://muninn:8080/api/v1/admin/decay
   ```

3. **Archive old data** to cold storage
   ```bash
   curl -X POST http://muninn:8080/api/v1/admin/archive?older_than=90d
   ```

4. **Expand volume** if needed (Kubernetes PVC)

---

## Emergency Contacts

| Role | Contact | Escalation |
|---|---|---|
| On-call Engineer | PagerDuty | Immediate |
| Security Team | security@company.com | Within 1 hour |
| Platform Team | #platform-ops | Business hours |

---

## SLO Reference

| Metric | Target | Alert Threshold |
|---|---|---|
| Cold recall | < 20ms p99 | > 20ms for 5 min |
| Hot recall | < 500µs p99 | > 500µs for 5 min |
| Write ack | Fsynced to WAL | Any un-acked write |
| Availability | 99.9% | < 99.9% monthly |
| RPO | ≤ 5 min | > 5 min |
| RTO | ≤ 15 min | > 15 min |
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
# 1788294677
// commit 45 1788294954345876883
// commit 117 1788294955445504781
// commit 213 1788294956949231303
// commit 237 1788294957318218030
// commit 333 1788294958819021606
// commit 357 1788294959192838541
// commit 405 1788294959928579445
