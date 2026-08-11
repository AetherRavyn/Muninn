use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::Mutex;
use uuid::Uuid;


/// Audit event types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditEvent {
    /// A memory record was written
    Write {
        record_id: Uuid,
        tenant_id: String,
        agent_id: String,
        tier: String,
        trust_tier: String,
        visibility: String,
    },
    /// A memory record was read
    Read {
        record_id: Uuid,
        tenant_id: String,
        agent_id: String,
        requesting_agent: String,
    },
    /// A record was superseded
    Supersede {
        record_id: Uuid,
        superseded_by: Uuid,
        tenant_id: String,
    },
    /// Lineage was traced (for poisoning investigation)
    LineageTrace {
        record_id: Uuid,
        traced_by: String,
        downstream_count: usize,
        affects_shared_memory: bool,
    },
    /// Tenant data purge initiated
    TenantPurge {
        tenant_id: String,
        initiated_by: String,
        records_purged: usize,
    },
    /// Legal-hold record accessed
    LegalHoldAccess {
        record_id: Uuid,
        tenant_id: String,
        accessor: String,
        access_type: String,
    },
    /// Consolidation produced a fact
    Consolidation {
        source_episode_ids: Vec<Uuid>,
        fact_id: Uuid,
        trust_tier: String,
        quarantined: bool,
    },
    /// Anomaly detected during consolidation
    Anomaly {
        anomaly_type: String,
        description: String,
        severity: String,
    },
    /// Authentication failure
    AuthFailure {
        source_ip: Option<String>,
        api_key_hint: String,
        reason: String,
    },
}

/// Complete audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub event: AuditEvent,
}

/// Append-only audit log for compliance and security.
/// Thread-safe, writes to file + optional in-memory buffer for recent events.
pub struct AuditLog {
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    recent_buffer: Arc<Mutex<Vec<AuditLogEntry>>>,
    buffer_capacity: usize,
}

impl AuditLog {
    /// Create a new audit log writing to a file
    pub fn new(log_path: &PathBuf, buffer_capacity: usize) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(log_path.parent().unwrap_or(std::path::Path::new(".")))?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;

        Ok(Self {
            writer: Arc::new(Mutex::new(Box::new(file))),
            recent_buffer: Arc::new(Mutex::new(Vec::with_capacity(buffer_capacity))),
            buffer_capacity,
        })
    }

    /// Create an audit log that writes to stdout (for testing/dev)
    pub fn stdout(buffer_capacity: usize) -> Self {
        Self {
            writer: Arc::new(Mutex::new(Box::new(std::io::stdout()))),
            recent_buffer: Arc::new(Mutex::new(Vec::with_capacity(buffer_capacity))),
            buffer_capacity,
        }
    }

    /// Log an audit event
    pub fn log(&self, event: AuditEvent) {
        let entry = AuditLogEntry {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event,
        };

        // Write to file (append-only)
        if let Ok(json) = serde_json::to_string(&entry) {
            let mut writer = self.writer.lock();
            let _ = writeln!(writer, "{}", json);
            let _ = writer.flush();
        }

        // Add to recent buffer
        let mut buffer = self.recent_buffer.lock();
        if buffer.len() >= self.buffer_capacity {
            buffer.remove(0);
        }
        buffer.push(entry);
    }

    /// Get recent audit entries (for debugging/introspection)
    pub fn recent(&self) -> Vec<AuditLogEntry> {
        self.recent_buffer.lock().clone()
    }

    /// Get recent entries for a specific tenant
    pub fn recent_for_tenant(&self, tenant_id: &str) -> Vec<AuditLogEntry> {
        self.recent_buffer
            .lock()
            .iter()
            .filter(|e| match &e.event {
                AuditEvent::Write { tenant_id: t, .. } => t == tenant_id,
                AuditEvent::Read { tenant_id: t, .. } => t == tenant_id,
                AuditEvent::Supersede { tenant_id: t, .. } => t == tenant_id,
                AuditEvent::TenantPurge { tenant_id: t, .. } => t == tenant_id,
                AuditEvent::LegalHoldAccess { tenant_id: t, .. } => t == tenant_id,
                _ => false,
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_log_stdout() {
        let log = AuditLog::stdout(100);
        log.log(AuditEvent::Write {
            record_id: Uuid::new_v4(),
            tenant_id: "t1".to_string(),
            agent_id: "a1".to_string(),
            tier: "episodic".to_string(),
            trust_tier: "standard".to_string(),
            visibility: "private".to_string(),
        });

        let recent = log.recent();
        assert_eq!(recent.len(), 1);
    }

    #[test]
    fn test_audit_log_tenant_filter() {
        let log = AuditLog::stdout(100);
        log.log(AuditEvent::Write {
            record_id: Uuid::new_v4(),
            tenant_id: "t1".to_string(),
            agent_id: "a1".to_string(),
            tier: "episodic".to_string(),
            trust_tier: "standard".to_string(),
            visibility: "private".to_string(),
        });
        log.log(AuditEvent::Write {
            record_id: Uuid::new_v4(),
            tenant_id: "t2".to_string(),
            agent_id: "a2".to_string(),
            tier: "episodic".to_string(),
            trust_tier: "standard".to_string(),
            visibility: "private".to_string(),
        });

        let t1_events = log.recent_for_tenant("t1");
        assert_eq!(t1_events.len(), 1);
    }

    #[test]
    fn test_audit_log_buffer_eviction() {
        let log = AuditLog::stdout(3);
        for _ in 0..5 {
            log.log(AuditEvent::Write {
                record_id: Uuid::new_v4(),
                tenant_id: "t1".to_string(),
                agent_id: "a1".to_string(),
                tier: "episodic".to_string(),
                trust_tier: "standard".to_string(),
                visibility: "private".to_string(),
            });
        }

        let recent = log.recent();
        assert_eq!(recent.len(), 3); // Buffer capped at 3
    }
}
# 1788294674
# 1788294674
# 1788294674
# 1788294674
# 1788294674
# 1788294674
# 1788294674
# 1788294674
# 1788294674
# 1788294674
# 1788294674
// commit 6 1788294953776683867
// commit 30 1788294954129469959
// commit 54 1788294954481886978
// commit 126 1788294955588167569
