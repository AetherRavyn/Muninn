use std::collections::HashMap;
use parking_lot::RwLock;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::model::{AgentId, MemoryRecord, MemoryTier, RetentionClass, TenantId};
use crate::trust::TrustTier;
use crate::visibility::Visibility;
use crate::vector_clock::VectorClock;

/// A versioned, reusable learned routine.
/// Capacity-bounded per agent; old/unused routines decay like episodic memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Procedure {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub agent_id: AgentId,
    pub name: String,
    pub description: String,
    pub steps: Vec<ProcedureStep>,
    pub version: u32,
    pub created_at: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
    pub use_count: u32,
    pub success_rate: f32,
    pub importance: f32,
    pub tags: Vec<String>,
    pub superseded_by: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcedureStep {
    pub order: usize,
    pub action: String,
    pub parameters: HashMap<String, String>,
    pub expected_outcome: Option<String>,
}

/// Procedural memory store — versioned routines per agent.
pub struct ProceduralMemory {
    /// Map of agent_id -> (procedure_name -> procedures, newest first)
    procedures: RwLock<HashMap<String, HashMap<String, Vec<Procedure>>>>,
    max_procedures_per_agent: usize,
    max_versions_per_procedure: usize,
}

impl ProceduralMemory {
    pub fn new(max_procedures_per_agent: usize, max_versions_per_procedure: usize) -> Self {
        Self {
            procedures: RwLock::new(HashMap::new()),
            max_procedures_per_agent,
            max_versions_per_procedure,
        }
    }

    /// Store a new procedure (or a new version of an existing one)
    pub fn store(&self, procedure: Procedure) -> Result<()> {
        let mut procedures = self.procedures.write();
        let agent_id = procedure.agent_id.0.clone();
        let proc_name = procedure.name.clone();

        let agent_procedures = procedures
            .entry(agent_id)
            .or_insert_with(HashMap::new);

        // Check capacity before inserting
        let total_procedures: usize = agent_procedures.values().map(|v| v.len()).sum();
        if total_procedures >= self.max_procedures_per_agent {
            // Find and remove the least-used procedure name
            let worst_name = agent_procedures.iter()
                .min_by_key(|(_, versions)| {
                    versions.first().map(|p| p.use_count).unwrap_or(0)
                })
                .map(|(name, _)| name.clone());
            if let Some(name) = worst_name {
                agent_procedures.remove(&name);
            }
        }

        let versions = agent_procedures
            .entry(proc_name)
            .or_insert_with(Vec::new);

        // Version cap
        if versions.len() >= self.max_versions_per_procedure {
            versions.remove(0); // Remove oldest version
        }

        versions.push(procedure);
        // Sort by version descending (newest first)
        versions.sort_by(|a, b| b.version.cmp(&a.version));

        Ok(())
    }

    /// Get the latest version of a procedure by name
    pub fn get_latest(&self, agent_id: &AgentId, name: &str) -> Option<Procedure> {
        let procedures = self.procedures.read();
        procedures
            .get(&agent_id.0)?
            .get(name)?
            .first()
            .cloned()
    }

    /// Get a specific version of a procedure
    pub fn get_version(&self, agent_id: &AgentId, name: &str, version: u32) -> Option<Procedure> {
        let procedures = self.procedures.read();
        procedures
            .get(&agent_id.0)?
            .get(name)?
            .iter()
            .find(|p| p.version == version)
            .cloned()
    }

    /// List all procedure names for an agent
    pub fn list_names(&self, agent_id: &AgentId) -> Vec<String> {
        let procedures = self.procedures.read();
        procedures
            .get(&agent_id.0)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Record usage of a procedure
    pub fn record_usage(&self, agent_id: &AgentId, name: &str, success: bool) {
        let mut procedures = self.procedures.write();
        if let Some(agent_procs) = procedures.get_mut(&agent_id.0) {
            if let Some(versions) = agent_procs.get_mut(name) {
                if let Some(proc) = versions.first_mut() {
                    proc.last_used = Utc::now();
                    proc.use_count += 1;
                    // Update success rate with exponential moving average
                    let alpha = 0.1;
                    let success_val = if success { 1.0 } else { 0.0 };
                    proc.success_rate = proc.success_rate * (1.0 - alpha) + success_val * alpha;
                }
            }
        }
    }

    /// Supersede a procedure with a new version
    pub fn supersede(&self, agent_id: &AgentId, name: &str, old_version: u32, new_id: Uuid) -> Result<()> {
        let mut procedures = self.procedures.write();
        if let Some(agent_procs) = procedures.get_mut(&agent_id.0) {
            if let Some(versions) = agent_procs.get_mut(name) {
                for proc in versions.iter_mut() {
                    if proc.version == old_version {
                        proc.superseded_by = Some(new_id);
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Decay pass — remove procedures that haven't been used recently
    pub fn decay_pass(&self, max_age_days: i64) -> usize {
        let cutoff = Utc::now() - chrono::Duration::days(max_age_days);
        let mut removed = 0;

        let mut procedures = self.procedures.write();
        for agent_procs in procedures.values_mut() {
            for versions in agent_procs.values_mut() {
                let initial_len = versions.len();
                versions.retain(|p| {
                    p.last_used > cutoff || p.importance > 0.8 || p.success_rate > 0.7
                });
                removed += initial_len - versions.len();
            }
        }

        removed
    }

    /// Convert a procedure to a memory record for storage in the main index
    pub fn to_memory_record(&self, procedure: &Procedure) -> MemoryRecord {
        let content = format!(
            "Procedure: {} v{} — {} (success rate: {:.0}%, used {} times)",
            procedure.name,
            procedure.version,
            procedure.description,
            procedure.success_rate * 100.0,
            procedure.use_count
        );

        MemoryRecord {
            id: procedure.id,
            tenant_id: procedure.tenant_id.clone(),
            agent_id: procedure.agent_id.clone(),
            tier: MemoryTier::Procedural,
            schema_version: crate::model::CURRENT_SCHEMA_VERSION,
            content,
            embedding: vec![], // Populated by embedding provider
            embedding_model_version: String::new(),
            importance: procedure.importance,
            retention_class: RetentionClass::Standard,
            trust_tier: TrustTier::Verified, // Procedures are from known agent actions
            created_at: procedure.created_at,
            last_accessed: procedure.last_used,
            access_count: procedure.use_count,
            visibility: Visibility::Private,
            source_ids: vec![],
            superseded_by: procedure.superseded_by,
            vector_clock: VectorClock::new(),
        }
    }

    /// Get total procedure count for an agent
    pub fn count(&self, agent_id: &AgentId) -> usize {
        let procedures = self.procedures.read();
        procedures
            .get(&agent_id.0)
            .map(|m| m.values().map(|v| v.len()).sum())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_procedure(name: &str, version: u32) -> Procedure {
        Procedure {
            id: Uuid::new_v4(),
            tenant_id: TenantId("t1".to_string()),
            agent_id: AgentId("a1".to_string()),
            name: name.to_string(),
            description: format!("Test procedure {}", name),
            steps: vec![ProcedureStep {
                order: 1,
                action: "do_something".to_string(),
                parameters: HashMap::new(),
                expected_outcome: None,
            }],
            version,
            created_at: Utc::now(),
            last_used: Utc::now(),
            use_count: 0,
            success_rate: 1.0,
            importance: 0.5,
            tags: vec![],
            superseded_by: None,
        }
    }

    #[test]
    fn test_store_and_retrieve() {
        let mem = ProceduralMemory::new(100, 5);
        let proc = test_procedure("fetch_data", 1);
        mem.store(proc).unwrap();

        let latest = mem.get_latest(&AgentId("a1".to_string()), "fetch_data");
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().version, 1);
    }

    #[test]
    fn test_versioning() {
        let mem = ProceduralMemory::new(100, 5);
        mem.store(test_procedure("fetch_data", 1)).unwrap();
        mem.store(test_procedure("fetch_data", 2)).unwrap();
        mem.store(test_procedure("fetch_data", 3)).unwrap();

        let latest = mem.get_latest(&AgentId("a1".to_string()), "fetch_data").unwrap();
        assert_eq!(latest.version, 3);

        let v1 = mem.get_version(&AgentId("a1".to_string()), "fetch_data", 1).unwrap();
        assert_eq!(v1.version, 1);
    }

    #[test]
    fn test_usage_tracking() {
        let mem = ProceduralMemory::new(100, 5);
        mem.store(test_procedure("fetch_data", 1)).unwrap();

        let agent = AgentId("a1".to_string());
        mem.record_usage(&agent, "fetch_data", true);
        mem.record_usage(&agent, "fetch_data", true);
        mem.record_usage(&agent, "fetch_data", false);

        let proc = mem.get_latest(&agent, "fetch_data").unwrap();
        assert_eq!(proc.use_count, 3);
        assert!(proc.success_rate < 1.0); // Should be < 1.0 due to one failure
    }

    #[test]
    fn test_capacity_eviction() {
        let mem = ProceduralMemory::new(2, 5); // Max 2 procedures per agent
        mem.store(test_procedure("proc_a", 1)).unwrap();
        mem.record_usage(&AgentId("a1".to_string()), "proc_a", true);

        mem.store(test_procedure("proc_b", 1)).unwrap();
        mem.record_usage(&AgentId("a1".to_string()), "proc_b", true);

        // Adding a third should evict the least used
        mem.store(test_procedure("proc_c", 1)).unwrap();

        assert_eq!(mem.count(&AgentId("a1".to_string())), 2);
    }

    #[test]
    fn test_decay() {
        let mem = ProceduralMemory::new(100, 5);
        let mut proc = test_procedure("old_proc", 1);
        proc.last_used = Utc::now() - chrono::Duration::days(100);
        proc.importance = 0.2;
        proc.success_rate = 0.3;
        mem.store(proc).unwrap();

        let removed = mem.decay_pass(30);
        assert_eq!(removed, 1);
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
# 1788294674
# 1788294674
# 1788294674
# 1788294674
# 1788294674
// commit 56 1788294954513987400
// commit 80 1788294954881560456
// commit 128 1788294955618802901
