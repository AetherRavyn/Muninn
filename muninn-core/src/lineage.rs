use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::MemoryTier;
use crate::trust::TrustTier;

/// Lineage graph for tracking the origin and derivation chain of memory records.
/// This is the core of anti-poisoning defense: when a compromised source is identified,
/// trace_lineage() finds every fact derived from it for invalidation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageRecord {
    pub id: Uuid,
    pub source_episode_id: Option<Uuid>,
    pub derived_facts: Vec<Uuid>,
    pub parent_lineage: Option<Uuid>,
    pub trust_tier: TrustTier,
    pub created_at: DateTime<Utc>,
}

/// Full lineage graph returned by trace_lineage()
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullLineageGraph {
    /// The root episode or record being traced
    pub root_id: Uuid,
    /// All records reachable from the root via source_ids
    pub nodes: Vec<LineageNode>,
    /// Edges representing derivation relationships
    pub edges: Vec<LineageEdge>,
    /// Total number of downstream facts (transitive closure)
    pub total_downstream_facts: usize,
    /// Whether any downstream facts are in shared/semantic memory
    pub affects_shared_memory: bool,
    /// Whether any downstream facts are in other agents' private memory
    pub affects_other_agents: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageNode {
    pub id: Uuid,
    pub tier: MemoryTier,
    pub trust_tier: TrustTier,
    pub content_preview: String,
    pub created_at: DateTime<Utc>,
    pub agent_id: String,
    pub is_superseded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    pub from: Uuid,
    pub to: Uuid,
    pub relationship: LineageRelationship,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LineageRelationship {
    /// Episode was used as source to derive a fact
    EpisodeToFact,
    /// Fact was used as source to derive another fact
    FactToFact,
    /// Episode was consolidated into another episode
    EpisodeToEpisode,
}

/// Poisoning incident report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoisoningIncidentReport {
    pub incident_id: Uuid,
    pub source_id: Uuid,
    pub source_trust_tier: TrustTier,
    pub discovered_at: DateTime<Utc>,
    pub discovered_by: String,
    pub downstream_facts_invalidated: Vec<Uuid>,
    pub affected_agents: Vec<String>,
    pub affected_tenants: Vec<String>,
    pub blast_radius_summary: String,
    pub remediation_actions: Vec<String>,
}

/// Lineage tracker trait
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageTracker {
    /// Maps record_id -> list of records that list this as a source
    pub forward_edges: std::collections::HashMap<Uuid, Vec<Uuid>>,
    /// Maps record_id -> list of source_ids for this record
    pub backward_edges: std::collections::HashMap<Uuid, Vec<Uuid>>,
}

impl LineageTracker {
    pub fn new() -> Self {
        Self {
            forward_edges: std::collections::HashMap::new(),
            backward_edges: std::collections::HashMap::new(),
        }
    }

    /// Record that `record_id` was derived from `source_id`
    pub fn add_derivation(&mut self, record_id: Uuid, source_id: Uuid) {
        self.forward_edges
            .entry(source_id)
            .or_default()
            .push(record_id);
        self.backward_edges
            .entry(record_id)
            .or_default()
            .push(source_id);
    }

    /// Trace all downstream records from a given source (BFS)
    pub fn trace_downstream(&self, source_id: &Uuid) -> Vec<Uuid> {
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(*source_id);

        while let Some(current) = queue.pop_front() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current);

            if let Some(children) = self.forward_edges.get(&current) {
                for child in children {
                    queue.push_back(*child);
                }
            }
        }

        visited.into_iter().collect()
    }

    /// Get direct sources of a record
    pub fn get_sources(&self, record_id: &Uuid) -> Vec<Uuid> {
        self.backward_edges
            .get(record_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get all records directly derived from a source
    pub fn get_direct_derivations(&self, source_id: &Uuid) -> Vec<Uuid> {
        self.forward_edges
            .get(source_id)
            .cloned()
            .unwrap_or_default()
    }
}

impl Default for LineageTracker {
    fn default() -> Self {
        Self::new()
    }
}
