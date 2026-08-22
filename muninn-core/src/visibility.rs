use crate::model::AgentId;
use serde::{Deserialize, Serialize};

/// Visibility model — enforced at the storage layer, not just the API handler
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Visibility {
    /// Only the owning agent can read
    Private,
    /// All agents in the same tenant can read
    Shared,
    /// Only specific agents can read
    SharedWith(Vec<AgentId>),
}

impl Visibility {
    /// Check if a given agent can access this record
    pub fn allows_access(&self, owner_agent_id: &AgentId, requesting_agent_id: &AgentId) -> bool {
        match self {
            Visibility::Private => owner_agent_id == requesting_agent_id,
            Visibility::Shared => true,
            Visibility::SharedWith(allowed_agents) => {
                owner_agent_id == requesting_agent_id || allowed_agents.contains(requesting_agent_id)
            }
        }
    }
}
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
// commit 172 1788294956304472277
// commit 244 1788294957427347445
// commit 268 1788294957814796483
// commit 292 1788294958182933770
