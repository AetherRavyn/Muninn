use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Vector clock for optimistic concurrency control on shared memory.
/// Last-writer-wins with conflict detection — conflicting writes retained, never silently dropped.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorClock {
    /// Maps agent_id -> logical timestamp
    clocks: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    /// Increment the clock for a given agent
    pub fn increment(&mut self, agent_id: &str) {
        *self.clocks.entry(agent_id.to_string()).or_insert(0) += 1;
    }

    /// Get the current timestamp for an agent
    pub fn get(&self, agent_id: &str) -> u64 {
        *self.clocks.get(agent_id).unwrap_or(&0)
    }

    /// Merge two vector clocks (take component-wise max)
    pub fn merge(&mut self, other: &VectorClock) {
        for (agent, &timestamp) in &other.clocks {
            let entry = self.clocks.entry(agent.clone()).or_insert(0);
            *entry = (*entry).max(timestamp);
        }
    }

    /// Check if this clock dominates (is strictly newer than) another
    pub fn dominates(&self, other: &VectorClock) -> bool {
        // self dominates other if for all agents, self >= other,
        // and for at least one agent, self > other
        let mut any_greater = false;
        let all_agents: std::collections::HashSet<&String> =
            self.clocks.keys().chain(other.clocks.keys()).collect();

        for agent in all_agents {
            let self_ts = self.get(agent);
            let other_ts = other.get(agent);
            if self_ts < other_ts {
                return false;
            }
            if self_ts > other_ts {
                any_greater = true;
            }
        }
        any_greater
    }

    /// Check if two clocks are concurrent (neither dominates)
    pub fn is_concurrent_with(&self, other: &VectorClock) -> bool {
        !self.dominates(other) && !other.dominates(self)
    }

    /// Check if two clocks conflict (are concurrent)
    pub fn conflicts_with(&self, other: &VectorClock) -> bool {
        self.is_concurrent_with(other)
    }

    /// Create a child clock (increment for a given agent)
    pub fn child(&self, agent_id: &str) -> VectorClock {
        let mut child = self.clone();
        child.increment(agent_id);
        child
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut clock = VectorClock::new();
        assert_eq!(clock.get("agent_a"), 0);

        clock.increment("agent_a");
        assert_eq!(clock.get("agent_a"), 1);

        clock.increment("agent_a");
        assert_eq!(clock.get("agent_a"), 2);
    }

    #[test]
    fn test_merge() {
        let mut clock_a = VectorClock::new();
        clock_a.increment("agent_a");
        clock_a.increment("agent_a");

        let mut clock_b = VectorClock::new();
        clock_b.increment("agent_a");
        clock_b.increment("agent_b");

        let mut merged = clock_a.clone();
        merged.merge(&clock_b);

        assert_eq!(merged.get("agent_a"), 2); // max(2, 1)
        assert_eq!(merged.get("agent_b"), 1);
    }

    #[test]
    fn test_dominance() {
        let mut clock_a = VectorClock::new();
        clock_a.increment("agent_a");
        clock_a.increment("agent_b");

        let mut clock_b = VectorClock::new();
        clock_b.increment("agent_a");

        assert!(clock_a.dominates(&clock_b));
        assert!(!clock_b.dominates(&clock_a));
    }

    #[test]
    fn test_concurrent() {
        let mut clock_a = VectorClock::new();
        clock_a.increment("agent_a");

        let mut clock_b = VectorClock::new();
        clock_b.increment("agent_b");

        assert!(clock_a.is_concurrent_with(&clock_b));
        assert!(clock_a.conflicts_with(&clock_b));
    }
}
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
// commit 4 1788294953745901815
// commit 28 1788294954099204680
