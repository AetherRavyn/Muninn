use serde::{Deserialize, Serialize};

/// Trust tiers for memory records — gatekeeper for what gets promoted
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TrustTier {
    /// From an authenticated, known-good agent action
    Verified,
    /// Normal agent-to-agent traffic
    Standard,
    /// Content sourced from external input — web fetches, third-party APIs,
    /// another tenant's shared data, anything an attacker could have influenced
    Untrusted,
}

impl TrustTier {
    /// Can this trust tier be auto-promoted to semantic memory?
    /// Only Verified and Standard can be auto-promoted.
    /// Untrusted must go through quarantine + corroboration or explicit review.
    pub fn can_auto_promote(&self) -> bool {
        matches!(self, TrustTier::Verified | TrustTier::Standard)
    }

    /// Can this trust tier contribute to shared memory?
    pub fn can_write_shared(&self) -> bool {
        matches!(self, TrustTier::Verified | TrustTier::Standard)
    }

    /// Score multiplier for retrieval — lower trust = lower visibility in results
    pub fn retrieval_multiplier(&self) -> f32 {
        match self {
            TrustTier::Verified => 1.0,
            TrustTier::Standard => 0.9,
            TrustTier::Untrusted => 0.5,
        }
    }
}

impl std::fmt::Display for TrustTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrustTier::Verified => write!(f, "verified"),
            TrustTier::Standard => write!(f, "standard"),
            TrustTier::Untrusted => write!(f, "untrusted"),
        }
    }
}
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
// commit 3 1788294953731655293
// commit 51 1788294954436126230
