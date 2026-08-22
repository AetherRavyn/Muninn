use thiserror::Error;

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Internal(format!("IO error: {}", e))
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serialization(format!("JSON error: {}", e))
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::Serialization(format!("Bincode error: {}", e))
    }
}

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("embedding error: {0}")]
    Embedding(String),

    #[error("visibility violation: agent {agent_id} cannot access record owned by {owner_id}")]
    VisibilityViolation { agent_id: String, owner_id: String },

    #[error("tenant isolation breach: tenant {requester} cannot access tenant {target}")]
    TenantIsolationBreach { requester: String, target: String },

    #[error("authorization denied: {reason}")]
    AuthorizationDenied { reason: String },

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("capacity exceeded: tier {tier} at capacity for agent {agent_id}")]
    CapacityExceeded { tier: String, agent_id: String },

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("quota exceeded: {resource} limit {limit} for tenant {tenant_id}")]
    QuotaExceeded {
        resource: String,
        limit: u64,
        tenant_id: String,
    },

    #[error("consolidation error: {0}")]
    Consolidation(String),

    #[error("circuit breaker open for {service}")]
    CircuitBreakerOpen { service: String },

    #[error("rate limited: {0}")]
    RateLimited(String),

    #[error("schema migration required: from {from} to {to}")]
    SchemaMigrationRequired { from: u16, to: u16 },

    #[error("internal error: {0}")]
    Internal(String),

    #[error("precondition failed: {0}")]
    PreconditionFailed(String),
}

pub type Result<T> = std::result::Result<T, Error>;
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
// commit 26 1788294954070741267
// commit 242 1788294957396389231
// commit 266 1788294957782960537
// commit 290 1788294958152860832
