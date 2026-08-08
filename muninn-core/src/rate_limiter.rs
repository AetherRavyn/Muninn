use std::collections::HashMap;
use std::time::{Duration, Instant};
use parking_lot::RwLock;

use crate::error::{Error, Result};
use crate::model::{TenantId, SourceRateLimit};

/// Token bucket rate limiter per tenant.
/// Enforces write rate limits and influence caps.
pub struct RateLimiter {
    buckets: RwLock<HashMap<String, TokenBucket>>,
    default_limit: SourceRateLimit,
}

struct TokenBucket {
    tokens: f64,
    max_tokens: f64,
    refill_rate: f64, // tokens per second
    last_refill: Instant,
    /// Rolling window for influence tracking
    influence_window: Vec<(Instant, f32)>,
    max_influence_per_hour: f32,
}

impl TokenBucket {
    fn new(limit: &SourceRateLimit) -> Self {
        Self {
            tokens: limit.max_writes_per_minute as f64,
            max_tokens: limit.max_writes_per_minute as f64,
            refill_rate: limit.max_writes_per_minute as f64 / 60.0,
            last_refill: Instant::now(),
            influence_window: Vec::new(),
            max_influence_per_hour: limit.max_influence_score_per_hour,
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.max_tokens);
        self.last_refill = now;
    }

    fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn check_influence(&mut self, influence: f32) -> bool {
        let now = Instant::now();
        let one_hour_ago = now - Duration::from_secs(3600);

        // Prune old entries
        self.influence_window.retain(|(t, _)| *t > one_hour_ago);

        let current_influence: f32 = self.influence_window.iter().map(|(_, i)| i).sum();
        current_influence + influence <= self.max_influence_per_hour
    }

    fn record_influence(&mut self, influence: f32) {
        self.influence_window.push((Instant::now(), influence));
    }
}

impl RateLimiter {
    /// Create a new rate limiter with default limits
    pub fn new(default_limit: SourceRateLimit) -> Self {
        Self {
            buckets: RwLock::new(HashMap::new()),
            default_limit,
        }
    }

    /// Check if a tenant can perform a write
    pub fn check_write(&self, tenant_id: &TenantId) -> Result<()> {
        let mut buckets = self.buckets.write();
        let bucket = buckets
            .entry(tenant_id.0.clone())
            .or_insert_with(|| TokenBucket::new(&self.default_limit));

        if bucket.try_consume() {
            Ok(())
        } else {
            Err(Error::RateLimited(format!(
                "Write rate limit exceeded for tenant {}",
                tenant_id
            )))
        }
    }

    /// Check if a tenant can influence shared memory
    pub fn check_influence(&self, tenant_id: &TenantId, influence: f32) -> Result<()> {
        let mut buckets = self.buckets.write();
        let bucket = buckets
            .entry(tenant_id.0.clone())
            .or_insert_with(|| TokenBucket::new(&self.default_limit));

        if bucket.check_influence(influence) {
            Ok(())
        } else {
            Err(Error::QuotaExceeded {
                resource: "shared_memory_influence".to_string(),
                limit: self.default_limit.max_influence_score_per_hour as u64,
                tenant_id: tenant_id.0.clone(),
            })
        }
    }

    /// Record influence usage after a successful write
    pub fn record_influence(&self, tenant_id: &TenantId, influence: f32) {
        let mut buckets = self.buckets.write();
        if let Some(bucket) = buckets.get_mut(&tenant_id.0) {
            bucket.record_influence(influence);
        }
    }

    /// Get remaining write capacity for a tenant
    pub fn remaining_writes(&self, tenant_id: &TenantId) -> f64 {
        let mut buckets = self.buckets.write();
        let bucket = buckets
            .entry(tenant_id.0.clone())
            .or_insert_with(|| TokenBucket::new(&self.default_limit));
        bucket.refill();
        bucket.tokens
    }

    /// Reset a tenant's rate limit state
    pub fn reset(&self, tenant_id: &TenantId) {
        let mut buckets = self.buckets.write();
        buckets.remove(&tenant_id.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_limit() -> SourceRateLimit {
        SourceRateLimit {
            max_writes_per_minute: 10,
            max_bytes_per_minute: 1024 * 1024,
            max_influence_score_per_hour: 50.0,
        }
    }

    #[test]
    fn test_rate_limit_allows_writes() {
        let limiter = RateLimiter::new(default_limit());
        let tenant = TenantId("t1".to_string());

        // Should allow initial writes
        for _ in 0..10 {
            assert!(limiter.check_write(&tenant).is_ok());
        }
    }

    #[test]
    fn test_rate_limit_blocks_after_exhaustion() {
        let limiter = RateLimiter::new(default_limit());
        let tenant = TenantId("t1".to_string());

        // Exhaust the bucket
        for _ in 0..10 {
            limiter.check_write(&tenant).unwrap();
        }

        // Should now be blocked
        assert!(limiter.check_write(&tenant).is_err());
    }

    #[test]
    fn test_influence_limit() {
        let limiter = RateLimiter::new(default_limit());
        let tenant = TenantId("t1".to_string());

        // Should allow influence up to limit
        assert!(limiter.check_influence(&tenant, 30.0).is_ok());
        limiter.record_influence(&tenant, 30.0);

        // Should block when exceeding
        assert!(limiter.check_influence(&tenant, 30.0).is_err());
    }

    #[test]
    fn test_tenant_isolation() {
        let limiter = RateLimiter::new(default_limit());
        let t1 = TenantId("t1".to_string());
        let t2 = TenantId("t2".to_string());

        // Exhaust t1
        for _ in 0..10 {
            limiter.check_write(&t1).unwrap();
        }
        assert!(limiter.check_write(&t1).is_err());

        // t2 should still be allowed
        assert!(limiter.check_write(&t2).is_ok());
    }

    #[test]
    fn test_reset() {
        let limiter = RateLimiter::new(default_limit());
        let tenant = TenantId("t1".to_string());

        for _ in 0..10 {
            limiter.check_write(&tenant).unwrap();
        }
        assert!(limiter.check_write(&tenant).is_err());

        limiter.reset(&tenant);
        assert!(limiter.check_write(&tenant).is_ok());
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
// commit 55 1788294954498454934
// commit 79 1788294954866010764
