use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::Mutex;

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation
    Closed,
    /// Failing, rejecting requests
    Open,
    /// Testing if service recovered
    HalfOpen,
}

/// Circuit breaker for upstream service calls.
/// Prevents cascading failures when embedding provider or other services are down.
pub struct CircuitBreaker {
    state: Arc<Mutex<CircuitState>>,
    failure_count: AtomicU64,
    success_count: AtomicU64,
    last_failure: Arc<Mutex<Option<Instant>>>,
    failure_threshold: u64,
    success_threshold: u64,
    recovery_timeout: Duration,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(
        failure_threshold: u64,
        success_threshold: u64,
        recovery_timeout: Duration,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(CircuitState::Closed)),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            last_failure: Arc::new(Mutex::new(None)),
            failure_threshold,
            success_threshold,
            recovery_timeout,
        }
    }

    /// Check if a request is allowed
    pub fn is_allowed(&self) -> bool {
        let state = self.state.lock();

        match *state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if recovery timeout has elapsed
                let last_failure = self.last_failure.lock();
                if let Some(last) = *last_failure {
                    if last.elapsed() >= self.recovery_timeout {
                        drop(last_failure);
                        drop(state);
                        // Transition to HalfOpen
                        let mut state = self.state.lock();
                        *state = CircuitState::HalfOpen;
                        self.success_count.store(0, Ordering::SeqCst);
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true, // Allow one request to test
        }
    }

    /// Record a success
    pub fn record_success(&self) {
        let state = self.state.lock();
        match *state {
            CircuitState::Closed => {
                self.failure_count.store(0, Ordering::SeqCst);
            }
            CircuitState::HalfOpen => {
                let successes = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;
                if successes >= self.success_threshold {
                    drop(state);
                    let mut state = self.state.lock();
                    *state = CircuitState::Closed;
                    self.failure_count.store(0, Ordering::SeqCst);
                }
            }
            CircuitState::Open => {}
        }
    }

    /// Record a failure
    pub fn record_failure(&self) {
        let mut state = self.state.lock();
        match *state {
            CircuitState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                if failures >= self.failure_threshold {
                    *state = CircuitState::Open;
                    *self.last_failure.lock() = Some(Instant::now());
                }
            }
            CircuitState::HalfOpen => {
                *state = CircuitState::Open;
                *self.last_failure.lock() = Some(Instant::now());
            }
            CircuitState::Open => {}
        }
    }

    /// Get current state
    pub fn state(&self) -> CircuitState {
        *self.state.lock()
    }

    /// Reset the circuit breaker
    pub fn reset(&self) {
        let mut state = self.state.lock();
        *state = CircuitState::Closed;
        self.failure_count.store(0, Ordering::SeqCst);
        self.success_count.store(0, Ordering::SeqCst);
    }
}

/// Retry with exponential backoff and jitter
pub async fn retry_with_jitter<F, Fut, T, E>(
    mut operation: F,
    max_retries: u32,
    base_delay: Duration,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut last_error = None;

    for attempt in 0..=max_retries {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                last_error = Some(e);
                if attempt < max_retries {
                    let delay = base_delay * 2u32.pow(attempt);
                    let jitter = Duration::from_millis(rand::random::<u64>() % 100);
                    tokio::time::sleep(delay + jitter).await;
                }
            }
        }
    }

    Err(last_error.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_closed() {
        let cb = CircuitBreaker::new(3, 2, Duration::from_secs(1));
        assert!(cb.is_allowed());
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_opens_after_failures() {
        let cb = CircuitBreaker::new(3, 2, Duration::from_secs(10));
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_allowed()); // Still closed, 2 < 3
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_half_open() {
        let cb = CircuitBreaker::new(2, 2, Duration::from_millis(10));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for recovery timeout
        std::thread::sleep(Duration::from_millis(20));
        assert!(cb.is_allowed());
        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_recovery() {
        let cb = CircuitBreaker::new(2, 2, Duration::from_millis(10));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        std::thread::sleep(Duration::from_millis(20));
        cb.is_allowed(); // Transition to HalfOpen
        cb.record_success();
        cb.record_success(); // 2 successes, should close
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let cb = CircuitBreaker::new(2, 2, Duration::from_secs(10));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_retry_with_jitter() {
        let mut attempt = 0;
        let result = retry_with_jitter(
            || {
                attempt += 1;
                async move {
                    if attempt < 3 {
                        Err("not yet")
                    } else {
                        Ok("success")
                    }
                }
            },
            5,
            Duration::from_millis(1),
        )
        .await;

        assert_eq!(result.unwrap(), "success");
        assert_eq!(attempt, 3);
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
// commit 150 1788294955973496505
