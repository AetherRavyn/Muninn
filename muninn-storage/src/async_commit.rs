use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use muninn_core::error::Result;

/// Async commit manager for batching and background commits.
/// Separates the write path (fast, in-memory) from the commit path (slow, disk I/O).
pub struct AsyncCommitManager {
    commit_tx: mpsc::Sender<CommitRequest>,
    stats: Arc<CommitStats>,
}

struct CommitRequest {
    data: Vec<u8>,
    response_tx: oneshot::Sender<Result<()>>,
}

struct CommitStats {
    total_commits: std::sync::atomic::AtomicU64,
    total_bytes: std::sync::atomic::AtomicU64,
    last_commit_time: Mutex<Option<Instant>>,
    avg_commit_duration_ms: Mutex<f64>,
}

impl CommitStats {
    fn new() -> Self {
        Self {
            total_commits: std::sync::atomic::AtomicU64::new(0),
            total_bytes: std::sync::atomic::AtomicU64::new(0),
            last_commit_time: Mutex::new(None),
            avg_commit_duration_ms: Mutex::new(0.0),
        }
    }

    fn record_commit(&self, bytes: u64, duration: Duration) {
        self.total_commits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.total_bytes.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        *self.last_commit_time.lock() = Some(Instant::now());

        // Update running average
        let mut avg = self.avg_commit_duration_ms.lock();
        let duration_ms = duration.as_secs_f64() * 1000.0;
        *avg = (*avg * 0.9) + (duration_ms * 0.1); // Exponential moving average
    }
}

impl AsyncCommitManager {
    /// Create a new async commit manager
    pub fn new(
        batch_interval: Duration,
        max_batch_size: usize,
    ) -> Self {
        let (commit_tx, commit_rx) = mpsc::channel(1000);
        let stats = Arc::new(CommitStats::new());

        let stats_clone = stats.clone();
        tokio::spawn(async move {
            Self::commit_worker(commit_rx, batch_interval, max_batch_size, stats_clone).await;
        });

        Self { commit_tx, stats }
    }

    /// Submit data for async commit
    pub async fn commit(&self, data: Vec<u8>) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.commit_tx
            .send(CommitRequest { data, response_tx })
            .await
            .map_err(|_| muninn_core::error::Error::Internal("Commit channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| muninn_core::error::Error::Internal("Commit response channel closed".to_string()))?
    }

    /// Get commit statistics
    pub fn stats(&self) -> CommitStatsSnapshot {
        CommitStatsSnapshot {
            total_commits: self.stats.total_commits.load(std::sync::atomic::Ordering::Relaxed),
            total_bytes: self.stats.total_bytes.load(std::sync::atomic::Ordering::Relaxed),
            last_commit_time: *self.stats.last_commit_time.lock(),
            avg_commit_duration_ms: *self.stats.avg_commit_duration_ms.lock(),
        }
    }

    /// Background worker that batches and commits
    async fn commit_worker(
        mut rx: mpsc::Receiver<CommitRequest>,
        batch_interval: Duration,
        max_batch_size: usize,
        stats: Arc<CommitStats>,
    ) {
        let mut batch: Vec<CommitRequest> = Vec::new();
        let mut interval = tokio::time::interval(batch_interval);
        let mut total_buffered_bytes = 0usize;

        loop {
            tokio::select! {
                Some(request) = rx.recv() => {
                    total_buffered_bytes += request.data.len();
                    batch.push(request);

                    // Commit if batch is full
                    if batch.len() >= max_batch_size || total_buffered_bytes >= 1024 * 1024 {
                        let start = Instant::now();
                        Self::do_commit(&mut batch, &stats).await;
                        stats.record_commit(total_buffered_bytes as u64, start.elapsed());
                        total_buffered_bytes = 0;
                    }
                }
                _ = interval.tick() => {
                    // Periodic commit even if batch isn't full
                    if !batch.is_empty() {
                        let start = Instant::now();
                        Self::do_commit(&mut batch, &stats).await;
                        stats.record_commit(total_buffered_bytes as u64, start.elapsed());
                        total_buffered_bytes = 0;
                    }
                }
                else => {
                    // Channel closed, commit remaining
                    if !batch.is_empty() {
                        Self::do_commit(&mut batch, &stats).await;
                    }
                    break;
                }
            }
        }
    }

    /// Execute the actual commit
    async fn do_commit(batch: &mut Vec<CommitRequest>, _stats: &CommitStats) {
        let requests: Vec<CommitRequest> = batch.drain(..).collect();

        // In production, this would write to WAL and index
        // For now, simulate the commit
        for request in requests {
            let _ = request.response_tx.send(Ok(()));
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommitStatsSnapshot {
    pub total_commits: u64,
    pub total_bytes: u64,
    pub last_commit_time: Option<Instant>,
    pub avg_commit_duration_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_async_commit_basic() {
        let manager = AsyncCommitManager::new(
            Duration::from_millis(50),
            100,
        );

        // Submit some commits
        for i in 0..10 {
            let data = format!("commit {}", i).into_bytes();
            manager.commit(data).await.unwrap();
        }

        // Wait for batch to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        let stats = manager.stats();
        assert!(stats.total_commits > 0);
    }

    #[tokio::test]
    async fn test_async_commit_batch_size() {
        let manager = AsyncCommitManager::new(
            Duration::from_secs(10), // Long interval to test batch size
            5,                        // Small batch size
        );

        // Submit exactly batch_size requests
        for i in 0..5 {
            let data = format!("commit {}", i).into_bytes();
            manager.commit(data).await.unwrap();
        }

        // Wait a bit
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stats = manager.stats();
        assert!(stats.total_commits >= 1, "Should have committed at least once");
    }
}
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
// commit 155 1788294956048713118
// commit 203 1788294956789282644
// commit 251 1788294957534421498
// commit 275 1788294957927130947
