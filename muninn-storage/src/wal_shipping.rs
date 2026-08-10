use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{info, error};

use muninn_core::error::Result;

use crate::wal::WalEntry;

/// WAL shipping configuration
#[derive(Debug, Clone)]
pub struct WalShippingConfig {
    /// How often to ship WAL segments to replica
    pub ship_interval: Duration,
    /// Maximum WAL segment size before rotation
    pub max_segment_size: u64,
    /// Directory containing WAL segments to ship
    pub wal_dir: PathBuf,
    /// Remote replica endpoint (URL or path)
    pub replica_endpoint: String,
}

/// WAL shipper: ships WAL segments to an async standby replica.
/// Target: RPO ≤ 5 minutes (determined by ship interval).
pub struct WalShipper {
    config: WalShippingConfig,
    last_shipped_offset: Arc<RwLock<u64>>,
    shipper_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl WalShipper {
    /// Create a new WAL shipper
    pub fn new(config: WalShippingConfig) -> Self {
        Self {
            config,
            last_shipped_offset: Arc::new(RwLock::new(0)),
            shipper_handle: None,
            shutdown_tx: None,
        }
    }

    /// Start the background shipping task
    pub fn start(&mut self) {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        let ship_config = self.config.clone();
        let log_config = self.config.clone();
        let last_shipped = self.last_shipped_offset.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(ship_config.ship_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = ship_wal_segments(&ship_config, &last_shipped) {
                            error!("WAL shipping failed: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("WAL shipper shutting down");
                        // Final ship attempt
                        let _ = ship_wal_segments(&ship_config, &last_shipped);
                        break;
                    }
                }
            }
        });

        self.shipper_handle = Some(handle);
        info!("WAL shipper started with interval {:?}", log_config.ship_interval);
    }

    /// Stop the background shipping task
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
        if let Some(handle) = self.shipper_handle.take() {
            let _ = handle.await;
        }
        info!("WAL shipper stopped");
    }

    /// Get the last shipped offset
    pub fn last_shipped_offset(&self) -> u64 {
        *self.last_shipped_offset.read()
    }
}

/// Ship WAL segments to the replica
fn ship_wal_segments(
    config: &WalShippingConfig,
    last_shipped: &Arc<RwLock<u64>>,
) -> Result<()> {
    let wal_dir = &config.wal_dir;

    // List WAL segments
    let mut segments: Vec<(u64, PathBuf)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(wal_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(idx) = name_str
                .strip_prefix("wal_")
                .and_then(|s| s.strip_suffix(".log"))
            {
                if let Ok(index) = idx.parse::<u64>() {
                    segments.push((index, entry.path()));
                }
            }
        }
    }
    segments.sort_by_key(|(idx, _)| *idx);

    for (_idx, segment_path) in &segments {
        // Ship this segment
        ship_segment(segment_path, &config.replica_endpoint)?;

        // Update shipped offset
        let segment_size = std::fs::metadata(segment_path)
            .map(|m| m.len())
            .unwrap_or(0);
        let new_offset = *last_shipped.read() + segment_size;
        *last_shipped.write() = new_offset;
    }

    Ok(())
}

/// Ship a single WAL segment to the replica
fn ship_segment(segment_path: &Path, replica_endpoint: &str) -> Result<()> {
    // In production, this would send the segment over the network
    // For now, just log that we would ship it
    info!(
        "Shipping WAL segment {:?} to {}",
        segment_path.file_name().unwrap_or_default(),
        replica_endpoint
    );

    // TODO: Implement actual network shipping
    // Options:
    // 1. gRPC streaming of WAL entries
    // 2. rsync of segment files
    // 3. S3 upload for cloud replicas

    Ok(())
}

/// WAL replay client for replicas
pub struct WalReplayClient {
    endpoint: String,
}

impl WalReplayClient {
    pub fn new(endpoint: String) -> Self {
        Self { endpoint }
    }

    /// Replay WAL entries from a given offset
    pub async fn replay_from(&self, offset: u64) -> Result<Vec<WalEntry>> {
        // In production, this would fetch entries from the primary
        // For now, return empty
        info!(
            "Replaying WAL from offset {} via {}",
            offset, self.endpoint
        );
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shipper_creation() {
        let config = WalShippingConfig {
            ship_interval: Duration::from_secs(1),
            max_segment_size: 1024 * 1024,
            wal_dir: PathBuf::from("/tmp/test_wal"),
            replica_endpoint: "localhost:50052".to_string(),
        };

        let mut shipper = WalShipper::new(config);
        assert_eq!(shipper.last_shipped_offset(), 0);
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
// commit 107 1788294955292617053
