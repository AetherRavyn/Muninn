use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock;
use std::collections::HashMap;

/// Simple Prometheus-compatible metrics collector.
/// In production, use `prometheus` crate directly or OpenTelemetry.
pub struct MetricsCollector {
    counters: RwLock<HashMap<String, AtomicU64>>,
    gauges: RwLock<HashMap<String, AtomicU64>>,
    histograms: RwLock<HashMap<String, Vec<f64>>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }

    /// Increment a counter
    pub fn increment_counter(&self, name: &str, value: u64) {
        let counters = self.counters.read();
        if let Some(counter) = counters.get(name) {
            counter.fetch_add(value, Ordering::Relaxed);
        } else {
            drop(counters);
            let mut counters = self.counters.write();
            let counter = counters
                .entry(name.to_string())
                .or_insert_with(|| AtomicU64::new(0));
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Set a gauge value
    pub fn set_gauge(&self, name: &str, value: u64) {
        let gauges = self.gauges.read();
        if let Some(gauge) = gauges.get(name) {
            gauge.store(value, Ordering::Relaxed);
        } else {
            drop(gauges);
            let mut gauges = self.gauges.write();
            let gauge = gauges
                .entry(name.to_string())
                .or_insert_with(|| AtomicU64::new(0));
            gauge.store(value, Ordering::Relaxed);
        }
    }

    /// Record a histogram value
    pub fn record_histogram(&self, name: &str, value: f64) {
        let mut histograms = self.histograms.write();
        histograms
            .entry(name.to_string())
            .or_insert_with(Vec::new)
            .push(value);
    }

    /// Get counter value
    pub fn get_counter(&self, name: &str) -> u64 {
        self.counters
            .read()
            .get(name)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get gauge value
    pub fn get_gauge(&self, name: &str) -> u64 {
        self.gauges
            .read()
            .get(name)
            .map(|g| g.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Export all metrics in Prometheus text format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        // Counters
        for (name, counter) in self.counters.read().iter() {
            output.push_str(&format!(
                "# TYPE muninn_{} counter\nmuninn_{} {}\n",
                name,
                name,
                counter.load(Ordering::Relaxed)
            ));
        }

        // Gauges
        for (name, gauge) in self.gauges.read().iter() {
            output.push_str(&format!(
                "# TYPE muninn_{} gauge\nmuninn_{} {}\n",
                name,
                name,
                gauge.load(Ordering::Relaxed)
            ));
        }

        // Histograms
        for (name, values) in self.histograms.read().iter() {
            if values.is_empty() {
                continue;
            }
            let mut sorted = values.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            let sum: f64 = sorted.iter().sum();
            let count = sorted.len();
            let _min = sorted.first().unwrap();
            let max = sorted.last().unwrap();
            let _mean = sum / count as f64;

            output.push_str(&format!(
                "# TYPE muninn_{} histogram\nmuninn_{}{{quantile=\"0.5\"}} {}\nmuninn_{}{{quantile=\"0.99\"}} {}\nmuninn_{}{{quantile=\"1.0\"}} {}\nmuninn_{}_sum {}\nmuninn_{}_count {}\n",
                name, name, sorted[count * 99 / 100],
                name, sorted[count * 99 / 100],
                name, max,
                name, sum,
                name, count
            ));
        }

        output
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Pre-defined metric names for Muninn
pub mod metric_names {
    pub const WRITE_REQUESTS: &str = "write_requests_total";
    pub const WRITE_ERRORS: &str = "write_errors_total";
    pub const READ_REQUESTS: &str = "read_requests_total";
    pub const READ_ERRORS: &str = "read_errors_total";
    pub const READ_LATENCY_MS: &str = "read_latency_ms";
    pub const WRITE_LATENCY_MS: &str = "write_latency_ms";
    pub const RECORDS_TOTAL: &str = "records_total";
    pub const RECORDS_BY_TIER: &str = "records_by_tier";
    pub const CONSOLIDATION_JOBS: &str = "consolidation_jobs_total";
    pub const QUARANTINED_FACTS: &str = "quarantined_facts_total";
    pub const TENANT_PURGES: &str = "tenant_purges_total";
    pub const LINEAGE_TRACES: &str = "lineage_traces_total";
    pub const WAL_BYTES: &str = "wal_bytes_total";
    pub const VECTOR_INDEX_SIZE: &str = "vector_index_size";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_increment() {
        let metrics = MetricsCollector::new();
        metrics.increment_counter("test_counter", 5);
        metrics.increment_counter("test_counter", 3);
        assert_eq!(metrics.get_counter("test_counter"), 8);
    }

    #[test]
    fn test_gauge() {
        let metrics = MetricsCollector::new();
        metrics.set_gauge("test_gauge", 42);
        assert_eq!(metrics.get_gauge("test_gauge"), 42);
        metrics.set_gauge("test_gauge", 100);
        assert_eq!(metrics.get_gauge("test_gauge"), 100);
    }

    #[test]
    fn test_histogram() {
        let metrics = MetricsCollector::new();
        metrics.record_histogram("test_hist", 1.0);
        metrics.record_histogram("test_hist", 2.0);
        metrics.record_histogram("test_hist", 3.0);

        let prometheus = metrics.export_prometheus();
        assert!(prometheus.contains("muninn_test_hist"));
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = MetricsCollector::new();
        metrics.increment_counter("requests", 100);
        metrics.set_gauge("connections", 5);

        let output = metrics.export_prometheus();
        assert!(output.contains("muninn_requests 100"));
        assert!(output.contains("muninn_connections 5"));
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
// commit 78 1788294954851252158
// commit 102 1788294955217386689
// commit 270 1788294957847202454
// commit 318 1788294958576249943
