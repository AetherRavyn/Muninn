use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::ObservabilityConfig;

/// Initialize the tracing subscriber with structured JSON logging.
pub fn init_tracing(config: &ObservabilityConfig) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    let fmt_layer = fmt::layer()
        .json()
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();
}

/// Create a span for a memory operation
pub fn memory_span(operation: &str, tenant_id: &str, agent_id: &str) -> tracing::Span {
    tracing::info_span!(
        "memory_operation",
        operation = operation,
        tenant_id = tenant_id,
        agent_id = agent_id,
    )
}

/// Create a span for retrieval
pub fn retrieval_span(tenant_id: &str, agent_id: &str, query_len: usize) -> tracing::Span {
    tracing::info_span!(
        "retrieval",
        tenant_id = tenant_id,
        agent_id = agent_id,
        query_length = query_len,
    )
}

/// Record a metric as a tracing event (for correlation)
pub fn record_metric(name: &str, value: f64, labels: &[(&str, &str)]) {
    tracing::info!(
        metric_name = name,
        metric_value = value,
        labels = ?labels,
        "metric_recorded"
    );
}
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
// commit 8 1788294953805229080
// commit 32 1788294954157979336
// commit 248 1788294957488518592
// commit 296 1788294958241341031
// commit 416 1788294960095973911
