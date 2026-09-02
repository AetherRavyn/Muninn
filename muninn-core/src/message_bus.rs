use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::warn;

use crate::error::Result;
use crate::model::*;

/// Event types for the message bus
#[derive(Debug, Clone)]
pub enum MemoryEvent {
    /// A record was written
    RecordWritten {
        record_id: uuid::Uuid,
        tenant_id: TenantId,
        agent_id: AgentId,
        tier: MemoryTier,
        visibility: String,
    },
    /// A record was superseded
    RecordSuperseded {
        record_id: uuid::Uuid,
        superseded_by: uuid::Uuid,
        tenant_id: TenantId,
    },
    /// Shared memory was updated
    SharedMemoryUpdated {
        tenant_id: TenantId,
        agent_id: AgentId,
        record_id: uuid::Uuid,
    },
    /// Consolidation produced a fact
    FactConsolidated {
        tenant_id: TenantId,
        fact_id: uuid::Uuid,
        source_episode_ids: Vec<uuid::Uuid>,
    },
    /// Anomaly detected
    AnomalyDetected {
        tenant_id: TenantId,
        anomaly_type: String,
        severity: String,
    },
    /// Tenant purged
    TenantPurged {
        tenant_id: TenantId,
        records_purged: usize,
    },
}

/// Message bus for inter-agent communication.
/// Uses broadcast channels for pub/sub pattern.
pub struct MessageBus {
    /// Per-tenant event channels
    tenant_channels: RwLock<HashMap<String, broadcast::Sender<MemoryEvent>>>,
    /// Global event channel for system-wide events
    global_channel: broadcast::Sender<MemoryEvent>,
    /// Maximum channel capacity
    max_capacity: usize,
}

impl MessageBus {
    /// Create a new message bus
    pub fn new(max_capacity: usize) -> Self {
        let (global_channel, _) = broadcast::channel(max_capacity);

        Self {
            tenant_channels: RwLock::new(HashMap::new()),
            global_channel,
            max_capacity,
        }
    }

    /// Publish an event to the message bus
    pub fn publish(&self, event: MemoryEvent) -> Result<()> {
        // Get tenant_id from event
        let tenant_id = match &event {
            MemoryEvent::RecordWritten { tenant_id, .. } => tenant_id.0.clone(),
            MemoryEvent::RecordSuperseded { tenant_id, .. } => tenant_id.0.clone(),
            MemoryEvent::SharedMemoryUpdated { tenant_id, .. } => tenant_id.0.clone(),
            MemoryEvent::FactConsolidated { tenant_id, .. } => tenant_id.0.clone(),
            MemoryEvent::AnomalyDetected { tenant_id, .. } => tenant_id.0.clone(),
            MemoryEvent::TenantPurged { tenant_id, .. } => tenant_id.0.clone(),
        };

        // Publish to tenant channel
        let channels = self.tenant_channels.read();
        if let Some(sender) = channels.get(&tenant_id) {
            let _ = sender.send(event.clone()); // Ignore if no subscribers
        }
        drop(channels);

        // Publish to global channel
        let _ = self.global_channel.send(event);

        Ok(())
    }

    /// Subscribe to events for a specific tenant
    pub fn subscribe_tenant(&self, tenant_id: &str) -> broadcast::Receiver<MemoryEvent> {
        let mut channels = self.tenant_channels.write();
        let sender = channels
            .entry(tenant_id.to_string())
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(self.max_capacity);
                sender
            });
        sender.subscribe()
    }

    /// Subscribe to all global events
    pub fn subscribe_global(&self) -> broadcast::Receiver<MemoryEvent> {
        self.global_channel.subscribe()
    }

    /// Get the number of subscribers for a tenant
    pub fn subscriber_count(&self, tenant_id: &str) -> usize {
        let channels = self.tenant_channels.read();
        channels
            .get(tenant_id)
            .map(|s| s.receiver_count())
            .unwrap_or(0)
    }

    /// Get global subscriber count
    pub fn global_subscriber_count(&self) -> usize {
        self.global_channel.receiver_count()
    }
}

impl Default for MessageBus {
    fn default() -> Self {
        Self::new(10000)
    }
}

/// Event listener trait for processing events
#[async_trait::async_trait]
pub trait EventListener: Send + Sync {
    /// Handle a memory event
    async fn handle_event(&self, event: &MemoryEvent) -> Result<()>;
}

/// Event processor that dispatches events to listeners
pub struct EventProcessor {
    listeners: RwLock<Vec<Arc<dyn EventListener>>>,
}

impl EventProcessor {
    pub fn new() -> Self {
        Self {
            listeners: RwLock::new(Vec::new()),
        }
    }

    /// Register a listener
    pub fn register(&self, listener: Arc<dyn EventListener>) {
        self.listeners.write().push(listener);
    }

    /// Process an event through all listeners
    pub async fn process(&self, event: &MemoryEvent) -> Result<()> {
        let listeners = self.listeners.read().clone();

        for listener in listeners {
            if let Err(e) = listener.handle_event(event).await {
                warn!("Event listener error: {}", e);
            }
        }

        Ok(())
    }
}

impl Default for EventProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_bus_publish_subscribe() {
        let bus = MessageBus::new(100);
        let mut subscriber = bus.subscribe_tenant("t1");

        let event = MemoryEvent::RecordWritten {
            record_id: uuid::Uuid::new_v4(),
            tenant_id: TenantId("t1".to_string()),
            agent_id: AgentId("a1".to_string()),
            tier: MemoryTier::Episodic,
            visibility: "private".to_string(),
        };

        bus.publish(event.clone()).unwrap();

        // Should receive the event
        let received = subscriber.try_recv();
        assert!(received.is_ok());
    }

    #[test]
    fn test_message_bus_tenant_isolation() {
        let bus = MessageBus::new(100);
        let mut t1_sub = bus.subscribe_tenant("t1");
        let mut t2_sub = bus.subscribe_tenant("t2");

        // Publish to t1
        let event = MemoryEvent::RecordWritten {
            record_id: uuid::Uuid::new_v4(),
            tenant_id: TenantId("t1".to_string()),
            agent_id: AgentId("a1".to_string()),
            tier: MemoryTier::Episodic,
            visibility: "private".to_string(),
        };
        bus.publish(event).unwrap();

        // t1 should receive, t2 should not
        assert!(t1_sub.try_recv().is_ok());
        assert!(t2_sub.try_recv().is_err());
    }

    #[test]
    fn test_message_bus_global_subscription() {
        let bus = MessageBus::new(100);
        let mut global_sub = bus.subscribe_global();

        let event = MemoryEvent::RecordWritten {
            record_id: uuid::Uuid::new_v4(),
            tenant_id: TenantId("t1".to_string()),
            agent_id: AgentId("a1".to_string()),
            tier: MemoryTier::Episodic,
            visibility: "private".to_string(),
        };

        bus.publish(event).unwrap();

        // Global subscriber should receive all events
        assert!(global_sub.try_recv().is_ok());
    }

    #[test]
    fn test_message_bus_capacity() {
        let bus = MessageBus::new(2); // Small capacity

        // Publish multiple events
        for _ in 0..5 {
            let event = MemoryEvent::RecordWritten {
                record_id: uuid::Uuid::new_v4(),
                tenant_id: TenantId("t1".to_string()),
                agent_id: AgentId("a1".to_string()),
                tier: MemoryTier::Episodic,
                visibility: "private".to_string(),
            };
            bus.publish(event).unwrap();
        }

        // Channel should handle overflow gracefully
        assert_eq!(bus.subscriber_count("t1"), 0);
    }
}
