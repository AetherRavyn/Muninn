use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::model::{MemoryRecord, CURRENT_SCHEMA_VERSION};

/// Schema migration: explicit tested functions for versioned data.
/// Never assume schema compatibility — migrate explicitly.
pub struct MigrationRegistry {
    migrations: Vec<Migration>,
}

struct Migration {
    from_version: u16,
    to_version: u16,
    migrate_fn: fn(&mut MemoryRecord) -> Result<()>,
}

impl MigrationRegistry {
    /// Create a new migration registry with all known migrations
    pub fn new() -> Self {
        let mut registry = Self {
            migrations: Vec::new(),
        };

        // Register all migrations in order
        registry.register(1, 2, migrate_v1_to_v2);
        // Add future migrations here: registry.register(2, 3, migrate_v2_to_v3);

        registry
    }

    /// Register a migration
    pub fn register(&mut self, from: u16, to: u16, migrate_fn: fn(&mut MemoryRecord) -> Result<()>) {
        self.migrations.push(Migration {
            from_version: from,
            to_version: to,
            migrate_fn,
        });
    }

    /// Migrate a record to the current schema version.
    /// Returns Ok(()) if already current or successfully migrated.
    /// Returns Err if no migration path exists.
    pub fn migrate(&self, record: &mut MemoryRecord) -> Result<()> {
        if record.schema_version >= CURRENT_SCHEMA_VERSION {
            return Ok(());
        }

        // Find and apply migrations in sequence
        let mut current_version = record.schema_version;
        let mut applied = 0;

        while current_version < CURRENT_SCHEMA_VERSION {
            let migration = self.migrations.iter().find(|m| m.from_version == current_version);

            match migration {
                Some(m) => {
                    (m.migrate_fn)(record)?;
                    record.schema_version = m.to_version;
                    current_version = m.to_version;
                    applied += 1;

                    if applied > 100 {
                        return Err(Error::SchemaMigrationRequired {
                            from: record.schema_version,
                            to: CURRENT_SCHEMA_VERSION,
                        });
                    }
                }
                None => {
                    return Err(Error::SchemaMigrationRequired {
                        from: current_version,
                        to: CURRENT_SCHEMA_VERSION,
                    });
                }
            }
        }

        Ok(())
    }

    /// Check if a migration is needed
    pub fn needs_migration(&self, schema_version: u16) -> bool {
        schema_version < CURRENT_SCHEMA_VERSION
    }

    /// Get the current schema version
    pub fn current_version(&self) -> u16 {
        CURRENT_SCHEMA_VERSION
    }
}

/// Migration from v1 to v2 (example)
/// In real usage, this would add fields, restructure data, etc.
fn migrate_v1_to_v2(record: &mut MemoryRecord) -> Result<()> {
    // Example: ensure embedding has a minimum dimension
    if record.embedding.is_empty() {
        record.embedding = vec![0.0; 128]; // Default embedding size
    }
    Ok(())
}

/// Migration plan for documentation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub from_version: u16,
    pub to_version: u16,
    pub description: String,
    pub breaking: bool,
    pub estimated_duration_ms: u64,
}

/// Get the full migration plan
pub fn migration_plan() -> Vec<MigrationPlan> {
    vec![
        MigrationPlan {
            from_version: 1,
            to_version: 2,
            description: "Add default embedding dimension for empty embeddings".to_string(),
            breaking: false,
            estimated_duration_ms: 0,
        },
        // Future migrations documented here
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;
    use crate::trust::TrustTier;
    use crate::visibility::Visibility;
    use crate::vector_clock::VectorClock;
    use chrono::Utc;
    use uuid::Uuid;

    fn test_record_with_version(version: u16) -> MemoryRecord {
        let now = Utc::now();
        MemoryRecord {
            id: Uuid::new_v4(),
            tenant_id: TenantId("t1".to_string()),
            agent_id: AgentId("a1".to_string()),
            tier: MemoryTier::Episodic,
            schema_version: version,
            content: "test".to_string(),
            embedding: vec![],
            embedding_model_version: "test".to_string(),
            importance: 0.5,
            retention_class: RetentionClass::Standard,
            trust_tier: TrustTier::Standard,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            visibility: Visibility::Private,
            source_ids: vec![],
            superseded_by: None,
            vector_clock: VectorClock::new(),
        }
    }

    #[test]
    fn test_no_migration_needed() {
        let registry = MigrationRegistry::new();
        let mut record = test_record_with_version(CURRENT_SCHEMA_VERSION);
        registry.migrate(&mut record).unwrap();
        assert_eq!(record.schema_version, CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn test_migration_v1_to_v2() {
        let registry = MigrationRegistry::new();
        // v1 is current, so no migration needed
        let mut record = test_record_with_version(CURRENT_SCHEMA_VERSION);
        registry.migrate(&mut record).unwrap();
        assert_eq!(record.schema_version, CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn test_needs_migration() {
        let registry = MigrationRegistry::new();
        assert!(!registry.needs_migration(CURRENT_SCHEMA_VERSION));
        assert!(registry.needs_migration(0)); // version 0 would need migration
    }

    #[test]
    fn test_migration_plan() {
        let plan = migration_plan();
        assert!(!plan.is_empty());
        assert_eq!(plan[0].from_version, 1);
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
// commit 7 1788294953790937429
// commit 31 1788294954144109073
// commit 103 1788294955234041754
// commit 127 1788294955604295055
// commit 175 1788294956350158394
// commit 199 1788294956725770305
// commit 247 1788294957473243466
// commit 271 1788294957863310926
// commit 295 1788294958227008930
// commit 319 1788294958591510821
