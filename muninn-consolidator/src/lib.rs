use async_trait::async_trait;
use tracing::{info, warn};

use muninn_core::error::Result;
use muninn_core::lineage::LineageTracker;
use muninn_core::model::*;
use muninn_core::traits::*;
use muninn_core::trust::TrustTier;

/// Default consolidator — distills episodic memories into semantic facts.
///
/// Key security property (§7.2): Untrusted-tier episodic content is NEVER
/// auto-promoted into semantic or shared memory. It sits in quarantine until
/// either corroboration from independent Verified/Standard sources is met,
/// or explicit review approves promotion.
pub struct DefaultConsolidator {
    embedding_provider: Box<dyn EmbeddingProvider>,
    lineage_tracker: std::sync::Arc<parking_lot::RwLock<LineageTracker>>,
}

impl DefaultConsolidator {
    pub fn new(embedding_provider: Box<dyn EmbeddingProvider>) -> Self {
        Self {
            embedding_provider,
            lineage_tracker: std::sync::Arc::new(parking_lot::RwLock::new(LineageTracker::new())),
        }
    }

    /// Check if a set of episodes corroborates a claim
    fn check_corroboration(
        &self,
        episodes: &[MemoryRecord],
        subject: &str,
        predicate: &str,
    ) -> CorroborationResult {
        let mut verified_sources = 0;
        let mut standard_sources = 0;
        let mut untrusted_sources = 0;
        let mut unique_agents = std::collections::HashSet::new();

        for episode in episodes {
            let content_lower = episode.content.to_lowercase();
            if content_lower.contains(&subject.to_lowercase())
                && content_lower.contains(&predicate.to_lowercase())
            {
                match episode.trust_tier {
                    TrustTier::Verified => verified_sources += 1,
                    TrustTier::Standard => standard_sources += 1,
                    TrustTier::Untrusted => untrusted_sources += 1,
                }
                unique_agents.insert(&episode.agent_id);
            }
        }

        // Require at least 2 independent sources (different agents) for corroboration
        let independent_sources = verified_sources + standard_sources;
        let corroborated = independent_sources >= 2 && unique_agents.len() >= 2;

        CorroborationResult {
            corroborated,
            verified_sources,
            standard_sources,
            untrusted_sources,
            unique_agents: unique_agents.len(),
        }
    }

    /// Extract potential facts from a batch of episodes
    fn extract_facts(&self, episodes: &[MemoryRecord]) -> Vec<ExtractedFact> {
        let mut facts = Vec::new();

        // Simple fact extraction: look for subject-predicate-object patterns
        // In production, this would use an LLM for more sophisticated extraction
        for episode in episodes {
            let content = &episode.content;

            // Very basic fact extraction — in production, use LLM
            if let Some(pos) = content.find(" is ") {
                let subject = content[..pos].trim().to_string();
                let rest = content[pos + 4..].trim();
                if rest.find(' ').is_some() {
                    let predicate = "is".to_string();
                    let object = rest.to_string();
                    facts.push(ExtractedFact {
                        subject,
                        predicate,
                        object,
                        source_episode_id: episode.id,
                        confidence: episode.importance,
                        trust_tier: episode.trust_tier,
                    });
                }
            }

            if let Some(pos) = content.find(" has ") {
                let subject = content[..pos].trim().to_string();
                let rest = content[pos + 5..].trim().to_string();
                facts.push(ExtractedFact {
                    subject,
                    predicate: "has".to_string(),
                    object: rest,
                    source_episode_id: episode.id,
                    confidence: episode.importance,
                    trust_tier: episode.trust_tier,
                });
            }

            if let Some(pos) = content.find(" knows ") {
                let subject = content[..pos].trim().to_string();
                let rest = content[pos + 7..].trim().to_string();
                facts.push(ExtractedFact {
                    subject,
                    predicate: "knows".to_string(),
                    object: rest,
                    source_episode_id: episode.id,
                    confidence: episode.importance,
                    trust_tier: episode.trust_tier,
                });
            }
        }

        facts
    }
}

struct ExtractedFact {
    subject: String,
    predicate: String,
    object: String,
    source_episode_id: uuid::Uuid,
    confidence: f32,
    trust_tier: TrustTier,
}

#[allow(dead_code)]
struct CorroborationResult {
    corroborated: bool,
    verified_sources: usize,
    standard_sources: usize,
    untrusted_sources: usize,
    unique_agents: usize,
}

#[async_trait]
impl Consolidator for DefaultConsolidator {
    async fn consolidate(&self, episodes: Vec<MemoryRecord>) -> Result<ConsolidationOutput> {
        let mut facts_created = Vec::new();
        let facts_superseded = Vec::new();
        let mut episodes_quarantined = Vec::new();
        let mut anomalies_detected = Vec::new();

        // Extract potential facts from episodes
        let extracted_facts = self.extract_facts(&episodes);

        info!("Extracted {} potential facts from {} episodes", extracted_facts.len(), episodes.len());

        for fact in extracted_facts {
            // §7.2: Quarantine untrusted content
            if fact.trust_tier == TrustTier::Untrusted {
                // Check corroboration from independent verified/standard sources
                let corroboration = self.check_corroboration(
                    &episodes,
                    &fact.subject,
                    &fact.predicate,
                );

                if corroboration.corroborated {
                    // Corroborated — allow promotion
                    info!(
                        "Untrusted fact corroborated by {} independent sources, promoting",
                        corroboration.unique_agents
                    );
                } else {
                    // Not corroborated — quarantine
                    episodes_quarantined.push(fact.source_episode_id);
                    warn!(
                        "Fact from untrusted source quarantined: {} {} {}",
                        fact.subject, fact.predicate, fact.object
                    );
                    continue;
                }
            }

            // Generate embedding for the fact
            let content = format!("{} {} {}", fact.subject, fact.predicate, fact.object);
            let embedding = self.embedding_provider.embed(&content).await?;

            // Create semantic fact record
            let record = MemoryRecord::new_semantic(
                TenantId("default".to_string()), // TODO: get from episode
                AgentId("consolidator".to_string()),
                fact.subject,
                fact.predicate,
                fact.object,
                fact.confidence,
                vec![fact.source_episode_id],
                embedding,
                self.embedding_provider.model_version().to_string(),
                fact.trust_tier,
            );

            // Track lineage
            {
                let mut tracker = self.lineage_tracker.write();
                tracker.add_derivation(record.id, fact.source_episode_id);
            }

            facts_created.push(record);
        }

        // Check for anomalies
        if facts_created.len() > episodes.len() * 2 {
            anomalies_detected.push(muninn_core::traits::AnomalyFlag {
                record_id: uuid::Uuid::new_v4(),
                anomaly_type: muninn_core::traits::AnomalyType::WriteRateAnomaly,
                description: format!(
                    "Unusually high fact extraction rate: {} facts from {} episodes",
                    facts_created.len(),
                    episodes.len()
                ),
                severity: muninn_core::traits::AnomalySeverity::Medium,
            });
        }

        Ok(ConsolidationOutput {
            facts_created,
            facts_superseded,
            episodes_quarantined,
            anomalies_detected,
        })
    }
}
