use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::model::{MemoryRecord, MemoryTier};
use crate::trust::TrustTier;
use crate::visibility::Visibility;

/// Retrieval query
#[derive(Debug, Clone)]
pub struct RetrievalQuery {
    pub tenant_id: crate::model::TenantId,
    pub agent_id: crate::model::AgentId,
    pub embedding: Vec<f32>,
    pub tiers: Vec<MemoryTier>,
    pub max_results: usize,
    pub min_score: f32,
    pub visibility_filter: Option<Visibility>,
    pub trust_tier_minimum: Option<TrustTier>,
    pub time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub keyword_query: Option<String>,
}

/// Score breakdown for explainability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreBreakdown {
    pub cosine_similarity: f32,
    pub recency_score: f32,
    pub importance_score: f32,
    pub keyword_score: f32,
    pub trust_multiplier: f32,
    pub total_score: f32,
    pub weight_relevance: f32,
    pub weight_recency: f32,
    pub weight_importance: f32,
    pub weight_keyword: f32,
}

/// A scored memory record with full score breakdown for explainability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredRecord {
    pub record: MemoryRecord,
    pub score: ScoreBreakdown,
    pub rank: usize,
}

impl ScoredRecord {
    /// Calculate the full score breakdown for a record against a query
    pub fn calculate(
        record: MemoryRecord,
        cosine_similarity: f32,
        bm25_score: f32,
        weights: &RetrievalWeights,
        now: DateTime<Utc>,
    ) -> Self {
        let recency = (-weights.decay_rate
            * (now - record.last_accessed).num_milliseconds() as f32
            / 60000.0)
            .exp();

        let importance = record.importance;

        // Normalize BM25 score to 0..1 range (BM25 scores are unbounded positive)
        let normalized_bm25 = (bm25_score / (1.0 + bm25_score)).clamp(0.0, 1.0);

        let trust_multiplier = record.trust_tier.retrieval_multiplier();

        let raw_score = weights.weight_relevance * cosine_similarity
            + weights.weight_recency * recency
            + weights.weight_importance * importance
            + weights.weight_keyword * normalized_bm25;

        let total_score = raw_score * trust_multiplier;

        Self {
            record,
            score: ScoreBreakdown {
                cosine_similarity,
                recency_score: recency,
                importance_score: importance,
                keyword_score: normalized_bm25,
                trust_multiplier,
                total_score,
                weight_relevance: weights.weight_relevance,
                weight_recency: weights.weight_recency,
                weight_importance: weights.weight_importance,
                weight_keyword: weights.weight_keyword,
            },
            rank: 0,
        }
    }
}

/// Retrieval weights — per-agent config, hot-reloadable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalWeights {
    pub weight_relevance: f32,
    pub weight_recency: f32,
    pub weight_importance: f32,
    pub weight_keyword: f32,
    pub decay_rate: f32,
}

impl Default for RetrievalWeights {
    fn default() -> Self {
        Self {
            weight_relevance: 0.5,
            weight_recency: 0.2,
            weight_importance: 0.2,
            weight_keyword: 0.1,
            decay_rate: 0.01,
        }
    }
}

/// Introspection result for a past recall event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallIntrospection {
    pub query: RetrievalQuerySnapshot,
    pub results: Vec<ScoredRecord>,
    pub timestamp: DateTime<Utc>,
    pub total_candidates: usize,
    pub filtered_by_visibility: usize,
    pub filtered_by_trust: usize,
    pub filtered_by_tier: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalQuerySnapshot {
    pub embedding_model_version: String,
    pub tiers: Vec<String>,
    pub max_results: usize,
    pub min_score: f32,
    pub trust_tier_minimum: Option<String>,
}
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
# 1788294673
// commit 52 1788294954451413574
// commit 76 1788294954821725876
// commit 100 1788294955186251346
