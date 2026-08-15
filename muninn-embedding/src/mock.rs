use async_trait::async_trait;

use muninn_core::error::Result;
use muninn_core::traits::EmbeddingProvider;

/// Mock embedding provider for testing — deterministic, no network calls.
/// Generates embeddings based on content hash for reproducible tests.
pub struct MockEmbeddingProvider {
    dimension: usize,
}

impl MockEmbeddingProvider {
    pub fn new(dimension: usize) -> Self {
        Self { dimension }
    }
}

#[async_trait]
impl EmbeddingProvider for MockEmbeddingProvider {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        // Deterministic embedding based on text content
        let mut embedding = vec![0.0; self.dimension];
        let bytes = text.as_bytes();

        for (i, val) in embedding.iter_mut().enumerate() {
            let byte_idx = i % bytes.len();
            *val = (bytes[byte_idx] as f32) / 255.0;

            // Add some variation based on position
            *val += (i as f32 * 0.001).sin();
        }

        // Normalize
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for val in &mut embedding {
                *val /= norm;
            }
        }

        Ok(embedding)
    }

    fn model_version(&self) -> &str {
        "mock-v1"
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn health_check(&self) -> bool {
        true
    }
}
# 1788294676
# 1788294676
// commit 17 1788294953939653946
// commit 65 1788294954653152064
// commit 137 1788294955768359120
// commit 185 1788294956506179118
