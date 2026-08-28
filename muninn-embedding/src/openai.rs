use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use muninn_core::error::{Error, Result};
use muninn_core::traits::EmbeddingProvider;

/// OpenAI-compatible embedding provider
pub struct OpenAiEmbeddingProvider {
    client: Client,
    api_key: String,
    api_base_url: String,
    model: String,
    dimension: usize,
    #[allow(dead_code)]
    batch_size: usize,
}

#[derive(Serialize)]
struct EmbeddingRequest {
    model: String,
    input: Vec<String>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct EmbeddingResponse {
    data: Vec<EmbeddingData>,
    model: String,
    usage: Usage,
}

#[derive(Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct Usage {
    prompt_tokens: u32,
    total_tokens: u32,
}

impl OpenAiEmbeddingProvider {
    pub fn new(
        api_key: String,
        api_base_url: String,
        model: String,
        dimension: usize,
        batch_size: usize,
    ) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            api_key,
            api_base_url,
            model,
            dimension,
            batch_size,
        }
    }

    /// Embed a batch of texts
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let request = EmbeddingRequest {
            model: self.model.clone(),
            input: texts.to_vec(),
        };

        let response = self
            .client
            .post(format!("{}/v1/embeddings", self.api_base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Embedding(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Error::Embedding(format!(
                "API returned {}: {}",
                status, body
            )));
        }

        let embedding_response: EmbeddingResponse = response
            .json()
            .await
            .map_err(|e| Error::Embedding(format!("Failed to parse response: {}", e)))?;

        let mut embeddings = vec![vec![0.0; self.dimension]; texts.len()];
        for data in embedding_response.data {
            if data.index < embeddings.len() {
                embeddings[data.index] = data.embedding;
            }
        }

        Ok(embeddings)
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAiEmbeddingProvider {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let results = self.embed_batch(&[text.to_string()]).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| Error::Embedding("No embedding returned".to_string()))
    }

    fn model_version(&self) -> &str {
        &self.model
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn health_check(&self) -> bool {
        // Try to embed a simple text
        self.embed("health check").await.is_ok()
    }
}
# 1788294676
# 1788294676
# 1788294676
# 1788294676
// commit 136 1788294955753418943
// commit 160 1788294956122574261
// commit 328 1788294958736969839
// commit 352 1788294959121711930
