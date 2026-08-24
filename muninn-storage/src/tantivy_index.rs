use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tantivy::collector::TopDocs;
use tantivy::query::{Occur, QueryParser};
use tantivy::schema::*;
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy};
use uuid::Uuid;

use muninn_core::error::{Error, Result};

/// Pending document to be indexed
struct PendingDoc {
    id: Uuid,
    content: String,
    tenant_id: String,
    agent_id: String,
    tier: String,
    trust_tier: String,
}

/// Full-text search index backed by Tantivy.
/// Provides BM25 scoring for keyword queries.
/// Uses batched commits for low write latency.
pub struct TantivyIndex {
    index: Index,
    reader: IndexReader,
    writer: Mutex<IndexWriter>,
    pending: Mutex<Vec<PendingDoc>>,
    last_commit: Mutex<Instant>,
    commit_interval: Duration,
    max_pending: usize,
    docs_since_commit: AtomicU64,
    #[allow(dead_code)]
    schema: Schema,
    // Field handles
    id_field: Field,
    content_field: Field,
    tenant_id_field: Field,
    agent_id_field: Field,
    tier_field: Field,
    trust_tier_field: Field,
}

impl TantivyIndex {
    /// Open or create a Tantivy index at the given path
    pub fn open(index_dir: &Path) -> Result<Self> {
        Self::open_with_config(index_dir, Duration::from_millis(100), 100)
    }

    /// Open with custom batch configuration
    pub fn open_with_config(
        index_dir: &Path,
        commit_interval: Duration,
        max_pending: usize,
    ) -> Result<Self> {
        std::fs::create_dir_all(index_dir)?;

        let mut schema_builder = Schema::builder();

        let id_field = schema_builder.add_text_field("id", STRING | STORED);
        let content_field = schema_builder.add_text_field("content", TEXT | STORED);
        let tenant_id_field = schema_builder.add_text_field("tenant_id", STRING | STORED);
        let agent_id_field = schema_builder.add_text_field("agent_id", STRING | STORED);
        let tier_field = schema_builder.add_text_field("tier", STRING | STORED);
        let trust_tier_field = schema_builder.add_text_field("trust_tier", STRING | STORED);

        let schema = schema_builder.build();

        let index = if index_dir.exists() && index_dir.join("meta.json").exists() {
            Index::open_in_dir(index_dir)
                .map_err(|e| Error::Storage(format!("Failed to open Tantivy index: {}", e)))?
        } else {
            Index::create_in_dir(index_dir, schema.clone())
                .map_err(|e| Error::Storage(format!("Failed to create Tantivy index: {}", e)))?
        };

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .map_err(|e| Error::Storage(format!("Failed to create index reader: {}", e)))?;

        let writer: IndexWriter = index
            .writer(50_000_000) // 50MB heap
            .map_err(|e| Error::Storage(format!("Failed to create index writer: {}", e)))?;

        Ok(Self {
            index,
            reader,
            writer: Mutex::new(writer),
            pending: Mutex::new(Vec::new()),
            last_commit: Mutex::new(Instant::now()),
            commit_interval,
            max_pending,
            docs_since_commit: AtomicU64::new(0),
            schema,
            id_field,
            content_field,
            tenant_id_field,
            agent_id_field,
            tier_field,
            trust_tier_field,
        })
    }

    /// Buffer a document for batched indexing.
    /// Returns immediately without disk I/O — commit happens in background.
    pub fn buffer_document(
        &self,
        id: Uuid,
        content: &str,
        tenant_id: &str,
        agent_id: &str,
        tier: &str,
        trust_tier: &str,
    ) {
        let pending = PendingDoc {
            id,
            content: content.to_string(),
            tenant_id: tenant_id.to_string(),
            agent_id: agent_id.to_string(),
            tier: tier.to_string(),
            trust_tier: trust_tier.to_string(),
        };

        self.pending.lock().push(pending);

        // Check if we should commit now
        self.maybe_commit();
    }

    /// Index a document immediately (with commit).
    /// Use `buffer_document` for better throughput.
    pub fn index_document(
        &self,
        id: Uuid,
        content: &str,
        tenant_id: &str,
        agent_id: &str,
        tier: &str,
        trust_tier: &str,
    ) -> Result<()> {
        let mut writer = self.writer.lock();

        let doc = doc!(
            self.id_field => id.to_string(),
            self.content_field => content,
            self.tenant_id_field => tenant_id,
            self.agent_id_field => agent_id,
            self.tier_field => tier,
            self.trust_tier_field => trust_tier,
        );

        writer
            .add_document(doc)
            .map_err(|e| Error::Storage(format!("Failed to add document: {}", e)))?;

        writer
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {}", e)))?;

        Ok(())
    }

    /// Flush all pending documents to disk
    pub fn flush(&self) -> Result<()> {
        let mut pending = self.pending.lock();
        if pending.is_empty() {
            return Ok(());
        }

        let docs: Vec<PendingDoc> = pending.drain(..).collect();
        let mut writer = self.writer.lock();

        for p in &docs {
            let doc = doc!(
                self.id_field => p.id.to_string(),
                self.content_field => p.content.clone(),
                self.tenant_id_field => p.tenant_id.clone(),
                self.agent_id_field => p.agent_id.clone(),
                self.tier_field => p.tier.clone(),
                self.trust_tier_field => p.trust_tier.clone(),
            );

            writer
                .add_document(doc)
                .map_err(|e| Error::Storage(format!("Failed to add document: {}", e)))?;
        }

        writer
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {}", e)))?;

        self.docs_since_commit.fetch_add(docs.len() as u64, Ordering::Relaxed);
        *self.last_commit.lock() = Instant::now();

        Ok(())
    }

    /// Check if we should commit based on time or batch size
    fn maybe_commit(&self) {
        let should_commit_time = self.last_commit.lock().elapsed() >= self.commit_interval;
        let should_commit_size = self.pending.lock().len() >= self.max_pending;

        if should_commit_time || should_commit_size {
            // Best-effort flush — don't block the caller
            let _ = self.flush();
        }
    }

    /// Remove a document by ID
    pub fn remove_document(&self, id: Uuid) -> Result<bool> {
        let mut writer = self.writer.lock();

        let query = tantivy::query::TermQuery::new(
            Term::from_field_text(self.id_field, &id.to_string()),
            tantivy::schema::IndexRecordOption::Basic,
        );

        let num_deleted = writer
            .delete_query(Box::new(query))
            .map_err(|e| Error::Storage(format!("Failed to delete: {}", e)))?;

        writer
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {}", e)))?;

        Ok(num_deleted > 0)
    }

    /// Search for documents matching a text query within a tenant.
    /// Returns (document_id, bm25_score) pairs.
    pub fn search(
        &self,
        query_text: &str,
        tenant_id: &str,
        top_k: usize,
    ) -> Result<Vec<(Uuid, f32)>> {
        // Ensure pending docs are searchable
        let _ = self.flush();

        let searcher = self.reader.searcher();

        let content_query = QueryParser::for_index(&self.index, vec![self.content_field])
            .parse_query(query_text)
            .map_err(|e| Error::Storage(format!("Query parse failed: {}", e)))?;

        let tenant_query = tantivy::query::TermQuery::new(
            Term::from_field_text(self.tenant_id_field, tenant_id),
            tantivy::schema::IndexRecordOption::Basic,
        );

        let combined_query = tantivy::query::BooleanQuery::new(vec![
            (Occur::Must, Box::new(content_query) as Box<dyn tantivy::query::Query>),
            (Occur::Must, Box::new(tenant_query) as Box<dyn tantivy::query::Query>),
        ]);

        let top_docs = searcher
            .search(&combined_query, &TopDocs::with_limit(top_k))
            .map_err(|e| Error::Storage(format!("Search failed: {}", e)))?;

        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            if let Ok(doc) = searcher.doc::<tantivy::TantivyDocument>(doc_address) {
                if let Some(id_value) = doc.get_first(self.id_field) {
                    if let Some(id_str) = id_value.as_str() {
                        if let Ok(uuid) = Uuid::parse_str(id_str) {
                            results.push((uuid, score));
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Get the number of indexed documents
    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    /// Get the number of pending (uncommitted) documents
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }

    /// Get total documents since last commit
    pub fn docs_since_commit(&self) -> u64 {
        self.docs_since_commit.load(Ordering::Relaxed)
    }

    /// Clear the index
    pub fn clear(&self) -> Result<()> {
        self.pending.lock().clear();

        let mut writer = self.writer.lock();

        let query = tantivy::query::AllQuery;
        writer
            .delete_query(Box::new(query))
            .map_err(|e| Error::Storage(format!("Failed to delete all: {}", e)))?;

        writer
            .commit()
            .map_err(|e| Error::Storage(format!("Failed to commit: {}", e)))?;

        Ok(())
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
// commit 58 1788294954545285188
// commit 82 1788294954913923738
// commit 106 1788294955278818985
// commit 202 1788294956773058205
// commit 298 1788294958270424643
