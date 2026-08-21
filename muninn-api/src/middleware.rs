use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use tracing::warn;

use crate::server::AppState;
use muninn_core::model::TenantId;

/// Authentication middleware — validates API key or JWT
pub async fn auth_middleware(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Extract API key from header
    let api_key = headers
        .get(&state.config.security.api_key_header)
        .and_then(|v| v.to_str().ok());

    match api_key {
        Some(key) => {
            // TODO: Validate API key against tenant database
            // For now, accept any non-empty key
            if key.is_empty() {
                warn!("Empty API key provided");
                return Err(StatusCode::UNAUTHORIZED);
            }

            // TODO: Extract tenant_id and agent_id from validated key
            // For now, pass through
            Ok(next.run(request).await)
        }
        None => {
            warn!("No API key provided");
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

/// Rate limiting middleware (per-tenant)
pub async fn rate_limit_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Extract tenant_id from request headers or path
    // For now, use a default tenant — in production, extract from JWT/API key
    let tenant_id = request
        .headers()
        .get("X-Tenant-Id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("default");

    match state.rate_limiter.check_write(&TenantId(tenant_id.to_string())) {
        Ok(()) => Ok(next.run(request).await),
        Err(_) => {
            warn!("Rate limit exceeded for tenant {}", tenant_id);
            Err(StatusCode::TOO_MANY_REQUESTS)
        }
    }
}
# 1788294676
# 1788294676
// commit 279 1788294957990688136
