use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use tracing::{info, warn};

/// Default timeout for HTTP requests to the JetKVM device.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Build a base URL from a host string.
/// If the host already starts with `http://` or `https://`, use as-is.
/// Otherwise default to `http://{host}` (JetKVM devices typically serve plain HTTP on local networks).
pub fn base_url(host: &str) -> String {
    if host.starts_with("http://") || host.starts_with("https://") {
        host.trim_end_matches('/').to_string()
    } else {
        format!("http://{host}")
    }
}

pub async fn login(host: &str, password: &str, no_tls_verify: bool) -> Result<Client> {
    let base = base_url(host);
    let url = format!("{base}/auth/login-local");

    if base.starts_with("http://") {
        warn!("sending credentials over plaintext HTTP — use only on trusted local networks");
    }

    let jar = Arc::new(reqwest::cookie::Jar::default());
    let mut builder = Client::builder()
        .cookie_provider(jar)
        .timeout(HTTP_TIMEOUT)
        .connect_timeout(HTTP_TIMEOUT);

    if no_tls_verify {
        warn!("TLS certificate verification disabled");
        builder = builder.danger_accept_invalid_certs(true);
    }

    let client = builder.build().context("failed to build HTTP client")?;

    let resp = client
        .post(&url)
        .json(&serde_json::json!({"password": password}))
        .send()
        .await
        .context("failed to send auth request")?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("authentication failed (HTTP {status}): {body}");
    }

    info!("authenticated with JetKVM at {base}");
    Ok(client)
}
