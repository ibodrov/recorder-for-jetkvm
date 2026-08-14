use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::cookie::CookieStore;
use reqwest::header::HeaderValue;
use reqwest::{Client, Url};
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
#[derive(Clone)]
pub struct AuthenticatedClient {
    client: Client,
    cookie_jar: Arc<reqwest::cookie::Jar>,
    base_url: String,
    no_tls_verify: bool,
}

impl AuthenticatedClient {
    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn no_tls_verify(&self) -> bool {
        self.no_tls_verify
    }

    #[cfg(test)]
    pub(crate) fn test_client() -> Self {
        let cookie_jar = Arc::new(reqwest::cookie::Jar::default());
        let client = Client::builder()
            .cookie_provider(Arc::clone(&cookie_jar))
            .build()
            .expect("test HTTP client");
        Self {
            client,
            cookie_jar,
            // Port zero has no listener, making HTTP uploads fail immediately
            // and deterministically exercise their WebRTC fallback.
            base_url: "http://127.0.0.1:0".to_owned(),
            no_tls_verify: false,
        }
    }

    pub fn cookie_header(&self) -> Result<Option<HeaderValue>> {
        let url = Url::parse(&self.base_url).context("invalid JetKVM base URL")?;
        Ok(self.cookie_jar.cookies(&url))
    }
}

pub async fn authenticate(
    host: &str,
    password: &str,
    no_tls_verify: bool,
) -> Result<AuthenticatedClient> {
    let base = base_url(host);
    let url = format!("{base}/auth/login-local");

    if base.starts_with("http://") {
        warn!("sending credentials over plaintext HTTP — use only on trusted local networks");
    }

    let cookie_jar = Arc::new(reqwest::cookie::Jar::default());
    let mut builder = Client::builder()
        .cookie_provider(Arc::clone(&cookie_jar))
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
        .context("failed to send authentication request")?;

    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("authentication failed (HTTP {status})");
    }

    info!("authenticated with JetKVM at {base}");
    Ok(AuthenticatedClient {
        client,
        cookie_jar,
        base_url: base,
        no_tls_verify,
    })
}
