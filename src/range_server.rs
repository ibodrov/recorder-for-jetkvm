use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use rand::RngCore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

const MAX_REQUEST_BYTES: usize = 8 * 1024;
const RESPONSE_BUFFER_BYTES: usize = 64 * 1024;
const MAX_CONCURRENT_READS: usize = 8;
const ROUTE_PREFIX: &str = "/jetkvm-controller/media/";
pub const REDACTED_LOCAL_MEDIA_URL: &str = "controller://local-image";

pub struct RangeServer {
    file_path: PathBuf,
    mount_url: String,
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl fmt::Debug for RangeServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeServer")
            .field("file_path", &self.file_path)
            .field("mount_url", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl RangeServer {
    pub async fn start(file_path: &Path, jetkvm_base_url: &str) -> Result<Self> {
        let bind_address = select_reachable_address(jetkvm_base_url).await?;
        Self::start_on(file_path, bind_address).await
    }

    async fn start_on(file_path: &Path, bind_address: IpAddr) -> Result<Self> {
        let canonical = tokio::fs::canonicalize(file_path).await.with_context(|| {
            format!(
                "failed to canonicalize media image: {}",
                file_path.display()
            )
        })?;
        let file = std::fs::File::open(&canonical)
            .with_context(|| format!("failed to open media image: {}", canonical.display()))?;
        let metadata = file.metadata().context("failed to inspect media image")?;
        if !metadata.is_file() {
            bail!("media image is not a regular file");
        }
        if metadata.len() == 0 {
            bail!("media image is empty");
        }
        let file_size = metadata.len();

        let listener = TcpListener::bind(SocketAddr::new(bind_address, 0))
            .await
            .with_context(|| format!("failed to bind range server on {bind_address}"))?;
        let address = listener
            .local_addr()
            .context("failed to read range server address")?;
        let mut token = [0_u8; 32];
        rand::rng().fill_bytes(&mut token);
        let token = token
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let route = format!("{ROUTE_PREFIX}{token}");
        let host = match address.ip() {
            IpAddr::V4(ip) => ip.to_string(),
            IpAddr::V6(ip) => format!("[{ip}]"),
        };
        let mount_url = format!("http://{host}:{}{route}", address.port());

        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let file = Arc::new(file);
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_READS));
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = task_cancellation.cancelled() => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.context("range server accept failed")?;
                        let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
                            connections.spawn(async move {
                                let mut stream = stream;
                                let _ = write_simple_response(
                                    &mut stream,
                                    503,
                                    "Service Unavailable",
                                    &[],
                                ).await;
                            });
                            continue;
                        };
                        let file = Arc::clone(&file);
                        let route = route.clone();
                        connections.spawn(async move {
                            let _permit = permit;
                            let _ = handle_connection(stream, file, file_size, &route).await;
                        });
                    }
                    completed = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(error)) = completed {
                            return Err(error).context("range server connection task failed");
                        }
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
            Ok(())
        });

        Ok(Self {
            file_path: canonical,
            mount_url,
            cancellation,
            task,
        })
    }

    pub(crate) fn mount_url(&self) -> &str {
        &self.mount_url
    }

    pub fn is_healthy(&self) -> bool {
        !self.task.is_finished()
    }

    pub fn ensure_healthy(&self) -> Result<()> {
        if !self.is_healthy() {
            bail!("local media server stopped unexpectedly");
        }
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<()> {
        self.cancellation.cancel();
        (&mut self.task)
            .await
            .context("range server task failed")??;
        Ok(())
    }
}

impl Drop for RangeServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

pub fn is_controller_owned_url(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .is_some_and(|url| url.path().starts_with(ROUTE_PREFIX))
}

async fn select_reachable_address(jetkvm_base_url: &str) -> Result<IpAddr> {
    let url = reqwest::Url::parse(jetkvm_base_url).context("invalid JetKVM URL")?;
    let host = url.host_str().context("JetKVM URL has no host")?;
    let port = url.port_or_known_default().unwrap_or(80);
    let target = tokio::net::lookup_host((host, port))
        .await
        .context("failed to resolve JetKVM host")?
        .next()
        .context("JetKVM host resolved to no addresses")?;
    let bind = if target.is_ipv4() {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
    };
    let socket = UdpSocket::bind(bind)
        .await
        .context("failed to open route probe socket")?;
    socket
        .connect(target)
        .await
        .context("failed to select route to JetKVM")?;
    Ok(socket.local_addr()?.ip())
}

async fn handle_connection(
    mut stream: TcpStream,
    file: Arc<std::fs::File>,
    file_size: u64,
    expected_route: &str,
) -> Result<()> {
    let Some(request) = read_request(&mut stream).await? else {
        return Ok(());
    };
    let mut lines = request.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let route = parts.next().unwrap_or_default();
    let version = parts.next().unwrap_or_default();
    if parts.next().is_some() || version != "HTTP/1.1" {
        return write_simple_response(&mut stream, 400, "Bad Request", &[]).await;
    }
    if route != expected_route {
        return write_simple_response(&mut stream, 404, "Not Found", &[]).await;
    }
    if method != "GET" && method != "HEAD" {
        return write_simple_response(
            &mut stream,
            405,
            "Method Not Allowed",
            &[("Allow", "GET, HEAD".to_owned())],
        )
        .await;
    }

    let mut range_header = None;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            return write_simple_response(&mut stream, 400, "Bad Request", &[]).await;
        };
        if name.eq_ignore_ascii_case("range") {
            if range_header.is_some() {
                return write_simple_response(&mut stream, 400, "Bad Request", &[]).await;
            }
            range_header = Some(value.trim());
        }
    }

    let selected = match range_header {
        Some(value) => match parse_range(value, file_size) {
            Ok(range) => Some(range),
            Err(RangeError::Malformed) => {
                return write_simple_response(&mut stream, 400, "Bad Request", &[]).await;
            }
            Err(RangeError::Unsatisfiable) => {
                return write_simple_response(
                    &mut stream,
                    416,
                    "Range Not Satisfiable",
                    &[("Content-Range", format!("bytes */{file_size}"))],
                )
                .await;
            }
        },
        None => None,
    };
    let (status, reason, start, end) = match selected {
        Some((start, end)) => (206, "Partial Content", start, end),
        None => (200, "OK", 0, file_size - 1),
    };
    let content_length = end - start + 1;
    let mut headers = vec![
        ("Accept-Ranges", "bytes".to_owned()),
        ("Content-Type", "application/octet-stream".to_owned()),
        ("Content-Length", content_length.to_string()),
        ("Connection", "close".to_owned()),
    ];
    if status == 206 {
        headers.push(("Content-Range", format!("bytes {start}-{end}/{file_size}")));
    }
    write_headers(&mut stream, status, reason, &headers).await?;
    if method == "HEAD" {
        return Ok(());
    }

    let mut offset = start;
    let mut remaining = content_length;
    let mut buffer = vec![0_u8; RESPONSE_BUFFER_BYTES];
    while remaining > 0 {
        let read_len = usize::try_from(remaining.min(RESPONSE_BUFFER_BYTES as u64))
            .expect("bounded read length fits usize");
        let worker_file = Arc::clone(&file);
        let worker_buffer = buffer;
        let (returned, count) = tokio::task::spawn_blocking(move || {
            let mut buffer = worker_buffer;
            let count = worker_file.read_at(&mut buffer[..read_len], offset)?;
            Ok::<_, std::io::Error>((buffer, count))
        })
        .await
        .context("range reader task failed")??;
        buffer = returned;
        if count == 0 {
            bail!("media image ended during range response");
        }
        stream.write_all(&buffer[..count]).await?;
        offset += count as u64;
        remaining -= count as u64;
    }
    stream.shutdown().await?;
    Ok(())
}

async fn read_request(stream: &mut TcpStream) -> Result<Option<String>> {
    let mut request = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    loop {
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Ok(None);
        }
        request.extend_from_slice(&chunk[..count]);
        if request.len() > MAX_REQUEST_BYTES {
            write_simple_response(stream, 431, "Request Header Fields Too Large", &[]).await?;
            return Ok(None);
        }
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            return String::from_utf8(request)
                .map(Some)
                .context("HTTP request headers are not UTF-8");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeError {
    Malformed,
    Unsatisfiable,
}

fn parse_range(value: &str, size: u64) -> std::result::Result<(u64, u64), RangeError> {
    let Some(spec) = value.strip_prefix("bytes=") else {
        return Err(RangeError::Malformed);
    };
    if spec.contains(',') {
        return Err(RangeError::Malformed);
    }
    let Some((start, end)) = spec.split_once('-') else {
        return Err(RangeError::Malformed);
    };
    if start.is_empty() {
        let suffix = end.parse::<u64>().map_err(|_| RangeError::Malformed)?;
        if suffix == 0 || size == 0 {
            return Err(RangeError::Unsatisfiable);
        }
        let length = suffix.min(size);
        return Ok((size - length, size - 1));
    }
    let start = start.parse::<u64>().map_err(|_| RangeError::Malformed)?;
    if start >= size {
        return Err(RangeError::Unsatisfiable);
    }
    let end = if end.is_empty() {
        size - 1
    } else {
        end.parse::<u64>().map_err(|_| RangeError::Malformed)?
    };
    if end < start {
        return Err(RangeError::Unsatisfiable);
    }
    Ok((start, end.min(size - 1)))
}

async fn write_simple_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    headers: &[(&str, String)],
) -> Result<()> {
    let mut all_headers = headers.to_vec();
    all_headers.push(("Content-Length", "0".to_owned()));
    all_headers.push(("Connection", "close".to_owned()));
    write_headers(stream, status, reason, &all_headers).await
}

async fn write_headers(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    headers: &[(&str, String)],
) -> Result<()> {
    let mut response = format!("HTTP/1.1 {status} {reason}\r\n");
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    stream.write_all(response.as_bytes()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::RANGE;

    #[test]
    fn parses_range_forms_and_errors() {
        assert_eq!(parse_range("bytes=2-4", 10), Ok((2, 4)));
        assert_eq!(parse_range("bytes=7-", 10), Ok((7, 9)));
        assert_eq!(parse_range("bytes=-3", 10), Ok((7, 9)));
        assert_eq!(parse_range("bytes=20-", 10), Err(RangeError::Unsatisfiable));
        assert_eq!(parse_range("bytes=4-2", 10), Err(RangeError::Unsatisfiable));
        assert_eq!(parse_range("bytes=1-2,4-5", 10), Err(RangeError::Malformed));
        assert_eq!(parse_range("items=1-2", 10), Err(RangeError::Malformed));
    }

    #[tokio::test]
    async fn serves_exact_ranges_and_headers_to_real_client() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("image.iso");
        tokio::fs::write(&path, b"0123456789")
            .await
            .expect("fixture write");
        let server = RangeServer::start_on(&path, IpAddr::V4(Ipv4Addr::LOCALHOST))
            .await
            .expect("range server should start");
        let client = reqwest::Client::new();

        let response = client
            .get(server.mount_url())
            .header(RANGE, "bytes=2-4")
            .send()
            .await
            .expect("range request");
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()["content-range"], "bytes 2-4/10");
        assert_eq!(response.headers()["accept-ranges"], "bytes");
        assert_eq!(response.bytes().await.expect("range body"), &b"234"[..]);

        let response = client
            .head(server.mount_url())
            .header(RANGE, "bytes=-2")
            .send()
            .await
            .expect("HEAD request");
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()["content-length"], "2");
        assert!(response.bytes().await.expect("HEAD body").is_empty());

        let hidden = reqwest::Url::parse(server.mount_url()).expect("server URL");
        let unrelated = hidden.join("/etc/passwd").expect("unrelated URL");
        assert_eq!(
            client
                .get(unrelated)
                .send()
                .await
                .expect("404 request")
                .status(),
            404
        );
        server.shutdown().await.expect("range server shutdown");
    }

    #[tokio::test]
    async fn dropping_server_stops_listener() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("image.iso");
        tokio::fs::write(&path, b"0123456789")
            .await
            .expect("fixture write");
        let server = RangeServer::start_on(&path, IpAddr::V4(Ipv4Addr::LOCALHOST))
            .await
            .expect("range server should start");
        let url = server.mount_url().to_owned();
        drop(server);

        let result = reqwest::Client::new()
            .get(url)
            .timeout(std::time::Duration::from_secs(1))
            .send()
            .await;
        assert!(
            result.is_err(),
            "range listener remained reachable after drop"
        );
    }
}
