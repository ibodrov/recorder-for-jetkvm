use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use crate::paths;
#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum CliCommand {
    /// Run the persistent controller protocol server.
    Serve {
        /// Read NDJSON requests from stdin and write responses to stdout.
        #[arg(long)]
        stdio: bool,
    },
}

#[derive(Parser, Debug, Clone)]
#[command(name = "recorder-for-jetkvm", about = "Record JetKVM screen changes")]
pub struct Config {
    /// JetKVM host address (e.g. 192.168.1.130)
    #[arg(long, global = true, default_value = "")]
    pub host: String,

    /// JetKVM local password (prefer JETKVM_PASSWORD env var or --password-file instead)
    #[arg(long, env = "JETKVM_PASSWORD", global = true)]
    pub password: Option<String>,

    /// Path to a file containing the JetKVM password
    #[arg(long, global = true)]
    pub password_file: Option<PathBuf>,

    /// Directory to store recorded MP4 files (default: your OS video directory)
    #[arg(long)]
    pub output_dir: Option<PathBuf>,

    /// Change detection sensitivity (0.0-1.0, fraction of pixels that must change)
    #[arg(long, default_value_t = 0.02, value_parser = parse_sensitivity)]
    pub sensitivity: f64,

    /// Pre-event buffer duration in seconds
    #[arg(long, default_value_t = 5)]
    pub pre_buffer: u64,

    /// Seconds of no changes before stopping recording
    #[arg(long, default_value_t = 10)]
    pub cooldown: u64,

    /// Interval between change detection checks in milliseconds
    #[arg(long, default_value_t = 500)]
    pub check_interval: u64,

    /// Interval between PLI (keyframe request) messages in seconds
    #[arg(long, default_value_t = 3, global = true)]
    pub pli_interval: u64,

    /// Accept invalid TLS certificates (for self-signed certs)
    #[arg(long, default_value_t = false, global = true)]
    pub no_tls_verify: bool,

    /// Capture a single screenshot and exit
    #[arg(long, default_value_t = false)]
    pub screenshot: bool,

    /// Path to write the screenshot PNG (default: your OS pictures directory)
    #[arg(long)]
    pub screenshot_output: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<CliCommand>,
}

fn parse_sensitivity(input: &str) -> std::result::Result<f64, String> {
    let value = input
        .parse::<f64>()
        .map_err(|_| format!("sensitivity must be a finite number in [0.0, 1.0], got '{input}'"))?;

    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(format!(
            "sensitivity must be a finite number in [0.0, 1.0], got '{input}'"
        ));
    }

    Ok(value)
}

impl Config {
    /// Resolve the recording output directory from CLI or platform defaults.
    pub fn recordings_dir(&self) -> PathBuf {
        self.output_dir
            .clone()
            .unwrap_or_else(paths::default_recordings_dir)
    }

    /// Resolve the screenshot output path from CLI or platform defaults.
    pub fn screenshot_output_path(&self) -> PathBuf {
        self.screenshot_output
            .clone()
            .unwrap_or_else(paths::default_screenshot_path)
    }

    /// Resolve the password from CLI arg, env var (handled by clap), or password file.
    pub fn resolve_password(&self) -> Result<String> {
        if let Some(password) = &self.password {
            return Ok(password.clone());
        }

        if let Some(path) = &self.password_file {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read password file: {}", path.display()))?;
            let password = contents.trim().to_string();
            if password.is_empty() {
                anyhow::bail!("password file is empty: {}", path.display());
            }
            return Ok(password);
        }

        anyhow::bail!(
            "no password provided; use --password, JETKVM_PASSWORD env var, or --password-file"
        )
    }

    pub fn validate(&self) -> Result<()> {
        if self.host.trim().is_empty() {
            anyhow::bail!("--host is required");
        }
        if matches!(self.command, Some(CliCommand::Serve { stdio: false })) {
            anyhow::bail!("serve currently requires --stdio");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;

    #[test]
    fn test_parse_with_required_host() {
        let cfg = Config::try_parse_from(["recorder-for-jetkvm", "--host", "192.168.1.130"])
            .expect("expected config parsing to succeed");
        assert_eq!(cfg.host, "192.168.1.130");
        assert!(cfg.output_dir.is_none());
        assert_eq!(cfg.sensitivity, 0.02);
        assert_eq!(cfg.check_interval, 500);
        assert!(!cfg.screenshot);
        assert!(cfg.screenshot_output.is_none());
    }

    #[test]
    fn test_screenshot_flag_enables_single_capture_mode() {
        let cfg = Config::try_parse_from([
            "recorder-for-jetkvm",
            "--host",
            "192.168.1.130",
            "--screenshot",
        ])
        .expect("expected screenshot mode parsing to succeed");

        assert!(cfg.screenshot);
    }

    #[test]
    fn test_explicit_output_paths_override_defaults() {
        let cfg = Config::try_parse_from([
            "recorder-for-jetkvm",
            "--host",
            "192.168.1.130",
            "--output-dir",
            "/tmp/recordings",
            "--screenshot-output",
            "/tmp/capture.png",
        ])
        .expect("expected explicit output paths to parse");

        assert_eq!(cfg.recordings_dir(), PathBuf::from("/tmp/recordings"));
        assert_eq!(
            cfg.screenshot_output_path(),
            PathBuf::from("/tmp/capture.png")
        );
    }

    #[test]
    fn test_noise_threshold_flag_is_rejected() {
        let err = Config::try_parse_from([
            "recorder-for-jetkvm",
            "--host",
            "192.168.1.130",
            "--noise-threshold",
            "25",
        ])
        .expect_err("expected removed --noise-threshold flag to fail parsing");

        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn test_sensitivity_valid_values() {
        for (raw, expected) in [("0.0", 0.0), ("0.02", 0.02), ("1.0", 1.0)] {
            let sensitivity_arg = format!("--sensitivity={raw}");
            let cfg = Config::try_parse_from(vec![
                "recorder-for-jetkvm".to_string(),
                "--host".to_string(),
                "192.168.1.130".to_string(),
                sensitivity_arg,
            ])
            .expect("expected sensitivity to parse");

            assert_eq!(cfg.sensitivity, expected);
        }
    }

    #[test]
    fn test_sensitivity_invalid_values_are_rejected() {
        for raw in ["-0.1", "1.1", "NaN", "inf"] {
            let sensitivity_arg = format!("--sensitivity={raw}");
            let err = Config::try_parse_from(vec![
                "recorder-for-jetkvm".to_string(),
                "--host".to_string(),
                "192.168.1.130".to_string(),
                sensitivity_arg,
            ])
            .expect_err("expected invalid sensitivity to fail parsing");

            let message = err.to_string();
            assert!(
                message.contains("sensitivity must be a finite number in [0.0, 1.0]"),
                "unexpected clap error for --sensitivity={raw}: {message}"
            );
        }
    }

    #[test]
    fn validation_requires_host_for_every_mode() {
        let config = Config::try_parse_from(["recorder-for-jetkvm", "serve", "--stdio"])
            .expect("serve arguments should parse");
        assert_eq!(
            config.validate().unwrap_err().to_string(),
            "--host is required"
        );
    }

    #[test]
    fn serve_requires_explicit_stdio_transport() {
        let config =
            Config::try_parse_from(["recorder-for-jetkvm", "serve", "--host", "192.168.1.130"])
                .expect("serve arguments should parse");
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("--stdio")
        );
    }

    #[test]
    fn serve_stdio_accepts_global_options_after_subcommand() {
        let config = Config::try_parse_from([
            "recorder-for-jetkvm",
            "serve",
            "--stdio",
            "--host",
            "192.168.1.130",
        ])
        .expect("serve arguments should parse");
        config.validate().expect("serve --stdio should validate");
    }
}
