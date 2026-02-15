use std::path::{Path, PathBuf};

use chrono::Local;
use directories::{BaseDirs, UserDirs};

pub fn default_recordings_dir() -> PathBuf {
    UserDirs::new()
        .and_then(|dirs| dirs.video_dir().map(Path::to_path_buf))
        .unwrap_or_else(|| fallback_home_dir(default_video_dir_name()))
}

pub fn default_screenshot_path() -> PathBuf {
    let filename = format!(
        "recorder-for-jetkvm_{}.png",
        Local::now().format("%Y-%m-%d_%H-%M-%S")
    );

    default_pictures_dir().join(filename)
}

fn default_pictures_dir() -> PathBuf {
    UserDirs::new()
        .and_then(|dirs| dirs.picture_dir().map(Path::to_path_buf))
        .unwrap_or_else(|| fallback_home_dir("Pictures"))
}

fn fallback_home_dir(default_leaf: &str) -> PathBuf {
    BaseDirs::new()
        .map(|dirs| dirs.home_dir().join(default_leaf))
        .unwrap_or_else(|| PathBuf::from(default_leaf))
}

#[cfg(target_os = "macos")]
fn default_video_dir_name() -> &'static str {
    "Movies"
}

#[cfg(not(target_os = "macos"))]
fn default_video_dir_name() -> &'static str {
    "Videos"
}
