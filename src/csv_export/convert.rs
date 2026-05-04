//! RocksDB to CSV conversion
//!
//! Streams entries from RocksDB directly to CSV files with optional gzip
//! compression and automatic file splitting by row count.

use crate::error::{CsvError, WalkerError};
use crate::rocksdb::schema::{RocksEntry, RocksHandle};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use tracing::info;

const BUF_SIZE: usize = 128 * 1024; // 128KB write buffer

/// CSV column headers
const CSV_HEADER: &[&str] = &[
    "path",
    "filename",
    "extension",
    "inode",
    "file_type",
    "size",
    "allocated_blocks",
    "nlink",
    "uid",
    "gid",
    "permissions",
    "mtime_us",
    "atime_us",
    "ctime_us",
    "depth",
    "parent_path",
    "checksum",
    "mime_type",
];

/// Configuration for CSV export
pub struct CsvExportConfig {
    /// Maximum rows per output file before splitting
    pub rows_per_file: usize,
    /// Enable gzip compression (.csv.gz)
    pub gzip: bool,
    /// Gzip compression level (1-9, only used if gzip=true)
    pub gzip_level: u32,
    /// Show progress
    pub progress: bool,
}

impl Default for CsvExportConfig {
    fn default() -> Self {
        Self {
            rows_per_file: 10_000_000,
            gzip: false,
            gzip_level: 6,
            progress: true,
        }
    }
}

/// Statistics from an export operation
#[derive(Debug, Clone)]
pub struct CsvExportStats {
    pub entries_exported: u64,
    pub files_written: u32,
    pub total_bytes_written: u64,
}

/// Progress callback type
pub type ProgressCallback = Box<dyn Fn(u64, u64) + Send>;

/// Writer abstraction for plain or gzip-compressed CSV output
enum CsvOutput {
    Plain(csv::Writer<BufWriter<File>>),
    Gzip(csv::Writer<BufWriter<GzEncoder<File>>>),
}

impl CsvOutput {
    fn write_record(&mut self, record: &[&str]) -> Result<(), CsvError> {
        match self {
            CsvOutput::Plain(w) => w.write_record(record).map_err(CsvError::Csv),
            CsvOutput::Gzip(w) => w.write_record(record).map_err(CsvError::Csv),
        }
    }

    fn flush_and_close(self) -> Result<(), CsvError> {
        match self {
            CsvOutput::Plain(w) => {
                let mut buf = w.into_inner().map_err(|e| CsvError::Io(e.into_error()))?;
                buf.flush().map_err(CsvError::Io)?;
            }
            CsvOutput::Gzip(w) => {
                let mut buf = w.into_inner().map_err(|e| CsvError::Io(e.into_error()))?;
                buf.flush().map_err(CsvError::Io)?;
                let gz = buf.into_inner().map_err(|e| CsvError::Io(e.into_error()))?;
                gz.finish().map_err(CsvError::Io)?;
            }
        }
        Ok(())
    }
}

/// Open a new CSV part file writer.
fn open_csv_part(
    output_dir: &Path,
    part_number: u32,
    gzip: bool,
    gzip_level: u32,
) -> Result<CsvOutput, CsvError> {
    let ext = if gzip { "csv.gz" } else { "csv" };
    let filename = format!("part-{:05}.{}", part_number, ext);
    let path = output_dir.join(&filename);
    let file = File::create(&path).map_err(CsvError::Io)?;

    if gzip {
        let gz = GzEncoder::new(file, Compression::new(gzip_level));
        let buf = BufWriter::with_capacity(BUF_SIZE, gz);
        Ok(CsvOutput::Gzip(csv::Writer::from_writer(buf)))
    } else {
        let buf = BufWriter::with_capacity(BUF_SIZE, file);
        Ok(CsvOutput::Plain(csv::Writer::from_writer(buf)))
    }
}

/// Convert entry_type u8 to a human-readable string.
fn file_type_string(entry_type: u8) -> &'static str {
    match entry_type {
        0 => "file",
        1 => "directory",
        2 => "symlink",
        _ => "other",
    }
}

/// Extract the parent path from a full path.
fn compute_parent_path(path: &str) -> &str {
    if path == "/" || !path.contains('/') {
        return "/";
    }
    match path.rfind('/') {
        Some(0) => "/",
        Some(pos) => &path[..pos],
        None => "/",
    }
}

/// Format an optional i64 as a string, empty for None.
fn opt_i64_str(val: Option<i64>) -> String {
    match val {
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

/// Write a single RocksEntry as a CSV row.
fn write_entry(
    writer: &mut CsvOutput,
    entry: &RocksEntry,
) -> Result<(), CsvError> {
    let inode_s = entry.inode.to_string();
    let size_s = entry.size.to_string();
    let blocks_s = entry.blocks.to_string();
    let nlink_s = entry.nlink.unwrap_or(1).to_string();
    let uid_s = entry.uid.unwrap_or(0).to_string();
    let gid_s = entry.gid.unwrap_or(0).to_string();
    let perms_s = format!("{:04o}", entry.mode.map(|m| m & 0o7777).unwrap_or(0o644));
    let mtime_s = opt_i64_str(entry.mtime);
    let atime_s = opt_i64_str(entry.atime);
    let ctime_s = opt_i64_str(entry.ctime);
    let depth_s = entry.depth.to_string();
    let parent = compute_parent_path(&entry.path);

    writer.write_record(&[
        &entry.path,
        &entry.name,
        entry.extension.as_deref().unwrap_or(""),
        &inode_s,
        file_type_string(entry.entry_type),
        &size_s,
        &blocks_s,
        &nlink_s,
        &uid_s,
        &gid_s,
        &perms_s,
        &mtime_s,
        &atime_s,
        &ctime_s,
        &depth_s,
        parent,
        entry.checksum.as_deref().unwrap_or(""),
        entry.file_type.as_deref().unwrap_or(""),
    ])
}

/// Convert a RocksDB database to CSV files.
///
/// Streams entries one at a time to CSV with automatic file splitting
/// when `rows_per_file` is reached. Each file includes its own header row.
pub fn convert_rocks_to_csv<P1, P2>(
    rocks_path: P1,
    output_dir: P2,
    config: CsvExportConfig,
    progress_callback: Option<ProgressCallback>,
) -> Result<CsvExportStats, WalkerError>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let rocks_path = rocks_path.as_ref();
    let output_dir = output_dir.as_ref();

    info!("Opening RocksDB: {}", rocks_path.display());
    let rocks = RocksHandle::open_readonly(rocks_path).map_err(|e| {
        WalkerError::Csv(CsvError::Other(format!("Failed to open RocksDB: {}", e)))
    })?;

    // Create output directory
    fs::create_dir_all(output_dir).map_err(CsvError::Io)?;

    info!("Exporting to CSV: {}", output_dir.display());

    // Open first part file
    let mut part_number: u32 = 0;
    let mut writer = open_csv_part(output_dir, part_number, config.gzip, config.gzip_level)?;
    writer.write_record(CSV_HEADER)?;
    part_number += 1;

    let mut total_exported: u64 = 0;
    let mut rows_in_current_file: usize = 0;
    let mut progress_counter: u64 = 0;

    for result in rocks.iter_by_path() {
        let entry = result.map_err(|e| {
            WalkerError::Csv(CsvError::Other(format!("Failed to read entry: {}", e)))
        })?;

        // Split to new file before writing if current file is full
        if rows_in_current_file >= config.rows_per_file {
            writer.flush_and_close()?;
            writer = open_csv_part(output_dir, part_number, config.gzip, config.gzip_level)?;
            writer.write_record(CSV_HEADER)?;
            part_number += 1;
            rows_in_current_file = 0;
        }

        write_entry(&mut writer, &entry)?;

        total_exported += 1;
        rows_in_current_file += 1;

        // Progress reporting every 100K entries
        progress_counter += 1;
        if progress_counter >= 100_000 {
            if let Some(ref cb) = progress_callback {
                cb(total_exported, 0);
            }
            progress_counter = 0;
        }
    }

    // Close final file
    writer.flush_and_close()?;

    // Final progress
    if let Some(cb) = progress_callback {
        cb(total_exported, total_exported);
    }

    let total_bytes = recalculate_total_bytes(output_dir);

    info!(
        "Export complete: {} entries in {} files ({} bytes)",
        total_exported, part_number, total_bytes
    );

    Ok(CsvExportStats {
        entries_exported: total_exported,
        files_written: part_number,
        total_bytes_written: total_bytes,
    })
}

/// Recalculate total bytes of all files in a directory.
fn recalculate_total_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.metadata().ok().map(|m| m.len()))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::{DbEntry, EntryType};
    use crate::rocksdb::schema::meta_keys;
    use crate::rocksdb::writer::{RocksWriter, RocksWriterConfig};
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn create_test_rocks(dir: &Path) -> PathBuf {
        let rocks_path = dir.join("test.rocks");
        let config = RocksWriterConfig::default();
        let writer = RocksWriter::open(&rocks_path, config).unwrap();

        let entries = vec![
            DbEntry {
                parent_path: Some("/".to_string()),
                name: "file1.txt".to_string(),
                path: "/file1.txt".to_string(),
                entry_type: EntryType::File,
                size: 1024,
                mtime: Some(1700000000),
                atime: Some(1700000100),
                ctime: Some(1699999000),
                mode: Some(0o644),
                uid: Some(1000),
                gid: Some(1000),
                nlink: Some(1),
                inode: 100,
                depth: 1,
                extension: Some("txt".to_string()),
                blocks: 8,
                checksum: None,
                file_type: None,
            },
            DbEntry {
                parent_path: Some("/".to_string()),
                name: "dir1".to_string(),
                path: "/dir1".to_string(),
                entry_type: EntryType::Directory,
                size: 4096,
                mtime: Some(1700000000),
                atime: None,
                ctime: None,
                mode: Some(0o755),
                uid: Some(0),
                gid: Some(0),
                nlink: Some(2),
                inode: 200,
                depth: 1,
                extension: None,
                blocks: 8,
                checksum: None,
                file_type: None,
            },
            DbEntry {
                parent_path: Some("/dir1".to_string()),
                name: "nested.log".to_string(),
                path: "/dir1/nested.log".to_string(),
                entry_type: EntryType::File,
                size: 2048,
                mtime: Some(1700001000),
                atime: None,
                ctime: Some(1700001000),
                mode: Some(0o600),
                uid: Some(1000),
                gid: Some(100),
                nlink: None,
                inode: 300,
                depth: 2,
                extension: Some("log".to_string()),
                blocks: 16,
                checksum: None,
                file_type: None,
            },
        ];

        writer
            .set_metadata(meta_keys::SOURCE_URL, "nfs://test/export")
            .unwrap();
        writer
            .set_metadata(meta_keys::START_TIME, "1700000000")
            .unwrap();
        writer
            .set_metadata(meta_keys::TOTAL_FILES, "2")
            .unwrap();
        writer
            .set_metadata(meta_keys::TOTAL_DIRS, "1")
            .unwrap();
        writer.write_batch(&entries).unwrap();
        writer.flush().unwrap();
        drop(writer);

        rocks_path
    }

    #[test]
    fn test_export_creates_csv_files() {
        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("csv_output");

        let config = CsvExportConfig {
            rows_per_file: 10_000_000,
            gzip: false,
            gzip_level: 6,
            progress: false,
        };

        let stats = convert_rocks_to_csv(&rocks_path, &output_dir, config, None).unwrap();

        assert_eq!(stats.entries_exported, 3);
        assert_eq!(stats.files_written, 1);
        assert!(stats.total_bytes_written > 0);
        assert!(output_dir.join("part-00000.csv").exists());
    }

    #[test]
    fn test_export_file_splitting() {
        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("csv_output");

        let config = CsvExportConfig {
            rows_per_file: 2, // Force split after 2 rows
            gzip: false,
            gzip_level: 6,
            progress: false,
        };

        let stats = convert_rocks_to_csv(&rocks_path, &output_dir, config, None).unwrap();

        assert_eq!(stats.entries_exported, 3);
        assert_eq!(stats.files_written, 2);
        assert!(output_dir.join("part-00000.csv").exists());
        assert!(output_dir.join("part-00001.csv").exists());
    }

    #[test]
    fn test_export_csv_content() {
        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("csv_output");

        let config = CsvExportConfig::default();
        convert_rocks_to_csv(&rocks_path, &output_dir, config, None).unwrap();

        // Read back and verify header + content
        let content = std::fs::read_to_string(output_dir.join("part-00000.csv")).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // Header row
        assert!(lines[0].starts_with("path,filename,extension,"));
        // Should have header + 3 data rows
        assert_eq!(lines.len(), 4);
    }

    #[test]
    fn test_export_with_gzip() {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("csv_output");

        let config = CsvExportConfig {
            rows_per_file: 10_000_000,
            gzip: true,
            gzip_level: 6,
            progress: false,
        };

        let stats = convert_rocks_to_csv(&rocks_path, &output_dir, config, None).unwrap();

        assert_eq!(stats.entries_exported, 3);
        assert!(output_dir.join("part-00000.csv.gz").exists());

        // Decompress and verify
        let file = File::open(output_dir.join("part-00000.csv.gz")).unwrap();
        let mut decoder = GzDecoder::new(file);
        let mut content = String::new();
        decoder.read_to_string(&mut content).unwrap();

        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 4); // header + 3 rows
        assert!(lines[0].starts_with("path,filename,extension,"));
    }

    #[test]
    fn test_export_with_progress_callback() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("csv_output");

        let config = CsvExportConfig::default();

        let last_count = Arc::new(AtomicU64::new(0));
        let cb_count = last_count.clone();
        let callback: ProgressCallback = Box::new(move |exported, _total| {
            cb_count.store(exported, Ordering::SeqCst);
        });

        let stats =
            convert_rocks_to_csv(&rocks_path, &output_dir, config, Some(callback)).unwrap();

        // Final callback should have been called with total entries
        assert_eq!(last_count.load(Ordering::SeqCst), stats.entries_exported);
    }

    #[test]
    fn test_each_split_file_has_header() {
        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("csv_output");

        let config = CsvExportConfig {
            rows_per_file: 1, // Split every row
            gzip: false,
            gzip_level: 6,
            progress: false,
        };

        let stats = convert_rocks_to_csv(&rocks_path, &output_dir, config, None).unwrap();
        assert_eq!(stats.files_written, 3);

        // Each file should have a header
        for i in 0..3 {
            let content =
                std::fs::read_to_string(output_dir.join(format!("part-{:05}.csv", i))).unwrap();
            let first_line = content.lines().next().unwrap();
            assert!(
                first_line.starts_with("path,filename,extension,"),
                "File part-{:05}.csv missing header",
                i
            );
        }
    }
}
