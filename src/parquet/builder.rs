//! Shared row builder for Parquet output.
//!
//! Both the post-scan converter (`convert_rocks_to_parquet`) and the
//! streaming writer used during a live scan accumulate rows into the
//! same 18-column Arrow schema and then flush them as a `RecordBatch`.
//! This module owns the column-builder boilerplate so neither path
//! has to duplicate it.

use crate::error::{ParquetError, WalkerError};
use crate::nfs::types::DbEntry;
use crate::parquet::schema::{compute_parent_path, file_type_string, parquet_schema_ref};
use crate::rocksdb::schema::RocksEntry;
use arrow::array::{
    ArrayRef, Int32Builder, Int64Builder, StringBuilder, StringDictionaryBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{Schema, UInt32Type};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Per-row identity bytes that travel with every flush: the scan UUID
/// and the scan start timestamp. Cheap to clone (`scan_id` is short).
#[derive(Debug, Clone)]
pub struct RowContext {
    pub scan_id: String,
    pub scan_timestamp_us: i64,
    /// Multiplier applied to incoming mtime/atime/ctime values before
    /// writing to the Parquet `*_us` columns. `1` for fresh databases
    /// (already microseconds); `1_000_000` for legacy databases that
    /// stored only seconds. The exporter chooses based on the
    /// `MTIME_FORMAT` metadata key; `Default::default` is the
    /// pass-through case so unit-test fixtures don't need to set it.
    pub mtime_scale: i64,
}

impl Default for RowContext {
    fn default() -> Self {
        Self {
            scan_id: String::new(),
            scan_timestamp_us: 0,
            mtime_scale: 1,
        }
    }
}

/// Accumulates rows into Arrow column builders and produces a
/// `RecordBatch` on demand. Re-usable across batch boundaries -- the
/// `finish()` method moves out and resets each builder so the same
/// `RowBuilder` keeps writing into a fresh batch.
pub struct RowBuilder {
    schema: Arc<Schema>,
    ctx: RowContext,
    rows: usize,

    b_path: StringBuilder,
    b_filename: StringBuilder,
    b_extension: StringBuilder,
    b_inode: UInt64Builder,
    b_file_type: StringBuilder,
    b_size: UInt64Builder,
    b_alloc_blocks: UInt64Builder,
    b_nlink: UInt32Builder,
    b_uid: UInt32Builder,
    b_gid: UInt32Builder,
    b_permissions: UInt16Builder,
    b_mtime: Int64Builder,
    b_atime: Int64Builder,
    b_ctime: Int64Builder,
    b_mtime_sec: Int64Builder,
    b_mtime_nsec: Int32Builder,
    b_atime_sec: Int64Builder,
    b_atime_nsec: Int32Builder,
    b_ctime_sec: Int64Builder,
    b_ctime_nsec: Int32Builder,
    b_depth: UInt16Builder,
    b_parent_path: StringBuilder,
    /// scan_id is dictionary-encoded — every row in a given batch
    /// shares the same UUID, so the builder produces a 1-entry
    /// dictionary plus a tiny index array instead of materializing
    /// ~9 MB of repeated bytes per row group.
    b_scan_id: StringDictionaryBuilder<UInt32Type>,
    b_scan_ts: Int64Builder,
}

impl RowBuilder {
    pub fn new(ctx: RowContext) -> Self {
        Self {
            schema: parquet_schema_ref(),
            ctx,
            rows: 0,
            b_path: StringBuilder::new(),
            b_filename: StringBuilder::new(),
            b_extension: StringBuilder::new(),
            b_inode: UInt64Builder::new(),
            b_file_type: StringBuilder::new(),
            b_size: UInt64Builder::new(),
            b_alloc_blocks: UInt64Builder::new(),
            b_nlink: UInt32Builder::new(),
            b_uid: UInt32Builder::new(),
            b_gid: UInt32Builder::new(),
            b_permissions: UInt16Builder::new(),
            b_mtime: Int64Builder::new(),
            b_atime: Int64Builder::new(),
            b_ctime: Int64Builder::new(),
            b_mtime_sec: Int64Builder::new(),
            b_mtime_nsec: Int32Builder::new(),
            b_atime_sec: Int64Builder::new(),
            b_atime_nsec: Int32Builder::new(),
            b_ctime_sec: Int64Builder::new(),
            b_ctime_nsec: Int32Builder::new(),
            b_depth: UInt16Builder::new(),
            b_parent_path: StringBuilder::new(),
            b_scan_id: StringDictionaryBuilder::<UInt32Type>::new(),
            b_scan_ts: Int64Builder::new(),
        }
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn row_count(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Append a row sourced from a `RocksEntry` (used by the post-scan
    /// converter, which iterates RocksDB directly).
    pub fn push_rocks_entry(&mut self, entry: &RocksEntry) {
        let parent = compute_parent_path(&entry.path);
        self.append_common(
            &entry.path,
            &entry.name,
            entry.extension.as_deref(),
            entry.inode,
            entry.entry_type,
            entry.size,
            entry.blocks,
            entry.nlink,
            entry.uid,
            entry.gid,
            entry.mode,
            entry.mtime,
            entry.atime,
            entry.ctime,
            entry.mtime_sec,
            entry.mtime_nsec,
            entry.atime_sec,
            entry.atime_nsec,
            entry.ctime_sec,
            entry.ctime_nsec,
            entry.depth,
            parent,
        );
    }

    /// Append a row sourced from a `DbEntry` (used by the streaming
    /// writer fed from the walker pipeline).
    pub fn push_db_entry(&mut self, entry: &DbEntry) {
        // Prefer the writer-supplied parent_path; fall back to recomputing
        // from the path string when it's None (root entries). The hot
        // path is `as_deref()` which doesn't allocate — the
        // recompute-from-path branch only fires for root-ish entries.
        let parent: &str = entry
            .parent_path
            .as_deref()
            .unwrap_or_else(|| compute_parent_path(&entry.path));
        self.append_common(
            &entry.path,
            &entry.name,
            entry.extension.as_deref(),
            entry.inode,
            entry.entry_type as u8,
            entry.size,
            entry.blocks,
            entry.nlink,
            entry.uid,
            entry.gid,
            entry.mode,
            entry.mtime,
            entry.atime,
            entry.ctime,
            entry.mtime_sec,
            entry.mtime_nsec,
            entry.atime_sec,
            entry.atime_nsec,
            entry.ctime_sec,
            entry.ctime_nsec,
            entry.depth,
            parent,
        );
    }

    /// Move the column builders into a `RecordBatch` and reset row count.
    /// After calling this the `RowBuilder` is empty and ready to accumulate
    /// the next batch.
    pub fn finish(&mut self) -> Result<RecordBatch, WalkerError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.b_path.finish()),
            Arc::new(self.b_filename.finish()),
            Arc::new(self.b_extension.finish()),
            Arc::new(self.b_inode.finish()),
            Arc::new(self.b_file_type.finish()),
            Arc::new(self.b_size.finish()),
            Arc::new(self.b_alloc_blocks.finish()),
            Arc::new(self.b_nlink.finish()),
            Arc::new(self.b_uid.finish()),
            Arc::new(self.b_gid.finish()),
            Arc::new(self.b_permissions.finish()),
            Arc::new(self.b_mtime.finish()),
            Arc::new(self.b_atime.finish()),
            Arc::new(self.b_ctime.finish()),
            Arc::new(self.b_mtime_sec.finish()),
            Arc::new(self.b_mtime_nsec.finish()),
            Arc::new(self.b_atime_sec.finish()),
            Arc::new(self.b_atime_nsec.finish()),
            Arc::new(self.b_ctime_sec.finish()),
            Arc::new(self.b_ctime_nsec.finish()),
            Arc::new(self.b_depth.finish()),
            Arc::new(self.b_parent_path.finish()),
            Arc::new(self.b_scan_id.finish()),
            Arc::new(self.b_scan_ts.finish()),
        ];

        self.rows = 0;
        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| WalkerError::Parquet(ParquetError::Arrow(e)))
    }

    #[allow(clippy::too_many_arguments)]
    fn append_common(
        &mut self,
        path: &str,
        name: &str,
        extension: Option<&str>,
        inode: u64,
        entry_type: u8,
        size: u64,
        blocks: u64,
        nlink: Option<u64>,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
        mtime: Option<i64>,
        atime: Option<i64>,
        ctime: Option<i64>,
        mtime_sec: Option<i64>,
        mtime_nsec: Option<i32>,
        atime_sec: Option<i64>,
        atime_nsec: Option<i32>,
        ctime_sec: Option<i64>,
        ctime_nsec: Option<i32>,
        depth: u32,
        parent_path: &str,
    ) {
        self.b_path.append_value(path);
        self.b_filename.append_value(name);
        match extension {
            Some(ext) => self.b_extension.append_value(ext),
            None => self.b_extension.append_null(),
        }
        self.b_inode.append_value(inode);
        self.b_file_type.append_value(file_type_string(entry_type));
        self.b_size.append_value(size);
        self.b_alloc_blocks.append_value(blocks);
        self.b_nlink.append_value(nlink.unwrap_or(1) as u32);
        self.b_uid.append_value(uid.unwrap_or(0));
        self.b_gid.append_value(gid.unwrap_or(0));
        self.b_permissions
            .append_value(mode.map(|m| (m & 0o7777) as u16).unwrap_or(0o644));
        let scale = self.ctx.mtime_scale;
        match mtime {
            Some(t) => self.b_mtime.append_value(t.saturating_mul(scale)),
            None => self.b_mtime.append_null(),
        }
        match atime {
            Some(t) => self.b_atime.append_value(t.saturating_mul(scale)),
            None => self.b_atime.append_null(),
        }
        match ctime {
            Some(t) => self.b_ctime.append_value(t.saturating_mul(scale)),
            None => self.b_ctime.append_null(),
        }
        // High-precision time companions. No mtime_scale applied — these
        // carry libnfs's full nanosecond precision directly. None for
        // legacy rows that didn't capture nsec.
        match mtime_sec {
            Some(v) => self.b_mtime_sec.append_value(v),
            None => self.b_mtime_sec.append_null(),
        }
        match mtime_nsec {
            Some(v) => self.b_mtime_nsec.append_value(v),
            None => self.b_mtime_nsec.append_null(),
        }
        match atime_sec {
            Some(v) => self.b_atime_sec.append_value(v),
            None => self.b_atime_sec.append_null(),
        }
        match atime_nsec {
            Some(v) => self.b_atime_nsec.append_value(v),
            None => self.b_atime_nsec.append_null(),
        }
        match ctime_sec {
            Some(v) => self.b_ctime_sec.append_value(v),
            None => self.b_ctime_sec.append_null(),
        }
        match ctime_nsec {
            Some(v) => self.b_ctime_nsec.append_value(v),
            None => self.b_ctime_nsec.append_null(),
        }
        self.b_depth.append_value(depth as u16);
        self.b_parent_path.append_value(parent_path);
        self.b_scan_id.append_value(&self.ctx.scan_id);
        self.b_scan_ts.append_value(self.ctx.scan_timestamp_us);

        self.rows += 1;
    }
}
