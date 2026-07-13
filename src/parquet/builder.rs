//! Shared row builder for Parquet output.
//!
//! The streaming writer fed by the walker pipeline accumulates rows into
//! the 24-column Arrow schema and then flushes them as a `RecordBatch`.
//! This module owns the column-builder boilerplate.

use crate::error::{ParquetError, WalkerError};
use crate::nfs::types::DbEntry;
use crate::parquet::schema::{compute_parent_path, file_type_string, parquet_schema_ref};
use arrow::array::{
    ArrayRef, Int32Builder, Int64Builder, StringBuilder, StringDictionaryBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{Schema, UInt32Type};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Pack a (seconds, nanoseconds) timestamp into microseconds-since-epoch
/// for the legacy `*_us` columns. `nsec` is clamped to
/// `[0, 999_999_999]` so garbage from a malformed stat struct can't
/// corrupt the microsecond math.
#[inline]
pub fn pack_micros(sec: i64, nsec: i64) -> i64 {
    let nsec = nsec.clamp(0, 999_999_999);
    sec.saturating_mul(1_000_000)
        .saturating_add(nsec / 1_000)
}

/// Derive a nullable `*_us` column value from a (sec, nsec) pair.
#[inline]
fn micros_from_parts(sec: Option<i64>, nsec: Option<i32>) -> Option<i64> {
    sec.map(|s| pack_micros(s, nsec.unwrap_or(0) as i64))
}

/// Per-row identity bytes that travel with every flush: the scan UUID
/// and the scan start timestamp. Cheap to clone (`scan_id` is short).
#[derive(Debug, Clone, Default)]
pub struct RowContext {
    pub scan_id: String,
    pub scan_timestamp_us: i64,
}

/// Bytes-per-row guesses for the string columns' data buffers. These
/// only set the initial reservation; underestimating costs a couple of
/// tail reallocs, overestimating strands memory across every shard.
const PATH_BYTES_PER_ROW: usize = 48;
const NAME_BYTES_PER_ROW: usize = 16;
const EXT_BYTES_PER_ROW: usize = 4;

/// Accumulates rows into Arrow column builders and produces a
/// `RecordBatch` on demand. Re-usable across batch boundaries -- the
/// `finish()` method moves out each builder and replaces it with a
/// fresh pre-sized one, so the same `RowBuilder` keeps writing into a
/// fresh batch without re-growing every column from zero.
pub struct RowBuilder {
    schema: Arc<Schema>,
    ctx: RowContext,
    /// Row-count hint used to pre-size every column builder (typically
    /// the writer's row-group size).
    capacity: usize,
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
    /// `capacity` should be the writer's row-group size so a full group
    /// accumulates without any column-buffer reallocation.
    pub fn new(ctx: RowContext, capacity: usize) -> Self {
        Self {
            schema: parquet_schema_ref(),
            ctx,
            capacity,
            rows: 0,
            b_path: StringBuilder::with_capacity(capacity, capacity * PATH_BYTES_PER_ROW),
            b_filename: StringBuilder::with_capacity(capacity, capacity * NAME_BYTES_PER_ROW),
            b_extension: StringBuilder::with_capacity(capacity, capacity * EXT_BYTES_PER_ROW),
            b_inode: UInt64Builder::with_capacity(capacity),
            b_file_type: StringBuilder::with_capacity(capacity, capacity * EXT_BYTES_PER_ROW),
            b_size: UInt64Builder::with_capacity(capacity),
            b_alloc_blocks: UInt64Builder::with_capacity(capacity),
            b_nlink: UInt32Builder::with_capacity(capacity),
            b_uid: UInt32Builder::with_capacity(capacity),
            b_gid: UInt32Builder::with_capacity(capacity),
            b_permissions: UInt16Builder::with_capacity(capacity),
            b_mtime: Int64Builder::with_capacity(capacity),
            b_atime: Int64Builder::with_capacity(capacity),
            b_ctime: Int64Builder::with_capacity(capacity),
            b_mtime_sec: Int64Builder::with_capacity(capacity),
            b_mtime_nsec: Int32Builder::with_capacity(capacity),
            b_atime_sec: Int64Builder::with_capacity(capacity),
            b_atime_nsec: Int32Builder::with_capacity(capacity),
            b_ctime_sec: Int64Builder::with_capacity(capacity),
            b_ctime_nsec: Int32Builder::with_capacity(capacity),
            b_depth: UInt16Builder::with_capacity(capacity),
            b_parent_path: StringBuilder::with_capacity(capacity, capacity * PATH_BYTES_PER_ROW),
            b_scan_id: StringDictionaryBuilder::<UInt32Type>::new(),
            b_scan_ts: Int64Builder::with_capacity(capacity),
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

    /// Append a row sourced from a `DbEntry` (the streaming writer fed
    /// from the walker pipeline).
    pub fn push_db_entry(&mut self, entry: &DbEntry) {
        // Prefer the walker-supplied parent_path; recompute zero-copy
        // from the path string when it's None (the common case — the
        // walker deliberately skips the per-entry clone).
        let parent: &str = entry
            .parent_path
            .as_deref()
            .unwrap_or_else(|| compute_parent_path(&entry.path));

        self.b_path.append_value(&entry.path);
        self.b_filename.append_value(&entry.name);
        match entry.extension.as_deref() {
            Some(ext) => self.b_extension.append_value(ext),
            None => self.b_extension.append_null(),
        }
        self.b_inode.append_value(entry.inode);
        self.b_file_type
            .append_value(file_type_string(entry.entry_type as u8));
        self.b_size.append_value(entry.size);
        self.b_alloc_blocks.append_value(entry.blocks);
        self.b_nlink.append_value(entry.nlink.unwrap_or(1) as u32);
        self.b_uid.append_value(entry.uid.unwrap_or(0));
        self.b_gid.append_value(entry.gid.unwrap_or(0));
        self.b_permissions
            .append_value(entry.mode.map(|m| (m & 0o7777) as u16).unwrap_or(0o644));
        // Legacy microsecond columns, derived from the (sec, nsec)
        // pairs the walker carries.
        self.b_mtime
            .append_option(micros_from_parts(entry.mtime_sec, entry.mtime_nsec));
        self.b_atime
            .append_option(micros_from_parts(entry.atime_sec, entry.atime_nsec));
        self.b_ctime
            .append_option(micros_from_parts(entry.ctime_sec, entry.ctime_nsec));
        self.b_mtime_sec.append_option(entry.mtime_sec);
        self.b_mtime_nsec.append_option(entry.mtime_nsec);
        self.b_atime_sec.append_option(entry.atime_sec);
        self.b_atime_nsec.append_option(entry.atime_nsec);
        self.b_ctime_sec.append_option(entry.ctime_sec);
        self.b_ctime_nsec.append_option(entry.ctime_nsec);
        self.b_depth
            .append_value(entry.depth.min(u16::MAX as u32) as u16);
        self.b_parent_path.append_value(parent);
        self.b_scan_id.append_value(&self.ctx.scan_id);
        self.b_scan_ts.append_value(self.ctx.scan_timestamp_us);

        self.rows += 1;
    }

    /// Move the column builders into a `RecordBatch` and replace them
    /// with fresh pre-sized ones. After calling this the `RowBuilder`
    /// is empty and ready to accumulate the next batch without
    /// re-growing any column from zero.
    pub fn finish(&mut self) -> Result<RecordBatch, WalkerError> {
        let fresh = RowBuilder::new(self.ctx.clone(), self.capacity);
        let full = std::mem::replace(self, fresh);

        let RowBuilder {
            schema,
            mut b_path,
            mut b_filename,
            mut b_extension,
            mut b_inode,
            mut b_file_type,
            mut b_size,
            mut b_alloc_blocks,
            mut b_nlink,
            mut b_uid,
            mut b_gid,
            mut b_permissions,
            mut b_mtime,
            mut b_atime,
            mut b_ctime,
            mut b_mtime_sec,
            mut b_mtime_nsec,
            mut b_atime_sec,
            mut b_atime_nsec,
            mut b_ctime_sec,
            mut b_ctime_nsec,
            mut b_depth,
            mut b_parent_path,
            mut b_scan_id,
            mut b_scan_ts,
            ..
        } = full;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(b_path.finish()),
            Arc::new(b_filename.finish()),
            Arc::new(b_extension.finish()),
            Arc::new(b_inode.finish()),
            Arc::new(b_file_type.finish()),
            Arc::new(b_size.finish()),
            Arc::new(b_alloc_blocks.finish()),
            Arc::new(b_nlink.finish()),
            Arc::new(b_uid.finish()),
            Arc::new(b_gid.finish()),
            Arc::new(b_permissions.finish()),
            Arc::new(b_mtime.finish()),
            Arc::new(b_atime.finish()),
            Arc::new(b_ctime.finish()),
            Arc::new(b_mtime_sec.finish()),
            Arc::new(b_mtime_nsec.finish()),
            Arc::new(b_atime_sec.finish()),
            Arc::new(b_atime_nsec.finish()),
            Arc::new(b_ctime_sec.finish()),
            Arc::new(b_ctime_nsec.finish()),
            Arc::new(b_depth.finish()),
            Arc::new(b_parent_path.finish()),
            Arc::new(b_scan_id.finish()),
            Arc::new(b_scan_ts.finish()),
        ];

        RecordBatch::try_new(schema, columns)
            .map_err(|e| WalkerError::Parquet(ParquetError::Arrow(e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_micros_combines_seconds_and_nanos() {
        assert_eq!(pack_micros(1_777_857_024, 0), 1_777_857_024_000_000);
        // 716680045 ns / 1000 = 716680 us.
        assert_eq!(
            pack_micros(1_777_857_024, 716_680_045),
            1_777_857_024_716_680
        );
    }

    #[test]
    fn pack_micros_clamps_out_of_range_nsec() {
        // Negative nsec must not subtract from sec*1e6.
        assert_eq!(pack_micros(1_000, -42), 1_000_000_000);
        // 1.5s of "nanoseconds" clamps to 999_999us.
        assert_eq!(pack_micros(1_000, 1_500_000_000), 1_000_000_000 + 999_999);
    }

    #[test]
    fn micros_from_parts_derives_and_nulls() {
        assert_eq!(
            micros_from_parts(Some(1_700_000_000), Some(500_500_500)),
            Some(1_700_000_000_500_500)
        );
        // Missing nsec: whole-second precision.
        assert_eq!(
            micros_from_parts(Some(1_700_000_000), None),
            Some(1_700_000_000_000_000)
        );
        // Missing sec: column is null.
        assert_eq!(micros_from_parts(None, Some(5)), None);
    }

    #[test]
    fn finish_resets_and_preserves_capacity_behavior() {
        let mut rb = RowBuilder::new(RowContext::default(), 16);
        for i in 0..3 {
            rb.push_db_entry(&DbEntry {
                path: format!("/a/file-{i}"),
                name: format!("file-{i}"),
                inode: i,
                ..DbEntry::default()
            });
        }
        assert_eq!(rb.row_count(), 3);
        let batch = rb.finish().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert!(rb.is_empty());
        // Builder is reusable after finish().
        rb.push_db_entry(&DbEntry {
            path: "/a/b".into(),
            name: "b".into(),
            ..DbEntry::default()
        });
        assert_eq!(rb.finish().unwrap().num_rows(), 1);
    }

    #[test]
    fn parent_path_derived_when_none() {
        let mut rb = RowBuilder::new(RowContext::default(), 4);
        rb.push_db_entry(&DbEntry {
            path: "/data/sub/file.txt".into(),
            name: "file.txt".into(),
            parent_path: None,
            ..DbEntry::default()
        });
        let batch = rb.finish().unwrap();
        let parents = batch
            .column_by_name("parent_path")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(parents.value(0), "/data/sub");
    }
}
