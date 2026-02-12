// Query ID constants for type-safe references
export const Q = {
  // Capacity & Volume
  ENTRY_COUNTS: 'Q-001',
  CAPACITY_SUMMARY: 'Q-002',
  CAPACITY_BY_DEPTH: 'Q-003',
  INODES_BY_TYPE: 'Q-004',
  CAPACITY_BY_TOP_DIR: 'Q-005',
  HARD_LINKS: 'Q-006',
  DUPLICATE_INODES: 'Q-007',
  SPACE_WASTE: 'Q-008',

  // Ownership & Access
  USAGE_BY_UID: 'Q-010',
  USAGE_BY_GID: 'Q-011',
  WORLD_WRITABLE: 'Q-012',
  UID_EXTENSION: 'Q-013',
  OWNERSHIP_CONCENTRATION: 'Q-015',

  // Directory Structure
  WIDEST_DIRS: 'Q-020',
  DEEPEST_PATHS: 'Q-021',
  EMPTY_DIRS: 'Q-022',
  DEPTH_DISTRIBUTION: 'Q-025',
  FANOUT_DISTRIBUTION: 'Q-027',

  // File Size
  SIZE_HISTOGRAM: 'Q-030',
  LARGEST_FILES: 'Q-031',
  ZERO_BYTE: 'Q-032',
  SIZE_PERCENTILES: 'Q-033',
  SMALL_FILES: 'Q-034',
  LARGEST_BY_EXT: 'Q-036',

  // File Type & Extension
  COUNT_BY_EXT: 'Q-040',
  SIZE_BY_EXT: 'Q-041',
  TEMP_JUNK: 'Q-044',

  // Time-Based
  AGE_HISTOGRAM: 'Q-050',
  COLD_DATA: 'Q-051',
  HOT_DATA: 'Q-052',
  STALE_BY_OWNER: 'Q-053',
  GROWTH_BY_MONTH: 'Q-055',

  // Storage Efficiency
  RECLAIMABLE: 'Q-064',

  // Migration & Compliance
  CLASSIFICATION: 'Q-073',

  // Scan Operations
  SCAN_META: 'Q-080',
  FANOUT_STATS: 'Q-081',
} as const;
