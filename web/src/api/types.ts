// API response types matching the Rust server

export interface ScanInfo {
  scan_id: string;
  scan_dir: string;
  table_name: string;
  source_url: string | null;
  total_entries: number | null;
  scan_timestamp_us: number | null;
  parquet_files: string[];
}

export interface ScansResponse {
  scans: ScanInfo[];
  count: number;
  default_scan: string | null;
}

export interface ParamDef {
  name: string;
  description: string;
  param_type: 'integer' | 'string';
  default: string | null;
  required: boolean;
}

export interface QueryDef {
  id: string;
  name: string;
  description: string;
  category: string;
  params: ParamDef[];
}

export interface QueriesResponse {
  queries: QueryDef[];
  count: number;
}

export interface QueryResult {
  query_id: string;
  query_name: string;
  execution_ms: number;
  row_count: number;
  columns: string[];
  rows: Record<string, unknown>[];
}

export interface BatchResultItem {
  status: 'ok' | 'error';
  result?: QueryResult;
  query_id?: string;
  error?: string;
}

export interface BatchResponse {
  results: BatchResultItem[];
  count: number;
}

export interface BatchQueryItem {
  query_id: string;
  params?: Record<string, string>;
}
