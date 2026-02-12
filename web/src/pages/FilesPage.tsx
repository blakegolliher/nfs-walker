import { useBatchQueries, getFirstRow, getRows } from '../hooks/useBatchQueries';
import { Q } from '../api/queries';
import { formatBytes, formatNumber } from '../lib/format';
import { MetricCard } from '../components/data/MetricCard';
import { MetricCardGrid } from '../components/data/MetricCardGrid';
import { BarChart } from '../components/charts/BarChart';
import { AreaChart } from '../components/charts/AreaChart';
import { HorizontalBarChart } from '../components/charts/HorizontalBarChart';
import { DataTable } from '../components/data/DataTable';
import { LoadingSpinner } from '../components/shared/LoadingSpinner';
import { ErrorBanner } from '../components/shared/ErrorBanner';

const QUERIES = [
  { query_id: Q.SIZE_HISTOGRAM },
  { query_id: Q.LARGEST_FILES, params: { limit: '20' } },
  { query_id: Q.ZERO_BYTE, params: { limit: '20' } },
  { query_id: Q.SIZE_PERCENTILES },
  { query_id: Q.SMALL_FILES },
  { query_id: Q.SIZE_BY_EXT, params: { limit: '15' } },
  { query_id: Q.COUNT_BY_EXT, params: { limit: '15' } },
  { query_id: Q.TEMP_JUNK },
  { query_id: Q.COLD_DATA, params: { limit: '15' } },
  { query_id: Q.HOT_DATA, params: { limit: '15' } },
  { query_id: Q.GROWTH_BY_MONTH, params: { limit: '24' } },
];

export function FilesPage() {
  const { data, isLoading, error } = useBatchQueries(QUERIES);

  if (isLoading) return <LoadingSpinner />;
  if (error) return <ErrorBanner message={String(error)} />;

  const pct = getFirstRow(data, Q.SIZE_PERCENTILES);
  const small = getFirstRow(data, Q.SMALL_FILES);
  const sizeHist = getRows(data, Q.SIZE_HISTOGRAM);
  const growth = getRows(data, Q.GROWTH_BY_MONTH)
    .map((r) => ({ ...r, label: `${r.year}-${String(r.month).padStart(2, '0')}` }))
    .reverse();
  const extBySize = getRows(data, Q.SIZE_BY_EXT);
  const extByCount = getRows(data, Q.COUNT_BY_EXT);
  const largest = getRows(data, Q.LARGEST_FILES);
  const zeroByte = getRows(data, Q.ZERO_BYTE);
  const tempJunk = getRows(data, Q.TEMP_JUNK);
  const cold = getRows(data, Q.COLD_DATA);
  const hot = getRows(data, Q.HOT_DATA);

  return (
    <div className="space-y-4">
      <MetricCardGrid>
        <MetricCard label="Median Size" value={formatBytes(pct?.p50)} />
        <MetricCard label="P90" value={formatBytes(pct?.p90)} />
        <MetricCard label="P99" value={formatBytes(pct?.p99)} />
        <MetricCard label="Max" value={formatBytes(pct?.max)} />
        <MetricCard label="Small Files" value={formatNumber(small?.small_files)} sub={`${formatBytes(small?.wasted_bytes)} wasted`} />
        <MetricCard label="Mean" value={formatBytes(pct?.mean)} />
      </MetricCardGrid>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <BarChart
          data={sizeHist}
          xKey="size_bucket"
          bars={[{ key: 'file_count', label: 'Files' }]}
          title="File Size Distribution"
          formatValue={formatNumber}
        />
        <AreaChart
          data={growth}
          xKey="label"
          areas={[{ key: 'bytes_created', label: 'Bytes Created' }]}
          title="Data Growth by Month"
          formatValue={formatBytes}
        />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <HorizontalBarChart
          data={extBySize.slice(0, 12)}
          nameKey="ext"
          valueKey="total_bytes"
          title="Extensions by Size"
          formatValue={formatBytes}
        />
        <HorizontalBarChart
          data={extByCount.slice(0, 12)}
          nameKey="ext"
          valueKey="count"
          title="Extensions by Count"
          color="#22c55e"
          formatValue={formatNumber}
        />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <DataTable
          title="Largest Files"
          columns={[
            { key: 'path', label: 'Path' },
            { key: 'size', label: 'Size', align: 'right', format: (v) => formatBytes(v) },
            { key: 'ext', label: 'Ext' },
            { key: 'uid', label: 'UID', align: 'right' },
          ]}
          rows={largest}
        />
        <DataTable
          title="Zero-Byte Files"
          columns={[
            { key: 'ext', label: 'Extension' },
            { key: 'count', label: 'Count', align: 'right', format: (v) => formatNumber(v) },
          ]}
          rows={zeroByte}
        />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <DataTable
          title="Temp / Junk Files"
          columns={[
            { key: 'ext', label: 'Extension' },
            { key: 'count', label: 'Count', align: 'right', format: (v) => formatNumber(v) },
            { key: 'total_bytes', label: 'Size', align: 'right', format: (v) => formatBytes(v) },
          ]}
          rows={tempJunk}
        />
        <DataTable
          title="Cold Data (1y+)"
          columns={[
            { key: 'ext', label: 'Ext' },
            { key: 'count', label: 'Count', align: 'right', format: (v) => formatNumber(v) },
            { key: 'total_bytes', label: 'Size', align: 'right', format: (v) => formatBytes(v) },
          ]}
          rows={cold}
        />
        <DataTable
          title="Hot Data (30d)"
          columns={[
            { key: 'ext', label: 'Ext' },
            { key: 'count', label: 'Count', align: 'right', format: (v) => formatNumber(v) },
            { key: 'total_bytes', label: 'Size', align: 'right', format: (v) => formatBytes(v) },
          ]}
          rows={hot}
        />
      </div>
    </div>
  );
}
