import { useBatchQueries, getFirstRow, getRows } from '../hooks/useBatchQueries';
import { Q } from '../api/queries';
import { formatBytes, formatNumber, formatCompact } from '../lib/format';
import { MetricCard } from '../components/data/MetricCard';
import { MetricCardGrid } from '../components/data/MetricCardGrid';
import { BarChart } from '../components/charts/BarChart';
import { DataTable } from '../components/data/DataTable';
import { LoadingSpinner } from '../components/shared/LoadingSpinner';
import { ErrorBanner } from '../components/shared/ErrorBanner';

const QUERIES = [
  { query_id: Q.WIDEST_DIRS, params: { limit: '25' } },
  { query_id: Q.DEEPEST_PATHS, params: { limit: '25' } },
  { query_id: Q.EMPTY_DIRS, params: { limit: '25' } },
  { query_id: Q.DEPTH_DISTRIBUTION },
  { query_id: Q.FANOUT_DISTRIBUTION },
  { query_id: Q.FANOUT_STATS },
];

export function DirectoriesPage() {
  const { data, isLoading, error } = useBatchQueries(QUERIES);

  if (isLoading) return <LoadingSpinner />;
  if (error) return <ErrorBanner message={String(error)} />;

  const fanout = getFirstRow(data, Q.FANOUT_STATS);
  const depthDist = getRows(data, Q.DEPTH_DISTRIBUTION);
  const fanoutDist = getRows(data, Q.FANOUT_DISTRIBUTION);
  const widest = getRows(data, Q.WIDEST_DIRS);
  const deepest = getRows(data, Q.DEEPEST_PATHS);
  const empty = getRows(data, Q.EMPTY_DIRS);

  return (
    <div className="space-y-4">
      <MetricCardGrid>
        <MetricCard label="Total Dirs" value={formatNumber(fanout?.total_dirs)} />
        <MetricCard label="Min Children" value={formatNumber(fanout?.min_children)} />
        <MetricCard label="Max Children" value={formatCompact(fanout?.max_children)} />
        <MetricCard label="Avg Children" value={formatNumber(fanout?.avg_children)} />
        <MetricCard label="Median Children" value={formatNumber(fanout?.median_children)} />
      </MetricCardGrid>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <BarChart
          data={depthDist}
          xKey="depth"
          bars={[{ key: 'dir_count', label: 'Directories' }]}
          title="Depth Distribution"
          formatValue={formatNumber}
        />
        <BarChart
          data={fanoutDist}
          xKey="bucket"
          bars={[{ key: 'dir_count', label: 'Directories', color: '#22c55e' }]}
          title="Fanout Distribution"
          formatValue={formatNumber}
        />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <DataTable
          title="Widest Directories"
          columns={[
            { key: 'parent_path', label: 'Directory' },
            { key: 'child_count', label: 'Children', align: 'right', format: (v) => formatNumber(v) },
            { key: 'total_bytes', label: 'Size', align: 'right', format: (v) => formatBytes(v) },
          ]}
          rows={widest}
        />
        <DataTable
          title="Deepest Paths"
          columns={[
            { key: 'path', label: 'Path' },
            { key: 'depth', label: 'Depth', align: 'right' },
            { key: 'file_type', label: 'Type' },
            { key: 'size', label: 'Size', align: 'right', format: (v) => formatBytes(v) },
          ]}
          rows={deepest}
        />
      </div>

      <DataTable
        title="Empty Directories"
        columns={[
          { key: 'path', label: 'Path' },
          { key: 'depth', label: 'Depth', align: 'right' },
        ]}
        rows={empty}
      />
    </div>
  );
}
