import { useBatchQueries, getFirstRow, getRows } from '../hooks/useBatchQueries';
import { Q } from '../api/queries';
import { formatBytes, formatNumber, formatPct } from '../lib/format';
import { MetricCard } from '../components/data/MetricCard';
import { MetricCardGrid } from '../components/data/MetricCardGrid';
import { BarChart } from '../components/charts/BarChart';
import { HorizontalBarChart } from '../components/charts/HorizontalBarChart';
import { PieChart } from '../components/charts/PieChart';
import { DataTable } from '../components/data/DataTable';
import { LoadingSpinner } from '../components/shared/LoadingSpinner';
import { ErrorBanner } from '../components/shared/ErrorBanner';

const QUERIES = [
  { query_id: Q.CAPACITY_BY_DEPTH },
  { query_id: Q.INODES_BY_TYPE },
  { query_id: Q.CAPACITY_BY_TOP_DIR, params: { limit: '20' } },
  { query_id: Q.HARD_LINKS, params: { limit: '20' } },
  { query_id: Q.DUPLICATE_INODES, params: { limit: '20' } },
  { query_id: Q.SPACE_WASTE },
];

export function CapacityPage() {
  const { data, isLoading, error } = useBatchQueries(QUERIES);

  if (isLoading) return <LoadingSpinner />;
  if (error) return <ErrorBanner message={String(error)} />;

  const waste = getFirstRow(data, Q.SPACE_WASTE);
  const depthData = getRows(data, Q.CAPACITY_BY_DEPTH);
  const inodes = getRows(data, Q.INODES_BY_TYPE);
  const topDirs = getRows(data, Q.CAPACITY_BY_TOP_DIR);
  const hardLinks = getRows(data, Q.HARD_LINKS);
  const dupInodes = getRows(data, Q.DUPLICATE_INODES);

  return (
    <div className="space-y-4">
      <MetricCardGrid>
        <MetricCard label="Logical Size" value={formatBytes(waste?.logical_bytes)} />
        <MetricCard label="Allocated" value={formatBytes(waste?.allocated_bytes)} />
        <MetricCard label="Waste" value={formatBytes(waste?.waste_bytes)} />
        <MetricCard label="Waste %" value={formatPct(waste?.waste_pct)} />
      </MetricCardGrid>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <BarChart
          data={depthData}
          xKey="depth"
          bars={[
            { key: 'total_bytes', label: 'Bytes', color: '#3b82f6' },
          ]}
          title="Capacity by Depth"
          formatValue={formatBytes}
        />
        <HorizontalBarChart
          data={topDirs.slice(0, 15)}
          nameKey="top_dir"
          valueKey="total_bytes"
          title="Top Directories by Size"
          formatValue={formatBytes}
        />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <PieChart
          data={inodes}
          nameKey="file_type"
          valueKey="unique_inodes"
          title="Inodes by Type"
        />
        <DataTable
          title="Hard Links (nlink > 1)"
          columns={[
            { key: 'nlink', label: 'nlink', align: 'right' },
            { key: 'count', label: 'Count', align: 'right', format: (v) => formatNumber(v) },
            { key: 'total_bytes', label: 'Size', align: 'right', format: (v) => formatBytes(v) },
          ]}
          rows={hardLinks}
        />
      </div>

      <DataTable
        title="Duplicate Inodes"
        columns={[
          { key: 'inode', label: 'Inode', align: 'right' },
          { key: 'link_count', label: 'Links', align: 'right' },
          { key: 'example_path', label: 'Example Path' },
          { key: 'size', label: 'Size', align: 'right', format: (v) => formatBytes(v) },
        ]}
        rows={dupInodes}
      />
    </div>
  );
}
