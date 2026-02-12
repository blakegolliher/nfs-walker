import { useBatchQueries, getFirstRow, getRows } from '../hooks/useBatchQueries';
import { Q } from '../api/queries';
import { formatBytes, formatNumber } from '../lib/format';
import { MetricCard } from '../components/data/MetricCard';
import { MetricCardGrid } from '../components/data/MetricCardGrid';
import { PieChart } from '../components/charts/PieChart';
import { BarChart } from '../components/charts/BarChart';
import { HorizontalBarChart } from '../components/charts/HorizontalBarChart';
import { LoadingSpinner } from '../components/shared/LoadingSpinner';
import { ErrorBanner } from '../components/shared/ErrorBanner';

const QUERIES = [
  { query_id: Q.ENTRY_COUNTS },
  { query_id: Q.CAPACITY_SUMMARY },
  { query_id: Q.CAPACITY_BY_TOP_DIR, params: { limit: '15' } },
  { query_id: Q.SIZE_HISTOGRAM },
  { query_id: Q.AGE_HISTOGRAM },
];

export function OverviewPage() {
  const { data, isLoading, error } = useBatchQueries(QUERIES);

  if (isLoading) return <LoadingSpinner />;
  if (error) return <ErrorBanner message={String(error)} />;

  const summary = getFirstRow(data, Q.CAPACITY_SUMMARY);
  const entryTypes = getRows(data, Q.ENTRY_COUNTS);
  const topDirs = getRows(data, Q.CAPACITY_BY_TOP_DIR);
  const sizeHist = getRows(data, Q.SIZE_HISTOGRAM);
  const ageHist = getRows(data, Q.AGE_HISTOGRAM);

  return (
    <div className="space-y-4">
      <MetricCardGrid>
        <MetricCard label="Total Entries" value={formatNumber(summary?.total_entries)} />
        <MetricCard label="Files" value={formatNumber(summary?.files)} />
        <MetricCard label="Directories" value={formatNumber(summary?.directories)} />
        <MetricCard label="Symlinks" value={formatNumber(summary?.symlinks)} />
        <MetricCard label="Total Size" value={formatBytes(summary?.total_bytes)} />
        <MetricCard label="Allocated" value={formatBytes(summary?.allocated_bytes)} />
      </MetricCardGrid>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <PieChart
          data={entryTypes}
          nameKey="file_type"
          valueKey="count"
          title="Entry Types"
        />
        <BarChart
          data={sizeHist}
          xKey="size_bucket"
          bars={[{ key: 'file_count', label: 'Files' }]}
          title="File Size Distribution"
          formatValue={formatNumber}
        />
        <BarChart
          data={ageHist}
          xKey="age_bucket"
          bars={[{ key: 'file_count', label: 'Files' }]}
          title="File Age Distribution"
          formatValue={formatNumber}
        />
      </div>

      <HorizontalBarChart
        data={topDirs.slice(0, 15)}
        nameKey="top_dir"
        valueKey="total_bytes"
        title="Top Directories by Size"
        formatValue={formatBytes}
      />
    </div>
  );
}
