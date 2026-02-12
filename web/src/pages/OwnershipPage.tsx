import { useBatchQueries, getRows } from '../hooks/useBatchQueries';
import { Q } from '../api/queries';
import { formatBytes, formatNumber, formatPct } from '../lib/format';
import { PieChart } from '../components/charts/PieChart';
import { HorizontalBarChart } from '../components/charts/HorizontalBarChart';
import { DataTable } from '../components/data/DataTable';
import { LoadingSpinner } from '../components/shared/LoadingSpinner';
import { ErrorBanner } from '../components/shared/ErrorBanner';

const QUERIES = [
  { query_id: Q.USAGE_BY_UID, params: { limit: '20' } },
  { query_id: Q.USAGE_BY_GID, params: { limit: '20' } },
  { query_id: Q.WORLD_WRITABLE, params: { limit: '30' } },
  { query_id: Q.UID_EXTENSION, params: { limit: '30' } },
  { query_id: Q.OWNERSHIP_CONCENTRATION, params: { limit: '15' } },
  { query_id: Q.STALE_BY_OWNER, params: { limit: '20' } },
];

export function OwnershipPage() {
  const { data, isLoading, error } = useBatchQueries(QUERIES);

  if (isLoading) return <LoadingSpinner />;
  if (error) return <ErrorBanner message={String(error)} />;

  const concentration = getRows(data, Q.OWNERSHIP_CONCENTRATION);
  const byUid = getRows(data, Q.USAGE_BY_UID);
  const byGid = getRows(data, Q.USAGE_BY_GID);
  const uidExt = getRows(data, Q.UID_EXTENSION);
  const worldWritable = getRows(data, Q.WORLD_WRITABLE);
  const stale = getRows(data, Q.STALE_BY_OWNER);

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <PieChart
          data={concentration.slice(0, 8)}
          nameKey="uid"
          valueKey="uid_bytes"
          title="Ownership Concentration"
          formatValue={formatBytes}
        />
        <HorizontalBarChart
          data={byUid.slice(0, 12)}
          nameKey="uid"
          valueKey="total_bytes"
          title="Storage by UID"
          formatValue={formatBytes}
        />
        <HorizontalBarChart
          data={byGid.slice(0, 12)}
          nameKey="gid"
          valueKey="total_bytes"
          title="Storage by GID"
          formatValue={formatBytes}
          color="#22c55e"
        />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <DataTable
          title="UID x Extension"
          columns={[
            { key: 'uid', label: 'UID', align: 'right' },
            { key: 'ext', label: 'Extension' },
            { key: 'count', label: 'Count', align: 'right', format: (v) => formatNumber(v) },
            { key: 'total_bytes', label: 'Size', align: 'right', format: (v) => formatBytes(v) },
          ]}
          rows={uidExt}
        />
        <DataTable
          title="World-Writable Files"
          columns={[
            { key: 'path', label: 'Path' },
            { key: 'size', label: 'Size', align: 'right', format: (v) => formatBytes(v) },
            { key: 'uid', label: 'UID', align: 'right' },
            { key: 'gid', label: 'GID', align: 'right' },
          ]}
          rows={worldWritable}
        />
      </div>

      <DataTable
        title="Stale Data by Owner"
        columns={[
          { key: 'uid', label: 'UID', align: 'right' },
          { key: 'stale_files', label: 'Stale Files', align: 'right', format: (v) => formatNumber(v) },
          { key: 'stale_bytes', label: 'Stale Bytes', align: 'right', format: (v) => formatBytes(v) },
        ]}
        rows={stale}
      />

      <DataTable
        title="Ownership Concentration"
        columns={[
          { key: 'uid', label: 'UID', align: 'right' },
          { key: 'uid_bytes', label: 'Bytes', align: 'right', format: (v) => formatBytes(v) },
          { key: 'pct', label: '% of Total', align: 'right', format: (v) => formatPct(v) },
        ]}
        rows={concentration}
      />
    </div>
  );
}
