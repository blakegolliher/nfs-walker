import { useScan } from '../../hooks/useScan';
import { formatCompact } from '../../lib/format';

export function ScanSelector() {
  const { scans, activeScan, setActiveScan, isLoading } = useScan();

  if (isLoading) return <div className="text-xs text-gray-500">Loading scans...</div>;
  if (scans.length === 0) return <div className="text-xs text-gray-500">No scans</div>;

  return (
    <select
      value={activeScan ?? ''}
      onChange={(e) => setActiveScan(e.target.value)}
      className="rounded border border-border bg-surface-2 px-2 py-1 text-xs text-gray-300 outline-none focus:border-accent"
    >
      {scans.map((s) => (
        <option key={s.scan_id} value={s.scan_id}>
          {s.scan_id} ({formatCompact(s.total_entries)} entries)
        </option>
      ))}
    </select>
  );
}
