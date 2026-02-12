import { formatDuration } from '../../lib/format';
import type { QueryResult } from '../../api/types';

export function QueryMeta({ result }: { result: QueryResult }) {
  return (
    <div className="flex items-center gap-3 text-xs text-gray-500">
      <span>{result.query_id}</span>
      <span>{result.row_count} rows</span>
      <span>{formatDuration(result.execution_ms)}</span>
    </div>
  );
}
