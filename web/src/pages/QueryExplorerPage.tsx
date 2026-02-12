import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import { useScan } from '../hooks/useScan';
import type { QueryDef, QueryResult } from '../api/types';
import { formatBytes, formatNumber, formatDuration } from '../lib/format';
import { DataTable } from '../components/data/DataTable';
import { LoadingSpinner } from '../components/shared/LoadingSpinner';
import { ErrorBanner } from '../components/shared/ErrorBanner';

export function QueryExplorerPage() {
  const { data: catalog, isLoading: catalogLoading } = useQuery({
    queryKey: ['queries'],
    queryFn: api.queries,
  });

  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [params, setParams] = useState<Record<string, string>>({});
  const [result, setResult] = useState<QueryResult | null>(null);
  const [executing, setExecuting] = useState(false);
  const [execError, setExecError] = useState<string | null>(null);

  const { activeScan } = useScan();

  if (catalogLoading) return <LoadingSpinner />;

  const queries = catalog?.queries ?? [];
  const categories = [...new Set(queries.map((q) => q.category))];
  const selected = queries.find((q) => q.id === selectedId);

  const handleSelect = (q: QueryDef) => {
    setSelectedId(q.id);
    setResult(null);
    setExecError(null);
    const defaults: Record<string, string> = {};
    for (const p of q.params) {
      if (p.default) defaults[p.name] = p.default;
    }
    setParams(defaults);
  };

  const handleExecute = async () => {
    if (!selectedId) return;
    setExecuting(true);
    setExecError(null);
    try {
      const r = await api.execute(selectedId, params, activeScan);
      setResult(r);
    } catch (e) {
      setExecError(String(e));
    } finally {
      setExecuting(false);
    }
  };

  return (
    <div className="flex gap-4 h-full">
      {/* Left: query list */}
      <div className="w-72 shrink-0 space-y-3 overflow-y-auto">
        {categories.map((cat) => (
          <div key={cat}>
            <div className="mb-1 px-2 text-xs font-medium uppercase tracking-wider text-gray-500">
              {cat.replace(/_/g, ' ')}
            </div>
            {queries
              .filter((q) => q.category === cat)
              .map((q) => (
                <button
                  key={q.id}
                  onClick={() => handleSelect(q)}
                  className={`block w-full rounded px-2 py-1 text-left text-sm transition-colors ${
                    selectedId === q.id
                      ? 'bg-accent/15 text-accent-hover'
                      : 'text-gray-400 hover:bg-surface-3 hover:text-gray-200'
                  }`}
                >
                  <span className="font-mono text-xs text-gray-500">{q.id}</span>{' '}
                  {q.name}
                </button>
              ))}
          </div>
        ))}
      </div>

      {/* Right: detail + results */}
      <div className="flex-1 space-y-4 overflow-y-auto">
        {selected ? (
          <>
            <div className="card">
              <div className="mb-2 flex items-center gap-3">
                <span className="font-mono text-xs text-gray-500">{selected.id}</span>
                <h2 className="text-base font-semibold text-gray-200">{selected.name}</h2>
              </div>
              <p className="mb-3 text-sm text-gray-400">{selected.description}</p>

              {selected.params.length > 0 && (
                <div className="mb-3 flex flex-wrap gap-3">
                  {selected.params.map((p) => (
                    <label key={p.name} className="text-sm">
                      <span className="mb-0.5 block text-xs text-gray-500">{p.name}</span>
                      <input
                        type={p.param_type === 'integer' ? 'number' : 'text'}
                        value={params[p.name] ?? ''}
                        onChange={(e) => setParams({ ...params, [p.name]: e.target.value })}
                        placeholder={p.default ?? ''}
                        className="w-28 rounded border border-border bg-surface-3 px-2 py-1 text-sm text-gray-200 outline-none focus:border-accent"
                      />
                    </label>
                  ))}
                </div>
              )}

              <button
                onClick={handleExecute}
                disabled={executing}
                className="btn-primary"
              >
                {executing ? 'Executing...' : 'Execute'}
              </button>
            </div>

            {execError && <ErrorBanner message={execError} />}

            {executing && <LoadingSpinner />}

            {result && (
              <>
                <div className="flex items-center gap-4 text-xs text-gray-500">
                  <span>{result.row_count} rows</span>
                  <span>{formatDuration(result.execution_ms)}</span>
                </div>
                <DataTable
                  columns={result.columns.map((col) => ({
                    key: col,
                    label: col,
                    align: isNumericColumn(col, result.rows) ? 'right' as const : 'left' as const,
                    format: isBytesColumn(col)
                      ? (v: unknown) => formatBytes(v)
                      : isNumericColumn(col, result.rows)
                        ? (v: unknown) => formatNumber(v)
                        : undefined,
                  }))}
                  rows={result.rows}
                  maxRows={200}
                />
              </>
            )}
          </>
        ) : (
          <div className="flex items-center justify-center py-16 text-gray-500">
            Select a query from the list
          </div>
        )}
      </div>
    </div>
  );
}

function isNumericColumn(col: string, rows: Record<string, unknown>[]): boolean {
  const sample = rows[0];
  if (!sample) return false;
  return typeof sample[col] === 'number';
}

function isBytesColumn(col: string): boolean {
  return /bytes|size|allocated|waste|logical|stale_bytes|uid_bytes/i.test(col);
}
