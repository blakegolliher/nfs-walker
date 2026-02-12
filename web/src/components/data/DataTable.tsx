import { useState, useMemo } from 'react';
import { EmptyState } from '../shared/EmptyState';

interface Column {
  key: string;
  label?: string;
  align?: 'left' | 'right';
  format?: (v: unknown) => string;
}

interface DataTableProps {
  columns: Column[];
  rows: Record<string, unknown>[];
  maxRows?: number;
  title?: string;
}

export function DataTable({ columns, rows, maxRows = 50, title }: DataTableProps) {
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);

  const sorted = useMemo(() => {
    if (!sortKey) return rows.slice(0, maxRows);
    return [...rows]
      .sort((a, b) => {
        const av = a[sortKey], bv = b[sortKey];
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        if (typeof av === 'number' && typeof bv === 'number')
          return sortAsc ? av - bv : bv - av;
        return sortAsc
          ? String(av).localeCompare(String(bv))
          : String(bv).localeCompare(String(av));
      })
      .slice(0, maxRows);
  }, [rows, sortKey, sortAsc, maxRows]);

  const toggleSort = (key: string) => {
    if (sortKey === key) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(true); }
  };

  if (rows.length === 0) return <EmptyState />;

  return (
    <div className="card overflow-hidden p-0">
      {title && <div className="card-header px-4 pt-3">{title}</div>}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs text-gray-500">
              {columns.map((col) => (
                <th
                  key={col.key}
                  className={`cursor-pointer px-4 py-2 font-medium hover:text-gray-300 ${col.align === 'right' ? 'text-right' : ''}`}
                  onClick={() => toggleSort(col.key)}
                >
                  {col.label ?? col.key}
                  {sortKey === col.key && (sortAsc ? ' \u25b2' : ' \u25bc')}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((row, i) => (
              <tr key={i} className="border-b border-border/50 hover:bg-surface-3/50">
                {columns.map((col) => (
                  <td
                    key={col.key}
                    className={`px-4 py-1.5 font-mono text-xs ${col.align === 'right' ? 'text-right' : ''}`}
                    title={String(row[col.key] ?? '')}
                  >
                    {col.format ? col.format(row[col.key]) : String(row[col.key] ?? '—')}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {rows.length > maxRows && (
        <div className="px-4 py-2 text-xs text-gray-500">
          Showing {maxRows} of {rows.length} rows
        </div>
      )}
    </div>
  );
}
