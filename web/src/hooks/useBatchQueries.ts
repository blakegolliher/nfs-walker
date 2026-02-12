import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import type { BatchQueryItem, QueryResult } from '../api/types';
import { useScan } from './useScan';

/** Execute multiple queries in a single batch call. Returns a map of query_id -> QueryResult. */
export function useBatchQueries(queries: BatchQueryItem[], enabled = true) {
  const { activeScan } = useScan();

  const queryIds = queries.map((q) => q.query_id).sort();

  return useQuery({
    queryKey: ['batch', activeScan, queryIds],
    queryFn: async () => {
      const resp = await api.batch(queries, activeScan);
      const map: Record<string, QueryResult> = {};
      for (const item of resp.results) {
        if (item.status === 'ok' && item.result) {
          map[item.result.query_id] = item.result;
        }
      }
      return map;
    },
    enabled: enabled && !!activeScan,
  });
}

/** Get rows from a batch result for a specific query. */
export function getRows(data: Record<string, QueryResult> | undefined, queryId: string) {
  return data?.[queryId]?.rows ?? [];
}

/** Get the first row from a batch result for a specific query. */
export function getFirstRow(data: Record<string, QueryResult> | undefined, queryId: string) {
  const rows = getRows(data, queryId);
  return rows[0] as Record<string, unknown> | undefined;
}
