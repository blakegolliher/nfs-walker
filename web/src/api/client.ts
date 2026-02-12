import type {
  ScansResponse,
  QueriesResponse,
  QueryResult,
  BatchResponse,
  BatchQueryItem,
} from './types';

const BASE = '/api';

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${url}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`API ${res.status}: ${body}`);
  }
  return res.json();
}

export const api = {
  health: () => request<{ status: string }>('/health'),

  scans: () => request<ScansResponse>('/scans'),

  queries: () => request<QueriesResponse>('/queries'),

  execute: (queryId: string, params?: Record<string, string>, scanId?: string) =>
    request<QueryResult>(`/queries/${queryId}/execute`, {
      method: 'POST',
      body: JSON.stringify({ params: params ?? {}, scan_id: scanId }),
    }),

  batch: (queries: BatchQueryItem[], scanId?: string) =>
    request<BatchResponse>('/queries/batch', {
      method: 'POST',
      body: JSON.stringify({ queries, scan_id: scanId }),
    }),
};
