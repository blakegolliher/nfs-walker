import { createContext, useContext, useState, useEffect, type ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import type { ScanInfo } from '../api/types';

interface ScanContextValue {
  scans: ScanInfo[];
  activeScan: string | undefined;
  setActiveScan: (id: string) => void;
  isLoading: boolean;
}

const ScanContext = createContext<ScanContextValue | null>(null);

export function ScanProvider({ children }: { children: ReactNode }) {
  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['scans'],
    queryFn: api.scans,
  });

  const [activeScan, setActiveScan] = useState<string | undefined>();

  useEffect(() => {
    if (data?.default_scan && !activeScan) {
      setActiveScan(data.default_scan);
    }
  }, [data?.default_scan, activeScan]);

  // Without a scan list the dashboard is unusable, so surface failures
  // and the zero-scan case here instead of rendering blank pages.
  if (error) {
    return (
      <div className="flex h-screen items-center justify-center p-8">
        <div className="max-w-lg space-y-3 rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-400">
          <p>Failed to load scans: {String(error)}</p>
          <button
            onClick={() => refetch()}
            className="rounded border border-red-500/30 px-2 py-1 text-xs hover:bg-red-500/20"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!isLoading && (data?.scans.length ?? 0) === 0) {
    return (
      <div className="flex h-screen flex-col items-center justify-center gap-2 p-8 text-center">
        <p className="text-sm text-gray-300">
          No scans found in {data?.data_dir ?? 'the data directory'}
        </p>
        <p className="text-xs text-gray-500">
          Run nfs-walker against an NFS export first, then refresh this page.
        </p>
      </div>
    );
  }

  return (
    <ScanContext.Provider
      value={{
        scans: data?.scans ?? [],
        activeScan,
        setActiveScan,
        isLoading,
      }}
    >
      {children}
    </ScanContext.Provider>
  );
}

export function useScanContext() {
  const ctx = useContext(ScanContext);
  if (!ctx) throw new Error('useScanContext must be inside ScanProvider');
  return ctx;
}
