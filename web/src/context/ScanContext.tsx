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
  const { data, isLoading } = useQuery({
    queryKey: ['scans'],
    queryFn: api.scans,
  });

  const [activeScan, setActiveScan] = useState<string | undefined>();

  useEffect(() => {
    if (data?.default_scan && !activeScan) {
      setActiveScan(data.default_scan);
    }
  }, [data?.default_scan, activeScan]);

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
