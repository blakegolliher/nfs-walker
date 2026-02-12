import type { ReactNode } from 'react';

export function MetricCardGrid({ children }: { children: ReactNode }) {
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
      {children}
    </div>
  );
}
