import { ScanSelector } from './ScanSelector';

export function TopBar() {
  return (
    <header className="flex h-12 items-center justify-between border-b border-border bg-surface-1 px-4">
      <h1 className="text-sm font-medium text-gray-400">Analytics Dashboard</h1>
      <ScanSelector />
    </header>
  );
}
