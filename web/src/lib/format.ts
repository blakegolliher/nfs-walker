const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];

export function formatBytes(bytes: unknown): string {
  const n = Number(bytes);
  if (!Number.isFinite(n) || n === 0) return '0 B';
  const sign = n < 0 ? '-' : '';
  let abs = Math.abs(n);
  let i = 0;
  while (abs >= 1024 && i < UNITS.length - 1) {
    abs /= 1024;
    i++;
  }
  return `${sign}${abs < 10 ? abs.toFixed(2) : abs < 100 ? abs.toFixed(1) : abs.toFixed(0)} ${UNITS[i]}`;
}

export function formatNumber(n: unknown): string {
  const num = Number(n);
  if (!Number.isFinite(num)) return '—';
  return num.toLocaleString('en-US');
}

export function formatCompact(n: unknown): string {
  const num = Number(n);
  if (!Number.isFinite(num)) return '—';
  if (Math.abs(num) >= 1e9) return `${(num / 1e9).toFixed(1)}B`;
  if (Math.abs(num) >= 1e6) return `${(num / 1e6).toFixed(1)}M`;
  if (Math.abs(num) >= 1e3) return `${(num / 1e3).toFixed(1)}K`;
  return num.toLocaleString('en-US');
}

export function formatPct(n: unknown): string {
  const num = Number(n);
  if (!Number.isFinite(num)) return '—';
  return `${num.toFixed(1)}%`;
}

export function formatDuration(ms: unknown): string {
  const n = Number(ms);
  if (!Number.isFinite(n)) return '—';
  if (n < 1) return '<1ms';
  if (n < 1000) return `${Math.round(n)}ms`;
  return `${(n / 1000).toFixed(1)}s`;
}
