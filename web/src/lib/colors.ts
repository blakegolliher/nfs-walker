// Chart color palette — Grafana-inspired
export const CHART_COLORS = [
  '#3b82f6', // blue
  '#22c55e', // green
  '#f59e0b', // amber
  '#ef4444', // red
  '#a855f7', // purple
  '#06b6d4', // cyan
  '#f97316', // orange
  '#ec4899', // pink
  '#84cc16', // lime
  '#6366f1', // indigo
];

export function chartColor(index: number): string {
  return CHART_COLORS[index % CHART_COLORS.length];
}

// Semantic colors for file types
export const FILE_TYPE_COLORS: Record<string, string> = {
  file: '#3b82f6',
  directory: '#22c55e',
  symlink: '#f59e0b',
  block_device: '#ef4444',
  char_device: '#a855f7',
  fifo: '#06b6d4',
  socket: '#f97316',
};
