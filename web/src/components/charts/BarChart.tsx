import {
  BarChart as RBarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import { chartColor } from '../../lib/colors';
import { EmptyState } from '../shared/EmptyState';

interface BarChartProps {
  data: Record<string, unknown>[];
  xKey: string;
  bars: { key: string; label?: string; color?: string }[];
  title?: string;
  height?: number;
  formatValue?: (v: number) => string;
  formatLabel?: (v: string) => string;
}

export function BarChart({
  data,
  xKey,
  bars,
  title,
  height = 260,
  formatValue,
  formatLabel,
}: BarChartProps) {
  if (data.length === 0) return <EmptyState />;

  return (
    <div className="card">
      {title && <div className="card-header">{title}</div>}
      <ResponsiveContainer width="100%" height={height}>
        <RBarChart data={data} margin={{ top: 4, right: 4, bottom: 4, left: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#2a2e37" />
          <XAxis
            dataKey={xKey}
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={formatLabel}
            axisLine={{ stroke: '#2a2e37' }}
          />
          <YAxis
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={formatValue}
            axisLine={{ stroke: '#2a2e37' }}
            width={60}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1f2229',
              border: '1px solid #2a2e37',
              borderRadius: 6,
              fontSize: 12,
            }}
            formatter={(value: number) => formatValue ? formatValue(value) : value.toLocaleString()}
          />
          {bars.map((b, i) => (
            <Bar
              key={b.key}
              dataKey={b.key}
              name={b.label ?? b.key}
              fill={b.color ?? chartColor(i)}
              radius={[2, 2, 0, 0]}
            />
          ))}
        </RBarChart>
      </ResponsiveContainer>
    </div>
  );
}
