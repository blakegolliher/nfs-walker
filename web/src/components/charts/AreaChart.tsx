import {
  AreaChart as RAreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import { chartColor } from '../../lib/colors';
import { EmptyState } from '../shared/EmptyState';

interface AreaChartProps {
  data: Record<string, unknown>[];
  xKey: string;
  areas: { key: string; label?: string; color?: string }[];
  title?: string;
  height?: number;
  formatValue?: (v: number) => string;
}

export function AreaChart({
  data,
  xKey,
  areas,
  title,
  height = 260,
  formatValue,
}: AreaChartProps) {
  if (data.length === 0) return <EmptyState />;

  return (
    <div className="card">
      {title && <div className="card-header">{title}</div>}
      <ResponsiveContainer width="100%" height={height}>
        <RAreaChart data={data} margin={{ top: 4, right: 4, bottom: 4, left: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#2a2e37" />
          <XAxis
            dataKey={xKey}
            tick={{ fill: '#9ca3af', fontSize: 11 }}
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
          {areas.map((a, i) => (
            <Area
              key={a.key}
              type="monotone"
              dataKey={a.key}
              name={a.label ?? a.key}
              fill={a.color ?? chartColor(i)}
              fillOpacity={0.15}
              stroke={a.color ?? chartColor(i)}
              strokeWidth={2}
            />
          ))}
        </RAreaChart>
      </ResponsiveContainer>
    </div>
  );
}
