import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import { chartColor } from '../../lib/colors';
import { EmptyState } from '../shared/EmptyState';

interface HorizontalBarChartProps {
  data: Record<string, unknown>[];
  nameKey: string;
  valueKey: string;
  title?: string;
  height?: number;
  color?: string;
  formatValue?: (v: number) => string;
}

export function HorizontalBarChart({
  data,
  nameKey,
  valueKey,
  title,
  height,
  color,
  formatValue,
}: HorizontalBarChartProps) {
  if (data.length === 0) return <EmptyState />;

  const h = height ?? Math.max(200, data.length * 28 + 40);

  return (
    <div className="card">
      {title && <div className="card-header">{title}</div>}
      <ResponsiveContainer width="100%" height={h}>
        <BarChart
          data={data}
          layout="vertical"
          margin={{ top: 4, right: 4, bottom: 4, left: 4 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#2a2e37" horizontal={false} />
          <XAxis
            type="number"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={formatValue}
            axisLine={{ stroke: '#2a2e37' }}
          />
          <YAxis
            dataKey={nameKey}
            type="category"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            width={120}
            axisLine={{ stroke: '#2a2e37' }}
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
          <Bar
            dataKey={valueKey}
            fill={color ?? chartColor(0)}
            radius={[0, 2, 2, 0]}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
