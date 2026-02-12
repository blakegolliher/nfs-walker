import {
  PieChart as RPieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts';
import { CHART_COLORS } from '../../lib/colors';
import { EmptyState } from '../shared/EmptyState';

interface PieChartProps {
  data: Record<string, unknown>[];
  nameKey: string;
  valueKey: string;
  title?: string;
  height?: number;
  formatValue?: (v: number) => string;
}

export function PieChart({
  data,
  nameKey,
  valueKey,
  title,
  height = 260,
  formatValue,
}: PieChartProps) {
  if (data.length === 0) return <EmptyState />;

  return (
    <div className="card">
      {title && <div className="card-header">{title}</div>}
      <ResponsiveContainer width="100%" height={height}>
        <RPieChart>
          <Pie
            data={data}
            dataKey={valueKey}
            nameKey={nameKey}
            cx="50%"
            cy="50%"
            innerRadius="40%"
            outerRadius="70%"
            paddingAngle={2}
            strokeWidth={0}
          >
            {data.map((_, i) => (
              <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{
              backgroundColor: '#1f2229',
              border: '1px solid #2a2e37',
              borderRadius: 6,
              fontSize: 12,
            }}
            formatter={(value: number) => formatValue ? formatValue(value) : value.toLocaleString()}
          />
          <Legend
            wrapperStyle={{ fontSize: 11, color: '#9ca3af' }}
          />
        </RPieChart>
      </ResponsiveContainer>
    </div>
  );
}
