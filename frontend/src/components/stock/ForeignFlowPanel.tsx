import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { EmptyState } from '@/components/shared/EmptyState'
import { Skeleton } from '@/components/ui/skeleton'
import { useForeignFlow } from '@/hooks/useBoard'

interface ForeignFlowPanelProps {
  symbol: string
  fromDate?: string
  toDate?: string
}

const toMillionShares = (value: number | null | undefined, sign = 1) =>
  +(((value ?? 0) * sign) / 1_000_000).toFixed(2)

export function ForeignFlowPanel({ symbol, fromDate, toDate }: ForeignFlowPanelProps) {
  const { data, isLoading } = useForeignFlow(symbol, {
    days: 30,
    from: fromDate || undefined,
    to: toDate || undefined,
  })
  const rows = data ?? []

  if (isLoading) return <Skeleton className="h-64" />
  if (rows.length === 0) {
    return <EmptyState message="Không có dữ liệu khối ngoại" />
  }

  const chartData = [...rows].reverse().map((row) => ({
    date: new Date(row.trading_date).toLocaleDateString('vi-VN', {
      day: 'numeric',
      month: 'short',
    }),
    mua: toMillionShares(row.foreign_buy_volume),
    ban: toMillionShares(row.foreign_sell_volume, -1),
    rong: toMillionShares(row.foreign_net_volume),
  }))

  return (
    <div className="grid gap-4">
      <h3 className="text-sm font-semibold text-app-heading">
        Giao dịch khối ngoại (triệu CP)
      </h3>
      <ResponsiveContainer width="100%" height={240}>
        <ComposedChart data={chartData} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#2D3545" />
          <XAxis dataKey="date" tick={{ fontSize: 11, fill: '#94A3B8' }} />
          <YAxis tick={{ fontSize: 11, fill: '#94A3B8' }} />
          <Tooltip
            contentStyle={{
              backgroundColor: '#111827',
              border: '1px solid #334155',
              borderRadius: 8,
            }}
            labelStyle={{ color: '#E5E7EB' }}
          />
          <Legend />
          <Bar dataKey="mua" name="Mua" fill="#22C55E" opacity={0.85} />
          <Bar dataKey="ban" name="Bán" fill="#EF4444" opacity={0.85} />
          <Line
            dataKey="rong"
            name="Ròng"
            type="monotone"
            stroke="#60A5FA"
            strokeWidth={2}
            dot={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}
