import { useMemo, useState } from 'react'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { EmptyState } from '@/components/shared/EmptyState'
import { Skeleton } from '@/components/ui/skeleton'
import { usePrices } from '@/hooks/usePrices'
import { CHART_THEME } from '@/theme/chartTheme'
import { useTheme } from '@/theme/ThemeProvider'
import type { PriceRow } from '@/types'
import { formatDate, formatPrice, formatVolume } from '@/utils/formatters'

const RANGES = [
  { label: '1M', days: 30 },
  { label: '3M', days: 90 },
  { label: '6M', days: 180 },
  { label: '1Y', days: 365 },
  { label: 'Tất cả', days: null },
]

interface PriceChartProps {
  symbol: string
  fromDate?: string
  toDate?: string
}

const sortAscending = (rows: PriceRow[]): PriceRow[] =>
  [...rows].sort((a, b) => a.trading_date.localeCompare(b.trading_date))

function PriceTooltip({
  active,
  payload,
}: {
  active?: boolean
  payload?: Array<{ payload: PriceRow }>
}) {
  const row = payload?.[0]?.payload
  if (!active || !row) return null

  return (
    <div className="rounded-lg border border-app-border bg-panel-dark p-3 text-xs shadow-xl">
      <p className="mb-2 font-semibold text-app-heading">{formatDate(row.trading_date)}</p>
      <div className="grid gap-1.5">
        {[
          { label: 'Mở cửa', value: row.open, color: 'text-yellow-300' },
          { label: 'Cao nhất', value: row.high, color: 'text-price-up' },
          { label: 'Thấp nhất', value: row.low, color: 'text-price-down' },
          { label: 'Đóng cửa', value: row.close, color: 'text-accent' },
        ].map((item) => (
          <div key={item.label} className="flex min-w-44 justify-between gap-5">
            <span className="text-app-muted">{item.label}</span>
            <span className={`font-mono font-semibold ${item.color}`}>
              {formatPrice(item.value)}
            </span>
          </div>
        ))}
        <div className="mt-1 flex min-w-44 justify-between gap-5 border-t border-app-border pt-2">
          <span className="text-app-muted">Khối lượng</span>
          <span className="font-mono font-semibold text-app-heading">
            {formatVolume(row.volume)}
          </span>
        </div>
      </div>
    </div>
  )
}

export function PriceChart({ symbol, fromDate, toDate }: PriceChartProps) {
  const [rangeDays, setRangeDays] = useState<number | null>(365)
  const dateRangeActive = !!fromDate || !!toDate
  const { data, isLoading } = usePrices(symbol, {
    page_size: 500,
    from: fromDate || undefined,
    to: toDate || undefined,
  })
  const { theme } = useTheme()
  const chartTheme = CHART_THEME[theme]

  const rows = useMemo(() => {
    const sorted = sortAscending(data?.data ?? [])
    if (dateRangeActive) return sorted
    if (rangeDays == null || sorted.length === 0) return sorted
    const latest = new Date(sorted[sorted.length - 1].trading_date)
    const cutoff = new Date(latest)
    cutoff.setDate(cutoff.getDate() - rangeDays)
    return sorted.filter((row) => new Date(row.trading_date) >= cutoff)
  }, [data?.data, dateRangeActive, rangeDays])

  if (isLoading) return <Skeleton className="h-80" />
  if (rows.length === 0) return <EmptyState message="Không có dữ liệu giá" />

  return (
    <div className="grid gap-4">
      <div className="flex flex-wrap gap-2">
        {dateRangeActive ? (
          <span className="text-xs text-app-muted">
            Theo khoảng ngày đã chọn
          </span>
        ) : (
          RANGES.map((range) => (
            <button
              key={range.label}
              type="button"
              onClick={() => setRangeDays(range.days)}
              className={`h-8 rounded-md px-3 text-xs font-medium transition-colors ${
                rangeDays === range.days
                  ? 'bg-accent text-white'
                  : 'bg-app-hover text-app-muted hover:bg-app-input hover:text-app-heading'
              }`}
            >
              {range.label}
            </button>
          ))
        )}
      </div>
      <div className="h-[22rem]">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={rows} margin={{ top: 10, right: 12, bottom: 0, left: 4 }}>
            <CartesianGrid stroke={chartTheme.grid} strokeDasharray="3 3" />
            <XAxis
              dataKey="trading_date"
              stroke={chartTheme.axis}
              tick={{ fontSize: 11 }}
              minTickGap={32}
              tickFormatter={(value: string) => formatDate(value).slice(0, 5)}
            />
            <YAxis
              stroke={chartTheme.axis}
              tick={{ fontSize: 11 }}
              width={58}
              tickFormatter={(value: number) => formatPrice(value)}
            />
            <Tooltip
              content={<PriceTooltip />}
            />
            <Legend />
            <Line
              type="monotone"
              dataKey="open"
              name="Mở cửa"
              stroke="#f59e0b"
              strokeWidth={1.6}
              dot={false}
            />
            <Line
              type="monotone"
              dataKey="high"
              name="Cao nhất"
              stroke="#22c55e"
              strokeWidth={1.6}
              dot={false}
            />
            <Line
              type="monotone"
              dataKey="low"
              name="Thấp nhất"
              stroke="#ef4444"
              strokeWidth={1.6}
              dot={false}
            />
            <Line
              type="monotone"
              dataKey="close"
              name="Giá đóng cửa"
              stroke="#3b82f6"
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
