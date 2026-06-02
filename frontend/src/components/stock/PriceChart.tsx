import { useMemo, useState } from 'react'
import {
  CartesianGrid,
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
import { formatPrice } from '@/utils/formatters'

const RANGES = [
  { label: '1M', days: 30 },
  { label: '3M', days: 90 },
  { label: '6M', days: 180 },
  { label: '1Y', days: 365 },
  { label: 'Tất cả', days: null },
]

interface PriceChartProps {
  symbol: string
}

const sortAscending = (rows: PriceRow[]): PriceRow[] =>
  [...rows].sort((a, b) => a.trading_date.localeCompare(b.trading_date))

export function PriceChart({ symbol }: PriceChartProps) {
  const [rangeDays, setRangeDays] = useState<number | null>(365)
  const { data, isLoading } = usePrices(symbol, { page_size: 500 })
  const { theme } = useTheme()
  const chartTheme = CHART_THEME[theme]

  const rows = useMemo(() => {
    const sorted = sortAscending(data?.data ?? [])
    if (rangeDays == null || sorted.length === 0) return sorted
    const latest = new Date(sorted[sorted.length - 1].trading_date)
    const cutoff = new Date(latest)
    cutoff.setDate(cutoff.getDate() - rangeDays)
    return sorted.filter((row) => new Date(row.trading_date) >= cutoff)
  }, [data?.data, rangeDays])

  if (isLoading) return <Skeleton className="h-80" />
  if (rows.length === 0) return <EmptyState message="Không có dữ liệu giá" />

  return (
    <div className="grid gap-4">
      <div className="flex flex-wrap gap-2">
        {RANGES.map((range) => (
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
        ))}
      </div>
      <div className="h-80">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={rows} margin={{ top: 10, right: 12, bottom: 0, left: 0 }}>
            <CartesianGrid stroke={chartTheme.grid} strokeDasharray="3 3" />
            <XAxis
              dataKey="trading_date"
              stroke={chartTheme.axis}
              tick={{ fontSize: 11 }}
              tickFormatter={(value: string) => value.slice(5)}
            />
            <YAxis
              stroke={chartTheme.axis}
              tick={{ fontSize: 11 }}
              width={58}
              tickFormatter={(value: number) => formatPrice(value)}
            />
            <Tooltip
              contentStyle={{
                background: chartTheme.tooltipBg,
                border: `1px solid ${chartTheme.tooltipBorder}`,
                borderRadius: 8,
              }}
              labelStyle={{ color: chartTheme.tooltipText }}
              itemStyle={{ color: chartTheme.tooltipText }}
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
