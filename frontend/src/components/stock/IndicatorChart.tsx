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
import { useIndicators } from '@/hooks/useIndicators'
import { CHART_THEME } from '@/theme/chartTheme'
import { useTheme } from '@/theme/ThemeProvider'
import type { IndicatorRow } from '@/types'
import { formatPrice } from '@/utils/formatters'

interface IndicatorChartProps {
  symbol: string
  fromDate?: string
  toDate?: string
}

type IndicatorKey = 'ma7' | 'ma20' | 'ma50' | 'rsi14' | 'macd_line'

const OPTIONS: Array<{ key: IndicatorKey; label: string; color: string }> = [
  { key: 'ma7', label: 'MA7', color: '#22c55e' },
  { key: 'ma20', label: 'MA20', color: '#38bdf8' },
  { key: 'ma50', label: 'MA50', color: '#f59e0b' },
  { key: 'rsi14', label: 'RSI14', color: '#a78bfa' },
  { key: 'macd_line', label: 'MACD', color: '#f43f5e' },
]

const sortAscending = (rows: IndicatorRow[]): IndicatorRow[] =>
  [...rows].sort((a, b) => a.trading_date.localeCompare(b.trading_date))

export function IndicatorChart({ symbol, fromDate, toDate }: IndicatorChartProps) {
  const { data, isLoading } = useIndicators(symbol, {
    page_size: 500,
    from: fromDate || undefined,
    to: toDate || undefined,
  })
  const { theme } = useTheme()
  const chartTheme = CHART_THEME[theme]
  const [active, setActive] = useState<Record<IndicatorKey, boolean>>({
    ma7: true,
    ma20: true,
    ma50: false,
    rsi14: true,
    macd_line: true,
  })

  const rows = useMemo(() => sortAscending(data?.data ?? []), [data?.data])

  if (isLoading) return <Skeleton className="h-96" />
  if (rows.length === 0) return <EmptyState message="Không có dữ liệu chỉ báo" />

  const toggle = (key: IndicatorKey) => {
    setActive((prev) => ({ ...prev, [key]: !prev[key] }))
  }

  return (
    <div className="grid gap-4">
      <div className="flex flex-wrap gap-2">
        {OPTIONS.map((option) => (
          <button
            key={option.key}
            type="button"
            onClick={() => toggle(option.key)}
            className={`h-8 rounded-md px-3 text-xs font-medium transition-colors ${
              active[option.key]
                ? 'bg-app-input text-app-heading'
                : 'bg-app-hover text-app-muted hover:bg-app-input hover:text-app-heading'
            }`}
          >
            {option.label}
          </button>
        ))}
      </div>
      <div className="h-96">
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
              yAxisId="price"
              stroke={chartTheme.axis}
              tick={{ fontSize: 11 }}
              width={58}
              tickFormatter={(value: number) => formatPrice(value)}
            />
            <YAxis yAxisId="osc" orientation="right" stroke={chartTheme.axis} tick={{ fontSize: 11 }} width={48} />
            <Tooltip
              contentStyle={{
                background: chartTheme.tooltipBg,
                border: `1px solid ${chartTheme.tooltipBorder}`,
                borderRadius: 8,
              }}
              labelStyle={{ color: chartTheme.tooltipText }}
              itemStyle={{ color: chartTheme.tooltipText }}
            />
            <Line yAxisId="price" type="monotone" dataKey="close" name="Giá đóng cửa" stroke={chartTheme.closeLine} strokeWidth={1.5} dot={false} />
            {OPTIONS.map((option) => (
              <Line
                key={option.key}
                yAxisId={option.key === 'rsi14' || option.key === 'macd_line' ? 'osc' : 'price'}
                type="monotone"
                dataKey={option.key}
                name={option.label}
                stroke={option.color}
                strokeWidth={1.8}
                dot={false}
                hide={!active[option.key]}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
