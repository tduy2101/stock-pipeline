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
import { useIndicators } from '@/hooks/useIndicators'
import { CHART_THEME } from '@/theme/chartTheme'
import { useTheme } from '@/theme/ThemeProvider'
import type { IndicatorRow } from '@/types'
import { formatDate, formatPrice } from '@/utils/formatters'

interface IndicatorChartProps {
  symbol: string
  fromDate?: string
  toDate?: string
}

type MaKey = 'ma7' | 'ma20' | 'ma50'
type OscKey = 'rsi14' | 'macd_line'
type BandKey = 'bb_upper' | 'bb_middle' | 'bb_lower'

const MA_OPTIONS: Array<{ key: MaKey; label: string; color: string }> = [
  { key: 'ma7', label: 'MA7', color: '#22c55e' },
  { key: 'ma20', label: 'MA20', color: '#38bdf8' },
  { key: 'ma50', label: 'MA50', color: '#f59e0b' },
]

const OSC_OPTIONS: Array<{ key: OscKey; label: string; color: string }> = [
  { key: 'rsi14', label: 'RSI14', color: '#a78bfa' },
  { key: 'macd_line', label: 'MACD', color: '#f43f5e' },
]

const BAND_OPTIONS: Array<{ key: BandKey; label: string; color: string }> = [
  { key: 'bb_upper', label: 'BB trên', color: '#94a3b8' },
  { key: 'bb_middle', label: 'BB giữa', color: '#64748b' },
  { key: 'bb_lower', label: 'BB dưới', color: '#94a3b8' },
]

const sortAscending = (rows: IndicatorRow[]): IndicatorRow[] =>
  [...rows].sort((a, b) => a.trading_date.localeCompare(b.trading_date))

function ToggleGroup<T extends string>({
  title,
  options,
  active,
  onToggle,
}: {
  title: string
  options: Array<{ key: T; label: string }>
  active: Record<T, boolean>
  onToggle: (key: T) => void
}) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <span className="text-xs font-medium text-app-muted">{title}</span>
      {options.map((option) => (
        <button
          key={option.key}
          type="button"
          onClick={() => onToggle(option.key)}
          aria-pressed={active[option.key]}
          className={`h-8 rounded-md px-3 text-xs font-medium transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent ${
            active[option.key]
              ? 'bg-accent/15 text-app-heading ring-1 ring-accent/40'
              : 'bg-app-hover text-app-muted hover:bg-app-input hover:text-app-heading'
          }`}
        >
          {option.label}
        </button>
      ))}
    </div>
  )
}

export function IndicatorChart({ symbol, fromDate, toDate }: IndicatorChartProps) {
  const { data, isLoading } = useIndicators(symbol, {
    page_size: 500,
    from: fromDate || undefined,
    to: toDate || undefined,
  })
  const { theme } = useTheme()
  const chartTheme = CHART_THEME[theme]
  const [maActive, setMaActive] = useState<Record<MaKey, boolean>>({
    ma7: true,
    ma20: true,
    ma50: false,
  })
  const [oscActive, setOscActive] = useState<Record<OscKey, boolean>>({
    rsi14: true,
    macd_line: false,
  })
  const [bandActive, setBandActive] = useState<Record<BandKey, boolean>>({
    bb_upper: false,
    bb_middle: false,
    bb_lower: false,
  })

  const rows = useMemo(() => sortAscending(data?.data ?? []), [data?.data])
  const showOscillator = oscActive.rsi14 || oscActive.macd_line

  if (isLoading) return <Skeleton className="h-96" />
  if (rows.length === 0) return <EmptyState message="Không có dữ liệu chỉ báo" />

  const toggleMa = (key: MaKey) => setMaActive((prev) => ({ ...prev, [key]: !prev[key] }))
  const toggleOsc = (key: OscKey) => setOscActive((prev) => ({ ...prev, [key]: !prev[key] }))
  const toggleBand = (key: BandKey) => setBandActive((prev) => ({ ...prev, [key]: !prev[key] }))

  return (
    <div className="grid gap-4">
      <div className="grid gap-3">
        <ToggleGroup title="Trung bình động" options={MA_OPTIONS} active={maActive} onToggle={toggleMa} />
        <ToggleGroup title="Dao động" options={OSC_OPTIONS} active={oscActive} onToggle={toggleOsc} />
        <ToggleGroup title="Bollinger" options={BAND_OPTIONS} active={bandActive} onToggle={toggleBand} />
      </div>
      <div className="h-[26rem]">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={rows} margin={{ top: 10, right: showOscillator ? 52 : 12, bottom: 0, left: 4 }}>
            <CartesianGrid stroke={chartTheme.grid} strokeDasharray="3 3" />
            <XAxis
              dataKey="trading_date"
              stroke={chartTheme.axis}
              tick={{ fontSize: 11 }}
              minTickGap={28}
              tickFormatter={(value: string) => formatDate(value).slice(0, 5)}
            />
            <YAxis
              yAxisId="price"
              stroke={chartTheme.axis}
              tick={{ fontSize: 11 }}
              width={64}
              tickFormatter={(value: number) => formatPrice(value)}
            />
            {showOscillator && (
              <YAxis yAxisId="osc" orientation="right" stroke={chartTheme.axis} tick={{ fontSize: 11 }} width={48} />
            )}
            <Tooltip
              contentStyle={{
                background: chartTheme.tooltipBg,
                border: `1px solid ${chartTheme.tooltipBorder}`,
                borderRadius: 8,
                fontSize: 12,
              }}
              labelFormatter={(value) => formatDate(String(value))}
              labelStyle={{ color: chartTheme.tooltipText }}
              itemStyle={{ color: chartTheme.tooltipText }}
            />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Line
              yAxisId="price"
              type="monotone"
              dataKey="close"
              name="Giá đóng cửa"
              stroke={chartTheme.closeLine}
              strokeWidth={2}
              dot={false}
            />
            {MA_OPTIONS.map((option) => (
              <Line
                key={option.key}
                yAxisId="price"
                type="monotone"
                dataKey={option.key}
                name={option.label}
                stroke={option.color}
                strokeWidth={1.6}
                dot={false}
                hide={!maActive[option.key]}
              />
            ))}
            {BAND_OPTIONS.map((option) => (
              <Line
                key={option.key}
                yAxisId="price"
                type="monotone"
                dataKey={option.key}
                name={option.label}
                stroke={option.color}
                strokeWidth={1.2}
                strokeDasharray="4 4"
                dot={false}
                hide={!bandActive[option.key]}
              />
            ))}
            {showOscillator &&
              OSC_OPTIONS.map((option) => (
                <Line
                  key={option.key}
                  yAxisId="osc"
                  type="monotone"
                  dataKey={option.key}
                  name={option.label}
                  stroke={option.color}
                  strokeWidth={1.6}
                  dot={false}
                  hide={!oscActive[option.key]}
                />
              ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
