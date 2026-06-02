import { useMemo, useState } from 'react'
import { EmptyState } from '@/components/shared/EmptyState'
import { Skeleton } from '@/components/ui/skeleton'
import { useFinancials } from '@/hooks/useFinancials'
import type { FinancialSummaryRow } from '@/types'
import { formatPercentPoints, formatPrice } from '@/utils/formatters'

interface FinancialTableProps {
  symbol: string
}

type MetricKey = Exclude<
  keyof FinancialSummaryRow,
  'ticker' | 'period' | 'period_type' | 'year' | 'quarter'
>

interface MetricColumn {
  key: MetricKey
  label: string
  kind?: 'number' | 'percent' | 'signedPercent'
}

const metrics: MetricColumn[] = [
  { key: 'pe_ratio', label: 'P/E' },
  { key: 'pb_ratio', label: 'P/B' },
  { key: 'ps_ratio', label: 'P/S' },
  { key: 'eps', label: 'EPS' },
  { key: 'roe', label: 'ROE', kind: 'percent' },
  { key: 'roa', label: 'ROA', kind: 'percent' },
  { key: 'gross_profit_margin', label: 'Biên gộp', kind: 'percent' },
  { key: 'net_profit_margin', label: 'Biên ròng', kind: 'percent' },
  { key: 'current_ratio', label: 'TT hiện hành' },
  { key: 'quick_ratio', label: 'TT nhanh' },
  { key: 'debt_to_equity', label: 'Nợ/VCSH' },
  { key: 'revenue_growth', label: 'Tăng DT', kind: 'signedPercent' },
  { key: 'profit_growth', label: 'Tăng LN', kind: 'signedPercent' },
  { key: 'dividend_yield', label: 'Cổ tức', kind: 'percent' },
]

const formatMetric = (
  row: FinancialSummaryRow,
  metric: MetricColumn,
): string => {
  const value = row[metric.key] as number | null
  if (metric.kind === 'percent') return formatPercentPoints(value, false)
  if (metric.kind === 'signedPercent') return formatPercentPoints(value, true)
  return formatPrice(value)
}

const hasVisibleMetric = (row: FinancialSummaryRow): boolean =>
  metrics.some((metric) => row[metric.key] != null)

export function FinancialTable({ symbol }: FinancialTableProps) {
  const [periodType, setPeriodType] = useState<string | undefined>('quarter')
  const { data, isLoading } = useFinancials(symbol, periodType)

  const rows = useMemo(
    () => (data ?? []).filter(hasVisibleMetric).slice(0, 40),
    [data],
  )

  if (isLoading) return <Skeleton className="h-80" />
  if (rows.length === 0) return <EmptyState message="Không có chỉ số tài chính" />

  return (
    <div className="grid gap-4">
      <div className="flex flex-wrap gap-2">
        {[
          { label: 'Quý', value: 'quarter' },
          { label: 'Năm', value: 'annual' },
          { label: 'Tất cả', value: undefined },
        ].map((item) => (
          <button
            key={item.label}
            type="button"
            onClick={() => setPeriodType(item.value)}
            className={`h-8 rounded-md px-3 text-xs font-medium transition-colors ${
              periodType === item.value
                ? 'bg-accent text-white'
                : 'bg-app-hover text-app-muted hover:bg-app-input hover:text-app-heading'
            }`}
          >
            {item.label}
          </button>
        ))}
      </div>

      <div className="max-h-[32rem] overflow-auto rounded-lg border border-app-border scrollbar-thin">
        <table className="w-full min-w-[1180px] text-sm">
          <thead className="sticky top-0 bg-app-table-head text-xs uppercase text-app-muted">
            <tr>
              <th className="sticky left-0 z-10 bg-app-table-head px-3 py-2 text-left">Kỳ</th>
              {metrics.map((metric) => (
                <th key={metric.key} className="px-3 py-2 text-right">
                  {metric.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-app-border bg-card-dark">
            {rows.map((row) => (
              <tr key={`${row.ticker}-${row.period}`} className="hover:bg-app-hover">
                <td className="sticky left-0 bg-card-dark px-3 py-2 font-mono text-app-heading">
                  <span className="block">{row.period}</span>
                  <span className="text-[11px] text-app-muted">
                    {row.period_type ?? 'N/A'}
                  </span>
                </td>
                {metrics.map((metric) => (
                  <td key={metric.key} className="whitespace-nowrap px-3 py-2 text-right font-mono text-app-text">
                    {formatMetric(row, metric)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
