import { useMemo, useState } from 'react'
import { EmptyState } from '@/components/shared/EmptyState'
import { Skeleton } from '@/components/ui/skeleton'
import { useFinancials } from '@/hooks/useFinancials'
import { formatPrice } from '@/utils/formatters'

interface FinancialTableProps {
  symbol: string
}

const metricUnit = (itemCode: string): string => {
  const code = itemCode.toLowerCase()
  if (code.includes('ratio') || code === 'roe' || code === 'roa' || code.includes('margin')) return '% / lần'
  if (code.includes('eps')) return 'VND / cp'
  if (code.includes('share')) return 'cổ phiếu'
  return 'theo nguồn'
}

export function FinancialTable({ symbol }: FinancialTableProps) {
  const [periodType, setPeriodType] = useState<string | undefined>('quarter')
  const { data, isLoading } = useFinancials(symbol, periodType)

  const rows = useMemo(() => (data ?? []).slice(0, 160), [data])

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
        <table className="w-full min-w-[760px] text-sm">
          <thead className="sticky top-0 bg-app-table-head text-xs uppercase text-app-muted">
            <tr>
              <th className="px-3 py-2 text-left">Kỳ</th>
              <th className="px-3 py-2 text-left">Mã</th>
              <th className="px-3 py-2 text-left">Chỉ tiêu</th>
              <th className="px-3 py-2 text-left">Đơn vị</th>
              <th className="px-3 py-2 text-right">Giá trị</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-app-border bg-card-dark">
            {rows.map((row) => (
              <tr key={`${row.period}-${row.item_code}`} className="hover:bg-app-hover">
                <td className="whitespace-nowrap px-3 py-2 font-mono text-app-text">{row.period}</td>
                <td className="px-3 py-2 font-mono text-xs text-app-muted">{row.item_code}</td>
                <td className="px-3 py-2 text-app-text">{row.item_name ?? 'N/A'}</td>
                <td className="px-3 py-2 text-xs text-app-muted">{metricUnit(row.item_code)}</td>
                <td className="px-3 py-2 text-right font-mono text-app-heading">{formatPrice(row.value)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
