import { Link } from 'react-router-dom'
import type { TopMover } from '@/types'
import { formatPercentPoints, formatPrice, priceColor } from '@/utils/formatters'
import { EmptyState } from '@/components/shared/EmptyState'

interface TopMoversTableProps {
  gainers: TopMover[] | null
  losers: TopMover[] | null
}

function MoverRows({ rows }: { rows: TopMover[] }) {
  return (
    <div className="overflow-hidden rounded-lg border border-app-border">
      <table className="w-full table-fixed text-sm">
        <thead className="bg-app-table-head text-xs uppercase text-app-muted">
          <tr>
            <th className="px-3 py-2 text-left">Ticker</th>
            <th className="px-3 py-2 text-right">Giá đóng cửa</th>
            <th className="px-3 py-2 text-right">Biến động</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-app-border bg-card-dark">
          {rows.map((row) => (
            <tr key={row.ticker} className="hover:bg-app-hover">
              <td className="px-3 py-2">
                <Link to={`/stock/${row.ticker}`} className="font-mono font-semibold text-app-heading hover:text-accent">
                  {row.ticker}
                </Link>
              </td>
              <td className="px-3 py-2 text-right font-mono text-app-text">{formatPrice(row.close)}</td>
              <td className={`px-3 py-2 text-right font-mono font-semibold ${priceColor(row.percent_change)}`}>
                {formatPercentPoints(row.percent_change, true)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function TopMoversTable({ gainers, losers }: TopMoversTableProps) {
  if (!gainers?.length && !losers?.length) {
    return <EmptyState message="Không có dữ liệu biến động" />
  }

  return (
    <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div>
        <h3 className="mb-2 text-sm font-semibold text-price-up">Tăng mạnh</h3>
        <MoverRows rows={gainers ?? []} />
      </div>
      <div>
        <h3 className="mb-2 text-sm font-semibold text-price-down">Giảm mạnh</h3>
        <MoverRows rows={losers ?? []} />
      </div>
    </div>
  )
}
