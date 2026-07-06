import { Link } from 'react-router-dom'
import type { TopMover } from '@/types'
import { formatPercentPoints, formatPrice, priceColor } from '@/utils/formatters'
import { EmptyState } from '@/components/shared/EmptyState'

interface TopMoversTableProps {
  gainers: TopMover[] | null
  losers: TopMover[] | null
}

const ROW_COUNT = 5

function padRows(rows: TopMover[]): TopMover[] {
  const padded = [...rows]
  while (padded.length < ROW_COUNT) {
    padded.push({ ticker: '', close: 0, percent_change: 0 })
  }
  return padded.slice(0, ROW_COUNT)
}

function MoverRows({ rows, emptyLabel }: { rows: TopMover[]; emptyLabel: string }) {
  const padded = padRows(rows)

  return (
    <div className="min-h-[13.5rem] overflow-hidden rounded-lg border border-app-border">
      <table className="w-full table-fixed text-sm">
        <thead className="bg-app-table-head text-xs uppercase text-app-muted">
          <tr>
            <th className="px-3 py-2.5 text-left">Mã</th>
            <th className="px-3 py-2.5 text-right">Giá đóng cửa</th>
            <th className="px-3 py-2.5 text-right">Biến động</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-app-border bg-card-dark">
          {padded.map((row, index) => {
            if (!row.ticker) {
              return (
                <tr key={`empty-${index}`} className="h-10">
                  <td colSpan={3} className="px-3 py-2 text-center text-xs text-app-subtle">
                    {rows.length === 0 ? emptyLabel : '—'}
                  </td>
                </tr>
              )
            }
            return (
              <tr key={row.ticker} className="h-10 hover:bg-app-hover">
                <td className="px-3 py-2">
                  <Link
                    to={`/stock/${row.ticker}`}
                    className="font-mono text-sm font-semibold text-app-heading hover:text-accent focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
                  >
                    {row.ticker}
                  </Link>
                </td>
                <td className="px-3 py-2 text-right font-mono text-app-text">
                  {formatPrice(row.close)}
                </td>
                <td
                  className={`px-3 py-2 text-right font-mono text-sm font-semibold ${priceColor(row.percent_change)}`}
                >
                  {formatPercentPoints(row.percent_change, true)}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

export function TopMoversTable({ gainers, losers }: TopMoversTableProps) {
  if (!gainers?.length && !losers?.length) {
    return <EmptyState message="Không có dữ liệu biến động trong phiên" />
  }

  return (
    <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div>
        <h3 className="mb-2 text-sm font-semibold text-price-up">Tăng mạnh</h3>
        <MoverRows rows={gainers ?? []} emptyLabel="Không có mã tăng nổi bật" />
      </div>
      <div>
        <h3 className="mb-2 text-sm font-semibold text-price-down">Giảm mạnh</h3>
        <MoverRows rows={losers ?? []} emptyLabel="Không có mã giảm nổi bật" />
      </div>
    </div>
  )
}
