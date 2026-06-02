import { EmptyState } from '@/components/shared/EmptyState'
import { Skeleton } from '@/components/ui/skeleton'
import { usePriceBoard } from '@/hooks/useBoard'
import type { PriceBoardRow } from '@/types'

interface PriceBoardTableProps {
  symbol: string
  fromDate?: string
  toDate?: string
}

const formatNumber = (value: number | null | undefined) =>
  value == null ? 'N/A' : value.toLocaleString('vi-VN')

function PriceCell({
  price,
  referencePrice,
  ceilingPrice,
  floorPrice,
}: {
  price: number | null | undefined
  referencePrice: number | null | undefined
  ceilingPrice: number | null | undefined
  floorPrice: number | null | undefined
}) {
  if (price == null || price === 0) return <span className="text-app-subtle">N/A</span>

  const className =
    ceilingPrice != null && price === ceilingPrice
      ? 'text-purple-400'
      : floorPrice != null && price === floorPrice
        ? 'text-cyan-400'
        : referencePrice != null && price > referencePrice
          ? 'text-price-up'
          : referencePrice != null && price < referencePrice
            ? 'text-price-down'
            : 'text-yellow-300'

  return (
    <span className={`font-mono font-semibold ${className}`}>
      {price.toLocaleString('vi-VN')}
    </span>
  )
}

const getBoardNumber = (row: PriceBoardRow, key: keyof PriceBoardRow) =>
  row[key] as number | null

export function PriceBoardTable({ symbol, fromDate, toDate }: PriceBoardTableProps) {
  const { data, isLoading } = usePriceBoard(symbol, {
    page_size: 1,
    from: fromDate || undefined,
    to: toDate || undefined,
  })
  const row = data?.[0]

  if (isLoading) return <Skeleton className="h-72" />
  if (!row) return <EmptyState message="Chưa có dữ liệu bảng giá" />

  return (
    <div className="grid gap-4">
      <h3 className="text-sm font-semibold text-app-heading">
        Bảng giá {row.trading_date ? new Date(row.trading_date).toLocaleDateString('vi-VN') : 'gần nhất'}
      </h3>

      <div className="grid grid-cols-3 gap-3 text-sm">
        {[
          { label: 'Trần', value: row.ceiling_price, className: 'text-purple-400' },
          { label: 'Tham chiếu', value: row.reference_price, className: 'text-yellow-300' },
          { label: 'Sàn', value: row.floor_price, className: 'text-cyan-400' },
        ].map((item) => (
          <div key={item.label} className="rounded-lg border border-app-border bg-app-hover p-3 text-center">
            <div className="mb-1 text-xs text-app-muted">{item.label}</div>
            <div className={`font-mono text-sm font-semibold ${item.className}`}>
              {formatNumber(item.value)}
            </div>
          </div>
        ))}
      </div>

      <div className="overflow-hidden rounded-lg border border-app-border">
        <table className="w-full text-sm">
          <thead className="bg-app-table-head">
            <tr>
              <th className="px-3 py-2 text-left text-xs font-medium text-app-muted">KL mua</th>
              <th className="px-3 py-2 text-center text-xs font-medium text-price-up">Giá mua</th>
              <th className="px-3 py-2 text-center text-xs font-medium text-price-down">Giá bán</th>
              <th className="px-3 py-2 text-right text-xs font-medium text-app-muted">KL bán</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-app-border bg-card-dark">
            {[1, 2, 3].map((level) => (
              <tr key={level} className="hover:bg-app-hover">
                <td className="px-3 py-2 text-left font-mono text-app-text">
                  {formatNumber(getBoardNumber(row, `bid_vol_${level}` as keyof PriceBoardRow))}
                </td>
                <td className="px-3 py-2 text-center">
                  <PriceCell
                    price={getBoardNumber(row, `bid_price_${level}` as keyof PriceBoardRow)}
                    referencePrice={row.reference_price}
                    ceilingPrice={row.ceiling_price}
                    floorPrice={row.floor_price}
                  />
                </td>
                <td className="px-3 py-2 text-center">
                  <PriceCell
                    price={getBoardNumber(row, `ask_price_${level}` as keyof PriceBoardRow)}
                    referencePrice={row.reference_price}
                    ceilingPrice={row.ceiling_price}
                    floorPrice={row.floor_price}
                  />
                </td>
                <td className="px-3 py-2 text-right font-mono text-app-text">
                  {formatNumber(getBoardNumber(row, `ask_vol_${level}` as keyof PriceBoardRow))}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="grid grid-cols-1 gap-3 text-xs text-app-muted sm:grid-cols-2">
        <div className="rounded-lg border border-app-border bg-app-hover p-3">
          <div className="mb-1">Spread bid/ask</div>
          <div className="font-mono text-app-heading">
            {row.spread_pct != null ? `${row.spread_pct.toFixed(3)}%` : 'N/A'}
          </div>
        </div>
        <div className="rounded-lg border border-app-border bg-app-hover p-3">
          <div className="mb-1">Room ngoại còn lại</div>
          <div className="font-mono text-app-heading">{formatNumber(row.foreign_room)}</div>
        </div>
      </div>
    </div>
  )
}
