import { Minus, TrendingDown, TrendingUp } from 'lucide-react'
import { formatDate, formatIndexPoints, formatPercent, priceColor } from '@/utils/formatters'

interface IndexCardProps {
  name: string
  close: number | null
  dailyReturn: number | null
  tradingDate?: string | null
}

export function IndexCard({ name, close, dailyReturn, tradingDate }: IndexCardProps) {
  const trendIcon =
    dailyReturn == null || dailyReturn === 0 ? (
      <Minus className="text-price-flat" size={18} aria-hidden />
    ) : dailyReturn > 0 ? (
      <TrendingUp className="text-price-up" size={18} aria-hidden />
    ) : (
      <TrendingDown className="text-price-down" size={18} aria-hidden />
    )

  const trendLabel =
    dailyReturn == null || dailyReturn === 0
      ? 'Đi ngang'
      : dailyReturn > 0
        ? 'Tăng'
        : 'Giảm'

  return (
    <div className="rounded-lg border border-app-border bg-card-dark p-4">
      <div className="flex items-center justify-between gap-2">
        <p className="text-sm font-medium text-app-text">{name}</p>
        <span className="inline-flex items-center gap-1 text-xs text-app-muted" title={trendLabel}>
          {trendIcon}
          <span className="sr-only">{trendLabel}</span>
        </span>
      </div>
      <p className="mt-3 font-mono text-2xl font-bold tracking-tight text-app-heading">
        {formatIndexPoints(close)}
      </p>
      <p className={`mt-2 text-sm font-medium ${priceColor(dailyReturn)}`}>
        {formatPercent(dailyReturn)} so với phiên trước
      </p>
      {tradingDate && (
        <p className="mt-1 text-xs text-app-muted">Phiên {formatDate(tradingDate)}</p>
      )}
    </div>
  )
}
