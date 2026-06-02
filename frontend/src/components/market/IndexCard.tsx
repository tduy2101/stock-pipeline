import { Minus, TrendingDown, TrendingUp } from 'lucide-react'
import { formatPercent, formatPrice, priceColor } from '@/utils/formatters'

interface IndexCardProps {
  name: string
  close: number | null
  dailyReturn: number | null
}

export function IndexCard({ name, close, dailyReturn }: IndexCardProps) {
  const trendIcon =
    dailyReturn == null || dailyReturn === 0 ? (
      <Minus className="text-price-flat" size={18} />
    ) : dailyReturn > 0 ? (
      <TrendingUp className="text-price-up" size={18} />
    ) : (
      <TrendingDown className="text-price-down" size={18} />
    )

  return (
    <div className="rounded-lg border border-app-border bg-card-dark p-4">
      <div className="flex items-center justify-between">
        <p className="text-sm font-medium text-app-text">{name}</p>
        {trendIcon}
      </div>
      <p className="mt-3 font-mono text-2xl font-bold text-app-heading">{formatPrice(close)}</p>
      <p className="mt-1 text-xs text-app-muted">Điểm chỉ số</p>
      <p className={`mt-1 text-sm font-medium ${priceColor(dailyReturn)}`}>
        {formatPercent(dailyReturn)} so với phiên trước
      </p>
    </div>
  )
}
