import { format } from 'date-fns'

export const formatPrice = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  return new Intl.NumberFormat('vi-VN', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value)
}

export const formatVolume = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  return new Intl.NumberFormat('vi-VN').format(value)
}

export const formatShares = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  return `${formatVolume(value)} cổ phiếu`
}

export const formatBillionVnd = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  return `${new Intl.NumberFormat('vi-VN', {
    maximumFractionDigits: 2,
  }).format(value / 1_000_000_000)} tỷ VND`
}

export const formatBytes = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  if (value < 1024) return `${value} B`
  const units = ['KB', 'MB', 'GB']
  let size = value / 1024
  let unitIndex = 0
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex += 1
  }
  return `${new Intl.NumberFormat('vi-VN', {
    maximumFractionDigits: 1,
  }).format(size)} ${units[unitIndex]}`
}

export const formatCompact = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  return new Intl.NumberFormat('vi-VN', {
    notation: 'compact',
    maximumFractionDigits: 2,
  }).format(value)
}

export const formatPercent = (
  value: number | null | undefined,
  showSign = true,
): string => {
  if (value == null) return 'N/A'
  const pct = (value * 100).toFixed(2)
  if (!showSign) return `${pct}%`
  return value >= 0 ? `+${pct}%` : `${pct}%`
}

export const formatPercentPoints = (
  value: number | null | undefined,
  showSign = false,
): string => {
  if (value == null) return 'N/A'
  const pct = value.toFixed(2)
  if (!showSign) return `${pct}%`
  return value >= 0 ? `+${pct}%` : `${pct}%`
}

export const priceColor = (value: number | null | undefined): string => {
  if (value == null || value === 0) return 'text-price-flat'
  return value > 0 ? 'text-price-up' : 'text-price-down'
}

export const sentimentColor = (label: string | null | undefined): string => {
  if (label === 'positive') return 'text-sentiment-positive'
  if (label === 'negative') return 'text-sentiment-negative'
  return 'text-sentiment-neutral'
}

export const formatDate = (value: string | null | undefined): string => {
  if (!value) return 'N/A'
  try {
    return format(new Date(value), 'dd/MM/yyyy')
  } catch {
    return value
  }
}
