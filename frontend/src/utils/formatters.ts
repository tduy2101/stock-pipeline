import { format, parseISO } from 'date-fns'

const viNumber = (value: number, options?: Intl.NumberFormatOptions) =>
  new Intl.NumberFormat('vi-VN', options).format(value)

const parseDateValue = (value: string): Date => {
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return parseISO(value)
  }
  return new Date(value)
}

export const formatPrice = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  return viNumber(value, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  })
}

export const formatIndexPoints = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  return `${viNumber(value, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })} điểm`
}

export const formatVolume = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  return viNumber(value, { maximumFractionDigits: 0 })
}

export const formatCompactShares = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  const abs = Math.abs(value)
  if (abs >= 1_000_000_000) {
    return `${viNumber(value / 1_000_000_000, { maximumFractionDigits: 2 })} tỷ cổ phiếu`
  }
  if (abs >= 1_000_000) {
    return `${viNumber(value / 1_000_000, { maximumFractionDigits: 2 })} triệu cổ phiếu`
  }
  if (abs >= 1_000) {
    return `${viNumber(value / 1_000, { maximumFractionDigits: 2 })} nghìn cổ phiếu`
  }
  return `${viNumber(value, { maximumFractionDigits: 0 })} cổ phiếu`
}

/** Trading value stored as full VND (close × volume from silver.price). */
export const formatTradingValueVnd = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  const abs = Math.abs(value)
  if (abs >= 1_000_000_000_000) {
    return formatTradingValueThousandBillionVnd(value)
  }
  if (abs >= 1_000_000_000) {
    return `${viNumber(value / 1_000_000_000, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })} tỷ đồng`
  }
  if (abs >= 1_000_000) {
    return `${viNumber(value / 1_000_000, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })} triệu đồng`
  }
  return `${viNumber(value, { maximumFractionDigits: 0 })} đồng`
}

/** Market/session turnover: always express as nghìn tỷ đồng (value / 10^12 VND). */
export const formatTradingValueThousandBillionVnd = (
  value: number | null | undefined,
): string => {
  if (value == null) return 'N/A'
  return `${viNumber(value / 1_000_000_000_000, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })} nghìn tỷ đồng`
}

/** @deprecated Use formatTradingValueVnd — kept for gradual migration */
export const formatBillionVnd = formatTradingValueVnd

export const formatShares = (value: number | null | undefined): string => {
  if (value == null) return 'N/A'
  return `${formatVolume(value)} cổ phiếu`
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
  return `${viNumber(size, { maximumFractionDigits: 1 })} ${units[unitIndex]}`
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
  const pct = viNumber(value * 100, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })
  if (!showSign) return `${pct}%`
  return value >= 0 ? `+${pct}%` : `${pct}%`
}

export const formatPercentPoints = (
  value: number | null | undefined,
  showSign = false,
): string => {
  if (value == null) return 'N/A'
  const pct = viNumber(value, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })
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
    return format(parseDateValue(value), 'dd/MM/yyyy')
  } catch {
    return value
  }
}

/** Ngày đăng tin theo giờ VN — khớp bộ lọc API /news (ICT). */
export const formatNewsPublishDate = (
  publishedAt: string | null | undefined,
  publishedDate: string | null | undefined,
): string => {
  if (publishedAt) {
    try {
      return new Intl.DateTimeFormat('vi-VN', {
        timeZone: 'Asia/Ho_Chi_Minh',
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
      }).format(parseDateValue(publishedAt))
    } catch {
      // fall through to published_date
    }
  }
  return formatDate(publishedDate)
}

export const formatDateTime = (value: string | null | undefined): string => {
  if (!value) return 'N/A'
  try {
    return format(parseDateValue(value), 'dd/MM/yyyy HH:mm')
  } catch {
    return value
  }
}
