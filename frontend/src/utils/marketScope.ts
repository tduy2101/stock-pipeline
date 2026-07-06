import type { MarketOverviewResponse } from '@/types'
import { formatDate } from '@/utils/formatters'

/** Tiêu đề khu vực tổng hợp breadth / thanh khoản / top movers */
export const SESSION_OVERVIEW_TITLE = 'Tổng quan các mã có dữ liệu trong phiên'

export const SESSION_SCOPE_TOOLTIP =
  'Các số liệu tăng, giảm, đứng giá, tổng khối lượng và tổng giá trị giao dịch được tính trên các mã có dữ liệu giá trong phiên đang chọn, không đại diện cho toàn bộ thị trường.'

export const TOP_MOVERS_SCOPE_NOTE =
  'Xếp hạng trong tập mã có dữ liệu của phiên'

export const formatTickerCount = (count: number): string =>
  new Intl.NumberFormat('vi-VN').format(count)

/**
 * Số mã dùng cho breadth và thanh khoản.
 * Ưu tiên `universe_size` từ API; fallback cộng advances + declines + unchanged.
 */
export const resolveUniverseSize = (
  overview: Pick<
    MarketOverviewResponse,
    'universe_size' | 'advances' | 'declines' | 'unchanged'
  >,
): number => {
  if (overview.universe_size != null && overview.universe_size > 0) {
    return overview.universe_size
  }
  return (overview.advances ?? 0) + (overview.declines ?? 0) + (overview.unchanged ?? 0)
}

export const buildSessionScopeDescription = (
  universeSize: number,
  tradingDate?: string | null,
): string => {
  if (universeSize <= 0) {
    return 'Chưa có dữ liệu cho phiên được chọn'
  }
  const countLabel = `${formatTickerCount(universeSize)} mã cổ phiếu`
  if (tradingDate) {
    return `Các chỉ số độ rộng và thanh khoản được tổng hợp từ ${countLabel} có dữ liệu trong phiên ${formatDate(tradingDate)}.`
  }
  return `Các chỉ số độ rộng và thanh khoản được tổng hợp từ ${countLabel} có dữ liệu trong phiên.`
}

export const buildLiquidityScopeNote = (universeSize: number): string => {
  if (universeSize <= 0) return 'Chưa có dữ liệu cho phiên được chọn'
  return `Tổng hợp trên ${formatTickerCount(universeSize)} mã có dữ liệu trong phiên`
}

export const formatBreadthSummary = (
  advances: number | null | undefined,
  declines: number | null | undefined,
  unchanged: number | null | undefined,
): string => {
  const up = advances ?? 0
  const down = declines ?? 0
  const flat = unchanged ?? 0
  return `${formatTickerCount(up)} tăng · ${formatTickerCount(down)} giảm · ${formatTickerCount(flat)} đứng giá`
}
