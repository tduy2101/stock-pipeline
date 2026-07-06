import { useState } from 'react'
import { IndexCard } from './IndexCard'
import { TopMoversTable } from './TopMoversTable'
import { EmptyState } from '@/components/shared/EmptyState'
import { InfoTooltip } from '@/components/shared/InfoTooltip'
import { QueryErrorState } from '@/components/shared/QueryErrorState'
import { Skeleton } from '@/components/ui/skeleton'
import { useMarketOverview } from '@/hooks/useMarketOverview'
import {
  SESSION_OVERVIEW_TITLE,
  SESSION_SCOPE_TOOLTIP,
  TOP_MOVERS_SCOPE_NOTE,
  buildLiquidityScopeNote,
  buildSessionScopeDescription,
  formatBreadthSummary,
  formatTickerCount,
  resolveUniverseSize,
} from '@/utils/marketScope'
import {
  formatCompactShares,
  formatDate,
  formatTradingValueThousandBillionVnd,
} from '@/utils/formatters'
import type { MarketOverviewResponse } from '@/types'

function SessionDateFilter({
  selectedDate,
  onChange,
  onClear,
  isFetching,
}: {
  selectedDate: string
  onChange: (value: string) => void
  onClear: () => void
  isFetching?: boolean
}) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <label className="text-xs text-app-muted" htmlFor="market-date">
        Chọn phiên
      </label>
      <input
        id="market-date"
        type="date"
        value={selectedDate}
        onChange={(event) => onChange(event.target.value)}
        aria-busy={isFetching}
        className="h-9 rounded-md border border-app-border bg-app-input px-3 text-sm text-app-heading outline-none focus:border-accent focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
      />
      {selectedDate ? (
        <button
          type="button"
          onClick={onClear}
          className="h-9 rounded-md border border-app-border px-3 text-xs text-app-muted hover:text-app-heading"
        >
          Mới nhất
        </button>
      ) : null}
      {isFetching ? (
        <span className="text-xs text-app-subtle" aria-live="polite">
          Đang tải…
        </span>
      ) : null}
    </div>
  )
}

function SectionHeader({
  selectedDate,
  onDateChange,
  onDateClear,
  isFetching,
  scopeDescription,
}: {
  selectedDate: string
  onDateChange: (value: string) => void
  onDateClear: () => void
  isFetching?: boolean
  scopeDescription?: string
}) {
  return (
    <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-lg font-semibold text-app-heading">{SESSION_OVERVIEW_TITLE}</h2>
          <InfoTooltip text={SESSION_SCOPE_TOOLTIP} label="Giải thích phạm vi dữ liệu phiên" />
        </div>
        {scopeDescription ? (
          <p className="mt-1 text-sm leading-6 text-app-muted">{scopeDescription}</p>
        ) : (
          <p className="mt-1 text-sm leading-6 text-app-muted">
            Chọn ngày giao dịch để xem tổng quan phiên tương ứng trong hệ thống.
          </p>
        )}
        <p className="mt-1 text-xs text-app-subtle">
          Chỉ số VN-Index, VN30 và HNX ở dưới lấy từ dữ liệu chỉ số — khác phạm vi tổng hợp mã cổ
          phiếu bên dưới.
        </p>
      </div>
      <SessionDateFilter
        selectedDate={selectedDate}
        onChange={onDateChange}
        onClear={onDateClear}
        isFetching={isFetching}
      />
    </div>
  )
}

function OverviewSkeleton() {
  return (
    <div className="grid gap-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <Skeleton className="h-36" />
        <Skeleton className="h-36" />
        <Skeleton className="h-36" />
      </div>
      <Skeleton className="h-72" />
    </div>
  )
}

export function MarketOverviewSection() {
  const [selectedDate, setSelectedDate] = useState('')
  const { data, isLoading, isFetching, isError, refetch } = useMarketOverview(
    selectedDate || undefined,
  )

  const showInitialSkeleton = isLoading && data === undefined
  const hasSessionData = data != null
  const scopeDescription = hasSessionData
    ? buildSessionScopeDescription(resolveUniverseSize(data), data.trading_date)
    : selectedDate
      ? `Đang xem phiên ${formatDate(selectedDate)}`
      : undefined

  return (
    <section className="grid gap-6">
      <SectionHeader
        selectedDate={selectedDate}
        onDateChange={setSelectedDate}
        onDateClear={() => setSelectedDate('')}
        isFetching={isFetching && !showInitialSkeleton}
        scopeDescription={scopeDescription}
      />

      {showInitialSkeleton ? <OverviewSkeleton /> : null}

      {!showInitialSkeleton && isError ? (
        <QueryErrorState
          title="Không tải được dữ liệu phiên"
          message="Kiểm tra backend đang chạy (port 8000) và thử lại."
          onRetry={() => refetch()}
        />
      ) : null}

      {!showInitialSkeleton && !isError && !hasSessionData ? (
        <EmptyState
          message={
            selectedDate
              ? `Không có dữ liệu tổng quan cho phiên ${formatDate(selectedDate)}`
              : 'Chưa có dữ liệu tổng quan thị trường'
          }
          subMessage={
            selectedDate
              ? 'Ngày này có thể không phải ngày giao dịch hoặc pipeline chưa nạp dữ liệu. Chọn ngày khác hoặc bấm «Mới nhất».'
              : 'Chạy pipeline structured và refresh Gold để có dữ liệu phiên mới nhất.'
          }
        />
      ) : null}

      {!showInitialSkeleton && !isError && hasSessionData ? (
        <div
          className={`grid gap-6 transition-opacity ${isFetching ? 'opacity-60' : 'opacity-100'}`}
          aria-busy={isFetching}
        >
          <SessionOverviewContent data={data} />
        </div>
      ) : null}
    </section>
  )
}

function SessionOverviewContent({ data }: { data: MarketOverviewResponse }) {
  const universeSize = resolveUniverseSize(data)
  const liquidityScopeNote = buildLiquidityScopeNote(universeSize)
  const hasUniverse = universeSize > 0
  const upPct = hasUniverse ? ((data.advances ?? 0) / universeSize) * 100 : 0
  const downPct = hasUniverse ? ((data.declines ?? 0) / universeSize) * 100 : 0
  const flatPct = hasUniverse ? ((data.unchanged ?? 0) / universeSize) * 100 : 0

  return (
    <>
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <IndexCard
          name="VN-Index"
          close={data.vnindex_close}
          dailyReturn={data.vnindex_return}
          tradingDate={data.trading_date}
        />
        <IndexCard
          name="VN30-Index"
          close={data.vn30_close}
          dailyReturn={data.vn30_return}
          tradingDate={data.trading_date}
        />
        <IndexCard
          name="HNX-Index"
          close={data.hnx_close}
          dailyReturn={data.hnx_return}
          tradingDate={data.trading_date}
        />
      </div>

      <div className="grid gap-4 rounded-lg border border-app-border bg-panel-dark p-4">
        <div>
          <h3 className="text-sm font-semibold text-app-heading">Độ rộng trong phiên</h3>
          {hasUniverse ? (
            <>
              <p className="mt-1 text-sm text-app-muted">
                {formatBreadthSummary(data.advances, data.declines, data.unchanged)}
              </p>
              <p className="mt-1 text-xs text-app-subtle">
                Tổng cộng: {formatTickerCount(universeSize)} mã có dữ liệu trong phiên
              </p>
            </>
          ) : (
            <p className="mt-1 text-sm text-app-muted">Chưa có dữ liệu mã cổ phiếu cho phiên này</p>
          )}
        </div>
        {hasUniverse ? (
          <>
            <div
              className="flex h-3 overflow-hidden rounded-full bg-app-hover"
              role="img"
              aria-label={`Độ rộng: ${data.advances ?? 0} tăng, ${data.declines ?? 0} giảm, ${data.unchanged ?? 0} đứng giá, tổng ${universeSize} mã`}
            >
              <div className="bg-price-up transition-all" style={{ width: `${upPct}%` }} />
              <div className="bg-app-muted transition-all" style={{ width: `${flatPct}%` }} />
              <div className="bg-price-down transition-all" style={{ width: `${downPct}%` }} />
            </div>
            <div className="flex flex-wrap gap-4 text-xs text-app-muted">
              <span className="inline-flex items-center gap-1.5">
                <span className="h-2 w-2 rounded-full bg-price-up" aria-hidden />
                Tăng
              </span>
              <span className="inline-flex items-center gap-1.5">
                <span className="h-2 w-2 rounded-full bg-app-muted" aria-hidden />
                Đứng giá
              </span>
              <span className="inline-flex items-center gap-1.5">
                <span className="h-2 w-2 rounded-full bg-price-down" aria-hidden />
                Giảm
              </span>
            </div>
          </>
        ) : null}
      </div>

      <div className="grid gap-3">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="text-sm font-semibold text-app-heading">Thanh khoản</h3>
            <InfoTooltip
              text={`${SESSION_SCOPE_TOOLTIP} Khối lượng là tổng cổ phiếu khớp lệnh; giá trị giao dịch quy đổi nghìn tỷ đồng (VND ÷ 10¹²) trên cùng tập mã.`}
              label="Giải thích thanh khoản trong phiên"
            />
          </div>
          <p className="mt-1 text-xs text-app-subtle">{liquidityScopeNote}</p>
        </div>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div className="rounded-lg border border-app-border bg-card-dark p-4">
            <p className="text-xs text-app-muted">Tổng khối lượng</p>
            <p className="mt-2 font-mono text-xl font-bold text-app-heading">
              {hasUniverse ? formatCompactShares(data.total_volume) : 'Chưa có dữ liệu'}
            </p>
            {hasUniverse ? (
              <p className="mt-1 text-[11px] text-app-subtle">Trong tập mã của phiên</p>
            ) : null}
          </div>
          <div className="rounded-lg border border-app-border bg-card-dark p-4">
            <p className="text-xs text-app-muted">Tổng giá trị giao dịch</p>
            <p className="mt-2 font-mono text-xl font-bold text-app-heading">
              {hasUniverse
                ? formatTradingValueThousandBillionVnd(data.total_value)
                : 'Chưa có dữ liệu'}
            </p>
            {hasUniverse ? (
              <p className="mt-1 text-[11px] text-app-subtle">Trong tập mã của phiên</p>
            ) : null}
          </div>
        </div>
      </div>

      <div className="grid gap-2">
        <div>
          <h3 className="text-sm font-semibold text-app-heading">Biến động nổi bật</h3>
          <p className="mt-1 text-xs text-app-subtle">{TOP_MOVERS_SCOPE_NOTE}</p>
        </div>
        <TopMoversTable gainers={data.top_gainers} losers={data.top_losers} />
      </div>
    </>
  )
}
