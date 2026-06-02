import { IndexCard } from './IndexCard'
import { TopMoversTable } from './TopMoversTable'
import { Skeleton } from '@/components/ui/skeleton'
import { useMarketOverview } from '@/hooks/useMarketOverview'
import { formatBillionVnd, formatShares } from '@/utils/formatters'

export function MarketOverviewSection() {
  const { data, isLoading } = useMarketOverview()

  if (isLoading) {
    return (
      <div className="grid gap-4">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
          <Skeleton className="h-32" />
          <Skeleton className="h-32" />
          <Skeleton className="h-32" />
        </div>
        <Skeleton className="h-72" />
      </div>
    )
  }

  return (
    <section className="grid gap-6">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <IndexCard name="VN-Index" close={data?.vnindex_close ?? null} dailyReturn={data?.vnindex_return ?? null} />
        <IndexCard name="VN30" close={data?.vn30_close ?? null} dailyReturn={data?.vn30_return ?? null} />
        <IndexCard name="HNX-Index" close={data?.hnx_close ?? null} dailyReturn={data?.hnx_return ?? null} />
      </div>

      {data && (
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-5">
          {[
            { label: 'Mã tăng', value: data.advances, unit: 'mã cổ phiếu', color: 'text-price-up' },
            { label: 'Mã giảm', value: data.declines, unit: 'mã cổ phiếu', color: 'text-price-down' },
            { label: 'Đứng giá', value: data.unchanged, unit: 'mã cổ phiếu', color: 'text-price-flat' },
            { label: 'Khối lượng', value: formatShares(data.total_volume), unit: 'tổng cổ phiếu khớp lệnh', color: 'text-app-heading' },
            { label: 'Giá trị giao dịch', value: formatBillionVnd(data.total_value), unit: 'quy đổi tỷ VND', color: 'text-app-heading' },
          ].map((item) => (
            <div key={item.label} className="rounded-lg border border-app-border bg-card-dark p-3">
              <p className="text-xs text-app-muted">{item.label}</p>
              <p className={`mt-1 font-mono text-lg font-bold ${item.color}`}>{item.value ?? 'N/A'}</p>
              <p className="mt-1 text-[11px] text-app-subtle">{item.unit}</p>
            </div>
          ))}
        </div>
      )}

      <TopMoversTable gainers={data?.top_gainers ?? null} losers={data?.top_losers ?? null} />
    </section>
  )
}
