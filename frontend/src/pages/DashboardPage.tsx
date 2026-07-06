import { ArrowRight, CalendarDays, Database, FileText, Newspaper } from 'lucide-react'
import { Link } from 'react-router-dom'
import { MarketOverviewSection } from '@/components/market/MarketOverviewSection'
import { MarketNewsFeed } from '@/components/market/MarketNewsFeed'
import { RecentBctcList } from '@/components/market/RecentBctcList'
import { PageWrapper } from '@/components/layout/PageWrapper'
import { StatCard } from '@/components/shared/StatCard'
import { Skeleton } from '@/components/ui/skeleton'
import { useMarketOverview } from '@/hooks/useMarketOverview'
import { useTickers } from '@/hooks/useTickers'
import { resolveUniverseSize } from '@/utils/marketScope'
import { formatDate } from '@/utils/formatters'

export default function DashboardPage() {
  const { data: tickers, isLoading: tickersLoading } = useTickers()
  const { data: market, isLoading: marketLoading } = useMarketOverview()

  const newsTickerCount = tickers?.tickers.filter((item) => item.has_news).length ?? 0
  const bctcTickerCount = tickers?.tickers.filter((item) => item.has_bctc).length ?? 0
  const totalBctcDocs =
    tickers?.tickers.reduce((sum, item) => sum + (item.bctc_doc_count ?? 0), 0) ?? 0
  const tickerTotal = tickers?.total ?? 0

  return (
    <PageWrapper>
      <section className="flex flex-col gap-2">
        <h1 className="text-2xl font-bold text-app-heading">Tổng quan thị trường</h1>
        <p className="max-w-4xl text-sm leading-6 text-app-muted">
          Tra cứu giá cổ phiếu, hồ sơ doanh nghiệp, tin tức và báo cáo tài chính PDF đã được
          tổng hợp từ nhiều nguồn dữ liệu công khai.
        </p>
        {marketLoading ? (
          <Skeleton className="h-4 w-56" />
        ) : market ? (
          <p className="text-xs text-app-subtle">
            Phiên dữ liệu mới nhất:{' '}
            <strong className="text-app-text">{formatDate(market.trading_date)}</strong>
            {(() => {
              const sessionSize = market ? resolveUniverseSize(market) : 0
              if (sessionSize <= 0) return null
              return (
                <>
                  {' '}
                  · {new Intl.NumberFormat('vi-VN').format(sessionSize)} mã có dữ liệu giá trong
                  phiên (khác với tổng mã trong hệ thống bên dưới)
                </>
              )
            })()}
          </p>
        ) : null}
      </section>

      <section className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        {tickersLoading || marketLoading ? (
          <>
            <Skeleton className="h-28" />
            <Skeleton className="h-28" />
            <Skeleton className="h-28" />
          </>
        ) : (
          <>
            <StatCard
              icon={CalendarDays}
              label="Phiên mới nhất"
              value={market ? formatDate(market.trading_date) : 'Chưa có dữ liệu'}
              note="Giá và chỉ số thị trường đã cập nhật"
            />
            <StatCard
              icon={Database}
              label="Mã trong hệ thống"
              value={`${new Intl.NumberFormat('vi-VN').format(tickerTotal)} mã`}
              note="Danh mục hợp nhất từ niêm yết, giá, doanh nghiệp, tin tức và BCTC — không trùng với số mã có giá trong một phiên"
            />
            <StatCard
              icon={FileText}
              label="BCTC PDF"
              value={`${new Intl.NumberFormat('vi-VN').format(totalBctcDocs)} tài liệu`}
              note={`${new Intl.NumberFormat('vi-VN').format(bctcTickerCount)} mã cổ phiếu có tài liệu`}
            />
          </>
        )}
      </section>

      <MarketOverviewSection />

      <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {[
          {
            to: '/news',
            icon: Newspaper,
            title: 'Kho tin tức',
            value: `${new Intl.NumberFormat('vi-VN').format(newsTickerCount)} mã có tin`,
            description: 'Tìm kiếm bài viết theo mã, sắc thái và khoảng thời gian.',
          },
          {
            to: '/bctc',
            icon: FileText,
            title: 'Kho BCTC PDF',
            value: `${new Intl.NumberFormat('vi-VN').format(totalBctcDocs)} tài liệu`,
            description: 'Tra cứu báo cáo tài chính đã crawl, lọc theo mã và năm báo cáo.',
          },
        ].map((item) => (
          <Link
            key={item.to}
            to={item.to}
            className="group rounded-lg border border-app-border bg-panel-dark p-5 transition-colors hover:border-accent/60 hover:bg-app-hover focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
          >
            <div className="flex items-start justify-between gap-4">
              <div className="flex min-w-0 gap-3">
                <span className="grid h-10 w-10 shrink-0 place-items-center rounded-lg bg-accent/10 text-accent">
                  <item.icon size={20} aria-hidden />
                </span>
                <div className="min-w-0">
                  <h2 className="text-base font-semibold text-app-heading">{item.title}</h2>
                  <p className="mt-1 text-sm text-app-muted">{item.description}</p>
                  <p className="mt-3 font-mono text-sm font-semibold text-app-heading">{item.value}</p>
                </div>
              </div>
              <ArrowRight
                className="shrink-0 text-app-muted transition-colors group-hover:text-accent"
                size={18}
                aria-hidden
              />
            </div>
          </Link>
        ))}
      </section>

      <section className="grid grid-cols-1 gap-5 xl:grid-cols-2">
        <MarketNewsFeed />
        <RecentBctcList />
      </section>
    </PageWrapper>
  )
}
