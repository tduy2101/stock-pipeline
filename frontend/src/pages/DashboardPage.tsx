import { Activity, ArrowRight, Database, FileText, Newspaper } from 'lucide-react'
import { Link } from 'react-router-dom'
import { MarketOverviewSection } from '@/components/market/MarketOverviewSection'
import { MarketNewsFeed } from '@/components/market/MarketNewsFeed'
import { RecentBctcList } from '@/components/market/RecentBctcList'
import { PageWrapper } from '@/components/layout/PageWrapper'
import { useTickers } from '@/hooks/useTickers'

export default function DashboardPage() {
  const { data } = useTickers()
  const newsTickerCount = data?.tickers.filter((item) => item.has_news).length ?? 0
  const bctcTickerCount = data?.tickers.filter((item) => item.has_bctc).length ?? 0

  return (
    <PageWrapper>
      <section className="flex flex-col gap-3">
        <div>
          <h1 className="text-2xl font-bold text-app-heading">Vietnam Stock Intelligence</h1>
          <p className="mt-1 text-sm text-app-muted">
            Bảng điều khiển Gold layer cho giá, sắc thái tin tức và BCTC PDF của thị trường chứng khoán Việt Nam.
          </p>
        </div>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
          {[
            { icon: Activity, label: 'Tầng thị trường', value: 'Gold marts', note: 'Giá và chỉ số thị trường' },
            { icon: Database, label: 'Danh mục mã', value: `${data?.total ?? 0} mã`, note: 'Listing, hồ sơ, giá, tin tức, BCTC' },
            { icon: FileText, label: 'Tài liệu công bố', value: `${bctcTickerCount} mã`, note: 'Ticker có BCTC PDF' },
          ].map((item) => (
            <div key={item.label} className="flex items-center gap-3 rounded-lg border border-app-border bg-panel-dark p-4">
              <item.icon className="text-accent" size={18} />
              <div>
                <p className="text-xs text-app-muted">{item.label}</p>
                <p className="text-sm font-semibold text-app-heading">{item.value}</p>
                <p className="mt-1 text-xs text-app-subtle">{item.note}</p>
              </div>
            </div>
          ))}
        </div>
      </section>

      <MarketOverviewSection />

      <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {[
          {
            to: '/news',
            icon: Newspaper,
            title: 'Kho tin tức đã crawl',
            value: `${newsTickerCount} mã có tin`,
            description: 'Mở trang tổng hợp tất cả bài báo, lọc theo ticker, sắc thái và nội dung.',
          },
          {
            to: '/bctc',
            icon: FileText,
            title: 'Kho BCTC PDF',
            value: `${bctcTickerCount} mã có PDF`,
            description: 'Mở trang tổng hợp tài liệu BCTC, lọc theo ticker, năm và tiêu đề.',
          },
        ].map((item) => (
          <Link
            key={item.to}
            to={item.to}
            className="group rounded-lg border border-app-border bg-panel-dark p-5 transition-colors hover:border-accent/60 hover:bg-app-hover"
          >
            <div className="flex items-start justify-between gap-4">
              <div className="flex min-w-0 gap-3">
                <span className="grid h-10 w-10 shrink-0 place-items-center rounded-lg bg-accent/10 text-accent">
                  <item.icon size={20} />
                </span>
                <div className="min-w-0">
                  <h2 className="text-base font-semibold text-app-heading">{item.title}</h2>
                  <p className="mt-1 text-sm text-app-muted">{item.description}</p>
                  <p className="mt-3 font-mono text-sm font-semibold text-app-heading">{item.value}</p>
                </div>
              </div>
              <ArrowRight className="shrink-0 text-app-muted transition-colors group-hover:text-accent" size={18} />
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
