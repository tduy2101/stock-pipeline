import { Link, useParams } from 'react-router-dom'
import { useState } from 'react'
import { BctcPanel } from '@/components/stock/BctcPanel'
import { FinancialTable } from '@/components/stock/FinancialTable'
import { ForeignFlowPanel } from '@/components/stock/ForeignFlowPanel'
import { IndicatorChart } from '@/components/stock/IndicatorChart'
import { NewsSignalBadge } from '@/components/stock/NewsSignalBadge'
import { NewsPanel } from '@/components/stock/NewsPanel'
import { PriceBoardTable } from '@/components/stock/PriceBoardTable'
import { PriceChart } from '@/components/stock/PriceChart'
import { PageWrapper } from '@/components/layout/PageWrapper'
import { EmptyState } from '@/components/shared/EmptyState'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useCompany } from '@/hooks/useCompany'
import {
  formatDate,
  formatPercentPoints,
  formatPrice,
  formatShares,
} from '@/utils/formatters'

export default function StockDetailPage() {
  const { symbol } = useParams<{ symbol: string }>()
  const ticker = (symbol ?? '').toUpperCase()
  const { data: company, isLoading } = useCompany(ticker)
  const [fromDate, setFromDate] = useState('')
  const [toDate, setToDate] = useState('')

  if (isLoading) {
    return (
      <PageWrapper>
        <Skeleton className="h-28" />
        <Skeleton className="h-96" />
      </PageWrapper>
    )
  }

  if (!company) {
    return (
      <PageWrapper className="items-center py-20 text-center">
        <EmptyState message={`Không tìm thấy mã ${ticker}`} subMessage="Thử tìm mã khác từ thanh tìm kiếm hoặc quay về tổng quan." />
        <Link to="/" className="text-sm font-medium text-accent hover:underline">
          Về tổng quan
        </Link>
      </PageWrapper>
    )
  }

  return (
    <PageWrapper>
      <nav className="text-xs text-app-muted">
        <Link to="/" className="hover:text-app-heading">Tổng quan</Link>
        <span className="mx-2">/</span>
        <span className="text-app-heading">{ticker}</span>
      </nav>

      {company.has_full_profile === false && (
        <section className="rounded-lg border border-amber-500/40 bg-amber-500/10 p-4">
          <p className="text-sm font-medium text-amber-200">Hồ sơ chưa đầy đủ</p>
          <p className="mt-1 text-xs text-app-muted">
            Mã có trong danh mục nhưng thiếu một số thông tin doanh nghiệp. Các tab khác vẫn hiển thị nếu có dữ liệu.
          </p>
        </section>
      )}

      <section className="rounded-lg border border-app-border bg-panel-dark p-5">
        <div className="flex flex-col justify-between gap-4 md:flex-row md:items-start">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-3">
              <h1 className="font-mono text-3xl font-bold text-app-heading">{ticker}</h1>
              <span className="rounded-md bg-app-hover px-2 py-1 text-xs text-app-text">{company.exchange ?? 'N/A'}</span>
              {company.sector && <span className="rounded-md bg-app-hover px-2 py-1 text-xs text-app-text">{company.sector}</span>}
              {[
                { label: 'Giá', active: company.has_price },
                { label: 'Tin tức', active: company.has_news },
                { label: 'BCTC', active: company.has_bctc },
                { label: 'Hồ sơ', active: company.has_full_profile },
              ]
                .filter((item) => item.active)
                .map((item) => (
                  <span key={item.label} className="rounded-md border border-app-border bg-app-hover px-2 py-1 text-xs text-app-muted">
                    {item.label}
                  </span>
                ))}
            </div>
            <p className="mt-2 max-w-3xl text-sm text-app-text">{company.organ_name ?? company.company_name ?? 'N/A'}</p>
          </div>
          <div className="md:text-right">
            <div className="flex flex-wrap items-center gap-3 md:justify-end">
              <p className="font-mono text-3xl font-bold text-app-heading">{formatPrice(company.latest_close)}</p>
              <NewsSignalBadge symbol={ticker} />
            </div>
            <p className="mt-1 text-xs text-app-muted">
              Giá đóng cửa · {formatDate(company.latest_trading_date)}
            </p>
          </div>
        </div>
      </section>

      <section className="rounded-lg border border-app-border bg-panel-dark p-4">
        <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            <h2 className="text-sm font-semibold text-app-heading">Khoảng ngày dữ liệu</h2>
            <p className="mt-1 text-xs text-app-muted">
              Áp dụng cho giá, chỉ báo, bảng giá, khối ngoại, tin theo phiên và BCTC theo ngày công bố.
            </p>
          </div>
          <div className="flex flex-wrap items-end gap-3">
            <label className="grid gap-1">
              <span className="text-[11px] text-app-muted">Từ ngày</span>
              <input
                type="date"
                value={fromDate}
                onChange={(event) => setFromDate(event.target.value)}
                className="h-9 rounded-md border border-app-border bg-app-input px-3 text-sm text-app-heading outline-none focus:border-accent"
              />
            </label>
            <label className="grid gap-1">
              <span className="text-[11px] text-app-muted">Đến ngày</span>
              <input
                type="date"
                value={toDate}
                onChange={(event) => setToDate(event.target.value)}
                className="h-9 rounded-md border border-app-border bg-app-input px-3 text-sm text-app-heading outline-none focus:border-accent"
              />
            </label>
            {(fromDate || toDate) && (
              <button
                type="button"
                onClick={() => {
                  setFromDate('')
                  setToDate('')
                }}
                className="h-9 rounded-md border border-app-border px-3 text-xs text-app-muted hover:text-app-heading"
              >
                Xóa lọc
              </button>
            )}
          </div>
        </div>
      </section>

      <Tabs defaultValue="overview">
        <TabsList className="flex-wrap">
          <TabsTrigger value="overview">Tổng quan</TabsTrigger>
          <TabsTrigger value="chart">Biểu đồ giá</TabsTrigger>
          <TabsTrigger value="indicators">Chỉ báo</TabsTrigger>
          <TabsTrigger value="board">Bảng giá</TabsTrigger>
          <TabsTrigger value="financials">Tài chính</TabsTrigger>
          <TabsTrigger value="news">Tin tức</TabsTrigger>
          <TabsTrigger value="bctc">BCTC</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-4 grid grid-cols-1 gap-5 lg:grid-cols-2">
          <div className="rounded-lg border border-app-border bg-card-dark p-5">
            <h2 className="mb-4 text-sm font-semibold text-app-heading">Hồ sơ công ty</h2>
            <dl className="grid gap-3">
              {[
                { label: 'Tên ngắn', value: company.short_name },
                { label: 'Ngành', value: company.industry },
                { label: 'Năm thành lập', value: company.established_year?.toString() },
                { label: 'Ngày niêm yết', value: formatDate(company.listed_date) },
                { label: 'Website', value: company.website },
              ].map((item) => (
                <div key={item.label} className="flex justify-between gap-4 border-b border-app-border pb-2 last:border-0">
                  <dt className="text-xs text-app-muted">{item.label}</dt>
                  <dd className="max-w-[65%] truncate text-right text-xs text-app-heading">{item.value ?? 'N/A'}</dd>
                </div>
              ))}
            </dl>
          </div>

          <div className="rounded-lg border border-app-border bg-card-dark p-5">
            <h2 className="mb-4 text-sm font-semibold text-app-heading">Chỉ số và đơn vị</h2>
            <dl className="grid gap-3">
              {[
                { label: 'Cao nhất 52 tuần', value: formatPrice(company.high_52w), note: 'giá / cổ phiếu' },
                { label: 'Thấp nhất 52 tuần', value: formatPrice(company.low_52w), note: 'giá / cổ phiếu' },
                { label: 'KLGD TB 20 phiên', value: formatShares(company.avg_volume_20d), note: 'cổ phiếu / phiên' },
                { label: 'P/E', value: formatPrice(company.pe_ratio), note: 'lần lợi nhuận' },
                { label: 'P/B', value: formatPrice(company.pb_ratio), note: 'lần giá trị sổ sách' },
                { label: 'EPS', value: formatPrice(company.eps), note: 'lợi nhuận / cổ phiếu' },
                { label: 'ROE', value: formatPercentPoints(company.roe, false), note: 'lợi nhuận trên vốn chủ' },
                { label: 'ROA', value: formatPercentPoints(company.roa, false), note: 'lợi nhuận trên tài sản' },
              ].map((item) => (
                <div key={item.label} className="flex justify-between gap-4 border-b border-app-border pb-2 last:border-0">
                  <dt>
                    <span className="block text-xs text-app-muted">{item.label}</span>
                    <span className="mt-0.5 block text-[11px] text-app-subtle">{item.note}</span>
                  </dt>
                  <dd className="font-mono text-xs text-app-heading">{item.value}</dd>
                </div>
              ))}
            </dl>
          </div>

          {company.description && (
            <div className="rounded-lg border border-app-border bg-card-dark p-5 lg:col-span-2">
              <h2 className="mb-2 text-sm font-semibold text-app-heading">Mô tả</h2>
              <p className="text-sm leading-6 text-app-text">{company.description}</p>
            </div>
          )}
        </TabsContent>

        <TabsContent value="chart" className="mt-4 rounded-lg border border-app-border bg-card-dark p-5">
          <h2 className="mb-4 text-sm font-semibold text-app-heading">Biểu đồ giá lịch sử</h2>
          <PriceChart symbol={ticker} fromDate={fromDate} toDate={toDate} />
        </TabsContent>

        <TabsContent value="indicators" className="mt-4 rounded-lg border border-app-border bg-card-dark p-5">
          <h2 className="mb-4 text-sm font-semibold text-app-heading">Chỉ báo kỹ thuật</h2>
          <IndicatorChart symbol={ticker} fromDate={fromDate} toDate={toDate} />
        </TabsContent>

        <TabsContent value="board" className="mt-4 grid gap-5">
          <section className="rounded-lg border border-app-border bg-card-dark p-5">
            <ForeignFlowPanel symbol={ticker} fromDate={fromDate} toDate={toDate} />
          </section>
          <section className="rounded-lg border border-app-border bg-card-dark p-5">
            <PriceBoardTable symbol={ticker} fromDate={fromDate} toDate={toDate} />
          </section>
        </TabsContent>

        <TabsContent value="financials" className="mt-4 rounded-lg border border-app-border bg-card-dark p-5">
          <h2 className="mb-4 text-sm font-semibold text-app-heading">Chỉ số tài chính</h2>
          <FinancialTable symbol={ticker} />
        </TabsContent>

        <TabsContent value="news" className="mt-4 rounded-lg border border-app-border bg-card-dark p-5">
          <h2 className="mb-4 text-sm font-semibold text-app-heading">Tin tức theo phiên giao dịch</h2>
          <NewsPanel symbol={ticker} fromDate={fromDate} toDate={toDate} />
        </TabsContent>

        <TabsContent value="bctc" className="mt-4 rounded-lg border border-app-border bg-card-dark p-5">
          <h2 className="mb-4 text-sm font-semibold text-app-heading">Kho tài liệu BCTC PDF</h2>
          <p className="mb-4 text-xs text-app-muted">
            Danh mục báo cáo tài chính đã crawl. Mở PDF để xem nội dung gốc — hệ thống chưa trích xuất số liệu từ file.
          </p>
          <BctcPanel symbol={ticker} fromDate={fromDate} toDate={toDate} />
        </TabsContent>
      </Tabs>
    </PageWrapper>
  )
}
