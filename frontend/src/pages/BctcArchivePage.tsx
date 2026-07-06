import { ExternalLink, FileText, Search } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { getBctcFileUrl } from '@/api/bctc'
import { PageWrapper } from '@/components/layout/PageWrapper'
import { EmptyState } from '@/components/shared/EmptyState'
import { Skeleton } from '@/components/ui/skeleton'
import { useAllBctcDocuments } from '@/hooks/useBctc'
import { formatBytes, formatDate, formatVolume } from '@/utils/formatters'
import { Calendar } from 'lucide-react';
const PAGE_SIZE = 25

export default function BctcArchivePage() {
  const [page, setPage] = useState(1)
  const [query, setQuery] = useState('')
  const [ticker, setTicker] = useState('')
  const [year, setYear] = useState('')
  const [fromDate, setFromDate] = useState('')
  const [toDate, setToDate] = useState('')

  const params = useMemo(
    () => ({
      page,
      page_size: PAGE_SIZE,
      q: query.trim() || undefined,
      ticker: ticker.trim().toUpperCase() || undefined,
      year: year ? Number(year) : undefined,
      from: fromDate || undefined,
      to: toDate || undefined,
    }),
    [fromDate, page, query, ticker, toDate, year],
  )

  const { data, isLoading } = useAllBctcDocuments(params)
  const rows = data?.data ?? []

  const updateFilter = (setter: (value: string) => void, value: string) => {
    setter(value)
    setPage(1)
  }

  return (
    <PageWrapper>
      <section className="rounded-lg border border-app-border bg-panel-dark p-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="flex items-center gap-2 text-accent">
              <FileText size={20} />
              <span className="text-xs font-semibold uppercase">Kho PDF</span>
            </div>
            <h1 className="mt-2 text-2xl font-bold text-app-heading">Kho BCTC PDF</h1>
            <p className="mt-1 text-sm text-app-muted">
              Kho tài liệu báo cáo tài chính đã crawl — tra cứu metadata và mở file PDF. Hệ thống chưa trích xuất số liệu từ PDF.
            </p>
          </div>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
            <div className="rounded-lg border border-app-border bg-card-dark p-3">
              <p className="text-xs text-app-muted">Tổng tài liệu</p>
              <p className="mt-1 font-mono text-lg font-bold text-app-heading">{formatVolume(data?.total)}</p>
            </div>
            <div className="rounded-lg border border-app-border bg-card-dark p-3">
              <p className="text-xs text-app-muted">Mỗi trang</p>
              <p className="mt-1 font-mono text-lg font-bold text-app-heading">{PAGE_SIZE} PDF</p>
            </div>
            <div className="rounded-lg border border-app-border bg-card-dark p-3">
              <p className="text-xs text-app-muted">Trang hiện tại</p>
              <p className="mt-1 font-mono text-lg font-bold text-app-heading">{page}</p>
            </div>
          </div>
        </div>
      </section>

      <section className="rounded-lg border border-app-border bg-panel-dark p-4">
        {/* Thêm items-end để các ô input không có label phụ luôn thẳng hàng ở đáy với 2 ô Date */}
        <div className="grid gap-3 lg:grid-cols-[1fr_10rem_9rem_10rem_10rem] items-end">

          {/* Ô Tìm kiếm - Giữ nguyên placeholder gốc */}
          <div className="relative w-full">
            <Search className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-app-muted" size={16} />
            <input
              value={query}
              onChange={(event) => updateFilter(setQuery, event.target.value)}
              placeholder="Tìm theo tiêu đề tài liệu"
              className="h-10 w-full rounded-lg border border-app-border bg-app-input pl-9 pr-3 text-sm text-app-heading outline-none focus:border-accent"
            />
          </div>

          {/* Ô Ticker - Giữ nguyên */}
          <input
            value={ticker}
            onChange={(event) => updateFilter(setTicker, event.target.value)}
            placeholder="Ticker"
            className="h-10 rounded-lg border border-app-border bg-app-input px-3 text-sm font-mono text-app-heading outline-none focus:border-accent"
          />

          {/* Ô Năm - Giữ nguyên */}
          <input
            value={year}
            onChange={(event) => updateFilter(setYear, event.target.value)}
            placeholder="Năm"
            inputMode="numeric"
            className="h-10 rounded-lg border border-app-border bg-app-input px-3 text-sm font-mono text-app-heading outline-none focus:border-accent"
          />

          {/* Ô Từ ngày công bố - Cập nhật giao diện đồng bộ */}
          <label className="grid gap-1 relative group cursor-pointer">
            <span className="text-[11px] font-medium text-app-muted">Từ ngày công bố</span>
            <div className="relative flex items-center">
              <input
                type="date"
                value={fromDate}
                onChange={(event) => updateFilter(setFromDate, event.target.value)}
                className="h-10 w-full rounded-lg border border-app-border bg-app-input pl-3 pr-10 text-sm text-app-heading outline-none focus:border-accent [color-scheme:dark] 
          [&::-webkit-calendar-picker-indicator]:absolute [&::-webkit-calendar-picker-indicator]:inset-0 [&::-webkit-calendar-picker-indicator]:w-full [&::-webkit-calendar-picker-indicator]:h-full [&::-webkit-calendar-picker-indicator]:opacity-0 [&::-webkit-calendar-picker-indicator]:cursor-pointer
          [&::-webkit-datetime-edit]:opacity-0 focus:[&::-webkit-datetime-edit]:opacity-100 data-[has-value=true]:[&::-webkit-datetime-edit]:opacity-100"
                data-has-value={!!fromDate}
              />
              {!fromDate && (
                <span className="absolute left-3 text-sm text-app-muted pointer-events-none group-focus-within:hidden">
                  dd/mm/yyyy
                </span>
              )}
              <Calendar
                size={16}
                className="absolute right-3 text-app-muted pointer-events-none group-focus-within:text-accent transition-colors"
              />
            </div>
          </label>

          {/* Ô Đến ngày công bố - Cập nhật giao diện đồng bộ */}
          <label className="grid gap-1 relative group cursor-pointer">
            <span className="text-[11px] font-medium text-app-muted">Đến ngày công bố</span>
            <div className="relative flex items-center">
              <input
                type="date"
                value={toDate}
                onChange={(event) => updateFilter(setToDate, event.target.value)}
                className="h-10 w-full rounded-lg border border-app-border bg-app-input pl-3 pr-10 text-sm text-app-heading outline-none focus:border-accent [color-scheme:dark] 
          [&::-webkit-calendar-picker-indicator]:absolute [&::-webkit-calendar-picker-indicator]:inset-0 [&::-webkit-calendar-picker-indicator]:w-full [&::-webkit-calendar-picker-indicator]:h-full [&::-webkit-calendar-picker-indicator]:opacity-0 [&::-webkit-calendar-picker-indicator]:cursor-pointer
          [&::-webkit-datetime-edit]:opacity-0 focus:[&::-webkit-datetime-edit]:opacity-100 data-[has-value=true]:[&::-webkit-datetime-edit]:opacity-100"
                data-has-value={!!toDate}
              />
              {!toDate && (
                <span className="absolute left-3 text-sm text-app-muted pointer-events-none group-focus-within:hidden">
                  dd/mm/yyyy
                </span>
              )}
              <Calendar
                size={16}
                className="absolute right-3 text-app-muted pointer-events-none group-focus-within:text-accent transition-colors"
              />
            </div>
          </label>

        </div>
      </section>

      {isLoading ? (
        <Skeleton className="h-[32rem]" />
      ) : rows.length === 0 ? (
        <EmptyState message="Không tìm thấy BCTC PDF" subMessage="Thử bỏ bớt bộ lọc hoặc tìm ticker khác" />
      ) : (
        <section className="grid gap-3">
          {rows.map((doc) => (
            <article key={doc.doc_id} className="rounded-lg border border-app-border bg-card-dark p-4">
              <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                <div className="min-w-0">
                  <div className="mb-2 flex flex-wrap items-center gap-2">
                    <Link to={`/stock/${doc.ticker}`} className="font-mono text-xs font-semibold text-accent hover:underline">
                      {doc.ticker}
                    </Link>
                    <span className="text-xs text-app-muted">{doc.year ?? 'N/A'} / {doc.period_key ?? 'N/A'}</span>
                    <span className="text-xs text-app-subtle">{formatDate(doc.published_at)}</span>
                    <span className="rounded border border-app-border bg-app-hover px-2 py-0.5 text-xs text-app-muted">
                      {doc.doc_class ?? 'tài liệu'}
                    </span>
                  </div>
                  <h2 className="line-clamp-2 text-base font-semibold leading-6 text-app-heading">
                    {doc.title ?? 'Báo cáo tài chính'}
                  </h2>
                  <p className="mt-2 text-sm text-app-muted">
                    Dung lượng: {formatBytes(doc.file_size)} / Trạng thái: {doc.display_status ?? 'N/A'}
                  </p>
                </div>
                {doc.is_available_for_web && (
                  <a
                    href={getBctcFileUrl(doc.ticker, doc.doc_id)}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex h-9 shrink-0 items-center justify-center gap-2 rounded-md border border-accent/50 px-3 text-xs font-medium text-accent transition-colors hover:bg-accent hover:text-white"
                  >
                    <ExternalLink size={14} />
                    Xem PDF
                  </a>
                )}
              </div>
            </article>
          ))}
        </section>
      )}

      <div className="flex items-center justify-between rounded-lg border border-app-border bg-panel-dark p-3">
        <button
          type="button"
          onClick={() => setPage((current) => Math.max(1, current - 1))}
          disabled={page <= 1}
          className="h-9 rounded-md border border-app-border px-3 text-sm text-app-heading disabled:cursor-not-allowed disabled:opacity-40"
        >
          Trang trước
        </button>
        <span className="text-xs text-app-muted">
          {formatVolume(rows.length)} / {formatVolume(data?.total)} tài liệu
        </span>
        <button
          type="button"
          onClick={() => setPage((current) => current + 1)}
          disabled={!data?.has_more}
          className="h-9 rounded-md border border-app-border px-3 text-sm text-app-heading disabled:cursor-not-allowed disabled:opacity-40"
        >
          Trang sau
        </button>
      </div>
    </PageWrapper>
  )
}
