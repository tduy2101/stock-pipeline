import { ExternalLink, Newspaper, Search } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { PageWrapper } from '@/components/layout/PageWrapper'
import { EmptyState } from '@/components/shared/EmptyState'
import { SentimentBadge } from '@/components/shared/SentimentBadge'
import { Skeleton } from '@/components/ui/skeleton'
import { useAllNewsArticles } from '@/hooks/useNews'
import type { NewsArticleRow } from '@/types'
import { formatDate, formatPrice, formatVolume } from '@/utils/formatters'

const PAGE_SIZE = 25

const snippet = (article: NewsArticleRow): string =>
  article.summary?.trim() ||
  article.body_text?.trim() ||
  'Chưa có tóm tắt nội dung. Mở bài gốc để đọc chi tiết.'

export default function NewsArchivePage() {
  const [page, setPage] = useState(1)
  const [query, setQuery] = useState('')
  const [ticker, setTicker] = useState('')
  const [sentiment, setSentiment] = useState('')

  const params = useMemo(
    () => ({
      page,
      page_size: PAGE_SIZE,
      q: query.trim() || undefined,
      ticker: ticker.trim().toUpperCase() || undefined,
      sentiment: sentiment || undefined,
    }),
    [page, query, sentiment, ticker],
  )

  const { data, isLoading } = useAllNewsArticles(params)
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
              <Newspaper size={20} />
              <span className="text-xs font-semibold uppercase">Kho tin tức</span>
            </div>
            <h1 className="mt-2 text-2xl font-bold text-app-heading">Kho tin tức đã crawl</h1>
            <p className="mt-1 text-sm text-app-muted">
              Tất cả bài viết trong Gold `fact_news_article`, giữ tiêu đề, tóm tắt, nguồn, sắc thái và link bài gốc.
            </p>
          </div>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
            <div className="rounded-lg border border-app-border bg-card-dark p-3">
              <p className="text-xs text-app-muted">Tổng bài phù hợp</p>
              <p className="mt-1 font-mono text-lg font-bold text-app-heading">{formatVolume(data?.total)}</p>
            </div>
            <div className="rounded-lg border border-app-border bg-card-dark p-3">
              <p className="text-xs text-app-muted">Mỗi trang</p>
              <p className="mt-1 font-mono text-lg font-bold text-app-heading">{PAGE_SIZE} bài</p>
            </div>
            <div className="rounded-lg border border-app-border bg-card-dark p-3">
              <p className="text-xs text-app-muted">Trang hiện tại</p>
              <p className="mt-1 font-mono text-lg font-bold text-app-heading">{page}</p>
            </div>
          </div>
        </div>
      </section>

      <section className="rounded-lg border border-app-border bg-panel-dark p-4">
        <div className="grid gap-3 lg:grid-cols-[1fr_10rem_11rem]">
          <label className="relative">
            <Search className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-app-muted" size={16} />
            <input
              value={query}
              onChange={(event) => updateFilter(setQuery, event.target.value)}
              placeholder="Tìm theo tiêu đề, tóm tắt, nội dung"
              className="h-10 w-full rounded-lg border border-app-border bg-app-input pl-9 pr-3 text-sm text-app-heading outline-none focus:border-accent"
            />
          </label>
          <input
            value={ticker}
            onChange={(event) => updateFilter(setTicker, event.target.value)}
            placeholder="Ticker"
            className="h-10 rounded-lg border border-app-border bg-app-input px-3 text-sm font-mono text-app-heading outline-none focus:border-accent"
          />
          <select
            value={sentiment}
            onChange={(event) => updateFilter(setSentiment, event.target.value)}
            className="h-10 rounded-lg border border-app-border bg-app-input px-3 text-sm text-app-heading outline-none focus:border-accent"
          >
            <option value="">Tất cả sắc thái</option>
            <option value="positive">Tích cực</option>
            <option value="neutral">Trung lập</option>
            <option value="negative">Tiêu cực</option>
          </select>
        </div>
      </section>

      {isLoading ? (
        <Skeleton className="h-[32rem]" />
      ) : rows.length === 0 ? (
        <EmptyState message="Không tìm thấy tin tức" subMessage="Thử bỏ bớt bộ lọc hoặc tìm ticker khác" />
      ) : (
        <section className="grid gap-3">
          {rows.map((article) => (
            <article key={article.article_id} className="rounded-lg border border-app-border bg-card-dark p-4">
              <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                <div className="min-w-0">
                  <div className="mb-2 flex flex-wrap items-center gap-2">
                    {article.ticker ? (
                      <Link to={`/stock/${article.ticker}`} className="font-mono text-xs font-semibold text-accent hover:underline">
                        {article.ticker}
                      </Link>
                    ) : (
                      <span className="font-mono text-xs font-semibold text-app-muted">THỊ TRƯỜNG</span>
                    )}
                    <span className="text-xs text-app-muted">{article.source ?? 'Không rõ nguồn'}</span>
                    <span className="text-xs text-app-subtle">{formatDate(article.published_at ?? article.published_date)}</span>
                    <SentimentBadge label={article.sentiment_label} />
                    <span className="text-xs text-app-muted">Điểm {formatPrice(article.sentiment_score)}</span>
                  </div>
                  <h2 className="line-clamp-2 text-base font-semibold leading-6 text-app-heading">
                    {article.title}
                  </h2>
                  <p className="mt-2 line-clamp-3 text-sm leading-6 text-app-text">{snippet(article)}</p>
                  {!!article.ticker_mentions?.length && (
                    <p className="mt-2 text-xs text-app-muted">
                      Mã được nhắc tới: {article.ticker_mentions.slice(0, 8).join(', ')}
                    </p>
                  )}
                </div>
                {article.url && (
                  <a
                    href={article.url}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex h-9 shrink-0 items-center justify-center gap-2 rounded-md border border-accent/50 px-3 text-xs font-medium text-accent transition-colors hover:bg-accent hover:text-white"
                  >
                    <ExternalLink size={14} />
                    Xem bài gốc
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
          {formatVolume(rows.length)} / {formatVolume(data?.total)} bài
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
