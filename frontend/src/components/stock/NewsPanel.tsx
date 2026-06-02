import { ExternalLink } from 'lucide-react'
import { EmptyState } from '@/components/shared/EmptyState'
import { SentimentBadge } from '@/components/shared/SentimentBadge'
import { Skeleton } from '@/components/ui/skeleton'
import { useNewsSignal } from '@/hooks/useNews'
import type { NewsSignalRow, TopArticle } from '@/types'
import { formatDate, formatPrice } from '@/utils/formatters'

interface NewsPanelProps {
  symbol: string
  fromDate?: string
  toDate?: string
}

function SentimentBar({ row }: { row: NewsSignalRow }) {
  const total = row.news_count || 1
  const positiveWidth = ((row.positive_count ?? 0) / total) * 100
  const negativeWidth = ((row.negative_count ?? 0) / total) * 100
  const neutralWidth = Math.max(0, 100 - positiveWidth - negativeWidth)

  return (
    <div className="flex h-1.5 w-full overflow-hidden rounded-full bg-app-hover">
      <div className="bg-price-up" style={{ width: `${positiveWidth}%` }} />
      <div className="bg-app-muted" style={{ width: `${neutralWidth}%` }} />
      <div className="bg-price-down" style={{ width: `${negativeWidth}%` }} />
    </div>
  )
}

function ArticleCard({ article }: { article: TopArticle }) {
  return (
    <a
      href={article.url}
      target="_blank"
      rel="noreferrer"
      className="block rounded-lg border border-app-border bg-app-hover p-3 transition-colors hover:border-accent/50 hover:bg-app-input"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="line-clamp-2 text-sm font-medium leading-6 text-app-heading">
            {article.title}
          </p>
          <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-app-muted">
            <SentimentBadge label={article.sentiment ?? null} />
            {article.relevance === 'title' && (
              <span className="text-accent">nhắc trong tiêu đề</span>
            )}
            {article.published_at && <span>{formatDate(article.published_at)}</span>}
          </div>
        </div>
        <ExternalLink className="mt-1 shrink-0 text-app-muted" size={14} />
      </div>
    </a>
  )
}

export function NewsPanel({ symbol, fromDate, toDate }: NewsPanelProps) {
  const { data, isLoading } = useNewsSignal(symbol, {
    page_size: 30,
    from: fromDate || undefined,
    to: toDate || undefined,
  })
  const rows = data?.data ?? []

  if (isLoading) return <Skeleton className="h-96" />

  if (rows.length === 0) {
    return (
      <EmptyState
        message="Không có tin tức cho ticker này"
        subMessage="Ticker có thể chỉ có giá, hồ sơ hoặc BCTC trong Gold layer"
      />
    )
  }

  return (
    <div className="grid gap-4">
      {rows.map((row) => (
        <section
          key={row.trading_date}
          className="overflow-hidden rounded-lg border border-app-border bg-card-dark"
        >
          <div className="flex flex-col gap-3 border-b border-app-border bg-app-hover px-4 py-3 md:flex-row md:items-center md:justify-between">
            <div className="flex flex-wrap items-center gap-3">
              <span className="text-sm font-semibold text-app-heading">
                {formatDate(row.trading_date)}
              </span>
              <SentimentBadge label={row.dominant_sentiment} />
              {row.news_signal && row.news_signal !== 'neutral' && (
                <span
                  className={`rounded-full px-2 py-0.5 text-xs font-medium ${
                    row.news_signal === 'buy_signal'
                      ? 'bg-price-up/10 text-price-up'
                      : 'bg-price-down/10 text-price-down'
                  }`}
                >
                  {row.news_signal === 'buy_signal' ? 'Tích cực' : 'Tiêu cực'}
                </span>
              )}
            </div>
            <div className="flex flex-wrap items-center gap-3 text-xs text-app-muted">
              <span>{row.news_count} tin</span>
              <span className="font-mono">
                avg {formatPrice(row.avg_sentiment_score)}
              </span>
              <span
                className={`font-mono ${
                  (row.weighted_sentiment ?? 0) > 0
                    ? 'text-price-up'
                    : (row.weighted_sentiment ?? 0) < 0
                      ? 'text-price-down'
                      : 'text-app-muted'
                }`}
              >
                weighted {formatPrice(row.weighted_sentiment)}
              </span>
            </div>
          </div>

          <div className="grid gap-3 p-4">
            <SentimentBar row={row} />
            {row.top_articles?.length ? (
              <div className="grid gap-2">
                {row.top_articles.map((article) => (
                  <ArticleCard key={article.article_id} article={article} />
                ))}
              </div>
            ) : (
              <p className="text-sm text-app-muted">Không có bài nổi bật trong phiên này</p>
            )}
          </div>
        </section>
      ))}
    </div>
  )
}
