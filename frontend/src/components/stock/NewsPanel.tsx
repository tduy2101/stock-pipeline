import { ExternalLink } from 'lucide-react'
import { EmptyState } from '@/components/shared/EmptyState'
import { InfoTooltip } from '@/components/shared/InfoTooltip'
import { SentimentBadge } from '@/components/shared/SentimentBadge'
import { Skeleton } from '@/components/ui/skeleton'
import { useNewsSignal } from '@/hooks/useNews'
import type { NewsSignalRow, TopArticle } from '@/types'
import { formatDate, formatNewsPublishDate } from '@/utils/formatters'

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
    <div
      className="flex h-2 w-full overflow-hidden rounded-full bg-app-hover"
      role="img"
      aria-label={`Sắc thái: ${row.positive_count ?? 0} tích cực, ${row.neutral_count ?? 0} trung lập, ${row.negative_count ?? 0} tiêu cực`}
    >
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
      className="block rounded-lg border border-app-border bg-app-hover p-3 transition-colors hover:border-accent/50 hover:bg-app-input focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
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
            {article.published_at && (
              <span>{formatNewsPublishDate(article.published_at, null)}</span>
            )}
          </div>
        </div>
        <ExternalLink className="mt-1 shrink-0 text-app-muted" size={14} aria-hidden />
      </div>
    </a>
  )
}

const signalLabel = (signal: NewsSignalRow['news_signal']): string | null => {
  if (signal === 'buy_signal') return 'Xu hướng tích cực'
  if (signal === 'sell_signal') return 'Xu hướng tiêu cực'
  return null
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
        message="Không có tin tức cho mã này"
        subMessage="Mã có thể chưa được nhắc trong các bài viết đã thu thập."
      />
    )
  }

  return (
    <div className="grid gap-4">
      <p className="flex items-start gap-2 rounded-lg border border-app-border bg-app-hover/60 px-3 py-2 text-xs leading-5 text-app-muted">
        <InfoTooltip
          text="Sắc thái được xác định bằng phương pháp đếm từ khóa và chỉ mang tính tham khảo, không phải khuyến nghị đầu tư."
          label="Giải thích sắc thái tin tức"
        />
        <span>
          Tổng hợp tin theo phiên giao dịch. Sắc thái dựa trên từ khóa — chỉ mang tính tham khảo,
          không phải khuyến nghị mua/bán.
        </span>
      </p>

      {rows.map((row) => (
        <section
          key={row.trading_date}
          className="overflow-hidden rounded-lg border border-app-border bg-card-dark"
        >
          <div className="flex flex-col gap-3 border-b border-app-border bg-app-hover px-4 py-3 md:flex-row md:items-center md:justify-between">
            <div className="flex flex-wrap items-center gap-3">
              <span className="text-sm font-semibold text-app-heading">
                Phiên {formatDate(row.trading_date)}
              </span>
              <SentimentBadge label={row.dominant_sentiment} />
              {signalLabel(row.news_signal) && (
                <span
                  className={`rounded-full px-2 py-0.5 text-xs font-medium ${
                    row.news_signal === 'buy_signal'
                      ? 'bg-price-up/10 text-price-up'
                      : 'bg-price-down/10 text-price-down'
                  }`}
                >
                  {signalLabel(row.news_signal)}
                </span>
              )}
            </div>
            <div className="text-xs text-app-muted">
              <span>{row.news_count} bài liên quan</span>
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
