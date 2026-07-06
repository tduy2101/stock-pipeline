import { ExternalLink, Newspaper } from 'lucide-react'
import { Link } from 'react-router-dom'
import { EmptyState } from '@/components/shared/EmptyState'
import { SentimentBadge } from '@/components/shared/SentimentBadge'
import { Skeleton } from '@/components/ui/skeleton'
import { useMarketNews } from '@/hooks/useNews'
import { formatNewsPublishDate } from '@/utils/formatters'

export function MarketNewsFeed() {
  const { data, isLoading } = useMarketNews(3)

  if (isLoading) return <Skeleton className="h-96" />
  if (!data?.length) return <EmptyState message="Không có tin tức thị trường" />

  return (
    <section className="rounded-lg border border-app-border bg-panel-dark p-5">
      <div className="mb-4 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <Newspaper className="text-accent" size={18} />
          <h2 className="text-sm font-semibold text-app-heading">Tin tức thị trường mới nhất</h2>
        </div>
        <Link to="/news" className="text-xs font-medium text-accent hover:underline">
          Xem tất cả
        </Link>
      </div>
      <div className="grid gap-3">
        {data.map((article) => (
          <article key={article.article_id} className="rounded-lg border border-app-border bg-card-dark p-3">
            <div className="mb-2 flex flex-wrap items-center gap-2">
              {article.ticker ? (
                <Link to={`/stock/${article.ticker}`} className="font-mono text-xs font-semibold text-accent hover:underline">
                  {article.ticker}
                </Link>
              ) : (
                <span className="font-mono text-xs font-semibold text-app-muted">THỊ TRƯỜNG</span>
              )}
              <span className="text-xs text-app-muted">{article.source ?? 'Không rõ nguồn'}</span>
              <span className="text-xs text-app-subtle">{formatNewsPublishDate(article.published_at, article.published_date)}</span>
              <SentimentBadge label={article.sentiment_label} />
            </div>
            <h3 className="line-clamp-2 text-sm font-semibold leading-6 text-app-heading">
              {article.title}
            </h3>
            {(article.summary || article.body_text) && (
              <p className="mt-2 line-clamp-2 text-xs leading-5 text-app-text">
                {article.summary ?? article.body_text}
              </p>
            )}
            {article.url && (
              <a
                href={article.url}
                target="_blank"
                rel="noreferrer"
                className="mt-3 inline-flex items-center gap-1.5 text-xs font-medium text-accent hover:underline"
              >
                <ExternalLink size={13} />
                Xem bài gốc
              </a>
            )}
          </article>
        ))}
      </div>
    </section>
  )
}
