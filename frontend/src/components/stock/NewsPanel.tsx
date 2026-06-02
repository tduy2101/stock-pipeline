import { ExternalLink } from 'lucide-react'
import { EmptyState } from '@/components/shared/EmptyState'
import { SentimentBadge } from '@/components/shared/SentimentBadge'
import { Skeleton } from '@/components/ui/skeleton'
import { useNews, useNewsArticles } from '@/hooks/useNews'
import type { NewsArticleRow } from '@/types'
import { formatDate, formatPrice } from '@/utils/formatters'

interface NewsPanelProps {
  symbol: string
}

const articleSnippet = (article: NewsArticleRow): string =>
  article.summary?.trim() ||
  article.body_text?.trim() ||
  'Chưa có tóm tắt nội dung. Mở bài gốc để xem chi tiết.'

export function NewsPanel({ symbol }: NewsPanelProps) {
  const daily = useNews(symbol, { page_size: 60 })
  const articles = useNewsArticles(symbol, { page_size: 100 })
  const dailyRows = daily.data?.data ?? []
  const articleRows = articles.data?.data ?? []

  if (daily.isLoading || articles.isLoading) return <Skeleton className="h-96" />

  if (dailyRows.length === 0 && articleRows.length === 0) {
    return (
      <EmptyState
        message="Không có tin tức cho ticker này"
        subMessage="Ticker có thể chỉ có giá, hồ sơ hoặc BCTC trong Gold layer"
      />
    )
  }

  return (
    <div className="grid gap-5">
      <section>
        <div className="mb-2 flex items-center justify-between gap-3">
          <h3 className="text-sm font-semibold text-app-heading">Sắc thái theo ngày</h3>
          <span className="text-xs text-app-muted">{dailyRows.length} ngày</span>
        </div>
        {dailyRows.length === 0 ? (
          <EmptyState message="Không có dữ liệu tổng hợp sắc thái" />
        ) : (
          <div className="overflow-hidden rounded-lg border border-app-border">
            <table className="w-full min-w-[640px] text-sm">
              <thead className="bg-app-table-head text-xs uppercase text-app-muted">
                <tr>
                  <th className="px-3 py-2 text-left">Ngày</th>
                  <th className="px-3 py-2 text-right">Số tin</th>
                  <th className="px-3 py-2 text-right">Điểm</th>
                  <th className="px-3 py-2 text-center">Sắc thái</th>
                  <th className="px-3 py-2 text-right">Tích cực</th>
                  <th className="px-3 py-2 text-right">Tiêu cực</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-app-border bg-card-dark">
                {dailyRows.slice(0, 12).map((row) => (
                  <tr key={row.published_date} className="hover:bg-app-hover">
                    <td className="px-3 py-2 text-app-text">{formatDate(row.published_date)}</td>
                    <td className="px-3 py-2 text-right font-mono text-app-heading">{row.news_count ?? 0}</td>
                    <td className="px-3 py-2 text-right font-mono text-app-text">{formatPrice(row.avg_sentiment_score)}</td>
                    <td className="px-3 py-2 text-center">
                      <SentimentBadge label={row.dominant_sentiment} />
                    </td>
                    <td className="px-3 py-2 text-right font-mono text-price-up">{row.positive_count ?? 0}</td>
                    <td className="px-3 py-2 text-right font-mono text-price-down">{row.negative_count ?? 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section>
        <div className="mb-2 flex items-center justify-between gap-3">
          <h3 className="text-sm font-semibold text-app-heading">Bài báo gần đây</h3>
          <span className="text-xs text-app-muted">{articles.data?.total ?? articleRows.length} bài</span>
        </div>
        {articleRows.length === 0 ? (
          <EmptyState
            message="Không có danh sách bài báo"
            subMessage="Aggregate vẫn có thể tồn tại nếu article mart chưa được build lại"
          />
        ) : (
          <div className="grid gap-3">
            {articleRows.map((article) => (
              <article
                key={article.article_id}
                className="rounded-lg border border-app-border bg-card-dark p-4 transition-colors hover:bg-app-hover"
              >
                <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                  <div className="min-w-0">
                    <div className="mb-2 flex flex-wrap items-center gap-2">
                      <span className="font-mono text-xs font-semibold text-accent">{article.ticker ?? symbol}</span>
                      <span className="text-xs text-app-muted">{article.source ?? 'Không rõ nguồn'}</span>
                      <span className="text-xs text-app-subtle">{formatDate(article.published_at ?? article.published_date)}</span>
                      <SentimentBadge label={article.sentiment_label} />
                    </div>
                    <h4 className="line-clamp-2 text-sm font-semibold leading-6 text-app-heading">
                      {article.title}
                    </h4>
                    <p className="mt-2 line-clamp-3 text-sm leading-6 text-app-text">
                      {articleSnippet(article)}
                    </p>
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
          </div>
        )}
      </section>
    </div>
  )
}
