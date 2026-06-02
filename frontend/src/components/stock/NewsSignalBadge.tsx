import { useNewsSignalSummary } from '@/hooks/useNews'

interface NewsSignalBadgeProps {
  symbol: string
}

const signalConfig = {
  buy_signal: {
    label: 'Tin tích cực',
    className: 'border-price-up/40 bg-price-up/10 text-price-up',
  },
  sell_signal: {
    label: 'Tin tiêu cực',
    className: 'border-price-down/40 bg-price-down/10 text-price-down',
  },
  neutral: {
    label: 'Tin trung lập',
    className: 'border-app-border bg-app-hover text-app-muted',
  },
}

export function NewsSignalBadge({ symbol }: NewsSignalBadgeProps) {
  const { data, isLoading } = useNewsSignalSummary(symbol)

  if (isLoading || !data) return null

  const config = signalConfig[data.news_signal ?? 'neutral']
  const articles = data.top_articles ?? []

  return (
    <div className="group relative inline-flex">
      <span
        className={`inline-flex h-8 items-center gap-1.5 rounded-full border px-3 text-xs font-medium ${config.className}`}
      >
        <span className="h-1.5 w-1.5 rounded-full bg-current" />
        {config.label}
        {data.news_count != null && (
          <span className="opacity-75">({data.news_count} tin)</span>
        )}
      </span>

      {articles.length > 0 && (
        <div className="pointer-events-none absolute right-0 top-full z-50 mt-2 w-80 rounded-lg border border-app-border bg-panel-dark p-3 opacity-0 shadow-xl transition-opacity group-hover:pointer-events-auto group-hover:opacity-100">
          <p className="mb-2 text-xs font-medium text-app-muted">Tin gần nhất</p>
          <ul className="grid gap-2">
            {articles.map((article) => (
              <li key={article.article_id}>
                <a
                  href={article.url}
                  target="_blank"
                  rel="noreferrer"
                  className="line-clamp-2 text-xs leading-5 text-app-heading hover:text-accent"
                >
                  {article.title}
                </a>
                <div className="mt-1 flex items-center gap-2 text-[11px] text-app-muted">
                  <span
                    className={
                      article.sentiment === 'positive'
                        ? 'text-price-up'
                        : article.sentiment === 'negative'
                          ? 'text-price-down'
                          : 'text-app-muted'
                    }
                  >
                    {article.sentiment ?? 'neutral'}
                  </span>
                  {article.relevance === 'title' && (
                    <span className="text-accent">nhắc trong tiêu đề</span>
                  )}
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}
