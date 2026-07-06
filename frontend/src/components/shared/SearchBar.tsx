import { Search } from 'lucide-react'
import { useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useTickers } from '@/hooks/useTickers'
import type { TickerItem } from '@/types'
import { WATCHLIST_TICKERS } from '@/utils/watchlistTickers'

const emptyTickerItem = (ticker: string): TickerItem => ({
  ticker,
  exchange: null,
  organ_name: null,
  has_full_profile: false,
  has_price: false,
  has_news: false,
  has_bctc: false,
  news_count: 0,
  bctc_doc_count: 0,
})

const flagBadges = (item: TickerItem): string[] => {
  const badges: string[] = []
  if (item.has_price) badges.push('Giá')
  if (item.has_news) badges.push('Tin tức')
  if (item.has_bctc) badges.push('BCTC')
  if (item.has_full_profile) badges.push('Hồ sơ')
  return badges
}

export function SearchBar() {
  const navigate = useNavigate()
  const { data } = useTickers()
  const [query, setQuery] = useState('')
  const [focused, setFocused] = useState(false)

  const suggestions = useMemo(() => {
    const all = data?.tickers ?? []
    const byTicker = new Map(all.map((item) => [item.ticker, item]))
    const text = query.trim().toUpperCase()
    if (!text) {
      return WATCHLIST_TICKERS.map((ticker) => byTicker.get(ticker) ?? emptyTickerItem(ticker))
    }
    return all
      .filter((item) => {
        const name = item.organ_name?.toUpperCase() ?? ''
        return item.ticker.toUpperCase().includes(text) || name.includes(text)
      })
      .slice(0, 12)
  }, [data?.tickers, query])

  const goToTicker = (ticker: string) => {
    setQuery('')
    setFocused(false)
    navigate(`/stock/${ticker}`)
  }

  return (
    <div className="relative w-full max-w-xl">
      <Search className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-app-muted" size={17} />
      <input
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        onFocus={() => setFocused(true)}
        onBlur={() => window.setTimeout(() => setFocused(false), 120)}
        onKeyDown={(event) => {
          if (event.key === 'Enter' && suggestions[0]) goToTicker(suggestions[0].ticker)
        }}
        placeholder="Tìm ticker hoặc công ty"
        className="h-10 w-full rounded-lg border border-app-border bg-app-input pl-10 pr-3 text-sm text-app-heading outline-none transition placeholder:text-app-muted focus:border-accent"
      />
      {focused && suggestions.length > 0 && (
        <div className="absolute z-30 mt-2 max-h-80 w-full overflow-auto rounded-lg border border-app-border bg-card-dark shadow-xl scrollbar-thin">
          {suggestions.map((item) => (
            <button
              key={item.ticker}
              type="button"
              onMouseDown={() => goToTicker(item.ticker)}
              className="flex w-full items-center justify-between gap-4 px-3 py-2 text-left hover:bg-app-hover"
            >
              <span className="flex min-w-0 flex-col gap-1">
                <span className="flex items-center gap-2">
                  <span className="font-mono text-sm font-semibold text-app-heading">{item.ticker}</span>
                  <span className="truncate text-xs text-app-muted">{item.exchange ?? 'N/A'}</span>
                </span>
                <span className="truncate text-xs text-app-muted">{item.organ_name ?? 'Chưa có tên công ty'}</span>
              </span>
              <span className="flex shrink-0 flex-wrap justify-end gap-1">
                {flagBadges(item).slice(0, 3).map((badge) => (
                  <span
                    key={badge}
                    className="rounded border border-app-border bg-app-hover px-1.5 py-0.5 text-[10px] font-medium text-app-muted"
                  >
                    {badge}
                  </span>
                ))}
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
