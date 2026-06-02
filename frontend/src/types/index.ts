export interface TickerItem {
  ticker: string
  exchange: string | null
  organ_name: string | null
  has_full_profile: boolean
  has_price: boolean
  has_news: boolean
  has_bctc: boolean
  news_count: number
  bctc_doc_count: number
}

export interface TickerListResponse {
  tickers: TickerItem[]
  total: number
}

export interface TopMover {
  ticker: string
  close: number
  percent_change: number
}

export interface MarketOverviewResponse {
  trading_date: string
  vnindex_close: number | null
  vnindex_return: number | null
  vn30_close: number | null
  vn30_return: number | null
  hnx_close: number | null
  hnx_return: number | null
  total_volume: number | null
  total_value: number | null
  advances: number | null
  declines: number | null
  unchanged: number | null
  top_gainers: TopMover[] | null
  top_losers: TopMover[] | null
}

export interface CompanyProfileResponse {
  ticker: string
  symbol: string | null
  exchange: string | null
  company_name: string | null
  short_name: string | null
  industry: string | null
  sector: string | null
  charter_capital: number | null
  established_year: number | null
  listed_date: string | null
  website: string | null
  description: string | null
  organ_name: string | null
  en_organ_name: string | null
  latest_close: number | null
  latest_trading_date: string | null
  high_52w: number | null
  low_52w: number | null
  avg_volume_20d: number | null
  pe_ratio: number | null
  pb_ratio: number | null
  eps: number | null
  roe: number | null
  roa: number | null
  has_full_profile: boolean
  has_price: boolean
  has_news: boolean
  has_bctc: boolean
}

export interface PriceRow {
  ticker: string
  trading_date: string
  open: number | null
  high: number | null
  low: number | null
  close: number | null
  volume: number | null
  value: number | null
  daily_return: number | null
}

export interface IndicatorRow {
  ticker: string
  trading_date: string
  close: number | null
  ma7: number | null
  ma20: number | null
  ma50: number | null
  rsi14: number | null
  macd_line: number | null
  macd_signal: number | null
  macd_hist: number | null
  bb_upper: number | null
  bb_middle: number | null
  bb_lower: number | null
  volatility_20d: number | null
}

export interface FinancialRatioRow {
  ticker: string
  period: string
  period_type: string | null
  year: number | null
  quarter: number | null
  item_code: string
  item_name: string | null
  value: number | null
}

export type SentimentLabel = 'positive' | 'neutral' | 'negative'

export interface NewsDailyRow {
  ticker: string
  published_date: string
  news_count: number | null
  avg_sentiment_score: number | null
  positive_count: number | null
  negative_count: number | null
  neutral_count: number | null
  dominant_sentiment: SentimentLabel | null
}

export interface NewsArticleRow {
  article_id: string
  ticker: string | null
  ticker_mentions: string[] | null
  title: string
  summary: string | null
  body_text: string | null
  url: string | null
  source: string | null
  published_at: string | null
  published_date: string | null
  sentiment_score: number | null
  sentiment_label: SentimentLabel | null
  word_count: number | null
  language: string | null
}

export interface BctcDocumentRow {
  doc_id: string
  ticker: string
  year: number | null
  period_key: string | null
  title: string | null
  published_at: string | null
  doc_class: string | null
  is_consolidated: boolean | null
  display_status: string | null
  is_available_for_web: boolean | null
  url_pdf: string | null
  file_size: number | null
}

export interface PaginatedResponse<T> {
  data: T[]
  total: number
  page: number
  page_size: number
  has_more: boolean
}
