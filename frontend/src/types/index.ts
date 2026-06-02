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
  free_float_percentage: number | null
  free_float: number | null
  number_of_employees: number | null
  founded_date: string | null
  ceo_name: string | null
  ceo_position: string | null
  outstanding_shares: number | null
  auditor: string | null
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
  volume_ma20: number | null
  obv: number | null
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

export interface FinancialSummaryRow {
  ticker: string
  period: string
  period_type: string | null
  year: number | null
  quarter: number | null
  pe_ratio: number | null
  pb_ratio: number | null
  ps_ratio: number | null
  ev_ebit: number | null
  ev_ebitda: number | null
  eps: number | null
  roe: number | null
  roe_trailing: number | null
  roa: number | null
  roa_trailing: number | null
  roce: number | null
  gross_profit_margin: number | null
  net_profit_margin: number | null
  ebit_margin: number | null
  ebitda_margin: number | null
  current_ratio: number | null
  quick_ratio: number | null
  cash_ratio: number | null
  debt_to_equity: number | null
  debt_to_assets: number | null
  liabilities_to_equity: number | null
  liabilities_to_assets: number | null
  revenue_growth: number | null
  gross_profit_growth: number | null
  profit_growth: number | null
  dividend_yield: number | null
  dividend_per_share: number | null
  book_value_per_share: number | null
  cash_flow_per_share: number | null
  beta: number | null
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

export interface TopArticle {
  article_id: string
  title: string
  url: string
  sentiment?: SentimentLabel | null
  published_at?: string | null
  relevance?: 'title' | 'summary' | 'body' | null
  weight?: number | null
}

export interface NewsSignalRow {
  ticker: string
  trading_date: string
  news_count: number
  positive_count: number | null
  negative_count: number | null
  neutral_count: number | null
  avg_sentiment_score: number | null
  weighted_sentiment: number | null
  dominant_sentiment: SentimentLabel | null
  news_signal: 'buy_signal' | 'sell_signal' | 'neutral' | null
  top_articles: TopArticle[] | null
}

export interface NewsSignalSummary {
  ticker: string
  latest_date: string | null
  news_signal: 'buy_signal' | 'sell_signal' | 'neutral' | null
  weighted_sentiment: number | null
  news_count: number | null
  top_articles: TopArticle[] | null
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
  ticker_relevance: 'title' | 'summary' | 'body' | null
  source_tier: number | null
}

export interface PriceBoardRow {
  symbol: string
  trading_date: string
  exchange: string | null
  ceiling_price: number | null
  floor_price: number | null
  reference_price: number | null
  close_price: number | null
  open_price: number | null
  high_price: number | null
  low_price: number | null
  average_price: number | null
  percent_change: number | null
  price_change: number | null
  volume_accumulated: number | null
  total_value: number | null
  bid_price_1: number | null
  bid_vol_1: number | null
  bid_price_2: number | null
  bid_vol_2: number | null
  bid_price_3: number | null
  bid_vol_3: number | null
  ask_price_1: number | null
  ask_vol_1: number | null
  ask_price_2: number | null
  ask_vol_2: number | null
  ask_price_3: number | null
  ask_vol_3: number | null
  foreign_buy_volume: number | null
  foreign_sell_volume: number | null
  foreign_net_volume: number | null
  foreign_room: number | null
  spread_pct: number | null
  snapshot_at: string | null
}

export interface ForeignFlowRow {
  trading_date: string
  foreign_buy_volume: number | null
  foreign_sell_volume: number | null
  foreign_net_volume: number | null
  foreign_room: number | null
}

export interface BctcDocumentRow {
  doc_id: string
  ticker: string
  year: number | null
  period_key: string | null
  title: string | null
  normalized_title: string | null
  published_at: string | null
  doc_class: string | null
  canonical_priority: number | null
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
