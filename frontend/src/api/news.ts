import client from './client'
import type {
  NewsArticleRow,
  NewsSignalRow,
  NewsSignalSummary,
  PaginatedResponse,
} from '@/types'
import type { DatePageParams } from './stocks'

export interface NewsArchiveParams extends DatePageParams {
  ticker?: string
  q?: string
  sentiment?: string
  relevance?: string
}

export interface NewsArticleParams extends DatePageParams {
  relevance?: string
}

export const fetchNews = async (
  symbol: string,
  params?: DatePageParams,
): Promise<PaginatedResponse<NewsSignalRow> | null> => {
  const response = await client.get<PaginatedResponse<NewsSignalRow> | null>(
    `/news/${symbol}`,
    { params },
  )
  return response.data
}

export const fetchNewsSignal = fetchNews

export const fetchNewsSignalSummary = async (
  symbol: string,
): Promise<NewsSignalSummary | null> => {
  const response = await client.get<NewsSignalSummary | null>(
    `/news/${symbol}/signal`,
  )
  return response.data
}

export const fetchNewsArticles = async (
  symbol: string,
  params?: NewsArticleParams,
): Promise<PaginatedResponse<NewsArticleRow> | null> => {
  const response = await client.get<PaginatedResponse<NewsArticleRow> | null>(
    `/news/${symbol}/articles`,
    { params },
  )
  return response.data
}

export const fetchMarketNews = async (
  pageSize = 10,
): Promise<NewsArticleRow[]> => {
  const response = await client.get<NewsArticleRow[]>('/news/market', {
    params: { page_size: pageSize },
  })
  return response.data
}

export const fetchAllNewsArticles = async (
  params?: NewsArchiveParams,
): Promise<PaginatedResponse<NewsArticleRow>> => {
  const response = await client.get<PaginatedResponse<NewsArticleRow>>(
    '/news/articles',
    { params },
  )
  return response.data
}
