import client from './client'
import type { NewsArticleRow, NewsDailyRow, PaginatedResponse } from '@/types'
import type { DatePageParams } from './stocks'

export interface NewsArchiveParams extends DatePageParams {
  ticker?: string
  q?: string
  sentiment?: string
}

export const fetchNews = async (
  symbol: string,
  params?: DatePageParams,
): Promise<PaginatedResponse<NewsDailyRow> | null> => {
  const response = await client.get<PaginatedResponse<NewsDailyRow> | null>(
    `/news/${symbol}`,
    { params },
  )
  return response.data
}

export const fetchNewsArticles = async (
  symbol: string,
  params?: DatePageParams,
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
