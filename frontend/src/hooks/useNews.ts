import { useQuery } from '@tanstack/react-query'
import {
  fetchAllNewsArticles,
  fetchMarketNews,
  fetchNews,
  fetchNewsArticles,
  type NewsArchiveParams,
} from '@/api/news'
import type { DatePageParams } from '@/api/stocks'

export const useNews = (symbol: string, params?: DatePageParams) =>
  useQuery({
    queryKey: ['news', symbol, params],
    queryFn: () => fetchNews(symbol, params),
    enabled: symbol.length > 0,
    staleTime: 60_000,
    retry: 2,
  })

export const useNewsArticles = (symbol: string, params?: DatePageParams) =>
  useQuery({
    queryKey: ['newsArticles', symbol, params],
    queryFn: () => fetchNewsArticles(symbol, params),
    enabled: symbol.length > 0,
    staleTime: 60_000,
    retry: 2,
  })

export const useMarketNews = (pageSize = 10) =>
  useQuery({
    queryKey: ['marketNews', pageSize],
    queryFn: () => fetchMarketNews(pageSize),
    staleTime: 60_000,
    retry: 2,
  })

export const useAllNewsArticles = (params?: NewsArchiveParams) =>
  useQuery({
    queryKey: ['allNewsArticles', params],
    queryFn: () => fetchAllNewsArticles(params),
    staleTime: 60_000,
    retry: 2,
  })
