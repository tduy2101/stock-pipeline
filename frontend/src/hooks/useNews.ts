import { useQuery } from '@tanstack/react-query'
import {
  fetchAllNewsArticles,
  fetchMarketNews,
  fetchNews,
  fetchNewsArticles,
  fetchNewsSignal,
  fetchNewsSignalSummary,
  type NewsArticleParams,
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

export const useNewsSignal = (symbol: string, params?: DatePageParams) =>
  useQuery({
    queryKey: ['newsSignal', symbol, params],
    queryFn: () => fetchNewsSignal(symbol, params),
    enabled: symbol.length > 0,
    staleTime: 5 * 60_000,
    retry: 2,
  })

export const useNewsSignalSummary = (symbol: string) =>
  useQuery({
    queryKey: ['newsSignalSummary', symbol],
    queryFn: () => fetchNewsSignalSummary(symbol),
    enabled: symbol.length > 0,
    staleTime: 5 * 60_000,
    retry: 2,
  })

export const useNewsArticles = (symbol: string, params?: NewsArticleParams) =>
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
