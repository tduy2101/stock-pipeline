import { useQuery } from '@tanstack/react-query'
import {
  fetchAllBctcDocuments,
  fetchBctcDocuments,
  fetchRecentBctcDocuments,
  type BctcArchiveParams,
} from '@/api/bctc'

export const useBctc = (
  symbol: string,
  params?: Pick<BctcArchiveParams, 'year' | 'from' | 'to'>,
) =>
  useQuery({
    queryKey: ['bctc', symbol, params],
    queryFn: () => fetchBctcDocuments(symbol, params),
    enabled: symbol.length > 0,
    staleTime: 5 * 60_000,
    retry: 2,
  })

export const useRecentBctc = (pageSize = 10) =>
  useQuery({
    queryKey: ['recentBctc', pageSize],
    queryFn: () => fetchRecentBctcDocuments(pageSize),
    staleTime: 5 * 60_000,
    retry: 2,
  })

export const useAllBctcDocuments = (params?: BctcArchiveParams) =>
  useQuery({
    queryKey: ['allBctcDocuments', params],
    queryFn: () => fetchAllBctcDocuments(params),
    staleTime: 5 * 60_000,
    retry: 2,
  })
