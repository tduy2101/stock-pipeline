import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { fetchMarketOverview } from '@/api/market'

export const useMarketOverview = (date?: string) =>
  useQuery({
    queryKey: ['marketOverview', date ?? 'latest'],
    queryFn: () => fetchMarketOverview(date),
    staleTime: 30_000,
    retry: 2,
    placeholderData: keepPreviousData,
  })
