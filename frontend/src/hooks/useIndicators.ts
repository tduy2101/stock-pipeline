import { useQuery } from '@tanstack/react-query'
import { fetchIndicators, type DatePageParams } from '@/api/stocks'

export const useIndicators = (symbol: string, params?: DatePageParams) =>
  useQuery({
    queryKey: ['indicators', symbol, params],
    queryFn: () => fetchIndicators(symbol, params),
    enabled: symbol.length > 0,
    staleTime: 30_000,
    retry: 2,
  })
