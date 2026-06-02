import { useQuery } from '@tanstack/react-query'
import { fetchPrices, type DatePageParams } from '@/api/stocks'

export const usePrices = (symbol: string, params?: DatePageParams) =>
  useQuery({
    queryKey: ['prices', symbol, params],
    queryFn: () => fetchPrices(symbol, params),
    enabled: symbol.length > 0,
    staleTime: 30_000,
    retry: 2,
  })
