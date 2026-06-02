import { useQuery } from '@tanstack/react-query'
import { fetchFinancials } from '@/api/stocks'

export const useFinancials = (symbol: string, periodType?: string) =>
  useQuery({
    queryKey: ['financials', symbol, periodType],
    queryFn: () => fetchFinancials(symbol, periodType),
    enabled: symbol.length > 0,
    staleTime: 5 * 60_000,
    retry: 2,
  })
