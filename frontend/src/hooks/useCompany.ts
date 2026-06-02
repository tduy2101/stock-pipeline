import { useQuery } from '@tanstack/react-query'
import { fetchCompany } from '@/api/stocks'

export const useCompany = (symbol: string) =>
  useQuery({
    queryKey: ['company', symbol],
    queryFn: () => fetchCompany(symbol),
    enabled: symbol.length > 0,
    staleTime: 5 * 60_000,
    retry: 2,
  })
