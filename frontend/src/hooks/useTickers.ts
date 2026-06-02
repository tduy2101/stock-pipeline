import { useQuery } from '@tanstack/react-query'
import { fetchTickers } from '@/api/stocks'

export const useTickers = () =>
  useQuery({
    queryKey: ['tickers'],
    queryFn: fetchTickers,
    staleTime: 5 * 60_000,
    retry: 2,
  })
