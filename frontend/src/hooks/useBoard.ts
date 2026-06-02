import { useQuery } from '@tanstack/react-query'
import { fetchForeignFlow, fetchPriceBoard } from '@/api/board'
import type { DatePageParams } from '@/api/stocks'

export const usePriceBoard = (
  symbol: string,
  params?: Omit<DatePageParams, 'page'>,
) =>
  useQuery({
    queryKey: ['priceBoard', symbol, params],
    queryFn: () => fetchPriceBoard(symbol, params),
    enabled: symbol.length > 0,
    staleTime: 30_000,
    retry: 2,
  })

export const useForeignFlow = (
  symbol: string,
  params?: Omit<DatePageParams, 'page' | 'page_size'> & { days?: number },
) =>
  useQuery({
    queryKey: ['foreignFlow', symbol, params],
    queryFn: () => fetchForeignFlow(symbol, params),
    enabled: symbol.length > 0,
    staleTime: 60_000,
    retry: 2,
  })
