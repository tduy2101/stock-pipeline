import client from './client'
import type { ForeignFlowRow, PriceBoardRow } from '@/types'
import type { DatePageParams } from './stocks'

export const fetchPriceBoard = async (
  symbol: string,
  params?: Omit<DatePageParams, 'page'>,
): Promise<PriceBoardRow[] | null> => {
  const response = await client.get<PriceBoardRow[] | null>(
    `/board/${symbol}`,
    { params },
  )
  return response.data
}

export const fetchForeignFlow = async (
  symbol: string,
  params?: Omit<DatePageParams, 'page' | 'page_size'> & { days?: number },
): Promise<ForeignFlowRow[] | null> => {
  const response = await client.get<ForeignFlowRow[] | null>(
    `/board/${symbol}/foreign-flow`,
    { params },
  )
  return response.data
}
