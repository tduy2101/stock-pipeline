import client from './client'
import type { MarketOverviewResponse } from '@/types'

export const fetchMarketOverview = async (
  date?: string,
): Promise<MarketOverviewResponse | null> => {
  const params = date ? { date } : {}
  const response = await client.get<MarketOverviewResponse | null>('/market/overview', {
    params,
  })
  return response.data
}
