import client from './client'
import type {
  CompanyProfileResponse,
  FinancialRatioRow,
  IndicatorRow,
  PaginatedResponse,
  PriceRow,
  TickerListResponse,
} from '@/types'

export interface DatePageParams {
  from?: string
  to?: string
  page?: number
  page_size?: number
}

export const fetchTickers = async (): Promise<TickerListResponse> => {
  const response = await client.get<TickerListResponse>('/tickers')
  return response.data
}

export const fetchCompany = async (
  symbol: string,
): Promise<CompanyProfileResponse | null> => {
  const response = await client.get<CompanyProfileResponse | null>(
    `/companies/${symbol}`,
  )
  return response.data
}

export const fetchPrices = async (
  symbol: string,
  params?: DatePageParams,
): Promise<PaginatedResponse<PriceRow> | null> => {
  const response = await client.get<PaginatedResponse<PriceRow> | null>(
    `/prices/${symbol}`,
    { params },
  )
  return response.data
}

export const fetchIndicators = async (
  symbol: string,
  params?: DatePageParams,
): Promise<PaginatedResponse<IndicatorRow> | null> => {
  const response = await client.get<PaginatedResponse<IndicatorRow> | null>(
    `/indicators/${symbol}`,
    { params },
  )
  return response.data
}

export const fetchFinancials = async (
  symbol: string,
  periodType?: string,
): Promise<FinancialRatioRow[] | null> => {
  const params = periodType ? { period_type: periodType } : {}
  const response = await client.get<FinancialRatioRow[] | null>(
    `/financials/${symbol}`,
    { params },
  )
  return response.data
}
