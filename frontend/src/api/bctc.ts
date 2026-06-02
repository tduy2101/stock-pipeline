import client from './client'
import type { BctcDocumentRow, PaginatedResponse } from '@/types'

export interface BctcArchiveParams {
  ticker?: string
  year?: number
  q?: string
  from?: string
  to?: string
  page?: number
  page_size?: number
}

export const fetchBctcDocuments = async (
  symbol: string,
  params?: Pick<BctcArchiveParams, 'year' | 'from' | 'to'>,
): Promise<BctcDocumentRow[] | null> => {
  const response = await client.get<BctcDocumentRow[] | null>(`/bctc/${symbol}`, {
    params,
  })
  return response.data
}

export const fetchRecentBctcDocuments = async (
  pageSize = 10,
): Promise<BctcDocumentRow[]> => {
  const response = await client.get<BctcDocumentRow[]>('/bctc/recent', {
    params: { page_size: pageSize },
  })
  return response.data
}

export const fetchAllBctcDocuments = async (
  params?: BctcArchiveParams,
): Promise<PaginatedResponse<BctcDocumentRow>> => {
  const response = await client.get<PaginatedResponse<BctcDocumentRow>>(
    '/bctc/documents',
    { params },
  )
  return response.data
}

export const getBctcFileUrl = (symbol: string, docId: string): string => {
  const base = import.meta.env.VITE_API_URL ?? '/api'
  return `${base}/bctc/${symbol}/file/${docId}`
}
