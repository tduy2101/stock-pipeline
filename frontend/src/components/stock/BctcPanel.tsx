import { ExternalLink, FileText } from 'lucide-react'
import { useMemo, useState } from 'react'
import { getBctcFileUrl } from '@/api/bctc'
import { EmptyState } from '@/components/shared/EmptyState'
import { Skeleton } from '@/components/ui/skeleton'
import { useBctc } from '@/hooks/useBctc'
import { formatBytes, formatDate } from '@/utils/formatters'

interface BctcPanelProps {
  symbol: string
  fromDate?: string
  toDate?: string
}

export function BctcPanel({ symbol, fromDate, toDate }: BctcPanelProps) {
  const [year, setYear] = useState<number | undefined>(undefined)
  const allDocs = useBctc(symbol, {
    from: fromDate || undefined,
    to: toDate || undefined,
  })
  const filteredDocs = useBctc(symbol, {
    year,
    from: fromDate || undefined,
    to: toDate || undefined,
  })
  const data = year == null ? allDocs.data : filteredDocs.data
  const isLoading = year == null ? allDocs.isLoading : filteredDocs.isLoading

  const years = useMemo(() => {
    const values = new Set<number>()
    for (const doc of allDocs.data ?? []) {
      if (doc.year != null) values.add(doc.year)
    }
    return [...values].sort((a, b) => b - a)
  }, [allDocs.data])

  const openPdf = (docId: string) => {
    window.open(getBctcFileUrl(symbol, docId), '_blank', 'noopener,noreferrer')
  }

  if (isLoading) return <Skeleton className="h-80" />

  return (
    <div className="grid gap-4">
      {years.length > 0 && (
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => setYear(undefined)}
            className={`h-8 rounded-md px-3 text-xs font-medium transition-colors ${
              year == null ? 'bg-accent text-white' : 'bg-app-hover text-app-muted hover:bg-app-input hover:text-app-heading'
            }`}
          >
            Tất cả
          </button>
          {years.map((item) => (
            <button
              key={item}
              type="button"
              onClick={() => setYear(item)}
              className={`h-8 rounded-md px-3 text-xs font-medium transition-colors ${
                year === item ? 'bg-accent text-white' : 'bg-app-hover text-app-muted hover:bg-app-input hover:text-app-heading'
              }`}
            >
              {item}
            </button>
          ))}
        </div>
      )}

      {!data?.length ? (
        <EmptyState message="Không có báo cáo tài chính" subMessage="Thử ticker hoặc năm khác" />
      ) : (
        <div className="grid gap-3">
          {data.map((doc) => (
            <div
              key={doc.doc_id}
              className="flex flex-col gap-3 rounded-lg border border-app-border bg-card-dark p-4 md:flex-row md:items-start md:justify-between"
            >
              <div className="flex min-w-0 gap-3">
                <FileText className="mt-0.5 shrink-0 text-accent" size={20} />
                <div className="min-w-0">
                  <p className="line-clamp-2 text-sm font-medium text-app-heading">
                    {doc.title ?? 'Báo cáo tài chính'}
                  </p>
                  <p className="mt-1 text-xs text-app-muted">
                    {doc.year ?? 'N/A'} / {doc.period_key ?? 'N/A'} / {doc.doc_class ?? 'tài liệu'} / {formatBytes(doc.file_size)}
                  </p>
                  <p className="mt-1 text-xs text-app-subtle">{formatDate(doc.published_at)}</p>
                </div>
              </div>
              {doc.is_available_for_web && (
                <button
                  type="button"
                  onClick={() => openPdf(doc.doc_id)}
                  className="inline-flex h-9 shrink-0 items-center justify-center gap-2 rounded-md border border-accent/50 px-3 text-xs font-medium text-accent transition-colors hover:bg-accent hover:text-white"
                >
                  <ExternalLink size={14} />
                  Xem PDF
                </button>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
