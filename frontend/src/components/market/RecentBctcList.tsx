import { ExternalLink, FileText } from 'lucide-react'
import { Link } from 'react-router-dom'
import { getBctcFileUrl } from '@/api/bctc'
import { EmptyState } from '@/components/shared/EmptyState'
import { Skeleton } from '@/components/ui/skeleton'
import { useRecentBctc } from '@/hooks/useBctc'
import { formatBytes, formatDate } from '@/utils/formatters'

export function RecentBctcList() {
  const { data, isLoading } = useRecentBctc(8)

  if (isLoading) return <Skeleton className="h-96" />
  if (!data?.length) return <EmptyState message="Không có BCTC mới" />

  return (
    <section className="rounded-lg border border-app-border bg-panel-dark p-5">
      <div className="mb-4 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <FileText className="text-accent" size={18} />
          <h2 className="text-sm font-semibold text-app-heading">BCTC mới nhất</h2>
        </div>
        <Link to="/bctc" className="text-xs font-medium text-accent hover:underline">
          Xem tất cả
        </Link>
      </div>
      <div className="grid gap-3">
        {data.map((doc) => (
          <article key={doc.doc_id} className="rounded-lg border border-app-border bg-card-dark p-3">
            <div className="mb-2 flex flex-wrap items-center gap-2">
              <Link to={`/stock/${doc.ticker}`} className="font-mono text-xs font-semibold text-accent hover:underline">
                {doc.ticker}
              </Link>
              <span className="text-xs text-app-muted">{doc.year ?? 'N/A'} / {doc.period_key ?? 'N/A'}</span>
              <span className="text-xs text-app-subtle">{formatDate(doc.published_at)}</span>
            </div>
            <h3 className="line-clamp-2 text-sm font-semibold leading-6 text-app-heading">
              {doc.title ?? 'Báo cáo tài chính'}
            </h3>
            <p className="mt-2 text-xs text-app-muted">
              {doc.doc_class ?? 'tài liệu'} / {formatBytes(doc.file_size)}
            </p>
            {doc.is_available_for_web && (
              <a
                href={getBctcFileUrl(doc.ticker, doc.doc_id)}
                target="_blank"
                rel="noreferrer"
                className="mt-3 inline-flex items-center gap-1.5 text-xs font-medium text-accent hover:underline"
              >
                <ExternalLink size={13} />
                Xem PDF
              </a>
            )}
          </article>
        ))}
      </div>
    </section>
  )
}
