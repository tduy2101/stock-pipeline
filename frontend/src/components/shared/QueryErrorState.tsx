import { AlertTriangle, RefreshCw } from 'lucide-react'

interface QueryErrorStateProps {
  title?: string
  message?: string
  onRetry?: () => void
}

export function QueryErrorState({
  title = 'Không tải được dữ liệu',
  message = 'Vui lòng kiểm tra kết nối backend và thử lại.',
  onRetry,
}: QueryErrorStateProps) {
  return (
    <div className="flex min-h-32 flex-col items-center justify-center rounded-lg border border-red-500/30 bg-red-500/5 p-6 text-center">
      <AlertTriangle className="mb-3 text-red-400" size={24} aria-hidden />
      <p className="text-sm font-medium text-app-heading">{title}</p>
      <p className="mt-1 max-w-md text-xs leading-5 text-app-muted">{message}</p>
      {onRetry && (
        <button
          type="button"
          onClick={onRetry}
          className="mt-4 inline-flex h-9 items-center gap-2 rounded-md border border-app-border bg-app-hover px-3 text-xs font-medium text-app-heading transition-colors hover:bg-app-input focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
        >
          <RefreshCw size={14} aria-hidden />
          Thử lại
        </button>
      )}
    </div>
  )
}
