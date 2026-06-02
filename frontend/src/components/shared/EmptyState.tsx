import { Inbox } from 'lucide-react'

interface EmptyStateProps {
  message: string
  subMessage?: string
}

export function EmptyState({ message, subMessage }: EmptyStateProps) {
  return (
    <div className="flex min-h-32 flex-col items-center justify-center rounded-lg border border-dashed border-app-border bg-card-dark/70 p-6 text-center">
      <Inbox className="mb-3 text-app-subtle" size={24} />
      <p className="text-sm font-medium text-app-heading">{message}</p>
      {subMessage && <p className="mt-1 text-xs text-app-muted">{subMessage}</p>}
    </div>
  )
}
