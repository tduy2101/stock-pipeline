import type { SentimentLabel } from '@/types'

interface SentimentBadgeProps {
  label: SentimentLabel | null
}

const LABELS: Record<SentimentLabel, string> = {
  positive: 'Tích cực',
  neutral: 'Trung lập',
  negative: 'Tiêu cực',
}

export function SentimentBadge({ label }: SentimentBadgeProps) {
  const safeLabel = label ?? 'neutral'
  const className =
    safeLabel === 'positive'
      ? 'border-green-500/50 bg-green-50 text-green-700 dark:border-green-700/60 dark:bg-green-950/40 dark:text-green-300'
      : safeLabel === 'negative'
        ? 'border-red-500/50 bg-red-50 text-red-700 dark:border-red-700/60 dark:bg-red-950/40 dark:text-red-300'
        : 'border-app-border bg-app-hover text-app-text'
  return (
    <span className={`inline-flex rounded-full border px-2 py-0.5 text-xs ${className}`}>
      {LABELS[safeLabel]}
    </span>
  )
}
