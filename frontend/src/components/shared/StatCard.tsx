import type { LucideIcon } from 'lucide-react'

interface StatCardProps {
  icon?: LucideIcon
  label: string
  value: string
  note?: string
  accent?: 'default' | 'up' | 'down' | 'neutral'
}

const accentClasses = {
  default: 'text-app-heading',
  up: 'text-price-up',
  down: 'text-price-down',
  neutral: 'text-app-muted',
}

export function StatCard({ icon: Icon, label, value, note, accent = 'default' }: StatCardProps) {
  return (
    <div className="flex items-start gap-3 rounded-lg border border-app-border bg-panel-dark p-4">
      {Icon && (
        <span className="grid h-10 w-10 shrink-0 place-items-center rounded-lg bg-accent/10 text-accent">
          <Icon size={18} aria-hidden />
        </span>
      )}
      <div className="min-w-0">
        <p className="text-xs text-app-muted">{label}</p>
        <p className={`mt-1 text-base font-semibold leading-snug ${accentClasses[accent]}`}>
          {value}
        </p>
        {note && <p className="mt-1 text-xs leading-5 text-app-subtle">{note}</p>}
      </div>
    </div>
  )
}
