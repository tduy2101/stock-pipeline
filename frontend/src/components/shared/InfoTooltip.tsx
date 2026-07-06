import { HelpCircle } from 'lucide-react'

interface InfoTooltipProps {
  text: string
  label?: string
}

export function InfoTooltip({ text, label = 'Giải thích' }: InfoTooltipProps) {
  return (
    <span className="group relative inline-flex shrink-0 align-middle">
      <button
        type="button"
        className="inline-flex h-5 w-5 items-center justify-center rounded-full text-app-muted transition-colors hover:bg-app-hover hover:text-app-heading focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
        aria-label={label}
        aria-describedby={undefined}
      >
        <HelpCircle size={14} aria-hidden />
      </button>
      <span
        role="tooltip"
        className="pointer-events-none absolute bottom-full left-0 z-50 mb-2 hidden w-[min(18rem,calc(100vw-2rem))] rounded-lg border border-app-border bg-panel-dark p-3 text-left text-xs leading-5 text-app-text shadow-xl group-hover:block group-focus-within:block sm:left-1/2 sm:w-72 sm:-translate-x-1/2"
      >
        {text}
      </span>
    </span>
  )
}
