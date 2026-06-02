import { Moon, Sun } from 'lucide-react'
import { useTheme } from '@/theme/ThemeProvider'

export function ThemeToggle() {
  const { theme, toggleTheme } = useTheme()
  const isDark = theme === 'dark'

  return (
    <button
      type="button"
      onClick={toggleTheme}
      aria-label={isDark ? 'Chuyển sang giao diện sáng' : 'Chuyển sang giao diện tối'}
      title={isDark ? 'Giao diện sáng' : 'Giao diện tối'}
      className="grid h-10 w-10 shrink-0 place-items-center rounded-lg border border-app-border bg-card-dark text-app-text transition-colors hover:bg-app-hover hover:text-app-heading"
    >
      {isDark ? <Sun size={17} /> : <Moon size={17} />}
    </button>
  )
}
