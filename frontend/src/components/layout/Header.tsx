import { BarChart3, FileText, Home, Newspaper } from 'lucide-react'
import { Link, NavLink } from 'react-router-dom'
import { SearchBar } from '@/components/shared/SearchBar'
import { ThemeToggle } from '@/components/layout/ThemeToggle'

const navItems = [
  { to: '/', label: 'Tổng quan', icon: Home },
  { to: '/news', label: 'Tin tức', icon: Newspaper },
  { to: '/bctc', label: 'BCTC PDF', icon: FileText },
]

export function Header() {
  return (
    <header className="sticky top-0 z-20 border-b border-app-border bg-app-dark/95 backdrop-blur">
      <div className="mx-auto flex max-w-7xl flex-col gap-3 px-4 py-3 sm:px-6 lg:px-8">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <Link to="/" className="flex items-center gap-3">
            <span className="grid h-10 w-10 place-items-center rounded-lg bg-accent text-white">
              <BarChart3 size={21} />
            </span>
            <div>
              <p className="text-sm font-semibold text-app-heading">Vietnam Stock Intelligence</p>
              <p className="text-xs text-app-muted">Phân tích Gold layer cho thị trường, tin tức và công bố PDF</p>
            </div>
          </Link>
          <div className="flex w-full items-center gap-2 lg:max-w-2xl">
            <SearchBar />
            <ThemeToggle />
          </div>
        </div>
        <nav className="flex gap-2 overflow-x-auto scrollbar-thin">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === '/'}
              className={({ isActive }) =>
                `inline-flex h-9 shrink-0 items-center gap-2 rounded-md border px-3 text-sm font-medium transition-colors ${
                  isActive
                    ? 'border-accent bg-accent text-white'
                    : 'border-app-border bg-panel-dark text-app-muted hover:bg-app-hover hover:text-app-heading'
                }`
              }
            >
              <item.icon size={15} />
              {item.label}
            </NavLink>
          ))}
        </nav>
      </div>
    </header>
  )
}
