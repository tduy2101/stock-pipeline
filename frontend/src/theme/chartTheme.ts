import type { Theme } from '@/theme/ThemeProvider'

export interface ChartTheme {
  grid: string
  axis: string
  tooltipBg: string
  tooltipBorder: string
  tooltipText: string
  closeLine: string
}

export const CHART_THEME: Record<Theme, ChartTheme> = {
  light: {
    grid: '#e2e8f0',
    axis: '#64748b',
    tooltipBg: '#ffffff',
    tooltipBorder: '#cbd5e1',
    tooltipText: '#0f172a',
    closeLine: '#0f172a',
  },
  dark: {
    grid: '#334155',
    axis: '#94a3b8',
    tooltipBg: '#111827',
    tooltipBorder: '#334155',
    tooltipText: '#e5e7eb',
    closeLine: '#e5e7eb',
  },
}
