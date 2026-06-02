import type { Config } from 'tailwindcss'

const config: Config = {
  darkMode: 'class',
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        'app-dark': 'rgb(var(--color-bg) / <alpha-value>)',
        'panel-dark': 'rgb(var(--color-panel) / <alpha-value>)',
        'card-dark': 'rgb(var(--color-card) / <alpha-value>)',
        'app-border': 'rgb(var(--color-border) / <alpha-value>)',
        'app-heading': 'rgb(var(--color-heading) / <alpha-value>)',
        'app-text': 'rgb(var(--color-text) / <alpha-value>)',
        'app-muted': 'rgb(var(--color-muted) / <alpha-value>)',
        'app-subtle': 'rgb(var(--color-subtle) / <alpha-value>)',
        'app-hover': 'rgb(var(--color-hover) / <alpha-value>)',
        'app-input': 'rgb(var(--color-input) / <alpha-value>)',
        'app-table-head': 'rgb(var(--color-table-head) / <alpha-value>)',
        'app-skeleton': 'rgb(var(--color-skeleton) / <alpha-value>)',
        'price-up': '#16a34a',
        'price-down': '#dc2626',
        'price-flat': '#9ca3af',
        'sentiment-positive': '#16a34a',
        'sentiment-neutral': '#9ca3af',
        'sentiment-negative': '#dc2626',
        accent: '#3b82f6',
      },
      fontFamily: {
        sans: ['Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'ui-monospace', 'monospace'],
      },
    },
  },
  plugins: [],
}

export default config
