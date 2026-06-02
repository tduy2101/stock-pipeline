import { Component, type ErrorInfo, type ReactNode } from 'react'

interface ErrorBoundaryProps {
  children: ReactNode
}

interface ErrorBoundaryState {
  hasError: boolean
}

export class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  state: ErrorBoundaryState = { hasError: false }

  static getDerivedStateFromError(): ErrorBoundaryState {
    return { hasError: true }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Frontend render error', error, info)
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="mx-auto mt-16 max-w-xl rounded-lg border border-red-300 bg-red-50 p-6 text-center dark:border-red-900/60 dark:bg-red-950/30">
          <p className="text-base font-semibold text-red-700 dark:text-red-200">Không thể hiển thị màn hình</p>
          <p className="mt-2 text-sm text-red-600 dark:text-red-100/80">
            Hãy tải lại trang hoặc quay về dashboard.
          </p>
        </div>
      )
    }
    return this.props.children
  }
}
