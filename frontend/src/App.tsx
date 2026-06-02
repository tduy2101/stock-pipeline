import { lazy, Suspense } from 'react'
import { Route, Routes } from 'react-router-dom'
import { Header } from '@/components/layout/Header'
import { ErrorBoundary } from '@/components/shared/ErrorBoundary'
import { Skeleton } from '@/components/ui/skeleton'

const DashboardPage = lazy(() => import('@/pages/DashboardPage'))
const StockDetailPage = lazy(() => import('@/pages/StockDetailPage'))
const NewsArchivePage = lazy(() => import('@/pages/NewsArchivePage'))
const BctcArchivePage = lazy(() => import('@/pages/BctcArchivePage'))
const NotFoundPage = lazy(() => import('@/pages/NotFoundPage'))

function PageLoader() {
  return (
    <div className="mx-auto flex max-w-7xl flex-col gap-4 px-4 py-6 sm:px-6 lg:px-8">
      <Skeleton className="h-10 w-56" />
      <Skeleton className="h-80 w-full" />
    </div>
  )
}

export default function App() {
  return (
    <div className="min-h-screen bg-app-dark text-app-text">
      <Header />
      <main>
        <ErrorBoundary>
          <Suspense fallback={<PageLoader />}>
            <Routes>
              <Route path="/" element={<DashboardPage />} />
              <Route path="/news" element={<NewsArchivePage />} />
              <Route path="/bctc" element={<BctcArchivePage />} />
              <Route path="/stock/:symbol" element={<StockDetailPage />} />
              <Route path="*" element={<NotFoundPage />} />
            </Routes>
          </Suspense>
        </ErrorBoundary>
      </main>
    </div>
  )
}
