import { Home } from 'lucide-react'
import { Link } from 'react-router-dom'

export default function NotFoundPage() {
  return (
    <div className="flex min-h-[60vh] flex-col items-center justify-center gap-4 px-4 text-center">
      <p className="font-mono text-6xl font-bold text-app-subtle">404</p>
      <p className="text-lg font-medium text-app-heading">Trang không tồn tại</p>
      <Link to="/" className="inline-flex items-center gap-2 text-sm font-medium text-accent hover:underline">
        <Home size={15} />
        Về tổng quan
      </Link>
    </div>
  )
}
