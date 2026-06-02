interface SkeletonProps {
  className?: string
}

export function Skeleton({ className = '' }: SkeletonProps) {
  return <div className={`animate-pulse rounded-lg bg-app-skeleton/80 ${className}`} />
}
