import {
  createContext,
  type ReactNode,
  useContext,
  useMemo,
  useState,
} from 'react'

interface TabsContextValue {
  value: string
  setValue: (value: string) => void
}

const TabsContext = createContext<TabsContextValue | null>(null)

const useTabs = () => {
  const context = useContext(TabsContext)
  if (!context) throw new Error('Tabs components must be used inside Tabs')
  return context
}

interface TabsProps {
  defaultValue: string
  children: ReactNode
}

export function Tabs({ defaultValue, children }: TabsProps) {
  const [value, setValue] = useState(defaultValue)
  const contextValue = useMemo(() => ({ value, setValue }), [value])
  return (
    <TabsContext.Provider value={contextValue}>{children}</TabsContext.Provider>
  )
}

interface TabsListProps {
  className?: string
  children: ReactNode
}

export function TabsList({ className = '', children }: TabsListProps) {
  return <div className={`flex flex-wrap gap-2 ${className}`}>{children}</div>
}

interface TabsTriggerProps {
  value: string
  children: ReactNode
}

export function TabsTrigger({ value, children }: TabsTriggerProps) {
  const { value: activeValue, setValue } = useTabs()
  const active = activeValue === value
  return (
    <button
      type="button"
      onClick={() => setValue(value)}
      className={`h-9 rounded-md px-3 text-sm font-medium transition-colors ${
        active
          ? 'bg-accent text-white'
          : 'bg-app-hover text-app-muted hover:bg-app-input hover:text-app-heading'
      }`}
    >
      {children}
    </button>
  )
}

interface TabsContentProps {
  value: string
  className?: string
  children: ReactNode
}

export function TabsContent({ value, className = '', children }: TabsContentProps) {
  const { value: activeValue } = useTabs()
  if (activeValue !== value) return null
  return <div className={className}>{children}</div>
}
