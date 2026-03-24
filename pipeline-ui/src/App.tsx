import { useState } from 'react'
import StatsRow from './components/StatsRow'
import ExceptionQueue from './components/ExceptionQueue'
import DecisionsList from './components/DecisionsList'
import ItemDrawer from './components/ItemDrawer'
import { useStatus } from './api/hooks'

export default function App() {
  const [selectedItem, setSelectedItem] = useState<string | null>(null)
  const { data: status } = useStatus()

  return (
    <div className="max-w-[1100px] mx-auto px-4 py-6">
      <header className="flex justify-between items-baseline mb-6">
        <h1 className="text-xl font-semibold text-text-primary">Pipeline Dashboard</h1>
        {status?.last_processed && (
          <span className="text-xs text-text-secondary">
            Last: {new Date(status.last_processed).toLocaleString()}
          </span>
        )}
      </header>

      {status && <StatsRow status={status} />}

      <ExceptionQueue onSelect={setSelectedItem} />
      <DecisionsList onSelect={setSelectedItem} />

      {selectedItem && (
        <ItemDrawer itemId={selectedItem} onClose={() => setSelectedItem(null)} />
      )}
    </div>
  )
}
