import { useState } from 'react'
import AppSwitcher from './components/layout/AppSwitcher'
import StatsRow from './components/StatsRow'
import ExceptionQueue from './components/ExceptionQueue'
import DecisionsList from './components/DecisionsList'
import CorrectionsPanel from './components/CorrectionsPanel'
import ItemDrawer from './components/ItemDrawer'
import RulesList from './components/RulesList'
import HistoryView from './components/HistoryView'
import SettingsPanel from './components/SettingsPanel'
import { useStatus } from './api/hooks'
import type { SelectedItem } from './api/types'

type Tab = 'dashboard' | 'history' | 'rules' | 'settings'

export default function App() {
  const [tab, setTab] = useState<Tab>('dashboard')
  const [selectedItem, setSelectedItem] = useState<SelectedItem | null>(null)
  const { data: status } = useStatus()

  return (
    <div className="max-w-[1100px] mx-auto px-4 py-6">
      <header className="flex flex-wrap items-center gap-y-2 gap-x-6 mb-6">
        <div className="flex items-center gap-2">
          <a href="/" className="text-xl font-semibold text-text-primary hover:text-accent transition-colors">Pipeline</a>
          <AppSwitcher currentApp="Pipeline" />
        </div>
        <nav className="flex gap-1">
          <TabButton active={tab === 'dashboard'} onClick={() => setTab('dashboard')}>
            Dashboard
          </TabButton>
          <TabButton active={tab === 'history'} onClick={() => setTab('history')}>
            History
          </TabButton>
          <TabButton active={tab === 'rules'} onClick={() => setTab('rules')}>
            Rules
          </TabButton>
          <TabButton active={tab === 'settings'} onClick={() => setTab('settings')}>
            Settings
          </TabButton>
        </nav>
        {status?.last_processed && (
          <span className="hidden sm:inline text-xs text-text-secondary ml-auto">
            Last: {new Date(status.last_processed).toLocaleString()}
          </span>
        )}
      </header>

      {tab === 'dashboard' && (
        <>
          {status && <StatsRow status={status} />}
          <ExceptionQueue onSelect={(id) => setSelectedItem({ id, context: 'exception' })} />
          <CorrectionsPanel />
          <DecisionsList onSelect={(id) => setSelectedItem({ id, context: 'decision' })} />
          {selectedItem && (
            <ItemDrawer
              itemId={selectedItem.id}
              context={selectedItem.context}
              onClose={() => setSelectedItem(null)}
            />
          )}
        </>
      )}

      {tab === 'history' && (
        <>
          <HistoryView onSelect={(id) => setSelectedItem({ id, context: 'decision' })} />
          {selectedItem && (
            <ItemDrawer
              itemId={selectedItem.id}
              context={selectedItem.context}
              onClose={() => setSelectedItem(null)}
            />
          )}
        </>
      )}

      {tab === 'rules' && <RulesList />}

      {tab === 'settings' && <SettingsPanel />}
    </div>
  )
}

function TabButton({ active, onClick, children }: {
  active: boolean; onClick: () => void; children: React.ReactNode
}) {
  return (
    <button
      onClick={onClick}
      className={`px-3 py-1 text-sm rounded transition-colors ${
        active
          ? 'bg-accent/15 text-accent font-medium'
          : 'text-text-secondary hover:text-text-primary hover:bg-bg-hover'
      }`}
    >
      {children}
    </button>
  )
}
