import { useState, useMemo } from 'react'
import { useCorrections, useAcceptCorrection, useRejectCorrection } from '../api/hooks'

type Tab = 'pending' | 'accepted' | 'rejected'

export default function CorrectionsPanel() {
  const [tab, setTab] = useState<Tab>('pending')
  const [search, setSearch] = useState('')
  const { data: pendingData } = useCorrections('pending')
  const { data: tabData } = useCorrections(tab)
  const accept = useAcceptCorrection()
  const reject = useRejectCorrection()

  const pendingCount = pendingData?.items?.length ?? 0
  const allItems = tabData?.items ?? []

  const items = useMemo(() => {
    let filtered = allItems
    if (search) {
      const q = search.toLowerCase()
      filtered = filtered.filter(item =>
        item.field?.toLowerCase().includes(q) ||
        item.original_value?.toLowerCase().includes(q) ||
        item.corrected_value?.toLowerCase().includes(q) ||
        item.label?.toLowerCase().includes(q) ||
        item.item_id?.toLowerCase().includes(q)
      )
    }
    if (tab !== 'pending') {
      return filtered.slice().reverse()
    }
    return filtered
  }, [allItems, search, tab])

  const tabs: { key: Tab; label: string }[] = [
    { key: 'pending', label: 'Pending' },
    { key: 'accepted', label: 'Accepted' },
    { key: 'rejected', label: 'Rejected' },
  ]

  return (
    <section className="mb-7">
      <h2 className="text-base font-semibold text-text-primary mb-2.5 flex items-center gap-2">
        Corrections
        {pendingCount > 0 && (
          <span className="bg-accent text-white text-[11px] font-bold px-2 py-0.5 rounded-full">
            {pendingCount}
          </span>
        )}
      </h2>

      <div className="flex items-center gap-3 mb-2.5">
        <div className="flex gap-1">
          {tabs.map(t => (
            <button
              key={t.key}
              onClick={() => { setTab(t.key); setSearch('') }}
              className={`text-xs px-2.5 py-1 rounded border-none cursor-pointer transition-colors ${
                tab === t.key
                  ? 'bg-accent text-white'
                  : 'bg-bg-secondary text-text-secondary hover:text-text-primary'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>
        {tab !== 'pending' && (
          <input
            type="text"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search corrections..."
            className="text-xs px-2.5 py-1 bg-bg-secondary text-text-primary border border-border rounded flex-1 max-w-[250px] placeholder:text-text-secondary"
          />
        )}
      </div>

      <div className="flex flex-col gap-1.5">
        {items.length === 0 && (
          <div className="text-sm text-text-secondary py-5 text-center">
            No {tab} corrections
          </div>
        )}
        {items.map((item) => (
          <div
            key={item.id}
            className="px-3.5 py-2.5 bg-bg-card border border-border border-l-3 border-l-accent rounded-md"
          >
            {tab === 'pending' && (
              <div className="flex justify-end gap-1 mb-1.5">
                <button
                  onClick={() => accept.mutate(item.id)}
                  disabled={accept.isPending}
                  className="text-[11px] px-2 py-1 bg-success text-white border-none rounded cursor-pointer hover:opacity-80 disabled:opacity-50"
                >
                  Accept
                </button>
                <button
                  onClick={() => reject.mutate(item.id)}
                  disabled={reject.isPending}
                  className="text-[11px] px-2 py-1 bg-border text-text-secondary border-none rounded cursor-pointer hover:opacity-80 disabled:opacity-50"
                >
                  Reject
                </button>
              </div>
            )}
            <div className="text-sm text-text-primary">
              <span className="font-medium">{item.field}</span>
              {item.original_value && (
                <span className="text-text-secondary">
                  {' '}<span className="line-through">{item.original_value}</span>
                </span>
              )}
              {' '}<span className="text-accent">{item.corrected_value}</span>
            </div>
            {item.proposed_action && (
              <div className="text-xs text-text-secondary mt-0.5">
                {item.proposed_action.description}
              </div>
            )}
            <div className="text-[11px] text-text-secondary mt-0.5">
              {item.item_id?.slice(0, 8)} &middot; {item.label} &middot; {new Date(item.created_at).toLocaleString()}
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}
