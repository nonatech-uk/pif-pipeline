import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '../api/client'
import type { DecisionItem } from '../api/types'

const TIER_COLORS: Record<string, string> = {
  deterministic: 'text-tier-deterministic',
  clip: 'text-tier-clip',
  llm: 'text-tier-llm',
  claude: 'text-tier-claude',
}

const SOURCE_ICONS: Record<string, string> = {
  scanner: '\u{1F4C4}',
  camera: '\u{1F4F7}',
  email: '\u{2709}\u{FE0F}',
}

const PAGE_SIZE = 25

interface Props {
  onSelect: (id: string) => void
}

export default function HistoryView({ onSelect }: Props) {
  const [source, setSource] = useState('all')
  const [label, setLabel] = useState('')
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [hideIgnored, setHideIgnored] = useState(true)
  const [page, setPage] = useState(0)

  const params = new URLSearchParams()
  params.set('limit', String(PAGE_SIZE))
  params.set('offset', String(page * PAGE_SIZE))
  if (source !== 'all') params.set('source', source)
  if (label) params.set('label', label)
  if (dateFrom) params.set('date_from', dateFrom)
  if (dateTo) params.set('date_to', dateTo)
  if (hideIgnored) params.set('hide_ignored', 'true')

  const { data } = useQuery<{ items: DecisionItem[]; total: number }>({
    queryKey: ['history', source, label, dateFrom, dateTo, hideIgnored, page],
    queryFn: () => apiFetch(`/decisions?${params.toString()}`),
  })

  const { data: labelsData } = useQuery<{ labels: string[] }>({
    queryKey: ['labels'],
    queryFn: () => apiFetch('/decisions/labels'),
  })

  const items = data?.items ?? []
  const total = data?.total ?? 0
  const totalPages = Math.ceil(total / PAGE_SIZE)

  const resetFilters = () => {
    setSource('all')
    setLabel('')
    setDateFrom('')
    setDateTo('')
    setHideIgnored(true)
    setPage(0)
  }

  return (
    <div>
      <div className="flex flex-wrap gap-3 mb-4 items-end">
        <div>
          <label className="block text-[11px] text-text-secondary mb-1">Source</label>
          <select
            className="text-xs px-2 py-1.5 bg-bg-secondary text-text-primary border border-border rounded"
            value={source}
            onChange={(e) => { setSource(e.target.value); setPage(0) }}
          >
            <option value="all">All sources</option>
            <option value="scanner">Scanner</option>
            <option value="camera">Camera</option>
            <option value="email">Email</option>
          </select>
        </div>
        <div>
          <label className="block text-[11px] text-text-secondary mb-1">Label</label>
          <select
            className="text-xs px-2 py-1.5 bg-bg-secondary text-text-primary border border-border rounded"
            value={label}
            onChange={(e) => { setLabel(e.target.value); setPage(0) }}
          >
            <option value="">All labels</option>
            {(labelsData?.labels ?? []).map(l => (
              <option key={l} value={l}>{l}</option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-[11px] text-text-secondary mb-1">From</label>
          <input
            type="date"
            className="text-xs px-2 py-1.5 bg-bg-secondary text-text-primary border border-border rounded"
            value={dateFrom}
            onChange={(e) => { setDateFrom(e.target.value); setPage(0) }}
          />
        </div>
        <div>
          <label className="block text-[11px] text-text-secondary mb-1">To</label>
          <input
            type="date"
            className="text-xs px-2 py-1.5 bg-bg-secondary text-text-primary border border-border rounded"
            value={dateTo}
            onChange={(e) => { setDateTo(e.target.value); setPage(0) }}
          />
        </div>
        <label className="flex items-center gap-1.5 text-xs text-text-secondary cursor-pointer self-end pb-1">
          <input
            type="checkbox"
            checked={hideIgnored}
            onChange={(e) => { setHideIgnored(e.target.checked); setPage(0) }}
            className="rounded"
          />
          Hide ignored
        </label>
        <button
          onClick={resetFilters}
          className="text-xs px-2 py-1.5 text-text-secondary hover:text-text-primary"
        >
          Clear
        </button>
        <div className="ml-auto text-xs text-text-secondary self-end pb-1">
          {total} result{total !== 1 ? 's' : ''}
        </div>
      </div>

      <div className="flex flex-col gap-1">
        {items.length === 0 && (
          <div className="text-sm text-text-secondary py-8 text-center">No decisions found</div>
        )}
        {items.map((item) => (
          <div
            key={item.item_id}
            className="flex items-center gap-3 px-3.5 py-2 bg-bg-card border border-border rounded-md cursor-pointer hover:bg-bg-hover transition-colors"
            onClick={() => onSelect(item.item_id)}
          >
            <div className="text-lg min-w-[28px] text-center">
              {SOURCE_ICONS[item.source_type] ?? '\u{2753}'}
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-[13px] font-medium text-text-primary truncate">
                {item.source_path?.split('/').pop() ?? item.item_id.slice(0, 8)}
              </div>
              <div className="flex gap-1.5 mt-1 items-center">
                {item.label && (
                  <span className="text-[11px] bg-bg-hover text-accent px-1.5 py-0.5 rounded">
                    {item.label}
                  </span>
                )}
                {item.confidence != null && (
                  <span className="text-[11px] text-success">
                    {Math.round(item.confidence * 100)}%
                  </span>
                )}
                {item.tier_used && (
                  <span className={`text-[10px] font-semibold uppercase ${TIER_COLORS[item.tier_used] ?? 'text-text-secondary'}`}>
                    {item.tier_used}
                  </span>
                )}
                {(item.extracted?.['_tags'] as string[] | undefined)?.map(t => (
                  <span key={t} className="text-[10px] bg-accent/15 text-accent px-1 py-0.5 rounded">{t}</span>
                ))}
              </div>
            </div>
            <div className="flex gap-1 flex-wrap">
              {item.destinations.map((d) => (
                <span
                  key={d}
                  className={`text-[10px] px-1.5 py-0.5 rounded text-white ${item.exception_queued ? 'bg-danger' : 'bg-success'}`}
                >
                  {d}
                </span>
              ))}
            </div>
            <div className="text-[11px] text-text-secondary min-w-[130px] text-right">
              {new Date(item.timestamp).toLocaleString()}
            </div>
          </div>
        ))}
      </div>

      {totalPages > 1 && (
        <div className="flex justify-center items-center gap-3 mt-4">
          <button
            disabled={page === 0}
            onClick={() => setPage(p => p - 1)}
            className="text-xs px-3 py-1 bg-bg-secondary border border-border rounded disabled:opacity-30"
          >
            Prev
          </button>
          <span className="text-xs text-text-secondary">
            Page {page + 1} of {totalPages}
          </span>
          <button
            disabled={page >= totalPages - 1}
            onClick={() => setPage(p => p + 1)}
            className="text-xs px-3 py-1 bg-bg-secondary border border-border rounded disabled:opacity-30"
          >
            Next
          </button>
        </div>
      )}
    </div>
  )
}
