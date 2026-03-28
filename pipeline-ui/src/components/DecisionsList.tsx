import { useState } from 'react'
import { useDecisions } from '../api/hooks'

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

interface Props {
  onSelect: (id: string) => void
}

export default function DecisionsList({ onSelect }: Props) {
  const [source, setSource] = useState('all')
  const [hideIgnored, setHideIgnored] = useState(true)
  const { data } = useDecisions(source)
  const allItems = data?.items ?? []
  const items = hideIgnored ? allItems.filter(i => i.destinations.length > 0) : allItems

  return (
    <section className="mb-7">
      <div className="flex justify-between items-center mb-2.5">
        <h2 className="text-base font-semibold text-text-primary">Decisions</h2>
        <div className="flex items-center gap-3">
          <label className="flex items-center gap-1.5 text-xs text-text-secondary cursor-pointer">
            <input
              type="checkbox"
              checked={hideIgnored}
              onChange={(e) => setHideIgnored(e.target.checked)}
              className="rounded"
            />
            Hide ignored
          </label>
          <select
            className="text-xs px-2 py-1 bg-bg-secondary text-text-primary border border-border rounded"
            value={source}
            onChange={(e) => setSource(e.target.value)}
          >
            <option value="all">All sources</option>
            <option value="scanner">Scanner</option>
            <option value="camera">Camera</option>
            <option value="email">Email</option>
          </select>
        </div>
      </div>

      <div className="flex flex-col gap-1">
        {items.length === 0 && (
          <div className="text-sm text-text-secondary py-5 text-center">No decisions yet</div>
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
            <div className="text-[11px] text-text-secondary min-w-[105px] text-right">
              {new Date(item.timestamp).toLocaleString()}
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}
