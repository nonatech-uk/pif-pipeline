import { useState } from 'react'
import { useDecisions, useArchiveDecisions, useSubmitFeedback } from '../api/hooks'
import type { ArchiveResult, DecisionItem } from '../api/types'

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

function FeedbackControls({ item }: { item: DecisionItem }) {
  const feedback = useSubmitFeedback()
  const [pendingDirection, setPendingDirection] = useState<1 | -1 | null>(null)
  const [note, setNote] = useState('')
  const [editing, setEditing] = useState(false)

  const handleSubmit = () => {
    const dir = pendingDirection ?? item.feedback as 1 | -1
    if (dir === -1 && !note.trim()) return // mandatory note for disagree
    feedback.mutate({ itemId: item.item_id, feedback: dir, note: note.trim() || undefined })
    setPendingDirection(null)
    setEditing(false)
    setNote('')
  }

  const handleCancel = () => {
    setPendingDirection(null)
    setEditing(false)
    setNote('')
  }

  // Note input — shown for new feedback or editing existing
  if (pendingDirection != null || editing) {
    const dir = pendingDirection ?? item.feedback as 1 | -1
    const isDisagree = dir === -1
    return (
      <div className="flex items-center gap-1" onClick={e => e.stopPropagation()}>
        <span className="text-sm">{dir === 1 ? '\u{1F44D}' : '\u{1F44E}'}</span>
        <input
          type="text"
          value={note}
          onChange={e => setNote(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') handleSubmit()
            if (e.key === 'Escape') handleCancel()
          }}
          placeholder={isDisagree ? 'Why? (required)' : 'Note (optional)'}
          className="text-[11px] px-1.5 py-0.5 bg-bg-secondary text-text-primary border border-border rounded w-[160px]"
          autoFocus
        />
        <button
          onClick={handleSubmit}
          disabled={isDisagree && !note.trim()}
          className={`text-[10px] px-1.5 py-0.5 text-white rounded disabled:opacity-30 ${isDisagree ? 'bg-danger hover:bg-danger/80' : 'bg-success hover:bg-success/80'}`}
        >
          {editing ? 'Update' : 'Send'}
        </button>
        <button
          onClick={handleCancel}
          className="text-[10px] px-1 py-0.5 text-text-secondary hover:text-text-primary bg-transparent border-none cursor-pointer"
        >
          Esc
        </button>
      </div>
    )
  }

  // Already has feedback — show result, click thumb or note to edit
  if (item.feedback != null) {
    return (
      <div className="flex items-center gap-1.5 min-w-[60px] justify-end" onClick={e => e.stopPropagation()}>
        <button
          className={`text-sm bg-transparent border-none cursor-pointer p-0 hover:scale-110 transition-transform ${item.feedback === 1 ? 'text-success' : 'text-danger'}`}
          title="Click to edit feedback"
          onClick={() => { setNote(item.feedback_note ?? ''); setEditing(true) }}
        >
          {item.feedback === 1 ? '\u{1F44D}' : '\u{1F44E}'}
        </button>
        {item.feedback_note ? (
          <span
            className="text-[10px] text-text-secondary max-w-[120px] truncate cursor-pointer hover:text-text-primary"
            title={`${item.feedback_note} (click to edit)`}
            onClick={() => { setNote(item.feedback_note ?? ''); setEditing(true) }}
          >
            {item.feedback_note}
          </span>
        ) : (
          <button
            onClick={() => { setNote(''); setEditing(true) }}
            className="text-[10px] text-text-secondary hover:text-text-primary bg-transparent border-none cursor-pointer opacity-40 hover:opacity-100"
            title="Add note"
          >
            +note
          </button>
        )}
      </div>
    )
  }

  // No feedback yet — show thumbs
  return (
    <div className="flex items-center gap-1 min-w-[60px] justify-end" onClick={e => e.stopPropagation()}>
      <button
        onClick={() => { setNote(''); setPendingDirection(1) }}
        disabled={feedback.isPending}
        className="text-sm opacity-60 hover:opacity-100 transition-opacity bg-transparent border-none cursor-pointer p-0.5"
        title="Good decision"
      >
        {'\u{1F44D}'}
      </button>
      <button
        onClick={() => { setNote(''); setPendingDirection(-1) }}
        disabled={feedback.isPending}
        className="text-sm opacity-60 hover:opacity-100 transition-opacity bg-transparent border-none cursor-pointer p-0.5"
        title="Bad decision"
      >
        {'\u{1F44E}'}
      </button>
    </div>
  )
}

export default function DecisionsList({ onSelect }: Props) {
  const [source, setSource] = useState('all')
  const [hideIgnored, setHideIgnored] = useState(true)
  const { data } = useDecisions(source, false)
  const archive = useArchiveDecisions()
  const [archiveResult, setArchiveResult] = useState<ArchiveResult | null>(null)
  const allItems = data?.items ?? []
  const items = hideIgnored ? allItems.filter(i => i.destinations.length > 0 && !i.destinations.every(d => d === 'ignored')) : allItems

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
          {allItems.length > 0 && (
            <button
              onClick={() => archive.mutate(undefined, {
                onSuccess: (data) => setArchiveResult(data),
              })}
              disabled={archive.isPending}
              className="text-xs px-3 py-1 bg-accent text-white rounded hover:bg-accent/80 disabled:opacity-50 transition-colors"
            >
              {archive.isPending ? 'Archiving...' : 'Archive'}
            </button>
          )}
        </div>
      </div>

      {archiveResult && (
        <div className="mb-3 p-3 bg-success/10 border border-success/30 rounded-md text-xs text-text-primary">
          <div className="font-medium mb-1">Archived {archiveResult.archived_count} decisions</div>
          {(archiveResult.emails.moved_to_archive > 0 || archiveResult.emails.deleted > 0) && (
            <div className="text-text-secondary">
              {archiveResult.emails.moved_to_archive > 0 && <span>{archiveResult.emails.moved_to_archive} emails moved to Archive. </span>}
              {archiveResult.emails.deleted > 0 && <span>{archiveResult.emails.deleted} emails no longer in Pipelined. </span>}
              {archiveResult.emails.already_moved.length > 0 && <span>{archiveResult.emails.already_moved.length} manually moved. </span>}
            </div>
          )}
          {archiveResult.suggestions.length > 0 && (
            <div className="mt-1 text-accent">
              Suggested {archiveResult.suggestions.length} sender(s) to ignore — check Corrections panel.
            </div>
          )}
          <button
            onClick={() => setArchiveResult(null)}
            className="mt-1 text-[11px] text-text-secondary hover:text-text-primary bg-transparent border-none cursor-pointer"
          >
            Dismiss
          </button>
        </div>
      )}

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
                {(item.extracted?.['_summary'] as string)?.slice(0, 60) || item.source_path?.split('/').pop() || item.item_id.slice(0, 8)}
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
            <FeedbackControls item={item} />
            <div className="text-[11px] text-text-secondary min-w-[105px] text-right">
              {new Date(item.timestamp).toLocaleString()}
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}
