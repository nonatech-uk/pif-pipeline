import type { PipelineStatus } from '../api/types'

interface Props {
  status: PipelineStatus
}

export default function StatsRow({ status }: Props) {
  const cards = [
    { label: 'Processed Today', value: status.processed_today, color: 'text-accent' },
    { label: 'Auto-Filed', value: status.auto_filed_today, color: 'text-success' },
    { label: 'Exceptions', value: status.exceptions_pending, color: status.exceptions_pending > 0 ? 'text-danger' : 'text-text-secondary' },
    { label: 'Corrections', value: status.corrections_pending, color: status.corrections_pending > 0 ? 'text-warning' : 'text-text-secondary' },
  ]

  return (
    <div className="grid grid-cols-4 gap-3 mb-7">
      {cards.map((c) => (
        <div key={c.label} className="bg-bg-card border border-border rounded-lg px-5 py-4 text-center">
          <div className={`text-3xl font-bold ${c.color}`}>{c.value}</div>
          <div className="text-xs text-text-secondary mt-1 uppercase tracking-wide">{c.label}</div>
        </div>
      ))}
    </div>
  )
}
