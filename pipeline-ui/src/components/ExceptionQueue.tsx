import { useExceptions, useTriageException } from '../api/hooks'

interface Props {
  onSelect: (id: string) => void
}

export default function ExceptionQueue({ onSelect }: Props) {
  const { data } = useExceptions()
  const triage = useTriageException()
  const items = data?.items ?? []

  if (items.length === 0) return null

  return (
    <section className="mb-7">
      <h2 className="text-base font-semibold text-text-primary mb-2.5 flex items-center gap-2">
        Exceptions
        <span className="bg-danger text-white text-[11px] font-bold px-2 py-0.5 rounded-full">
          {items.length}
        </span>
      </h2>

      <div className="flex flex-col gap-1.5">
        {items.map((item) => (
          <div
            key={item.item_id}
            className="flex items-center gap-3 px-3.5 py-2.5 bg-bg-card border border-border border-l-3 border-l-danger rounded-md cursor-pointer hover:bg-bg-hover transition-colors"
            onClick={() => onSelect(item.item_id)}
          >
            <div className="text-[11px] font-bold text-danger min-w-[30px]">
              P{item.review_priority}
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-sm font-medium text-text-primary truncate">
                {(item.envelope as Record<string, string>)?.file_name || item.item_id.slice(0, 8)}
              </div>
              <div className="text-xs text-text-secondary mt-0.5">{item.reason}</div>
            </div>
            <div className="flex flex-col items-end gap-1">
              {(item.classification as Record<string, string>)?.label && (
                <span className="text-[11px] bg-bg-hover text-accent px-1.5 py-0.5 rounded">
                  {(item.classification as Record<string, string>).label}
                </span>
              )}
              <span className="text-[11px] text-text-secondary">
                {new Date(item.created_at).toLocaleTimeString()}
              </span>
            </div>
            <div className="flex gap-1">
              <button
                className="text-[11px] px-2 py-1 bg-success text-white border-none rounded cursor-pointer hover:opacity-80"
                onClick={(e) => {
                  e.stopPropagation()
                  const dest = prompt('File as (document type):')
                  if (dest) triage.mutate({ itemId: item.item_id, action: 'file_as', destination: dest })
                }}
              >
                File
              </button>
              <button
                className="text-[11px] px-2 py-1 bg-accent text-white border-none rounded cursor-pointer hover:opacity-80"
                onClick={(e) => { e.stopPropagation(); triage.mutate({ itemId: item.item_id, action: 'retrigger' }) }}
              >
                Retry
              </button>
              <button
                className="text-[11px] px-2 py-1 bg-border text-text-secondary border-none rounded cursor-pointer hover:opacity-80"
                onClick={(e) => { e.stopPropagation(); triage.mutate({ itemId: item.item_id, action: 'discard' }) }}
              >
                Discard
              </button>
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}
