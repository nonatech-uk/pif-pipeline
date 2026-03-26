import { useCorrections, useAcceptCorrection, useRejectCorrection } from '../api/hooks'

export default function CorrectionsPanel() {
  const { data } = useCorrections()
  const accept = useAcceptCorrection()
  const reject = useRejectCorrection()
  const items = data?.items ?? []

  if (items.length === 0) return null

  return (
    <section className="mb-7">
      <h2 className="text-base font-semibold text-text-primary mb-2.5 flex items-center gap-2">
        Corrections
        <span className="bg-accent text-white text-[11px] font-bold px-2 py-0.5 rounded-full">
          {items.length}
        </span>
      </h2>

      <div className="flex flex-col gap-1.5">
        {items.map((item) => (
          <div
            key={item.id}
            className="flex items-center gap-3 px-3.5 py-2.5 bg-bg-card border border-border border-l-3 border-l-accent rounded-md"
          >
            <div className="flex-1 min-w-0">
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
            <div className="flex gap-1">
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
          </div>
        ))}
      </div>
    </section>
  )
}
