import { useDecisionDetail } from '../api/hooks'

interface Props {
  itemId: string
  onClose: () => void
}

export default function ItemDrawer({ itemId, onClose }: Props) {
  const { data: detail, isLoading } = useDecisionDetail(itemId)

  return (
    <div className="fixed inset-0 bg-black/50 flex justify-end z-50" onClick={onClose}>
      <div
        className="w-[480px] max-w-[90vw] bg-bg-primary border-l border-border h-screen overflow-auto p-5"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-base text-text-primary font-medium">{itemId.slice(0, 12)}...</h3>
          <button className="bg-transparent border-none text-text-secondary text-lg cursor-pointer px-2" onClick={onClose}>
            &times;
          </button>
        </div>

        {isLoading && <div className="text-text-secondary py-5">Loading...</div>}

        {detail && (
          <div className="space-y-5">
            <Section title="Classification">
              <Row label="Label" value={detail.label} />
              <Row label="Confidence" value={detail.confidence != null ? `${Math.round(detail.confidence * 100)}%` : null} />
              <Row label="Tier" value={detail.tier_used} />
              <Row label="Source" value={`${detail.source_type} — ${detail.source_path ?? ''}`} />
              <Row label="Media" value={detail.media_type} />
            </Section>

            {detail.extracted && Object.keys(detail.extracted).filter(k => !k.startsWith('_')).length > 0 && (
              <Section title="Extracted Fields">
                {Object.entries(detail.extracted)
                  .filter(([k]) => !k.startsWith('_'))
                  .map(([k, v]) => (
                    <Row key={k} label={k} value={typeof v === 'object' ? JSON.stringify(v) : String(v ?? '')} />
                  ))}
              </Section>
            )}

            <Section title="Destinations">
              <div className="flex gap-1 flex-wrap">
                {detail.destinations.map((d) => (
                  <span key={d} className="text-xs bg-success text-white px-2 py-0.5 rounded">{d}</span>
                ))}
                {detail.exception_queued && (
                  <span className="text-xs bg-danger text-white px-2 py-0.5 rounded">Exception queued</span>
                )}
              </div>
            </Section>

            {detail.trace && (
              <>
                <Section title="Tier Trace">
                  {detail.trace.tiers.map((t, i) => (
                    <div key={i} className="flex gap-2 items-center py-0.5 text-xs">
                      <span className="text-tier-claude font-semibold min-w-[80px]">{t.tier}</span>
                      {t.skipped ? (
                        <span className="text-text-secondary">skipped ({t.skip_reason})</span>
                      ) : (
                        <span className="text-success">
                          {t.label} @ {t.confidence != null ? Math.round(t.confidence * 100) : '?'}% ({t.duration_ms}ms)
                        </span>
                      )}
                    </div>
                  ))}
                </Section>

                <Section title="Rules Evaluated">
                  {detail.trace.rules.map((r, i) => (
                    <div key={i} className="flex gap-2 items-center py-0.5 text-xs">
                      <span className={r.matched ? 'text-success' : 'text-border'}>
                        {r.matched ? '\u2713' : '\u2717'}
                      </span>
                      <span className="text-text-primary">{r.rule_name}</span>
                    </div>
                  ))}
                </Section>

                <Section title="Actions">
                  {detail.trace.actions.map((a, i) => (
                    <div key={i} className="flex gap-2 items-center py-0.5 text-xs">
                      <span className={a.ok ? 'text-success' : 'text-danger'}>
                        {a.ok ? '\u2713' : '\u2717'}
                      </span>
                      <span className="text-text-primary">{a.handler}</span>
                      {a.ref && <span className="text-text-secondary text-[11px]">ref: {a.ref.slice(0, 12)}</span>}
                      {a.reason && <span className="text-danger text-[11px]">{a.reason}</span>}
                      {a.duration_ms != null && <span className="text-text-secondary text-[11px] ml-auto">{a.duration_ms}ms</span>}
                    </div>
                  ))}
                </Section>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <h4 className="text-xs font-semibold text-accent uppercase tracking-wide mb-1.5 border-b border-border pb-1">
        {title}
      </h4>
      {children}
    </div>
  )
}

function Row({ label, value }: { label: string; value: string | null | undefined }) {
  if (!value) return null
  return (
    <div className="flex justify-between py-0.5 text-[13px]">
      <span className="text-text-secondary">{label}</span>
      <span className="text-text-primary text-right max-w-[60%] break-words">{value}</span>
    </div>
  )
}
