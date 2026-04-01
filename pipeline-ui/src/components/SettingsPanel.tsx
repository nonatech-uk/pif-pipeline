import { useState } from 'react'
import { useIgnoreSenders, useAddIgnoreSender, useDeleteIgnoreSender } from '../api/hooks'

export default function SettingsPanel() {
  const { data } = useIgnoreSenders()
  const addSender = useAddIgnoreSender()
  const deleteSender = useDeleteIgnoreSender()
  const [address, setAddress] = useState('')
  const [note, setNote] = useState('')

  const handleAdd = () => {
    const trimmed = address.trim()
    if (!trimmed) return
    addSender.mutate({ address: trimmed, note: note.trim() }, {
      onSuccess: () => { setAddress(''); setNote('') },
    })
  }

  return (
    <div>
      <h2 className="text-base font-semibold text-text-primary mb-3">Settings</h2>

      <div className="bg-bg-card border border-border rounded-md p-4">
        <h3 className="text-sm font-medium text-text-primary mb-3">Ignored Email Senders</h3>
        <p className="text-xs text-text-secondary mb-3">
          Emails from these senders will be skipped during ingestion.
        </p>

        <div className="flex gap-2 mb-4">
          <input
            type="text"
            placeholder="email@example.com"
            value={address}
            onChange={(e) => setAddress(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleAdd()}
            className="flex-1 text-sm px-2.5 py-1.5 bg-bg-primary border border-border rounded text-text-primary placeholder:text-text-secondary/50 outline-none focus:border-accent"
          />
          <input
            type="text"
            placeholder="Note (optional)"
            value={note}
            onChange={(e) => setNote(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleAdd()}
            className="flex-1 text-sm px-2.5 py-1.5 bg-bg-primary border border-border rounded text-text-primary placeholder:text-text-secondary/50 outline-none focus:border-accent"
          />
          <button
            onClick={handleAdd}
            disabled={!address.trim() || addSender.isPending}
            className="text-sm px-3 py-1.5 bg-accent text-white rounded hover:bg-accent/80 disabled:opacity-50 transition-colors"
          >
            Add
          </button>
        </div>

        {data?.items.length === 0 && (
          <p className="text-xs text-text-secondary italic">No ignored senders configured.</p>
        )}

        <div className="space-y-1">
          {data?.items.map((sender) => (
            <div
              key={sender.id}
              className="flex items-center justify-between gap-2 px-3 py-2 bg-bg-primary border border-border rounded text-sm"
            >
              <div className="min-w-0">
                <span className="text-text-primary font-mono text-xs">{sender.address}</span>
                {sender.note && (
                  <span className="text-text-secondary text-xs ml-2">{sender.note}</span>
                )}
              </div>
              <button
                onClick={() => deleteSender.mutate(sender.id)}
                className="text-xs text-danger hover:text-danger/80 bg-transparent border-none cursor-pointer shrink-0"
              >
                Remove
              </button>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
