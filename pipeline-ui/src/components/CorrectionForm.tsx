import { useState } from 'react'
import { useCreateCorrection } from '../api/hooks'

interface Props {
  itemId: string
  currentLabel: string | null
  currentExtracted: Record<string, unknown>
}

const LABELS = [
  'receipt', 'invoice', 'boarding_pass', 'bank_statement', 'insurance_policy',
  'tax_document', 'wine_label', 'pet_photo', 'geo_tagged', 'landscape',
  'document', 'duplicate',
]

export default function CorrectionForm({ itemId, currentLabel, currentExtracted }: Props) {
  const [open, setOpen] = useState(false)
  const [label, setLabel] = useState(currentLabel ?? '')
  const [correspondent, setCorrespondent] = useState(
    String(currentExtracted?.['_correspondent'] ?? '')
  )
  const [docType, setDocType] = useState('')
  const currentTags = (currentExtracted?.['_tags'] as string[] | undefined) ?? []
  const [tags, setTags] = useState<string[]>(currentTags)
  const [newTag, setNewTag] = useState('')
  const [submitted, setSubmitted] = useState(false)
  const create = useCreateCorrection()

  const addTag = () => {
    const t = newTag.trim()
    if (t && !tags.includes(t)) {
      setTags([...tags, t])
    }
    setNewTag('')
  }

  const removeTag = (tag: string) => {
    setTags(tags.filter(t => t !== tag))
  }

  const handleSubmit = () => {
    const corrections: { field: string; original: string; corrected: string }[] = []

    if (label && label !== currentLabel) {
      corrections.push({ field: 'label', original: currentLabel ?? '', corrected: label })
    }
    if (correspondent && correspondent !== String(currentExtracted?.['_correspondent'] ?? '')) {
      corrections.push({
        field: 'correspondent',
        original: String(currentExtracted?.['_correspondent'] ?? ''),
        corrected: correspondent,
      })
    }
    if (docType) {
      corrections.push({ field: 'document_type', original: '', corrected: docType })
    }

    // Tag changes
    const added = tags.filter(t => !currentTags.includes(t))
    const removed = currentTags.filter(t => !tags.includes(t))
    for (const t of added) {
      corrections.push({ field: 'tag_added', original: '', corrected: t })
    }
    for (const t of removed) {
      corrections.push({ field: 'tag_removed', original: t, corrected: '' })
    }

    if (corrections.length === 0) return

    create.mutate({ item_id: itemId, corrections }, {
      onSuccess: () => setSubmitted(true),
    })
  }

  if (submitted) {
    return (
      <div className="bg-success/10 border border-success/30 rounded p-3 text-xs text-success">
        Correction submitted
      </div>
    )
  }

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="text-xs text-accent bg-transparent border border-accent/30 rounded px-3 py-1.5 cursor-pointer hover:bg-accent/10 transition-colors w-full"
      >
        Correct classification
      </button>
    )
  }

  return (
    <div className="space-y-2.5 bg-bg-hover rounded p-3 border border-border">
      <div>
        <label className="block text-[11px] text-text-secondary mb-0.5">Label</label>
        <select
          value={label}
          onChange={(e) => setLabel(e.target.value)}
          className="w-full text-xs bg-bg-primary border border-border rounded px-2 py-1.5 text-text-primary"
        >
          <option value="">-- select --</option>
          {LABELS.map((l) => (
            <option key={l} value={l}>{l}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="block text-[11px] text-text-secondary mb-0.5">Correspondent</label>
        <input
          type="text"
          value={correspondent}
          onChange={(e) => setCorrespondent(e.target.value)}
          className="w-full text-xs bg-bg-primary border border-border rounded px-2 py-1.5 text-text-primary"
          placeholder="e.g. Apple"
        />
      </div>

      <div>
        <label className="block text-[11px] text-text-secondary mb-0.5">Document type</label>
        <input
          type="text"
          value={docType}
          onChange={(e) => setDocType(e.target.value)}
          className="w-full text-xs bg-bg-primary border border-border rounded px-2 py-1.5 text-text-primary"
          placeholder="e.g. Invoice"
        />
      </div>

      <div>
        <label className="block text-[11px] text-text-secondary mb-0.5">Tags</label>
        <div className="flex gap-1 flex-wrap mb-1.5">
          {tags.map(t => (
            <span
              key={t}
              className="text-[11px] bg-accent/15 text-accent px-1.5 py-0.5 rounded flex items-center gap-1"
            >
              {t}
              <button
                onClick={() => removeTag(t)}
                className="bg-transparent border-none text-accent/60 hover:text-danger cursor-pointer text-xs px-0"
              >
                &times;
              </button>
            </span>
          ))}
        </div>
        <div className="flex gap-1">
          <input
            type="text"
            value={newTag}
            onChange={(e) => setNewTag(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); addTag() } }}
            className="flex-1 text-xs bg-bg-primary border border-border rounded px-2 py-1 text-text-primary"
            placeholder="Add tag..."
          />
          <button
            onClick={addTag}
            className="text-xs bg-bg-primary border border-border rounded px-2 py-1 text-text-secondary hover:text-text-primary cursor-pointer"
          >
            +
          </button>
        </div>
      </div>

      <div className="flex gap-2 pt-1">
        <button
          onClick={handleSubmit}
          disabled={create.isPending}
          className="text-xs bg-accent text-white border-none rounded px-3 py-1.5 cursor-pointer hover:opacity-80 disabled:opacity-50"
        >
          {create.isPending ? 'Saving...' : 'Submit correction'}
        </button>
        <button
          onClick={() => setOpen(false)}
          className="text-xs bg-transparent text-text-secondary border border-border rounded px-3 py-1.5 cursor-pointer hover:bg-bg-hover"
        >
          Cancel
        </button>
      </div>
    </div>
  )
}
