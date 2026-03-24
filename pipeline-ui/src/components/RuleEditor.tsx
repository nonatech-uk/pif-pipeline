import { useState } from 'react'
import { useCreateRule, useUpdateRule } from '../api/hooks'
import type { Rule, RuleCondition, RuleAction } from '../api/types'

const CONDITION_TYPES = [
  'classification', 'media_type', 'source_type', 'gps_proximity',
  'pet_recognition', 'date_range', 'empty',
]
const HANDLER_NAMES = [
  'paperless', 'finance', 'immich_album', 'immich_tag',
  'location', 'exception_queue', 'notify',
]

interface Props {
  rule: Rule | null  // null = creating new
  onClose: () => void
}

function emptyRule(): Rule {
  return {
    id: '',
    name: '',
    priority: 50,
    conditions: [],
    actions: [],
    on_match: 'stop',
    enabled: true,
  }
}

export default function RuleEditor({ rule, onClose }: Props) {
  const isNew = rule === null
  const [form, setForm] = useState<Rule>(rule ? structuredClone(rule) : emptyRule())
  const [error, setError] = useState<string | null>(null)
  const createRule = useCreateRule()
  const updateRule = useUpdateRule()

  const saving = createRule.isPending || updateRule.isPending

  function set<K extends keyof Rule>(key: K, value: Rule[K]) {
    setForm(prev => ({ ...prev, [key]: value }))
  }

  function addCondition() {
    setForm(prev => ({
      ...prev,
      conditions: [...prev.conditions, { type: 'classification' }],
    }))
  }

  function updateCondition(index: number, cond: RuleCondition) {
    setForm(prev => ({
      ...prev,
      conditions: prev.conditions.map((c, i) => i === index ? cond : c),
    }))
  }

  function removeCondition(index: number) {
    setForm(prev => ({
      ...prev,
      conditions: prev.conditions.filter((_, i) => i !== index),
    }))
  }

  function addAction() {
    setForm(prev => ({
      ...prev,
      actions: [...prev.actions, { handler: 'paperless', params: {} }],
    }))
  }

  function updateAction(index: number, action: RuleAction) {
    setForm(prev => ({
      ...prev,
      actions: prev.actions.map((a, i) => i === index ? action : a),
    }))
  }

  function removeAction(index: number) {
    setForm(prev => ({
      ...prev,
      actions: prev.actions.filter((_, i) => i !== index),
    }))
  }

  async function handleSave() {
    setError(null)
    if (!form.id.trim()) { setError('ID is required'); return }
    if (!form.name.trim()) { setError('Name is required'); return }

    try {
      if (isNew) {
        await createRule.mutateAsync(form)
      } else {
        await updateRule.mutateAsync({ id: rule!.id, rule: form })
      }
      onClose()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Save failed')
    }
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-sm font-medium text-text-secondary uppercase tracking-wide">
          {isNew ? 'New Rule' : `Edit: ${rule!.name}`}
        </h2>
        <button onClick={onClose} className="text-xs text-text-secondary hover:text-text-primary">
          Back to list
        </button>
      </div>

      <div className="bg-bg-card border border-border rounded-lg p-5 space-y-5">
        {error && (
          <div className="text-xs text-danger bg-danger/10 border border-danger/20 rounded px-3 py-2">
            {error}
          </div>
        )}

        {/* Basic fields */}
        <div className="grid grid-cols-2 gap-4">
          <Field label="ID" value={form.id} onChange={v => set('id', v)}
            placeholder="p10-my-rule" disabled={!isNew} />
          <Field label="Name" value={form.name} onChange={v => set('name', v)}
            placeholder="My rule description" />
        </div>
        <div className="grid grid-cols-3 gap-4">
          <div>
            <label className="block text-xs text-text-secondary mb-1">Priority</label>
            <input type="number" value={form.priority}
              onChange={e => set('priority', parseInt(e.target.value) || 0)}
              className="w-full bg-bg-primary border border-border rounded px-3 py-1.5 text-sm text-text-primary" />
          </div>
          <div>
            <label className="block text-xs text-text-secondary mb-1">On Match</label>
            <select value={form.on_match} onChange={e => set('on_match', e.target.value as 'stop' | 'continue')}
              className="w-full bg-bg-primary border border-border rounded px-3 py-1.5 text-sm text-text-primary">
              <option value="stop">Stop</option>
              <option value="continue">Continue</option>
            </select>
          </div>
          <div className="flex items-end">
            <label className="flex items-center gap-2 text-sm text-text-primary cursor-pointer">
              <input type="checkbox" checked={form.enabled}
                onChange={e => set('enabled', e.target.checked)}
                className="accent-accent" />
              Enabled
            </label>
          </div>
        </div>

        {/* Conditions */}
        <section>
          <div className="flex justify-between items-center mb-2">
            <h3 className="text-xs font-medium text-text-secondary uppercase tracking-wide">
              Conditions (all must match)
            </h3>
            <button onClick={addCondition}
              className="text-xs px-2 py-1 rounded bg-accent/20 text-accent hover:bg-accent/30">
              + Add
            </button>
          </div>
          {form.conditions.length === 0 && (
            <p className="text-xs text-text-secondary italic">No conditions — rule always matches</p>
          )}
          <div className="space-y-2">
            {form.conditions.map((c, i) => (
              <ConditionRow key={i} condition={c}
                onChange={cond => updateCondition(i, cond)}
                onRemove={() => removeCondition(i)} />
            ))}
          </div>
        </section>

        {/* Actions */}
        <section>
          <div className="flex justify-between items-center mb-2">
            <h3 className="text-xs font-medium text-text-secondary uppercase tracking-wide">Actions</h3>
            <button onClick={addAction}
              className="text-xs px-2 py-1 rounded bg-accent/20 text-accent hover:bg-accent/30">
              + Add
            </button>
          </div>
          {form.actions.length === 0 && (
            <p className="text-xs text-text-secondary italic">No actions defined</p>
          )}
          <div className="space-y-2">
            {form.actions.map((a, i) => (
              <ActionRow key={i} action={a}
                onChange={act => updateAction(i, act)}
                onRemove={() => removeAction(i)} />
            ))}
          </div>
        </section>

        {/* Save/Cancel */}
        <div className="flex gap-3 pt-2 border-t border-border">
          <button onClick={handleSave} disabled={saving}
            className="px-4 py-1.5 rounded bg-accent text-bg-primary text-sm font-medium hover:bg-accent-hover disabled:opacity-50 transition-colors">
            {saving ? 'Saving...' : isNew ? 'Create Rule' : 'Save Changes'}
          </button>
          <button onClick={onClose}
            className="px-4 py-1.5 rounded text-sm text-text-secondary hover:text-text-primary hover:bg-bg-hover transition-colors">
            Cancel
          </button>
        </div>
      </div>
    </div>
  )
}


function Field({ label, value, onChange, placeholder, disabled }: {
  label: string; value: string; onChange: (v: string) => void
  placeholder?: string; disabled?: boolean
}) {
  return (
    <div>
      <label className="block text-xs text-text-secondary mb-1">{label}</label>
      <input type="text" value={value} onChange={e => onChange(e.target.value)}
        placeholder={placeholder} disabled={disabled}
        className="w-full bg-bg-primary border border-border rounded px-3 py-1.5 text-sm text-text-primary disabled:opacity-50" />
    </div>
  )
}


function ConditionRow({ condition, onChange, onRemove }: {
  condition: RuleCondition; onChange: (c: RuleCondition) => void; onRemove: () => void
}) {
  const { type, ...params } = condition
  const [paramsText, setParamsText] = useState(Object.keys(params).length > 0 ? JSON.stringify(params, null, 2) : '')

  function handleTypeChange(newType: string) {
    onChange({ type: newType })
    setParamsText('')
  }

  function handleParamsBlur() {
    try {
      const parsed = paramsText.trim() ? JSON.parse(paramsText) : {}
      onChange({ type, ...parsed })
    } catch {
      // leave as-is until valid
    }
  }

  return (
    <div className="flex gap-2 items-start bg-bg-primary border border-border rounded p-3">
      <select value={type} onChange={e => handleTypeChange(e.target.value)}
        className="bg-bg-secondary border border-border rounded px-2 py-1 text-xs text-text-primary shrink-0">
        {CONDITION_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
      </select>
      <textarea value={paramsText}
        onChange={e => setParamsText(e.target.value)}
        onBlur={handleParamsBlur}
        placeholder='{"label": "receipt", "min_confidence": 0.7}'
        rows={Math.max(1, paramsText.split('\n').length)}
        className="flex-1 bg-bg-secondary border border-border rounded px-2 py-1 text-xs text-text-primary font-mono resize-y min-h-[28px]" />
      <button onClick={onRemove}
        className="text-xs text-danger/60 hover:text-danger px-1 shrink-0">
        X
      </button>
    </div>
  )
}


function ActionRow({ action, onChange, onRemove }: {
  action: RuleAction; onChange: (a: RuleAction) => void; onRemove: () => void
}) {
  const [paramsText, setParamsText] = useState(
    Object.keys(action.params).length > 0 ? JSON.stringify(action.params, null, 2) : ''
  )

  function handleParamsBlur() {
    try {
      const parsed = paramsText.trim() ? JSON.parse(paramsText) : {}
      onChange({ ...action, params: parsed })
    } catch {
      // leave as-is
    }
  }

  return (
    <div className="flex gap-2 items-start bg-bg-primary border border-border rounded p-3">
      <select value={action.handler}
        onChange={e => onChange({ ...action, handler: e.target.value })}
        className="bg-bg-secondary border border-border rounded px-2 py-1 text-xs text-text-primary shrink-0">
        {HANDLER_NAMES.map(h => <option key={h} value={h}>{h}</option>)}
      </select>
      <textarea value={paramsText}
        onChange={e => setParamsText(e.target.value)}
        onBlur={handleParamsBlur}
        placeholder='{"document_type": "Receipt"}'
        rows={Math.max(1, paramsText.split('\n').length)}
        className="flex-1 bg-bg-secondary border border-border rounded px-2 py-1 text-xs text-text-primary font-mono resize-y min-h-[28px]" />
      <button onClick={onRemove}
        className="text-xs text-danger/60 hover:text-danger px-1 shrink-0">
        X
      </button>
    </div>
  )
}
