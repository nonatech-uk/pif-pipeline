import { useState } from 'react'
import { useRules, useToggleRule, useDeleteRule } from '../api/hooks'
import type { Rule } from '../api/types'
import RuleEditor from './RuleEditor'

const ON_MATCH_LABEL: Record<string, string> = { stop: 'Stop', continue: 'Continue' }
const HANDLER_COLORS: Record<string, string> = {
  paperless: 'bg-blue-900/40 text-blue-300',
  finance: 'bg-green-900/40 text-green-300',
  immich_album: 'bg-purple-900/40 text-purple-300',
  immich_tag: 'bg-purple-900/40 text-purple-300',
  location: 'bg-yellow-900/40 text-yellow-300',
  notify: 'bg-gray-700/40 text-gray-300',
  exception_queue: 'bg-red-900/40 text-red-300',
}

export default function RulesList() {
  const { data, isLoading } = useRules()
  const toggleRule = useToggleRule()
  const deleteRule = useDeleteRule()
  const [editing, setEditing] = useState<Rule | null>(null)
  const [creating, setCreating] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)

  if (isLoading) return <div className="text-text-secondary text-sm py-8 text-center">Loading rules...</div>

  const rules = data?.items ?? []

  if (editing || creating) {
    return (
      <RuleEditor
        rule={editing}
        onClose={() => { setEditing(null); setCreating(false) }}
      />
    )
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-sm font-medium text-text-secondary uppercase tracking-wide">
          Rules ({rules.length})
        </h2>
        <button
          onClick={() => setCreating(true)}
          className="text-xs px-3 py-1.5 rounded bg-accent/20 text-accent hover:bg-accent/30 transition-colors"
        >
          + New Rule
        </button>
      </div>

      <div className="space-y-2">
        {rules.map(rule => (
          <div
            key={rule.id}
            className={`bg-bg-card border border-border rounded-lg p-4 transition-opacity ${
              rule.enabled ? '' : 'opacity-40'
            }`}
          >
            <div className="flex items-start justify-between gap-4">
              {/* Left: priority + name + meta */}
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-xs font-mono text-text-secondary bg-bg-hover px-1.5 py-0.5 rounded">
                    P{rule.priority}
                  </span>
                  <span className="font-medium text-text-primary truncate">{rule.name}</span>
                  <span className={`text-xs px-1.5 py-0.5 rounded ${
                    rule.on_match === 'stop'
                      ? 'bg-red-900/30 text-red-400'
                      : 'bg-green-900/30 text-green-400'
                  }`}>
                    {ON_MATCH_LABEL[rule.on_match] ?? rule.on_match}
                  </span>
                </div>

                {/* Conditions summary */}
                <div className="flex flex-wrap gap-1.5 mt-2">
                  {rule.conditions.map((c, i) => (
                    <span key={i} className="text-xs bg-bg-hover text-text-secondary px-2 py-0.5 rounded">
                      {_conditionSummary(c)}
                    </span>
                  ))}
                  {rule.conditions.length === 0 && (
                    <span className="text-xs text-text-secondary italic">No conditions (always matches)</span>
                  )}
                </div>

                {/* Actions */}
                <div className="flex flex-wrap gap-1.5 mt-2">
                  {rule.actions.map((a, i) => (
                    <span key={i} className={`text-xs px-2 py-0.5 rounded ${
                      HANDLER_COLORS[a.handler] ?? 'bg-bg-hover text-text-secondary'
                    }`}>
                      {a.handler}
                    </span>
                  ))}
                </div>
              </div>

              {/* Right: actions */}
              <div className="flex items-center gap-1 shrink-0">
                <button
                  onClick={() => toggleRule.mutate(rule.id)}
                  className={`text-xs px-2 py-1 rounded transition-colors ${
                    rule.enabled
                      ? 'text-success hover:bg-success/10'
                      : 'text-text-secondary hover:bg-bg-hover'
                  }`}
                  title={rule.enabled ? 'Disable' : 'Enable'}
                >
                  {rule.enabled ? 'ON' : 'OFF'}
                </button>
                <button
                  onClick={() => setEditing(rule)}
                  className="text-xs px-2 py-1 rounded text-accent hover:bg-accent/10 transition-colors"
                >
                  Edit
                </button>
                {confirmDelete === rule.id ? (
                  <div className="flex gap-1">
                    <button
                      onClick={() => { deleteRule.mutate(rule.id); setConfirmDelete(null) }}
                      className="text-xs px-2 py-1 rounded bg-danger/20 text-danger hover:bg-danger/30"
                    >
                      Confirm
                    </button>
                    <button
                      onClick={() => setConfirmDelete(null)}
                      className="text-xs px-2 py-1 rounded text-text-secondary hover:bg-bg-hover"
                    >
                      Cancel
                    </button>
                  </div>
                ) : (
                  <button
                    onClick={() => setConfirmDelete(rule.id)}
                    className="text-xs px-2 py-1 rounded text-danger/60 hover:text-danger hover:bg-danger/10 transition-colors"
                  >
                    Del
                  </button>
                )}
              </div>
            </div>

            {/* Rule ID */}
            <div className="mt-2 text-xs text-text-secondary font-mono">{rule.id}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

function _conditionSummary(c: Record<string, unknown>): string {
  const { type, ...rest } = c
  switch (type) {
    case 'classification': {
      const label = Array.isArray(rest.label) ? rest.label.join('|') : rest.label
      const conf = rest.min_confidence ? ` >= ${rest.min_confidence}` : ''
      return `label: ${label}${conf}`
    }
    case 'media_type':
      return `mime: ${rest.value}`
    case 'source_type':
      return `source: ${Array.isArray(rest.value) ? rest.value.join('|') : rest.value}`
    case 'gps_proximity':
      return `GPS: ${rest.lat},${rest.lng} r${rest.radius_km ?? 1}km`
    case 'pet_recognition':
      return `pet: ${rest.pet}${rest.min_confidence ? ` >= ${rest.min_confidence}` : ''}`
    case 'date_range':
      return `date: ${rest.from ?? '...'} - ${rest.to ?? '...'}`
    case 'empty':
      return 'always'
    default:
      return `${type}: ${JSON.stringify(rest)}`
  }
}
