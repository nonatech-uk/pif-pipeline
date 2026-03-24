export interface PipelineStatus {
  processed_today: number
  auto_filed_today: number
  exceptions_pending: number
  corrections_pending: number
  last_processed: string | null
}

export interface ExceptionItem {
  item_id: string
  reason: string
  review_priority: number
  classification: Record<string, unknown>
  envelope: Record<string, unknown>
  created_at: string
  status: string
}

export interface DecisionItem {
  item_id: string
  timestamp: string
  source_type: string
  source_path: string | null
  file_sha256: string | null
  media_type: string | null
  label: string | null
  confidence: number | null
  tier_used: string | null
  destinations: string[]
  exception_queued: boolean
  extracted: Record<string, unknown>
}

export interface TierTrace {
  tier: string
  label: string | null
  confidence: number | null
  all_labels: Record<string, number>
  skipped: boolean
  skip_reason: string | null
  duration_ms: number | null
}

export interface RuleTrace {
  rule_id: string
  rule_name: string
  matched: boolean
  conditions_met: string[]
  conditions_failed: string[]
  on_match: string | null
}

export interface ActionTrace {
  handler: string
  destination: string
  ok: boolean
  ref: string | null
  reason: string | null
  duration_ms: number | null
}

export interface DecisionDetail extends DecisionItem {
  trace: {
    tiers: TierTrace[]
    rules: RuleTrace[]
    actions: ActionTrace[]
  }
}

export interface RuleCondition {
  type: string
  [key: string]: unknown
}

export interface RuleAction {
  handler: string
  params: Record<string, unknown>
}

export interface Rule {
  id: string
  name: string
  priority: number
  conditions: RuleCondition[]
  actions: RuleAction[]
  on_match: 'stop' | 'continue'
  enabled: boolean
}
