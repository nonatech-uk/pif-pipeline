import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiFetch } from './client'
import type { PipelineStatus, ExceptionItem, DecisionItem, DecisionDetail, Rule, CorrectionItem } from './types'

const POLL_MS = 10_000

export function useStatus() {
  return useQuery<PipelineStatus>({
    queryKey: ['status'],
    queryFn: () => apiFetch('/status'),
    refetchInterval: POLL_MS,
  })
}

export function useExceptions(status = 'pending') {
  return useQuery<{ items: ExceptionItem[]; total: number }>({
    queryKey: ['exceptions', status],
    queryFn: () => apiFetch(`/exceptions?status=${status}&limit=20`),
    refetchInterval: POLL_MS,
  })
}

export function useDecisions(source = 'all') {
  return useQuery<{ items: DecisionItem[]; total: number }>({
    queryKey: ['decisions', source],
    queryFn: () => apiFetch(`/decisions?source=${source}&limit=50`),
    refetchInterval: POLL_MS,
  })
}

export function useDecisionDetail(itemId: string | null) {
  return useQuery<DecisionDetail>({
    queryKey: ['decision', itemId],
    queryFn: () => apiFetch(`/decisions/${itemId}`),
    enabled: !!itemId,
  })
}

export function useRules() {
  return useQuery<{ items: Rule[]; total: number }>({
    queryKey: ['rules'],
    queryFn: () => apiFetch('/rules'),
  })
}

export function useCreateRule() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (rule: Rule) => apiFetch('/rules', {
      method: 'POST',
      body: JSON.stringify({
        ...rule,
        conditions: rule.conditions.map(c => {
          const { type, ...params } = c
          return { type, params }
        }),
      }),
    }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['rules'] }),
  })
}

export function useUpdateRule() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, rule }: { id: string; rule: Rule }) => apiFetch(`/rules/${id}`, {
      method: 'PUT',
      body: JSON.stringify({
        ...rule,
        conditions: rule.conditions.map(c => {
          const { type, ...params } = c
          return { type, params }
        }),
      }),
    }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['rules'] }),
  })
}

export function useDeleteRule() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => apiFetch(`/rules/${id}`, { method: 'DELETE' }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['rules'] }),
  })
}

export function useToggleRule() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => apiFetch(`/rules/${id}/toggle`, { method: 'POST' }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['rules'] }),
  })
}

export function useExceptionDetail(itemId: string | null) {
  return useQuery<ExceptionItem>({
    queryKey: ['exception', itemId],
    queryFn: () => apiFetch(`/exceptions/${itemId}`),
    enabled: !!itemId,
  })
}

export function useCorrections(status = 'pending') {
  return useQuery<{ items: CorrectionItem[]; total: number }>({
    queryKey: ['corrections', status],
    queryFn: () => apiFetch(`/corrections?status=${status}`),
    refetchInterval: POLL_MS,
  })
}

export function useCreateCorrection() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (data: { item_id: string; corrections: { field: string; original: string; corrected: string }[] }) =>
      apiFetch('/corrections', { method: 'POST', body: JSON.stringify(data) }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['corrections'] })
      qc.invalidateQueries({ queryKey: ['status'] })
    },
  })
}

export function useAcceptCorrection() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => apiFetch(`/corrections/${id}/accept`, { method: 'POST' }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['corrections'] }),
  })
}

export function useRejectCorrection() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => apiFetch(`/corrections/${id}/reject`, { method: 'POST' }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['corrections'] }),
  })
}

export function useTriageException() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ itemId, action, destination, reason }: {
      itemId: string
      action: string
      destination?: string
      reason?: string
    }) => apiFetch(`/exceptions/${itemId}/triage`, {
      method: 'POST',
      body: JSON.stringify({ action, destination, reason }),
    }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['exceptions'] })
      qc.invalidateQueries({ queryKey: ['status'] })
    },
  })
}
