import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiFetch } from './client'
import type { PipelineStatus, ExceptionItem, DecisionItem, DecisionDetail } from './types'

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
