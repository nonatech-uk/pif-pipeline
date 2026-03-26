import { useState } from 'react'

interface Props {
  itemId: string
  mediaType: string | null
  context: 'decision' | 'exception'
}

export default function DocumentPreview({ itemId, mediaType, context }: Props) {
  const [error, setError] = useState(false)
  const [expanded, setExpanded] = useState(false)
  const previewUrl = `/api/preview/${itemId}?context=${context === 'exception' ? 'exception' : 'audit'}`
  const fullUrl = `${previewUrl}&size=full`
  const mt = mediaType ?? ''

  if (error) {
    return (
      <div className="bg-bg-hover rounded p-4 text-center text-xs text-text-secondary mb-4">
        No preview available
      </div>
    )
  }

  const isImage = mt.startsWith('image/')
  const isPdf = mt === 'application/pdf'

  return (
    <div className="mb-4">
      <div
        className={`bg-bg-hover rounded overflow-hidden border border-border cursor-pointer transition-all ${
          expanded ? 'max-h-[80vh]' : 'max-h-[300px]'
        }`}
        onClick={() => setExpanded(!expanded)}
      >
        {isImage && (
          <img
            src={previewUrl}
            alt="Document preview"
            className="w-full object-contain max-h-[inherit]"
            onError={() => setError(true)}
          />
        )}
        {isPdf && (
          <iframe
            src={previewUrl}
            className="w-full border-none"
            style={{ height: expanded ? '80vh' : '300px' }}
            onError={() => setError(true)}
          />
        )}
        {!isImage && !isPdf && (
          <iframe
            src={previewUrl}
            className="w-full border-none bg-white"
            style={{ height: expanded ? '80vh' : '300px' }}
            sandbox="allow-same-origin"
            onError={() => setError(true)}
          />
        )}
      </div>
      <div className="flex justify-between mt-1">
        <button
          className="text-[11px] text-accent bg-transparent border-none cursor-pointer hover:underline"
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? 'Collapse' : 'Expand'}
        </button>
        <a
          href={fullUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="text-[11px] text-accent no-underline hover:underline"
        >
          Open full
        </a>
      </div>
    </div>
  )
}
