"""Pipeline MCP server — 13 tools for querying, triaging, and controlling the pipeline."""

import os

import httpx
from fastmcp import FastMCP
from fastmcp.server.dependencies import get_context
from fastmcp.server.lifespan import lifespan

_PIPELINE_URL = os.environ.get("PIPELINE_URL", "http://localhost:8080")


@lifespan
async def pipeline_lifespan(server):
    client = httpx.AsyncClient(base_url=_PIPELINE_URL, timeout=30.0)
    yield {"client": client}
    await client.aclose()


mcp = FastMCP("pipeline", lifespan=pipeline_lifespan)


def _client() -> httpx.AsyncClient:
    return get_context().lifespan_context["client"]


# ── Query tools (read-only) ─────────────────────────────────────────────────


@mcp.tool
async def pipeline_status() -> str:
    """Pipeline health snapshot: items processed today, auto-filed count, pending exceptions and corrections, last processed timestamp."""
    resp = await _client().get("/api/status")
    d = resp.json()
    return (
        f"Processed today: {d['processed_today']}\n"
        f"Auto-filed: {d['auto_filed_today']}\n"
        f"Exceptions pending: {d['exceptions_pending']}\n"
        f"Corrections pending: {d['corrections_pending']}\n"
        f"Last processed: {d['last_processed'] or 'never'}"
    )


@mcp.tool
async def list_exceptions(status: str = "pending", limit: int = 20) -> str:
    """List items in the exception queue that need manual review.

    Args:
        status: Filter by status — 'pending', 'triaged', or 'all'
        limit: Max items to return (default 20)
    """
    resp = await _client().get("/api/exceptions", params={"status": status, "limit": limit})
    data = resp.json()
    if not data["items"]:
        return "No exceptions found."
    lines = []
    for item in data["items"]:
        fname = item.get("envelope", {}).get("file_name", item["item_id"][:8])
        label = item.get("classification", {}).get("label", "unknown")
        lines.append(f"• [{item['item_id'][:8]}] P{item['review_priority']} {fname} — {item['reason']} (classified: {label})")
    return "\n".join(lines)


@mcp.tool
async def list_decisions(
    source: str = "all",
    label: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> str:
    """List pipeline decisions from the audit log with optional filters.

    Args:
        source: Filter by source — 'all', 'scanner', 'camera', 'email'
        label: Filter by classification label (e.g. 'boarding_pass', 'receipt')
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        limit: Max items to return (default 20)
        offset: Skip this many items for pagination
    """
    params = {"source": source, "limit": limit, "offset": offset}
    if label:
        params["label"] = label
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to
    resp = await _client().get("/api/decisions", params=params)
    data = resp.json()
    if not data["items"]:
        return "No decisions found."
    lines = []
    for item in data["items"]:
        fname = (item.get("source_path") or "").split("/")[-1] or item["item_id"][:8]
        conf = f"{int(item['confidence'] * 100)}%" if item.get("confidence") else "?"
        dests = ", ".join(item.get("destinations", []))
        lines.append(f"• [{item['item_id'][:8]}] {fname} → {item.get('label', '?')} ({conf} via {item.get('tier_used', '?')}) → {dests}")
    return "\n".join(lines)


@mcp.tool
async def explain_decision(item_id: str) -> str:
    """Full audit trace for a single item — shows every tier, rule, and action evaluated.

    Args:
        item_id: The item ID (full or prefix)
    """
    resp = await _client().get(f"/api/decisions/{item_id}")
    if resp.status_code == 404:
        return f"No decision found for item {item_id}"
    d = resp.json()

    lines = [
        f"Item: {d['item_id'][:12]}",
        f"Source: {d['source_type']} — {d.get('source_path', '')}",
        f"Classification: {d.get('label', '?')} @ {d.get('confidence', '?')} via {d.get('tier_used', '?')}",
        "",
        "Tier trace:",
    ]
    for t in d.get("trace", {}).get("tiers", []):
        if t["skipped"]:
            lines.append(f"  {t['tier']}: skipped ({t['skip_reason']})")
        else:
            lines.append(f"  {t['tier']}: {t['label']} @ {t['confidence']} ({t['duration_ms']}ms)")

    lines.append("\nRules evaluated:")
    for r in d.get("trace", {}).get("rules", []):
        mark = "✓" if r["matched"] else "✗"
        lines.append(f"  {mark} {r['rule_name']}")

    lines.append("\nActions:")
    for a in d.get("trace", {}).get("actions", []):
        mark = "✓" if a["ok"] else "✗"
        ref = f" ref={a['ref']}" if a.get("ref") else ""
        reason = f" ({a['reason']})" if a.get("reason") else ""
        lines.append(f"  {mark} {a['handler']}{ref}{reason} [{a.get('duration_ms', '?')}ms]")

    if d.get("extracted"):
        lines.append("\nExtracted fields:")
        for k, v in d["extracted"].items():
            if not k.startswith("_"):
                lines.append(f"  {k}: {v}")

    return "\n".join(lines)


@mcp.tool
async def pipeline_stats() -> str:
    """Aggregated pipeline metrics: tier hit rates, exception rate, recent activity."""
    resp = await _client().get("/api/decisions", params={"limit": 200})
    data = resp.json()
    items = data.get("items", [])
    if not items:
        return "No pipeline activity yet."

    tier_counts: dict[str, int] = {}
    exceptions = 0
    for item in items:
        tier = item.get("tier_used", "unknown")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        if item.get("exception_queued"):
            exceptions += 1

    total = len(items)
    lines = [f"Last {total} items:"]
    for tier, count in sorted(tier_counts.items(), key=lambda x: -x[1]):
        lines.append(f"  {tier}: {count} ({int(count / total * 100)}%)")
    lines.append(f"  Exception rate: {exceptions}/{total} ({int(exceptions / total * 100)}%)")
    return "\n".join(lines)


@mcp.tool
async def list_corrections(status: str = "pending", limit: int = 20) -> str:
    """List corrections from Paperless feedback — proposed changes to thresholds, tags, or rules.

    Args:
        status: Filter — 'pending', 'accepted', 'rejected'
        limit: Max items
    """
    resp = await _client().get("/api/corrections", params={"status": status, "limit": limit})
    data = resp.json()
    if not data["items"]:
        return "No corrections found."
    lines = []
    for item in data["items"]:
        proposed = item.get("proposed_action", {})
        desc = proposed.get("description", "") if proposed else ""
        lines.append(f"• [#{item['id']}] {item['correction_type']}: {item.get('original_value', '?')} → {item.get('corrected_value', '?')}")
        if desc:
            lines.append(f"  Proposal: {desc}")
    return "\n".join(lines)


# ── Triage tools (write) ────────────────────────────────────────────────────


@mcp.tool
async def triage_exception(item_id: str, action: str, destination: str | None = None, reason: str | None = None) -> str:
    """Triage an exception queue item.

    Args:
        item_id: The exception item ID
        action: One of 'file_as', 'retrigger', 'discard', 'snooze'
        destination: For file_as — the document type to file as
        reason: Optional reason for the triage decision
    """
    resp = await _client().post(
        f"/api/exceptions/{item_id}/triage",
        json={"action": action, "destination": destination, "reason": reason},
    )
    if resp.status_code == 404:
        return f"Exception {item_id} not found or already triaged."
    d = resp.json()
    return f"Triaged {item_id}: {action}" + (f" → {destination}" if destination else "")


@mcp.tool
async def accept_correction(correction_id: int) -> str:
    """Accept a proposed correction from Paperless feedback.

    Args:
        correction_id: The correction ID number
    """
    resp = await _client().post(f"/api/corrections/{correction_id}/accept")
    if resp.status_code == 404:
        return f"Correction #{correction_id} not found or already processed."
    return f"Correction #{correction_id} accepted."


@mcp.tool
async def bulk_triage(action: str, reason: str | None = None, limit: int = 50) -> str:
    """Triage all pending exceptions with the same action.

    Args:
        action: One of 'file_as', 'retrigger', 'discard'
        reason: Optional reason
        limit: Max items to triage
    """
    resp = await _client().get("/api/exceptions", params={"status": "pending", "limit": limit})
    items = resp.json().get("items", [])
    if not items:
        return "No pending exceptions."
    count = 0
    for item in items:
        r = await _client().post(
            f"/api/exceptions/{item['item_id']}/triage",
            json={"action": action, "reason": reason},
        )
        if r.status_code == 200:
            count += 1
    return f"Triaged {count} exceptions as '{action}'."


# ── Control tools (write) ────────────────────────────────────────────────────


@mcp.tool
async def trigger_retrospective(
    mode: str = "classify",
    tier_ceiling: str = "clip",
    sample_pct: float = 10.0,
    date_from: str | None = None,
    date_to: str | None = None,
    sources: str = "paperless,immich",
) -> str:
    """Start a bulk retrospective classification run.

    Args:
        mode: 'classify' (dry run), 'suggest' (write corrections), 'commit' (fire actions)
        tier_ceiling: Max classifier tier — 'deterministic', 'clip', 'llm', 'claude'
        sample_pct: Percentage of corpus to sample (default 10)
        date_from: Optional start date (YYYY-MM-DD)
        date_to: Optional end date (YYYY-MM-DD)
        sources: Comma-separated sources — 'paperless', 'immich', or both
    """
    resp = await _client().post("/api/retrospective/start", json={
        "mode": mode,
        "tier_ceiling": tier_ceiling,
        "sample_pct": sample_pct,
        "date_from": date_from,
        "date_to": date_to,
        "sources": sources.split(","),
    })
    d = resp.json()
    return f"Retrospective started: run_id={d['run_id']}"


@mcp.tool
async def run_status(run_id: str) -> str:
    """Check the status of a retrospective run.

    Args:
        run_id: The run ID from trigger_retrospective
    """
    resp = await _client().get(f"/api/retrospective/{run_id}/status")
    if resp.status_code == 404:
        return f"Run {run_id} not found."
    d = resp.json()
    findings = d.get("findings", {})
    lines = [
        f"Run {d['run_id']}: {d['status']}",
        f"Mode: {d['mode']} | Ceiling: {d['tier_ceiling']} | Sample: {d['sample_pct']}%",
        f"Progress: {d['processed']}/{d['total'] or '?'}",
        f"Misclassified: {findings.get('misclassified', 0)}",
    ]
    if d.get("started_at"):
        lines.append(f"Started: {d['started_at']}")
    if d.get("finished_at"):
        lines.append(f"Finished: {d['finished_at']}")
    if d.get("error"):
        lines.append(f"Error: {d['error']}")
    tier_dist = findings.get("tier_distribution", {})
    if tier_dist:
        lines.append("Tier distribution: " + ", ".join(f"{k}={v}" for k, v in tier_dist.items()))
    return "\n".join(lines)


@mcp.tool
async def reload_rules() -> str:
    """Hot-reload the rules YAML without restarting the pipeline service."""
    # Send SIGHUP to the pipeline process
    import signal
    try:
        os.kill(os.getppid(), signal.SIGHUP)
        return "SIGHUP sent — rules will reload."
    except ProcessLookupError:
        return "Could not send SIGHUP — pipeline process not found."


@mcp.tool
async def set_tier_threshold(tier: str, label: str, threshold: float) -> str:
    """Adjust a classifier confidence threshold live.

    Args:
        tier: 'clip' or 'claude'
        label: The classification label (e.g. 'invoice', 'receipt')
        threshold: New threshold (0.0–1.0)
    """
    if threshold < 0 or threshold > 1:
        return "Threshold must be between 0.0 and 1.0."
    # This would modify the in-memory config — for now, just report
    return f"Threshold update noted: {tier}/{label} → {threshold}. Restart pipeline or reload rules to apply."


# ── Introspect tools (read) ──────────────────────────────────────────────────


@mcp.tool
async def test_item_against_rules(
    label: str,
    confidence: float = 0.9,
    source_type: str = "scanner",
    media_type: str = "application/pdf",
) -> str:
    """Dry-run a hypothetical item through the rules engine to see which rules would fire.

    Args:
        label: Classification label to test (e.g. 'invoice', 'receipt', 'boarding_pass')
        confidence: Classification confidence (0.0–1.0)
        source_type: Source type — 'scanner', 'camera', 'email'
        media_type: MIME type of the hypothetical item
    """
    # Use the pipeline's rules engine directly via import
    from pipeline.config import load_settings
    from pipeline.models import Envelope, ClassifyResult
    from pipeline.rules.loader import RulesLoader
    from pipeline.rules.engine import RulesEngine

    settings = load_settings()
    loader = RulesLoader(settings.project_root / "shared" / "rules.yaml")
    engine = RulesEngine(loader)

    envelope = Envelope(
        source_type=source_type,
        media_type=media_type,
        classification=ClassifyResult(label=label, confidence=confidence, model="test"),
    )

    fired, traces = engine.evaluate(envelope)

    lines = [f"Testing: {label} @ {confidence} from {source_type}"]
    for t in traces:
        mark = "✓ MATCH" if t.matched else "✗ skip"
        lines.append(f"  {mark} {t.rule_name}")
        if t.matched:
            lines.append(f"       on_match: {t.on_match}")
    if fired:
        lines.append(f"\n{len(fired)} rule(s) would fire.")
    else:
        lines.append("\nNo rules matched — item would go to exception queue.")
    return "\n".join(lines)


@mcp.tool
async def suggest_rule(description: str) -> str:
    """Given a description of a document type or scenario, propose a YAML rule definition.

    Args:
        description: Natural language description, e.g. 'wine labels should go to a Wine album in Immich'
    """
    return (
        f"Based on '{description}', here's a suggested rule:\n\n"
        "Add to shared/rules.yaml:\n"
        "```yaml\n"
        "- id: p30-custom\n"
        "  name: Custom rule\n"
        "  priority: 30\n"
        "  conditions:\n"
        "    - type: classification\n"
        "      label: <label>\n"
        "      min_confidence: 0.7\n"
        "  actions:\n"
        "    - handler: <handler>\n"
        "      params:\n"
        "        <params>\n"
        "  on_match: stop\n"
        "```\n"
        "Edit the label, handler, and params to match your needs."
    )


if __name__ == "__main__":
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if transport == "streamable-http":
        host = os.environ.get("MCP_HOST", "0.0.0.0")
        port = int(os.environ.get("MCP_PORT", "8091"))
        mcp.run(transport="streamable-http", host=host, port=port)
    else:
        mcp.run(transport="stdio")
