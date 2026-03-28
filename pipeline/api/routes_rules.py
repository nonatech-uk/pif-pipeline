"""Rules management API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from pipeline.api.deps import get_rules_loader

router = APIRouter()


class ConditionBody(BaseModel):
    type: str
    params: dict[str, Any] = Field(default_factory=dict)


class ActionBody(BaseModel):
    handler: str
    params: dict[str, Any] = Field(default_factory=dict)


class RuleBody(BaseModel):
    id: str
    name: str
    priority: int = 50
    conditions: list[ConditionBody] = Field(default_factory=list)
    actions: list[ActionBody] = Field(default_factory=list)
    on_match: str = "stop"
    enabled: bool = True


@router.post("/rules/reload")
async def reload_rules():
    """Reload rules from the YAML file."""
    loader = get_rules_loader()
    loader.reload()
    return {"ok": True, "rules_loaded": len(loader.rules)}


@router.get("/rules")
async def list_rules():
    """List all rules in priority order."""
    loader = get_rules_loader()
    return {
        "items": [_rule_to_dict(r) for r in loader.rules],
        "total": len(loader.rules),
    }


@router.get("/rules/{rule_id}")
async def get_rule(rule_id: str):
    """Get a single rule by ID."""
    loader = get_rules_loader()
    for r in loader.rules:
        if r.id == rule_id:
            return _rule_to_dict(r)
    raise HTTPException(404, "Rule not found")


@router.post("/rules")
async def create_rule(body: RuleBody):
    """Create a new rule."""
    loader = get_rules_loader()

    # Check for duplicate ID
    for r in loader.rules:
        if r.id == body.id:
            raise HTTPException(409, f"Rule '{body.id}' already exists")

    _save_rule(loader, body, is_new=True)
    return {"ok": True, "id": body.id}


@router.put("/rules/{rule_id}")
async def update_rule(rule_id: str, body: RuleBody):
    """Update an existing rule."""
    loader = get_rules_loader()

    found = False
    for r in loader.rules:
        if r.id == rule_id:
            found = True
            break

    if not found:
        raise HTTPException(404, "Rule not found")

    # If ID changed, check for conflict
    if body.id != rule_id:
        for r in loader.rules:
            if r.id == body.id:
                raise HTTPException(409, f"Rule '{body.id}' already exists")

    _save_rule(loader, body, old_id=rule_id)
    return {"ok": True, "id": body.id}


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: str):
    """Delete a rule."""
    loader = get_rules_loader()

    found = False
    for r in loader.rules:
        if r.id == rule_id:
            found = True
            break

    if not found:
        raise HTTPException(404, "Rule not found")

    _delete_rule(loader, rule_id)
    return {"ok": True}


@router.post("/rules/{rule_id}/toggle")
async def toggle_rule(rule_id: str):
    """Toggle a rule's enabled state."""
    loader = get_rules_loader()

    for r in loader.rules:
        if r.id == rule_id:
            body = RuleBody(
                id=r.id,
                name=r.name,
                priority=r.priority,
                conditions=[ConditionBody(type=c.type, params=c.params) for c in r.conditions],
                actions=[ActionBody(handler=a.handler, params=a.params) for a in r.actions],
                on_match=r.on_match,
                enabled=not r.enabled,
            )
            _save_rule(loader, body, old_id=rule_id)
            return {"ok": True, "enabled": body.enabled}

    raise HTTPException(404, "Rule not found")


def _rule_to_dict(r) -> dict:
    return {
        "id": r.id,
        "name": r.name,
        "priority": r.priority,
        "conditions": [{"type": c.type, **c.params} for c in r.conditions],
        "actions": [{"handler": a.handler, "params": a.params} for a in r.actions],
        "on_match": r.on_match,
        "enabled": r.enabled,
    }


def _save_rule(loader, body: RuleBody, is_new: bool = False, old_id: str | None = None) -> None:
    """Write a rule to the YAML file and reload."""
    import yaml

    path = loader._path
    raw = yaml.safe_load(path.read_text()) or {}
    rules_list = raw.get("rules", [])

    rule_dict = {
        "id": body.id,
        "name": body.name,
        "priority": body.priority,
        "conditions": [{"type": c.type, **c.params} for c in body.conditions],
        "actions": [{"handler": a.handler, "params": a.params} for a in body.actions],
        "on_match": body.on_match,
        "enabled": body.enabled,
    }

    if is_new:
        rules_list.append(rule_dict)
    else:
        target_id = old_id or body.id
        for i, entry in enumerate(rules_list):
            if entry.get("id") == target_id:
                rules_list[i] = rule_dict
                break

    # Sort by priority
    rules_list.sort(key=lambda r: r.get("priority", 50))
    raw["rules"] = rules_list

    # Preserve the header comment
    header = ""
    text = path.read_text()
    for line in text.splitlines():
        if line.startswith("#"):
            header += line + "\n"
        else:
            break

    yaml_body = yaml.dump(raw, default_flow_style=False, allow_unicode=True, sort_keys=False)
    path.write_text(header + "\n" + yaml_body)
    loader.reload()


def _delete_rule(loader, rule_id: str) -> None:
    """Remove a rule from the YAML file and reload."""
    import yaml

    path = loader._path
    raw = yaml.safe_load(path.read_text()) or {}
    rules_list = raw.get("rules", [])
    raw["rules"] = [r for r in rules_list if r.get("id") != rule_id]

    header = ""
    text = path.read_text()
    for line in text.splitlines():
        if line.startswith("#"):
            header += line + "\n"
        else:
            break

    yaml_body = yaml.dump(raw, default_flow_style=False, allow_unicode=True, sort_keys=False)
    path.write_text(header + "\n" + yaml_body)
    loader.reload()
