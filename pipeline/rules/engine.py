"""Rules engine — evaluates a classified Envelope against loaded rules."""

from __future__ import annotations

import logging
from typing import Any

from pipeline.audit.models import RuleTrace
from pipeline.models import Envelope
from pipeline.rules.conditions import evaluate_condition
from pipeline.rules.loader import ActionSpec, Rule, RulesLoader

log = logging.getLogger(__name__)


class FiredRule:
    """A rule that matched, along with its action specs."""

    def __init__(self, rule: Rule, action_specs: list[ActionSpec]) -> None:
        self.rule = rule
        self.action_specs = action_specs


class RulesEngine:
    """Evaluates an envelope against rules in priority order."""

    def __init__(self, loader: RulesLoader) -> None:
        self._loader = loader

    def evaluate(self, envelope: Envelope) -> tuple[list[FiredRule], list[RuleTrace]]:
        """Evaluate all rules against an envelope.

        Returns (fired_rules, traces). Respects on_match: stop/continue semantics.
        """
        fired: list[FiredRule] = []
        traces: list[RuleTrace] = []

        for rule in self._loader.rules:
            if not rule.enabled:
                continue

            conditions_met: list[str] = []
            conditions_failed: list[str] = []
            matched = True

            for condition in rule.conditions:
                result = evaluate_condition(condition.type, condition.params, envelope)
                if result:
                    conditions_met.append(condition.type)
                else:
                    conditions_failed.append(condition.type)
                    matched = False

            # Empty conditions list = always matches (catch-all)
            if not rule.conditions:
                matched = True

            traces.append(RuleTrace(
                rule_id=rule.id,
                rule_name=rule.name,
                matched=matched,
                conditions_met=conditions_met,
                conditions_failed=conditions_failed,
                on_match=rule.on_match if matched else None,
            ))

            if matched:
                log.info("Rule matched: %s (p%d, %s)", rule.name, rule.priority, rule.on_match)
                fired.append(FiredRule(rule=rule, action_specs=rule.actions))

                if rule.on_match == "stop":
                    break
                # continue = keep evaluating

        if not fired:
            log.info("No rules matched for envelope %s", envelope.id[:8])

        return fired, traces
