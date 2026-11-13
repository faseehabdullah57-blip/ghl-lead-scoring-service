"""Standalone, configurable Lead Scoring Rules Engine for GoHighLevel events."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from src.models.lead_event import LeadEvent, RuleMatch, ScoringResult


def _normalize_token(value: str) -> str:
    """Normalize a string token for case-insensitive comparison."""
    return re.sub(r"[\s_\-]+", " ", str(value).strip().lower())


def _parse_numeric_budget(raw_val: Any) -> Optional[float]:
    """Parse numeric budget values such as 6000, '$7,500', '10k', or '$5.5k'."""
    if isinstance(raw_val, bool):
        return None
    if isinstance(raw_val, (int, float)):
        return float(raw_val)
    if not isinstance(raw_val, str):
        return None

    text = raw_val.strip().lower().replace(",", "")
    if not text:
        return None

    # Reject explicit '<' or 'under' / 'below' expressions
    if any(prefix in text for prefix in ("<", "under", "below", "less than")):
        return None

    # Handle range like "5000-10000" or "$5k - $10k" by extracting the first or max value
    k_match = re.search(r"(\d+(?:\.\d+)?)\s*k\b", text)
    if k_match:
        val = float(k_match.group(1)) * 1000.0
        if ">" in text or "+" in text or "over" in text or "above" in text:
            return max(val + 1.0, val)
        return val

    num_match = re.search(r"(\d+(?:\.\d+)?)", text)
    if num_match:
        val = float(num_match.group(1))
        if (">" in text or "+" in text or "over" in text or "above" in text) and val == 5000.0:
            return 5001.0
        return val

    return None


def is_budget_over_5k(event: LeadEvent) -> Tuple[bool, str]:
    """Evaluate whether a LeadEvent indicates a budget greater than $5,000."""
    # 1. Check custom fields
    for key, val in event.custom_fields.items():
        norm_key = _normalize_token(key)

        # Direct custom field named "Budget > $5k"
        if norm_key in ("budget > $5k", "budget > 5k", "budget over 5k", "budget_over_5k"):
            if isinstance(val, bool) and val:
                return True, f"Custom field '{key}' is True"
            if isinstance(val, str) and _normalize_token(val) in (
                "true",
                "yes",
                "1",
                "qualified",
                "budget > $5k",
                "> $5k",
                ">$5k",
            ):
                return True, f"Custom field '{key}'='{val}'"
            if isinstance(val, (int, float)) and val > 0:
                return True, f"Custom field '{key}'={val}"

        # Generic "budget" custom field
        if "budget" in norm_key:
            if isinstance(val, str):
                clean_val = val.strip().lower()
                if any(
                    pattern in clean_val
                    for pattern in (
                        "> $5k",
                        ">$5k",
                        "> 5k",
                        ">5k",
                        "> $5,000",
                        ">$5,000",
                        "> 5000",
                        "$5k+",
                        "$5,000+",
                        "5k+",
                        "5000+",
                        "over $5k",
                        "over 5k",
                        "over $5,000",
                        "above $5k",
                        "budget > $5k",
                    )
                ):
                    return True, f"Custom field '{key}'='{val}'"

            numeric_budget = _parse_numeric_budget(val)
            if numeric_budget is not None and numeric_budget > 5000.0:
                return True, f"Custom field '{key}'={val} (> $5,000)"

    # 2. Check if "Budget > $5k" was passed as an activity or tag signal
    for signal in (*event.activities, *event.tags):
        if _normalize_token(signal) in ("budget > $5k", "budget > 5k", "budget over 5k"):
            return True, f"Signal matched: '{signal}'"

    return False, ""


@dataclass(frozen=True)
class ScoringRule:
    """Immutable definition of a single lead scoring rule."""

    rule_id: str
    name: str
    points: int
    description: str = ""
    rule_type: str = "activity"  # "activity", "custom_field", "tag", or "custom"
    aliases: Tuple[str, ...] = field(default_factory=tuple)
    field_key: Optional[str] = None
    expected_value: Optional[Any] = None
    condition: Optional[Callable[[LeadEvent], Tuple[bool, str]]] = None
    enabled: bool = True

    @property
    def category(self) -> str:
        """Return 'positive' for >= 0 points, 'negative' for < 0 points."""
        return "positive" if self.points >= 0 else "negative"

    def evaluate(self, event: LeadEvent) -> Optional[RuleMatch]:
        """Evaluate this rule against a LeadEvent and return a RuleMatch if triggered."""
        if not self.enabled:
            return None

        # 1. Custom callable predicate takes precedence if provided
        if self.condition is not None:
            matched, reason = self.condition(event)
            if matched:
                return RuleMatch(
                    rule_id=self.rule_id,
                    rule_name=self.name,
                    points=self.points,
                    reason=reason or f"Rule matched: {self.name}",
                    category=self.category,
                )
            return None

        # 2. Custom field key/value rule
        if self.rule_type == "custom_field" and self.field_key:
            target_key = _normalize_token(self.field_key)
            for k, v in event.custom_fields.items():
                if _normalize_token(k) == target_key:
                    if self.expected_value is None and v:
                        return RuleMatch(
                            rule_id=self.rule_id,
                            rule_name=self.name,
                            points=self.points,
                            reason=f"Custom field '{k}' present with value '{v}'",
                            category=self.category,
                        )
                    if (
                        self.expected_value is not None
                        and _normalize_token(str(v)) == _normalize_token(str(self.expected_value))
                    ):
                        return RuleMatch(
                            rule_id=self.rule_id,
                            rule_name=self.name,
                            points=self.points,
                            reason=f"Custom field '{k}' matched '{v}'",
                            category=self.category,
                        )
            return None

        # 3. Activity / Event / Tag matching
        candidate_tokens = {
            _normalize_token(self.name),
            _normalize_token(self.rule_id),
            *(_normalize_token(a) for a in self.aliases),
        }

        for activity in event.activities:
            if _normalize_token(activity) in candidate_tokens:
                return RuleMatch(
                    rule_id=self.rule_id,
                    rule_name=self.name,
                    points=self.points,
                    reason=f"Activity matched: {self.name}",
                    category=self.category,
                )

        for tag in event.tags:
            if _normalize_token(tag) in candidate_tokens:
                return RuleMatch(
                    rule_id=self.rule_id,
                    rule_name=self.name,
                    points=self.points,
                    reason=f"Tag matched: {self.name}",
                    category=self.category,
                )

        return None


DEFAULT_SCORING_RULES: Tuple[ScoringRule, ...] = (
    ScoringRule(
        rule_id="booked_appointment",
        name="Booked Appointment",
        points=20,
        description="Lead scheduled a discovery or sales appointment.",
        rule_type="activity",
        aliases=("appointment_booked", "appointment booked", "AppointmentCreate"),
    ),
    ScoringRule(
        rule_id="clicked_sms_link",
        name="Clicked SMS Link",
        points=10,
        description="Lead clicked a tracked link inside an SMS campaign.",
        rule_type="activity",
        aliases=("sms_link_clicked", "sms clicked", "TriggerLinkClicked"),
    ),
    ScoringRule(
        rule_id="form_submitted",
        name="Form Submitted",
        points=15,
        description="Lead completed and submitted a GHL funnel/website form.",
        rule_type="activity",
        aliases=("form_submission", "survey_submitted", "FormSubmitted"),
    ),
    ScoringRule(
        rule_id="budget_over_5k",
        name="Budget > $5k",
        points=5,
        description="Lead custom field indicates budget exceeds $5,000.",
        rule_type="custom_field",
        field_key="budget",
        condition=is_budget_over_5k,
    ),
    ScoringRule(
        rule_id="unsubscribed",
        name="Unsubscribed",
        points=-15,
        description="Lead opted out / unsubscribed from communications.",
        rule_type="activity",
        aliases=("opt_out", "opted_out", "dnd_enabled", "unsubscribe"),
    ),
    ScoringRule(
        rule_id="no_show",
        name="No Show",
        points=-10,
        description="Lead missed a scheduled appointment without cancelling.",
        rule_type="activity",
        aliases=("appointment_no_show", "no-show", "noshow"),
    ),
)


RuleOverrideValue = Union[int, Dict[str, Any], ScoringRule]


class LeadScoringEngine:
    """Configurable rules engine for evaluating GoHighLevel lead events."""

    def __init__(
        self,
        rules: Optional[Iterable[ScoringRule]] = None,
        rule_overrides: Optional[Dict[str, RuleOverrideValue]] = None,
    ) -> None:
        base_rules = list(rules) if rules is not None else list(DEFAULT_SCORING_RULES)
        self._rules: List[ScoringRule] = list(base_rules)
        if rule_overrides:
            self.apply_overrides(rule_overrides)

    @property
    def rules(self) -> List[ScoringRule]:
        """Return a copy of currently configured scoring rules."""
        return list(self._rules)

    def get_rule(self, identifier: str) -> Optional[ScoringRule]:
        """Find a rule by rule_id or human-readable name (case-insensitive)."""
        norm_id = _normalize_token(identifier)
        for rule in self._rules:
            if _normalize_token(rule.rule_id) == norm_id or _normalize_token(rule.name) == norm_id:
                return rule
        return None

    def register_rule(self, rule: ScoringRule) -> None:
        """Register a new rule or replace an existing rule with the same ID/name."""
        norm_id = _normalize_token(rule.rule_id)
        norm_name = _normalize_token(rule.name)
        for idx, existing in enumerate(self._rules):
            if (
                _normalize_token(existing.rule_id) == norm_id
                or _normalize_token(existing.name) == norm_name
            ):
                self._rules[idx] = rule
                return
        self._rules.append(rule)

    def disable_rule(self, identifier: str) -> bool:
        """Disable a rule by ID or name. Returns True if found and disabled."""
        norm_id = _normalize_token(identifier)
        for idx, existing in enumerate(self._rules):
            if (
                _normalize_token(existing.rule_id) == norm_id
                or _normalize_token(existing.name) == norm_id
            ):
                self._rules[idx] = replace(existing, enabled=False)
                return True
        return False

    def apply_overrides(self, overrides: Dict[str, RuleOverrideValue]) -> None:
        """Apply custom rule point overrides, attribute overrides, or add new rules.

        Examples of supported override formats:
        - ``{"Booked Appointment": 30}`` (updates points for an existing rule)
        - ``{"booked_appointment": {"points": 25, "enabled": True}}``
        - ``{"Attended Webinar": 12}`` (registers a new activity/tag rule dynamically)
        - ``{"custom_rule": ScoringRule(...)}`` (registers a full ScoringRule instance)
        """
        for key, override in overrides.items():
            if isinstance(override, ScoringRule):
                self.register_rule(override)
                continue

            existing = self.get_rule(key)
            if existing is not None:
                if isinstance(override, int):
                    updated = replace(existing, points=override)
                elif isinstance(override, dict):
                    allowed_fields = {
                        k: v
                        for k, v in override.items()
                        if k
                        in {
                            "name",
                            "points",
                            "description",
                            "rule_type",
                            "aliases",
                            "field_key",
                            "expected_value",
                            "condition",
                            "enabled",
                        }
                    }
                    if "aliases" in allowed_fields and isinstance(allowed_fields["aliases"], list):
                        allowed_fields["aliases"] = tuple(allowed_fields["aliases"])
                    updated = replace(existing, **allowed_fields)
                else:
                    raise TypeError(f"Unsupported rule override type for '{key}': {type(override)}")
                self.register_rule(updated)
            else:
                # Create a new custom rule from key + int/dict specification
                slug = re.sub(r"[^a-z0-9]+", "_", key.strip().lower()).strip("_") or "custom_rule"
                if isinstance(override, int):
                    new_rule = ScoringRule(
                        rule_id=slug,
                        name=key.strip(),
                        points=override,
                        description=f"Custom scoring rule for '{key.strip()}'",
                        rule_type="activity",
                    )
                elif isinstance(override, dict):
                    points = int(override.get("points", 0))
                    new_rule = ScoringRule(
                        rule_id=str(override.get("rule_id", slug)),
                        name=str(override.get("name", key.strip())),
                        points=points,
                        description=str(
                            override.get("description", f"Custom rule for '{key.strip()}'")
                        ),
                        rule_type=str(override.get("rule_type", "activity")),
                        aliases=tuple(override.get("aliases", ())),
                        field_key=override.get("field_key"),
                        expected_value=override.get("expected_value"),
                        condition=override.get("condition"),
                        enabled=bool(override.get("enabled", True)),
                    )
                else:
                    raise TypeError(f"Unsupported rule override type for '{key}': {type(override)}")
                self.register_rule(new_rule)

    def evaluate(self, event: Union[LeadEvent, Dict[str, Any]]) -> ScoringResult:
        """Evaluate all enabled rules against a LeadEvent (or raw webhook payload dict)."""
        lead_event = (
            event
            if isinstance(event, LeadEvent)
            else LeadEvent.from_webhook_payload(event)
        )

        matched_rules: List[RuleMatch] = []
        total_delta = 0

        for rule in self._rules:
            match = rule.evaluate(lead_event)
            if match is not None:
                matched_rules.append(match)
                total_delta += match.points

        return ScoringResult(
            contact_id=lead_event.contact_id,
            points_delta=total_delta,
            matched_rules=matched_rules,
        )
