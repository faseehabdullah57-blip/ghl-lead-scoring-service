"""Standalone, configurable Lead Scoring Rules Engine with Inactivity Time-Decay for GoHighLevel."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from src.models.lead_event import LeadEvent, RuleMatch, ScoringResult


def _normalize_token(value: str) -> str:
    """Normalize a string token for case-insensitive comparison."""
    return re.sub(r"[\s_\-]+", " ", str(value).strip().lower())


def parse_iso_datetime(raw_value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp string into a timezone-aware UTC datetime."""
    if not raw_value or not isinstance(raw_value, str):
        return None
    cleaned = raw_value.strip()
    if not cleaned:
        return None
    if cleaned.endswith("Z") or cleaned.endswith("z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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
class InactivityDecayConfig:
    """Configuration for the 7-day Inactivity Time-Decay Engine."""

    enabled: bool = True
    period_days: int = 7
    points_per_period: int = -5
    max_decay_points: int = -30
    rule_id: str = "inactivity_time_decay"
    rule_name: str = "Inactivity Time-Decay"


def evaluate_inactivity_decay(
    event: LeadEvent,
    config: InactivityDecayConfig = InactivityDecayConfig(),
    reference_time: Optional[datetime] = None,
) -> Tuple[Optional[RuleMatch], int]:
    """Calculate 7-day inactivity decay penalty from `lastActivityDate` on the contact payload.

    Returns a tuple of ``(RuleMatch | None, inactive_days)``.
    Default penalty is -5 points per full 7 days of inactivity, capped at -30 points maximum decay.
    """
    if not config.enabled or not event.last_activity_date:
        return None, 0

    last_activity_dt = parse_iso_datetime(event.last_activity_date)
    if last_activity_dt is None:
        return None, 0

    ref_dt = (
        reference_time
        or parse_iso_datetime(event.event_timestamp)
        or datetime.now(timezone.utc)
    )
    if ref_dt.tzinfo is None:
        ref_dt = ref_dt.replace(tzinfo=timezone.utc)

    elapsed_seconds = (ref_dt - last_activity_dt).total_seconds()
    if elapsed_seconds <= 0:
        return None, 0

    inactive_days = int(elapsed_seconds // 86400)
    period_length = max(1, int(config.period_days))
    full_periods = inactive_days // period_length
    if full_periods <= 0:
        return None, inactive_days

    step_penalty = -abs(int(config.points_per_period))
    cap_penalty = -abs(int(config.max_decay_points))
    raw_penalty = step_penalty * full_periods
    applied_penalty = max(raw_penalty, cap_penalty)

    cap_suffix = " [capped at max decay]" if raw_penalty < cap_penalty else ""
    reason = (
        f"{inactive_days} days inactive ({full_periods}x {period_length}-day "
        f"periods since {event.last_activity_date}){cap_suffix}"
    )

    match = RuleMatch(
        rule_id=config.rule_id,
        rule_name=config.rule_name,
        points=applied_penalty,
        reason=reason,
        category="negative",
    )
    return match, inactive_days


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
    """Configurable rules engine for evaluating GoHighLevel lead events and inactivity decay."""

    def __init__(
        self,
        rules: Optional[Iterable[ScoringRule]] = None,
        rule_overrides: Optional[Dict[str, RuleOverrideValue]] = None,
        decay_config: Optional[InactivityDecayConfig] = None,
        decay_points_per_period: int = -5,
        max_decay_points: int = -30,
        decay_period_days: int = 7,
    ) -> None:
        base_rules = list(rules) if rules is not None else list(DEFAULT_SCORING_RULES)
        self._rules: List[ScoringRule] = list(base_rules)
        self.decay_config: InactivityDecayConfig = decay_config or InactivityDecayConfig(
            enabled=True,
            period_days=decay_period_days,
            points_per_period=decay_points_per_period,
            max_decay_points=max_decay_points,
        )
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
        if norm_id in ("inactivity time decay", "inactivity_time_decay"):
            self.decay_config = replace(self.decay_config, enabled=False)
            return True
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
        - ``{"Inactivity Time-Decay": {"points_per_period": -10, "max_decay_points": -40}}``
        - ``{"Attended Webinar": 12}`` (registers a new activity/tag rule dynamically)
        - ``{"custom_rule": ScoringRule(...)}`` (registers a full ScoringRule instance)
        """
        for key, override in overrides.items():
            norm_key = _normalize_token(key)
            if norm_key in ("inactivity time decay", "inactivity_time_decay"):
                if isinstance(override, int):
                    self.decay_config = replace(self.decay_config, points_per_period=override)
                elif isinstance(override, dict):
                    decay_updates: Dict[str, Any] = {}
                    if "enabled" in override:
                        decay_updates["enabled"] = bool(override["enabled"])
                    if "period_days" in override:
                        decay_updates["period_days"] = int(override["period_days"])
                    if "points_per_period" in override or "points" in override:
                        decay_updates["points_per_period"] = int(
                            override.get("points_per_period", override.get("points"))
                        )
                    if "max_decay_points" in override:
                        decay_updates["max_decay_points"] = int(override["max_decay_points"])
                    self.decay_config = replace(self.decay_config, **decay_updates)
                continue

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

    def evaluate(
        self,
        event: Union[LeadEvent, Dict[str, Any]],
        reference_time: Optional[datetime] = None,
    ) -> ScoringResult:
        """Evaluate all enabled rules and inactivity time-decay against a LeadEvent."""
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

        # Evaluate 7-day Inactivity Time-Decay Engine
        decay_match, inactive_days = evaluate_inactivity_decay(
            lead_event,
            config=self.decay_config,
            reference_time=reference_time,
        )
        decay_points = 0
        if decay_match is not None:
            matched_rules.append(decay_match)
            total_delta += decay_match.points
            decay_points = decay_match.points

        return ScoringResult(
            contact_id=lead_event.contact_id,
            points_delta=total_delta,
            matched_rules=matched_rules,
            decay_points=decay_points,
            inactive_days=inactive_days,
        )
