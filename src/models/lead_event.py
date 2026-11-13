"""Domain models for GoHighLevel webhook events, rule matches, and pipeline actions."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_custom_fields(raw_fields: Any) -> Dict[str, Any]:
    """Normalize GHL custom fields from either a dict or GHL v2 array format.

    GHL API v2 webhooks and contact endpoints can represent custom fields either as:
    1. A dictionary: ``{"budget": "$7,500", "lead_score": 25}``
    2. A list of objects: ``[{"id": "cf_1", "key": "contact.budget", "value": "$7,500"}]``
    """
    normalized: Dict[str, Any] = {}
    if isinstance(raw_fields, dict):
        for k, v in raw_fields.items():
            clean_key = str(k).strip()
            normalized[clean_key] = v
            if clean_key.startswith("contact."):
                normalized[clean_key.removeprefix("contact.")] = v
    elif isinstance(raw_fields, list):
        for item in raw_fields:
            if not isinstance(item, dict):
                continue
            val = (
                item.get("value")
                if "value" in item
                else item.get("field_value", item.get("fieldValue"))
            )
            for key_attr in ("key", "fieldKey", "name", "id"):
                raw_key = item.get(key_attr)
                if raw_key:
                    clean_key = str(raw_key).strip()
                    normalized[clean_key] = val
                    if clean_key.startswith("contact."):
                        normalized[clean_key.removeprefix("contact.")] = val
    return normalized


@dataclass
class LeadEvent:
    """Normalized representation of an incoming GoHighLevel webhook payload."""

    contact_id: str
    event_type: str = ""
    activities: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    custom_fields: Dict[str, Any] = field(default_factory=dict)
    opportunity_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    location_id: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    full_name: Optional[str] = None
    event_id: Optional[str] = None
    event_timestamp: Optional[str] = None
    last_activity_date: Optional[str] = None
    assigned_to: Optional[str] = None
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_webhook_payload(cls, payload: Dict[str, Any]) -> LeadEvent:
        """Construct a validated LeadEvent from a GHL webhook dictionary."""
        if not isinstance(payload, dict):
            raise ValueError("Webhook payload must be a JSON object/dictionary.")

        contact_block = (
            payload.get("contact") if isinstance(payload.get("contact"), dict) else {}
        )

        contact_id = (
            payload.get("contactId")
            or payload.get("contact_id")
            or contact_block.get("id")
            or contact_block.get("contactId")
            or payload.get("id")
        )
        if not contact_id or not str(contact_id).strip():
            raise ValueError(
                "Missing required contact identifier ('contactId' or 'contact_id')."
            )
        contact_id = str(contact_id).strip()

        event_type = str(
            payload.get("event_type")
            or payload.get("eventType")
            or payload.get("type")
            or payload.get("event")
            or payload.get("activity")
            or ""
        ).strip()

        # Collect activities from multiple potential webhook keys
        activities: List[str] = []
        raw_activities = (
            payload.get("activities")
            or payload.get("events")
            or contact_block.get("activities")
            or []
        )
        if isinstance(raw_activities, str):
            activities.extend(
                [act.strip() for act in raw_activities.split(",") if act.strip()]
            )
        elif isinstance(raw_activities, list):
            activities.extend(
                [str(act).strip() for act in raw_activities if str(act).strip()]
            )

        single_activity = payload.get("activity") or payload.get("action")
        if isinstance(single_activity, str) and single_activity.strip():
            if single_activity.strip() not in activities:
                activities.append(single_activity.strip())

        if event_type and event_type not in activities:
            activities.append(event_type)

        # Normalize tags
        raw_tags = (
            payload.get("tags") if "tags" in payload else contact_block.get("tags", [])
        )
        tags: List[str] = []
        if isinstance(raw_tags, str):
            tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
        elif isinstance(raw_tags, list):
            tags = [str(t).strip() for t in raw_tags if str(t).strip()]

        # Normalize custom fields
        merged_custom_fields: Dict[str, Any] = {}
        for source in (
            contact_block.get("customFields"),
            contact_block.get("custom_fields"),
            payload.get("customFields"),
            payload.get("custom_fields"),
        ):
            if source is not None:
                merged_custom_fields.update(normalize_custom_fields(source))

        # Top-level convenience fields (e.g. budget or lead_score passed directly)
        for direct_field in ("budget", "Budget", "lead_score", "leadScore"):
            if direct_field in payload and direct_field not in merged_custom_fields:
                merged_custom_fields[direct_field] = payload[direct_field]

        # Extract opportunity info if present
        opp_block = (
            payload.get("opportunity")
            if isinstance(payload.get("opportunity"), dict)
            else {}
        )
        opportunity_id = (
            payload.get("opportunityId")
            or payload.get("opportunity_id")
            or opp_block.get("id")
        )
        pipeline_id = (
            payload.get("pipelineId")
            or payload.get("pipeline_id")
            or opp_block.get("pipelineId")
        )
        location_id = (
            payload.get("locationId")
            or payload.get("location_id")
            or contact_block.get("locationId")
        )

        email = payload.get("email") or contact_block.get("email")
        phone = payload.get("phone") or contact_block.get("phone")
        full_name = (
            payload.get("fullName")
            or payload.get("full_name")
            or payload.get("name")
            or contact_block.get("name")
            or contact_block.get("fullName")
        )

        # Extract idempotency eventId / webhookId
        raw_event_id = (
            payload.get("eventId")
            or payload.get("event_id")
            or payload.get("webhookId")
            or payload.get("webhook_id")
            or payload.get("messageId")
        )
        event_id = str(raw_event_id).strip() if raw_event_id else None

        # Extract event timestamp
        raw_timestamp = (
            payload.get("timestamp")
            or payload.get("eventTimestamp")
            or payload.get("event_timestamp")
            or payload.get("createdAt")
            or payload.get("created_at")
            or payload.get("dateAdded")
        )
        event_timestamp = str(raw_timestamp).strip() if raw_timestamp else None

        # Extract lastActivityDate for inactivity time-decay engine
        raw_last_activity = (
            payload.get("lastActivityDate")
            or payload.get("last_activity_date")
            or contact_block.get("lastActivityDate")
            or contact_block.get("last_activity_date")
            or merged_custom_fields.get("lastActivityDate")
            or merged_custom_fields.get("last_activity_date")
        )
        last_activity_date = (
            str(raw_last_activity).strip() if raw_last_activity else None
        )

        # Extract assignedTo user ID for Speed-to-Lead task creation
        raw_assigned_to = (
            payload.get("assignedTo")
            or payload.get("assigned_to")
            or payload.get("userId")
            or payload.get("user_id")
            or contact_block.get("assignedTo")
            or contact_block.get("assigned_to")
        )
        assigned_to = str(raw_assigned_to).strip() if raw_assigned_to else None

        return cls(
            contact_id=contact_id,
            event_type=event_type,
            activities=activities,
            tags=tags,
            custom_fields=merged_custom_fields,
            opportunity_id=str(opportunity_id) if opportunity_id else None,
            pipeline_id=str(pipeline_id) if pipeline_id else None,
            location_id=str(location_id) if location_id else None,
            email=str(email) if email else None,
            phone=str(phone) if phone else None,
            full_name=str(full_name) if full_name else None,
            event_id=event_id,
            event_timestamp=event_timestamp,
            last_activity_date=last_activity_date,
            assigned_to=assigned_to,
            raw_payload=payload,
        )

    def get_initial_score_hint(self) -> Optional[int]:
        """Extract an initial/existing lead_score from custom fields if present."""
        for key in ("lead_score", "leadScore", "score", "Lead Score"):
            if key in self.custom_fields and self.custom_fields[key] is not None:
                try:
                    return int(float(str(self.custom_fields[key]).strip()))
                except (ValueError, TypeError):
                    continue
        return None

    def compute_idempotency_key(self) -> str:
        """Return explicit eventId/webhookId or deterministic SHA-256 hash of contactId + activity + timestamp."""
        if self.event_id:
            return self.event_id
        activity_signature = (
            "|".join(sorted(self.activities)) if self.activities else self.event_type
        )
        timestamp_part = self.event_timestamp or ""
        composite = f"{self.contact_id}:{activity_signature}:{timestamp_part}"
        digest = hashlib.sha256(composite.encode("utf-8")).hexdigest()
        return f"sha256:{digest}"


@dataclass(frozen=True)
class RuleMatch:
    """Represents a single scoring rule triggered by a LeadEvent."""

    rule_id: str
    rule_name: str
    points: int
    reason: str
    category: str = "positive"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize rule match to a JSON-friendly dictionary."""
        return {
            "ruleId": self.rule_id,
            "ruleName": self.rule_name,
            "points": self.points,
            "reason": self.reason,
            "category": self.category,
        }


@dataclass
class ScoringResult:
    """Aggregated evaluation result produced by the LeadScoringEngine."""

    contact_id: str
    points_delta: int
    matched_rules: List[RuleMatch] = field(default_factory=list)
    decay_points: int = 0
    inactive_days: int = 0
    evaluated_at: str = field(default_factory=_utc_now_iso)

    @property
    def positive_matches(self) -> List[RuleMatch]:
        """Return only positive scoring rule matches."""
        return [m for m in self.matched_rules if m.points > 0]

    @property
    def negative_matches(self) -> List[RuleMatch]:
        """Return only negative scoring rule matches."""
        return [m for m in self.matched_rules if m.points < 0]

    @property
    def matched_rule_names(self) -> List[str]:
        """Return list of human-readable names of all matched rules."""
        return [m.rule_name for m in self.matched_rules]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize scoring result to a dictionary."""
        return {
            "contactId": self.contact_id,
            "pointsDelta": self.points_delta,
            "decayPoints": self.decay_points,
            "inactiveDays": self.inactive_days,
            "matchedRules": [m.to_dict() for m in self.matched_rules],
            "evaluatedAt": self.evaluated_at,
        }


@dataclass
class PipelineActionResult:
    """Outcome of applying a ScoringResult to GoHighLevel CRM via API v2."""

    contact_id: str
    previous_score: int
    points_delta: int
    new_score: int
    threshold_status: str
    tags_added: List[str] = field(default_factory=list)
    tags_removed: List[str] = field(default_factory=list)
    opportunity_stage_moved: Optional[str] = None
    task_created: Optional[Dict[str, Any]] = None
    note_body: str = ""
    idempotency_key: str = ""
    duplicate_ignored: bool = False
    dry_run: bool = True
    api_operations: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize pipeline action result for Lambda HTTP response."""
        return {
            "contactId": self.contact_id,
            "previousScore": self.previous_score,
            "pointsDelta": self.points_delta,
            "newScore": self.new_score,
            "thresholdStatus": self.threshold_status,
            "tagsAdded": self.tags_added,
            "tagsRemoved": self.tags_removed,
            "opportunityStageMoved": self.opportunity_stage_moved,
            "taskCreated": self.task_created,
            "noteBody": self.note_body,
            "idempotencyKey": self.idempotency_key,
            "duplicateIgnored": self.duplicate_ignored,
            "dryRun": self.dry_run,
            "apiOperations": self.api_operations,
        }
