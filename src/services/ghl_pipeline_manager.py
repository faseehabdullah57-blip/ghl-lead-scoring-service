"""GoHighLevel (GHL API v2) client for contact score updates, pipeline stages, tags, tasks, and notes."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.models.lead_event import (
    LeadEvent,
    PipelineActionResult,
    ScoringResult,
    normalize_custom_fields,
)
from src.rules.scoring_rules import parse_iso_datetime

logger = logging.getLogger(__name__)


class GHLAPIError(Exception):
    """Raised when a GoHighLevel API v2 request fails after retries."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[Any] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class IdempotencyCache:
    """In-memory TTL cache for deduplicating webhook events across warm Lambda executions."""

    def __init__(self, ttl_seconds: int = 3600) -> None:
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._entries: Dict[str, Tuple[float, Dict[str, Any]]] = {}

    def _evict_expired(self, now: float) -> None:
        expired = [k for k, (exp, _) in self._entries.items() if exp <= now]
        for k in expired:
            self._entries.pop(k, None)

    def has_processed(self, key: str) -> bool:
        """Return True if the idempotency key is active in the TTL cache."""
        if not key:
            return False
        now = time.time()
        self._evict_expired(now)
        return key in self._entries

    def get_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached metadata for a processed idempotency key."""
        if not self.has_processed(key):
            return None
        return self._entries[key][1]

    def mark_processed(
        self, key: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record an idempotency key as processed until TTL expiration."""
        if not key:
            return
        now = time.time()
        self._evict_expired(now)
        self._entries[key] = (now + self.ttl_seconds, dict(metadata or {}))

    def clear(self) -> None:
        """Clear all entries from the cache."""
        self._entries.clear()


# Module-level warm container idempotency cache for AWS Lambda reuse
GLOBAL_IDEMPOTENCY_CACHE = IdempotencyCache(ttl_seconds=3600)


class GHLPipelineManager:
    """Lightweight GoHighLevel API v2 client with retry/backoff, idempotency, tasks, and dry-run mode."""

    DEFAULT_BASE_URL = "https://services.leadconnectorhq.com"
    DEFAULT_API_VERSION = "2021-07-28"

    HOT_LEAD_THRESHOLD = 50
    DISQUALIFIED_THRESHOLD = 0

    HOT_LEAD_TAG = "Hot Lead"
    REENGAGEMENT_TAG = "Re-Engagement Needed"
    DISQUALIFIED_TAG = "Disqualified"
    PRIORITY_OUTREACH_STAGE = "Priority Outreach"
    SPEED_TO_LEAD_DUE_MINUTES = 15

    RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        location_id: Optional[str] = None,
        lead_score_field_id: Optional[str] = None,
        priority_stage_id: Optional[str] = None,
        dry_run: Optional[bool] = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        timeout_seconds: int = 10,
        session: Optional[requests.Session] = None,
        idempotency_cache: Optional[IdempotencyCache] = None,
        idempotency_ttl_seconds: int = 3600,
    ) -> None:
        resolved_key = (
            api_key
            if api_key is not None
            else os.environ.get("GHL_API_KEY", "").strip()
        )
        self.api_key: str = resolved_key
        self.base_url: str = (
            base_url
            or os.environ.get("GHL_API_BASE_URL", self.DEFAULT_BASE_URL)
        ).rstrip("/")
        self.location_id: str = (
            location_id or os.environ.get("GHL_LOCATION_ID", "loc_default")
        )
        self.lead_score_field_id: str = (
            lead_score_field_id
            or os.environ.get("GHL_LEAD_SCORE_FIELD_ID", "lead_score")
        )
        self.priority_stage_id: str = (
            priority_stage_id
            or os.environ.get("GHL_PRIORITY_STAGE_ID", "stage_priority_outreach")
        )

        if dry_run is not None:
            self.dry_run = bool(dry_run)
        else:
            env_dry_run = os.environ.get("GHL_DRY_RUN", "").strip().lower()
            if env_dry_run in ("1", "true", "yes"):
                self.dry_run = True
            elif env_dry_run in ("0", "false", "no") and bool(self.api_key):
                self.dry_run = False
            else:
                self.dry_run = not bool(self.api_key)

        self.max_retries = max(0, int(max_retries))
        self.backoff_factor = max(0.0, float(backoff_factor))
        self.timeout_seconds = timeout_seconds
        self._session = session or requests.Session()
        self.idempotency_cache = (
            idempotency_cache
            if idempotency_cache is not None
            else IdempotencyCache(ttl_seconds=idempotency_ttl_seconds)
        )

        # In-memory stores for dry-run state verification and testing
        self._mock_contacts: Dict[str, Dict[str, Any]] = {}
        self._mock_notes: Dict[str, List[Dict[str, Any]]] = {}
        self._mock_tasks: Dict[str, List[Dict[str, Any]]] = {}
        self._recorded_operations: List[Dict[str, Any]] = []

    @property
    def headers(self) -> Dict[str, str]:
        """Build standard GoHighLevel API v2 headers."""
        token = self.api_key or "DRY_RUN_MOCK_TOKEN"
        return {
            "Authorization": f"Bearer {token}",
            "Version": self.DEFAULT_API_VERSION,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def seed_mock_contact(
        self,
        contact_id: str,
        lead_score: int = 0,
        tags: Optional[List[str]] = None,
        opportunity_id: Optional[str] = None,
        custom_fields: Optional[Dict[str, Any]] = None,
        notes: Optional[List[str]] = None,
    ) -> None:
        """Seed mock contact state for local dry-run or unit testing."""
        fields = dict(custom_fields or {})
        fields["lead_score"] = lead_score
        note_objs = [
            {
                "id": f"note_{contact_id}_{i}",
                "contactId": contact_id,
                "body": body,
            }
            for i, body in enumerate(notes or [])
        ]
        self._mock_notes[contact_id] = list(note_objs)
        self._mock_contacts[contact_id] = {
            "id": contact_id,
            "tags": list(tags or []),
            "opportunityId": opportunity_id or f"opp_{contact_id}",
            "notes": list(note_objs),
            "customFields": [
                {"id": self.lead_score_field_id, "key": k, "field_value": v}
                for k, v in fields.items()
            ],
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute an HTTP request against GHL API v2 with exponential backoff or dry-run simulation."""
        path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        url = f"{self.base_url}{path}"

        operation_record: Dict[str, Any] = {
            "method": method.upper(),
            "endpoint": path,
            "url": url,
            "payload": payload,
            "params": params,
            "dryRun": self.dry_run,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        self._recorded_operations.append(operation_record)

        if self.dry_run:
            logger.info(
                "[DRY-RUN] %s %s | payload=%s params=%s",
                method.upper(),
                path,
                payload,
                params,
            )
            return self._simulate_dry_run_response(method.upper(), path, payload, params)

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self._session.request(
                    method=method.upper(),
                    url=url,
                    headers=self.headers,
                    json=payload,
                    params=params,
                    timeout=self.timeout_seconds,
                )

                if response.status_code in self.RETRYABLE_STATUS_CODES:
                    if attempt < self.max_retries:
                        sleep_time = self.backoff_factor * (2 ** attempt)
                        logger.warning(
                            "GHL API returned %s on %s %s (attempt %d/%d). Retrying in %.2fs...",
                            response.status_code,
                            method.upper(),
                            path,
                            attempt + 1,
                            self.max_retries + 1,
                            sleep_time,
                        )
                        if sleep_time > 0:
                            time.sleep(sleep_time)
                        continue
                    raise GHLAPIError(
                        f"GHL API request failed after {self.max_retries + 1} attempts with status {response.status_code}",
                        status_code=response.status_code,
                        response_body=response.text,
                    )

                if response.status_code >= 400:
                    raise GHLAPIError(
                        f"GHL API error {response.status_code} on {method.upper()} {path}: {response.text}",
                        status_code=response.status_code,
                        response_body=response.text,
                    )

                if not response.text:
                    return {}
                return response.json()

            except requests.RequestException as exc:
                last_error = exc
                if attempt < self.max_retries:
                    sleep_time = self.backoff_factor * (2 ** attempt)
                    logger.warning(
                        "Network error on %s %s (attempt %d/%d): %s. Retrying in %.2fs...",
                        method.upper(),
                        path,
                        attempt + 1,
                        self.max_retries + 1,
                        exc,
                        sleep_time,
                    )
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    continue
                raise GHLAPIError(
                    f"Network failure communicating with GHL API v2: {exc}"
                ) from exc

        raise GHLAPIError(f"GHL API request failed: {last_error}")

    def _simulate_dry_run_response(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]],
        params: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Generate realistic GHL API v2 mock responses during dry-run execution."""
        parts = [p for p in path.split("/") if p]

        # GET /contacts/{contactId}
        if method == "GET" and len(parts) == 2 and parts[0] == "contacts":
            contact_id = parts[1]
            contact = self._mock_contacts.get(
                contact_id,
                {
                    "id": contact_id,
                    "tags": [],
                    "notes": self._mock_notes.get(contact_id, []),
                    "customFields": [],
                },
            )
            return {"contact": contact}

        # PUT /contacts/{contactId}
        if method == "PUT" and len(parts) == 2 and parts[0] == "contacts":
            contact_id = parts[1]
            contact = self._mock_contacts.setdefault(
                contact_id,
                {"id": contact_id, "tags": [], "notes": [], "customFields": []},
            )
            if payload and "customFields" in payload:
                contact["customFields"] = payload["customFields"]
            return {"contact": contact, "succeded": True}

        # POST /contacts/{contactId}/tags
        if method == "POST" and len(parts) == 3 and parts[0] == "contacts" and parts[2] == "tags":
            contact_id = parts[1]
            new_tags = (payload or {}).get("tags", [])
            contact = self._mock_contacts.setdefault(
                contact_id,
                {"id": contact_id, "tags": [], "notes": [], "customFields": []},
            )
            for tag in new_tags:
                if tag not in contact["tags"]:
                    contact["tags"].append(tag)
            return {"tags": contact["tags"]}

        # DELETE /contacts/{contactId}/tags
        if method == "DELETE" and len(parts) == 3 and parts[0] == "contacts" and parts[2] == "tags":
            contact_id = parts[1]
            removed_tags = (payload or {}).get("tags", [])
            contact = self._mock_contacts.setdefault(
                contact_id,
                {"id": contact_id, "tags": [], "notes": [], "customFields": []},
            )
            contact["tags"] = [t for t in contact["tags"] if t not in removed_tags]
            return {"tags": contact["tags"]}

        # GET /contacts/{contactId}/notes
        if method == "GET" and len(parts) == 3 and parts[0] == "contacts" and parts[2] == "notes":
            contact_id = parts[1]
            return {"notes": self._mock_notes.get(contact_id, [])}

        # POST /contacts/{contactId}/notes
        if method == "POST" and len(parts) == 3 and parts[0] == "contacts" and parts[2] == "notes":
            contact_id = parts[1]
            note_obj = {
                "id": f"note_{contact_id}_{len(self._mock_notes.get(contact_id, [])) + 1}",
                "contactId": contact_id,
                "body": (payload or {}).get("body", ""),
                "dateAdded": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            self._mock_notes.setdefault(contact_id, []).append(note_obj)
            if contact_id in self._mock_contacts:
                self._mock_contacts[contact_id].setdefault("notes", []).append(note_obj)
            return {"note": note_obj}

        # POST /contacts/{contactId}/tasks
        if method == "POST" and len(parts) == 3 and parts[0] == "contacts" and parts[2] == "tasks":
            contact_id = parts[1]
            task_obj = {
                "id": f"task_{contact_id}_{len(self._mock_tasks.get(contact_id, [])) + 1}",
                "contactId": contact_id,
                **(payload or {}),
            }
            self._mock_tasks.setdefault(contact_id, []).append(task_obj)
            return {"task": task_obj}

        # GET /opportunities/search
        if method == "GET" and path == "/opportunities/search":
            contact_id = (params or {}).get("contact_id", "unknown")
            return {
                "opportunities": [
                    {
                        "id": f"opp_{contact_id}",
                        "contactId": contact_id,
                        "status": "open",
                        "pipelineStageId": "stage_initial",
                    }
                ]
            }

        # PUT /opportunities/{opportunityId}
        if method == "PUT" and len(parts) == 2 and parts[0] == "opportunities":
            opp_id = parts[1]
            return {
                "opportunity": {
                    "id": opp_id,
                    **(payload or {}),
                }
            }

        return {"status": "ok"}

    def get_contact(self, contact_id: str) -> Dict[str, Any]:
        """Fetch a contact record from GHL API v2 via GET /contacts/{contactId}."""
        response = self._request("GET", f"/contacts/{contact_id}")
        if isinstance(response.get("contact"), dict):
            return response["contact"]
        return response

    def get_contact_notes(self, contact_id: str) -> List[Dict[str, Any]]:
        """Fetch recent notes for a contact via GET /contacts/{contactId}/notes."""
        response = self._request("GET", f"/contacts/{contact_id}/notes")
        notes = response.get("notes", [])
        return notes if isinstance(notes, list) else []

    @staticmethod
    def _extract_score_from_contact(
        contact_data: Dict[str, Any],
        lead_score_field_id: str,
        fallback_score: Optional[int] = None,
    ) -> int:
        """Extract integer lead_score from fetched contact dictionary."""
        raw_fields = (
            contact_data.get("customFields")
            if "customFields" in contact_data
            else contact_data.get("custom_fields", [])
        )
        normalized = normalize_custom_fields(raw_fields)

        for key in (
            lead_score_field_id,
            "lead_score",
            "leadScore",
            "score",
            "Lead Score",
        ):
            if key in normalized and normalized[key] is not None:
                try:
                    return int(float(str(normalized[key]).strip()))
                except (ValueError, TypeError):
                    continue

        if fallback_score is not None:
            return int(fallback_score)
        return 0

    @staticmethod
    def _contact_has_processed_event(
        contact_data: Dict[str, Any], idempotency_key: str
    ) -> bool:
        """Check contact notes and metadata/customFields for a previously processed idempotency key."""
        if not idempotency_key:
            return False

        # 1. Check customFields metadata (e.g., last_event_id)
        raw_fields = (
            contact_data.get("customFields")
            if "customFields" in contact_data
            else contact_data.get("custom_fields", [])
        )
        normalized_fields = normalize_custom_fields(raw_fields)
        for meta_key in ("last_event_id", "lastEventId", "idempotency_key"):
            if str(normalized_fields.get(meta_key, "")).strip() == idempotency_key:
                return True

        # 2. Check contact notes attached to contact record
        notes = contact_data.get("notes", [])
        if isinstance(notes, list):
            for note in notes:
                body = note.get("body", "") if isinstance(note, dict) else str(note)
                if idempotency_key in body:
                    return True

        return False

    def is_duplicate_event(
        self,
        idempotency_key: str,
        contact_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Check warm TTL cache and contact notes/metadata for duplicate webhook execution."""
        if not idempotency_key:
            return False
        if self.idempotency_cache.has_processed(idempotency_key):
            return True
        if contact_data and self._contact_has_processed_event(
            contact_data, idempotency_key
        ):
            self.idempotency_cache.mark_processed(idempotency_key)
            return True
        return False

    def get_current_score(
        self, contact_id: str, fallback_score: Optional[int] = None
    ) -> int:
        """Fetch the contact's current lead_score from custom fields, defaulting to fallback or 0."""
        contact_data = self.get_contact(contact_id)
        return self._extract_score_from_contact(
            contact_data=contact_data,
            lead_score_field_id=self.lead_score_field_id,
            fallback_score=fallback_score,
        )

    def update_contact_score(self, contact_id: str, new_score: int) -> Dict[str, Any]:
        """Update the contact's lead_score custom field via PUT /contacts/{contactId}."""
        payload = {
            "customFields": [
                {
                    "id": self.lead_score_field_id,
                    "key": "lead_score",
                    "field_value": int(new_score),
                }
            ]
        }
        return self._request("PUT", f"/contacts/{contact_id}", payload=payload)

    def add_contact_tags(self, contact_id: str, tags: List[str]) -> Dict[str, Any]:
        """Attach one or more tags to a contact via POST /contacts/{contactId}/tags."""
        payload = {"tags": list(tags)}
        return self._request("POST", f"/contacts/{contact_id}/tags", payload=payload)

    def remove_contact_tags(self, contact_id: str, tags: List[str]) -> Dict[str, Any]:
        """Remove one or more tags from a contact via DELETE /contacts/{contactId}/tags."""
        payload = {"tags": list(tags)}
        return self._request("DELETE", f"/contacts/{contact_id}/tags", payload=payload)

    def create_speed_to_lead_task(
        self,
        event: LeadEvent,
        scoring_result: ScoringResult,
        new_score: int,
    ) -> Dict[str, Any]:
        """Create an urgent Speed-to-Lead follow-up task via POST /contacts/{contactId}/tasks."""
        base_dt = (
            parse_iso_datetime(event.event_timestamp)
            or parse_iso_datetime(scoring_result.evaluated_at)
            or datetime.now(timezone.utc)
        )
        due_dt = base_dt + timedelta(minutes=self.SPEED_TO_LEAD_DUE_MINUTES)
        due_date_iso = due_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        positive_Rules = [
            f"{m.rule_name} ({m.points:+d} pts)"
            for m in scoring_result.positive_matches
        ]
        behaviors_summary = (
            ", ".join(positive_Rules)
            if positive_Rules
            else "Threshold crossed (>= 50 pts)"
        )
        task_body = (
            f"High-intent behaviors matched: {behaviors_summary}. "
            f"Immediate Speed-to-Lead outreach required within {self.SPEED_TO_LEAD_DUE_MINUTES} minutes."
        )

        payload: Dict[str, Any] = {
            "title": f"URGENT: Call Hot Lead (Score: {new_score})",
            "body": task_body,
            "dueDate": due_date_iso,
            "completed": False,
        }
        if event.assigned_to:
            payload["assignedTo"] = event.assigned_to

        self._request("POST", f"/contacts/{event.contact_id}/tasks", payload=payload)
        return payload

    def resolve_active_opportunity_id(
        self, contact_id: str, hint_opportunity_id: Optional[str] = None
    ) -> str:
        """Resolve the contact's active opportunity ID or search GHL opportunities."""
        if hint_opportunity_id:
            return hint_opportunity_id

        search_res = self._request(
            "GET",
            "/opportunities/search",
            params={"location_id": self.location_id, "contact_id": contact_id},
        )
        opportunities = search_res.get("opportunities", [])
        if isinstance(opportunities, list) and opportunities:
            first_opp = opportunities[0]
            if isinstance(first_opp, dict) and first_opp.get("id"):
                return str(first_opp["id"])

        return f"opp_{contact_id}"

    def move_opportunity_to_stage(
        self,
        contact_id: str,
        stage_name: str = PRIORITY_OUTREACH_STAGE,
        stage_id: Optional[str] = None,
        opportunity_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Move the contact's active opportunity to the target stage via PUT /opportunities/{opportunityId}."""
        resolved_opp_id = self.resolve_active_opportunity_id(
            contact_id=contact_id,
            hint_opportunity_id=opportunity_id,
        )
        payload = {
            "pipelineStageId": stage_id or self.priority_stage_id,
            "stageName": stage_name,
            "status": "open",
        }
        return self._request("PUT", f"/opportunities/{resolved_opp_id}", payload=payload)

    def create_contact_note(self, contact_id: str, body: str) -> Dict[str, Any]:
        """Append a timestamped audit note to the contact via POST /contacts/{contactId}/notes."""
        payload = {"body": body}
        return self._request("POST", f"/contacts/{contact_id}/notes", payload=payload)

    @staticmethod
    def format_scoring_note(
        scoring_result: ScoringResult,
        previous_score: int,
        new_score: int,
        threshold_status: str,
        tags_added: List[str],
        stage_moved: Optional[str],
        tags_removed: Optional[List[str]] = None,
        task_created: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> str:
        """Format a clear, timestamped contact note summarizing point breakdown and score totals."""
        delta_str = (
            f"+{scoring_result.points_delta}"
            if scoring_result.points_delta >= 0
            else str(scoring_result.points_delta)
        )
        lines = [
            f"[{scoring_result.evaluated_at}] Automated Lead Scoring Update",
            f"Previous Score: {previous_score} | Change: {delta_str} | New Total: {new_score}",
            f"Status: {threshold_status}",
            "Point Breakdown:",
        ]

        if scoring_result.matched_rules:
            for match in scoring_result.matched_rules:
                pts_str = f"+{match.points}" if match.points >= 0 else str(match.points)
                lines.append(
                    f"  - {match.rule_name}: {pts_str} pts ({match.reason})"
                )
        else:
            lines.append("  - No scoring rules triggered (+0 pts)")

        actions: List[str] = []
        if tags_removed:
            actions.append(f"Removed tag(s): {', '.join(tags_removed)}")
        if tags_added:
            actions.append(f"Added tag(s): {', '.join(tags_added)}")
        if stage_moved:
            actions.append(f"Moved opportunity to stage: {stage_moved}")
        if task_created:
            actions.append(f"Created task: {task_created.get('title')}")
        if actions:
            lines.append(f"Actions Triggered: {'; '.join(actions)}")

        if idempotency_key:
            lines.append(f"Idempotency Key: {idempotency_key}")

        return "\n".join(lines)

    def process_scoring_result(
        self,
        event: LeadEvent,
        scoring_result: ScoringResult,
    ) -> PipelineActionResult:
        """Execute atomic score update, evaluate thresholds/decay/tasks, and log audit note in GoHighLevel."""
        start_op_idx = len(self._recorded_operations)
        contact_id = event.contact_id
        idempotency_key = event.compute_idempotency_key()

        # Fast-path warm-cache idempotency check before making upstream API calls
        if self.idempotency_cache.has_processed(idempotency_key):
            cached_meta = self.idempotency_cache.get_metadata(idempotency_key) or {}
            cached_score = int(
                cached_meta.get(
                    "new_score",
                    event.get_initial_score_hint() or 0,
                )
            )
            logger.info(
                "Duplicate webhook event ignored via warm TTL cache (key=%s, contact=%s)",
                idempotency_key,
                contact_id,
            )
            return PipelineActionResult(
                contact_id=contact_id,
                previous_score=cached_score,
                points_delta=0,
                new_score=cached_score,
                threshold_status="DUPLICATE_IGNORED",
                idempotency_key=idempotency_key,
                duplicate_ignored=True,
                dry_run=self.dry_run,
                api_operations=self._recorded_operations[start_op_idx:],
            )

        # 1. Fetch contact record to inspect current score and recent notes/metadata
        contact_data = self.get_contact(contact_id)
        previous_score = self._extract_score_from_contact(
            contact_data=contact_data,
            lead_score_field_id=self.lead_score_field_id,
            fallback_score=event.get_initial_score_hint(),
        )

        # Cold-start idempotency check against contact notes & metadata
        if self._contact_has_processed_event(contact_data, idempotency_key):
            self.idempotency_cache.mark_processed(
                idempotency_key, {"new_score": previous_score}
            )
            logger.info(
                "Duplicate webhook event ignored via contact notes/metadata (key=%s, contact=%s)",
                idempotency_key,
                contact_id,
            )
            return PipelineActionResult(
                contact_id=contact_id,
                previous_score=previous_score,
                points_delta=0,
                new_score=previous_score,
                threshold_status="DUPLICATE_IGNORED",
                idempotency_key=idempotency_key,
                duplicate_ignored=True,
                dry_run=self.dry_run,
                api_operations=self._recorded_operations[start_op_idx:],
            )

        new_score = previous_score + scoring_result.points_delta

        # 2. Update contact custom field `lead_score` via PUT /contacts/{contactId}
        self.update_contact_score(contact_id=contact_id, new_score=new_score)

        # 3. Evaluate thresholds, Hot Lead downgrade on decay, and Speed-to-Lead task creation
        tags_added: List[str] = []
        tags_removed: List[str] = []
        opportunity_stage_moved: Optional[str] = None
        task_created: Optional[Dict[str, Any]] = None
        threshold_status = "STANDARD"

        # Check if contact was previously a Hot Lead (>= 50) and dropped below 50
        if previous_score >= self.HOT_LEAD_THRESHOLD and new_score < self.HOT_LEAD_THRESHOLD:
            threshold_status = "RE_ENGAGEMENT_NEEDED"
            tags_removed.append(self.HOT_LEAD_TAG)
            self.remove_contact_tags(contact_id=contact_id, tags=[self.HOT_LEAD_TAG])
            tags_added.append(self.REENGAGEMENT_TAG)
            self.add_contact_tags(contact_id=contact_id, tags=[self.REENGAGEMENT_TAG])

        if new_score >= self.HOT_LEAD_THRESHOLD:
            threshold_status = "HOT_LEAD"
            tags_added.append(self.HOT_LEAD_TAG)
            self.add_contact_tags(contact_id=contact_id, tags=[self.HOT_LEAD_TAG])
            self.move_opportunity_to_stage(
                contact_id=contact_id,
                stage_name=self.PRIORITY_OUTREACH_STAGE,
                opportunity_id=event.opportunity_id,
            )
            opportunity_stage_moved = self.PRIORITY_OUTREACH_STAGE

            # Create urgent Speed-to-Lead follow-up task when newly crossing >= 50
            if previous_score < self.HOT_LEAD_THRESHOLD:
                task_created = self.create_speed_to_lead_task(
                    event=event,
                    scoring_result=scoring_result,
                    new_score=new_score,
                )
        elif new_score < self.DISQUALIFIED_THRESHOLD:
            threshold_status = "DISQUALIFIED"
            tags_added.append(self.DISQUALIFIED_TAG)
            self.add_contact_tags(contact_id=contact_id, tags=[self.DISQUALIFIED_TAG])

        # 4. Append timestamped contact note summarizing breakdown, previous score, and new total
        note_body = self.format_scoring_note(
            scoring_result=scoring_result,
            previous_score=previous_score,
            new_score=new_score,
            threshold_status=threshold_status,
            tags_added=tags_added,
            stage_moved=opportunity_stage_moved,
            tags_removed=tags_removed,
            task_created=task_created,
            idempotency_key=idempotency_key,
        )
        self.create_contact_note(contact_id=contact_id, body=note_body)

        # 5. Record idempotency key in TTL cache
        self.idempotency_cache.mark_processed(
            idempotency_key,
            {
                "contact_id": contact_id,
                "previous_score": previous_score,
                "new_score": new_score,
                "threshold_status": threshold_status,
            },
        )

        operations_for_turn = self._recorded_operations[start_op_idx:]

        return PipelineActionResult(
            contact_id=contact_id,
            previous_score=previous_score,
            points_delta=scoring_result.points_delta,
            new_score=new_score,
            threshold_status=threshold_status,
            tags_added=tags_added,
            tags_removed=tags_removed,
            opportunity_stage_moved=opportunity_stage_moved,
            task_created=task_created,
            note_body=note_body,
            idempotency_key=idempotency_key,
            duplicate_ignored=False,
            dry_run=self.dry_run,
            api_operations=operations_for_turn,
        )
